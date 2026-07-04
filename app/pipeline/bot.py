import json
import os
import posixpath
import re
import sqlite3
import sys
import threading
import time
import urllib.parse
import urllib.request
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, wait
from contextlib import nullcontext
from dataclasses import dataclass
from pathlib import Path

from pipeline.client115 import Client115
from pipeline.config import category_to_folder_id, category_to_msg_library_root, category_to_openlist_path
from pipeline.mediastation import (
    DEFAULT_MSG_BASE_URL,
    MediaStationClient,
    extract_codes,
    iter_code_matches,
    extract_media_id,
    extract_media_items,
    find_matching_media,
    media_haystack,
)
from pipeline.msgdb import DEFAULT_MSG_DATABASE_DSN, MediaStationDbClient, build_migration_target
from pipeline.offline_tasks import cancel_task_if_active, find_task_by_info_hash, find_tasks_by_info_hashes, task_can_cancel
from pipeline.openlist import DEFAULT_OPENLIST_URL, OpenListClient, OpenListTokenProvider
from pipeline.openlist_tokens import OpenListTokenStore
from pipeline.prowlarr import (
    DEFAULT_PROWLARR_CONFIG,
    DEFAULT_PROWLARR_URL,
    ProwlarrClient,
    ProwlarrConfig,
    is_prowlarr_download_uri,
)
from pipeline.resource_selector import ResourceSelector
from pipeline.version import format_version_info


DEFAULT_STATE_DB = "/bot-data/state.db"
DEFAULT_OPENLIST_DB = "/openlist-data/data.db"
DEFAULT_SEARCH_LIMIT = 100
DEFAULT_UPSTREAM_SEARCH_LIMIT = 100
DEFAULT_REQUIRED_INDEXER_SEARCH_LIMIT = 1000
DEFAULT_ANIME_INDEXER_SEARCH_LIMIT = 100
DEFAULT_PRIMARY_INDEXER_TIMEOUT_SECONDS = 12
DEFAULT_OPTIONAL_INDEXER_TIMEOUT_SECONDS = 12
DEFAULT_PROWLARR_SEARCH_TIMEOUT_SECONDS = 4
SEARCH_PAGE_SIZE = 5
DEFAULT_TASK_LIST_LIMIT = 10
DEFAULT_TASK_LIST_PAGE_SIZE = 5
DEFAULT_TASK_LIST_FETCH_LIMIT = 100
DEFAULT_OPENLIST_PRE_SCAN_CLEAN_MAX_BYTES = 20 * 1024 * 1024
FINAL_TASK_STATUS_NAMES = {"success", "failed", "cancelled"}
ACTIVE_115_STATUS_NAMES = {"submitted", "allocating", "downloading"}
ACTIVE_115_FAST_POLL_WINDOW_SECONDS = 20
ACTIVE_115_FAST_POLL_INTERVAL_SECONDS = 2
ACTIVE_115_SLOW_AFTER_POLLS = 10
ACTIVE_115_SLOW_POLL_INTERVAL_SECONDS = 600
ACTIVE_115_TIMEOUT_SECONDS = 7200
CATEGORY_LABELS = {"movie": "电影库", "tv": "剧集库", "anime": "动漫库", "adult": "成人库", "other": "其他库"}
ANIME_QUERY_HINT_PATTERN = re.compile(
    r"(anime|bangumi|mikan|nyaa|acg|动漫|動畫|动画|番剧|番劇|新番|日漫|"
    r"鬼灭|鬼滅|葬送|芙莉莲|芙莉蓮|海贼|海賊|火影|柯南|进击|進擊|咒术|咒術|"
    r"电锯|電鋸|间谍过家家|間諜家家酒|孤独摇滚|孤獨搖滾|我推|药屋|藥屋|"
    r"高达|高達|宝可梦|寶可夢|名侦探|名偵探|灌篮|灌籃|排球|无职|無職|"
    r"刀剑神域|刀劍神域|fate|re0|re:0|[ぁ-んァ-ン])",
    re.IGNORECASE,
)
CONTENT_PROFILE_LABELS = {
    "adult": "成人",
    "movie": "电影",
    "tv": "剧集",
    "anime": "动漫",
    "other": "其他",
}
DEFAULT_SEARCH_CATEGORY = "movie"
SEARCH_PROFILE_GENERAL = "general"
SEARCH_PROFILE_ADULT = "adult"
SEARCH_PROFILE_ANIME = "anime"
SEARCH_PROFILE_CATEGORIES = {
    SEARCH_PROFILE_GENERAL: (2000, 5000),
    SEARCH_PROFILE_ADULT: (6000,),
    SEARCH_PROFILE_ANIME: (2000, 5000),
}
SEARCH_PROFILE_TAG_LABELS = {
    SEARCH_PROFILE_GENERAL: ("media-general", "general"),
    SEARCH_PROFILE_ADULT: ("media-adult", "adult"),
    SEARCH_PROFILE_ANIME: ("media-anime", "anime"),
}
START_TEXT = "直接发送关键词、番号或磁链即可搜索/入库；/help 查看功能；/tasks 查看最近任务；/version 查看版本"
HELP_TEXT = """直接发送关键词、番号或磁链即可。

常用入口：
/tasks 查看最近任务
/status <info_hash> 查询任务状态
/migrate <关键词> 迁移已有媒体到其他库
/dedupe_refresh 刷新已入库记录（需二次确认）
/version 查看当前版本

搜索结果里选择资源后，再选择入电影、剧集、动漫、成人或其他库。"""
DEDUPE_REFRESH_WARNING_TEXT = """刷新已入库记录？

这个操作会主动刷新 OpenList 目录并重建 Bot 的重复判断基线，可能增加网盘侧请求量，资源多时也会比较慢。

不会删除文件，也不会提交新的离线任务。确认今天确实需要更新重复判断后再执行。"""
SUBTITLE_EXTENSIONS = {".ass", ".idx", ".srt", ".ssa", ".sub", ".vtt"}
VIDEO_EXTENSIONS = {
    ".avi",
    ".flv",
    ".m2ts",
    ".m4v",
    ".mkv",
    ".mov",
    ".mp4",
    ".mpeg",
    ".mpg",
    ".rmvb",
    ".ts",
    ".vob",
    ".webm",
    ".wmv",
}
TYPING_ACTION_INTERVAL_SECONDS = 4


@dataclass
class BotConfig:
    token: str
    allowed_user_ids: set
    state_db_path: str = DEFAULT_STATE_DB
    search_limit: int = DEFAULT_SEARCH_LIMIT
    openlist_db: str = DEFAULT_OPENLIST_DB
    openlist_url: str = DEFAULT_OPENLIST_URL
    prowlarr_url: str = DEFAULT_PROWLARR_URL
    prowlarr_config: str = DEFAULT_PROWLARR_CONFIG
    prowlarr_search_timeout_seconds: int = DEFAULT_PROWLARR_SEARCH_TIMEOUT_SECONDS
    telegram_timeout: int = 90
    msg_base_url: str = DEFAULT_MSG_BASE_URL
    msg_database_dsn: str = DEFAULT_MSG_DATABASE_DSN
    msg_admin_user: str = ""
    msg_admin_password: str = ""
    msg_enabled: bool = False
    msg_sync_poll_seconds: int = 60
    msg_sync_poll_interval_seconds: int = 5
    openlist_pre_scan_clean_enabled: bool = True
    openlist_pre_scan_clean_max_bytes: int = DEFAULT_OPENLIST_PRE_SCAN_CLEAN_MAX_BYTES
    openlist_adult_code_format_enabled: bool = True
    sync_recovery_interval_seconds: int = 60

    @classmethod
    def from_env(cls, env=None):
        env = env if env is not None else os.environ
        token = (env.get("TG_BOT_TOKEN") or "").strip()
        if not token:
            raise RuntimeError("TG_BOT_TOKEN missing")

        raw_allowed = (env.get("TG_ALLOWED_USER_IDS") or "").strip()
        if not raw_allowed:
            raise RuntimeError("TG_ALLOWED_USER_IDS missing")

        allowed = set()
        for item in raw_allowed.split(","):
            value = item.strip()
            if value:
                allowed.add(int(value))
        if not allowed:
            raise RuntimeError("TG_ALLOWED_USER_IDS missing")

        msg_admin_user = (env.get("MSG_ADMIN_USER") or "").strip()
        msg_admin_password = env.get("MSG_ADMIN_PASSWORD") or ""

        return cls(
            token=token,
            allowed_user_ids=allowed,
            state_db_path=env.get("BOT_STATE_DB", DEFAULT_STATE_DB),
            search_limit=int(env.get("BOT_SEARCH_LIMIT", DEFAULT_SEARCH_LIMIT)),
            openlist_db=env.get("OPENLIST_DB", DEFAULT_OPENLIST_DB),
            openlist_url=env.get("OPENLIST_URL", DEFAULT_OPENLIST_URL),
            prowlarr_url=env.get("PROWLARR_URL", DEFAULT_PROWLARR_URL),
            prowlarr_config=env.get("PROWLARR_CONFIG", DEFAULT_PROWLARR_CONFIG),
            prowlarr_search_timeout_seconds=int(
                env.get("PROWLARR_SEARCH_TIMEOUT_SECONDS", str(DEFAULT_PROWLARR_SEARCH_TIMEOUT_SECONDS))
            ),
            telegram_timeout=int(env.get("TG_API_TIMEOUT", "90")),
            msg_base_url=env.get("MSG_BASE_URL", DEFAULT_MSG_BASE_URL),
            msg_database_dsn=env.get("MSG_DATABASE_DSN", DEFAULT_MSG_DATABASE_DSN),
            msg_admin_user=msg_admin_user,
            msg_admin_password=msg_admin_password,
            msg_enabled=parse_bool(env.get("MSG_ENABLED"), bool(msg_admin_user and msg_admin_password)),
            msg_sync_poll_seconds=int(env.get("MSG_SYNC_POLL_SECONDS", "60")),
            msg_sync_poll_interval_seconds=int(env.get("MSG_SYNC_POLL_INTERVAL_SECONDS", "5")),
            openlist_pre_scan_clean_enabled=parse_bool(env.get("OPENLIST_PRE_SCAN_CLEAN_ENABLED"), True),
            openlist_pre_scan_clean_max_bytes=int(
                env.get("OPENLIST_PRE_SCAN_CLEAN_MAX_BYTES", str(DEFAULT_OPENLIST_PRE_SCAN_CLEAN_MAX_BYTES))
            ),
            openlist_adult_code_format_enabled=parse_bool(env.get("OPENLIST_ADULT_CODE_FORMAT_ENABLED"), True),
            sync_recovery_interval_seconds=int(env.get("BOT_SYNC_RECOVERY_INTERVAL_SECONDS", "60")),
        )


class CandidateStore:
    def __init__(self, db_path):
        self.db_path = str(db_path)
        parent = Path(self.db_path).parent
        parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    def _connect(self):
        return sqlite3.connect(self.db_path)

    def _init_schema(self):
        conn = self._connect()
        try:
            conn.execute(
                """
                create table if not exists candidates (
                    id integer primary key autoincrement,
                    user_id integer not null,
                    chat_id integer not null,
                    category text not null,
                    query text not null,
                    candidate_json text not null,
                    created_at integer not null
                )
                """
            )
            conn.execute(
                """
                create table if not exists offline_tasks (
                    info_hash text primary key,
                    user_id integer not null,
                    chat_id integer not null,
                    category text not null,
                    title text not null,
                    task_json text not null,
                    created_at integer not null,
                    updated_at integer not null
                )
                """
            )
            conn.execute(
                """
                create table if not exists search_sessions (
                    id integer primary key autoincrement,
                    user_id integer not null,
                    chat_id integer not null,
                    category text not null,
                    query text not null,
                    candidate_ids_json text not null,
                    created_at integer not null
                )
                """
            )
            conn.execute(
                """
                create table if not exists migration_candidates (
                    id integer primary key autoincrement,
                    user_id integer not null,
                    chat_id integer not null,
                    query text not null,
                    candidate_json text not null,
                    created_at integer not null
                )
                """
            )
            conn.execute(
                """
                create table if not exists dedupe_index (
                    id integer primary key autoincrement,
                    category text not null,
                    source text not null,
                    identity_type text not null,
                    identity_value text not null,
                    title text,
                    path text,
                    metadata_json text,
                    created_at integer not null,
                    updated_at integer not null,
                    unique(category, source, identity_type, identity_value)
                )
                """
            )
            conn.execute(
                """
                create index if not exists idx_dedupe_index_lookup
                on dedupe_index(category, identity_type, identity_value)
                """
            )
            conn.commit()
        finally:
            conn.close()

    def save_candidate(self, user_id, chat_id, category, query, candidate):
        payload = json.dumps(candidate, ensure_ascii=False, sort_keys=True)
        conn = self._connect()
        try:
            cursor = conn.execute(
                """
                insert into candidates (user_id, chat_id, category, query, candidate_json, created_at)
                values (?, ?, ?, ?, ?, ?)
                """,
                (int(user_id), int(chat_id), category, query, payload, int(time.time())),
            )
            conn.commit()
            return cursor.lastrowid
        finally:
            conn.close()

    def load_candidate(self, candidate_id):
        conn = self._connect()
        try:
            row = conn.execute(
                "select user_id, chat_id, category, query, candidate_json from candidates where id = ?",
                (int(candidate_id),),
            ).fetchone()
        finally:
            conn.close()
        if row is None:
            raise RuntimeError("candidate not found: %s" % candidate_id)
        return {
            "user_id": row[0],
            "chat_id": row[1],
            "category": row[2],
            "query": row[3],
            "candidate": json.loads(row[4]),
        }

    def save_search_session(self, user_id, chat_id, category, query, candidate_ids):
        payload = json.dumps([int(candidate_id) for candidate_id in candidate_ids], sort_keys=True)
        conn = self._connect()
        try:
            cursor = conn.execute(
                """
                insert into search_sessions (user_id, chat_id, category, query, candidate_ids_json, created_at)
                values (?, ?, ?, ?, ?, ?)
                """,
                (int(user_id), int(chat_id), category, query, payload, int(time.time())),
            )
            conn.commit()
            return cursor.lastrowid
        finally:
            conn.close()

    def load_search_session(self, session_id):
        conn = self._connect()
        try:
            row = conn.execute(
                "select id, user_id, chat_id, category, query, candidate_ids_json from search_sessions where id = ?",
                (int(session_id),),
            ).fetchone()
        finally:
            conn.close()
        if row is None:
            raise RuntimeError("search session not found: %s" % session_id)
        return {
            "id": row[0],
            "user_id": row[1],
            "chat_id": row[2],
            "category": row[3],
            "query": row[4],
            "candidate_ids": json.loads(row[5]),
        }

    def find_search_session_by_candidate(self, candidate_id):
        target = int(candidate_id)
        conn = self._connect()
        try:
            rows = conn.execute(
                """
                select id, user_id, chat_id, category, query, candidate_ids_json
                from search_sessions
                order by created_at desc, id desc
                """
            ).fetchall()
        finally:
            conn.close()
        for row in rows:
            candidate_ids = [int(value) for value in json.loads(row[5])]
            if target in candidate_ids:
                return {
                    "id": row[0],
                    "user_id": row[1],
                    "chat_id": row[2],
                    "category": row[3],
                    "query": row[4],
                    "candidate_ids": candidate_ids,
                }
        raise RuntimeError("search session not found for candidate: %s" % candidate_id)

    def save_migration_candidate(self, user_id, chat_id, query, candidate):
        payload = json.dumps(candidate, ensure_ascii=False, sort_keys=True)
        conn = self._connect()
        try:
            cursor = conn.execute(
                """
                insert into migration_candidates (user_id, chat_id, query, candidate_json, created_at)
                values (?, ?, ?, ?, ?)
                """,
                (int(user_id), int(chat_id), query or "", payload, int(time.time())),
            )
            conn.commit()
            return cursor.lastrowid
        finally:
            conn.close()

    def load_migration_candidate(self, candidate_id):
        conn = self._connect()
        try:
            row = conn.execute(
                """
                select user_id, chat_id, query, candidate_json
                from migration_candidates
                where id = ?
                """,
                (int(candidate_id),),
            ).fetchone()
        finally:
            conn.close()
        if row is None:
            raise RuntimeError("migration candidate not found: %s" % candidate_id)
        return {
            "user_id": row[0],
            "chat_id": row[1],
            "query": row[2],
            "candidate": json.loads(row[3]),
        }

    def save_task(self, user_id, chat_id, category, title, task):
        info_hash = str((task or {}).get("info_hash") or "").strip()
        if not info_hash:
            raise ValueError("task info_hash must not be empty")
        now = int(time.time())
        payload = json.dumps(task, ensure_ascii=False, sort_keys=True)
        conn = self._connect()
        try:
            conn.execute(
                """
                insert into offline_tasks (
                    info_hash, user_id, chat_id, category, title, task_json, created_at, updated_at
                ) values (?, ?, ?, ?, ?, ?, ?, ?)
                on conflict(info_hash) do update set
                    user_id = excluded.user_id,
                    chat_id = excluded.chat_id,
                    category = excluded.category,
                    title = excluded.title,
                    task_json = excluded.task_json,
                    updated_at = excluded.updated_at
                """,
                (info_hash, int(user_id), int(chat_id), category, title or "", payload, now, now),
            )
            conn.commit()
        finally:
            conn.close()

    def load_task(self, info_hash):
        conn = self._connect()
        try:
            row = conn.execute(
                """
                select info_hash, user_id, chat_id, category, title, task_json, created_at, updated_at
                from offline_tasks
                where lower(info_hash) = lower(?)
                """,
                (str(info_hash or "").strip(),),
            ).fetchone()
        finally:
            conn.close()
        if row is None:
            raise RuntimeError("offline task not found: %s" % info_hash)
        return task_record_from_row(row)

    def list_tasks(self, user_id, limit=DEFAULT_TASK_LIST_LIMIT):
        conn = self._connect()
        try:
            rows = conn.execute(
                """
                select info_hash, user_id, chat_id, category, title, task_json, created_at, updated_at
                from offline_tasks
                where user_id = ?
                order by updated_at desc, created_at desc
                limit ?
                """,
                (int(user_id), int(limit)),
            ).fetchall()
        finally:
            conn.close()
        return [task_record_from_row(row) for row in rows]

    def list_all_tasks(self, limit=500):
        conn = self._connect()
        try:
            rows = conn.execute(
                """
                select info_hash, user_id, chat_id, category, title, task_json, created_at, updated_at
                from offline_tasks
                order by updated_at desc, created_at desc
                limit ?
                """,
                (int(limit),),
            ).fetchall()
        finally:
            conn.close()
        return [task_record_from_row(row) for row in rows]

    def replace_dedupe_entries(self, source, entries):
        source = str(source or "").strip()
        if not source:
            raise ValueError("dedupe source must not be empty")
        normalized_entries = [normalize_dedupe_entry(entry, default_source=source) for entry in entries or []]
        now = int(time.time())
        conn = self._connect()
        try:
            conn.execute("delete from dedupe_index where source = ?", (source,))
            for entry in normalized_entries:
                conn.execute(
                    """
                    insert into dedupe_index (
                        category, source, identity_type, identity_value, title, path, metadata_json, created_at, updated_at
                    ) values (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    on conflict(category, source, identity_type, identity_value) do update set
                        title = excluded.title,
                        path = excluded.path,
                        metadata_json = excluded.metadata_json,
                        updated_at = excluded.updated_at
                    """,
                    (
                        entry["category"],
                        entry["source"],
                        entry["identity_type"],
                        entry["identity_value"],
                        entry.get("title"),
                        entry.get("path"),
                        json.dumps(entry.get("metadata") or {}, ensure_ascii=False, sort_keys=True),
                        now,
                        now,
                    ),
                )
            conn.commit()
        finally:
            conn.close()
        return len(normalized_entries)

    def migrate_dedupe_entries(self, source_path, target_path, source_category, target_category, source="openlist"):
        source = str(source or "").strip()
        source_path = normalize_openlist_path(source_path)
        target_path = normalize_openlist_path(target_path)
        source_category = str(source_category or "").strip()
        target_category = str(target_category or "").strip()
        if not source or not source_path or not target_path:
            raise ValueError("dedupe migration paths must not be empty")
        if source_category not in CATEGORY_LABELS or target_category not in CATEGORY_LABELS:
            raise ValueError("invalid dedupe migration category")

        now = int(time.time())
        conn = self._connect()
        try:
            rows = conn.execute(
                """
                select id, category, source, identity_type, identity_value, title, path, metadata_json, created_at
                from dedupe_index
                where source = ?
                  and category = ?
                  and (path = ? or path like ?)
                """,
                (source, source_category, source_path, source_path.rstrip("/") + "/%"),
            ).fetchall()
            for row in rows:
                new_path = replace_openlist_path_prefix(row[6], source_path, target_path)
                conn.execute("delete from dedupe_index where id = ?", (row[0],))
                conn.execute(
                    """
                    insert into dedupe_index (
                        category, source, identity_type, identity_value, title, path, metadata_json, created_at, updated_at
                    ) values (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    on conflict(category, source, identity_type, identity_value) do update set
                        title = excluded.title,
                        path = excluded.path,
                        metadata_json = excluded.metadata_json,
                        updated_at = excluded.updated_at
                    """,
                    (
                        target_category,
                        source,
                        row[3],
                        row[4],
                        row[5],
                        new_path,
                        row[7],
                        row[8],
                        now,
                    ),
                )
            conn.commit()
        finally:
            conn.close()
        return len(rows)

    def find_dedupe_entries(self, category, identities, limit=20):
        normalized = []
        seen = set()
        for identity in identities or []:
            identity_type = str((identity or {}).get("identity_type") or "").strip()
            identity_value = normalize_dedupe_identity_value(identity_type, (identity or {}).get("identity_value"))
            key = (identity_type, identity_value)
            if identity_type and identity_value and key not in seen:
                normalized.append(key)
                seen.add(key)
        if not normalized:
            return []

        rows = []
        conn = self._connect()
        try:
            for identity_type, identity_value in normalized:
                found = conn.execute(
                    """
                    select category, source, identity_type, identity_value, title, path, metadata_json, created_at, updated_at
                    from dedupe_index
                    where category = ? and identity_type = ? and identity_value = ?
                    order by updated_at desc, created_at desc
                    limit ?
                    """,
                    (category, identity_type, identity_value, int(limit)),
                ).fetchall()
                rows.extend(found)
                if len(rows) >= int(limit):
                    break
        finally:
            conn.close()
        return [dedupe_entry_from_row(row) for row in rows[: int(limit)]]

    def list_msg_sync_running_tasks(self, limit=50):
        conn = self._connect()
        try:
            rows = conn.execute(
                """
                select info_hash, user_id, chat_id, category, title, task_json, created_at, updated_at
                from offline_tasks
                order by updated_at asc, created_at asc
                limit ?
                """,
                (int(limit),),
            ).fetchall()
        finally:
            conn.close()
        records = [task_record_from_row(row) for row in rows]
        return [
            record
            for record in records
            if (record["task"] or {}).get("status_name") == "success"
            and task_sync_is_running(record["task"])
            and not task_msg_synced(record["task"])
        ]

    def list_active_115_tasks(self, limit=50):
        conn = self._connect()
        try:
            rows = conn.execute(
                """
                select info_hash, user_id, chat_id, category, title, task_json, created_at, updated_at
                from offline_tasks
                order by updated_at asc, created_at asc
                """
            ).fetchall()
        finally:
            conn.close()
        records = [task_record_from_row(row) for row in rows]
        active = [record for record in records if (record["task"] or {}).get("status_name") in ACTIVE_115_STATUS_NAMES]
        return active[: int(limit)]


class TelegramApi:
    def __init__(self, token, transport=None, timeout=30):
        self.base_url = "https://api.telegram.org/bot%s" % token
        self.transport = transport or TelegramTransport()
        self.timeout = timeout

    def get_updates(self, offset=None, timeout=30):
        payload = {"timeout": timeout}
        if offset is not None:
            payload["offset"] = offset
        return self._request("getUpdates", payload)

    def send_message(self, chat_id, text, reply_markup=None):
        payload = {"chat_id": chat_id, "text": text}
        if reply_markup is not None:
            payload["reply_markup"] = reply_markup
        return self._request("sendMessage", payload)

    def send_chat_action(self, chat_id, action="typing"):
        return self._request("sendChatAction", {"chat_id": chat_id, "action": action})

    def edit_message_text(self, chat_id, message_id, text, reply_markup=None):
        payload = {"chat_id": chat_id, "message_id": message_id, "text": text}
        if reply_markup is not None:
            payload["reply_markup"] = reply_markup
        return self._request("editMessageText", payload)

    def delete_message(self, chat_id, message_id):
        return self._request("deleteMessage", {"chat_id": chat_id, "message_id": message_id})

    def answer_callback_query(self, callback_query_id, text=None):
        payload = {"callback_query_id": callback_query_id}
        if text:
            payload["text"] = text
        return self._request("answerCallbackQuery", payload)

    def _request(self, method, payload):
        return self.transport.request(self.base_url + "/" + method, payload, timeout=self.timeout)


class TelegramTransport:
    def request(self, url, payload, timeout=None):
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        request = urllib.request.Request(
            url,
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                raw = response.read().decode("utf-8", "replace")
        except urllib.error.HTTPError as exc:
            raw = exc.read().decode("utf-8", "replace")
            try:
                data = json.loads(raw)
                description = data.get("description") or raw.strip()
            except (TypeError, ValueError):
                description = raw.strip() or str(exc)
            raise RuntimeError("Telegram API failed: %s" % description) from exc
        data = json.loads(raw)
        if not data.get("ok"):
            raise RuntimeError("Telegram API failed: %s" % data.get("description"))
        return data


class PipelineBotService:
    def __init__(self, config):
        self.config = config

    def search(self, query, category, limit=DEFAULT_SEARCH_LIMIT, profile=None):
        profile = profile or search_profile_for_query(category, query)
        api_key = ProwlarrConfig(self.config.prowlarr_config).load_api_key()
        prowlarr = ProwlarrClient(self.config.prowlarr_url, api_key, timeout=self.config.prowlarr_search_timeout_seconds)
        indexers = prowlarr.indexers()
        tags = safe_prowlarr_tags(prowlarr)
        candidates = search_profile_indexer_results(
            prowlarr,
            query,
            profile,
            max(int(limit), DEFAULT_UPSTREAM_SEARCH_LIMIT),
            indexers=indexers,
            tags=tags,
            timeout_seconds=self.config.prowlarr_search_timeout_seconds,
        )
        return ResourceSelector(indexer_priorities=indexer_priority_map(indexers)).select_ranked_limited(candidates, query=query, limit=limit)

    def search_adult(self, query, limit=DEFAULT_SEARCH_LIMIT):
        return self.search(query, "adult", limit=limit, profile=SEARCH_PROFILE_ADULT)

    def search_anime(self, query, limit=DEFAULT_SEARCH_LIMIT):
        return self.search(query, "movie", limit=limit, profile=SEARCH_PROFILE_ANIME)

    def search_migration_candidates(self, query, limit=20):
        return self._build_msg_db_client().search_migration_candidates(query, limit=limit)

    def submit(self, category, download_uri):
        download_uri = self._resolve_download_uri(download_uri)
        result = summarize_submit(
            self._call_115(category, lambda client: client.add_offline_urls([download_uri], category_to_folder_id(category)))
        )
        info_hash = first_info_hash(result)
        if info_hash:
            result["task_status"] = self._call_115(category, lambda client: find_task_by_info_hash(client, info_hash, max_pages=10))
        return result

    def _resolve_download_uri(self, download_uri):
        if not is_prowlarr_download_uri(download_uri):
            return download_uri
        api_key = ProwlarrConfig(self.config.prowlarr_config).load_api_key()
        return ProwlarrClient(self.config.prowlarr_url, api_key).resolve_download_uri(download_uri)

    def task_status(self, category, info_hash):
        return self._call_115(category, lambda client: find_task_by_info_hash(client, info_hash, max_pages=10))

    def task_statuses(self, category, info_hashes):
        return self._call_115(category, lambda client: find_tasks_by_info_hashes(client, info_hashes, max_pages=10))

    def cancel_task(self, category, info_hash):
        return self._call_115(category, lambda client: cancel_task_if_active(client, info_hash, max_pages=10))

    def migrate_media_candidate(self, candidate, target_category):
        target = build_migration_target(candidate, target_category)
        db_client = self._build_msg_db_client()
        db_client.validate_migration_target_available(candidate, target_category)
        db_client.validate_migration_source_ready(candidate)

        openlist_client = OpenListClient(self.config.openlist_url, OpenListTokenProvider().load_token())
        source_path = candidate["source_openlist_path"]
        if not openlist_child_exists(openlist_client, source_path):
            raise RuntimeError("OpenList source not found: %s" % source_path)
        if openlist_child_exists(openlist_client, target["target_openlist_path"]):
            raise RuntimeError("OpenList target already exists: %s" % target["target_openlist_path"])

        source_dir = posixpath.dirname(source_path.rstrip("/")) or "/"
        source_name = posixpath.basename(source_path.rstrip("/"))
        target_root = target["target_root_openlist_path"]
        openlist_client.move_names(source_dir, target_root, [source_name])
        openlist_client.list_path(source_dir, refresh=True)
        openlist_client.list_path(target_root, refresh=True)

        result = db_client.migrate_media_group(candidate, target_category)
        result["openlist_moved"] = True
        try:
            result["dedupe_index_count"] = CandidateStore(self.config.state_db_path).migrate_dedupe_entries(
                candidate["source_openlist_path"],
                target["target_openlist_path"],
                candidate.get("category") or "",
                target_category,
            )
        except (RuntimeError, ValueError, sqlite3.Error) as exc:
            result["dedupe_index_error"] = str(exc)
        return result

    def check_duplicate(self, category, query, candidate):
        if not self.config.msg_enabled:
            return None
        title = candidate.get("title") or query
        queries = media_search_queries(title, {"file_name": title})
        if query and query not in queries:
            queries.append(query)
        if not queries:
            return None
        client = self._build_msg_client()
        root = category_to_msg_library_root(category)
        media = find_matching_media(
            extract_media_items(client.search_media(queries[0], limit=20)),
            queries,
            library_id=root["library_id"],
        )
        if media is None:
            media = find_matching_media(
                extract_media_items(client.list_library_media(root["library_id"], page=1, page_size=200, group_versions=0)),
                queries,
                library_id=root["library_id"],
            )
        if media is None:
            return None
        codes = extract_codes(" ".join(queries))
        media_codes = extract_codes(media_haystack(media))
        return {
            "level": "strong" if category == "adult" and codes and codes.intersection(media_codes) else "weak",
            "reason": "mediastation_code" if category == "adult" and codes and codes.intersection(media_codes) else "mediastation_title",
            "source": "MediaStationGo",
            "title": media_display_title(media),
            "media_id": extract_media_id(media),
            "can_force": not (category == "adult" and codes and codes.intersection(media_codes)),
        }

    def collect_openlist_dedupe_entries(self, refresh=True):
        client = OpenListClient(self.config.openlist_url, OpenListTokenProvider().load_token())
        entries = []
        for category in CATEGORY_LABELS:
            path = category_to_openlist_path(category)
            if refresh:
                client.list_path(path, refresh=True)
            entries.extend(openlist_dedupe_entries(client, category, path))
        return unique_dedupe_entries(entries)

    def sync_completed_task(self, category, title, task, progress_callback=None):
        out = dict(task or {})
        if out.get("status_name") != "success":
            return out
        if task_msg_synced(out):
            return out
        if not self.config.msg_enabled:
            return out

        def capture_progress(progress):
            out.update(progress)
            if progress_callback:
                progress_callback(dict(progress))

        try:
            result = self._sync_mediastation(category, title, out, progress_callback=capture_progress)
        except (RuntimeError, ValueError) as exc:
            mark_current_sync_stage_failed(out, str(exc))
            out["msg_sync_status"] = "failed"
            out["msg_error"] = str(exc)
            out["msg_synced_at"] = int(time.time())
            return out
        out.update(result)
        return out

    def _call_115(self, category, callback):
        client = self._build_115_client(category)
        try:
            result = callback(client)
        except RuntimeError as exc:
            if not access_token_invalid_error(exc):
                raise
            return callback(self._build_115_client(category, refresh=True))
        if access_token_invalid_response(result):
            return callback(self._build_115_client(category, refresh=True))
        return result

    def _build_115_client(self, category, refresh=False):
        OpenListClient(self.config.openlist_url, OpenListTokenProvider().load_token()).list_path(category_to_openlist_path(category), refresh=refresh)
        token = OpenListTokenStore(self.config.openlist_db).load_access_token()
        return Client115(token.access_token)

    def _sync_mediastation(self, category, title, task, progress_callback=None):
        progress = dict(task or {})
        openlist_client = None
        msg_client = None

        def apply_progress(updates):
            progress.update(updates)
            progress.setdefault("msg_sync_status", "running")

        def emit(updates):
            apply_progress(updates)
            if progress_callback:
                progress_callback(dict(progress))

        def get_openlist_client():
            nonlocal openlist_client
            if openlist_client is None:
                openlist_client = self._refresh_openlist_for_msg(category)
            return openlist_client

        def get_msg_client():
            nonlocal msg_client
            if msg_client is None:
                msg_client = self._build_msg_client()
            return msg_client

        clean_result = prefixed_task_fields(progress, "openlist_clean_")
        if self.config.openlist_pre_scan_clean_enabled:
            if not stage_is_complete(progress.get("openlist_clean_status")):
                emit({"openlist_clean_status": "running", "openlist_clean_error": None})
                clean_result = self._clean_openlist_before_msg(get_openlist_client(), category, title, progress)
                if clean_result.get("openlist_clean_status") != "skipped":
                    emit(clean_result)
                else:
                    apply_progress(clean_result)
            else:
                apply_progress(clean_result)
        else:
            clean_result = {"openlist_clean_status": "skipped", "openlist_clean_reason": "disabled"}
            apply_progress(clean_result)

        format_result = prefixed_task_fields(progress, "openlist_adult_")
        if category == "adult" and self.config.openlist_adult_code_format_enabled:
            if not stage_is_complete(progress.get("openlist_adult_format_status")):
                emit({"openlist_adult_format_status": "running", "openlist_adult_format_error": None})
                format_result = self._format_openlist_adult_before_msg(get_openlist_client(), category, title, progress)
                if format_result.get("openlist_adult_format_status") != "skipped":
                    emit(format_result)
                else:
                    apply_progress(format_result)
            else:
                apply_progress(format_result)
        else:
            format_result = (
                {"openlist_adult_format_status": "skipped", "openlist_adult_format_reason": "disabled"}
                if category == "adult"
                else {}
            )
            apply_progress(format_result)

        queries = media_search_queries(title, progress)
        if format_result.get("openlist_adult_code"):
            queries = [format_result["openlist_adult_code"]] + queries

        media_id = progress.get("msg_media_id")
        media_title = progress.get("msg_media_title")
        root = category_to_msg_library_root(category)

        if progress.get("msg_scan_status") != "success" or not media_id:
            get_openlist_client()
            client = get_msg_client()
            emit({"msg_scan_status": "running", "msg_error": None})
            client.scan_root(root["library_id"], root["root_id"])
            media = self._wait_for_msg_media(client, root["library_id"], queries)
            media_id = extract_media_id(media)
            if not media_id:
                raise RuntimeError("MediaStationGo media id missing after scan")
            media_title = media_display_title(media)
            emit({"msg_scan_status": "success", "msg_media_id": media_id, "msg_media_title": media_title})

        if progress.get("msg_scrape_status") != "success":
            emit({"msg_scrape_status": "running", "msg_media_id": media_id, "msg_media_title": media_title})
            get_msg_client().scrape_media(media_id)
            emit({"msg_scrape_status": "success", "msg_media_id": media_id, "msg_media_title": media_title})
        artwork_result = prefixed_task_fields(progress, "msg_artwork_repair_")
        if root.get("media_type") == "adult":
            if not stage_is_complete(progress.get("msg_artwork_repair_status")):
                emit({"msg_artwork_repair_status": "running", "msg_artwork_repair_error": None})
                artwork_result = self._repair_msg_adult_artwork(get_msg_client(), media_id)
                if artwork_result.get("msg_artwork_repair_status") == "skipped":
                    apply_progress(artwork_result)
                else:
                    emit(artwork_result)
            else:
                apply_progress(artwork_result)
        msg_library_id = (root or {}).get("library_id") or progress.get("msg_library_id")
        msg_root_id = (root or {}).get("root_id") or progress.get("msg_root_id")
        return {
            "msg_sync_status": "success",
            "msg_scan_status": "success",
            "msg_scrape_status": "success",
            "msg_library_id": msg_library_id,
            "msg_root_id": msg_root_id,
            "msg_media_id": media_id,
            "msg_media_title": media_title,
            "msg_error": None,
            "msg_synced_at": int(time.time()),
            **clean_result,
            **format_result,
            **artwork_result,
        }

    def _repair_msg_adult_artwork(self, client, media_id):
        result = client.repair_adult_artwork(media_id)
        if not isinstance(result, dict):
            raise RuntimeError("MediaStationGo adult artwork repair returned invalid response")
        status = result.get("status")
        if status not in ("success", "skipped"):
            raise RuntimeError("MediaStationGo adult artwork repair returned invalid status: %s" % (status or "-"))
        return {
            "msg_artwork_repair_status": status,
            "msg_artwork_repair_updated": int(result.get("updated") or 0),
            "msg_artwork_repair_reason": result.get("reason"),
            "msg_artwork_repair_fields": ",".join(result.get("fields") or []),
            "msg_artwork_repair_error": None,
        }

    def _wait_for_msg_media(self, client, library_id, queries):
        deadline = time.monotonic() + max(0, int(self.config.msg_sync_poll_seconds))
        interval = max(1, int(self.config.msg_sync_poll_interval_seconds))
        while True:
            for query in queries:
                items = extract_media_items(client.search_media(query, limit=20))
                media = find_matching_media(items, queries, library_id=library_id)
                if media:
                    return media

            items = extract_media_items(client.list_library_media(library_id, page=1, page_size=200, group_versions=0))
            media = find_matching_media(items, queries, library_id=library_id)
            if media:
                return media

            if time.monotonic() >= deadline:
                break
            time.sleep(interval)
        raise RuntimeError("MediaStationGo media not found after root scan: %s" % (queries[0] if queries else "-"))

    def _refresh_openlist_for_msg(self, category):
        path = category_to_openlist_path(category)
        client = OpenListClient(self.config.openlist_url, OpenListTokenProvider().load_token())
        client.list_path(path, refresh=True)
        return client

    def _clean_openlist_before_msg(self, client, category, title, task):
        if not self.config.openlist_pre_scan_clean_enabled:
            return {"openlist_clean_status": "skipped", "openlist_clean_reason": "disabled"}
        try:
            return clean_openlist_task_media(
                client,
                category_to_openlist_path(category),
                media_search_queries(title, task),
                task=task,
                max_bytes=self.config.openlist_pre_scan_clean_max_bytes,
            )
        except (RuntimeError, ValueError) as exc:
            return {
                "openlist_clean_status": "failed",
                "openlist_clean_error": str(exc),
                "openlist_cleaned_count": 0,
                "openlist_cleaned_bytes": 0,
                "openlist_cleaned_at": int(time.time()),
            }

    def _format_openlist_adult_before_msg(self, client, category, title, task):
        if category != "adult":
            return {}
        if not self.config.openlist_adult_code_format_enabled:
            return {"openlist_adult_format_status": "skipped", "openlist_adult_format_reason": "disabled"}
        try:
            return format_openlist_adult_code(
                client,
                category_to_openlist_path(category),
                media_search_queries(title, task),
                task=task,
            )
        except (RuntimeError, ValueError) as exc:
            return {
                "openlist_adult_format_status": "failed",
                "openlist_adult_format_error": str(exc),
                "openlist_adult_formatted_at": int(time.time()),
            }

    def _build_msg_client(self):
        if not self.config.msg_admin_user or not self.config.msg_admin_password:
            raise RuntimeError("MediaStationGo credentials missing")
        return MediaStationClient(
            self.config.msg_base_url,
            self.config.msg_admin_user,
            self.config.msg_admin_password,
        )

    def _build_msg_db_client(self):
        return MediaStationDbClient(self.config.msg_database_dsn)


class TelegramBot:
    def __init__(self, config, telegram, store, service):
        self.config = config
        self.telegram = telegram
        self.store = store
        self.service = service
        self._recovery_thread = None

    def handle_update(self, update):
        if update.get("message"):
            self._handle_message(update["message"])
        elif update.get("callback_query"):
            self._handle_callback(update["callback_query"])

    def _handle_message(self, message):
        chat_id = message["chat"]["id"]
        user_id = message.get("from", {}).get("id")
        if not self._is_allowed(user_id):
            self.telegram.send_message(chat_id, "未授权用户")
            return

        text = (message.get("text") or "").strip()
        if not text:
            self.telegram.send_message(chat_id, START_TEXT)
            return

        command, argument = split_command(text)
        if command == "/start":
            self.telegram.send_message(chat_id, START_TEXT)
            return
        if command == "/help":
            self.telegram.send_message(chat_id, HELP_TEXT)
            return
        if command == "/tasks":
            self._send_task_list(chat_id, user_id)
            return
        if command == "/status":
            with self._typing_action(chat_id):
                self._handle_status_command(chat_id, user_id, argument)
            return
        if command == "/migrate":
            with self._typing_action(chat_id):
                self._handle_migrate_command(chat_id, user_id, argument)
            return
        if command == "/dedupe_refresh":
            self._handle_dedupe_refresh_command(chat_id)
            return
        if command == "/version":
            self.telegram.send_message(chat_id, format_version_info())
            return
        if text.startswith("/"):
            self.telegram.send_message(chat_id, "这个命令不再作为搜索入口。直接发送关键词、番号或磁链即可；/help 查看功能。")
            return

        direct_candidate = magnet_candidate_from_text(text)
        if direct_candidate:
            candidate_id = self.store.save_candidate(
                user_id,
                chat_id,
                DEFAULT_SEARCH_CATEGORY,
                direct_candidate["title"],
                direct_candidate,
            )
            self.telegram.send_message(
                chat_id,
                format_library_choice_message(direct_candidate),
                reply_markup=library_choice_reply_markup(candidate_id),
            )
            return

        query = text
        category = "adult" if is_strong_adult_code_query(query) else DEFAULT_SEARCH_CATEGORY
        if not query:
            self.telegram.send_message(chat_id, "请输入影片名")
            return

        try:
            with self._typing_action(chat_id):
                candidates = self.service.search(query, category, limit=self.config.search_limit)
        except RuntimeError as exc:
            if "no acceptable resource" in str(exc):
                self._send_empty_search_page(user_id, chat_id, category, query)
                return
            self.telegram.send_message(chat_id, "搜索失败：%s" % exc)
            return
        except Exception as exc:
            self.telegram.send_message(chat_id, "搜索失败：%s" % exc)
            return
        if not candidates:
            self._send_empty_search_page(user_id, chat_id, category, query)
            return

        candidate_ids = []
        for candidate in candidates:
            candidate_id = self.store.save_candidate(user_id, chat_id, category, query, candidate)
            candidate_ids.append(candidate_id)
        session_id = self.store.save_search_session(user_id, chat_id, category, query, candidate_ids)
        text, reply_markup = self._render_search_page(session_id, page=0)

        self.telegram.send_message(chat_id, text, reply_markup=reply_markup)

    def _send_empty_search_page(self, user_id, chat_id, category, query):
        session_id = self.store.save_search_session(user_id, chat_id, category, query, [])
        text, reply_markup = self._render_search_page(session_id, page=0)
        self.telegram.send_message(chat_id, text, reply_markup=reply_markup)

    def _handle_callback(self, callback):
        user_id = callback.get("from", {}).get("id")
        message = callback.get("message") or {}
        chat_id = message.get("chat", {}).get("id")
        message_id = message.get("message_id")
        callback_id = callback.get("id")
        if not self._is_allowed(user_id):
            self.telegram.answer_callback_query(callback_id, "未授权用户")
            return

        action, value = parse_callback_data(callback.get("data") or "")
        if action == "page":
            session_id, page = value
            self._handle_search_page_callback(user_id, chat_id, message_id, callback_id, session_id, page)
            return
        if action == "close_search":
            self._handle_close_search_callback(user_id, chat_id, message_id, callback_id, value)
            return
        if action == "adult_search":
            self._handle_adult_search_callback(user_id, chat_id, callback_id, value)
            return
        if action == "anime_search":
            self._handle_anime_search_callback(user_id, chat_id, callback_id, value)
            return
        if action == "tasks_page":
            self._handle_task_page_callback(user_id, chat_id, message_id, callback_id, value)
            return
        if action == "choose":
            self._handle_choose_library_callback(user_id, chat_id, message_id, callback_id, value)
            return
        if action == "close_choice":
            self._handle_close_choice_callback(user_id, chat_id, message_id, callback_id, value)
            return
        if action == "back_search":
            self._handle_back_to_search_callback(user_id, chat_id, message_id, callback_id, value)
            return
        if action == "profile":
            content_profile, candidate_id = value
            self._handle_profile_submit_callback(user_id, chat_id, message_id, callback_id, content_profile, candidate_id)
            return
        if action == "submit":
            category, candidate_id, content_profile = value
            self._handle_submit_callback(user_id, chat_id, message_id, callback_id, category, candidate_id, content_profile=content_profile)
            return
        if action == "force_submit":
            category, candidate_id, content_profile = value
            self._handle_force_submit_callback(user_id, chat_id, message_id, callback_id, category, candidate_id, content_profile=content_profile)
            return
        if action == "status":
            self._handle_status_callback(user_id, chat_id, message_id, callback_id, value)
            return
        if action == "retry_msg":
            self._handle_retry_msg_callback(user_id, chat_id, message_id, callback_id, value)
            return
        if action == "cancel":
            self._handle_cancel_callback(user_id, chat_id, message_id, callback_id, value)
            return
        if action == "migrate_select":
            self._handle_migrate_select_callback(user_id, chat_id, message_id, callback_id, value)
            return
        if action == "migrate_to":
            target_category, candidate_id = value
            self._handle_migrate_to_callback(user_id, chat_id, message_id, callback_id, target_category, candidate_id)
            return
        if action == "migrate_confirm":
            target_category, candidate_id = value
            self._handle_migrate_confirm_callback(user_id, chat_id, message_id, callback_id, target_category, candidate_id)
            return
        if action == "migrate_cancel":
            self._handle_migrate_cancel_callback(user_id, chat_id, message_id, callback_id, value)
            return
        if action == "dedupe_refresh_confirm":
            self._handle_dedupe_refresh_confirm_callback(chat_id, message_id, callback_id)
            return
        if action == "dedupe_refresh_cancel":
            self._handle_dedupe_refresh_cancel_callback(chat_id, message_id, callback_id)
            return
        else:
            self.telegram.answer_callback_query(callback_id, "不支持的操作")
            return

    def _handle_search_page_callback(self, user_id, chat_id, message_id, callback_id, session_id, page):
        try:
            session = self.store.load_search_session(session_id)
        except RuntimeError:
            self.telegram.answer_callback_query(callback_id, "搜索结果不存在")
            return
        if session["user_id"] != user_id:
            self.telegram.answer_callback_query(callback_id, "无权查看此搜索")
            return

        text, reply_markup = self._render_search_page(session_id, page)
        page_count = search_page_count(len(session["candidate_ids"]))
        safe_page = normalize_page(page, page_count)
        self.telegram.answer_callback_query(callback_id, "第 %s/%s 页" % (safe_page + 1, page_count))
        self._update_callback_message(
            chat_id,
            message_id,
            text,
            reply_markup=reply_markup,
            fallback_chat_id=session["chat_id"],
        )

    def _handle_close_search_callback(self, user_id, chat_id, message_id, callback_id, session_id):
        try:
            session = self.store.load_search_session(session_id)
        except RuntimeError:
            self.telegram.answer_callback_query(callback_id, "搜索结果不存在")
            return
        if session["user_id"] != user_id:
            self.telegram.answer_callback_query(callback_id, "无权查看此搜索")
            return
        self.telegram.answer_callback_query(callback_id, "已关闭搜索结果")
        self._delete_callback_message(chat_id, message_id)

    def _handle_adult_search_callback(self, user_id, chat_id, callback_id, session_id):
        try:
            session = self.store.load_search_session(session_id)
        except RuntimeError:
            self.telegram.answer_callback_query(callback_id, "搜索结果不存在")
            return
        if session["user_id"] != user_id:
            self.telegram.answer_callback_query(callback_id, "无权查看此搜索")
            return
        if session["category"] == "adult" or is_strong_adult_code_query(session["query"]):
            self.telegram.answer_callback_query(callback_id, "当前已是成人源结果")
            return

        self.telegram.answer_callback_query(callback_id, "正在补查成人源")
        try:
            with self._typing_action(session["chat_id"]):
                candidates = self.service.search_adult(session["query"], limit=self.config.search_limit)
        except RuntimeError as exc:
            if "no acceptable resource" in str(exc):
                self.telegram.send_message(session["chat_id"], "成人源未找到可用资源")
                return
            self.telegram.send_message(session["chat_id"], "成人源补查失败：%s" % exc)
            return
        except Exception as exc:
            self.telegram.send_message(session["chat_id"], "成人源补查失败：%s" % exc)
            return
        if not candidates:
            self.telegram.send_message(session["chat_id"], "成人源未找到可用资源")
            return

        candidate_ids = []
        for candidate in candidates:
            candidate_id = self.store.save_candidate(user_id, session["chat_id"], "adult", session["query"], candidate)
            candidate_ids.append(candidate_id)
        adult_session_id = self.store.save_search_session(user_id, session["chat_id"], "adult", session["query"], candidate_ids)
        text, reply_markup = self._render_search_page(adult_session_id, page=0)
        self.telegram.send_message(session["chat_id"], text, reply_markup=reply_markup)

    def _handle_anime_search_callback(self, user_id, chat_id, callback_id, session_id):
        try:
            session = self.store.load_search_session(session_id)
        except RuntimeError:
            self.telegram.answer_callback_query(callback_id, "搜索结果不存在")
            return
        if session["user_id"] != user_id:
            self.telegram.answer_callback_query(callback_id, "无权查看此搜索")
            return
        if session["category"] == "anime" or should_search_anime(DEFAULT_SEARCH_CATEGORY, session["query"]):
            self.telegram.answer_callback_query(callback_id, "当前已是动漫源结果")
            return

        self.telegram.answer_callback_query(callback_id, "正在补查动漫源")
        try:
            with self._typing_action(session["chat_id"]):
                candidates = self.service.search_anime(session["query"], limit=self.config.search_limit)
        except RuntimeError as exc:
            if "no acceptable resource" in str(exc):
                self.telegram.send_message(session["chat_id"], "动漫源未找到可用资源")
                return
            self.telegram.send_message(session["chat_id"], "动漫源补查失败：%s" % exc)
            return
        except Exception as exc:
            self.telegram.send_message(session["chat_id"], "动漫源补查失败：%s" % exc)
            return
        if not candidates:
            self.telegram.send_message(session["chat_id"], "动漫源未找到可用资源")
            return

        candidate_ids = []
        for candidate in candidates:
            candidate_id = self.store.save_candidate(user_id, session["chat_id"], "anime", session["query"], candidate)
            candidate_ids.append(candidate_id)
        anime_session_id = self.store.save_search_session(user_id, session["chat_id"], "anime", session["query"], candidate_ids)
        text, reply_markup = self._render_search_page(anime_session_id, page=0)
        self.telegram.send_message(session["chat_id"], text, reply_markup=reply_markup)

    def _handle_choose_library_callback(self, user_id, chat_id, message_id, callback_id, candidate_id):
        record = self.store.load_candidate(candidate_id)
        if record["user_id"] != user_id:
            self.telegram.answer_callback_query(callback_id, "无权操作此候选")
            return

        candidate = record["candidate"]
        self.telegram.answer_callback_query(callback_id, "请选择入库分类")
        self.telegram.send_message(
            chat_id or record["chat_id"],
            format_library_choice_message(candidate),
            reply_markup=library_choice_reply_markup(candidate_id, include_back=True),
        )

    def _handle_close_choice_callback(self, user_id, chat_id, message_id, callback_id, candidate_id):
        try:
            record = self.store.load_candidate(candidate_id)
        except RuntimeError:
            self.telegram.answer_callback_query(callback_id, "搜索结果不存在")
            return
        if record["user_id"] != user_id:
            self.telegram.answer_callback_query(callback_id, "无权查看此搜索")
            return
        self.telegram.answer_callback_query(callback_id, "返回结果")
        self._delete_callback_message(chat_id, message_id)

    def _handle_back_to_search_callback(self, user_id, chat_id, message_id, callback_id, candidate_id):
        try:
            record = self.store.load_candidate(candidate_id)
        except RuntimeError:
            self.telegram.answer_callback_query(callback_id, "搜索结果不存在")
            return
        if record["user_id"] != user_id:
            self.telegram.answer_callback_query(callback_id, "无权查看此搜索")
            return

        try:
            session = self.store.find_search_session_by_candidate(candidate_id)
        except RuntimeError:
            self.telegram.answer_callback_query(callback_id, "搜索结果不存在")
            return
        if session["user_id"] != user_id:
            self.telegram.answer_callback_query(callback_id, "无权查看此搜索")
            return

        candidate_ids = [int(value) for value in session["candidate_ids"]]
        page = candidate_ids.index(int(candidate_id)) // SEARCH_PAGE_SIZE
        text, reply_markup = self._render_search_page(session["id"], page)
        self.telegram.answer_callback_query(callback_id, "返回结果")
        self._update_callback_message(
            chat_id,
            message_id,
            text,
            reply_markup=reply_markup,
            fallback_chat_id=session["chat_id"],
        )

    def _handle_profile_submit_callback(self, user_id, chat_id, message_id, callback_id, content_profile, candidate_id):
        try:
            category = content_profile_to_category(content_profile)
        except ValueError:
            self.telegram.answer_callback_query(callback_id, "不支持的内容分类")
            return
        self._handle_submit_callback(
            user_id,
            chat_id,
            message_id,
            callback_id,
            category,
            candidate_id,
            content_profile=content_profile,
        )

    def _handle_submit_callback(self, user_id, chat_id, message_id, callback_id, category, candidate_id, content_profile=None):
        record = self.store.load_candidate(candidate_id)
        if record["user_id"] != user_id:
            self.telegram.answer_callback_query(callback_id, "无权操作此候选")
            return

        self._submit_candidate(user_id, chat_id, message_id, callback_id, category, candidate_id, record, force=False, content_profile=content_profile)

    def _handle_force_submit_callback(self, user_id, chat_id, message_id, callback_id, category, candidate_id, content_profile=None):
        record = self.store.load_candidate(candidate_id)
        if record["user_id"] != user_id:
            self.telegram.answer_callback_query(callback_id, "无权操作此候选")
            return
        self.telegram.answer_callback_query(callback_id, "已确认仍然入库")
        self._submit_candidate(
            user_id,
            chat_id,
            message_id,
            callback_id,
            category,
            candidate_id,
            record,
            force=True,
            answer=False,
            content_profile=content_profile,
        )

    def _submit_candidate(self, user_id, chat_id, message_id, callback_id, category, candidate_id, record, force=False, answer=True, content_profile=None):
        candidate = record["candidate"]
        content_profile = normalize_content_profile(category, content_profile)
        with self._typing_action(chat_id or record["chat_id"]):
            if not force:
                duplicate = self._find_duplicate_before_submit(category, record, candidate)
                if duplicate:
                    self.telegram.answer_callback_query(callback_id, "发现重复作品")
                    self._update_callback_message(
                        chat_id,
                        message_id,
                        format_duplicate_message(candidate, duplicate),
                        reply_markup=duplicate_reply_markup(duplicate, category, candidate_id, content_profile=content_profile),
                        fallback_chat_id=record["chat_id"],
                    )
                    return

            result = self.service.submit(category, candidate["download_uri"])
        self._save_tasks_from_submit(record, candidate, result, category, content_profile=content_profile)
        if answer:
            self.telegram.answer_callback_query(callback_id, "已提交 115 离线")
        self._delete_callback_message(chat_id, message_id)
        sent = self.telegram.send_message(
            chat_id or record["chat_id"],
            format_submit_message(candidate, result, category, content_profile=content_profile),
            reply_markup=submit_reply_markup(result),
        )
        status_message_id = telegram_message_id(sent)
        if status_message_id:
            self._save_tasks_from_submit(
                record,
                candidate,
                result,
                category,
                telegram_status_message_id=status_message_id,
                content_profile=content_profile,
            )

    def _find_duplicate_before_submit(self, category, record, candidate):
        local = find_local_duplicate(self.store.list_all_tasks(), category, record, candidate)
        if local:
            return local
        indexed = find_index_duplicate(self.store, category, record, candidate)
        if indexed:
            return indexed
        return self.service.check_duplicate(category, record.get("query") or "", candidate)

    def _handle_status_callback(self, user_id, chat_id, message_id, callback_id, info_hash):
        record = self._load_owned_task(user_id, callback_id, info_hash)
        if record is None:
            return
        record = self._remember_status_message_id(record, message_id)
        if task_is_final(record["task"]) and (record["task"] or {}).get("status_name") != "success":
            self.telegram.answer_callback_query(callback_id, "任务已结束")
            self._update_callback_message(
                chat_id,
                message_id,
                format_task_status_message(record["title"], record["task"], category=record["category"]),
                reply_markup=callback_task_reply_markup(record["task"]),
                fallback_chat_id=record["chat_id"],
            )
            return
        with self._typing_action(chat_id or record["chat_id"]):
            self.telegram.answer_callback_query(callback_id, "正在刷新进度")
            if task_is_final(record["task"]):
                task = self._update_status_message_before_sync(record, record["task"], chat_id, message_id)
                task = self._sync_completed_task(
                    record,
                    task,
                    progress_callback=self._callback_sync_progress_updater(record, chat_id, message_id),
                )
                task = self._task_with_known_status_message_id(record, task, message_id=message_id)
                self.store.save_task(user_id, record["chat_id"], record["category"], record["title"], task)
                self._update_callback_message(
                    chat_id,
                    message_id,
                    format_task_status_message(record["title"], task, category=record["category"]),
                    reply_markup=callback_task_reply_markup(task),
                    fallback_chat_id=record["chat_id"],
                )
                return
            try:
                task = self.service.task_status(record["category"], record["info_hash"])
            except RuntimeError as exc:
                self._update_callback_message(chat_id, message_id, "查询失败：%s" % exc, fallback_chat_id=record["chat_id"])
                return
            task = self._update_status_message_before_sync(record, task, chat_id, message_id)
            task = self._sync_completed_task(
                record,
                task,
                progress_callback=self._callback_sync_progress_updater(record, chat_id, message_id),
            )
            task = self._task_with_known_status_message_id(record, task, message_id=message_id)
            self.store.save_task(user_id, record["chat_id"], record["category"], record["title"], task)
            self._update_callback_message(
                chat_id,
                message_id,
                format_task_status_message(record["title"], task, category=record["category"]),
                reply_markup=callback_task_reply_markup(task),
                fallback_chat_id=record["chat_id"],
            )

    def _handle_cancel_callback(self, user_id, chat_id, message_id, callback_id, info_hash):
        record = self._load_owned_task(user_id, callback_id, info_hash)
        if record is None:
            return
        if task_is_final(record["task"]):
            self.telegram.answer_callback_query(callback_id, "任务已结束")
            self._update_callback_message(
                chat_id,
                message_id,
                format_task_status_message(record["title"], record["task"], category=record["category"]),
                reply_markup=callback_task_reply_markup(record["task"]),
                fallback_chat_id=record["chat_id"],
            )
            return
        try:
            result = self.service.cancel_task(record["category"], record["info_hash"])
        except RuntimeError as exc:
            self.telegram.answer_callback_query(callback_id, "取消失败")
            self._update_callback_message(chat_id, message_id, "取消失败：%s" % exc, fallback_chat_id=record["chat_id"])
            return
        task = result.get("task") or record["task"]
        self.store.save_task(user_id, record["chat_id"], record["category"], record["title"], task)
        self.telegram.answer_callback_query(callback_id, "已取消任务" if result.get("cancelled") else "任务不可取消")
        self._update_callback_message(
            chat_id,
            message_id,
            format_cancel_result_message(record["title"], result, category=record["category"]),
            reply_markup=callback_task_reply_markup(task),
            fallback_chat_id=record["chat_id"],
        )

    def _handle_retry_msg_callback(self, user_id, chat_id, message_id, callback_id, info_hash):
        record = self._load_owned_task(user_id, callback_id, info_hash)
        if record is None:
            return
        record = self._remember_status_message_id(record, message_id)
        if not task_can_retry_msg_sync(record["task"]):
            self.telegram.answer_callback_query(callback_id, "当前任务不能重试MSG同步")
            self._update_callback_message(
                chat_id,
                message_id,
                format_task_status_message(record["title"], record["task"], category=record["category"]),
                reply_markup=callback_task_reply_markup(record["task"]),
                fallback_chat_id=record["chat_id"],
            )
            return

        with self._typing_action(chat_id or record["chat_id"]):
            self.telegram.answer_callback_query(callback_id, "正在重试MSG同步")
            task = self._retry_msg_sync_task(
                record,
                record["task"],
                progress_callback=self._callback_sync_progress_updater(record, chat_id, message_id),
            )
        self.store.save_task(user_id, record["chat_id"], record["category"], record["title"], task)
        self._update_callback_message(
            chat_id,
            message_id,
            format_task_status_message(record["title"], task, category=record["category"]),
            reply_markup=callback_task_reply_markup(task),
            fallback_chat_id=record["chat_id"],
        )

    def _handle_migrate_command(self, chat_id, user_id, argument):
        query = str(argument or "").strip()
        if not query:
            self.telegram.send_message(chat_id, "请输入要迁移的媒体关键词，例如：/migrate 成龙历险记")
            return
        try:
            candidates = self.service.search_migration_candidates(query, limit=20)
        except (RuntimeError, ValueError) as exc:
            self.telegram.send_message(chat_id, "迁移搜索失败：%s" % exc)
            return
        if not candidates:
            self.telegram.send_message(chat_id, "未找到可迁移媒体：%s" % query)
            return

        saved = []
        for candidate in candidates:
            candidate_id = self.store.save_migration_candidate(user_id, chat_id, query, candidate)
            saved.append((candidate_id, candidate))
        self.telegram.send_message(
            chat_id,
            format_migration_search_message(query, saved),
            reply_markup=migration_search_reply_markup(saved),
        )

    def _handle_migrate_select_callback(self, user_id, chat_id, message_id, callback_id, candidate_id):
        record = self._load_owned_migration_candidate(user_id, callback_id, candidate_id)
        if record is None:
            return
        self.telegram.answer_callback_query(callback_id, "请选择目标库")
        self._update_callback_message(
            chat_id,
            message_id,
            format_migration_target_choice_message(record["candidate"]),
            reply_markup=migration_target_choice_reply_markup(candidate_id, record["candidate"]),
            fallback_chat_id=record["chat_id"],
        )

    def _handle_migrate_to_callback(self, user_id, chat_id, message_id, callback_id, target_category, candidate_id):
        record = self._load_owned_migration_candidate(user_id, callback_id, candidate_id)
        if record is None:
            return
        candidate = record["candidate"]
        try:
            target = build_migration_target(candidate, target_category)
        except ValueError as exc:
            self.telegram.answer_callback_query(callback_id, str(exc))
            return
        self.telegram.answer_callback_query(callback_id, "请确认迁移")
        self._update_callback_message(
            chat_id,
            message_id,
            format_migration_confirm_message(candidate, target_category, target),
            reply_markup=migration_confirm_reply_markup(candidate_id, target_category),
            fallback_chat_id=record["chat_id"],
        )

    def _handle_migrate_confirm_callback(self, user_id, chat_id, message_id, callback_id, target_category, candidate_id):
        record = self._load_owned_migration_candidate(user_id, callback_id, candidate_id)
        if record is None:
            return
        candidate = record["candidate"]
        self.telegram.answer_callback_query(callback_id, "开始迁移")
        self._update_callback_message(
            chat_id,
            message_id,
            format_migration_running_message(candidate, target_category),
            reply_markup={"inline_keyboard": []},
            fallback_chat_id=record["chat_id"],
        )
        try:
            with self._typing_action(chat_id or record["chat_id"]):
                result = self.service.migrate_media_candidate(candidate, target_category)
        except (RuntimeError, ValueError) as exc:
            self._update_callback_message(
                chat_id,
                message_id,
                "迁移失败：%s" % exc,
                reply_markup={"inline_keyboard": []},
                fallback_chat_id=record["chat_id"],
            )
            return
        self._update_callback_message(
            chat_id,
            message_id,
            format_migration_result_message(candidate, result),
            reply_markup={"inline_keyboard": []},
            fallback_chat_id=record["chat_id"],
        )

    def _handle_migrate_cancel_callback(self, user_id, chat_id, message_id, callback_id, candidate_id):
        record = self._load_owned_migration_candidate(user_id, callback_id, candidate_id)
        if record is None:
            return
        self.telegram.answer_callback_query(callback_id, "已取消迁移")
        self._update_callback_message(
            chat_id,
            message_id,
            "已取消迁移：%s" % record["candidate"].get("title"),
            reply_markup={"inline_keyboard": []},
            fallback_chat_id=record["chat_id"],
        )

    def _handle_status_command(self, chat_id, user_id, argument):
        info_hash = argument.strip()
        if not info_hash:
            self.telegram.send_message(chat_id, "请输入 info_hash")
            return
        try:
            record = self.store.load_task(info_hash)
        except RuntimeError:
            self.telegram.send_message(chat_id, "未找到该任务记录")
            return
        if record["user_id"] != user_id:
            self.telegram.send_message(chat_id, "无权查看该任务")
            return
        if task_is_final(record["task"]):
            task = self._send_status_message_before_sync(chat_id, record, record["task"])
            task = self._sync_completed_task(record, task)
            self.store.save_task(user_id, record["chat_id"], record["category"], record["title"], task)
            if task != record["task"]:
                self.telegram.send_message(chat_id, format_task_status_message(record["title"], task, category=record["category"]), reply_markup=task_reply_markup(task))
            return
        try:
            task = self.service.task_status(record["category"], record["info_hash"])
        except RuntimeError as exc:
            self.telegram.send_message(chat_id, "查询失败：%s" % exc)
            return
        task = self._send_status_message_before_sync(chat_id, record, task)
        task = self._sync_completed_task(record, task)
        self.store.save_task(user_id, record["chat_id"], record["category"], record["title"], task)
        self.telegram.send_message(chat_id, format_task_status_message(record["title"], task, category=record["category"]), reply_markup=task_reply_markup(task))

    def _send_task_list(self, chat_id, user_id):
        records, page, page_count, total = self._task_list_page(user_id, page=0)
        if not records:
            self.telegram.send_message(chat_id, "暂无任务")
            return
        self.telegram.send_message(
            chat_id,
            format_task_list_message(records, page=page, page_count=page_count, total=total),
            reply_markup=task_list_reply_markup(records, page=page, page_count=page_count),
        )

    def _handle_task_page_callback(self, user_id, chat_id, message_id, callback_id, page):
        records, page, page_count, total = self._task_list_page(user_id, page=page)
        if not records:
            self.telegram.answer_callback_query(callback_id, "暂无任务")
            self._update_callback_message(chat_id, message_id, "暂无任务", reply_markup={"inline_keyboard": []})
            return
        self.telegram.answer_callback_query(callback_id, "第 %s/%s 页" % (page + 1, page_count))
        self._update_callback_message(
            chat_id,
            message_id,
            format_task_list_message(records, page=page, page_count=page_count, total=total),
            reply_markup=task_list_reply_markup(records, page=page, page_count=page_count),
        )

    def _task_list_page(self, user_id, page=0):
        records = prioritized_task_records(self.store.list_tasks(user_id, limit=DEFAULT_TASK_LIST_FETCH_LIMIT))
        total = len(records)
        page_count = task_page_count(total)
        page = normalize_page(page, page_count)
        start = page * DEFAULT_TASK_LIST_PAGE_SIZE
        return records[start : start + DEFAULT_TASK_LIST_PAGE_SIZE], page, page_count, total

    def _handle_dedupe_refresh_command(self, chat_id):
        self.telegram.send_message(chat_id, DEDUPE_REFRESH_WARNING_TEXT, reply_markup=dedupe_refresh_confirm_reply_markup())

    def _handle_dedupe_refresh_confirm_callback(self, chat_id, message_id, callback_id):
        self.telegram.answer_callback_query(callback_id, "开始刷新已入库记录")
        self._update_callback_message(chat_id, message_id, "正在刷新已入库记录，请稍候...", reply_markup={"inline_keyboard": []})
        with self._typing_action(chat_id):
            self._run_dedupe_refresh(chat_id, message_id=message_id)

    def _handle_dedupe_refresh_cancel_callback(self, chat_id, message_id, callback_id):
        self.telegram.answer_callback_query(callback_id, "已取消刷新")
        self._update_callback_message(chat_id, message_id, "已取消刷新已入库记录。", reply_markup={"inline_keyboard": []})

    def _run_dedupe_refresh(self, chat_id, message_id=None):
        try:
            entries = self.service.collect_openlist_dedupe_entries(refresh=True)
            count = self.store.replace_dedupe_entries("openlist", entries)
        except (RuntimeError, ValueError) as exc:
            self._update_callback_message(
                chat_id,
                message_id,
                "OpenList已入库记录刷新失败：%s" % exc,
                reply_markup={"inline_keyboard": []},
                fallback_chat_id=chat_id,
            )
            return
        self._update_callback_message(
            chat_id,
            message_id,
            format_dedupe_refresh_message(entries, count),
            reply_markup={"inline_keyboard": []},
            fallback_chat_id=chat_id,
        )

    def _render_search_page(self, session_id, page):
        session = self.store.load_search_session(session_id)
        candidate_ids = session["candidate_ids"]
        page_count = search_page_count(len(candidate_ids))
        page = normalize_page(page, page_count)
        start = page * SEARCH_PAGE_SIZE
        page_candidate_ids = candidate_ids[start : start + SEARCH_PAGE_SIZE]
        candidates = []
        for candidate_id in page_candidate_ids:
            record = self.store.load_candidate(candidate_id)
            candidates.append((candidate_id, record["candidate"]))
        is_adult_session = session["category"] == "adult"
        is_anime_session = session["category"] == "anime"
        title = "搜索结果"
        if not candidate_ids:
            title = "未找到可用资源"
        elif is_adult_session:
            title = "成人源搜索结果"
        elif is_anime_session:
            title = "动漫源搜索结果"
        return format_search_page_message(
            session["query"],
            candidates,
            page,
            page_count,
            len(candidate_ids),
            title=title,
        ), search_page_reply_markup(
            session_id,
            candidates,
            page,
            page_count,
            allow_adult_retry=not is_adult_session and not is_anime_session and not is_strong_adult_code_query(session["query"]),
            allow_anime_retry=not is_adult_session
            and not is_anime_session
            and not is_strong_adult_code_query(session["query"])
            and not should_search_anime(DEFAULT_SEARCH_CATEGORY, session["query"]),
        )

    def _save_tasks_from_submit(self, record, candidate, result, category, telegram_status_message_id=None, content_profile=None):
        title = candidate.get("title") or record["query"]
        content_profile = normalize_content_profile(category, content_profile)
        for task in result.get("tasks") or []:
            info_hash = task.get("info_hash")
            if not info_hash:
                continue
            saved_task = task_from_submit_result(result, info_hash)
            saved_task["content_profile"] = content_profile
            if telegram_status_message_id is not None:
                saved_task["telegram_status_message_id"] = int(telegram_status_message_id)
            self.store.save_task(
                record["user_id"],
                record["chat_id"],
                category,
                title,
                saved_task,
            )

    def _load_owned_task(self, user_id, callback_id, info_hash):
        try:
            record = self.store.load_task(info_hash)
        except RuntimeError:
            self.telegram.answer_callback_query(callback_id, "任务不存在")
            return None
        if record["user_id"] != user_id:
            self.telegram.answer_callback_query(callback_id, "无权操作此任务")
            return None
        return record

    def _load_owned_migration_candidate(self, user_id, callback_id, candidate_id):
        try:
            record = self.store.load_migration_candidate(candidate_id)
        except RuntimeError:
            self.telegram.answer_callback_query(callback_id, "迁移候选不存在")
            return None
        if record["user_id"] != user_id:
            self.telegram.answer_callback_query(callback_id, "无权操作此迁移")
            return None
        return record

    def _sync_completed_task(self, record, task, progress_callback=None):
        return self.service.sync_completed_task(record["category"], record["title"], task, progress_callback=progress_callback)

    def _retry_msg_sync_task(self, record, task, progress_callback=None):
        retry_task = dict(task or {})
        retry_task["msg_sync_status"] = "running"
        retry_task["msg_error"] = None
        self.store.save_task(record["user_id"], record["chat_id"], record["category"], record["title"], retry_task)
        return self._sync_completed_task(record, retry_task, progress_callback=progress_callback)

    def _callback_sync_progress_updater(self, record, chat_id, message_id):
        def update(task):
            task = self._task_with_known_status_message_id(record, task, message_id=message_id)
            self.store.save_task(record["user_id"], record["chat_id"], record["category"], record["title"], task)
            self._update_callback_message(
                chat_id,
                message_id,
                format_task_status_message(record["title"], task, category=record["category"]),
                reply_markup=callback_task_reply_markup(task),
                fallback_chat_id=record["chat_id"],
            )

        return update

    def _update_status_message_before_sync(self, record, task, chat_id, message_id):
        task = self._task_with_syncing_status(task)
        task = self._task_with_known_status_message_id(record, task, message_id=message_id)
        if not task_sync_is_running(task):
            return task
        self.store.save_task(record["user_id"], record["chat_id"], record["category"], record["title"], task)
        self._update_callback_message(
            chat_id,
            message_id,
            format_task_status_message(record["title"], task, category=record["category"]),
            reply_markup=callback_task_reply_markup(task),
            fallback_chat_id=record["chat_id"],
        )
        return task

    def _send_status_message_before_sync(self, chat_id, record, task):
        task = self._task_with_syncing_status(task)
        if not task_sync_is_running(task):
            return task
        self.store.save_task(record["user_id"], record["chat_id"], record["category"], record["title"], task)
        sent = self.telegram.send_message(chat_id, format_task_status_message(record["title"], task, category=record["category"]), reply_markup=task_reply_markup(task))
        task = self._task_with_known_status_message_id(record, task, message_id=telegram_message_id(sent))
        self.store.save_task(record["user_id"], record["chat_id"], record["category"], record["title"], task)
        return task

    def _remember_status_message_id(self, record, message_id):
        task = self._task_with_known_status_message_id(record, record.get("task"), message_id=message_id)
        if task == record.get("task"):
            return record
        self.store.save_task(record["user_id"], record["chat_id"], record["category"], record["title"], task)
        out = dict(record)
        out["task"] = task
        return out

    def _task_with_known_status_message_id(self, record, task, message_id=None):
        status_message_id = normalize_telegram_message_id(message_id)
        if not status_message_id:
            status_message_id = task_telegram_status_message_id(task)
        if not status_message_id:
            status_message_id = task_telegram_status_message_id((record or {}).get("task"))
        if not status_message_id:
            info_hash = (task or {}).get("info_hash") or (record or {}).get("info_hash")
            if info_hash:
                try:
                    current_record = self.store.load_task(info_hash)
                except RuntimeError:
                    current_record = None
                if current_record is not None:
                    status_message_id = task_telegram_status_message_id(current_record.get("task"))
        if not status_message_id:
            return task
        out = dict(task or {})
        out["telegram_status_message_id"] = status_message_id
        return out

    def _task_with_syncing_status(self, task):
        if not self._should_show_syncing_status(task):
            return task
        out = dict(task or {})
        out["msg_sync_status"] = "running"
        out["msg_error"] = None
        return out

    def _should_show_syncing_status(self, task):
        return bool(
            self.config.msg_enabled
            and (task or {}).get("status_name") == "success"
            and not task_msg_synced(task)
            and not task_sync_is_running(task)
        )

    def _delete_callback_message(self, chat_id, message_id):
        if chat_id is None or message_id is None:
            return
        try:
            self.telegram.delete_message(chat_id, message_id)
        except RuntimeError:
            return

    def _update_callback_message(self, chat_id, message_id, text, reply_markup=None, fallback_chat_id=None):
        target_chat_id = chat_id or fallback_chat_id
        last_error = None
        if chat_id is not None and message_id is not None:
            try:
                self.telegram.edit_message_text(chat_id, message_id, text, reply_markup=reply_markup)
                return
            except RuntimeError as exc:
                if "message is not modified" in str(exc).lower():
                    return
                last_error = exc
        if target_chat_id is not None:
            try:
                self.telegram.send_message(target_chat_id, text, reply_markup=reply_markup)
                return
            except RuntimeError as exc:
                last_error = exc
        if last_error is not None:
            print("telegram message update failed: %s" % last_error, flush=True)

    def _is_allowed(self, user_id):
        return user_id in self.config.allowed_user_ids

    def _send_typing_action(self, chat_id):
        if chat_id is None:
            return
        try:
            self.telegram.send_chat_action(chat_id, "typing")
        except Exception as exc:
            print("telegram typing action failed: %s" % exc, flush=True)

    def _typing_action(self, chat_id):
        if chat_id is None:
            return nullcontext()
        return TypingActionPulse(self, chat_id)

    def recover_running_msg_sync_tasks_once(self):
        count = 0
        for record in self.store.list_msg_sync_running_tasks():
            try:
                task = self._sync_completed_task(record, record["task"])
                self.store.save_task(record["user_id"], record["chat_id"], record["category"], record["title"], task)
                self.telegram.send_message(
                    record["chat_id"],
                    format_task_status_message(record["title"], task, category=record["category"]),
                    reply_markup=task_reply_markup(task),
                )
                count += 1
            except Exception as exc:
                print("bot sync recovery failed for %s: %s" % (record["info_hash"], exc), flush=True)
        return count

    def recover_active_115_tasks_once(self, now=None):
        now = int(time.time() if now is None else now)
        count = 0
        records_by_category = defaultdict(list)
        for record in self.store.list_active_115_tasks():
            if active_115_task_timed_out(record, now):
                count += self._auto_cancel_timed_out_115_task(record, now)
            elif active_115_task_poll_due(record, now, normal_interval_seconds=self.config.sync_recovery_interval_seconds):
                records_by_category[record["category"]].append(record)

        for category, records in records_by_category.items():
            info_hashes = [record["info_hash"] for record in records]
            try:
                statuses = self.service.task_statuses(category, info_hashes)
            except Exception as exc:
                print("bot 115 status recovery failed for %s: %s" % (category, exc), flush=True)
                continue

            for record in records:
                polled_task = mark_active_115_task_polled(record, now)
                task = statuses.get(str(record["info_hash"]).strip().lower())
                if task:
                    updated_task = dict(polled_task)
                    updated_task.update(task)
                    task = updated_task
                    task.setdefault("info_hash", record["info_hash"])
                else:
                    task = polled_task
                task = self._task_with_known_status_message_id(record, task)
                task = self._sync_completed_task_with_store_progress(record, task)
                task = self._task_with_known_status_message_id(record, task)
                self.store.save_task(record["user_id"], record["chat_id"], record["category"], record["title"], task)
                self._update_recovered_115_task_message(record, task)
                count += 1
        return count

    def _update_recovered_115_task_message(self, record, task, final_fallback=True):
        message_id = task_telegram_status_message_id(task) or task_telegram_status_message_id(record.get("task"))
        text = format_task_status_message(record["title"], task, category=record["category"])
        markup = task_reply_markup(task)
        if message_id:
            self._update_callback_message(
                record["chat_id"],
                message_id,
                text,
                reply_markup=markup,
                fallback_chat_id=record["chat_id"] if final_fallback and task_is_final(task) else None,
            )
            return
        if final_fallback and task_is_final(task):
            self.telegram.send_message(record["chat_id"], text, reply_markup=markup)

    def _auto_cancel_timed_out_115_task(self, record, now):
        try:
            result = self.service.cancel_task(record["category"], record["info_hash"])
        except Exception as exc:
            print("bot 115 timeout cancel failed for %s: %s" % (record["info_hash"], exc), flush=True)
            return 0
        task = dict(result.get("task") or record["task"] or {})
        task.setdefault("info_hash", record["info_hash"])
        if result.get("cancelled"):
            mark_task_auto_cancelled(task, now)
        if task.get("status_name") == "success":
            task = self._sync_completed_task_with_store_progress(record, task)
        self.store.save_task(record["user_id"], record["chat_id"], record["category"], record["title"], task)
        if result.get("cancelled"):
            message = format_auto_cancel_result_message(record["title"], result, task, category=record["category"])
        else:
            message = format_cancel_result_message(record["title"], result, category=record["category"])
        self.telegram.send_message(record["chat_id"], message, reply_markup=task_reply_markup(task))
        return 1

    def _sync_completed_task_with_store_progress(self, record, task):
        def save_progress(progress):
            progress_task = dict(task or {})
            progress_task.update(progress)
            progress_task = self._task_with_known_status_message_id(record, progress_task)
            self.store.save_task(record["user_id"], record["chat_id"], record["category"], record["title"], progress_task)
            self._update_recovered_115_task_message(record, progress_task, final_fallback=False)

        return self._sync_completed_task(record, task, progress_callback=save_progress)

    def start_sync_recovery_thread(self):
        if self.config.sync_recovery_interval_seconds <= 0:
            return
        if self._recovery_thread is not None:
            return
        self._recovery_thread = threading.Thread(target=self._run_sync_recovery_loop, daemon=True)
        self._recovery_thread.start()

    def _run_sync_recovery_loop(self):
        interval = max(1, int(self.config.sync_recovery_interval_seconds))
        loop_interval = min(interval, ACTIVE_115_FAST_POLL_INTERVAL_SECONDS)
        last_msg_recovery_at = 0
        while True:
            now = int(time.time())
            self.recover_active_115_tasks_once(now=now)
            if now - last_msg_recovery_at >= interval:
                self.recover_running_msg_sync_tasks_once()
                last_msg_recovery_at = now
            time.sleep(loop_interval)

    def run_forever(self):
        self.start_sync_recovery_thread()
        offset = None
        while True:
            offset = self.poll_updates_once(offset)

    def poll_updates_once(self, offset):
        try:
            updates = self.telegram.get_updates(offset=offset, timeout=30).get("result") or []
        except Exception as exc:
            print("bot polling failed: %s" % exc, flush=True)
            time.sleep(5)
            return offset
        for update in updates:
            next_offset = update["update_id"] + 1
            try:
                self.handle_update(update)
            except Exception as exc:
                print("bot update failed: %s" % exc, flush=True)
                self._send_update_failure_message(update, exc)
            offset = next_offset
        return offset

    def _send_update_failure_message(self, update, exc):
        callback = update.get("callback_query") or {}
        callback_id = callback.get("id")
        message = update.get("message") or callback.get("message") or {}
        chat_id = (message.get("chat") or {}).get("id")
        if callback_id:
            try:
                self.telegram.answer_callback_query(callback_id, "处理失败")
            except Exception as answer_exc:
                print("telegram callback failure notice failed: %s" % answer_exc, flush=True)
        if chat_id is not None:
            try:
                self.telegram.send_message(chat_id, "处理失败：%s" % exc)
            except Exception as send_exc:
                print("telegram update failure notice failed: %s" % send_exc, flush=True)


class TypingActionPulse:
    def __init__(self, bot, chat_id, interval_seconds=TYPING_ACTION_INTERVAL_SECONDS):
        self.bot = bot
        self.chat_id = chat_id
        self.interval_seconds = interval_seconds
        self._stop = threading.Event()
        self._thread = None

    def __enter__(self):
        self.bot._send_typing_action(self.chat_id)
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        return self

    def __exit__(self, exc_type, exc, traceback):
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=0.2)
        return False

    def _run(self):
        while not self._stop.wait(self.interval_seconds):
            self.bot._send_typing_action(self.chat_id)


def split_command(text):
    command, _, argument = text.partition(" ")
    command = command.split("@", 1)[0]
    return command, argument.strip()


def search_profile_for_query(category, query):
    if category == "adult" or is_strong_adult_code_query(query):
        return SEARCH_PROFILE_ADULT
    if should_search_anime(category, query):
        return SEARCH_PROFILE_ANIME
    return SEARCH_PROFILE_GENERAL


def is_strong_adult_code_query(query):
    codes = sorted(extract_codes(query))
    if len(codes) != 1:
        return False
    code = codes[0]
    compact_query = re.sub(r"[^0-9A-Za-z]+", "", str(query or "")).upper()
    compact_code = re.sub(r"[^0-9A-Za-z]+", "", code).upper()
    return bool(compact_query) and compact_query == compact_code


def safe_prowlarr_tags(prowlarr):
    if not hasattr(prowlarr, "tags"):
        return []
    try:
        return prowlarr.tags()
    except Exception as error:
        print("prowlarr tag load failed: %s" % error, file=sys.stderr)
        return []


def search_profile_indexer_results(prowlarr, query, profile, limit, indexers=None, tags=None, timeout_seconds=None):
    if indexers is None:
        indexers = prowlarr.indexers()
    selected = search_profile_indexers(indexers, tags or [], profile)
    categories = SEARCH_PROFILE_CATEGORIES.get(profile, SEARCH_PROFILE_CATEGORIES[SEARCH_PROFILE_GENERAL])
    if not selected:
        return prowlarr.search(query, limit=limit, categories=categories)
    return search_indexers_concurrently(
        prowlarr,
        query,
        limit,
        selected,
        categories=categories,
        timeout_seconds=timeout_seconds or DEFAULT_PROWLARR_SEARCH_TIMEOUT_SECONDS,
    )


def search_profile_indexers(indexers, tags, profile):
    enabled = [indexer for indexer in indexers if indexer_enabled(indexer) and indexer.get("id") is not None]
    tag_ids = search_profile_tag_ids(tags, profile)
    if tag_ids:
        tagged = [indexer for indexer in enabled if tag_ids.intersection(set(indexer.get("tags") or []))]
        if tagged:
            return tagged
    categories = SEARCH_PROFILE_CATEGORIES.get(profile, ())
    return [indexer for indexer in enabled if indexer_supports_any_category(indexer, categories)]


def search_profile_tag_ids(tags, profile):
    labels = {label.casefold() for label in SEARCH_PROFILE_TAG_LABELS.get(profile, ())}
    ids = set()
    for tag in tags or []:
        label = str(tag.get("label") or tag.get("name") or "").casefold()
        if label in labels and tag.get("id") is not None:
            ids.add(tag.get("id"))
    return ids


def indexer_supports_any_category(indexer, categories):
    supported = indexer_category_ids(indexer)
    return any(int(category) in supported for category in categories)


def indexer_category_ids(indexer):
    ids = set()

    def visit(category):
        if not isinstance(category, dict):
            return
        category_id = category.get("id")
        try:
            ids.add(int(category_id))
        except (TypeError, ValueError):
            pass
        for child in category.get("subCategories") or []:
            visit(child)

    capabilities = (indexer or {}).get("capabilities") or {}
    for category in capabilities.get("categories") or []:
        visit(category)
    return ids


def indexer_priority_map(indexers):
    priorities = {}
    for indexer in indexers or []:
        indexer_id = indexer.get("id")
        priority = indexer.get("priority")
        try:
            priorities[int(indexer_id)] = int(priority)
        except (TypeError, ValueError):
            pass
    return priorities


def search_indexers_concurrently(prowlarr, query, limit, indexers, categories=None, timeout_seconds=DEFAULT_PROWLARR_SEARCH_TIMEOUT_SECONDS):
    results = []
    if not indexers:
        return results
    max_workers = max(1, min(8, len(indexers)))
    executor = ThreadPoolExecutor(max_workers=max_workers)
    future_to_indexer = {
        executor.submit(prowlarr.search, query, limit, [indexer.get("id")], categories): indexer for indexer in indexers
    }
    done, pending = wait(future_to_indexer, timeout=timeout_seconds)
    for future in done:
        indexer = future_to_indexer[future]
        try:
            results.extend(future.result())
        except Exception as error:
            print("profile indexer search failed: %s: %s" % (indexer.get("name") or indexer.get("id"), error), file=sys.stderr)
    for future in pending:
        indexer = future_to_indexer[future]
        print("profile indexer search timed out: %s" % (indexer.get("name") or indexer.get("id")), file=sys.stderr)
        future.cancel()
    executor.shutdown(wait=False, cancel_futures=True)
    return results


def search_primary_indexer_results(prowlarr, query, limit, indexers=None):
    if indexers is None:
        indexers = prowlarr.indexers()
    primary_indexers = [
        indexer
        for indexer in indexers
        if indexer_enabled(indexer)
        and not ResourceSelector.is_anime_specialized_item(indexer)
        and not ResourceSelector.is_sukebei_item(indexer)
        and indexer.get("id") is not None
    ]
    indexer_ids = [indexer.get("id") for indexer in primary_indexers]
    if not indexers:
        return prowlarr.search(query, limit=limit)
    if not indexer_ids:
        return []
    try:
        return prowlarr.search(query, limit=limit, indexer_ids=indexer_ids)
    except Exception as error:
        print("primary aggregate indexer search failed: %s" % error, file=sys.stderr)
        return search_primary_indexers_individually(prowlarr, query, limit, primary_indexers, error)


def search_primary_indexers_individually(prowlarr, query, limit, indexers, aggregate_error):
    results = []
    attempted = 0
    failures = []
    for indexer in indexers:
        indexer_id = indexer.get("id")
        if indexer_id is None:
            continue
        attempted += 1
        try:
            results.extend(search_indexer_with_timeout(prowlarr, query, limit, indexer_id, timeout=DEFAULT_PRIMARY_INDEXER_TIMEOUT_SECONDS))
        except Exception as error:
            failures.append((indexer.get("name") or indexer_id, error))
            print("primary indexer search failed: %s: %s" % (indexer.get("name") or indexer_id, error), file=sys.stderr)
    if attempted and len(failures) == attempted:
        raise RuntimeError("primary aggregate search failed and all primary indexers failed: %s" % aggregate_error)
    return results


def should_search_sukebei(category, query):
    return category == "adult" or is_strong_adult_code_query(query)


def should_search_anime(category, query):
    if category == "adult" or is_strong_adult_code_query(query):
        return False
    if category == "tv" and ANIME_QUERY_HINT_PATTERN.search(str(query or "")):
        return True
    return bool(ANIME_QUERY_HINT_PATTERN.search(str(query or "")))


def indexer_enabled(indexer):
    return (indexer or {}).get("enable", (indexer or {}).get("enabled", True)) is not False


def search_sukebei_indexer_results(prowlarr, query, indexers=None):
    return search_required_indexer_results(
        prowlarr,
        query,
        ResourceSelector.is_sukebei_item,
        DEFAULT_REQUIRED_INDEXER_SEARCH_LIMIT,
        indexers=indexers,
    )


def search_anime_indexer_results(prowlarr, query, indexers=None):
    return search_required_indexer_results(
        prowlarr,
        query,
        ResourceSelector.is_anime_specialized_item,
        DEFAULT_ANIME_INDEXER_SEARCH_LIMIT,
        indexers=indexers,
        optional=True,
        timeout=DEFAULT_OPTIONAL_INDEXER_TIMEOUT_SECONDS,
    )


def search_required_indexer_results(prowlarr, query, predicate, limit, indexers=None, optional=False, timeout=None):
    results = []
    if indexers is None:
        indexers = prowlarr.indexers()
    for indexer in indexers:
        if not indexer_enabled(indexer):
            continue
        if not predicate(indexer):
            continue
        indexer_id = indexer.get("id")
        if indexer_id is None:
            continue
        try:
            results.extend(search_indexer_with_timeout(prowlarr, query, limit, indexer_id, timeout=timeout))
        except Exception as error:
            if not optional:
                raise
            print("optional indexer search failed: %s: %s" % (indexer.get("name"), error), file=sys.stderr)
    return results


def search_indexer_with_timeout(prowlarr, query, limit, indexer_id, timeout=None):
    if timeout is None or not hasattr(prowlarr, "timeout"):
        return prowlarr.search(query, limit=limit, indexer_ids=[indexer_id])
    original_timeout = prowlarr.timeout
    prowlarr.timeout = min(original_timeout, timeout)
    try:
        return prowlarr.search(query, limit=limit, indexer_ids=[indexer_id])
    finally:
        prowlarr.timeout = original_timeout


def magnet_candidate_from_text(text):
    uri = extract_magnet_uri(text)
    if not uri:
        return None
    info_hash = magnet_info_hash(uri)
    return {
        "title": magnet_title(uri, info_hash),
        "download_uri": uri,
        "indexer": "磁链",
        "seeders": None,
        "size": None,
        "rank": 1,
        "infoHash": info_hash,
    }


def extract_magnet_uri(text):
    match = re.search(r"magnet:\?[^\s]+", str(text or ""), re.IGNORECASE)
    if not match:
        return None
    uri = match.group(0).rstrip(".,;，。；)")
    if urllib.parse.urlsplit(uri).scheme.lower() != "magnet":
        return None
    return uri


def magnet_title(uri, info_hash=None):
    params = urllib.parse.parse_qs(urllib.parse.urlsplit(uri).query)
    display_names = params.get("dn") or []
    for value in display_names:
        title = str(value or "").strip()
        if title:
            return title
    if info_hash:
        return "磁链 %s" % info_hash[:12]
    return "磁链"


def magnet_info_hash(uri):
    params = urllib.parse.parse_qs(urllib.parse.urlsplit(uri).query)
    for value in params.get("xt") or []:
        prefix = "urn:btih:"
        if value.lower().startswith(prefix):
            return value[len(prefix) :].strip()
    return None


def parse_bool(value, default=False):
    if value is None:
        return default
    return str(value).strip().lower() in ("1", "true", "yes", "on")


def normalize_content_profile(category, content_profile=None):
    if not content_profile:
        if category in CONTENT_PROFILE_LABELS:
            return category
        raise ValueError("unsupported category: %s" % (category or "-"))
    content_profile = str(content_profile).strip()
    if content_profile not in CONTENT_PROFILE_LABELS:
        raise ValueError("unsupported content profile: %s" % content_profile)
    expected_category = content_profile_to_category(content_profile)
    if expected_category != category:
        raise ValueError("content profile %s cannot be submitted to %s" % (content_profile, category))
    return content_profile


def content_profile_to_category(content_profile):
    content_profile = str(content_profile or "").strip()
    if content_profile in CONTENT_PROFILE_LABELS:
        return content_profile
    raise ValueError("unsupported content profile: %s" % (content_profile or "-"))


def task_msg_synced(task):
    return (task or {}).get("msg_sync_status") == "success" and (task or {}).get("msg_scrape_status") == "success"


def task_sync_is_running(task):
    task = task or {}
    return any(
        task.get(key) == "running"
        for key in (
            "msg_sync_status",
            "openlist_clean_status",
            "openlist_adult_format_status",
            "msg_scan_status",
            "msg_scrape_status",
            "msg_artwork_repair_status",
        )
    )


def stage_is_complete(status):
    return status in ("success", "skipped")


def prefixed_task_fields(task, prefix):
    return {key: value for key, value in (task or {}).items() if key.startswith(prefix)}


def mark_current_sync_stage_failed(task, error):
    if task.get("openlist_clean_status") == "running":
        task["openlist_clean_status"] = "failed"
        task["openlist_clean_error"] = error
    elif task.get("openlist_adult_format_status") == "running":
        task["openlist_adult_format_status"] = "failed"
        task["openlist_adult_format_error"] = error
    elif task.get("msg_scan_status") == "running":
        task["msg_scan_status"] = "failed"
    elif task.get("msg_scrape_status") == "running":
        task["msg_scrape_status"] = "failed"
    elif task.get("msg_artwork_repair_status") == "running":
        task["msg_artwork_repair_status"] = "failed"
        task["msg_artwork_repair_error"] = error


def task_can_retry_msg_sync(task):
    task = task or {}
    return bool(
        task.get("info_hash")
        and task.get("status_name") == "success"
        and task.get("msg_sync_status") == "failed"
        and not task_msg_synced(task)
    )


def find_local_duplicate(records, category, candidate_record, candidate):
    info_hash = candidate_info_hash(candidate)
    if info_hash:
        for record in records:
            if str(record.get("info_hash") or "").lower() == info_hash.lower():
                return duplicate_from_task("strong", "same_info_hash", "Bot状态库", record, can_force=False)

    if category == "adult":
        code = first_adult_code([candidate_record.get("query"), candidate.get("title"), candidate.get("download_uri")])
        if code:
            for record in records:
                if record.get("category") != "adult":
                    continue
                if code in task_duplicate_codes(record):
                    duplicate = duplicate_from_task("strong", "adult_code", "Bot状态库", record, can_force=False)
                    duplicate["code"] = code
                    return duplicate
    return None


def find_index_duplicate(store, category, candidate_record, candidate):
    identities = candidate_dedupe_identities(category, candidate_record, candidate)
    for identity in identities:
        matches = store.find_dedupe_entries(category, [identity], limit=1)
        if matches:
            return duplicate_from_dedupe_entry(identity, matches[0])
    return None


def candidate_dedupe_identities(category, candidate_record, candidate):
    identities = []
    info_hash = candidate_info_hash(candidate)
    if info_hash:
        identities.append({"identity_type": "info_hash", "identity_value": info_hash})

    values = [candidate_record.get("query"), candidate.get("title"), candidate.get("download_uri")]
    if category == "adult":
        code = first_adult_code(values)
        if code:
            identities.append({"identity_type": "adult_code", "identity_value": code})

    for value in (candidate_record.get("query"), candidate.get("title")):
        normalized_title = dedupe_title_identity(value)
        if normalized_title:
            identities.append({"identity_type": "normalized_title", "identity_value": normalized_title})
    return unique_dedupe_identities(identities)


def duplicate_from_dedupe_entry(identity, entry):
    identity_type = identity.get("identity_type")
    level = "weak"
    reason = "openlist_title"
    can_force = True
    if identity_type == "info_hash":
        level = "strong"
        reason = "same_info_hash"
        can_force = False
    elif identity_type == "adult_code":
        level = "strong"
        reason = "adult_code"
        can_force = False
    duplicate = {
        "level": level,
        "reason": reason,
        "source": dedupe_source_label(entry.get("source")),
        "title": entry.get("title"),
        "path": entry.get("path"),
        "can_force": can_force,
    }
    if identity_type == "info_hash":
        duplicate["info_hash"] = identity.get("identity_value")
    if identity_type == "adult_code":
        duplicate["code"] = identity.get("identity_value")
    return duplicate


def duplicate_from_task(level, reason, source, record, can_force=False):
    task = record.get("task") or {}
    return {
        "level": level,
        "reason": reason,
        "source": source,
        "title": record.get("title") or task.get("name") or task.get("file_name") or task.get("info_hash"),
        "info_hash": task.get("info_hash") or record.get("info_hash"),
        "status_name": task.get("status_name"),
        "msg_sync_status": task.get("msg_sync_status"),
        "msg_media_id": task.get("msg_media_id"),
        "can_force": can_force,
    }


def task_duplicate_codes(record):
    task = record.get("task") or {}
    values = [
        record.get("title"),
        task.get("name"),
        task.get("file_name"),
        task.get("openlist_adult_code"),
        task.get("openlist_adult_format_old_path"),
        task.get("openlist_adult_format_new_path"),
    ]
    codes = set()
    for value in values:
        codes.update(extract_codes(value))
    return codes


def candidate_info_hash(candidate):
    for key in ("infoHash", "info_hash"):
        value = str((candidate or {}).get(key) or "").strip()
        if value:
            return value
    return magnet_info_hash((candidate or {}).get("download_uri") or "")


def openlist_dedupe_entries(client, category, root_path):
    entries = []
    for item in openlist_work_items(client, root_path):
        entries.extend(dedupe_entries_from_openlist_work_item(client, category, root_path, item))
    return unique_dedupe_entries(entries)


def openlist_work_items(client, root_path):
    items = []
    for item in client.list_all(root_path, refresh=False):
        if openlist_item_is_dir(item):
            items.append(item)
            continue
        if is_openlist_video_file(item):
            items.append(item)
    return items


def dedupe_entries_from_openlist_work_item(client, category, root_path, item):
    name = openlist_item_name(item)
    path = openlist_item_path(root_path, item)
    if not name:
        return []
    title = name if openlist_item_is_dir(item) else posixpath.splitext(name)[0]
    child_names = openlist_descendant_names(client, root_path, item) if openlist_item_is_dir(item) else []
    base = {
        "category": category,
        "source": "openlist",
        "title": title,
        "path": path,
        "metadata": {
            "is_dir": openlist_item_is_dir(item),
            "size": openlist_item_size(item),
        },
    }
    entries = []

    normalized_title = dedupe_title_identity(name)
    if normalized_title:
        entries.append(
            {
                **base,
                "identity_type": "normalized_title",
                "identity_value": normalized_title,
            }
        )

    info_hash = str((item or {}).get("info_hash") or (item or {}).get("infoHash") or "").strip()
    if info_hash:
        entries.append(
            {
                **base,
                "identity_type": "info_hash",
                "identity_value": info_hash,
            }
        )

    if category == "adult":
        code_text = " ".join([name, path] + child_names)
        for code in sorted(extract_codes(code_text)):
            entries.append(
                {
                    **base,
                    "identity_type": "adult_code",
                    "identity_value": code,
                }
            )
    return entries


def openlist_descendant_names(client, dir_path, item):
    if not openlist_item_is_dir(item):
        return []
    names = []
    child_dir = openlist_item_path(dir_path, item)
    for child in client.list_all(child_dir, refresh=False):
        child_name = openlist_item_name(child)
        if child_name:
            names.append(child_name)
        if openlist_item_is_dir(child):
            names.extend(openlist_descendant_names(client, child_dir, child))
    return names


def unique_dedupe_entries(entries):
    out = []
    seen = set()
    for entry in entries or []:
        normalized = normalize_dedupe_entry(entry)
        key = (
            normalized["category"],
            normalized["source"],
            normalized["identity_type"],
            normalized["identity_value"],
        )
        if key in seen:
            continue
        seen.add(key)
        out.append(normalized)
    return out


def unique_dedupe_identities(identities):
    out = []
    seen = set()
    for identity in identities or []:
        identity_type = str((identity or {}).get("identity_type") or "").strip()
        identity_value = normalize_dedupe_identity_value(identity_type, (identity or {}).get("identity_value"))
        key = (identity_type, identity_value)
        if not identity_type or not identity_value or key in seen:
            continue
        seen.add(key)
        out.append({"identity_type": identity_type, "identity_value": identity_value})
    return out


def normalize_dedupe_entry(entry, default_source=None):
    entry = entry or {}
    category = str(entry.get("category") or "").strip()
    if category not in CATEGORY_LABELS:
        raise ValueError("invalid dedupe category: %s" % (category or "-"))
    source = str(entry.get("source") or default_source or "").strip()
    if not source:
        raise ValueError("dedupe source must not be empty")
    if default_source and source != default_source:
        raise ValueError("dedupe source mismatch: %s" % source)
    identity_type = str(entry.get("identity_type") or "").strip()
    identity_value = normalize_dedupe_identity_value(identity_type, entry.get("identity_value"))
    if not identity_type or not identity_value:
        raise ValueError("dedupe identity must not be empty")
    return {
        "category": category,
        "source": source,
        "identity_type": identity_type,
        "identity_value": identity_value,
        "title": str(entry.get("title") or "").strip(),
        "path": str(entry.get("path") or "").strip(),
        "metadata": entry.get("metadata") or {},
    }


def normalize_dedupe_identity_value(identity_type, value):
    raw = str(value or "").strip()
    if not raw:
        return ""
    if identity_type == "info_hash":
        return raw.lower()
    if identity_type == "adult_code":
        return first_adult_code([raw]) or raw.upper()
    if identity_type == "normalized_title":
        return dedupe_title_identity(raw)
    if identity_type == "openlist_path":
        return raw
    return raw


def dedupe_title_identity(value):
    text = str(value or "").strip()
    if not text:
        return ""
    stem = posixpath.splitext(text)[0]
    normalized = normalize_openlist_text(stem)
    if len(normalized) < 4:
        return ""
    return normalized


def dedupe_source_label(source):
    if source == "openlist":
        return "OpenList基线"
    if source == "bot":
        return "Bot状态库"
    if source == "msg":
        return "MediaStationGo"
    return source or "重复索引"


def media_search_queries(title, task):
    values = []
    for value in (title, (task or {}).get("name"), (task or {}).get("file_name")):
        if value:
            values.append(str(value))
    raw_text = " ".join(values)

    candidates = []
    candidates.extend(sorted(extract_codes(raw_text)))
    candidates.extend(extract_title_fragments(raw_text))
    if (task or {}).get("file_id"):
        candidates.append(str((task or {}).get("file_id")))
    candidates.extend(values)

    seen = set()
    out = []
    for value in candidates:
        normalized = value.strip()
        key = normalized.lower()
        if normalized and key not in seen:
            seen.add(key)
            out.append(normalized)
    return out


def extract_title_fragments(value):
    text = str(value or "")
    fragments = []
    for match in re.finditer(r"[\[\(（【](.*?)[\]\)）】]", text):
        fragment = match.group(1).strip()
        if is_useful_title_fragment(fragment):
            fragments.append(fragment)
    return fragments


def is_useful_title_fragment(value):
    if not value:
        return False
    normalized = normalize_fragment(value)
    if not normalized:
        return False
    if normalized in {
        "4khdr",
        "hdr",
        "imax",
        "mkv",
        "mp4",
        "uhdbdrip",
        "webdl",
    }:
        return False
    if normalized in {"hevc10bit", "flac", "aac", "2160p", "1080p", "720p"}:
        return False
    if extract_codes(value):
        return True
    if re.search(r"[\u4e00-\u9fff]", value):
        return len(normalized) >= 2
    return False


def normalize_fragment(value):
    return re.sub(r"[^0-9A-Za-z\u4e00-\u9fff]+", "", str(value or "")).lower()


def media_display_title(media):
    if not isinstance(media, dict):
        return ""
    for key in ("title", "name", "original_title", "file_name", "filename"):
        value = media.get(key)
        if value:
            return str(value)
    nested = media.get("media")
    if isinstance(nested, dict):
        return media_display_title(nested)
    return ""


def clean_openlist_task_media(client, category_path, queries, task=None, max_bytes=DEFAULT_OPENLIST_PRE_SCAN_CLEAN_MAX_BYTES):
    target = find_openlist_task_target(client, category_path, queries, task=task)
    if target is None:
        raise RuntimeError("OpenList target not found for cleanup")

    target_dir, target_item = target
    groups = defaultdict(list)
    cleaned_count = 0
    cleaned_bytes = 0
    for dir_path, item in iter_openlist_files(client, target_dir, target_item):
        if should_keep_openlist_file(item, max_bytes=max_bytes):
            continue
        name = openlist_item_name(item)
        if not name:
            continue
        groups[dir_path].append(name)
        cleaned_count += 1
        cleaned_bytes += openlist_item_size(item)

    for dir_path, names in sorted(groups.items()):
        client.remove_names(dir_path, names)

    return {
        "openlist_clean_status": "success",
        "openlist_clean_target": openlist_item_path(target_dir, target_item),
        "openlist_cleaned_count": cleaned_count,
        "openlist_cleaned_bytes": cleaned_bytes,
        "openlist_cleaned_at": int(time.time()),
        "openlist_clean_error": None,
    }


def format_openlist_adult_code(client, category_path, queries, task=None):
    target = find_openlist_task_target(client, category_path, queries, task=task)
    if target is None:
        return {
            "openlist_adult_format_status": "skipped",
            "openlist_adult_format_reason": "target_not_found",
            "openlist_adult_formatted_at": int(time.time()),
        }

    target_dir, target_item = target
    code = first_adult_code(list(queries or []) + openlist_target_names(client, target_dir, target_item))
    if not code:
        return {
            "openlist_adult_format_status": "skipped",
            "openlist_adult_format_reason": "code_not_found",
            "openlist_adult_formatted_at": int(time.time()),
        }

    old_name = openlist_item_name(target_item)
    old_path = openlist_item_path(target_dir, target_item)
    if adult_code_prefix_matches(old_name, code):
        return {
            "openlist_adult_format_status": "skipped",
            "openlist_adult_format_reason": "already_formatted",
            "openlist_adult_code": code,
            "openlist_adult_format_path": old_path,
            "openlist_adult_formatted_at": int(time.time()),
        }

    new_name = adult_code_formatted_name(code, old_name)
    new_path = posixpath.join(str(target_dir).rstrip("/") or "/", new_name)
    client.rename_path(old_path, new_name)
    return {
        "openlist_adult_format_status": "success",
        "openlist_adult_code": code,
        "openlist_adult_format_old_path": old_path,
        "openlist_adult_format_new_path": new_path,
        "openlist_adult_formatted_at": int(time.time()),
        "openlist_adult_format_error": None,
    }


def openlist_target_names(client, target_dir, target_item):
    names = [openlist_item_name(target_item)]
    if openlist_item_is_dir(target_item):
        for _dir_path, item in iter_openlist_files(client, target_dir, target_item):
            names.append(openlist_item_name(item))
    return names


def first_adult_code(values):
    seen = set()
    for value in values:
        for code in iter_code_matches(value):
            key = code.lower()
            if key not in seen:
                seen.add(key)
                return code
    return None


def adult_code_prefix_matches(name, code):
    return normalize_openlist_text(name).startswith(normalize_openlist_text(code))


def adult_code_formatted_name(code, old_name):
    if not old_name:
        return code
    return "%s - %s" % (code, old_name)


def find_openlist_task_target(client, category_path, queries, task=None):
    items = client.list_all(category_path, refresh=False)
    scored = []
    for item in items:
        score = openlist_target_match_score(item, queries)
        if score > 0:
            scored.append((score, item))
    if not scored:
        return None

    scored.sort(key=lambda pair: pair[0], reverse=True)
    best_score, best_item = scored[0]
    if best_score < 500:
        return None
    if len(scored) > 1 and scored[1][0] == best_score:
        tied_items = [item for score, item in scored if score == best_score]
        disambiguated = disambiguate_openlist_target(client, category_path, tied_items, task)
        if disambiguated is not None:
            return category_path, disambiguated
        raise RuntimeError("OpenList target is ambiguous for cleanup")
    return category_path, best_item


def openlist_target_match_score(item, queries):
    name = openlist_item_name(item)
    if not name:
        return 0
    raw_name = name.strip().casefold()
    raw_stem = posixpath.splitext(name)[0].strip().casefold()
    normalized_name = normalize_openlist_text(name)
    normalized_stem = normalize_openlist_text(posixpath.splitext(name)[0])
    item_codes = extract_codes(name)
    score = 0
    for query in queries or []:
        raw_query = str(query or "").strip().casefold()
        if raw_query and raw_query == raw_name:
            score = max(score, 20000)
        elif raw_query and raw_query == raw_stem:
            score = max(score, 19000)
        normalized_query = normalize_openlist_text(query)
        if not normalized_query:
            continue
        if normalized_query == normalized_name:
            score = max(score, 10000)
        elif normalized_query == normalized_stem:
            score = max(score, 9000)
        elif len(normalized_query) >= 4 and normalized_query in normalized_name:
            score = max(score, 500)
        elif len(normalized_name) >= 4 and normalized_name in normalized_query:
            score = max(score, 500)
        if item_codes.intersection(extract_codes(query)):
            score = max(score, 1000)
    if score and openlist_item_is_dir(item):
        score += 10
    return score


def disambiguate_openlist_target(client, category_path, items, task=None):
    expected_size = task_size(task)
    if expected_size <= 0:
        return None
    scored = []
    for item in items:
        total_size = openlist_item_total_size(client, category_path, item)
        diff = abs(total_size - expected_size)
        scored.append((diff, item))
    scored.sort(key=lambda pair: pair[0])
    if not scored:
        return None
    best_diff, best_item = scored[0]
    if len(scored) > 1 and scored[1][0] == best_diff:
        return None
    tolerance = max(1024 * 1024, int(expected_size * 0.02))
    if best_diff <= tolerance:
        return best_item
    return None


def task_size(task):
    try:
        return int((task or {}).get("size") or 0)
    except (TypeError, ValueError):
        return 0


def openlist_item_total_size(client, dir_path, item):
    if not openlist_item_is_dir(item):
        return openlist_item_size(item)
    total = 0
    for _child_dir, child in iter_openlist_files(client, dir_path, item):
        total += openlist_item_size(child)
    return total


def iter_openlist_files(client, dir_path, item):
    if openlist_item_is_dir(item):
        child_dir = openlist_item_path(dir_path, item)
        for child in client.list_all(child_dir, refresh=False):
            yield from iter_openlist_files(client, child_dir, child)
        return
    yield dir_path, item


def should_keep_openlist_file(item, max_bytes=DEFAULT_OPENLIST_PRE_SCAN_CLEAN_MAX_BYTES):
    name = openlist_item_name(item)
    suffix = posixpath.splitext(name)[1].lower()
    if suffix in SUBTITLE_EXTENSIONS:
        return True
    if suffix in VIDEO_EXTENSIONS:
        if suspicious_openlist_video_name(name):
            return False
        if openlist_video_name_looks_like_episode(name):
            return True
        return openlist_item_size(item) >= int(max_bytes)
    return False


def openlist_video_name_looks_like_episode(name):
    stem = posixpath.splitext(openlist_item_name({"name": name}))[0]
    bare_stem = stem.strip(" \t\r\n[](){}【】（）")
    if re.fullmatch(r"\d{1,4}", bare_stem):
        return True
    if re.search(r"第\s*\d{1,4}\s*[集话話]", stem):
        return True
    if re.search(r"(?i)\bS\d{1,2}E\d{1,4}\b", stem):
        return True
    if re.search(r"(?i)\bEP\s*[-_.]?\s*\d{1,4}\b", stem):
        return True
    return False


def suspicious_openlist_video_name(name):
    stem = posixpath.splitext(openlist_item_name({"name": name}))[0]
    normalized = normalize_openlist_text(stem)
    suspicious_tokens = {
        "ad",
        "ads",
        "advert",
        "advertisement",
        "sample",
        "trailer",
        "preview",
        "promo",
        "readme",
        "宣傳",
        "宣传",
        "預告",
        "预告",
        "樣片",
        "样片",
        "廣告",
        "广告",
    }
    return normalized in suspicious_tokens


def is_openlist_video_file(item):
    if openlist_item_is_dir(item):
        return False
    return posixpath.splitext(openlist_item_name(item))[1].lower() in VIDEO_EXTENSIONS


def openlist_item_path(dir_path, item):
    name = openlist_item_name(item)
    if not name:
        return dir_path
    return posixpath.join(str(dir_path).rstrip("/") or "/", name)


def openlist_item_name(item):
    if not isinstance(item, dict):
        return ""
    return str(item.get("name") or item.get("file_name") or item.get("filename") or "").strip()


def openlist_item_is_dir(item):
    if not isinstance(item, dict):
        return False
    if item.get("is_dir") is not None:
        return bool(item.get("is_dir"))
    return item.get("type") == 1


def openlist_item_size(item):
    if not isinstance(item, dict):
        return 0
    try:
        return int(item.get("size") or item.get("size_bytes") or item.get("sizeBytes") or 0)
    except (TypeError, ValueError):
        return 0


def openlist_child_exists(client, path):
    normalized = posixpath.normpath(str(path or "").strip())
    if not normalized or normalized == ".":
        raise ValueError("OpenList path must not be empty")
    source_dir = posixpath.dirname(normalized.rstrip("/")) or "/"
    source_name = posixpath.basename(normalized.rstrip("/"))
    for item in client.list_all(source_dir, refresh=False):
        if item.get("name") == source_name:
            return True
    return False


def normalize_openlist_path(path):
    raw = str(path or "").strip().replace("\\", "/")
    if not raw:
        return ""
    if not raw.startswith("/"):
        raw = "/" + raw
    normalized = posixpath.normpath(raw)
    if normalized == ".":
        return ""
    return normalized


def replace_openlist_path_prefix(path, old_prefix, new_prefix):
    path = normalize_openlist_path(path)
    old_prefix = normalize_openlist_path(old_prefix).rstrip("/")
    new_prefix = normalize_openlist_path(new_prefix).rstrip("/")
    if path == old_prefix:
        return new_prefix
    if path.startswith(old_prefix + "/"):
        return new_prefix + path[len(old_prefix) :]
    raise ValueError("path does not start with source prefix: %s" % path)


def normalize_openlist_text(value):
    return re.sub(r"[^0-9A-Za-z\u4e00-\u9fff]+", "", str(value or "")).lower()


def parse_callback_data(value):
    action, sep, payload = (value or "").partition(":")
    if not sep:
        return None, None
    if action == "choose":
        return "choose", int(payload)
    if action == "back_search":
        return "back_search", int(payload)
    if action == "close_choice":
        return "close_choice", int(payload)
    if action == "close_search":
        return "close_search", int(payload)
    if action == "adult_search":
        return "adult_search", int(payload)
    if action == "anime_search":
        return "anime_search", int(payload)
    if action == "profile":
        profile, profile_sep, candidate_id = payload.partition(":")
        if profile_sep:
            return "profile", (profile, int(candidate_id))
    if action == "page":
        session_id, session_sep, page = payload.partition(":")
        if session_sep:
            return "page", (int(session_id), int(page))
    if action == "tasks_page":
        return "tasks_page", int(payload)
    if action == "submit":
        parts = payload.split(":")
        if len(parts) >= 2 and parts[0] in CATEGORY_LABELS:
            content_profile = parts[2] if len(parts) >= 3 and parts[2] else None
            return "submit", (parts[0], int(parts[1]), content_profile)
        return "choose", int(payload)
    if action == "force_submit":
        parts = payload.split(":")
        if len(parts) >= 2 and parts[0] in CATEGORY_LABELS:
            content_profile = parts[2] if len(parts) >= 3 and parts[2] else None
            return "force_submit", (parts[0], int(parts[1]), content_profile)
    if action in ("status", "cancel", "retry_msg") and payload:
        return action, payload
    if action == "migrate_select" and payload:
        return "migrate_select", int(payload)
    if action in ("migrate_to", "migrate_confirm"):
        target_category, sep, candidate_id = payload.partition(":")
        if sep and target_category in CATEGORY_LABELS:
            return action, (target_category, int(candidate_id))
    if action == "migrate_cancel" and payload:
        return "migrate_cancel", int(payload)
    if action in ("dedupe_refresh_confirm", "dedupe_refresh_cancel") and payload:
        return action, payload
    return None, None


def task_record_from_row(row):
    task = json.loads(row[5])
    if not task.get("info_hash"):
        task["info_hash"] = row[0]
    return {
        "info_hash": row[0],
        "user_id": row[1],
        "chat_id": row[2],
        "category": row[3],
        "title": row[4],
        "task": task,
        "created_at": row[6],
        "updated_at": row[7],
    }


def task_poll_count(task):
    try:
        return int((task or {}).get("poll_count") or 0)
    except (TypeError, ValueError):
        return 0


def task_telegram_status_message_id(task):
    return normalize_telegram_message_id((task or {}).get("telegram_status_message_id"))


def normalize_telegram_message_id(value):
    try:
        message_id = int(value or 0)
    except (TypeError, ValueError):
        return None
    return message_id if message_id > 0 else None


def telegram_message_id(response):
    if not isinstance(response, dict):
        return None
    result = response.get("result")
    if not isinstance(result, dict):
        return None
    return normalize_telegram_message_id(result.get("message_id"))


def task_last_polled_at(task):
    try:
        return int((task or {}).get("last_polled_at") or 0)
    except (TypeError, ValueError):
        return 0


def active_115_task_age_seconds(record, now):
    try:
        created_at = int(record.get("created_at") or now)
    except (TypeError, ValueError):
        created_at = int(now)
    return max(0, int(now) - created_at)


def active_115_task_in_fast_window(record, now):
    return active_115_task_age_seconds(record, now) < ACTIVE_115_FAST_POLL_WINDOW_SECONDS


def active_115_task_timed_out(record, now):
    return active_115_task_age_seconds(record, now) >= ACTIVE_115_TIMEOUT_SECONDS


def active_115_task_poll_due(record, now, normal_interval_seconds):
    task = record.get("task") or {}
    last_polled_at = task_last_polled_at(task)
    if last_polled_at <= 0:
        return True
    elapsed = int(now) - last_polled_at
    if active_115_task_in_fast_window(record, now):
        return elapsed >= ACTIVE_115_FAST_POLL_INTERVAL_SECONDS
    if task_poll_count(task) >= ACTIVE_115_SLOW_AFTER_POLLS:
        return elapsed >= ACTIVE_115_SLOW_POLL_INTERVAL_SECONDS
    return elapsed >= max(1, int(normal_interval_seconds))


def mark_active_115_task_polled(record, now):
    task = dict(record.get("task") or {})
    task["last_polled_at"] = int(now)
    if active_115_task_in_fast_window(record, now):
        task["poll_count"] = task_poll_count(task)
    else:
        task["poll_count"] = task_poll_count(task) + 1
    return task


def mark_task_auto_cancelled(task, now):
    task["auto_cancelled_at"] = int(now)
    task["auto_cancel_reason"] = "115离线任务超过%s秒未完成，已自动取消" % ACTIVE_115_TIMEOUT_SECONDS
    return task


def dedupe_entry_from_row(row):
    metadata = {}
    if row[6]:
        metadata = json.loads(row[6])
    return {
        "category": row[0],
        "source": row[1],
        "identity_type": row[2],
        "identity_value": row[3],
        "title": row[4],
        "path": row[5],
        "metadata": metadata,
        "created_at": row[7],
        "updated_at": row[8],
    }


def task_from_submit_result(result, info_hash):
    info_hash = str(info_hash or "").strip()
    task_status = result.get("task_status") or {}
    status_hash = str(task_status.get("info_hash") or "").strip()
    if status_hash.lower() == info_hash.lower() or (task_status and not status_hash and len(result.get("tasks") or []) == 1):
        out = dict(task_status)
        out["info_hash"] = info_hash
        return out
    for task in result.get("tasks") or []:
        if str(task.get("info_hash") or "").lower() == info_hash.lower():
            out = dict(task)
            out.setdefault("status_name", "submitted")
            return out
    return {"info_hash": info_hash, "status_name": "submitted"}


def format_size(value):
    if value is None:
        return "-"
    size = float(value)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if size < 1024 or unit == "TB":
            return "%.1f%s" % (size, unit)
        size = size / 1024


def download_uri_label(value):
    value = str(value or "")
    if value.lower().startswith("magnet:"):
        return "磁链"
    if is_prowlarr_download_uri(value):
        return "Prowlarr下载项"
    if value:
        return "下载链接"
    return "-"


def migration_source_kind_label(candidate):
    if (candidate or {}).get("source_kind") == "file":
        return "文件"
    return "目录"


def msg_sync_status_label(value):
    return {
        "success": "已完成",
        "running": "进行中",
        "failed": "失败",
        "skipped": "已跳过",
    }.get(value, value or "-")


def search_page_count(total):
    if total <= 0:
        return 1
    return (total + SEARCH_PAGE_SIZE - 1) // SEARCH_PAGE_SIZE


def normalize_page(page, page_count):
    if page < 0:
        return 0
    if page >= page_count:
        return page_count - 1
    return page


def summarize_submit(response):
    tasks = []
    data = response.get("data") or []
    if isinstance(data, list):
        for item in data:
            tasks.append(
                {
                    "info_hash": item.get("info_hash"),
                    "state": item.get("state"),
                    "code": item.get("code"),
                    "message": item.get("message") or item.get("msg"),
                }
            )
    return {
        "state": response.get("state"),
        "code": response.get("code"),
        "message": response.get("message") or response.get("msg"),
        "tasks": tasks,
    }


def access_token_invalid_response(response):
    if not isinstance(response, dict):
        return False
    text = " ".join(
        str(response.get(key) or "")
        for key in ("code", "message", "msg", "error", "errno")
    )
    return access_token_invalid_text(text)


def access_token_invalid_error(exc):
    return access_token_invalid_text(str(exc))


def access_token_invalid_text(text):
    value = (text or "").lower()
    if "access_token" not in value:
        return False
    return any(token in value for token in ("invalid", "无效", "失效", "过期", "expired"))


def format_search_page_message(query, candidates, page, page_count, total, title="搜索结果"):
    lines = ["%s：%s" % (title, query), "第 %s/%s 页，共 %s 条" % (page + 1, page_count, total)]
    for _candidate_id, candidate in candidates:
        rank = candidate.get("rank")
        lines.append("%s. %s" % (rank, candidate.get("title")))
        lines.append("站点：%s  做种：%s  大小：%s" % (candidate.get("indexer"), candidate.get("seeders"), format_size(candidate.get("size"))))
    return "\n".join(lines)


def format_library_choice_message(candidate):
    lines = ["已选择：%s" % candidate.get("title")]
    if candidate.get("rank"):
        lines.append("候选：#%s" % candidate.get("rank"))
    lines.append("站点：%s  做种：%s  大小：%s" % (candidate.get("indexer"), candidate.get("seeders"), format_size(candidate.get("size"))))
    lines.append("链接类型：%s" % download_uri_label(candidate.get("download_uri")))
    info_hash = candidate_info_hash(candidate)
    if info_hash:
        lines.append("info_hash：%s" % info_hash)
    return "\n".join(lines)


def format_duplicate_message(candidate, duplicate):
    if duplicate.get("level") == "strong":
        lines = ["重复入库拦截：%s" % candidate.get("title")]
    else:
        lines = ["可能重复入库：%s" % candidate.get("title")]
    lines.append("原因：%s" % duplicate_reason_label(duplicate))
    if duplicate.get("source"):
        lines.append("来源：%s" % duplicate.get("source"))
    if duplicate.get("title"):
        lines.append("已有作品：%s" % duplicate.get("title"))
    if duplicate.get("path"):
        lines.append("已有路径：%s" % duplicate.get("path"))
    if duplicate.get("status_name"):
        lines.append("已有状态：%s" % duplicate.get("status_name"))
    if duplicate.get("msg_sync_status"):
        lines.append("已有MSG同步：%s" % duplicate.get("msg_sync_status"))
    if duplicate.get("info_hash"):
        lines.append("已有info_hash：%s" % duplicate.get("info_hash"))
    if duplicate.get("media_id"):
        lines.append("已有媒体ID：%s" % duplicate.get("media_id"))
    if duplicate.get("msg_media_id"):
        lines.append("已有MSG媒体ID：%s" % duplicate.get("msg_media_id"))
    if duplicate_can_force(duplicate):
        lines.append("如确认这是更高质量或不同版本，可点击“仍然入库”。")
    else:
        lines.append("该重复项不建议再次离线。")
    return "\n".join(lines)


def duplicate_reason_label(duplicate):
    reason = duplicate.get("reason")
    if reason == "same_info_hash":
        return "相同info_hash"
    if reason == "adult_code":
        return "成人番号重复（%s）" % duplicate.get("code")
    if reason == "mediastation_code":
        return "MediaStationGo 已有相同番号"
    if reason == "mediastation_title":
        return "MediaStationGo 标题相似"
    if reason == "openlist_title":
        return "OpenList 标题相似"
    return reason or "重复作品"


def duplicate_can_force(duplicate):
    if "can_force" in duplicate:
        return bool(duplicate.get("can_force"))
    return duplicate.get("level") == "weak"


def format_dedupe_refresh_message(entries, count):
    category_counts = defaultdict(int)
    identity_counts = defaultdict(int)
    for entry in entries or []:
        category_counts[entry.get("category")] += 1
        identity_counts[entry.get("identity_type")] += 1
    lines = ["OpenList已入库记录刷新完成", "写入：%s" % count]
    lines.append("电影库：%s" % category_counts.get("movie", 0))
    lines.append("剧集库：%s" % category_counts.get("tv", 0))
    lines.append("成人库：%s" % category_counts.get("adult", 0))
    lines.append("其他库：%s" % category_counts.get("other", 0))
    lines.append("成人番号：%s" % identity_counts.get("adult_code", 0))
    lines.append("标题索引：%s" % identity_counts.get("normalized_title", 0))
    return "\n".join(lines)


def format_submit_message(candidate, result, category=None, content_profile=None):
    lines = ["已提交：%s" % candidate.get("title")]
    if category:
        lines.append("入库目录：%s" % CATEGORY_LABELS.get(category, category))
    if content_profile:
        lines.append("内容分类：%s" % CONTENT_PROFILE_LABELS.get(content_profile, content_profile))
    for task in result.get("tasks") or []:
        if task.get("info_hash"):
            lines.append("info_hash：%s" % task["info_hash"])
    task_status = result.get("task_status")
    if task_status:
        lines.append("当前状态：%s" % task_status.get("status_name"))
        if task_status.get("percent_done") is not None:
            lines.append("完成进度：%s" % task_status.get("percent_done"))
        if task_status.get("file_id"):
            lines.append("file_id：%s" % task_status.get("file_id"))
    if result.get("message"):
        lines.append("message：%s" % result["message"])
    return "\n".join(lines)


def format_task_status_message(title, task, category=None):
    lines = ["任务状态：%s" % title]
    append_task_lines(lines, task, category=category)
    return "\n".join(lines)


def format_cancel_result_message(title, result, category=None):
    task = result.get("task") or {}
    if result.get("cancelled"):
        lines = ["已取消任务：%s" % title]
    else:
        lines = ["任务未取消：%s" % title]
        if result.get("reason"):
            lines.append("原因：%s" % result["reason"])
    append_task_lines(lines, task, category=category)
    return "\n".join(lines)


def format_auto_cancel_result_message(title, result, task, category=None):
    if result.get("cancelled"):
        lines = ["任务超时已自动取消：%s" % title]
    else:
        lines = ["任务超时自动取消结果：%s" % title]
        if result.get("reason"):
            lines.append("原因：%s" % result["reason"])
    if (task or {}).get("auto_cancel_reason"):
        lines.append("超时规则：%s" % task.get("auto_cancel_reason"))
    append_task_lines(lines, task or {}, category=category)
    return "\n".join(lines)


def format_migration_search_message(query, candidates):
    lines = ["媒体迁移搜索：%s" % query, "请选择要迁移的媒体。"]
    for index, (_candidate_id, candidate) in enumerate(candidates, 1):
        lines.append("%s. %s" % (index, candidate.get("title") or "-"))
        lines.append(
            "当前：%s  类型：%s  数量：%s  大小：%s"
            % (
                CATEGORY_LABELS.get(candidate.get("category"), candidate.get("library_name") or "-"),
                migration_source_kind_label(candidate),
                candidate.get("media_count") or 0,
                format_size(candidate.get("total_size")),
            )
        )
        lines.append("路径：%s" % candidate.get("source_openlist_path"))
    return "\n".join(lines)


def format_migration_target_choice_message(candidate):
    lines = ["迁移媒体：%s" % (candidate.get("title") or "-")]
    lines.append("当前库：%s" % CATEGORY_LABELS.get(candidate.get("category"), candidate.get("library_name") or "-"))
    lines.append("媒体数量：%s" % (candidate.get("media_count") or 0))
    lines.append("源路径：%s" % candidate.get("source_openlist_path"))
    lines.append("请选择目标库。")
    return "\n".join(lines)


def format_migration_confirm_message(candidate, target_category, target):
    lines = ["确认迁移？"]
    lines.append("媒体：%s" % (candidate.get("title") or "-"))
    lines.append("源库：%s" % CATEGORY_LABELS.get(candidate.get("category"), candidate.get("library_name") or "-"))
    lines.append("目标库：%s" % CATEGORY_LABELS.get(target_category, target_category))
    lines.append("媒体数量：%s" % (candidate.get("media_count") or 0))
    lines.append("源路径：%s" % candidate.get("source_openlist_path"))
    lines.append("目标路径：%s" % target.get("target_openlist_path"))
    lines.append("将移动 OpenList/115 路径并更新 MSG 数据库；不会重新扫描或重新刮削。")
    return "\n".join(lines)


def format_migration_running_message(candidate, target_category):
    return "正在迁移：%s -> %s" % (
        candidate.get("source_openlist_path"),
        CATEGORY_LABELS.get(target_category, target_category),
    )


def format_migration_result_message(candidate, result):
    lines = ["迁移完成：%s" % (candidate.get("title") or "-")]
    lines.append("源路径：%s" % result.get("source_openlist_path"))
    lines.append("目标路径：%s" % result.get("target_openlist_path"))
    lines.append("目标库：%s" % CATEGORY_LABELS.get(result.get("target_category"), result.get("target_category")))
    lines.append("MSG媒体记录：%s" % (result.get("media_count") or 0))
    if result.get("series_count"):
        lines.append("剧集记录：%s" % result.get("series_count"))
    return "\n".join(lines)


def format_task_list_message(records, page=0, page_count=1, total=None):
    if total is None:
        total = len(records)
    lines = ["最近任务：第 %s/%s 页，共 %s 条" % (page + 1, page_count, total)]
    start_index = page * DEFAULT_TASK_LIST_PAGE_SIZE + 1
    for idx, record in enumerate(records, 1):
        task = record["task"]
        title = record["title"] or task.get("name") or task.get("info_hash")
        display_index = start_index + idx - 1
        lines.append("%s. %s" % (display_index, title))
        lines.append(
            "入库：%s  状态：%s  进度：%s"
            % (
                CATEGORY_LABELS.get(record.get("category"), record.get("category") or "-"),
                task.get("status_name") or "-",
                format_percent(task.get("percent_done")),
            )
        )
        if task.get("content_profile"):
            lines.append("内容：%s" % CONTENT_PROFILE_LABELS.get(task.get("content_profile"), task.get("content_profile")))
        if task.get("msg_sync_status"):
            lines.append("MSG：%s" % msg_sync_status_label(task.get("msg_sync_status")))
        if task.get("info_hash"):
            lines.append("info_hash：%s" % task["info_hash"])
    return "\n".join(lines)


def append_task_lines(lines, task, category=None):
    if task.get("info_hash"):
        lines.append("info_hash：%s" % task["info_hash"])
    lines.append("当前状态：%s" % (task.get("status_name") or "-"))
    if task.get("percent_done") is not None:
        lines.append("完成进度：%s" % format_percent(task.get("percent_done")))
    if task.get("file_id"):
        lines.append("file_id：%s" % task.get("file_id"))
    if task.get("wp_path_id"):
        lines.append("wp_path_id：%s" % task.get("wp_path_id"))
    if task.get("content_profile"):
        lines.append("内容分类：%s" % CONTENT_PROFILE_LABELS.get(task.get("content_profile"), task.get("content_profile")))
    if task.get("msg_sync_status"):
        if task.get("msg_sync_status") == "success":
            lines.append("MSG同步：已完成")
            if task.get("msg_media_id"):
                lines.append("MSG媒体ID：%s" % task.get("msg_media_id"))
        elif task.get("msg_sync_status") == "running":
            lines.append("MSG同步：进行中")
        else:
            lines.append("MSG同步：失败")
            if task.get("msg_error"):
                lines.append("MSG错误：%s" % task.get("msg_error"))
    if task.get("openlist_clean_status") and task.get("openlist_clean_status") != "skipped":
        if task.get("openlist_clean_status") == "success":
            lines.append("OpenList清理：已完成（%s 个）" % (task.get("openlist_cleaned_count") or 0))
        elif task.get("openlist_clean_status") == "running":
            lines.append("OpenList清理：进行中")
        else:
            lines.append("OpenList清理：失败")
            if task.get("openlist_clean_error"):
                lines.append("OpenList错误：%s" % task.get("openlist_clean_error"))
            lines.append("OpenList处理：请手动进入目标目录检查并删除广告/样片等无效小文件，然后点击重试MSG同步")
    if category == "adult" and task.get("openlist_adult_format_status") and task.get("openlist_adult_format_status") != "skipped":
        if task.get("openlist_adult_format_status") == "success":
            lines.append("番号格式化：已完成（%s）" % task.get("openlist_adult_code"))
        elif task.get("openlist_adult_format_status") == "running":
            lines.append("番号格式化：进行中")
        else:
            lines.append("番号格式化：失败")
            if task.get("openlist_adult_format_error"):
                lines.append("番号错误：%s" % task.get("openlist_adult_format_error"))
            lines.append("番号处理：请手动将目录重命名为“标准番号 - 原名称”，然后点击重试MSG同步")
    if task.get("msg_scan_status"):
        if task.get("msg_scan_status") == "success":
            lines.append("MSG扫描：已完成")
        elif task.get("msg_scan_status") == "running":
            lines.append("MSG扫描：进行中")
        else:
            lines.append("MSG扫描：失败")
    if task.get("msg_scrape_status"):
        if task.get("msg_scrape_status") == "success":
            lines.append("MSG刮削：已完成")
        elif task.get("msg_scrape_status") == "running":
            lines.append("MSG刮削：进行中")
        else:
            lines.append("MSG刮削：失败")
    if category == "adult" and task.get("msg_artwork_repair_status"):
        if task.get("msg_artwork_repair_status") == "success":
            lines.append("成人图片修复：已完成（%s 项）" % (task.get("msg_artwork_repair_updated") or 0))
        elif task.get("msg_artwork_repair_status") == "running":
            lines.append("成人图片修复：进行中")
        elif task.get("msg_artwork_repair_reason") == "replacement_not_found":
            lines.append("成人图片修复：未完成（未找到可直连替代图源）")
        elif task.get("msg_artwork_repair_status") != "skipped":
            lines.append("成人图片修复：失败")
            if task.get("msg_artwork_repair_error"):
                lines.append("图片修复错误：%s" % task.get("msg_artwork_repair_error"))


def format_percent(value):
    if value is None:
        return "-"
    return str(value)


def submit_reply_markup(result):
    for task in result.get("tasks") or []:
        if task.get("info_hash"):
            return task_reply_markup(task_from_submit_result(result, task["info_hash"]))
    task_status = result.get("task_status")
    if task_status:
        return task_reply_markup(task_status)
    return None


def search_page_reply_markup(session_id, candidates, page, page_count, allow_adult_retry=False, allow_anime_retry=False):
    rows = []
    for candidate_id, candidate in candidates:
        rows.append([{"text": "#%s 入库" % candidate.get("rank"), "callback_data": "choose:%s" % candidate_id}])
    nav = []
    if page > 0:
        nav.append({"text": "上一页", "callback_data": "page:%s:%s" % (session_id, page - 1)})
    if page + 1 < page_count:
        nav.append({"text": "下一页", "callback_data": "page:%s:%s" % (session_id, page + 1)})
    if nav:
        rows.append(nav)
    page_jump = search_page_jump_buttons(session_id, page, page_count)
    if page_jump:
        rows.append(page_jump)
    retry = []
    if allow_adult_retry:
        retry.append({"text": "🔞", "callback_data": "adult_search:%s" % session_id})
    if allow_anime_retry:
        retry.append({"text": "动漫", "callback_data": "anime_search:%s" % session_id})
    if retry:
        rows.append(retry)
    rows.append([{"text": "关闭", "callback_data": "close_search:%s" % session_id}])
    return {"inline_keyboard": rows}


def search_page_jump_buttons(session_id, page, page_count):
    if page_count <= 1:
        return []

    candidates = {0, page_count - 1}
    for offset in (-1, 0, 1):
        target = page + offset
        if 0 <= target < page_count:
            candidates.add(target)
    if page <= 2:
        candidates.update(range(0, min(5, page_count)))
    if page >= page_count - 3:
        candidates.update(range(max(0, page_count - 5), page_count))

    row = []
    previous = None
    for target in sorted(candidates):
        if previous is not None and target - previous > 1:
            row.append({"text": "...", "callback_data": "page:%s:%s" % (session_id, page)})
        text = str(target + 1)
        if target == page:
            text = "[%s]" % text
        row.append({"text": text, "callback_data": "page:%s:%s" % (session_id, target)})
        previous = target
    return row


def library_choice_reply_markup(candidate_id, include_back=False):
    rows = [
        [
            {"text": "电影", "callback_data": "profile:movie:%s" % candidate_id},
            {"text": "剧集", "callback_data": "profile:tv:%s" % candidate_id},
            {"text": "动漫", "callback_data": "profile:anime:%s" % candidate_id},
        ],
        [
            {"text": "成人", "callback_data": "profile:adult:%s" % candidate_id},
            {"text": "其他", "callback_data": "profile:other:%s" % candidate_id},
        ],
    ]
    if include_back:
        rows.append([{"text": "返回结果", "callback_data": "close_choice:%s" % candidate_id}])
    return {"inline_keyboard": rows}


def migration_search_reply_markup(candidates):
    rows = []
    for index, (candidate_id, _candidate) in enumerate(candidates, 1):
        rows.append([{"text": "迁移 %s" % index, "callback_data": "migrate_select:%s" % candidate_id}])
    return {"inline_keyboard": rows}


def migration_target_choice_reply_markup(candidate_id, candidate):
    rows = []
    current = candidate.get("category")
    first = []
    for category in ("movie", "tv", "anime"):
        if category != current:
            first.append({"text": CATEGORY_LABELS[category].replace("库", ""), "callback_data": "migrate_to:%s:%s" % (category, candidate_id)})
    if first:
        rows.append(first)
    second = []
    for category in ("adult", "other"):
        if category != current:
            second.append({"text": CATEGORY_LABELS[category].replace("库", ""), "callback_data": "migrate_to:%s:%s" % (category, candidate_id)})
    if second:
        rows.append(second)
    rows.append([{"text": "取消", "callback_data": "migrate_cancel:%s" % candidate_id}])
    return {"inline_keyboard": rows}


def migration_confirm_reply_markup(candidate_id, target_category):
    return {
        "inline_keyboard": [
            [
                {"text": "确认迁移", "callback_data": "migrate_confirm:%s:%s" % (target_category, candidate_id)},
                {"text": "取消", "callback_data": "migrate_cancel:%s" % candidate_id},
            ]
        ]
    }


def dedupe_refresh_confirm_reply_markup():
    return {
        "inline_keyboard": [
            [
                {"text": "确认刷新", "callback_data": "dedupe_refresh_confirm:1"},
                {"text": "取消", "callback_data": "dedupe_refresh_cancel:1"},
            ]
        ]
    }


def duplicate_reply_markup(duplicate, category, candidate_id, content_profile=None):
    rows = []
    if duplicate.get("info_hash"):
        rows.append([{"text": "查看已有任务", "callback_data": "status:%s" % duplicate.get("info_hash")}])
    if duplicate_can_force(duplicate):
        callback_data = "force_submit:%s:%s" % (category, candidate_id)
        if content_profile:
            callback_data = "%s:%s" % (callback_data, content_profile)
        rows.append([{"text": "仍然入库", "callback_data": callback_data}])
    return {"inline_keyboard": rows}


def task_reply_markup(task):
    info_hash = (task or {}).get("info_hash")
    if task_can_retry_msg_sync(task):
        return {"inline_keyboard": [[{"text": "重试MSG同步", "callback_data": "retry_msg:%s" % info_hash}]]}
    if not info_hash or task_is_final(task):
        return None
    row = [{"text": "刷新进度", "callback_data": "status:%s" % info_hash}]
    if task_can_cancel(task):
        row.append({"text": "取消任务", "callback_data": "cancel:%s" % info_hash})
    return {"inline_keyboard": [row]}


def callback_task_reply_markup(task):
    return task_reply_markup(task) or {"inline_keyboard": []}


def task_is_final(task):
    return (task or {}).get("status_name") in FINAL_TASK_STATUS_NAMES


def task_page_count(total):
    if total <= 0:
        return 1
    return (total + DEFAULT_TASK_LIST_PAGE_SIZE - 1) // DEFAULT_TASK_LIST_PAGE_SIZE


def task_list_priority(record):
    task = (record or {}).get("task") or {}
    status = task.get("status_name")
    if task_can_retry_msg_sync(task):
        return 0
    if status not in FINAL_TASK_STATUS_NAMES:
        return 1
    if status in {"failed", "cancelled"}:
        return 2
    return 3


def prioritized_task_records(records):
    def sort_key(record):
        return (
            task_list_priority(record),
            -int(record.get("updated_at") or 0),
            -int(record.get("created_at") or 0),
        )

    return sorted(records or [], key=sort_key)


def task_list_reply_markup(records, page=0, page_count=1):
    rows = []
    for idx, record in enumerate(records, 1):
        task = record["task"]
        info_hash = task.get("info_hash") or record["info_hash"]
        display_index = page * DEFAULT_TASK_LIST_PAGE_SIZE + idx
        if task_can_retry_msg_sync(task):
            rows.append([{"text": "重试MSG %s" % display_index, "callback_data": "retry_msg:%s" % info_hash}])
            continue
        if task_is_final(task):
            continue
        row = [{"text": "刷新 %s" % display_index, "callback_data": "status:%s" % info_hash}]
        if task_can_cancel(task):
            row.append({"text": "取消 %s" % display_index, "callback_data": "cancel:%s" % info_hash})
        rows.append(row)
    nav = []
    if page > 0:
        nav.append({"text": "上一页", "callback_data": "tasks_page:%s" % (page - 1)})
    if page + 1 < page_count:
        nav.append({"text": "下一页", "callback_data": "tasks_page:%s" % (page + 1)})
    if nav:
        rows.append(nav)
    if not rows:
        return None
    return {"inline_keyboard": rows}


def first_info_hash(result):
    for task in result.get("tasks") or []:
        if task.get("info_hash"):
            return task["info_hash"]
    return None


def build_bot(config=None):
    config = config or BotConfig.from_env()
    telegram = TelegramApi(config.token, timeout=config.telegram_timeout)
    store = CandidateStore(config.state_db_path)
    service = PipelineBotService(config)
    return TelegramBot(config, telegram, store, service)
