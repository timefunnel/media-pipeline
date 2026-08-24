import hashlib
import hmac
import json
import re
import sqlite3
import threading
import time
import traceback
import urllib.parse
import uuid
from collections import defaultdict
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from pipeline.config import category_to_openlist_path
from pipeline.client115 import parse_115_share_url
from pipeline.dedupe import candidate_info_hash
from pipeline.openlist_utils import normalize_openlist_path
from pipeline.search import (
    ed2k_candidate_from_text,
    magnet_candidate_from_text,
    share115_candidate_from_text,
    valid_btih_info_hash,
)
from pipeline.season_subtitles import SeasonSubtitleTaskManager
from pipeline.external_subtitles import (
    subtitle_application_key,
    subtitle_candidate_application_status,
    subtitle_candidate_preview_status,
)
from pipeline.subtitle_asr import DEFAULT_ASR_MODEL, SubtitleAsrProcessor
from pipeline.telegram_ui import task_from_submit_result
from pipeline.task_state import ACTIVE_115_TIMEOUT_SECONDS


DEFAULT_API_HOST = "127.0.0.1"
DEFAULT_SEARCH_TTL_SECONDS = 15 * 60
DEFAULT_OFFLINE_WAIT_SLICE_SECONDS = 5 * 60
SUBSCRIPTION_SCAN_VISIBILITY_RETRY_DELAY_SECONDS = 2.0
SUBSCRIPTION_SCAN_VISIBILITY_RETRY_LIMIT = 1
MAX_JSON_BODY_BYTES = 1024 * 1024
OFFLINE_ACTIVE_STATUSES = {"submitted", "allocating", "downloading", "unknown", None, ""}
OFFLINE_SUCCESS_STATUS = "success"
OFFLINE_FAILED_STATUSES = {"failed", "cancelled", "canceled"}
FINAL_IMPORT_STATUSES = {"completed", "completed_with_warning", "failed", "canceled"}
RETRYABLE_IMPORT_STATUSES = {"completed_with_warning", "failed", "canceled"}
VALID_IMPORT_STATUSES = {"queued", "running", *FINAL_IMPORT_STATUSES}
FINAL_SUBTITLE_ASR_STATUSES = {"completed", "failed", "canceled"}
VALID_SUBTITLE_ASR_STATUSES = {"queued", "running", *FINAL_SUBTITLE_ASR_STATUSES}
VALID_SEARCH_SOURCES = {"default", "pansou", "bt4g"}
VALID_CATEGORIES = {"movie", "tv", "anime", "adult", "other"}
SYNC_STAGES = {
    "staging",
    "received_unclaimed",
    "verifying_staging",
    "promoting",
    "syncing",
    "scanning",
    "verifying_scan",
    "scraping",
    "subtitles",
    "removing_old_version",
    "cleanup",
}
RETRY_RESUME_STAGES = {"syncing", "scanning", "scraping", "subtitles", "post_enhancement", "removing_old_version"}
RETRY_STAGE_BY_FAILED_TASK_STATUS = (
    ("openlist_adult_format_status", "syncing"),
    ("openlist_trash_hide_status", "syncing"),
    ("openlist_clean_status", "syncing"),
    ("openlist_adult_extra_hide_status", "syncing"),
    ("msg_scan_status", "scanning"),
    ("msg_scrape_status", "scraping"),
    ("msg_extra_cleanup_status", "syncing"),
    ("msg_visibility_repair_status", "syncing"),
    ("subtitle_match_status", "subtitles"),
    ("post_enhancement_status", "post_enhancement"),
)


class ApiError(RuntimeError):
    def __init__(self, status, code, message, details=None):
        super().__init__(message)
        self.status = int(status)
        self.code = str(code)
        self.message = str(message)
        self.details = dict(details or {})


class WorkerStopping(RuntimeError):
    pass


class OfflineWaitDeferred(RuntimeError):
    pass


class ImportCanceled(RuntimeError):
    pass


class InternalApiStore:
    def __init__(self, db_path, search_ttl_seconds=DEFAULT_SEARCH_TTL_SECONDS):
        self.db_path = str(db_path)
        self.search_ttl_seconds = max(1, int(search_ttl_seconds))
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    def _connect(self):
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("pragma busy_timeout = 30000")
        conn.execute("pragma foreign_keys = on")
        return conn

    def _init_schema(self):
        conn = self._connect()
        try:
            conn.execute("pragma journal_mode = wal")
            conn.execute(
                """
                create table if not exists internal_api_search_sessions (
                    id text primary key,
                    owner_id text not null,
                    query text not null,
                    category text not null,
                    source text not null,
                    metadata_json text not null,
                    created_at integer not null,
                    expires_at integer not null
                )
                """
            )
            ensure_sqlite_column(conn, "internal_api_search_sessions", "expires_at", "integer not null default 0")
            conn.execute(
                """
                create table if not exists internal_api_search_candidates (
                    id text primary key,
                    session_id text not null,
                    owner_id text not null,
                    candidate_json text not null,
                    created_at integer not null,
                    foreign key(session_id) references internal_api_search_sessions(id) on delete cascade
                )
                """
            )
            conn.execute(
                """
                create index if not exists idx_internal_api_search_candidates_session
                on internal_api_search_candidates(session_id, owner_id)
                """
            )
            conn.execute(
                """
                create index if not exists idx_internal_api_search_sessions_expiry
                on internal_api_search_sessions(expires_at)
                """
            )
            conn.execute(
                """
                create table if not exists internal_api_imports (
                    id text primary key,
                    owner_id text not null,
                    idempotency_key text not null,
                    status text not null,
                    stage text not null,
                    request_json text not null,
                    result_json text,
                    error text,
                    info_hash text,
                    msg_media_id text,
                    cancel_requested integer not null default 0,
                    attempt_count integer not null default 0,
                    created_at integer not null,
                    updated_at integer not null,
                    started_at integer,
                    completed_at integer,
                    unique(owner_id, idempotency_key)
                )
                """
            )
            conn.execute(
                """
                create index if not exists idx_internal_api_imports_queue
                on internal_api_imports(status, created_at)
                """
            )
            conn.execute(
                """
                create index if not exists idx_internal_api_imports_owner
                on internal_api_imports(owner_id, updated_at)
                """
            )
            conn.execute(
                """
                create table if not exists internal_api_subscription_source_blocks (
                    source_key text primary key,
                    reason text not null,
                    origin_import_id text not null,
                    created_at integer not null
                )
                """
            )
            migrate_subscription_source_block_aliases(conn)
            conn.execute(
                """
                create table if not exists internal_api_subtitle_asr_tasks (
                    id text primary key,
                    owner_id text not null,
                    media_id text not null,
                    source_language text not null,
                    status text not null,
                    stage text not null,
                    progress_current integer not null default 0,
                    progress_total integer not null default 0,
                    result_json text,
                    error text,
                    attempt_count integer not null default 0,
                    created_at integer not null,
                    updated_at integer not null,
                    started_at integer,
                    completed_at integer
                )
                """
            )
            ensure_sqlite_column(
                conn,
                "internal_api_subtitle_asr_tasks",
                "asr_model",
                "text not null default 'FunAudioLLM/SenseVoiceSmall'",
            )
            ensure_sqlite_column(
                conn,
                "internal_api_subtitle_asr_tasks",
                "translation_provider",
                "text not null default 'local'",
            )
            ensure_sqlite_column(
                conn,
                "internal_api_subtitle_asr_tasks",
                "translation_model",
                "text not null default ''",
            )
            ensure_sqlite_column(
                conn,
                "internal_api_subtitle_asr_tasks",
                "cached_audio",
                "integer not null default 0",
            )
            ensure_sqlite_column(
                conn,
                "internal_api_subtitle_asr_tasks",
                "cached_transcript",
                "integer not null default 0",
            )
            conn.execute(
                """
                create index if not exists idx_internal_api_subtitle_asr_tasks_queue
                on internal_api_subtitle_asr_tasks(status, created_at)
                """
            )
            conn.execute(
                """
                create index if not exists idx_internal_api_subtitle_asr_tasks_owner
                on internal_api_subtitle_asr_tasks(owner_id, media_id, updated_at)
                """
            )
            conn.commit()
        finally:
            conn.close()

    def recover_running_imports(self):
        now = int(time.time())
        conn = self._connect()
        try:
            cursor = conn.execute(
                """
                update internal_api_imports
                set status = 'queued', stage = 'queued',
                    updated_at = ?, started_at = null, completed_at = null
                where status = 'running'
                """,
                (now,),
            )
            conn.commit()
            return int(cursor.rowcount or 0)
        finally:
            conn.close()

    def save_search(self, owner_id, query, category, source, items, metadata):
        session_id = uuid.uuid4().hex
        now = int(time.time())
        expires_at = now + self.search_ttl_seconds
        stored_items = []
        conn = self._connect()
        try:
            conn.execute("begin immediate")
            conn.execute("delete from internal_api_search_sessions where expires_at <= ?", (now,))
            conn.execute(
                """
                insert into internal_api_search_sessions
                    (id, owner_id, query, category, source, metadata_json, created_at, expires_at)
                values (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (session_id, owner_id, query, category, source, json_dumps(metadata), now, expires_at),
            )
            for item in items:
                candidate_id = uuid.uuid4().hex
                candidate = dict(item)
                conn.execute(
                    """
                    insert into internal_api_search_candidates
                        (id, session_id, owner_id, candidate_json, created_at)
                    values (?, ?, ?, ?, ?)
                    """,
                    (candidate_id, session_id, owner_id, json_dumps(candidate), now),
                )
                returned = dict(candidate)
                returned["candidate_id"] = candidate_id
                stored_items.append(returned)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
        return session_id, expires_at, stored_items

    def load_search_candidate(self, owner_id, session_id, candidate_id):
        conn = self._connect()
        try:
            conn.execute("begin immediate")
            session = conn.execute(
                """
                select * from internal_api_search_sessions
                where id = ? and owner_id = ?
                """,
                (session_id, owner_id),
            ).fetchone()
            if session is None:
                conn.commit()
                raise ApiError(404, "search_session_not_found", "search session not found")
            now = int(time.time())
            if int(session["expires_at"] or 0) <= now:
                conn.execute("delete from internal_api_search_sessions where id = ?", (session_id,))
                conn.commit()
                raise ApiError(410, "search_session_expired", "search session expired")
            row = conn.execute(
                """
                select s.id as session_id, s.query, s.category, s.source, c.id as candidate_id, c.candidate_json
                from internal_api_search_candidates c
                join internal_api_search_sessions s on s.id = c.session_id
                where s.id = ? and c.id = ? and s.owner_id = ? and c.owner_id = ?
                """,
                (session_id, candidate_id, owner_id, owner_id),
            ).fetchone()
            conn.execute("delete from internal_api_search_sessions where expires_at <= ?", (now,))
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
        if row is None:
            raise ApiError(404, "candidate_not_found", "search candidate not found")
        return {
            "session_id": row["session_id"],
            "candidate_id": row["candidate_id"],
            "query": row["query"],
            "category": row["category"],
            "source": row["source"],
            "candidate": json.loads(row["candidate_json"]),
        }

    def create_import(self, owner_id, idempotency_key, request):
        import_id = uuid.uuid4().hex
        request_json = json_dumps(request)
        now = int(time.time())
        conn = self._connect()
        try:
            conn.execute("begin immediate")
            existing = conn.execute(
                """
                select * from internal_api_imports
                where owner_id = ? and idempotency_key = ?
                """,
                (owner_id, idempotency_key),
            ).fetchone()
            if existing is not None:
                if existing["request_json"] != request_json:
                    raise ApiError(409, "idempotency_conflict", "Idempotency-Key was already used with a different request")
                conn.commit()
                return import_row(existing), False
            conn.execute(
                """
                insert into internal_api_imports
                    (id, owner_id, idempotency_key, status, stage, request_json,
                     created_at, updated_at)
                values (?, ?, ?, 'queued', 'queued', ?, ?, ?)
                """,
                (import_id, owner_id, idempotency_key, request_json, now, now),
            )
            row = conn.execute("select * from internal_api_imports where id = ?", (import_id,)).fetchone()
            conn.commit()
            return import_row(row), True
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def get_import_by_idempotency(self, owner_id, idempotency_key):
        conn = self._connect()
        try:
            row = conn.execute(
                """
                select * from internal_api_imports
                where owner_id = ? and idempotency_key = ?
                """,
                (owner_id, idempotency_key),
            ).fetchone()
        finally:
            conn.close()
        return import_row(row) if row is not None else None

    def get_import(self, owner_id, import_id):
        conn = self._connect()
        try:
            row = conn.execute(
                "select * from internal_api_imports where id = ? and owner_id = ?",
                (import_id, owner_id),
            ).fetchone()
        finally:
            conn.close()
        if row is None:
            raise ApiError(404, "import_not_found", "import task not found")
        return import_row(row)

    def list_pending_post_enhancements(self):
        conn = self._connect()
        try:
            rows = conn.execute(
                """
                select * from internal_api_imports
                where status in ('completed', 'completed_with_warning')
                  and result_json like '%post_enhancement_status%'
                order by updated_at asc, id asc
                """
            ).fetchall()
        finally:
            conn.close()
        pending = []
        for row in rows:
            task = import_row(row)
            task_result = (task.get("result") or {}).get("task") or {}
            if post_enhancement_needs_run(task_result):
                pending.append(task)
        return pending

    def update_completed_result(self, import_id, result):
        now = int(time.time())
        conn = self._connect()
        try:
            cursor = conn.execute(
                """
                update internal_api_imports
                set result_json = ?, updated_at = ?
                where id = ? and status in ('completed', 'completed_with_warning')
                """,
                (json_dumps(result), now, import_id),
            )
            conn.commit()
        finally:
            conn.close()
        if cursor.rowcount != 1:
            raise RuntimeError("completed import enhancement target not found: %s" % import_id)

    def finish_post_enhancement_warning(self, import_id, result, error):
        now = int(time.time())
        conn = self._connect()
        try:
            cursor = conn.execute(
                """
                update internal_api_imports
                set status = 'completed_with_warning', stage = 'completed_with_warning',
                    result_json = ?, error = ?, updated_at = ?, completed_at = ?
                where id = ? and status in ('completed', 'completed_with_warning')
                """,
                (json_dumps(result), str(error or "post-ingest enhancement failed"), now, now, import_id),
            )
            conn.commit()
        finally:
            conn.close()
        if cursor.rowcount != 1:
            raise RuntimeError("post-ingest enhancement warning target not found: %s" % import_id)

    def finish_post_enhancement_success(self, import_id, result):
        now = int(time.time())
        task = dict((result or {}).get("task") or {})
        warnings = [str(value).strip() for value in (result or {}).get("warnings") or [] if str(value).strip()]
        warnings.extend(str(value).strip() for value in task.get("warnings") or [] if str(value).strip())
        if warnings:
            status = "completed_with_warning"
            stage = "completed_with_warning"
            error = "; ".join(warnings)
        else:
            status = "completed"
            stage = "completed"
            error = None
        conn = self._connect()
        try:
            cursor = conn.execute(
                """
                update internal_api_imports
                set status = ?, stage = ?, result_json = ?, error = ?,
                    updated_at = ?, completed_at = ?
                where id = ? and status in ('completed', 'completed_with_warning')
                """,
                (status, stage, json_dumps(result), error, now, now, import_id),
            )
            conn.commit()
        finally:
            conn.close()
        if cursor.rowcount != 1:
            raise RuntimeError("post-ingest enhancement success target not found: %s" % import_id)

    def get_subscription_source_block(self, source_key):
        conn = self._connect()
        try:
            row = conn.execute(
                "select * from internal_api_subscription_source_blocks where source_key = ?",
                (str(source_key or "").strip(),),
            ).fetchone()
        finally:
            conn.close()
        return dict(row) if row is not None else None

    def block_subscription_source(self, source_key, reason, origin_import_id):
        source_key = str(source_key or "").strip()
        if not source_key:
            raise ValueError("subscription source key missing")
        now = int(time.time())
        conn = self._connect()
        try:
            conn.execute("begin immediate")
            conn.execute(
                """
                insert into internal_api_subscription_source_blocks
                    (source_key, reason, origin_import_id, created_at)
                values (?, ?, ?, ?)
                on conflict(source_key) do nothing
                """,
                (source_key, str(reason or "").strip() or "blocked", str(origin_import_id or "").strip(), now),
            )
            row = conn.execute(
                "select * from internal_api_subscription_source_blocks where source_key = ?", (source_key,)
            ).fetchone()
            conn.commit()
            return dict(row)
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def claim_next_import(self, blocked_owners):
        conn = self._connect()
        try:
            conn.execute("begin immediate")
            rows = conn.execute(
                """
                select * from internal_api_imports
                where status = 'queued'
                order by
                    case when stage = 'waiting_download' then 1 else 0 end,
                    case when stage = 'waiting_download' then updated_at else created_at end,
                    id
                """
            ).fetchall()
            row = next((item for item in rows if item["owner_id"] not in blocked_owners), None)
            if row is None:
                conn.commit()
                return None
            now = int(time.time())
            stage = row["stage"] if row["stage"] in RETRY_RESUME_STAGES else "starting"
            cursor = conn.execute(
                """
                update internal_api_imports
                set status = 'running', stage = ?, attempt_count = attempt_count + 1,
                    started_at = ?, completed_at = null, updated_at = ?, error = null
                where id = ? and status = 'queued'
                """,
                (stage, now, now, row["id"]),
            )
            if cursor.rowcount != 1:
                conn.commit()
                return None
            claimed = conn.execute("select * from internal_api_imports where id = ?", (row["id"],)).fetchone()
            conn.commit()
            return import_row(claimed)
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def save_running(self, import_id, stage, result=None, info_hash=None, msg_media_id=None):
        now = int(time.time())
        conn = self._connect()
        try:
            conn.execute(
                """
                update internal_api_imports
                set stage = ?, result_json = ?, info_hash = coalesce(?, info_hash),
                    msg_media_id = coalesce(?, msg_media_id), updated_at = ?
                where id = ? and status = 'running'
                """,
                (
                    stage,
                    json_dumps(result) if result is not None else None,
                    nonempty_or_none(info_hash),
                    nonempty_or_none(msg_media_id),
                    now,
                    import_id,
                ),
            )
            conn.commit()
        finally:
            conn.close()

    def finish_import(self, import_id, status, stage, result=None, error=None, info_hash=None, msg_media_id=None):
        if status not in FINAL_IMPORT_STATUSES:
            raise ValueError("invalid final import status: %s" % status)
        now = int(time.time())
        conn = self._connect()
        try:
            conn.execute(
                """
                update internal_api_imports
                set status = ?, stage = ?, result_json = ?, error = ?,
                    info_hash = coalesce(?, info_hash), msg_media_id = coalesce(?, msg_media_id),
                    cancel_requested = 0, updated_at = ?, completed_at = ?
                where id = ?
                """,
                (
                    status,
                    stage,
                    json_dumps(result) if result is not None else None,
                    str(error) if error else None,
                    nonempty_or_none(info_hash),
                    nonempty_or_none(msg_media_id),
                    now,
                    now,
                    import_id,
                ),
            )
            conn.commit()
        finally:
            conn.close()

    def requeue_running_import(self, import_id):
        now = int(time.time())
        conn = self._connect()
        try:
            conn.execute(
                """
                update internal_api_imports
                set status = 'queued', stage = 'queued',
                    updated_at = ?, started_at = null
                where id = ? and status = 'running'
                """,
                (now, import_id),
            )
            conn.commit()
        finally:
            conn.close()

    def defer_waiting_import(self, import_id):
        now = int(time.time())
        conn = self._connect()
        try:
            cursor = conn.execute(
                """
                update internal_api_imports
                set status = 'queued', stage = 'waiting_download',
                    updated_at = ?, started_at = null
                where id = ? and status = 'running' and stage = 'waiting_download'
                """,
                (now, import_id),
            )
            if cursor.rowcount != 1:
                conn.rollback()
                raise RuntimeError("cannot defer import that is not waiting for 115 download: %s" % import_id)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def request_cancel(self, owner_id, import_id):
        now = int(time.time())
        conn = self._connect()
        try:
            conn.execute("begin immediate")
            row = conn.execute(
                "select * from internal_api_imports where id = ? and owner_id = ?",
                (import_id, owner_id),
            ).fetchone()
            if row is None:
                raise ApiError(404, "import_not_found", "import task not found")
            if row["status"] == "canceled":
                conn.commit()
                return import_row(row)
            if row["status"] in FINAL_IMPORT_STATUSES:
                raise ApiError(409, "import_not_cancelable", "import task is already final")
            if row["stage"] in SYNC_STAGES or row["msg_media_id"]:
                raise ApiError(409, "import_not_cancelable", "import task cannot be canceled after MSG sync has started")
            if row["status"] == "queued":
                conn.execute(
                    """
                    update internal_api_imports
                    set status = 'canceled', stage = 'canceled', error = 'canceled before execution',
                        cancel_requested = 0, updated_at = ?, completed_at = ?
                    where id = ?
                    """,
                    (now, now, import_id),
                )
            else:
                conn.execute(
                    "update internal_api_imports set cancel_requested = 1, updated_at = ? where id = ?",
                    (now, import_id),
                )
            updated = conn.execute("select * from internal_api_imports where id = ?", (import_id,)).fetchone()
            conn.commit()
            return import_row(updated)
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def retry_import(self, owner_id, import_id):
        now = int(time.time())
        conn = self._connect()
        try:
            conn.execute("begin immediate")
            row = conn.execute(
                "select * from internal_api_imports where id = ? and owner_id = ?",
                (import_id, owner_id),
            ).fetchone()
            if row is None:
                raise ApiError(404, "import_not_found", "import task not found")
            if row["status"] not in RETRYABLE_IMPORT_STATUSES:
                raise ApiError(409, "import_not_retryable", "import task is not retryable")
            result = json.loads(row["result_json"]) if row["result_json"] else None
            request = json.loads(row["request_json"])
            retry_resubmit = subscription_retry_resubmit(result, request, row["msg_media_id"])
            preserve_sync = bool(
                not retry_resubmit
                and (
                    row["msg_media_id"]
                    or result_task_is_offline_success(result)
                    or (
                        request.get("subscription_follow")
                        and result_task_is_offline_active(result)
                    )
                )
            )
            resume_stage = warning_retry_stage(result, request) if preserve_sync else ""
            if retry_resubmit:
                result = {
                    "subscription_retry": {
                        "previous_info_hash": str(row["info_hash"] or "").strip(),
                        "previous_submit_kind": str(
                            ((result or {}).get("submit") or {}).get("submit_kind") or ""
                        ).strip(),
                        "previous_outcome": str(
                            ((result or {}).get("subscription_follow") or {}).get("outcome") or ""
                        ).strip(),
                    }
                }
            elif preserve_sync:
                result = dict(result or {})
                task = dict(result.get("task") or {})
                task["msg_sync_status"] = "running"
                task["msg_error"] = None
                result["task"] = task
                if request.get("subscription_follow"):
                    # A subscription retry that reached 115 success but failed
                    # while validating/promoting the staged resource must
                    # re-inspect the current staging contents.  Keeping the
                    # previous resource_manifest makes a multi-video retry
                    # compare the old failed classification with the fresh
                    # OpenList scan and fail again before episode mapping.
                    follow_result = dict(result.get("subscription_follow") or {})
                    follow_result.pop("resource_manifest", None)
                    follow_result.pop("ambiguous_videos", None)
                    follow_result.pop("unknown_videos", None)
                    follow_result.pop("duplicate_episodes", None)
                    follow_result.pop("verified_episodes", None)
                    follow_result.pop("selected_episodes", None)
                    result["subscription_follow"] = follow_result
                if resume_stage:
                    result["retry_from_stage"] = resume_stage
                else:
                    result.pop("retry_from_stage", None)
            else:
                result = None
            conn.execute(
                """
                update internal_api_imports
                set status = 'queued', stage = ?, result_json = ?, error = null,
                    info_hash = ?, msg_media_id = ?, cancel_requested = 0,
                    updated_at = ?, started_at = null, completed_at = null
                where id = ?
                """,
                (
                    resume_stage or "queued",
                    json_dumps(result) if result is not None else None,
                    row["info_hash"] if preserve_sync else None,
                    row["msg_media_id"] if preserve_sync else None,
                    now,
                    import_id,
                ),
            )
            updated = conn.execute("select * from internal_api_imports where id = ?", (import_id,)).fetchone()
            conn.commit()
            return import_row(updated)
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def recover_running_subtitle_asr_tasks(self):
        now = int(time.time())
        conn = self._connect()
        try:
            cursor = conn.execute(
                """
                update internal_api_subtitle_asr_tasks
                set status = 'queued', stage = 'queued', progress_current = 0, progress_total = 0,
                    result_json = null, error = null, updated_at = ?, started_at = null, completed_at = null
                where status = 'running'
                """,
                (now,),
            )
            conn.commit()
            return int(cursor.rowcount or 0)
        finally:
            conn.close()

    def create_subtitle_asr_task(
        self,
        owner_id,
        media_id,
        source_language,
        asr_model=DEFAULT_ASR_MODEL,
        translation_provider="local",
        translation_model="",
    ):
        task_id = uuid.uuid4().hex
        now = int(time.time())
        conn = self._connect()
        try:
            conn.execute("begin immediate")
            existing = conn.execute(
                """
                select * from internal_api_subtitle_asr_tasks
                where owner_id = ? and media_id = ? and status in ('queued', 'running')
                order by created_at desc, id desc limit 1
                """,
                (owner_id, media_id),
            ).fetchone()
            if existing is not None:
                conn.commit()
                return subtitle_asr_task_row(existing), False
            conn.execute(
                """
                insert into internal_api_subtitle_asr_tasks
                    (id, owner_id, media_id, source_language, asr_model,
                     translation_provider, translation_model, status, stage, created_at, updated_at)
                values (?, ?, ?, ?, ?, ?, ?, 'queued', 'queued', ?, ?)
                """,
                (
                    task_id,
                    owner_id,
                    media_id,
                    source_language,
                    asr_model,
                    translation_provider,
                    translation_model,
                    now,
                    now,
                ),
            )
            row = conn.execute(
                "select * from internal_api_subtitle_asr_tasks where id = ?", (task_id,)
            ).fetchone()
            conn.commit()
            return subtitle_asr_task_row(row), True
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def get_subtitle_asr_task(self, owner_id, task_id):
        conn = self._connect()
        try:
            row = conn.execute(
                "select * from internal_api_subtitle_asr_tasks where id = ? and owner_id = ?",
                (task_id, owner_id),
            ).fetchone()
        finally:
            conn.close()
        if row is None:
            raise ApiError(404, "subtitle_asr_task_not_found", "AI subtitle task not found")
        return subtitle_asr_task_row(row)

    def list_subtitle_asr_tasks(self, limit=50):
        limit = max(1, min(int(limit or 50), 200))
        conn = self._connect()
        try:
            rows = conn.execute(
                """
                select * from internal_api_subtitle_asr_tasks
                order by
                    case when status in ('queued', 'running') then 0 else 1 end,
                    updated_at desc,
                    id desc
                limit ?
                """,
                (limit,),
            ).fetchall()
        finally:
            conn.close()
        return [subtitle_asr_task_row(row) for row in rows]

    def list_subtitle_asr_task_ids_for_media(self, media_id):
        conn = self._connect()
        try:
            rows = conn.execute(
                """
                select id from internal_api_subtitle_asr_tasks
                where media_id = ?
                order by updated_at desc, created_at desc, id desc
                """,
                (media_id,),
            ).fetchall()
        finally:
            conn.close()
        return [str(row["id"]) for row in rows]

    def claim_next_subtitle_asr_task(self):
        conn = self._connect()
        try:
            conn.execute("begin immediate")
            row = conn.execute(
                """
                select * from internal_api_subtitle_asr_tasks
                where status = 'queued' order by created_at, id limit 1
                """
            ).fetchone()
            if row is None:
                conn.commit()
                return None
            now = int(time.time())
            cursor = conn.execute(
                """
                update internal_api_subtitle_asr_tasks
                set status = 'running', stage = 'starting', attempt_count = attempt_count + 1,
                    started_at = ?, completed_at = null, updated_at = ?, error = null
                where id = ? and status = 'queued'
                """,
                (now, now, row["id"]),
            )
            if cursor.rowcount != 1:
                conn.commit()
                return None
            claimed = conn.execute(
                "select * from internal_api_subtitle_asr_tasks where id = ?", (row["id"],)
            ).fetchone()
            conn.commit()
            return subtitle_asr_task_row(claimed)
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def save_subtitle_asr_progress(self, task_id, stage, current, total):
        now = int(time.time())
        conn = self._connect()
        try:
            conn.execute(
                """
                update internal_api_subtitle_asr_tasks
                set stage = ?, progress_current = ?, progress_total = ?, updated_at = ?
                where id = ? and status = 'running'
                """,
                (str(stage), max(0, int(current)), max(0, int(total)), now, task_id),
            )
            conn.commit()
        finally:
            conn.close()

    def save_subtitle_asr_cache(self, task_id, audio_cached, transcript_cached):
        now = int(time.time())
        conn = self._connect()
        try:
            conn.execute(
                """
                update internal_api_subtitle_asr_tasks
                set cached_audio = ?, cached_transcript = ?, updated_at = ?
                where id = ?
                """,
                (bool(audio_cached), bool(transcript_cached), now, task_id),
            )
            conn.commit()
        finally:
            conn.close()

    def retry_subtitle_asr_task(
        self,
        owner_id,
        task_id,
        asr_model,
        translation_provider,
        translation_model,
        audio_cached,
        transcript_cached,
    ):
        now = int(time.time())
        conn = self._connect()
        try:
            conn.execute("begin immediate")
            row = conn.execute(
                "select * from internal_api_subtitle_asr_tasks where id = ? and owner_id = ?",
                (task_id, owner_id),
            ).fetchone()
            if row is None:
                raise ApiError(404, "subtitle_asr_task_not_found", "AI subtitle task not found")
            if row["status"] != "failed":
                raise ApiError(409, "subtitle_asr_not_retryable", "only failed AI subtitle tasks can be retried")
            conn.execute(
                """
                update internal_api_subtitle_asr_tasks
                set status = 'queued', stage = 'queued', progress_current = 0,
                    progress_total = 0, result_json = null, error = null,
                    asr_model = ?, translation_provider = ?, translation_model = ?,
                    cached_audio = ?, cached_transcript = ?, updated_at = ?,
                    started_at = null, completed_at = null
                where id = ?
                """,
                (
                    asr_model,
                    translation_provider,
                    translation_model,
                    bool(audio_cached),
                    bool(transcript_cached),
                    now,
                    task_id,
                ),
            )
            updated = conn.execute(
                "select * from internal_api_subtitle_asr_tasks where id = ?", (task_id,)
            ).fetchone()
            conn.commit()
            return subtitle_asr_task_row(updated)
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def update_queued_subtitle_asr_task_model(
        self, owner_id, task_id, asr_model, translation_provider, translation_model
    ):
        now = int(time.time())
        conn = self._connect()
        try:
            conn.execute("begin immediate")
            row = conn.execute(
                "select * from internal_api_subtitle_asr_tasks where id = ? and owner_id = ?",
                (task_id, owner_id),
            ).fetchone()
            if row is None:
                raise ApiError(404, "subtitle_asr_task_not_found", "AI subtitle task not found")
            if row["status"] != "queued":
                raise ApiError(
                    409,
                    "subtitle_asr_model_not_editable",
                    "only queued AI subtitle tasks can change ASR or translation model",
                )
            conn.execute(
                """
                update internal_api_subtitle_asr_tasks
                set asr_model = ?, translation_provider = ?, translation_model = ?, updated_at = ?
                where id = ? and status = 'queued'
                """,
                (asr_model, translation_provider, translation_model, now, task_id),
            )
            updated = conn.execute(
                "select * from internal_api_subtitle_asr_tasks where id = ?", (task_id,)
            ).fetchone()
            conn.commit()
            return subtitle_asr_task_row(updated)
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def cancel_queued_subtitle_asr_task(self, owner_id, task_id):
        now = int(time.time())
        conn = self._connect()
        try:
            conn.execute("begin immediate")
            row = conn.execute(
                "select * from internal_api_subtitle_asr_tasks where id = ? and owner_id = ?",
                (task_id, owner_id),
            ).fetchone()
            if row is None:
                raise ApiError(404, "subtitle_asr_task_not_found", "AI subtitle task not found")
            if row["status"] != "queued":
                raise ApiError(
                    409,
                    "subtitle_asr_not_cancelable",
                    "only queued AI subtitle tasks can be canceled",
                )
            conn.execute(
                """
                update internal_api_subtitle_asr_tasks
                set status = 'canceled', stage = 'canceled', progress_current = 0,
                    progress_total = 0, result_json = null, error = null,
                    updated_at = ?, started_at = null, completed_at = ?
                where id = ? and status = 'queued'
                """,
                (now, now, task_id),
            )
            updated = conn.execute(
                "select * from internal_api_subtitle_asr_tasks where id = ?", (task_id,)
            ).fetchone()
            conn.commit()
            return subtitle_asr_task_row(updated)
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def clear_canceled_subtitle_asr_cache_state(self, owner_id, task_id):
        now = int(time.time())
        conn = self._connect()
        try:
            cursor = conn.execute(
                """
                update internal_api_subtitle_asr_tasks
                set cached_audio = 0, cached_transcript = 0, updated_at = ?
                where id = ? and owner_id = ? and status = 'canceled'
                """,
                (now, task_id, owner_id),
            )
            if cursor.rowcount != 1:
                raise ApiError(
                    409,
                    "subtitle_asr_cache_state_conflict",
                    "canceled AI subtitle task cache state changed unexpectedly",
                )
            row = conn.execute(
                "select * from internal_api_subtitle_asr_tasks where id = ?", (task_id,)
            ).fetchone()
            conn.commit()
            return subtitle_asr_task_row(row)
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def retranslate_completed_subtitle_asr_task(
        self,
        owner_id,
        task_id,
        translation_provider,
        translation_model,
        audio_cached,
        transcript_cached,
    ):
        now = int(time.time())
        conn = self._connect()
        try:
            conn.execute("begin immediate")
            row = conn.execute(
                "select * from internal_api_subtitle_asr_tasks where id = ? and owner_id = ?",
                (task_id, owner_id),
            ).fetchone()
            if row is None:
                raise ApiError(404, "subtitle_asr_task_not_found", "AI subtitle task not found")
            if row["status"] != "completed":
                raise ApiError(
                    409,
                    "subtitle_asr_not_retranslatable",
                    "only completed AI subtitle tasks can be retranslated",
                )
            if not audio_cached or not transcript_cached:
                raise ApiError(
                    409,
                    "subtitle_asr_cache_incomplete",
                    "cached audio and SenseVoice transcript are both required for retranslation",
                )
            active = conn.execute(
                """
                select id from internal_api_subtitle_asr_tasks
                where owner_id = ? and media_id = ? and status in ('queued', 'running') and id != ?
                limit 1
                """,
                (owner_id, row["media_id"], task_id),
            ).fetchone()
            if active is not None:
                raise ApiError(
                    409,
                    "subtitle_asr_active",
                    "another AI subtitle task is already active for this media",
                )
            conn.execute(
                """
                update internal_api_subtitle_asr_tasks
                set status = 'queued', stage = 'queued', progress_current = 0,
                    progress_total = 0, result_json = null, error = null,
                    translation_provider = ?, translation_model = ?,
                    cached_audio = 1, cached_transcript = 1, updated_at = ?,
                    started_at = null, completed_at = null
                where id = ? and status = 'completed'
                """,
                (translation_provider, translation_model, now, task_id),
            )
            updated = conn.execute(
                "select * from internal_api_subtitle_asr_tasks where id = ?", (task_id,)
            ).fetchone()
            conn.commit()
            return subtitle_asr_task_row(updated)
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def delete_subtitle_asr_task(self, owner_id, task_id):
        conn = self._connect()
        try:
            conn.execute("begin immediate")
            row = conn.execute(
                "select * from internal_api_subtitle_asr_tasks where id = ? and owner_id = ?",
                (task_id, owner_id),
            ).fetchone()
            if row is None:
                raise ApiError(404, "subtitle_asr_task_not_found", "AI subtitle task not found")
            if row["status"] in {"queued", "running"}:
                raise ApiError(409, "subtitle_asr_active", "active AI subtitle tasks cannot be deleted")
            conn.execute("delete from internal_api_subtitle_asr_tasks where id = ?", (task_id,))
            conn.commit()
            return subtitle_asr_task_row(row)
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def finish_subtitle_asr_task(self, task_id, status, stage, result=None, error=None):
        if status not in FINAL_SUBTITLE_ASR_STATUSES:
            raise ValueError("invalid final subtitle ASR status: %s" % status)
        now = int(time.time())
        conn = self._connect()
        try:
            conn.execute(
                """
                update internal_api_subtitle_asr_tasks
                set status = ?, stage = ?, result_json = ?, error = ?, updated_at = ?, completed_at = ?
                where id = ?
                """,
                (status, stage, json_dumps(result) if result is not None else None, str(error) if error else None, now, now, task_id),
            )
            conn.commit()
        finally:
            conn.close()


class ImportTaskManager:
    def __init__(
        self,
        service,
        store,
        workers=3,
        owner_workers=2,
        poll_seconds=2.0,
        offline_wait_slice_seconds=DEFAULT_OFFLINE_WAIT_SLICE_SECONDS,
    ):
        self.service = service
        self.store = store
        self.workers = max(1, int(workers))
        self.owner_workers = max(1, int(owner_workers))
        self.poll_seconds = max(0.01, float(poll_seconds))
        self.offline_wait_slice_seconds = max(0.01, float(offline_wait_slice_seconds))
        self._condition = threading.Condition()
        self._stop_event = threading.Event()
        self._threads = []
        self._active_by_owner = defaultdict(int)
        self._target_locks = {}
        self._target_locks_guard = threading.Lock()
        self._subscription_receive_lock = threading.Lock()
        self._post_enhancement_active = set()
        self._post_enhancement_threads = set()
        self._post_enhancement_guard = threading.Lock()

    def start(self):
        with self._condition:
            if self._threads:
                return
            self._stop_event.clear()
            self.store.recover_running_imports()
            self._threads = [
                threading.Thread(target=self._worker_loop, name="internal-api-import-%d" % index, daemon=True)
                for index in range(self.workers)
            ]
            for thread in self._threads:
                thread.start()
        for task in self.store.list_pending_post_enhancements():
            self._schedule_post_enhancement(task)

    def stop(self):
        self._stop_event.set()
        with self._condition:
            self._condition.notify_all()
        for thread in list(self._threads):
            thread.join(timeout=5)
        self._threads = []
        with self._post_enhancement_guard:
            enhancement_threads = list(self._post_enhancement_threads)
        for thread in enhancement_threads:
            thread.join(timeout=5)

    def _schedule_post_enhancement(self, task):
        task_id = str((task or {}).get("id") or "").strip()
        if not task_id:
            return False
        with self._post_enhancement_guard:
            if task_id in self._post_enhancement_active:
                return False
            self._post_enhancement_active.add(task_id)
            thread = threading.Thread(
                target=self._run_post_enhancement,
                args=(dict(task),),
                name="internal-api-post-enhancement-%s" % task_id[:8],
                daemon=True,
            )
            self._post_enhancement_threads.add(thread)
            thread.start()
        return True

    def _run_post_enhancement(self, task):
        task_id = str(task.get("id") or "").strip()
        try:
            current = self.store.get_import(task["owner_id"], task_id)
            request = dict(current.get("request") or {})
            result = dict(current.get("result") or {})
            offline_task = dict(result.get("task") or {})
            if not post_enhancement_needs_run(offline_task):
                return
            restore_deferred_post_enhancement_stages(offline_task)
            offline_task.update(
                {
                    "post_enhancement_status": "running",
                    "post_enhancement_execution": True,
                    "defer_enhancements": False,
                }
            )
            result["task"] = offline_task
            self.store.update_completed_result(task_id, result)

            def save_progress(progress):
                current_result = dict(result)
                current_task = dict(offline_task)
                current_task.update(dict(progress or {}))
                current_task["post_enhancement_status"] = "running"
                current_task["post_enhancement_execution"] = True
                current_result["task"] = current_task
                self.store.update_completed_result(task_id, current_result)

            enhanced = self.service.sync_completed_task(
                request["category"],
                str(request.get("upgrade_target_title") or request["title"]).strip(),
                offline_task,
                progress_callback=save_progress,
                target=request.get("target"),
                preferred_scrape_queries=request.get("upgrade_target_scrape_queries"),
                upgrade_media_id=request.get("upgrade_media_id"),
            )
            if not isinstance(enhanced, dict):
                raise RuntimeError("post-ingest enhancement returned invalid task")
            if enhanced.get("msg_sync_status") != "success":
                raise RuntimeError(str(enhanced.get("msg_error") or "post-ingest enhancement failed"))
            enhanced["post_enhancement_status"] = "success"
            enhanced["post_enhancement_stage"] = "completed"
            enhanced["post_enhancement_error"] = None
            post_warning = result.pop("post_enhancement_warning", None)
            if post_warning:
                remaining_warnings = [
                    value for value in result.get("warnings") or [] if str(value).strip() != str(post_warning).strip()
                ]
                if remaining_warnings:
                    result["warnings"] = remaining_warnings
                else:
                    result.pop("warnings", None)
            if enhanced.get("warning_stage") == "post_enhancement":
                enhanced.pop("warning_stage", None)
                enhanced.pop("warnings", None)
            enhanced.pop("post_enhancement_execution", None)
            enhanced.pop("defer_enhancements", None)
            result["task"] = enhanced
            self.store.finish_post_enhancement_success(task_id, result)
        except Exception as exc:
            try:
                current = self.store.get_import(task["owner_id"], task_id)
                result = dict(current.get("result") or {})
                offline_task = dict(result.get("task") or {})
                offline_task["post_enhancement_status"] = "failed"
                offline_task["post_enhancement_error"] = str(exc)
                offline_task["post_enhancement_stage"] = str(offline_task.get("post_enhancement_stage") or "enhance")
                offline_task["warning_stage"] = "post_enhancement"
                offline_task.pop("post_enhancement_execution", None)
                result["task"] = offline_task
                result["post_enhancement_warning"] = str(exc)
                warning_items = [str(value).strip() for value in result.get("warnings") or [] if str(value).strip()]
                if str(exc) not in warning_items:
                    warning_items.append(str(exc))
                result["warnings"] = warning_items
                self.store.finish_post_enhancement_warning(task_id, result, "; ".join(warning_items))
            except Exception as persist_exc:
                print("internal API post-enhancement persistence failed: %s" % persist_exc, flush=True)
            print("internal API post-enhancement %s failed: %s" % (task_id, exc), flush=True)
        finally:
            current_thread = threading.current_thread()
            with self._post_enhancement_guard:
                self._post_enhancement_active.discard(task_id)
                self._post_enhancement_threads.discard(current_thread)

    def notify(self):
        with self._condition:
            self._condition.notify_all()

    def create_import(self, owner_id, idempotency_key, payload):
        owner_id = require_text(owner_id, "owner_id", max_length=200)
        idempotency_key = require_text(idempotency_key, "Idempotency-Key", max_length=200)
        if not isinstance(payload, dict):
            raise ApiError(400, "invalid_request", "request body must be a JSON object")
        session_id = require_text(payload.get("search_session_id"), "search_session_id", max_length=100)
        candidate_id = require_text(payload.get("candidate_id"), "candidate_id", max_length=100)
        category = require_category(payload.get("category"))
        target = normalize_target(payload)
        configured_openlist_path = normalize_openlist_path(category_to_openlist_path(category))
        if normalize_openlist_path(target["root_openlist_path"]) != configured_openlist_path:
            raise ApiError(
                409,
                "target_transfer_not_configured",
                "该媒体库目录尚未配置115转存目标",
            )
        target["root_openlist_path"] = configured_openlist_path
        force_duplicate = optional_bool(payload.get("force_duplicate"), "force_duplicate", default=False)
        upgrade_media_id = optional_text(payload.get("upgrade_media_id"), "upgrade_media_id", max_length=100)
        upgrade_scope = normalize_upgrade_scope(category, upgrade_media_id, payload.get("upgrade_scope"))
        keep_old_version = optional_bool(payload.get("keep_old_version"), "keep_old_version", default=True)
        subscription_follow = normalize_subscription_follow(payload, category, target, force_duplicate, upgrade_media_id)
        existing = self.store.get_import_by_idempotency(owner_id, idempotency_key)
        if existing is not None:
            existing_request = existing["request"]
            incoming_identity = {
                "search_session_id": session_id,
                "candidate_id": candidate_id,
                "category": category,
                "target": target,
                "force_duplicate": force_duplicate,
                "upgrade_media_id": upgrade_media_id,
                "upgrade_scope": upgrade_scope,
                "keep_old_version": keep_old_version,
                "subscription_follow": subscription_follow,
            }
            persisted_identity = {key: existing_request.get(key) for key in incoming_identity}
            if json_dumps(persisted_identity) != json_dumps(incoming_identity):
                raise ApiError(409, "idempotency_conflict", "Idempotency-Key was already used with a different request")
            return existing, False
        candidate_record = self.store.load_search_candidate(owner_id, session_id, candidate_id)
        if candidate_record["category"] != category:
            raise ApiError(409, "category_mismatch", "category does not match the search session")
        candidate = candidate_record["candidate"]
        download_uri = str(candidate.get("download_uri") or "").strip()
        if not download_uri:
            raise ApiError(409, "candidate_not_importable", "stored candidate has no download_uri")
        request = {
            "search_session_id": session_id,
            "candidate_id": candidate_id,
            "query": candidate_record["query"],
            "category": category,
            "title": require_text(candidate.get("title"), "candidate title", max_length=1000),
            "candidate": candidate,
            "target": target,
            "force_duplicate": force_duplicate,
            "upgrade_media_id": upgrade_media_id,
            "upgrade_scope": upgrade_scope,
            "keep_old_version": keep_old_version,
            "subscription_follow": subscription_follow,
        }
        if subscription_follow:
            if not subscription_follow.get("manual_replenish"):
                self._check_subscription_source_block(request)
        else:
            self._check_duplicate(request)
        task, created = self.store.create_import(owner_id, idempotency_key, request)
        if created:
            self.notify()
        return task, created

    def _check_subscription_source_block(self, request):
        follow = request.get("subscription_follow")
        if not follow:
            return
        source_key = subscription_source_block_key((request.get("candidate") or {}).get("download_uri"))
        block = self.store.get_subscription_source_block(source_key)
        if subscription_source_block_is_effective(block):
            raise ApiError(
                409,
                "subscription_source_blocked",
                "订阅资源已拉黑：此前已确认不可用或不含新增视频",
                details={"reason": block.get("reason"), "origin_import_id": block.get("origin_import_id")},
            )

    def _check_duplicate(self, request):
        upgrade_media_id = str(request.get("upgrade_media_id") or "").strip()
        upgrade_target = None
        if upgrade_media_id:
            try:
                upgrade_target = self.service.validate_upgrade_target(upgrade_media_id, request["target"])
                if isinstance(upgrade_target, dict):
                    request["upgrade_target_title"] = str(
                        upgrade_target.get("display_title")
                        or upgrade_target.get("title")
                        or upgrade_target.get("original_name")
                        or ""
                    ).strip()
                    request["upgrade_target_scrape_queries"] = upgrade_target_scrape_queries(
                        upgrade_target,
                        request["target"],
                    )
            except ValueError as exc:
                raise ApiError(409, "invalid_upgrade_target", str(exc))
            except Exception as exc:
                raise ApiError(502, "upgrade_target_check_failed", str(exc))
        try:
            duplicate = self.service.check_duplicate(
                request["category"],
                request["query"],
                request["candidate"],
                target=request["target"],
            )
        except Exception as exc:
            raise ApiError(502, "duplicate_check_failed", str(exc))
        if duplicate is not None:
            if not isinstance(duplicate, dict):
                raise ApiError(502, "duplicate_check_failed", "pipeline duplicate check returned invalid response")
            summary = duplicate_summary(duplicate)
            if upgrade_media_id:
                if summary.get("media_id") == upgrade_media_id:
                    return
                try:
                    if self.service.upgrade_duplicate_matches_target(
                        upgrade_target,
                        summary,
                        request["category"],
                    ):
                        return
                except Exception as exc:
                    raise ApiError(502, "upgrade_duplicate_check_failed", str(exc))
                summary["can_force"] = False
            if not (request.get("force_duplicate") and summary["can_force"]):
                raise ApiError(
                    409,
                    "duplicate_media",
                    "所选资源与其他已入库作品重复" if upgrade_media_id else "target MediaStationGo library already contains matching media",
                    details={"duplicate": summary},
                )

    def get_import(self, owner_id, import_id):
        return self.store.get_import(require_text(owner_id, "owner_id", 200), import_id)

    def cancel_import(self, owner_id, import_id):
        task = self.store.request_cancel(require_text(owner_id, "owner_id", 200), import_id)
        self.notify()
        return task

    def retry_import(self, owner_id, import_id):
        owner_id = require_text(owner_id, "owner_id", 200)
        current = self.store.get_import(owner_id, import_id)
        if current["status"] in RETRYABLE_IMPORT_STATUSES:
            if current["request"].get("subscription_follow"):
                if not current["request"]["subscription_follow"].get("manual_replenish"):
                    self._check_subscription_source_block(current["request"])
            elif not (current.get("msg_media_id") or result_task_is_offline_success(current.get("result"))):
                self._check_duplicate(current["request"])
        task = self.store.retry_import(owner_id, import_id)
        self.notify()
        return task

    def _worker_loop(self):
        while not self._stop_event.is_set():
            try:
                task = self._claim_task()
            except sqlite3.Error as exc:
                print("internal API import worker claim failed: %s" % exc, flush=True)
                with self._condition:
                    self._condition.wait(timeout=self.poll_seconds)
                continue
            if task is None:
                with self._condition:
                    self._condition.wait(timeout=0.5)
                continue
            owner_id = task["owner_id"]
            try:
                self._execute_task(task)
            except OfflineWaitDeferred:
                self.store.defer_waiting_import(task["id"])
                print(
                    "internal API import %s deferred while waiting for 115 download" % task["id"], flush=True
                )
            except WorkerStopping:
                self.store.requeue_running_import(task["id"])
            except ImportCanceled as exc:
                current = self.store.get_import(owner_id, task["id"])
                canceled_result = self._settle_subscription_terminal_staging(
                    current,
                    dict(current.get("result") or {}),
                    "canceled",
                )
                self.store.finish_import(
                    task["id"],
                    "canceled",
                    "canceled",
                    result=canceled_result,
                    error=str(exc),
                    info_hash=current.get("info_hash"),
                    msg_media_id=current.get("msg_media_id"),
                )
            except Exception as exc:
                print("internal API import %s failed: %s" % (task["id"], exc), flush=True)
                current = self.store.get_import(owner_id, task["id"])
                failed_result = dict(current.get("result") or {})
                follow_audit = failed_result.get("subscription_follow")
                if isinstance(follow_audit, dict):
                    follow_audit = dict(follow_audit)
                    follow_audit.setdefault("outcome", "failed")
                    attempts = list(follow_audit.get("attempts") or [])
                    if attempts:
                        attempts[-1] = {
                            **dict(attempts[-1]),
                            "outcome": follow_audit["outcome"],
                            "error": str(exc),
                            "finished_at": int(time.time()),
                        }
                        follow_audit["attempts"] = attempts
                    failed_result["subscription_follow"] = follow_audit
                failed_result = self._settle_subscription_terminal_staging(
                    current,
                    failed_result,
                    "failed",
                )
                self.store.finish_import(
                    task["id"],
                    "failed",
                    "failed",
                    result=failed_result,
                    error=str(exc),
                    info_hash=current.get("info_hash"),
                    msg_media_id=current.get("msg_media_id"),
                )
            finally:
                with self._condition:
                    self._active_by_owner[owner_id] -= 1
                    if self._active_by_owner[owner_id] <= 0:
                        self._active_by_owner.pop(owner_id, None)
                    self._condition.notify_all()

    def _settle_subscription_terminal_staging(self, task, result, terminal_status):
        request = dict((task or {}).get("request") or {})
        if not request.get("subscription_follow"):
            return result
        result = dict(result or {})
        audit = dict(result.get("subscription_follow") or {})
        staging = dict(audit.get("staging") or {})
        if not staging.get("openlist_path") or audit.get("staging_cleaned_at"):
            return result
        promoted = bool(audit.get("moved_episodes"))
        try:
            settlement = self._with_subscription_staging_lock(
                request["subscription_follow"]["work_key"],
                lambda: self.service.settle_subscription_staging(
                    request["category"],
                    staging,
                    terminal_status,
                    promoted=promoted,
                ),
            )
        except Exception as cleanup_exc:
            settlement = {
                "status": "failed",
                "reason": "terminal_%s_cleanup" % terminal_status,
                "path": staging.get("openlist_path"),
                "error": str(cleanup_exc),
            }
            print(
                "internal API import %s staging cleanup failed: %s"
                % (task.get("id"), cleanup_exc),
                flush=True,
            )
        audit["staging_cleanup"] = dict(settlement or {})
        if audit["staging_cleanup"].get("status") == "cleaned":
            audit["staging_cleaned_at"] = int(
                audit["staging_cleanup"].get("cleaned_at") or time.time()
            )
        result["subscription_follow"] = audit
        return result

    def _with_subscription_staging_lock(self, work_key, callback):
        lock = self._target_lock(
            {
                "library_id": "subscription-staging",
                "root_id": str(work_key or "").strip(),
            }
        )
        self._acquire_target_lock(lock)
        try:
            return callback()
        finally:
            lock.release()

    def _claim_task(self):
        with self._condition:
            blocked = {owner for owner, count in self._active_by_owner.items() if count >= self.owner_workers}
            task = self.store.claim_next_import(blocked)
            if task is not None:
                self._active_by_owner[task["owner_id"]] += 1
            return task

    def _execute_task(self, task):
        request = task["request"]
        if request.get("subscription_follow"):
            return self._execute_subscription_follow_task(task)
        result = dict(task.get("result") or {})
        resume_stage = retry_resume_stage(result)
        category = request["category"]
        title = str(request.get("upgrade_target_title") or request["title"]).strip()
        info_hash = str(task.get("info_hash") or "").strip()
        offline_task = dict(result.get("task") or {})
        import_target = dict(result.get("import_target") or result.get("upgrade_staging") or {})
        if import_target:
            if not str(import_target.get("purpose") or "").strip():
                import_target["purpose"] = (
                    "upgrade"
                    if result.get("upgrade_staging") or str(request.get("upgrade_media_id") or "").strip()
                    else "import"
                )
            result["import_target"] = dict(import_target)
            result.pop("upgrade_staging", None)

        self._raise_if_stopping()
        self._raise_if_cancel_requested(task["owner_id"], task["id"], category, info_hash)
        if not info_hash:
            if not import_target:
                purpose = "upgrade" if str(request.get("upgrade_media_id") or "").strip() else "import"
                self.store.save_running(task["id"], "preparing_target", result=result)
                import_target = self.service.prepare_import_target(
                    category,
                    task["id"],
                    title,
                    purpose=purpose,
                )
                validate_import_target(import_target)
                result["import_target"] = dict(import_target)
                result.pop("upgrade_staging", None)
                self.store.save_running(task["id"], "preparing_target", result=result)
            self.store.save_running(task["id"], "submitting", result=result)
            submit_result = self.service.submit(
                category,
                request["candidate"]["download_uri"],
                target_folder_id=import_target.get("folder_id"),
            )
            info_hash = first_submit_info_hash(submit_result)
            if not info_hash:
                raise RuntimeError("pipeline submit returned no info_hash")
            offline_task = task_from_submit_result(submit_result, info_hash)
            apply_import_target(offline_task, import_target)
            result.update({"submit": submit_result, "task": offline_task})
            self.store.save_running(task["id"], "submitted", result=result, info_hash=info_hash)

        offline_wait_deadline = time.monotonic() + self.offline_wait_slice_seconds
        while offline_task.get("status_name") != OFFLINE_SUCCESS_STATUS:
            status_name = offline_task.get("status_name")
            if status_name in {"cancelled", "canceled"}:
                raise ImportCanceled("import task canceled")
            if status_name in OFFLINE_FAILED_STATUSES:
                raise RuntimeError(format_offline_task_failure(offline_task))
            if status_name not in OFFLINE_ACTIVE_STATUSES:
                raise RuntimeError("115 offline task returned invalid status: %s" % (status_name or "-"))
            self._raise_if_stopping()
            self._raise_if_cancel_requested(task["owner_id"], task["id"], category, info_hash)
            self.store.save_running(task["id"], "waiting_download", result=result, info_hash=info_hash)
            previous_poll_attempt = offline_task_poll_attempt(offline_task)
            offline_task = self.service.task_status(category, info_hash)
            if not isinstance(offline_task, dict):
                raise RuntimeError("pipeline task_status returned invalid response")
            offline_task = dict(offline_task)
            offline_task.setdefault("info_hash", info_hash)
            mark_offline_task_polled(offline_task, previous_poll_attempt)
            apply_import_target(offline_task, import_target)
            result["task"] = offline_task
            self.store.save_running(task["id"], "waiting_download", result=result, info_hash=info_hash)
            if offline_task.get("status_name") != OFFLINE_SUCCESS_STATUS:
                if time.monotonic() >= offline_wait_deadline:
                    raise OfflineWaitDeferred()
                self._wait_or_stop(offline_task_poll_delay(offline_task, self.poll_seconds))

        target = request["target"]
        self._raise_if_cancel_requested(task["owner_id"], task["id"], category, info_hash)
        target_lock = self._target_lock(target)
        hold_target_lock = resume_stage != "post_enhancement"
        if hold_target_lock:
            self._acquire_target_lock(target_lock)
        try:
            if resume_stage == "removing_old_version":
                return self._retry_upgrade_cleanup(task, result, info_hash, target)
            while True:
                self._raise_if_stopping()

                def save_progress(progress):
                    current = dict(result)
                    current["task"] = dict(progress or {})
                    media_id = str((progress or {}).get("msg_media_id") or "").strip()
                    self.store.save_running(
                        task["id"],
                        sync_stage(progress),
                        result=current,
                        info_hash=info_hash,
                        msg_media_id=media_id,
                    )

                initial_sync_stage = resume_stage if resume_stage in RETRY_RESUME_STAGES else "syncing"
                self.store.save_running(task["id"], initial_sync_stage, result=result, info_hash=info_hash)
                try:
                    offline_task["defer_enhancements"] = resume_stage != "post_enhancement"
                    if resume_stage == "post_enhancement":
                        offline_task["post_enhancement_execution"] = True
                    offline_task = self.service.sync_completed_task(
                        category,
                        title,
                        offline_task,
                        progress_callback=save_progress,
                        target=target,
                        preferred_scrape_queries=request.get("upgrade_target_scrape_queries"),
                        upgrade_media_id=request.get("upgrade_media_id"),
                    )
                except Exception as exc:
                    current = self.store.get_import(task["owner_id"], task["id"])
                    media_id = str(current.get("msg_media_id") or "").strip()
                    if not media_id:
                        raise
                    warning_result = dict(current.get("result") or result)
                    warning_result["msg_media_id"] = media_id
                    warning_result["warnings"] = [str(exc)]
                    warning_result["warning_stage"] = (
                        current.get("stage") if current.get("stage") in RETRY_RESUME_STAGES else "syncing"
                    )
                    warning_result.pop("retry_from_stage", None)
                    self.store.finish_import(
                        task["id"],
                        "completed_with_warning",
                        "completed_with_warning",
                        result=warning_result,
                        error=str(exc),
                        info_hash=info_hash,
                        msg_media_id=media_id,
                    )
                    return
                if not isinstance(offline_task, dict):
                    raise RuntimeError("pipeline sync_completed_task returned invalid response")
                offline_task = dict(offline_task)
                result["task"] = offline_task
                media_id = str(offline_task.get("msg_media_id") or "").strip()
                self.store.save_running(
                    task["id"],
                    sync_stage(offline_task),
                    result=result,
                    info_hash=info_hash,
                    msg_media_id=media_id,
                )
                if offline_task.get("msg_sync_status") == "running":
                    self._wait_or_stop()
                    continue
                if not media_id:
                    msg_error = str(offline_task.get("msg_error") or "").strip()
                    if msg_error:
                        raise RuntimeError(msg_error)
                    raise RuntimeError("MediaStationGo sync completed without msg_media_id")
                warnings = sync_warnings(offline_task)
                result["msg_media_id"] = media_id
                if resume_stage == "post_enhancement":
                    if offline_task.get("msg_sync_status") == "success":
                        offline_task["post_enhancement_status"] = "success"
                        offline_task["post_enhancement_stage"] = "completed"
                    else:
                        offline_task["post_enhancement_status"] = "failed"
                        offline_task["post_enhancement_stage"] = str(offline_task.get("post_enhancement_stage") or "enhance")
                        offline_task["warning_stage"] = "post_enhancement"
                    offline_task.pop("post_enhancement_execution", None)
                    offline_task.pop("defer_enhancements", None)
                    result["task"] = offline_task
                post_enhancement_pending = (
                    str(offline_task.get("post_enhancement_status") or "").strip().lower() == "pending"
                )
                upgrade_media_id = str(request.get("upgrade_media_id") or "").strip()
                if upgrade_media_id and not bool(request.get("keep_old_version", True)):
                    self.store.save_running(
                        task["id"],
                        "removing_old_version",
                        result=result,
                        info_hash=info_hash,
                        msg_media_id=media_id,
                    )
                    try:
                        result["upgrade_cleanup"] = self.service.remove_upgrade_target(
                            upgrade_media_id,
                            media_id,
                            target,
                            upgrade_scope=str(request.get("upgrade_scope") or "media"),
                            new_source_paths=upgrade_new_source_paths(result.get("task")),
                            category=category,
                        )
                    except Exception as exc:
                        warning = "旧片源移入回收站失败: %s" % exc
                        if warning not in warnings:
                            warnings.append(warning)
                        result["warning_stage"] = "removing_old_version"
                if warnings:
                    result["warnings"] = warnings
                    result["warning_stage"] = warning_retry_stage(result, request)
                    result.pop("retry_from_stage", None)
                    self.store.finish_import(
                        task["id"],
                        "completed_with_warning",
                        "completed_with_warning",
                        result=result,
                        error="; ".join(warnings),
                        info_hash=info_hash,
                        msg_media_id=media_id,
                    )
                else:
                    result.pop("warnings", None)
                    result.pop("warning_stage", None)
                    result.pop("retry_from_stage", None)
                    self.store.finish_import(
                        task["id"],
                        "completed",
                        "completed",
                        result=result,
                        info_hash=info_hash,
                        msg_media_id=media_id,
                    )
                if post_enhancement_pending:
                    current = self.store.get_import(task["owner_id"], task["id"])
                    self._schedule_post_enhancement(current)
                return
        finally:
            if hold_target_lock:
                target_lock.release()

    def _retry_upgrade_cleanup(self, task, result, info_hash, target):
        request = task["request"]
        upgrade_media_id = str(request.get("upgrade_media_id") or "").strip()
        media_id = str(task.get("msg_media_id") or result.get("msg_media_id") or "").strip()
        if not upgrade_media_id or bool(request.get("keep_old_version", True)):
            raise RuntimeError("upgrade cleanup retry is no longer applicable")
        if not media_id:
            raise RuntimeError("upgrade cleanup retry is missing the new MediaStationGo media id")
        self.store.save_running(
            task["id"],
            "removing_old_version",
            result=result,
            info_hash=info_hash,
            msg_media_id=media_id,
        )
        try:
            result["upgrade_cleanup"] = self.service.remove_upgrade_target(
                upgrade_media_id,
                media_id,
                target,
                upgrade_scope=str(request.get("upgrade_scope") or "media"),
                new_source_paths=upgrade_new_source_paths(result.get("task")),
                category=request.get("category"),
            )
        except Exception as exc:
            warning = "旧片源移入回收站失败: %s" % exc
            result.pop("upgrade_cleanup", None)
            result["warnings"] = [warning]
            result["warning_stage"] = "removing_old_version"
            result.pop("retry_from_stage", None)
            self.store.finish_import(
                task["id"],
                "completed_with_warning",
                "completed_with_warning",
                result=result,
                error=warning,
                info_hash=info_hash,
                msg_media_id=media_id,
            )
            return
        result.pop("warnings", None)
        result.pop("warning_stage", None)
        result.pop("retry_from_stage", None)
        self.store.finish_import(
            task["id"],
            "completed",
            "completed",
            result=result,
            info_hash=info_hash,
            msg_media_id=media_id,
        )

    def _execute_subscription_follow_task(self, task):
        request = task["request"]
        follow = dict(request.get("subscription_follow") or {})
        result = dict(task.get("result") or {})
        audit = dict(result.get("subscription_follow") or {})
        category = request["category"]
        title = str(request["title"]).strip()
        target_path = follow["target_openlist_path"]
        season = int(follow["season"])
        existing = {int(value) for value in follow.get("existing_episodes") or []}
        reserved = {int(value) for value in follow.get("reserved_episodes") or []}
        expected_episodes = {int(value) for value in follow.get("expected_episodes") or []}
        file_episode_hints = set()
        if follow.get("manual_replenish") and existing:
            upper_bound = max(existing)
            file_episode_hints.update(
                episode
                for episode in range(1, upper_bound + 1)
                if episode not in existing and episode not in reserved
            )

        def inspect_staging(current_staging):
            if expected_episodes:
                return self.service.inspect_subscription_staging(
                    category, current_staging, season, file_episode_hints, expected_episodes
                )
            return self.service.inspect_subscription_staging(
                category, current_staging, season, file_episode_hints
            )

        info_hash = str(task.get("info_hash") or "").strip()
        offline_task = dict(result.get("task") or {})
        target_lock = self._target_lock(
            {
                "library_id": "subscription:%s" % follow["work_key"],
                "root_id": "season:%s" % season,
            }
        )
        self._acquire_target_lock(target_lock)
        try:
            self._raise_if_stopping()
            self._raise_if_cancel_requested(task["owner_id"], task["id"], category, info_hash)
            attempts = list(audit.get("attempts") or [])
            attempt_number = int(task.get("attempt_count") or 1)
            if not attempts or int(attempts[-1].get("attempt") or 0) != attempt_number:
                attempts.append({"attempt": attempt_number, "started_at": int(time.time())})
            audit.update(
                {
                    "subscription_id": follow["subscription_id"],
                    "manual_replenish": bool(follow.get("manual_replenish")),
                    "work_key": follow["work_key"],
                    "season": season,
                    "title_class": follow["title_class"],
                    "baseline_episodes": sorted(existing),
                    "reserved_episodes": sorted(reserved),
                    "expected_episodes": sorted(expected_episodes),
                    "target_openlist_path": target_path,
                    "attempts": attempts,
                }
            )

            resumed_episodes = {int(value) for value in audit.get("moved_episodes") or []}
            if resumed_episodes:
                staging = dict(audit.get("staging") or {})
                if not staging.get("openlist_path"):
                    raise RuntimeError("promoted subscription retry is missing staging state")
                offline_task = reset_subscription_sync_for_retry(offline_task)
                result["task"] = offline_task
                self._complete_subscription_promotion(
                    task,
                    request,
                    result,
                    audit,
                    attempts,
                    category,
                    title,
                    target_path,
                    season,
                    info_hash,
                    offline_task,
                    staging,
                    resumed_episodes,
                    require_scan_added=False,
                )
                return

            def persist_blocked_source(
                error, block_reason=None, outcome="source_unavailable", block_source=True
            ):
                block_reason = block_reason or subscription_source_failure_block_reason(error)
                if not block_reason:
                    return False
                audit["outcome"] = outcome
                attempts[-1].update(
                    {
                        "outcome": outcome,
                        "error": str(error),
                        "finished_at": int(time.time()),
                    }
                )
                result["subscription_follow"] = audit
                self.store.save_running(task["id"], "cleanup", result=result, info_hash=info_hash or None)
                cleanup = self._with_subscription_staging_lock(
                    follow["work_key"],
                    lambda: self.service.cleanup_subscription_staging(category, staging),
                )
                record_subscription_staging_cleanup(audit, cleanup, outcome)
                if block_source and not follow.get("manual_replenish"):
                    source_key = subscription_source_block_key(
                        (request.get("candidate") or {}).get("download_uri")
                    )
                    audit["source_block"] = self.store.block_subscription_source(
                        source_key,
                        reason=block_reason,
                        origin_import_id=task["id"],
                    )
                result["subscription_follow"] = audit
                self.store.save_running(task["id"], "cleanup", result=result, info_hash=info_hash or None)
                return True

            staging = dict(audit.get("staging") or {})
            if audit.get("staging_cleaned_at"):
                staging = {}
            if not staging.get("openlist_path") or not subscription_receive_target_folder_id(staging):
                staging = self._with_subscription_staging_lock(
                    follow["work_key"],
                    lambda: self.service.prepare_subscription_staging(
                        category,
                        task["id"],
                        follow["work_key"],
                    ),
                )
                audit["staging"] = staging
                audit.pop("staging_cleaned_at", None)
                audit.pop("staging_cleanup", None)
                result["subscription_follow"] = audit
                self.store.save_running(task["id"], "staging", result=result, info_hash=info_hash or None)

            direct_receive = subscription_receives_directly_to_staging(staging)
            if direct_receive and not staging.get("claimed_at"):
                if subscription_direct_receive_scan_ready(result):
                    staging_state = inspect_staging(staging)
                    has_staged_files = bool(staging_state.get("entries"))
                else:
                    # While 115 is still active, the receive directory is not
                    # authoritative yet. Avoid an early OpenList scan here:
                    # a transient listing/TLS error must not abort the 115
                    # wait before the shared long-offline timeout is reached.
                    has_staged_files = False
            else:
                staging_state = inspect_staging(staging)
                has_staged_files = bool(staging_state.get("entries"))
            if not staging.get("claimed_at"):
                receive_lock = None if direct_receive else self._subscription_receive_lock
                if receive_lock is not None:
                    self._acquire_target_lock(receive_lock)
                try:
                    if direct_receive:
                        if subscription_direct_receive_scan_ready(result):
                            staging_state = inspect_staging(staging)
                            has_staged_files = bool(staging_state.get("entries"))
                        else:
                            has_staged_files = False
                    else:
                        staging_state = inspect_staging(staging)
                        has_staged_files = bool(staging_state.get("entries"))
                    if has_staged_files:
                        self.service.validate_subscription_receive_root(staging)
                        staging["recovered_at"] = int(time.time())
                        staging["claimed_at"] = staging["recovered_at"]
                        audit["staging"] = staging
                        result["subscription_follow"] = audit
                        self.store.save_running(task["id"], "staging", result=result, info_hash=info_hash or None)
                    else:
                        try:
                            submit_result = dict(result.get("submit") or {})
                            if (
                                submit_result
                                and direct_receive
                                and result_task_is_offline_success(result)
                            ):
                                retry = dict(result.get("subscription_retry") or {})
                                retry.update(
                                    {
                                        "previous_info_hash": str(info_hash or "").strip(),
                                        "previous_submit_kind": str(
                                            submit_result.get("submit_kind") or ""
                                        ).strip(),
                                        "previous_outcome": str(audit.get("outcome") or "").strip(),
                                    }
                                )
                                result.pop("submit", None)
                                result.pop("task", None)
                                result["subscription_retry"] = retry
                                submit_result = {}
                                offline_task = {}
                                info_hash = ""
                            if not submit_result:
                                retry = dict(result.get("subscription_retry") or {})
                                previous_info_hash = str(
                                    retry.get("previous_info_hash") or ""
                                ).strip()
                                if (
                                    previous_info_hash
                                    and retry.get("previous_submit_kind") == "115_offline"
                                    and not retry.get("offline_task_reset_at")
                                ):
                                    self.store.save_running(
                                        task["id"], "resetting_download", result=result
                                    )
                                    retry["offline_task_reset"] = self.service.reset_task_for_resubmit(
                                        category, previous_info_hash
                                    )
                                    retry["offline_task_reset_at"] = int(time.time())
                                    result["subscription_retry"] = retry
                                    self.store.save_running(
                                        task["id"], "resetting_download", result=result
                                    )
                                self.service.validate_subscription_receive_root(staging)
                                self.store.save_running(task["id"], "submitting", result=result)
                                submit_result = self.service.submit(
                                    category,
                                    request["candidate"]["download_uri"],
                                    target_folder_id=subscription_receive_target_folder_id(staging),
                                )
                                info_hash = first_submit_info_hash(submit_result)
                                if not info_hash:
                                    raise RuntimeError("pipeline submit returned no info_hash")
                                offline_task = task_from_submit_result(submit_result, info_hash)
                                result.update({"submit": submit_result, "task": offline_task})
                                self.store.save_running(
                                    task["id"],
                                    "received_unclaimed",
                                    result=result,
                                    info_hash=info_hash,
                                )
                            elif not info_hash:
                                info_hash = first_submit_info_hash(submit_result)
                            if not info_hash:
                                raise RuntimeError("pipeline submit returned no info_hash")
                            if not offline_task:
                                offline_task = task_from_submit_result(submit_result, info_hash)
                            if audit.get("offline_info_hash") != info_hash:
                                audit["offline_info_hash"] = info_hash
                                audit["offline_started_at"] = int(time.time())
                                result["subscription_follow"] = audit
                                self.store.save_running(
                                    task["id"], "waiting_download", result=result, info_hash=info_hash
                                )
                            offline_wait_deadline = time.monotonic() + self.offline_wait_slice_seconds
                            while offline_task.get("status_name") != OFFLINE_SUCCESS_STATUS:
                                status_name = offline_task.get("status_name")
                                if status_name in {"cancelled", "canceled"}:
                                    raise ImportCanceled("import task canceled")
                                if status_name in OFFLINE_FAILED_STATUSES:
                                    raise RuntimeError(format_offline_task_failure(offline_task))
                                if status_name not in OFFLINE_ACTIVE_STATUSES:
                                    raise RuntimeError(
                                        "115 offline task returned invalid status: %s" % (status_name or "-")
                                    )
                                self._raise_if_stopping()
                                self._raise_if_cancel_requested(
                                    task["owner_id"], task["id"], category, info_hash
                                )
                                if subscription_offline_wait_timed_out(audit):
                                    cancel_result = self.service.cancel_task(category, info_hash)
                                    offline_task = dict(cancel_result.get("task") or offline_task)
                                    offline_task.setdefault("info_hash", info_hash)
                                    result["task"] = offline_task
                                    self.store.save_running(
                                        task["id"], "waiting_download", result=result, info_hash=info_hash
                                    )
                                    error = RuntimeError(
                                        "115 offline task exceeded %s seconds and was canceled"
                                        % ACTIVE_115_TIMEOUT_SECONDS
                                    )
                                    persist_blocked_source(
                                        error,
                                        block_reason="offline_failed",
                                        outcome="source_unavailable",
                                    )
                                    raise ImportCanceled("import task canceled")
                                self.store.save_running(
                                    task["id"], "waiting_download", result=result, info_hash=info_hash
                                )
                                previous_poll_attempt = offline_task_poll_attempt(offline_task)
                                offline_task = self.service.task_status(category, info_hash)
                                if not isinstance(offline_task, dict):
                                    raise RuntimeError("pipeline task_status returned invalid response")
                                offline_task = dict(offline_task)
                                offline_task.setdefault("info_hash", info_hash)
                                mark_offline_task_polled(offline_task, previous_poll_attempt)
                                result["task"] = offline_task
                                self.store.save_running(
                                    task["id"], "waiting_download", result=result, info_hash=info_hash
                                )
                                if offline_task.get("status_name") != OFFLINE_SUCCESS_STATUS:
                                    if time.monotonic() >= offline_wait_deadline:
                                        raise OfflineWaitDeferred()
                                    self._wait_or_stop(offline_task_poll_delay(offline_task, self.poll_seconds))

                            manifest = dict(audit.get("resource_manifest") or {})
                            if str(manifest.get("info_hash") or "") != info_hash:
                                inspection_args = {
                                    "episode_hints": file_episode_hints,
                                }
                                if expected_episodes:
                                    inspection_args["expected_episodes"] = expected_episodes
                                manifest = self.service.inspect_subscription_submit_result(
                                    category, submit_result, offline_task, season, **inspection_args
                                )
                                manifest["info_hash"] = info_hash
                                audit["resource_manifest"] = manifest
                                result["subscription_follow"] = audit
                            offline_task["received_item_count"] = int(
                                manifest.get("top_level_item_count") or 0
                            )
                            result["task"] = offline_task
                            self.store.save_running(
                                task["id"],
                                "verifying_source",
                                result=result,
                                info_hash=info_hash,
                            )

                            def check_receive_wait():
                                self._raise_if_stopping()
                                self._raise_if_cancel_requested(
                                    task["owner_id"], task["id"], category, info_hash
                                )

                            staging = self.service.claim_subscription_transfer(
                                staging,
                                submit_result,
                                completed_task=offline_task,
                                stop_check=check_receive_wait,
                            )
                        except Exception as exc:
                            persist_blocked_source(exc)
                            raise
                        audit["staging"] = staging
                        result["subscription_follow"] = audit
                        self.store.save_running(task["id"], "staging", result=result, info_hash=info_hash)
                finally:
                    if receive_lock is not None:
                        receive_lock.release()
            staging_state = inspect_staging(staging)
            has_staged_files = bool(staging_state.get("entries"))
            if not info_hash and has_staged_files:
                info_hash = "subscription-staging:%s" % task["id"]
                offline_task = {
                    "info_hash": info_hash,
                    "status_name": OFFLINE_SUCCESS_STATUS,
                    "source_kind": "subscription_staging_recovery",
                    "percent_done": 100,
                }
                result["task"] = offline_task
                self.store.save_running(task["id"], "submitted", result=result, info_hash=info_hash)

            self.store.save_running(task["id"], "verifying_staging", result=result, info_hash=info_hash)
            staging_state = inspect_staging(staging)
            verified = {int(value) for value in staging_state.get("verified_episodes") or []}
            source_manifest = audit.get("resource_manifest") or {}
            source_verified = {
                int(value)
                for value in source_manifest.get("verified_episodes") or []
            }
            if source_manifest and source_verified != verified:
                raise RuntimeError(
                    "OpenList staging episode manifest does not match the 115 resource manifest"
                )
            audit.update(
                {
                    "verified_episodes": sorted(verified),
                    "unknown_videos": list(staging_state.get("unknown_videos") or []),
                    "duplicate_episodes": dict(staging_state.get("duplicate_episodes") or {}),
                }
            )
            if follow.get("manual_replenish"):
                selected = verified & file_episode_hints
            else:
                selected = verified - existing - reserved
            audit["selected_episodes"] = sorted(selected)
            if staging_state.get("ambiguous_videos"):
                audit["ambiguous_videos"] = list(staging_state.get("ambiguous_videos") or [])
                error = RuntimeError("OpenList staging has multiple videos without a clear primary video")
                persist_blocked_source(
                    error,
                    block_reason="invalid_episode_layout",
                    outcome="rejected",
                    block_source=False,
                )
                raise error
            if audit["unknown_videos"] and not selected:
                error = RuntimeError("OpenList staging contains unrecognized video names")
                persist_blocked_source(
                    error,
                    block_reason="invalid_episode_layout",
                    outcome="rejected",
                    block_source=False,
                )
                raise error
            if audit["unknown_videos"]:
                audit["ignored_unknown_videos"] = list(audit["unknown_videos"])
            if audit["duplicate_episodes"]:
                error = RuntimeError("OpenList staging contains multiple videos for one episode")
                persist_blocked_source(
                    error,
                    block_reason="invalid_episode_layout",
                    outcome="rejected",
                    block_source=False,
                )
                raise error
            if not selected:
                audit["outcome"] = "no_new_episodes"
                attempts[-1].update({"outcome": "no_new_episodes", "finished_at": int(time.time())})
                result["subscription_follow"] = audit
                self.store.save_running(task["id"], "cleanup", result=result, info_hash=info_hash)
                cleanup = self._with_subscription_staging_lock(
                    follow["work_key"],
                    lambda: self.service.cleanup_subscription_staging(category, staging),
                )
                record_subscription_staging_cleanup(audit, cleanup, "no_new_episodes")
                if follow.get("manual_replenish"):
                    result["subscription_follow"] = audit
                    self.store.finish_import(
                        task["id"],
                        "completed",
                        "completed",
                        result=result,
                        info_hash=info_hash,
                    )
                    return
                result["subscription_follow"] = audit
                self.store.save_running(task["id"], "cleanup", result=result, info_hash=info_hash)
                raise RuntimeError("OpenList staging contains no new episodes")

            plan = self.service.plan_subscription_promotion(staging_state, selected, season)
            audit["planned_files"] = list(plan.get("files") or [])
            result["subscription_follow"] = audit
            self.store.save_running(task["id"], "promoting", result=result, info_hash=info_hash)
            promotion = self.service.promote_subscription_episodes(
                category,
                staging_state,
                target_path,
                selected,
                season,
                plan=plan,
            )
            audit["promotion"] = promotion
            audit["moved_episodes"] = list(promotion.get("moved_episodes") or [])
            result["subscription_follow"] = audit
            self.service.refresh_subscription_target(target_path)

            self._complete_subscription_promotion(
                task,
                request,
                result,
                audit,
                attempts,
                category,
                title,
                target_path,
                season,
                info_hash,
                offline_task,
                staging,
                selected,
                require_scan_added=True,
            )
        finally:
            target_lock.release()

    def _complete_subscription_promotion(
        self,
        task,
        request,
        result,
        audit,
        attempts,
        category,
        title,
        target_path,
        season,
        info_hash,
        offline_task,
        staging,
        selected,
        require_scan_added,
    ):
        sync_input = dict(offline_task)
        sync_input["subscription_target_openlist_path"] = target_path
        if category == "anime":
            sync_input["subscription_target_season"] = season

        def save_progress(progress):
            current = dict(result)
            current["task"] = dict(progress or {})
            current["subscription_follow"] = audit
            media_id = str((progress or {}).get("msg_media_id") or "").strip()
            self.store.save_running(
                task["id"],
                sync_stage(progress),
                result=current,
                info_hash=info_hash,
                msg_media_id=media_id,
            )

        self.store.save_running(task["id"], "scanning", result=result, info_hash=info_hash)
        def sync_once(current_task):
            synced_result = self.service.sync_completed_task(
                category,
                title,
                current_task,
                progress_callback=save_progress,
                target=request["target"],
            )
            if not isinstance(synced_result, dict):
                raise RuntimeError("pipeline sync_completed_task returned invalid response")
            synced_result = dict(synced_result)
            result["task"] = synced_result
            return (
                synced_result,
                str(synced_result.get("msg_media_id") or "").strip(),
                int(synced_result.get("msg_ingest_scan_added") or 0),
            )

        synced, media_id, scan_added = sync_once(sync_input)
        audit["scan_added"] = scan_added
        verified_msg = self.service.verify_subscription_msg_episodes(
            category, target_path, season, selected
        )
        audit["msg_verification"] = verified_msg
        missing_episodes = list(verified_msg.get("missing_episodes") or [])
        duplicate_episodes = dict(verified_msg.get("duplicate_episodes") or {})
        scan_mismatch = require_scan_added and scan_added != len(selected)
        if (missing_episodes or scan_mismatch) and not duplicate_episodes:
            for retry_number in range(1, SUBSCRIPTION_SCAN_VISIBILITY_RETRY_LIMIT + 1):
                self._wait_or_stop(SUBSCRIPTION_SCAN_VISIBILITY_RETRY_DELAY_SECONDS)
                retry_input = reset_subscription_sync_for_retry(synced)
                retry_input["subscription_target_openlist_path"] = target_path
                if category == "anime":
                    retry_input["subscription_target_season"] = season
                synced, media_id, scan_added = sync_once(retry_input)
                audit["scan_visibility_retry_count"] = retry_number
                audit["scan_added"] = scan_added
                verified_msg = self.service.verify_subscription_msg_episodes(
                    category, target_path, season, selected
                )
                audit["msg_verification"] = verified_msg
                missing_episodes = list(verified_msg.get("missing_episodes") or [])
                duplicate_episodes = dict(verified_msg.get("duplicate_episodes") or {})
                scan_mismatch = require_scan_added and scan_added != len(selected)
                if not missing_episodes and not duplicate_episodes and not scan_mismatch:
                    break
        if duplicate_episodes or missing_episodes or scan_mismatch:
            audit["outcome"] = "failed"
            result["subscription_follow"] = audit
            self.store.save_running(
                task["id"],
                "verifying_scan",
                result=result,
                info_hash=info_hash,
                msg_media_id=media_id,
            )
            if duplicate_episodes or missing_episodes:
                raise RuntimeError("MediaStationGo episode verification did not match the promotion plan")
            raise RuntimeError(
                "MediaStationGo scan added %d episodes, expected %d"
                % (scan_added, len(selected))
            )
        self.store.save_running(
            task["id"],
            "cleanup",
            result=result,
            info_hash=info_hash,
            msg_media_id=media_id,
        )
        if not audit.get("staging_cleaned_at"):
            cleanup = self._with_subscription_staging_lock(
                request["subscription_follow"]["work_key"],
                lambda: self.service.cleanup_subscription_staging(category, staging),
            )
            record_subscription_staging_cleanup(audit, cleanup, "import_completed")
        audit["outcome"] = "imported"
        attempts[-1].update({"outcome": "imported", "finished_at": int(time.time())})
        result["subscription_follow"] = audit
        result["msg_media_id"] = media_id
        self.store.finish_import(
            task["id"],
            "completed",
            "completed",
            result=result,
            info_hash=info_hash,
            msg_media_id=media_id,
        )

    def _raise_if_cancel_requested(self, owner_id, import_id, category, info_hash):
        current = self.store.get_import(owner_id, import_id)
        if not current.get("cancel_requested"):
            return
        if info_hash:
            self.service.cancel_task(category, info_hash)
        raise ImportCanceled("import task canceled")

    def _wait_or_stop(self, delay_seconds=None):
        delay = self.poll_seconds if delay_seconds is None else max(0.01, float(delay_seconds))
        if self._stop_event.wait(delay):
            raise WorkerStopping("internal API worker stopping")

    def _raise_if_stopping(self):
        if self._stop_event.is_set():
            raise WorkerStopping("internal API worker stopping")

    def _target_lock(self, target):
        key = (str(target["library_id"]), str(target["root_id"]))
        with self._target_locks_guard:
            return self._target_locks.setdefault(key, threading.Lock())

    def _acquire_target_lock(self, lock):
        while not lock.acquire(timeout=0.25):
            self._raise_if_stopping()


class SubtitleAsrTaskManager:
    def __init__(self, processor, store, poll_seconds=1.0):
        self.processor = processor
        self.store = store
        self.poll_seconds = max(0.05, float(poll_seconds))
        self._condition = threading.Condition()
        self._stop_event = threading.Event()
        self._thread = None

    def start(self):
        with self._condition:
            if self._thread is not None:
                return
            self._stop_event.clear()
            self.store.recover_running_subtitle_asr_tasks()
            self._thread = threading.Thread(
                target=self._worker_loop,
                name="internal-api-subtitle-asr",
                daemon=True,
            )
            self._thread.start()

    def stop(self):
        self._stop_event.set()
        with self._condition:
            self._condition.notify_all()
        if self._thread is not None:
            self._thread.join(timeout=5)
            self._thread = None

    def notify(self):
        with self._condition:
            self._condition.notify_all()

    def create_task(self, payload):
        if not isinstance(payload, dict):
            raise ApiError(400, "invalid_request", "request body must be a JSON object")
        owner_id = require_text(payload.get("owner_id"), "owner_id", max_length=200)
        media_id = require_text(payload.get("media_id"), "media_id", max_length=200)
        source_language = str(payload.get("source_language") or "ja").strip().lower()
        if source_language not in {"auto", "ja", "en", "zh", "ko"}:
            raise ApiError(400, "invalid_source_language", "source_language must be one of: auto, ja, en, zh, ko")
        translation_provider = str(payload.get("translation_provider") or "local").strip().lower()
        translation_model = str(
            payload.get("translation_model")
            or getattr(getattr(self.processor, "config", None), "asr_translation_model", "")
        ).strip()
        asr_model = str(
            payload.get("asr_model")
            or getattr(getattr(self.processor, "config", None), "asr_model", DEFAULT_ASR_MODEL)
        ).strip()
        try:
            self.processor.ensure_available(translation_provider, translation_model, asr_model)
        except (RuntimeError, OSError, TimeoutError, ValueError) as exc:
            raise ApiError(503, "subtitle_asr_unavailable", str(exc))
        task, created = self.store.create_subtitle_asr_task(
            owner_id,
            media_id,
            source_language,
            asr_model,
            translation_provider,
            translation_model,
        )
        if created:
            self.notify()
        return task, created

    def get_task(self, owner_id, task_id):
        return self.store.get_subtitle_asr_task(
            require_text(owner_id, "owner_id", max_length=200),
            require_text(task_id, "task_id", max_length=200),
        )

    def list_tasks(self, limit=50):
        return self.store.list_subtitle_asr_tasks(limit)

    def list_models(self):
        try:
            return self.processor.translation_models()
        except (RuntimeError, OSError, TimeoutError, ValueError) as exc:
            raise ApiError(503, "subtitle_asr_unavailable", str(exc))

    def list_asr_models(self):
        try:
            return self.processor.asr_models()
        except (RuntimeError, OSError, TimeoutError, ValueError) as exc:
            raise ApiError(503, "subtitle_asr_unavailable", str(exc))

    def retry_task(self, owner_id, task_id, payload):
        owner_id = require_text(owner_id, "owner_id", max_length=200)
        task_id = require_text(task_id, "task_id", max_length=200)
        if payload is None:
            payload = {}
        if not isinstance(payload, dict):
            raise ApiError(400, "invalid_request", "request body must be a JSON object")
        current = self.store.get_subtitle_asr_task(owner_id, task_id)
        if current["status"] != "failed":
            raise ApiError(
                409,
                "subtitle_asr_not_retryable",
                "only failed AI subtitle tasks can be retried",
            )
        provider = str(
            payload.get("translation_provider")
            or current.get("translation_provider")
            or "local"
        ).strip().lower()
        model = str(
            payload.get("translation_model")
            or current.get("translation_model")
            or getattr(getattr(self.processor, "config", None), "asr_translation_model", "")
        ).strip()
        asr_model = str(
            payload.get("asr_model")
            or current.get("asr_model")
            or getattr(getattr(self.processor, "config", None), "asr_model", DEFAULT_ASR_MODEL)
        ).strip()
        try:
            audio_cached, transcript_cached = self.processor.cache_state(
                task_id, asr_model, current["media_id"]
            )
            if audio_cached and transcript_cached:
                self.processor.ensure_translation_available(provider, model)
            else:
                self.processor.ensure_available(provider, model, asr_model)
        except (RuntimeError, OSError, TimeoutError, ValueError) as exc:
            raise ApiError(503, "subtitle_asr_unavailable", str(exc))
        task = self.store.retry_subtitle_asr_task(
            owner_id,
            task_id,
            asr_model,
            provider,
            model,
            audio_cached,
            transcript_cached,
        )
        self.notify()
        return task

    def update_task_model(self, owner_id, task_id, payload):
        owner_id = require_text(owner_id, "owner_id", max_length=200)
        task_id = require_text(task_id, "task_id", max_length=200)
        if not isinstance(payload, dict):
            raise ApiError(400, "invalid_request", "request body must be a JSON object")
        provider = require_text(
            payload.get("translation_provider"), "translation_provider", max_length=50
        ).lower()
        model = require_text(
            payload.get("translation_model"), "translation_model", max_length=200
        )
        current = self.store.get_subtitle_asr_task(owner_id, task_id)
        asr_model = require_text(
            payload.get("asr_model") or current.get("asr_model") or DEFAULT_ASR_MODEL,
            "asr_model",
            max_length=200,
        )
        if current["status"] != "queued":
            raise ApiError(
                409,
                "subtitle_asr_model_not_editable",
                "only queued AI subtitle tasks can change ASR or translation model",
            )
        try:
            self.processor.ensure_available(provider, model, asr_model)
        except (RuntimeError, OSError, TimeoutError, ValueError) as exc:
            raise ApiError(503, "subtitle_asr_unavailable", str(exc))
        return self.store.update_queued_subtitle_asr_task_model(
            owner_id, task_id, asr_model, provider, model
        )

    def cancel_task(self, owner_id, task_id):
        owner_id = require_text(owner_id, "owner_id", max_length=200)
        task_id = require_text(task_id, "task_id", max_length=200)
        task = self.store.cancel_queued_subtitle_asr_task(owner_id, task_id)
        try:
            self.processor.delete_cache(
                task_id,
                task["media_id"],
                self.store.list_subtitle_asr_task_ids_for_media(task["media_id"]),
            )
        except (OSError, RuntimeError) as exc:
            raise ApiError(500, "subtitle_asr_cache_delete_failed", str(exc))
        return self.store.clear_canceled_subtitle_asr_cache_state(owner_id, task_id)

    def retranslate_task(self, owner_id, task_id, payload):
        owner_id = require_text(owner_id, "owner_id", max_length=200)
        task_id = require_text(task_id, "task_id", max_length=200)
        if not isinstance(payload, dict):
            raise ApiError(400, "invalid_request", "request body must be a JSON object")
        provider = require_text(
            payload.get("translation_provider"), "translation_provider", max_length=50
        ).lower()
        model = require_text(
            payload.get("translation_model"), "translation_model", max_length=200
        )
        current = self.store.get_subtitle_asr_task(owner_id, task_id)
        if current["status"] != "completed":
            raise ApiError(
                409,
                "subtitle_asr_not_retranslatable",
                "only completed AI subtitle tasks can be retranslated",
            )
        try:
            self.processor.ensure_translation_available(provider, model)
            audio_cached, transcript_cached = self.processor.cache_state(
                task_id, current.get("asr_model"), current["media_id"]
            )
        except (RuntimeError, OSError, TimeoutError, ValueError) as exc:
            raise ApiError(503, "subtitle_asr_unavailable", str(exc))
        task = self.store.retranslate_completed_subtitle_asr_task(
            owner_id,
            task_id,
            provider,
            model,
            audio_cached,
            transcript_cached,
        )
        self.notify()
        return task

    def delete_task(self, owner_id, task_id):
        owner_id = require_text(owner_id, "owner_id", max_length=200)
        task_id = require_text(task_id, "task_id", max_length=200)
        current = self.store.get_subtitle_asr_task(owner_id, task_id)
        if current["status"] in {"queued", "running"}:
            raise ApiError(409, "subtitle_asr_active", "active AI subtitle tasks cannot be deleted")
        try:
            self.processor.delete_cache(
                task_id,
                current["media_id"],
                self.store.list_subtitle_asr_task_ids_for_media(current["media_id"]),
            )
        except (OSError, RuntimeError) as exc:
            raise ApiError(500, "subtitle_asr_cache_delete_failed", str(exc))
        self.store.delete_subtitle_asr_task(owner_id, task_id)

    def _worker_loop(self):
        while not self._stop_event.is_set():
            try:
                task = self.store.claim_next_subtitle_asr_task()
            except sqlite3.Error as exc:
                print("internal API subtitle ASR worker claim failed: %s" % exc, flush=True)
                with self._condition:
                    self._condition.wait(timeout=self.poll_seconds)
                continue
            if task is None:
                with self._condition:
                    self._condition.wait(timeout=self.poll_seconds)
                continue
            try:
                legacy_audio_task_ids = self.store.list_subtitle_asr_task_ids_for_media(
                    task["media_id"]
                )
                result = self.processor.run(
                    task["id"],
                    task["media_id"],
                    task["source_language"],
                    asr_model=task["asr_model"],
                    translation_provider=task["translation_provider"],
                    translation_model=task["translation_model"],
                    progress_callback=lambda stage, current, total: self.store.save_subtitle_asr_progress(
                        task["id"], stage, current, total
                    ),
                    cache_callback=lambda audio, transcript: self.store.save_subtitle_asr_cache(
                        task["id"], audio, transcript
                    ),
                    legacy_audio_task_ids=legacy_audio_task_ids,
                )
                self.store.finish_subtitle_asr_task(
                    task["id"], "completed", "completed", result=result
                )
            except Exception as exc:
                print("internal API subtitle ASR %s failed: %s" % (task["id"], exc), flush=True)
                self.store.finish_subtitle_asr_task(
                    task["id"], "failed", "failed", error=str(exc)
                )


class InternalApiApplication:
    def __init__(self, service, store, manager, subtitle_asr_manager=None, season_subtitle_manager=None):
        self.service = service
        self.store = store
        self.manager = manager
        self.subtitle_asr_manager = subtitle_asr_manager
        self.season_subtitle_manager = season_subtitle_manager

    def search(self, payload):
        if not isinstance(payload, dict):
            raise ApiError(400, "invalid_request", "request body must be a JSON object")
        owner_id = require_text(payload.get("owner_id"), "owner_id", max_length=200)
        query = require_text(payload.get("query"), "query", max_length=1000)
        category = require_category(payload.get("category"))
        source = str(payload.get("source") or "default").strip().lower()
        if source not in VALID_SEARCH_SOURCES:
            raise ApiError(400, "invalid_source", "source must be one of: default, pansou, bt4g")
        try:
            limit = int(payload.get("limit") or 20)
        except (TypeError, ValueError):
            raise ApiError(400, "invalid_limit", "limit must be an integer")
        if limit < 1 or limit > 200:
            raise ApiError(400, "invalid_limit", "limit must be between 1 and 200")
        subscription_follow = optional_bool(
            payload.get("subscription_follow"), "subscription_follow", default=False
        )
        try:
            capabilities = self.service.search_capabilities()
        except Exception as exc:
            raise ApiError(502, "capability_check_failed", str(exc))
        if not isinstance(capabilities, dict):
            raise ApiError(502, "capability_check_failed", "pipeline capabilities returned invalid response")
        error_details = {"capabilities": capabilities}
        search_limit = 200 if subscription_follow else limit
        try:
            if source == "pansou":
                result = self.service.search_pansou(query, limit=search_limit)
            elif source == "bt4g":
                result = self.service.search_bt4g(query, limit=search_limit)
            elif subscription_follow:
                result = self.service.search_subscription_follow(query, category, limit=search_limit)
            else:
                result = self.service.search(query, category, limit=search_limit)
        except ApiError as exc:
            exc.details.setdefault("capabilities", capabilities)
            raise
        except Exception as exc:
            raise ApiError(502, "search_failed", str(exc), error_details)
        if not isinstance(result, list):
            raise ApiError(502, "search_failed", "pipeline search returned invalid response", error_details)
        for item in result:
            if not isinstance(item, dict):
                raise ApiError(502, "search_failed", "pipeline search returned an invalid candidate", error_details)
        metadata = dict(getattr(result, "metadata", {}) or {})
        blocked_count = 0
        if subscription_follow:
            available = []
            for item in result:
                source_key = subscription_source_block_key(item.get("download_uri"))
                if subscription_source_block_is_effective(
                    self.store.get_subscription_source_block(source_key)
                ):
                    blocked_count += 1
                    continue
                available.append(item)
            result = available[:limit]
        metadata.update(
            {
                "source": source,
                "category": category,
                "selected_count": len(result),
                "blocked_count": blocked_count,
                "subscription_follow": subscription_follow,
                "capabilities": capabilities,
            }
        )
        session_id, expires_at, items = self.store.save_search(owner_id, query, category, source, result, metadata)
        metadata.update({"session_id": session_id, "expires_at": expires_at})
        return {
            "session_id": session_id,
            "expires_at": expires_at,
            "items": items,
            "metadata": metadata,
            "capabilities": capabilities,
        }

    def prepare_manual_candidate(self, payload):
        if not isinstance(payload, dict):
            raise ApiError(400, "invalid_request", "request body must be a JSON object")
        owner_id = require_text(payload.get("owner_id"), "owner_id", max_length=200)
        input_text = require_text(payload.get("input"), "input", max_length=4096)
        title = require_text(payload.get("title"), "title", max_length=200)
        category = require_category(payload.get("category"))
        candidate = share115_candidate_from_text(input_text)
        if candidate is None:
            magnet_input = input_text
            if input_text.strip().casefold().startswith("magnet:?"):
                if any(token in input_text for token in ("\r", "\n", "\t")):
                    raise ApiError(400, "invalid_magnet", "磁链必须为单行完整链接")
                magnet_input = input_text.strip().replace(" ", "%20")
            candidate = magnet_candidate_from_text(magnet_input)
            if candidate is not None and not valid_btih_info_hash(candidate.get("infoHash")):
                raise ApiError(400, "invalid_magnet", "磁链缺少有效的 BTIH")
        if candidate is None:
            candidate = ed2k_candidate_from_text(input_text)
            if candidate is None and "ed2k://" in input_text.casefold():
                raise ApiError(400, "invalid_ed2k", "ED2K 文件链接格式无效")
        if candidate is None:
            raise ApiError(400, "unsupported_manual_input", "仅支持 115 分享链接、ED2K 或 BTIH 磁链")
        candidate = dict(candidate)
        candidate["title"] = title
        candidate["summary"] = "任务名称由用户填写"
        metadata = {
            "source": "manual",
            "category": category,
            "selected_count": 1,
            "manual_kind": candidate.get("source_kind"),
        }
        session_id, expires_at, items = self.store.save_search(
            owner_id, candidate["title"], category, "manual", [candidate], metadata
        )
        metadata.update({"session_id": session_id, "expires_at": expires_at})
        return {
            "session_id": session_id,
            "expires_at": expires_at,
            "items": items,
            "metadata": metadata,
        }


    def search_subtitles(self, payload):
        owner_id = require_text(payload.get("owner_id"), "owner_id", max_length=200)
        media_id = require_text(payload.get("media_id"), "media_id", max_length=200)
        try:
            limit = int(payload.get("limit") or 20)
        except (TypeError, ValueError):
            raise ApiError(400, "invalid_limit", "limit must be an integer")
        if limit < 1 or limit > 50:
            raise ApiError(400, "invalid_limit", "limit must be between 1 and 50")
        try:
            result = self.service.subtitle_search_candidates(media_id, limit=limit)
        except (RuntimeError, ValueError) as exc:
            raise ApiError(502, "subtitle_search_failed", str(exc))
        if not isinstance(result, dict) or not isinstance(result.get("candidates"), list):
            raise ApiError(502, "subtitle_search_failed", "pipeline subtitle search returned invalid response")
        candidates = result.get("candidates") or []
        for item in candidates:
            if not isinstance(item, dict) or str(item.get("media_id") or "").strip() != media_id:
                raise ApiError(502, "subtitle_search_failed", "pipeline subtitle search returned an invalid candidate")
        metadata = {
            "media_id": media_id,
            "title": str(result.get("title") or ""),
            "category": str(result.get("category") or ""),
            "query": str(result.get("query") or ""),
            "selected_count": len(candidates),
        }
        session_id, expires_at, stored = self.store.save_search(
            owner_id,
            metadata["query"] or media_id,
            metadata["category"] or "other",
            "subtitles",
            candidates,
            metadata,
        )
        return {
            "session_id": session_id,
            "expires_at": expires_at,
            "media_id": media_id,
            "title": metadata["title"],
            "category": metadata["category"],
            "query": metadata["query"],
            "items": [public_subtitle_candidate(item) for item in stored],
        }

    def search_season_subtitles(self, payload):
        if not isinstance(payload, dict):
            raise ApiError(400, "invalid_request", "request body must be a JSON object")
        owner_id = require_text(payload.get("owner_id"), "owner_id", max_length=200)
        media_id = require_text(payload.get("media_id"), "media_id", max_length=200)
        try:
            season = int(payload.get("season"))
        except (TypeError, ValueError):
            raise ApiError(400, "invalid_season", "season must be an integer")
        if season < 1 or season > 99:
            raise ApiError(400, "invalid_season", "season must be between 1 and 99")
        title = str(payload.get("title") or "").strip()
        if len(title) > 500:
            raise ApiError(400, "invalid_title", "title is too long")
        try:
            limit = int(payload.get("limit") or 20)
        except (TypeError, ValueError):
            raise ApiError(400, "invalid_limit", "limit must be an integer")
        if limit < 1 or limit > 50:
            raise ApiError(400, "invalid_limit", "limit must be between 1 and 50")
        episodes = normalize_season_subtitle_episode_requests(payload.get("episodes"), season)
        try:
            result = self.service.subtitle_search_season_candidates(
                media_id, season, title=title, targets=episodes, limit=limit,
            )
        except (RuntimeError, ValueError) as exc:
            raise ApiError(502, "subtitle_season_search_failed", str(exc))
        if not isinstance(result, dict) or not isinstance(result.get("candidates"), list):
            raise ApiError(502, "subtitle_season_search_failed", "pipeline season subtitle search returned invalid response")
        candidates = []
        for item in result.get("candidates") or []:
            if not isinstance(item, dict) or str(item.get("provider") or "") != "subhd":
                raise ApiError(502, "subtitle_season_search_failed", "pipeline season subtitle search returned an invalid candidate")
            candidates.append({**item, "season": season})
        metadata = {
            "media_id": media_id,
            "season": season,
            "title": title,
            "query": str(result.get("query") or ""),
            "category": str(result.get("category") or ""),
            "detail_url": str(result.get("detail_url") or ""),
            "detail_title": str(result.get("detail_title") or ""),
            "selected_count": len(candidates),
        }
        session_id, expires_at, stored = self.store.save_search(
            owner_id, metadata["query"] or media_id, metadata["category"] or "tv",
            "subtitle_season", candidates, metadata,
        )
        return {
            "session_id": session_id,
            "expires_at": expires_at,
            **metadata,
            "items": [public_subtitle_candidate(item) for item in stored],
        }

    def create_season_subtitle_task(self, payload):
        if self.season_subtitle_manager is None:
            raise ApiError(503, "season_subtitle_unavailable", "season subtitle task manager unavailable")
        if not isinstance(payload, dict):
            raise ApiError(400, "invalid_request", "request body must be a JSON object")
        owner_id = require_text(payload.get("owner_id"), "owner_id", max_length=200)
        media_id = require_text(payload.get("media_id"), "media_id", max_length=200)
        session_id = require_text(payload.get("search_session_id"), "search_session_id", max_length=200)
        try:
            season = int(payload.get("season"))
        except (TypeError, ValueError):
            raise ApiError(400, "invalid_season", "season must be an integer")
        if season < 1 or season > 99:
            raise ApiError(400, "invalid_season", "season must be between 1 and 99")
        requests = normalize_season_subtitle_episode_requests(
            payload.get("episodes"), season, require_candidate=True,
        )
        targets = []
        for request in requests:
            loaded = self.store.load_search_candidate(owner_id, session_id, request["candidate_id"])
            if loaded.get("source") != "subtitle_season":
                raise ApiError(404, "season_subtitle_candidate_not_found", "season subtitle candidate not found")
            candidate = loaded.get("candidate")
            if (
                not isinstance(candidate, dict)
                or str(candidate.get("media_id") or "").strip() != request["media_id"]
                or int(candidate.get("season") or 0) != season
                or str(candidate.get("episode_key") or "").strip().upper() != request["episode_key"]
                or str(candidate.get("provider") or "") != "subhd"
            ):
                raise ApiError(409, "season_subtitle_candidate_mismatch", "season subtitle candidate does not match this episode")
            targets.append({**request, "candidate": candidate})
        try:
            return self.season_subtitle_manager.create_task(
                owner_id, media_id, season, targets,
            )
        except (RuntimeError, ValueError) as exc:
            raise ApiError(400, "season_subtitle_task_invalid", str(exc))

    def get_season_subtitle_task(self, owner_id, task_id):
        if self.season_subtitle_manager is None:
            raise ApiError(503, "season_subtitle_unavailable", "season subtitle task manager unavailable")
        try:
            return self.season_subtitle_manager.get_task(owner_id, task_id)
        except (RuntimeError, ValueError) as exc:
            raise ApiError(404, "season_subtitle_task_not_found", str(exc))

    def retry_season_subtitle_task(self, task_id, payload):
        if self.season_subtitle_manager is None:
            raise ApiError(503, "season_subtitle_unavailable", "season subtitle task manager unavailable")
        if not isinstance(payload, dict):
            raise ApiError(400, "invalid_request", "request body must be a JSON object")
        owner_id = require_text(payload.get("owner_id"), "owner_id", max_length=200)
        try:
            return self.season_subtitle_manager.retry_failed_task(
                owner_id,
                task_id,
                payload.get("media_ids"),
            )
        except (RuntimeError, ValueError) as exc:
            raise ApiError(400, "season_subtitle_retry_invalid", str(exc))

    def preview_subtitle(self, payload):
        _owner_id, media_id, record = self._subtitle_candidate(payload)
        try:
            preview = self.service.preview_subtitle_candidate(record, max_chars=8000)
        except (RuntimeError, ValueError) as exc:
            raise ApiError(502, "subtitle_preview_failed", str(exc))
        if not isinstance(preview, dict):
            raise ApiError(502, "subtitle_preview_failed", "pipeline subtitle preview returned invalid response")
        out = public_subtitle_candidate({**record, **preview})
        out.update(
            {
                "candidate_id": str(payload.get("candidate_id") or ""),
                "media_id": media_id,
                "content_sample": str(preview.get("content_sample") or ""),
                "preview_char_count": int(preview.get("preview_char_count") or 0),
                "preview_line_count": int(preview.get("preview_line_count") or 0),
            }
        )
        return out

    def apply_subtitle(self, payload):
        _owner_id, media_id, record = self._subtitle_candidate(payload)
        try:
            result = self.service.apply_subtitle_candidate(record)
        except (RuntimeError, ValueError) as exc:
            raise ApiError(502, "subtitle_apply_failed", str(exc))
        if not isinstance(result, dict):
            raise ApiError(502, "subtitle_apply_failed", "pipeline subtitle apply returned invalid response")
        return {
            "media_id": media_id,
            "status": str(result.get("subtitle_match_status") or ""),
            "source": str(result.get("subtitle_match_source") or ""),
            "filename": str(result.get("subtitle_match_filename") or ""),
            "count": int(result.get("subtitle_match_count") or 0),
            "reason": str(result.get("subtitle_match_reason") or ""),
        }

    def create_subtitle_asr(self, payload):
        if self.subtitle_asr_manager is None:
            raise ApiError(503, "subtitle_asr_unavailable", "AI subtitle task manager unavailable")
        return self.subtitle_asr_manager.create_task(payload)

    def get_subtitle_asr(self, owner_id, task_id):
        if self.subtitle_asr_manager is None:
            raise ApiError(503, "subtitle_asr_unavailable", "AI subtitle task manager unavailable")
        return self.subtitle_asr_manager.get_task(owner_id, task_id)

    def list_subtitle_asr(self, limit=50):
        if self.subtitle_asr_manager is None:
            raise ApiError(503, "subtitle_asr_unavailable", "AI subtitle task manager unavailable")
        return {"items": self.subtitle_asr_manager.list_tasks(limit)}

    def list_subtitle_asr_models(self):
        if self.subtitle_asr_manager is None:
            raise ApiError(503, "subtitle_asr_unavailable", "AI subtitle task manager unavailable")
        return {"models": self.subtitle_asr_manager.list_models()}

    def list_subtitle_asr_engines(self):
        if self.subtitle_asr_manager is None:
            raise ApiError(503, "subtitle_asr_unavailable", "AI subtitle task manager unavailable")
        return {"models": self.subtitle_asr_manager.list_asr_models()}

    def retry_subtitle_asr(self, owner_id, task_id, payload):
        if self.subtitle_asr_manager is None:
            raise ApiError(503, "subtitle_asr_unavailable", "AI subtitle task manager unavailable")
        return self.subtitle_asr_manager.retry_task(owner_id, task_id, payload)

    def update_subtitle_asr_model(self, owner_id, task_id, payload):
        if self.subtitle_asr_manager is None:
            raise ApiError(503, "subtitle_asr_unavailable", "AI subtitle task manager unavailable")
        return self.subtitle_asr_manager.update_task_model(owner_id, task_id, payload)

    def cancel_subtitle_asr(self, owner_id, task_id):
        if self.subtitle_asr_manager is None:
            raise ApiError(503, "subtitle_asr_unavailable", "AI subtitle task manager unavailable")
        return self.subtitle_asr_manager.cancel_task(owner_id, task_id)

    def retranslate_subtitle_asr(self, owner_id, task_id, payload):
        if self.subtitle_asr_manager is None:
            raise ApiError(503, "subtitle_asr_unavailable", "AI subtitle task manager unavailable")
        return self.subtitle_asr_manager.retranslate_task(owner_id, task_id, payload)

    def delete_subtitle_asr(self, owner_id, task_id):
        if self.subtitle_asr_manager is None:
            raise ApiError(503, "subtitle_asr_unavailable", "AI subtitle task manager unavailable")
        self.subtitle_asr_manager.delete_task(owner_id, task_id)

    def _subtitle_candidate(self, payload):
        owner_id = require_text(payload.get("owner_id"), "owner_id", max_length=200)
        media_id = require_text(payload.get("media_id"), "media_id", max_length=200)
        session_id = require_text(payload.get("search_session_id"), "search_session_id", max_length=200)
        candidate_id = require_text(payload.get("candidate_id"), "candidate_id", max_length=200)
        loaded = self.store.load_search_candidate(owner_id, session_id, candidate_id)
        if loaded.get("source") not in {"subtitles", "subtitle_season"}:
            raise ApiError(404, "subtitle_candidate_not_found", "subtitle candidate not found")
        record = loaded.get("candidate")
        if not isinstance(record, dict) or str(record.get("media_id") or "").strip() != media_id:
            raise ApiError(409, "subtitle_media_mismatch", "subtitle candidate does not belong to this media")
        return owner_id, media_id, record


class InternalApiServer:
    def __init__(
        self,
        service,
        db_path,
        token,
        host=DEFAULT_API_HOST,
        port=8765,
        workers=3,
        owner_workers=2,
        offline_wait_slice_seconds=DEFAULT_OFFLINE_WAIT_SLICE_SECONDS,
        search_ttl_seconds=DEFAULT_SEARCH_TTL_SECONDS,
    ):
        self.token = require_text(token, "INTERNAL_API_TOKEN", max_length=1000)
        self.host = require_text(host, "INTERNAL_API_HOST", max_length=255)
        self.port = int(port)
        if self.port < 1 or self.port > 65535:
            raise ValueError("INTERNAL_API_PORT must be between 1 and 65535")
        self.store = InternalApiStore(db_path, search_ttl_seconds=search_ttl_seconds)
        self.manager = ImportTaskManager(
            service,
            self.store,
            workers=workers,
            owner_workers=owner_workers,
            offline_wait_slice_seconds=offline_wait_slice_seconds,
        )
        self.subtitle_asr_manager = SubtitleAsrTaskManager(
            SubtitleAsrProcessor(getattr(service, "config", None)),
            self.store,
        )
        self.season_subtitle_manager = SeasonSubtitleTaskManager(service, db_path)
        self.application = InternalApiApplication(
            service,
            self.store,
            self.manager,
            subtitle_asr_manager=self.subtitle_asr_manager,
            season_subtitle_manager=self.season_subtitle_manager,
        )
        self._httpd = None
        self._thread = None

    def start(self):
        if self._httpd is not None:
            return
        handler = self._handler_class()
        httpd = ThreadingHTTPServer((self.host, self.port), handler)
        httpd.daemon_threads = True
        self._httpd = httpd
        self.manager.start()
        self.subtitle_asr_manager.start()
        self.season_subtitle_manager.start()
        self._thread = threading.Thread(target=httpd.serve_forever, name="internal-api-http", daemon=True)
        self._thread.start()
        print("internal API listening on http://%s:%d" % (self.host, self.port), flush=True)

    def stop(self):
        httpd = self._httpd
        self._httpd = None
        if httpd is not None:
            httpd.shutdown()
            httpd.server_close()
        if self._thread is not None:
            self._thread.join(timeout=5)
            self._thread = None
        self.season_subtitle_manager.stop()
        self.subtitle_asr_manager.stop()
        self.manager.stop()

    def _handler_class(self):
        api_server = self

        class Handler(BaseHTTPRequestHandler):
            server_version = "MediaPipelineInternalApi/1"

            def do_GET(self):
                api_server._handle(self)

            def do_POST(self):
                api_server._handle(self)

            def do_DELETE(self):
                api_server._handle(self)

            def log_message(self, format_string, *args):
                print("internal API: " + (format_string % args), flush=True)

        return Handler

    def _handle(self, handler):
        try:
            parsed = urllib.parse.urlsplit(handler.path)
            path = parsed.path.rstrip("/") or "/"
            query = urllib.parse.parse_qs(parsed.query)
            if handler.command == "GET" and path == "/health":
                self._send_json(handler, 200, {"status": "ok"})
                return
            self._authenticate(handler)
            if handler.command == "POST" and path == "/v1/search":
                self._send_json(handler, 200, self.application.search(self._read_json(handler)))
                return
            if handler.command == "POST" and path == "/v1/manual-candidates":
                self._send_json(handler, 200, self.application.prepare_manual_candidate(self._read_json(handler)))
                return
            if handler.command == "POST" and path == "/v1/subtitles/search":
                self._send_json(handler, 200, self.application.search_subtitles(self._read_json(handler)))
                return
            if handler.command == "POST" and path == "/v1/subtitles/season/search":
                self._send_json(handler, 200, self.application.search_season_subtitles(self._read_json(handler)))
                return
            if handler.command == "POST" and path == "/v1/subtitles/season/apply":
                task, created = self.application.create_season_subtitle_task(self._read_json(handler))
                self._send_json(handler, 202 if created else 200, task)
                return
            if handler.command == "POST" and path.startswith("/v1/subtitles/season/tasks/") and path.endswith("/retry"):
                task_id = path.split("/")[-2]
                task, created = self.application.retry_season_subtitle_task(task_id, self._read_json(handler))
                self._send_json(handler, 202 if created else 200, task)
                return
            if handler.command == "GET" and path.startswith("/v1/subtitles/season/tasks/"):
                task_id = path.rsplit("/", 1)[-1]
                owner_id = (query.get("owner_id") or [""])[0]
                self._send_json(handler, 200, self.application.get_season_subtitle_task(owner_id, task_id))
                return
            if handler.command == "POST" and path == "/v1/subtitles/preview":
                self._send_json(handler, 200, self.application.preview_subtitle(self._read_json(handler)))
                return
            if handler.command == "POST" and path == "/v1/subtitles/apply":
                self._send_json(handler, 200, self.application.apply_subtitle(self._read_json(handler)))
                return
            if handler.command == "POST" and path == "/v1/subtitles/asr":
                task, created = self.application.create_subtitle_asr(self._read_json(handler))
                self._send_json(handler, 202 if created else 200, task)
                return
            if handler.command == "GET" and path == "/v1/subtitles/asr/models":
                self._send_json(handler, 200, self.application.list_subtitle_asr_models())
                return
            if handler.command == "GET" and path == "/v1/subtitles/asr/asr-models":
                self._send_json(handler, 200, self.application.list_subtitle_asr_engines())
                return
            if handler.command == "GET" and path == "/v1/subtitles/asr":
                raw_limit = (query.get("limit") or ["50"])[0]
                try:
                    limit = int(raw_limit)
                except (TypeError, ValueError):
                    raise ApiError(400, "invalid_limit", "limit must be an integer")
                self._send_json(handler, 200, self.application.list_subtitle_asr(limit))
                return
            subtitle_match = subtitle_asr_task_path_match(path)
            if subtitle_match:
                subtitle_asr_task_id, subtitle_action = subtitle_match
                if handler.command == "GET" and subtitle_action is None:
                    owner_id = owner_from_request(handler, query, body=None)
                    self._send_json(
                        handler,
                        200,
                        self.application.get_subtitle_asr(owner_id, subtitle_asr_task_id),
                    )
                    return
                if handler.command == "POST" and subtitle_action == "retry":
                    payload = self._read_json(handler)
                    owner_id = owner_from_request(handler, query, body=payload)
                    self._send_json(
                        handler,
                        202,
                        self.application.retry_subtitle_asr(owner_id, subtitle_asr_task_id, payload),
                    )
                    return
                if handler.command == "POST" and subtitle_action in {"model", "retranslate"}:
                    payload = self._read_json(handler)
                    owner_id = owner_from_request(handler, query, body=payload)
                    if subtitle_action == "model":
                        task = self.application.update_subtitle_asr_model(
                            owner_id, subtitle_asr_task_id, payload
                        )
                        status = 200
                    else:
                        task = self.application.retranslate_subtitle_asr(
                            owner_id, subtitle_asr_task_id, payload
                        )
                        status = 202
                    self._send_json(handler, status, task)
                    return
                if handler.command == "POST" and subtitle_action == "cancel":
                    payload = self._read_json(handler)
                    owner_id = owner_from_request(handler, query, body=payload)
                    self._send_json(
                        handler,
                        200,
                        self.application.cancel_subtitle_asr(owner_id, subtitle_asr_task_id),
                    )
                    return
                if handler.command == "DELETE" and subtitle_action is None:
                    owner_id = owner_from_request(handler, query, body=None)
                    self.application.delete_subtitle_asr(owner_id, subtitle_asr_task_id)
                    self._send_json(handler, 200, {"deleted": True})
                    return
            if handler.command == "POST" and path == "/v1/imports":
                payload = self._read_json(handler)
                owner_id = require_text(payload.get("owner_id"), "owner_id", max_length=200)
                idempotency_key = require_text(handler.headers.get("Idempotency-Key"), "Idempotency-Key", max_length=200)
                task, created = self.manager.create_import(owner_id, idempotency_key, payload)
                self._send_json(handler, 202 if created else 200, task)
                return
            match = import_path_match(path)
            if match:
                import_id, action = match
                if handler.command == "GET" and action is None:
                    owner_id = owner_from_request(handler, query, body=None)
                    self._send_json(handler, 200, self.manager.get_import(owner_id, import_id))
                    return
                if handler.command == "POST" and action in {"cancel", "retry"}:
                    payload = self._read_json(handler)
                    owner_id = owner_from_request(handler, query, body=payload)
                    if action == "cancel":
                        self._send_json(handler, 200, self.manager.cancel_import(owner_id, import_id))
                    else:
                        self._send_json(handler, 202, self.manager.retry_import(owner_id, import_id))
                    return
            raise ApiError(404, "not_found", "endpoint not found")
        except ApiError as exc:
            error = {"code": exc.code, "message": exc.message}
            error.update(exc.details)
            self._send_json(handler, exc.status, {"error": error})
        except Exception:
            traceback.print_exc()
            self._send_json(handler, 500, {"error": {"code": "internal_error", "message": "internal server error"}})

    def _authenticate(self, handler):
        value = str(handler.headers.get("Authorization") or "")
        scheme, separator, token = value.partition(" ")
        if separator != " " or scheme.lower() != "bearer" or not hmac.compare_digest(token, self.token):
            raise ApiError(401, "unauthorized", "valid Bearer token required")

    def _read_json(self, handler):
        content_type = str(handler.headers.get("Content-Type") or "").split(";", 1)[0].strip().lower()
        if content_type != "application/json":
            raise ApiError(415, "unsupported_media_type", "Content-Type must be application/json")
        raw_length = handler.headers.get("Content-Length")
        if raw_length is None:
            raise ApiError(411, "length_required", "Content-Length required")
        try:
            length = int(raw_length)
        except ValueError:
            raise ApiError(400, "invalid_content_length", "invalid Content-Length")
        if length < 0 or length > MAX_JSON_BODY_BYTES:
            raise ApiError(413, "request_too_large", "JSON request body is too large")
        try:
            payload = json.loads(handler.rfile.read(length).decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ApiError(400, "invalid_json", "invalid JSON: %s" % exc)
        if not isinstance(payload, dict):
            raise ApiError(400, "invalid_request", "request body must be a JSON object")
        return payload

    def _send_json(self, handler, status, payload):
        body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        handler.send_response(int(status))
        handler.send_header("Content-Type", "application/json; charset=utf-8")
        handler.send_header("Content-Length", str(len(body)))
        handler.end_headers()
        handler.wfile.write(body)


def import_path_match(path):
    parts = [part for part in path.split("/") if part]
    if len(parts) == 3 and parts[:2] == ["v1", "imports"]:
        return parts[2], None
    if len(parts) == 4 and parts[:2] == ["v1", "imports"] and parts[3] in {"cancel", "retry"}:
        return parts[2], parts[3]
    return None


def subtitle_asr_task_path_match(path):
    parts = [part for part in path.split("/") if part]
    if len(parts) == 4 and parts[:3] == ["v1", "subtitles", "asr"]:
        return parts[3], None
    if (
        len(parts) == 5
        and parts[:3] == ["v1", "subtitles", "asr"]
        and parts[4] in {"retry", "model", "cancel", "retranslate"}
    ):
        return parts[3], parts[4]
    return None


def public_subtitle_candidate(item):
    item = item or {}
    can_apply, unavailable_reason = subtitle_candidate_application_status(item)
    can_preview, preview_unavailable_reason = subtitle_candidate_preview_status(item)
    return {
        "candidate_id": str(item.get("candidate_id") or ""),
        "provider": str(item.get("provider") or ""),
        "application_key": subtitle_application_key(item.get("provider"), item.get("provider_id")),
        "can_apply": can_apply,
        "unavailable_reason": unavailable_reason,
        "can_preview": can_preview,
        "preview_unavailable_reason": preview_unavailable_reason,
        "title": str(item.get("subtitle_title") or item.get("filename") or ""),
        "filename": str(item.get("filename") or ""),
        "language": str(item.get("language") or ""),
        "source_score": int(item.get("source_score") or 0),
        "rank": int(item.get("rank") or 0),
        "media_id": str(item.get("media_id") or ""),
        "episode_key": str(item.get("episode_key") or ""),
        "url": str(item.get("url") or ""),
        "subtitle_group": str(item.get("subtitle_group") or ""),
        "source_type": str(item.get("source_type") or ""),
        "language_tags": public_text_list(item.get("language_tags")),
        "formats": public_text_list(item.get("formats")),
        "like_count": int(item.get("like_count") or 0),
        "download_count": int(item.get("download_count") or 0),
        "uploader": str(item.get("uploader") or ""),
        "uploaded_at": str(item.get("uploaded_at") or ""),
        "uploaded_date": str(item.get("uploaded_date") or ""),
    }


def public_text_list(value):
    if not isinstance(value, (list, tuple)):
        return []
    return [str(item).strip() for item in value if str(item or "").strip()]


def normalize_season_subtitle_episode_requests(items, season, require_candidate=False):
    if not isinstance(items, list) or not items:
        raise ApiError(400, "season_subtitle_episodes_required", "season subtitle episodes are required")
    if len(items) > 500:
        raise ApiError(400, "season_subtitle_episode_limit", "season subtitle episode count exceeds 500")
    expected_prefix = "S%02dE" % int(season)
    seen_media_ids = set()
    normalized = []
    for item in items:
        if not isinstance(item, dict):
            raise ApiError(400, "season_subtitle_episode_invalid", "season subtitle episode is invalid")
        media_id = require_text(item.get("media_id"), "episode media_id", max_length=200)
        episode_key = require_text(item.get("episode_key"), "episode_key", max_length=20).upper()
        if not re.fullmatch(r"S\d{2}E\d{2,3}", episode_key) or not episode_key.startswith(expected_prefix):
            raise ApiError(400, "season_subtitle_episode_invalid", "season subtitle episode key is invalid")
        if media_id in seen_media_ids:
            raise ApiError(400, "season_subtitle_episode_duplicate", "season subtitle episode media_id is duplicated")
        seen_media_ids.add(media_id)
        normalized_item = {"media_id": media_id, "episode_key": episode_key}
        if require_candidate:
            normalized_item["candidate_id"] = require_text(
                item.get("candidate_id"), "candidate_id", max_length=200,
            )
        normalized.append(normalized_item)
    return normalized


def owner_from_request(handler, query, body=None):
    values = query.get("owner_id") or []
    query_owner = values[0] if values else None
    header_owner = handler.headers.get("X-Owner-ID")
    body_owner = (body or {}).get("owner_id") if isinstance(body, dict) else None
    supplied = [str(value).strip() for value in (query_owner, header_owner, body_owner) if str(value or "").strip()]
    if not supplied:
        raise ApiError(400, "missing_owner_id", "owner_id is required")
    if any(value != supplied[0] for value in supplied[1:]):
        raise ApiError(400, "owner_id_conflict", "owner_id values do not match")
    return require_text(supplied[0], "owner_id", max_length=200)


def normalize_target(payload):
    target = {}
    for key in ("library_id", "root_id", "root_openlist_path", "provider", "media_type"):
        target[key] = require_text(payload.get(key), key, max_length=1000)
    if not target["root_openlist_path"].startswith("/"):
        raise ApiError(400, "invalid_root_openlist_path", "root_openlist_path must be absolute")
    return target


def subscription_source_block_key(download_uri):
    uri = str(download_uri or "").strip()
    if not uri:
        raise ValueError("subscription source uri missing")
    parsed = parse_115_share_url(uri)
    if parsed is not None:
        return "115-share:%s:%s" % (parsed.share_code.casefold(), str(parsed.pdir_fid or "0").strip() or "0")
    if urllib.parse.urlsplit(uri).scheme.casefold() == "prowlarr-download":
        query = urllib.parse.parse_qs(urllib.parse.urlsplit(uri).query)
        file_name = str((query.get("file") or [""])[0]).strip()
        if file_name:
            return "prowlarr-file-sha256:" + hashlib.sha256(file_name.casefold().encode("utf-8")).hexdigest()
    info_hash = str(candidate_info_hash({"download_uri": uri}) or "").strip().casefold()
    if info_hash:
        return "info-hash:" + info_hash
    return "uri-sha256:" + hashlib.sha256(uri.encode("utf-8")).hexdigest()


def migrate_subscription_source_block_aliases(conn):
    """Add stable Prowlarr filename aliases for legacy URI-hash blocks."""
    rows = conn.execute(
        """
        select b.source_key, b.reason, b.origin_import_id, b.created_at, i.request_json
        from internal_api_subscription_source_blocks b
        left join internal_api_imports i on i.id = b.origin_import_id
        where b.source_key like 'uri-sha256:%'
        """
    ).fetchall()
    for row in rows:
        try:
            request = json.loads(row["request_json"] or "{}")
            uri = (request.get("candidate") or {}).get("download_uri")
            alias = subscription_source_block_key(uri)
        except (TypeError, ValueError, json.JSONDecodeError):
            continue
        if not alias or alias == row["source_key"]:
            continue
        conn.execute(
            """
            insert or ignore into internal_api_subscription_source_blocks
                (source_key, reason, origin_import_id, created_at)
            values (?, ?, ?, ?)
            """,
            (alias, row["reason"], row["origin_import_id"], row["created_at"]),
        )


def subscription_receives_directly_to_staging(staging):
    return str((staging or {}).get("receive_mode") or "").strip().lower() == "direct_task_directory"


def subscription_receive_target_folder_id(staging):
    staging = staging or {}
    key = "receive_folder_id" if subscription_receives_directly_to_staging(staging) else "receive_root_folder_id"
    return str(staging.get(key) or "").strip()


def subscription_source_failure_block_reason(error):
    message = str(error or "").casefold()
    if any(marker in message for marker in ("链接已过期", "链接过期", "share expired", "link expired")):
        return "share_expired"
    if "115 offline task exceeded" in message:
        return "offline_failed"
    if "115 offline task ended with status: failed" in message and any(
        marker in message
        for marker in (
            "资源失效",
            "资源不存在",
            "种子不存在",
            "下载链接无效",
            "invalid torrent",
            "invalid magnet",
            "torrent not found",
            "magnet not found",
            "resource unavailable",
            "resource not found",
            "source unavailable",
        )
    ):
        return "offline_failed"
    return None


def format_offline_task_failure(task):
    task = task or {}
    details = []
    for key in ("message", "msg", "error", "code", "state", "errmsg", "errcode"):
        value = str(task.get(key) or "").strip()
        if value and value not in details:
            details.append(value)
    suffix = ": " + ", ".join(details) if details else ""
    return "115 offline task ended with status: %s%s" % (task.get("status_name") or "failed", suffix)


def subscription_source_block_is_effective(block):
    if not isinstance(block, dict):
        return False
    return str(block.get("reason") or "").strip() in {"share_expired", "offline_failed"}


def subscription_offline_wait_timed_out(audit, now=None):
    started_at = int((audit or {}).get("offline_started_at") or 0)
    if started_at <= 0:
        return False
    now = int(time.time() if now is None else now)
    return now - started_at >= ACTIVE_115_TIMEOUT_SECONDS


def normalize_subscription_follow(payload, category, target, force_duplicate, upgrade_media_id):
    enabled = optional_bool(payload.get("subscription_follow"), "subscription_follow", default=False)
    manual_replenish = optional_bool(payload.get("manual_replenish"), "manual_replenish", default=False)
    if not enabled:
        if manual_replenish:
            raise ApiError(400, "invalid_manual_replenish", "manual replenish requires subscription follow validation")
        return None
    if category not in {"tv", "anime"}:
        raise ApiError(400, "invalid_subscription_follow", "subscription follow only supports TV or anime")
    if force_duplicate:
        raise ApiError(400, "invalid_subscription_follow", "subscription follow cannot force duplicate imports")
    if upgrade_media_id:
        raise ApiError(400, "invalid_subscription_follow", "subscription follow cannot be an upgrade import")
    subscription_id = "" if manual_replenish else require_text(
        payload.get("subscription_id"), "subscription_id", max_length=100
    )
    work_key = require_text(payload.get("work_key"), "work_key", max_length=300)
    season = require_positive_int(payload.get("season"), "season", max_value=99)
    existing = normalize_episode_numbers(payload.get("existing_episodes"), "existing_episodes")
    reserved = normalize_episode_numbers(payload.get("reserved_episodes"), "reserved_episodes")
    expected = normalize_episode_numbers(payload.get("expected_episodes"), "expected_episodes")
    if manual_replenish and expected:
        raise ApiError(400, "invalid_expected_episodes", "manual replenish maps episodes from staged filenames")
    target_path = normalize_openlist_path(
        require_text(payload.get("target_openlist_path"), "target_openlist_path", max_length=2000)
    )
    root_path = normalize_openlist_path(target.get("root_openlist_path"))
    if not target_path or not root_path or not target_path.startswith(root_path.rstrip("/") + "/"):
        raise ApiError(
            409,
            "invalid_subscription_target",
            "subscription target must be an existing directory below the configured library root",
        )
    title_class = str(payload.get("title_class") or "unknown").strip().lower()
    if title_class not in {"single", "range", "cumulative_pack", "season_pack", "unknown"}:
        raise ApiError(400, "invalid_title_class", "invalid subscription candidate title class")
    return {
        "subscription_id": subscription_id,
        "manual_replenish": manual_replenish,
        "work_key": work_key,
        "season": season,
        "existing_episodes": existing,
        "reserved_episodes": reserved,
        "expected_episodes": expected,
        "target_openlist_path": target_path,
        "title_class": title_class,
    }


def normalize_episode_numbers(value, field):
    if value is None:
        return []
    if not isinstance(value, list):
        raise ApiError(400, "invalid_request", "%s must be an array" % field)
    out = []
    seen = set()
    for item in value:
        episode = require_positive_int(item, field, max_value=9999)
        if episode not in seen:
            seen.add(episode)
            out.append(episode)
    return sorted(out)


def require_category(value):
    category = str(value or "").strip().lower()
    if category not in VALID_CATEGORIES:
        raise ApiError(400, "invalid_category", "category must be one of: movie, tv, anime, adult, other")
    return category


def require_text(value, label, max_length):
    text = str(value or "").strip()
    if not text:
        code = "missing_%s" % str(label).lower().replace("-", "_").replace(" ", "_")
        raise ApiError(400, code, "%s is required" % label)
    if len(text) > int(max_length):
        raise ApiError(400, "invalid_%s" % label.lower().replace("-", "_"), "%s is too long" % label)
    return text


def require_positive_int(value, label, max_value=None):
    if isinstance(value, bool):
        raise ApiError(400, "invalid_request", "%s must be a positive integer" % label)
    try:
        number = int(value)
    except (TypeError, ValueError):
        raise ApiError(400, "invalid_request", "%s must be a positive integer" % label)
    if number <= 0 or (max_value is not None and number > int(max_value)):
        raise ApiError(400, "invalid_request", "%s must be a positive integer" % label)
    return number


def optional_text(value, label, max_length):
    if value is None or str(value).strip() == "":
        return ""
    return require_text(value, label, max_length)


def optional_bool(value, label, default=False):
    if value is None:
        return bool(default)
    if not isinstance(value, bool):
        raise ApiError(400, "invalid_%s" % label, "%s must be a boolean" % label)
    return value


def normalize_upgrade_scope(category, upgrade_media_id, value):
    requested = optional_text(value, "upgrade_scope", max_length=20).lower()
    if not upgrade_media_id:
        if requested:
            raise ApiError(400, "invalid_upgrade_scope", "upgrade_scope requires upgrade_media_id")
        return ""
    if category in ("tv", "anime"):
        if requested not in ("", "work"):
            raise ApiError(400, "invalid_upgrade_scope", "剧集只支持整剧升级")
        return "work"
    if requested not in ("", "media"):
        raise ApiError(400, "invalid_upgrade_scope", "当前媒体类型只支持单作品升级")
    return "media"


def validate_import_target(target):
    target = target or {}
    if not str(target.get("folder_id") or "").strip():
        raise RuntimeError("入库目标目录缺少115文件夹 ID")
    path = normalize_openlist_path(target.get("openlist_path"))
    if not path:
        raise RuntimeError("入库目标目录缺少 OpenList 路径")
    return path


def apply_import_target(offline_task, target):
    if not isinstance(offline_task, dict) or not target:
        return offline_task
    path = validate_import_target(target)
    offline_task["import_target_openlist_path"] = path
    offline_task["import_target_original_openlist_path"] = path
    offline_task["import_target_purpose"] = str(target.get("purpose") or "import").strip().lower()
    return offline_task


def upgrade_target_scrape_queries(media, target):
    media = dict(media or {})
    target = dict(target or {})
    provider = str(target.get("provider") or "").strip().lower()
    queries = []
    if provider == "tmdb":
        tmdb_id = media.get("tmdb_id") or media.get("tm_db_id")
        try:
            tmdb_id = int(tmdb_id or 0)
        except (TypeError, ValueError):
            tmdb_id = 0
        if tmdb_id > 0:
            queries.append("[tmdbid-%d]" % tmdb_id)
    for key in ("display_title", "title", "original_name"):
        value = str(media.get(key) or "").strip()
        if value:
            queries.append(value)
    return unique_text_values(queries)


def unique_text_values(values):
    out = []
    seen = set()
    for value in values or []:
        text = str(value or "").strip()
        key = text.lower()
        if text and key not in seen:
            seen.add(key)
            out.append(text)
    return out


def upgrade_new_source_paths(task):
    task = dict(task or {})
    if task.get("msg_target_scan_status") != "success":
        return []
    path = normalize_openlist_path(task.get("msg_target_scan_path"))
    return [path] if path else []


def duplicate_summary(duplicate):
    level = str(duplicate.get("level") or "")
    return {
        "level": level,
        "reason": str(duplicate.get("reason") or ""),
        "source": str(duplicate.get("source") or "MediaStationGo"),
        "title": str(duplicate.get("title") or ""),
        "media_id": str(duplicate.get("media_id") or "") or None,
        "can_force": bool(duplicate.get("can_force")) and level != "strong",
    }


def first_submit_info_hash(result):
    if not isinstance(result, dict):
        raise RuntimeError("pipeline submit returned invalid response")
    task_status = result.get("task_status") or {}
    value = str(task_status.get("info_hash") or "").strip()
    if value:
        return value
    for task in result.get("tasks") or []:
        value = str((task or {}).get("info_hash") or "").strip()
        if value:
            return value
    return ""


def result_task_is_offline_success(result):
    return isinstance(result, dict) and (result.get("task") or {}).get("status_name") == OFFLINE_SUCCESS_STATUS


def result_task_is_offline_active(result):
    return (
        isinstance(result, dict)
        and (result.get("task") or {}).get("status_name") in OFFLINE_ACTIVE_STATUSES
    )


def subscription_direct_receive_scan_ready(result):
    """Only inspect direct receive staging after 115 reports offline success."""
    return bool(isinstance(result, dict) and result.get("submit") and result_task_is_offline_success(result))


def subscription_retry_resubmit(result, request, msg_media_id=None):
    if not isinstance(result, dict) or not isinstance(request, dict):
        return False
    if not request.get("subscription_follow") or not result_task_is_offline_success(result):
        return False
    audit = result.get("subscription_follow") or {}
    if audit.get("staging_cleaned_at"):
        promotion = audit.get("promotion") or {}
        if audit.get("moved_episodes") or promotion.get("moved_episodes"):
            return False
        return True
    # A subscription follow may already have a MediaStationGo media id from a
    # failed verification pass.  That association must not keep a retry on the
    # old 115 task after its staging directory has been cleaned; the cleanup
    # marker above is authoritative for resubmission.
    if msg_media_id:
        return False
    staging = audit.get("staging") or {}
    return bool(
        str(staging.get("receive_mode") or "").strip().lower() == "direct_task_directory"
        and not staging.get("claimed_at")
    )


def reset_subscription_sync_for_retry(task):
    out = dict(task or {})
    for key in (
        "msg_scan_status",
        "msg_ingest_status",
        "msg_ingest_job_id",
        "msg_ingest_idempotency_key",
        "msg_ingest_error",
        "msg_target_scan_status",
        "msg_target_scan_error",
        "msg_error",
        "msg_synced_at",
    ):
        out.pop(key, None)
    out["msg_sync_status"] = "running"
    return out


def record_subscription_staging_cleanup(audit, cleanup, reason):
    cleaned_at = int(time.time())
    audit["staging_cleanup"] = {
        **dict(cleanup or {}),
        "status": "cleaned",
        "reason": str(reason or "cleanup").strip() or "cleanup",
        "cleaned_at": cleaned_at,
    }
    audit["staging_cleaned_at"] = cleaned_at
    return audit["staging_cleanup"]


def offline_task_poll_attempt(task):
    try:
        return max(0, int((task or {}).get("pipeline_poll_attempt") or 0))
    except (TypeError, ValueError):
        return 0


def mark_offline_task_polled(task, previous_attempt=0):
    task["pipeline_poll_attempt"] = max(0, int(previous_attempt or 0)) + 1
    task["pipeline_last_polled_at"] = int(time.time())
    return task


def offline_task_poll_delay(task, initial_seconds, max_seconds=60.0):
    initial = max(0.01, float(initial_seconds or 0.01))
    attempt = offline_task_poll_attempt(task)
    multiplier = 2 ** min(max(0, attempt - 1), 8)
    return min(max(initial, float(max_seconds)), initial * multiplier)


def retry_resume_stage(result):
    if not isinstance(result, dict):
        return ""
    stage = str(result.get("retry_from_stage") or "").strip()
    return stage if stage in RETRY_RESUME_STAGES else ""


def warning_retry_stage(result, request=None):
    if not isinstance(result, dict):
        return ""
    stage = str(result.get("warning_stage") or "").strip()
    if stage in RETRY_RESUME_STAGES:
        return stage
    task = result.get("task") or {}
    if isinstance(task, dict):
        for status_key, retry_stage in RETRY_STAGE_BY_FAILED_TASK_STATUS:
            if task.get(status_key) == "failed":
                return retry_stage
    request = request or {}
    if (
        isinstance(request, dict)
        and str(request.get("upgrade_media_id") or "").strip()
        and not bool(request.get("keep_old_version", True))
    ):
        return "removing_old_version"
    return ""


def sync_stage(task):
    task = task or {}
    if task.get("msg_scan_status") == "running" or task.get("msg_ingest_status") == "running":
        return "scanning"
    if task.get("msg_scrape_status") == "running":
        return "scraping"
    if task.get("subtitle_match_status") == "running":
        return "subtitles"
    return "syncing"


def sync_warnings(task):
    task = task or {}
    warnings = []
    if task.get("msg_sync_status") != "success":
        warnings.append("MSG sync status: %s" % (task.get("msg_sync_status") or "missing"))
    if task.get("msg_scrape_status") not in {"success", "skipped"} and not post_enhancement_needs_run(task):
        warnings.append("MSG scrape status: %s" % (task.get("msg_scrape_status") or "missing"))
    for key, value in sorted(task.items()):
        if key.endswith("_status") and value == "failed":
            label = key[: -len("_status")]
            error = task.get(label + "_error")
            warning = "%s failed" % label
            if error:
                warning += ": %s" % error
            if warning not in warnings:
                warnings.append(warning)
    if task.get("msg_error"):
        warning = "MSG error: %s" % task["msg_error"]
        if warning not in warnings:
            warnings.append(warning)
    return warnings


POST_ENHANCEMENT_DEFERRED_STAGES = (
    ("msg_scrape_status", "msg_scrape_reason"),
    ("msg_extra_cleanup_status", "msg_extra_cleanup_reason"),
    ("msg_visibility_repair_status", "msg_visibility_repair_reason"),
    ("subtitle_match_status", "subtitle_match_reason"),
)


def post_enhancement_has_deferred_stage(task):
    task = task or {}
    return any(task.get(reason_key) == "post_ingest_deferred" for _status_key, reason_key in POST_ENHANCEMENT_DEFERRED_STAGES)


def post_enhancement_needs_run(task):
    task = task or {}
    status = str(task.get("post_enhancement_status") or "").strip().lower()
    return status in {"pending", "running"} or (status == "success" and post_enhancement_has_deferred_stage(task))


def restore_deferred_post_enhancement_stages(task):
    task = task or {}
    for status_key, reason_key in POST_ENHANCEMENT_DEFERRED_STAGES:
        if task.get(reason_key) == "post_ingest_deferred":
            task[status_key] = "deferred"
            task.pop(reason_key, None)
    return task


def import_row(row):
    status = row["status"]
    if status not in VALID_IMPORT_STATUSES:
        raise RuntimeError("invalid persisted import status: %s" % status)
    result = json.loads(row["result_json"]) if row["result_json"] else None
    task = (result or {}).get("task") or {}
    return {
        "id": row["id"],
        "owner_id": row["owner_id"],
        "idempotency_key": row["idempotency_key"],
        "status": status,
        "stage": row["stage"],
        "message": import_status_message(status, row["stage"], row["error"]),
        "request": json.loads(row["request_json"]),
        "result": result,
        "error": row["error"],
        "info_hash": row["info_hash"],
        "msg_media_id": row["msg_media_id"],
        "msg_media_title": task.get("msg_media_title"),
        "cancel_requested": bool(row["cancel_requested"]),
        "attempt_count": int(row["attempt_count"] or 0),
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
        "started_at": row["started_at"],
        "completed_at": row["completed_at"],
    }


def subtitle_asr_task_row(row):
    status = row["status"]
    if status not in VALID_SUBTITLE_ASR_STATUSES:
        raise RuntimeError("invalid persisted subtitle ASR status: %s" % status)
    return {
        "id": row["id"],
        "owner_id": row["owner_id"],
        "media_id": row["media_id"],
        "source_language": row["source_language"],
        "asr_model": row["asr_model"] or DEFAULT_ASR_MODEL,
        "translation_provider": row["translation_provider"] or "local",
        "translation_model": row["translation_model"] or "",
        "status": status,
        "stage": row["stage"],
        "progress_current": int(row["progress_current"] or 0),
        "progress_total": int(row["progress_total"] or 0),
        "result": json.loads(row["result_json"]) if row["result_json"] else None,
        "error": row["error"],
        "attempt_count": int(row["attempt_count"] or 0),
        "cached_audio": bool(row["cached_audio"]),
        "cached_transcript": bool(row["cached_transcript"]),
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
        "started_at": row["started_at"],
        "completed_at": row["completed_at"],
    }


def import_status_message(status, stage, error):
    if status == "completed":
        return "导入完成"
    if status == "completed_with_warning":
        return "已入库，但后续处理存在告警"
    if status == "failed":
        return "导入失败：%s" % error if error else "导入失败"
    if status == "canceled":
        return "导入已取消"
    return {
        "queued": "等待执行",
        "starting": "开始执行",
        "preparing_target": "正在准备入库目录",
        "submitting": "正在提交 115 任务",
        "preparing_upgrade": "正在准备整剧升级目录",
        "submitted": "115 任务已提交",
        "waiting_download": "等待 115 下载完成",
        "syncing": "正在同步 MediaStationGo",
        "scanning": "MediaStationGo 正在扫描入库",
        "scraping": "MediaStationGo 正在刮削",
        "subtitles": "正在处理字幕",
    }.get(stage, "任务执行中")


def json_dumps(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def nonempty_or_none(value):
    value = str(value or "").strip()
    return value or None


def ensure_sqlite_column(conn, table, column, definition):
    columns = {row[1] for row in conn.execute("pragma table_info(%s)" % table)}
    if column not in columns:
        conn.execute("alter table %s add column %s %s" % (table, column, definition))
