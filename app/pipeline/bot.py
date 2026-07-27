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
import uuid
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError
from contextlib import nullcontext
from dataclasses import dataclass
from pathlib import Path

from pipeline.client115 import (
    Client115,
    P115QRCodeLoginClient,
    Share115Client,
    is_115_share_url,
    mask_p115_cookie,
    p115_cookie_is_valid,
    parse_115_share_url,
    qrcode_status_label,
    share_receive_task_id,
)
from pipeline.config import category_to_folder_id, category_to_msg_library_root, category_to_openlist_path, msg_library_roots
from pipeline.dedupe import (
    candidate_dedupe_identities,
    candidate_info_hash,
    dedupe_entry_from_row,
    dedupe_source_label,
    dedupe_title_identity,
    duplicate_from_dedupe_entry,
    duplicate_from_task,
    find_index_duplicate,
    find_local_duplicate,
    first_adult_code,
    normalize_dedupe_entry,
    normalize_dedupe_identity_value,
    openlist_dedupe_entries,
    openlist_descendant_names,
    openlist_work_items,
    task_duplicate_codes,
    unique_dedupe_entries,
    unique_dedupe_identities,
)
from pipeline.llm import (
    DEFAULT_LLM_BASE_URL,
    DEFAULT_LLM_MODEL,
    DEFAULT_LLM_SEARCH_RERANK_LIMIT,
    DEFAULT_LLM_TIMEOUT_SECONDS,
    SearchRerankClient,
)
from pipeline.external_subtitles import (
    DEFAULT_SUBTITLE_CACHE_DIR,
    DEFAULT_SUBTITLE_DOWNLOAD_MAX_BYTES,
    DEFAULT_SUBTITLE_PROVIDERS,
    DEFAULT_SUBTITLE_SEARCH_TIMEOUT_SECONDS,
    SOURCE_DECLARED_CHINESE_SUBTITLE_REASON,
    adult_source_declares_chinese_subtitles,
    build_subtitle_matcher_from_config,
)
from pipeline.openlist_utils import (
    is_openlist_video_file,
    normalize_openlist_path,
    normalize_openlist_text,
    openlist_item_is_dir,
    openlist_item_name,
    openlist_item_path,
    openlist_item_size,
    replace_openlist_path_prefix,
)
from pipeline.migration import (
    build_migration_target,
    format_migration_confirm_message,
    format_migration_result_message,
    format_migration_running_message,
    format_migration_search_message,
    format_migration_target_choice_message,
    format_size,
    migration_confirm_reply_markup,
    migration_search_reply_markup,
    migration_source_kind_label,
    migration_target_choice_reply_markup,
)
from pipeline.telegram_ui import (
    append_task_lines,
    callback_task_reply_markup,
    dedupe_refresh_confirm_reply_markup,
    download_uri_label,
    duplicate_can_force,
    duplicate_identity_label,
    duplicate_level_label,
    duplicate_reason_label,
    duplicate_reply_markup,
    format_auto_cancel_result_message,
    format_cancel_result_message,
    format_dedupe_refresh_message,
    format_duplicate_message,
    format_library_choice_message,
    format_percent,
    format_search_page_message,
    format_submit_message,
    format_task_list_message,
    format_task_status_message,
    home_back_reply_markup,
    home_reply_markup,
    library_choice_reply_markup,
    msg_match_mode_label,
    msg_sync_status_label,
    normalize_page,
    search_page_count,
    search_page_jump_buttons,
    search_page_reply_markup,
    submit_reply_markup,
    submit_callback_text,
    task_diagnostic_stage_values,
    task_from_submit_result,
    task_can_retry_msg_sync,
    task_is_final,
    task_list_priority,
    task_list_reply_markup,
    task_page_count,
    task_reply_markup,
)
from pipeline.mediastation import (
    DEFAULT_MSG_BASE_URL,
    MediaStationApiError,
    MediaStationClient,
    extract_codes,
    extract_media_id,
    extract_media_items,
    find_matching_media,
    media_belongs_to_library,
    media_haystack,
)
from pipeline.offline_tasks import cancel_task_if_active, find_task_by_info_hash, find_tasks_by_info_hashes
from pipeline.openlist import DEFAULT_OPENLIST_URL, OpenListClient, OpenListPasswordTokenProvider, OpenListTokenProvider
from pipeline.openlist_tokens import OpenListTokenStore
from pipeline.prowlarr import (
    DEFAULT_PROWLARR_CONFIG,
    DEFAULT_PROWLARR_URL,
    ProwlarrClient,
    ProwlarrConfig,
    ProwlarrSearchCache,
    is_prowlarr_download_uri,
)
from pipeline.pansou import DEFAULT_PANSOU_CLOUD_TYPES, DEFAULT_PANSOU_TIMEOUT_SECONDS, DEFAULT_PANSOU_URL, PanSouClient
from pipeline.resource_selector import ResourceSelector
from pipeline.search_stats import (
    SearchResultList,
    SearchStats,
    attach_search_metadata,
    exception_search_metadata,
    search_result_metadata,
)
from pipeline.search import (
    DEFAULT_ANIME_INDEXER_SEARCH_LIMIT,
    DEFAULT_OPTIONAL_INDEXER_TIMEOUT_SECONDS,
    DEFAULT_PRIMARY_INDEXER_TIMEOUT_SECONDS,
    DEFAULT_PROWLARR_EARLY_RETURN_AFTER_SECONDS,
    DEFAULT_PROWLARR_EARLY_RETURN_MIN_RESULTS,
    DEFAULT_PROWLARR_EARLY_RETURN_REQUIRED_PRIORITY,
    DEFAULT_PROWLARR_MAX_WORKERS,
    DEFAULT_PROWLARR_SEARCH_TIMEOUT_SECONDS,
    DEFAULT_REQUIRED_INDEXER_SEARCH_LIMIT,
    DEFAULT_UPSTREAM_SEARCH_LIMIT,
    SEARCH_PROFILE_ADULT,
    SEARCH_PROFILE_ANIME,
    SEARCH_PROFILE_CATEGORIES,
    SEARCH_PROFILE_GENERAL,
    SEARCH_PROFILE_TAG_LABELS,
    indexer_priority_map,
    indexer_enabled,
    is_strong_adult_code_query,
    magnet_candidate_from_text,
    parse_csv_ints,
    parse_csv_strings,
    safe_prowlarr_tags,
    search_anime_indexer_results,
    search_profile_categories_from_env,
    search_profile_for_query,
    search_profile_indexer_results,
    search_profile_max_workers_from_env,
    search_profile_tag_labels_from_env,
    search_profile_timeout_seconds_from_env,
    search_profile_upstream_limits_from_env,
    search_profile_value,
    search_primary_indexer_results,
    search_sukebei_indexer_results,
    should_search_anime,
    should_search_sukebei,
)
from pipeline.task_state import (
    STATUS_CANCELLED,
    STATUS_FAILED,
    STATUS_RUNNING,
    TASK_STATE,
)
from pipeline.version import format_version_info


DEFAULT_STATE_DB = "/bot-data/state.db"
DEFAULT_OPENLIST_DB = "/openlist-data/data.db"
DEFAULT_SEARCH_LIMIT = 100
SEARCH_PAGE_SIZE = 5
DEFAULT_TASK_LIST_LIMIT = 10
DEFAULT_TASK_LIST_PAGE_SIZE = 5
DEFAULT_TASK_LIST_FETCH_LIMIT = 100
DEFAULT_OPENLIST_PRE_SCAN_CLEAN_MAX_BYTES = 20 * 1024 * 1024
ADULT_EXTRA_VIDEO_HIDE_MIN_BYTES = 200 * 1024 * 1024
P115_COOKIE_SETTING_KEY = "p115_cookie"
DEFAULT_SUBTITLE_BACKFILL_LIMIT = 20
DEFAULT_SUBTITLE_REMATCH_LIMIT = 10
DEFAULT_SUBTITLE_FIND_LIMIT = 8
SUBTITLE_FIND_SCAN_PAGE_LIMIT = 5
DEFAULT_SUBTITLE_LLM_PREVIEW_LIMIT = 5
DEFAULT_SUBTITLE_LLM_PREVIEW_CHARS = 2000
MAX_SUBTITLE_BACKFILL_LIMIT = 50
SUBTITLE_BACKFILL_SKIP_STATUSES = {"failed", "not_found"}
SUBTITLE_REPORT_PAGE_SIZE = 8
SUBTITLE_REPORT_BUCKET_LABELS = {
    "pending": "待补",
    "cached": "已补",
    "untried": "未尝试",
    "not_found": "未找到",
    "failed": "失败",
    "no_code": "无番号",
}
SUBTITLE_BACKFILL_STATUS_FILTERS = {
    "pending": {"success", "untried"},
    "untried": {"untried"},
    "not_found": {"not_found"},
    "failed": {"failed"},
}
ACTIVE_115_FAST_POLL_WINDOW_SECONDS = 20
ACTIVE_115_FAST_POLL_INTERVAL_SECONDS = 2
ACTIVE_115_SLOW_AFTER_POLLS = 10
ACTIVE_115_SLOW_POLL_INTERVAL_SECONDS = 600
ACTIVE_115_TIMEOUT_SECONDS = 7200
DEFAULT_TASK_WORKERS = 2
DEFAULT_TASK_MESSAGE_EDIT_MIN_INTERVAL_SECONDS = 2
DEFAULT_INTERNAL_API_PORT = 8765
DEFAULT_INTERNAL_API_WORKERS = 3
DEFAULT_INTERNAL_API_OWNER_WORKERS = 2
DEFAULT_INTERNAL_API_SEARCH_TTL_SECONDS = 15 * 60
CATEGORY_LABELS = {"movie": "电影库", "tv": "剧集库", "anime": "动漫库", "adult": "成人库", "other": "其他库"}
CONTENT_PROFILE_LABELS = {
    "adult": "成人",
    "movie": "电影",
    "tv": "剧集",
    "anime": "动漫",
    "other": "其他",
}
DEFAULT_SEARCH_CATEGORY = "movie"
START_TEXT = """媒体资源助手

直接发送关键词、番号、磁链或 115 分享链接，即可搜索并选择入库。
也可以使用下方功能菜单。"""
HELP_TEXT = """使用说明

直接发送关键词、番号、磁链或 115 分享链接即可。

常用入口：
/tasks 查看最近任务
/status <info_hash> 查询任务状态
/diag <info_hash|media_id> 查看任务或MSG媒体诊断
/migrate 迁移已有媒体到其他库，下一条消息输入关键词
/subtitle_report 查看成人库字幕补齐统计
/subtitle_find <番号或标题> 查找成人库影片并重配字幕
/p115_cookie 查看或扫码更新 115 分享转存 Cookie
/dedupe_refresh 刷新已入库记录（需二次确认）
/cancel 退出当前等待输入
/version 查看当前版本

搜索统计会显示来源、耗时、返回/展示数量和 LLM 重排状态。搜索结果可单独补查网盘 115 分享。
搜索结果里选择资源后，再选择入电影、剧集、动漫、成人或其他库。"""
BOT_COMMANDS = [
    {"command": "start", "description": "打开功能菜单"},
    {"command": "help", "description": "查看完整使用说明"},
    {"command": "tasks", "description": "查看最近任务、刷新进度或取消任务"},
    {"command": "status", "description": "按 info_hash 查询任务进度"},
    {"command": "diag", "description": "查看任务或MSG媒体诊断"},
    {"command": "migrate", "description": "迁移已入库媒体到其他库"},
    {"command": "subtitle_report", "description": "查看成人库字幕补齐统计"},
    {"command": "subtitle_find", "description": "按番号或标题查找字幕影片"},
    {"command": "p115_cookie", "description": "查看或扫码更新 115 分享转存 Cookie"},
    {"command": "dedupe_refresh", "description": "刷新重复判断索引（需二次确认）"},
    {"command": "cancel", "description": "退出当前等待输入"},
    {"command": "version", "description": "查看当前版本"},
]

PENDING_ACTION_MIGRATE_QUERY = "migrate_query"
MIGRATE_PROMPT_TEXT = """媒体迁移

请发送要迁移的媒体关键词。
发送 取消、退出、q 或 /cancel 退出迁移。"""
PENDING_CANCEL_TEXTS = {"取消", "退出", "q", "Q"}

SUBTITLE_FIND_PROMPT_TEXT = """字幕影片查找

直接发送：
字幕 番号或标题

例如：
字幕 SSIS-218
字幕 秋色之空"""

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
EXTRA_SCAN_NAME_TOKENS = {
    "bonus",
    "cm",
    "extra",
    "extras",
    "gallery",
    "image",
    "images",
    "menu",
    "menus",
    "pv",
    "pvs",
    "sample",
    "special",
    "specials",
    "tokuten",
    "trailer",
    "trailers",
    "予告",
    "图集",
    "映像特典",
    "特典",
    "特典映像",
    "特報",
    "特报",
    "花絮",
    "菜单",
    "預告",
    "预告",
}
SCRAPE_QUERY_NOISE = {
    "4khdr",
    "4ksdr",
    "aac",
    "bdremux",
    "dbdraws",
    "flac",
    "flacx3",
    "hdr",
    "hevc10bit",
    "imax",
    "mkv",
    "mp4",
    "uhdbdrip",
    "webdl",
    "x264",
    "x265",
    "正片",
    "正片特典映像",
    "特典映像",
    "简繁外挂",
}
SCRAPE_RELEASE_MARKERS = (
    "2160p",
    "1080p",
    "720p",
    "bluray",
    "bdremux",
    "remux",
    "uhdbd",
    "uhdbluray",
    "webrip",
    "webdl",
    "x264",
    "x265",
)
from pipeline.subtitle_asr import (
    DEFAULT_ASR_MODEL,
    DEFAULT_ASR_TIMEOUT_SECONDS,
    DEFAULT_ASR_TRANSLATION_TIMEOUT_SECONDS,
)
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
    pansou_enabled: bool = False
    pansou_url: str = DEFAULT_PANSOU_URL
    pansou_token: str = ""
    pansou_timeout_seconds: int = DEFAULT_PANSOU_TIMEOUT_SECONDS
    pansou_cloud_types: tuple = DEFAULT_PANSOU_CLOUD_TYPES
    pansou_source_type: str = "all"
    pansou_plugins: tuple = ()
    telegram_timeout: int = 90
    msg_base_url: str = DEFAULT_MSG_BASE_URL
    msg_admin_user: str = ""
    msg_admin_password: str = ""
    msg_enabled: bool = False
    msg_sync_poll_seconds: int = 60
    msg_sync_poll_interval_seconds: int = 5
    msg_trash_hide_sync_enabled: bool = False
    msg_trash_hide_sync_limit: int = 100
    openlist_pre_scan_clean_enabled: bool = True
    openlist_pre_scan_clean_max_bytes: int = DEFAULT_OPENLIST_PRE_SCAN_CLEAN_MAX_BYTES
    openlist_adult_code_format_enabled: bool = True
    sync_recovery_interval_seconds: int = 60
    task_workers: int = DEFAULT_TASK_WORKERS
    task_message_edit_min_interval_seconds: float = DEFAULT_TASK_MESSAGE_EDIT_MIN_INTERVAL_SECONDS
    subtitle_auto_match_enabled: bool = False
    subtitle_auto_match_adult_only: bool = False
    subtitle_cache_dir: str = DEFAULT_SUBTITLE_CACHE_DIR
    subtitle_providers: tuple = DEFAULT_SUBTITLE_PROVIDERS
    subtitle_search_timeout_seconds: int = DEFAULT_SUBTITLE_SEARCH_TIMEOUT_SECONDS
    subtitle_download_max_bytes: int = DEFAULT_SUBTITLE_DOWNLOAD_MAX_BYTES
    subtitle_backfill_default_limit: int = DEFAULT_SUBTITLE_BACKFILL_LIMIT
    assrt_api_token: str = ""
    opensubtitles_api_key: str = ""
    opensubtitles_username: str = ""
    opensubtitles_password: str = ""
    openlist_scan_username: str = ""
    openlist_scan_password: str = ""
    search_page_size: int = SEARCH_PAGE_SIZE
    task_list_page_size: int = DEFAULT_TASK_LIST_PAGE_SIZE
    task_list_fetch_limit: int = DEFAULT_TASK_LIST_FETCH_LIMIT
    prowlarr_upstream_search_limit: int = DEFAULT_UPSTREAM_SEARCH_LIMIT
    prowlarr_required_indexer_search_limit: int = DEFAULT_REQUIRED_INDEXER_SEARCH_LIMIT
    prowlarr_anime_indexer_search_limit: int = DEFAULT_ANIME_INDEXER_SEARCH_LIMIT
    prowlarr_primary_indexer_timeout_seconds: int = DEFAULT_PRIMARY_INDEXER_TIMEOUT_SECONDS
    prowlarr_optional_indexer_timeout_seconds: int = DEFAULT_OPTIONAL_INDEXER_TIMEOUT_SECONDS
    prowlarr_max_workers: int = DEFAULT_PROWLARR_MAX_WORKERS
    prowlarr_early_return_after_seconds: float = DEFAULT_PROWLARR_EARLY_RETURN_AFTER_SECONDS
    prowlarr_early_return_min_results: int = DEFAULT_PROWLARR_EARLY_RETURN_MIN_RESULTS
    prowlarr_early_return_required_priority: int = DEFAULT_PROWLARR_EARLY_RETURN_REQUIRED_PRIORITY
    search_profile_categories: dict = None
    search_profile_tag_labels: dict = None
    search_profile_upstream_limits: dict = None
    search_profile_timeout_seconds: dict = None
    search_profile_max_workers: dict = None
    llm_search_rerank_enabled: bool = False
    llm_base_url: str = DEFAULT_LLM_BASE_URL
    llm_model: str = DEFAULT_LLM_MODEL
    llm_api_key: str = ""
    llm_timeout_seconds: int = DEFAULT_LLM_TIMEOUT_SECONDS
    llm_search_rerank_limit: int = DEFAULT_LLM_SEARCH_RERANK_LIMIT
    llm_thinking_disabled: bool = True
    asr_enabled: bool = False
    asr_base_url: str = ""
    asr_api_token: str = ""
    asr_model: str = DEFAULT_ASR_MODEL
    asr_timeout_seconds: int = DEFAULT_ASR_TIMEOUT_SECONDS
    asr_translation_base_url: str = DEFAULT_LLM_BASE_URL
    asr_translation_model: str = DEFAULT_LLM_MODEL
    asr_translation_api_key: str = ""
    asr_translation_timeout_seconds: int = DEFAULT_ASR_TRANSLATION_TIMEOUT_SECONDS
    asr_translation_thinking_disabled: bool = True
    asr_cache_dir: str = "/bot-data/subtitle-asr-cache"
    p115_cookie: str = ""
    internal_api_enabled: bool = False
    internal_api_token: str = ""
    internal_api_host: str = "127.0.0.1"
    internal_api_port: int = DEFAULT_INTERNAL_API_PORT
    internal_api_workers: int = DEFAULT_INTERNAL_API_WORKERS
    internal_api_owner_workers: int = DEFAULT_INTERNAL_API_OWNER_WORKERS
    internal_api_search_ttl_seconds: int = DEFAULT_INTERNAL_API_SEARCH_TTL_SECONDS

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
        llm_search_rerank_enabled = parse_bool(env.get("LLM_SEARCH_RERANK_ENABLED"), False)
        llm_api_key = (env.get("LLM_API_KEY") or env.get("DEEPSEEK_API_KEY") or "").strip()
        if llm_search_rerank_enabled and not llm_api_key:
            raise RuntimeError("LLM_API_KEY missing")
        internal_api_enabled = parse_bool(env.get("INTERNAL_API_ENABLED"), False)
        internal_api_token = (env.get("INTERNAL_API_TOKEN") or "").strip()
        if internal_api_enabled and not internal_api_token:
            raise RuntimeError("INTERNAL_API_TOKEN missing")
        asr_enabled = parse_bool(env.get("ASR_ENABLED"), False)
        asr_base_url = (env.get("ASR_BASE_URL") or "").strip()
        asr_api_token = (env.get("ASR_API_TOKEN") or "").strip()
        asr_translation_base_url = (
            env.get("ASR_TRANSLATION_BASE_URL") or env.get("LLM_BASE_URL") or DEFAULT_LLM_BASE_URL
        ).strip()
        asr_translation_model = (
            env.get("ASR_TRANSLATION_MODEL") or env.get("LLM_MODEL") or DEFAULT_LLM_MODEL
        ).strip()
        asr_translation_api_key = (
            env.get("ASR_TRANSLATION_API_KEY") or llm_api_key
        ).strip()
        if asr_enabled and not asr_base_url:
            raise RuntimeError("ASR_BASE_URL missing")
        if asr_enabled and not asr_api_token:
            raise RuntimeError("ASR_API_TOKEN missing")
        if asr_enabled and not asr_translation_base_url:
            raise RuntimeError("ASR_TRANSLATION_BASE_URL missing")
        if asr_enabled and not asr_translation_model:
            raise RuntimeError("ASR_TRANSLATION_MODEL missing")
        if asr_enabled and not asr_translation_api_key:
            raise RuntimeError("ASR_TRANSLATION_API_KEY missing")

        return cls(
            token=token,
            allowed_user_ids=allowed,
            state_db_path=env.get("BOT_STATE_DB", DEFAULT_STATE_DB),
            search_limit=int(env.get("BOT_SEARCH_LIMIT", DEFAULT_SEARCH_LIMIT)),
            openlist_db=env.get("OPENLIST_DB", DEFAULT_OPENLIST_DB),
            openlist_url=env.get("OPENLIST_URL", DEFAULT_OPENLIST_URL),
            openlist_scan_username=(env.get("OPENLIST_MEDIA_SCAN_USERNAME") or "").strip(),
            openlist_scan_password=env.get("OPENLIST_MEDIA_SCAN_PASSWORD") or "",
            prowlarr_url=env.get("PROWLARR_URL", DEFAULT_PROWLARR_URL),
            prowlarr_config=env.get("PROWLARR_CONFIG", DEFAULT_PROWLARR_CONFIG),
            prowlarr_search_timeout_seconds=int(
                env.get("PROWLARR_SEARCH_TIMEOUT_SECONDS", str(DEFAULT_PROWLARR_SEARCH_TIMEOUT_SECONDS))
            ),
            pansou_enabled=parse_bool(env.get("PANSOU_ENABLED"), bool((env.get("PANSOU_URL") or "").strip())),
            pansou_url=env.get("PANSOU_URL", DEFAULT_PANSOU_URL),
            pansou_token=env.get("PANSOU_TOKEN") or env.get("PANSOU_AUTH_TOKEN") or "",
            pansou_timeout_seconds=int(env.get("PANSOU_TIMEOUT_SECONDS", str(DEFAULT_PANSOU_TIMEOUT_SECONDS))),
            pansou_cloud_types=tuple(parse_csv_strings(env.get("PANSOU_CLOUD_TYPES"), DEFAULT_PANSOU_CLOUD_TYPES)),
            pansou_source_type=env.get("PANSOU_SOURCE_TYPE", "all"),
            pansou_plugins=tuple(parse_csv_strings(env.get("PANSOU_PLUGINS"), ())),
            telegram_timeout=int(env.get("TG_API_TIMEOUT", "90")),
            msg_base_url=env.get("MSG_BASE_URL", DEFAULT_MSG_BASE_URL),
            msg_admin_user=msg_admin_user,
            msg_admin_password=msg_admin_password,
            msg_enabled=parse_bool(env.get("MSG_ENABLED"), bool(msg_admin_user and msg_admin_password)),
            msg_sync_poll_seconds=int(env.get("MSG_SYNC_POLL_SECONDS", "60")),
            msg_sync_poll_interval_seconds=int(env.get("MSG_SYNC_POLL_INTERVAL_SECONDS", "5")),
            msg_trash_hide_sync_enabled=parse_bool(env.get("MSG_TRASH_HIDE_SYNC_ENABLED"), False),
            msg_trash_hide_sync_limit=int(env.get("MSG_TRASH_HIDE_SYNC_LIMIT", "100")),
            openlist_pre_scan_clean_enabled=parse_bool(env.get("OPENLIST_PRE_SCAN_CLEAN_ENABLED"), True),
            openlist_pre_scan_clean_max_bytes=int(
                env.get("OPENLIST_PRE_SCAN_CLEAN_MAX_BYTES", str(DEFAULT_OPENLIST_PRE_SCAN_CLEAN_MAX_BYTES))
            ),
            openlist_adult_code_format_enabled=parse_bool(env.get("OPENLIST_ADULT_CODE_FORMAT_ENABLED"), True),
            sync_recovery_interval_seconds=int(env.get("BOT_SYNC_RECOVERY_INTERVAL_SECONDS", "60")),
            task_workers=max(1, int(env.get("BOT_TASK_WORKERS", str(DEFAULT_TASK_WORKERS)))),
            task_message_edit_min_interval_seconds=max(
                0.0,
                float(
                    env.get(
                        "BOT_TASK_MESSAGE_EDIT_MIN_INTERVAL_SECONDS",
                        str(DEFAULT_TASK_MESSAGE_EDIT_MIN_INTERVAL_SECONDS),
                    )
                ),
            ),
            subtitle_auto_match_enabled=parse_bool(env.get("SUBTITLE_AUTO_MATCH_ENABLED"), False),
            subtitle_auto_match_adult_only=parse_bool(env.get("SUBTITLE_AUTO_MATCH_ADULT_ONLY"), False),
            subtitle_cache_dir=env.get("SUBTITLE_CACHE_DIR", DEFAULT_SUBTITLE_CACHE_DIR),
            subtitle_providers=parse_csv_strings(env.get("SUBTITLE_PROVIDERS"), DEFAULT_SUBTITLE_PROVIDERS),
            subtitle_search_timeout_seconds=max(
                1,
                int(env.get("SUBTITLE_SEARCH_TIMEOUT_SECONDS", str(DEFAULT_SUBTITLE_SEARCH_TIMEOUT_SECONDS))),
            ),
            subtitle_download_max_bytes=max(
                1024,
                int(env.get("SUBTITLE_DOWNLOAD_MAX_BYTES", str(DEFAULT_SUBTITLE_DOWNLOAD_MAX_BYTES))),
            ),
            subtitle_backfill_default_limit=normalize_subtitle_backfill_limit(
                env.get("SUBTITLE_BACKFILL_DEFAULT_LIMIT", str(DEFAULT_SUBTITLE_BACKFILL_LIMIT))
            ),
            assrt_api_token=env.get("ASSRT_API_TOKEN", ""),
            opensubtitles_api_key=env.get("OPENSUBTITLES_API_KEY", ""),
            opensubtitles_username=env.get("OPENSUBTITLES_USERNAME", ""),
            opensubtitles_password=env.get("OPENSUBTITLES_PASSWORD", ""),
            search_page_size=int(env.get("BOT_SEARCH_PAGE_SIZE", str(SEARCH_PAGE_SIZE))),
            task_list_page_size=int(env.get("BOT_TASK_LIST_PAGE_SIZE", str(DEFAULT_TASK_LIST_PAGE_SIZE))),
            task_list_fetch_limit=int(env.get("BOT_TASK_LIST_FETCH_LIMIT", str(DEFAULT_TASK_LIST_FETCH_LIMIT))),
            prowlarr_upstream_search_limit=int(env.get("PROWLARR_UPSTREAM_SEARCH_LIMIT", str(DEFAULT_UPSTREAM_SEARCH_LIMIT))),
            prowlarr_required_indexer_search_limit=int(
                env.get("PROWLARR_REQUIRED_INDEXER_SEARCH_LIMIT", str(DEFAULT_REQUIRED_INDEXER_SEARCH_LIMIT))
            ),
            prowlarr_anime_indexer_search_limit=int(env.get("PROWLARR_ANIME_INDEXER_SEARCH_LIMIT", str(DEFAULT_ANIME_INDEXER_SEARCH_LIMIT))),
            prowlarr_primary_indexer_timeout_seconds=int(
                env.get("PROWLARR_PRIMARY_INDEXER_TIMEOUT_SECONDS", str(DEFAULT_PRIMARY_INDEXER_TIMEOUT_SECONDS))
            ),
            prowlarr_optional_indexer_timeout_seconds=int(
                env.get("PROWLARR_OPTIONAL_INDEXER_TIMEOUT_SECONDS", str(DEFAULT_OPTIONAL_INDEXER_TIMEOUT_SECONDS))
            ),
            prowlarr_max_workers=int(env.get("PROWLARR_MAX_WORKERS", str(DEFAULT_PROWLARR_MAX_WORKERS))),
            prowlarr_early_return_after_seconds=float(
                env.get("PROWLARR_EARLY_RETURN_AFTER_SECONDS", str(DEFAULT_PROWLARR_EARLY_RETURN_AFTER_SECONDS))
            ),
            prowlarr_early_return_min_results=int(
                env.get("PROWLARR_EARLY_RETURN_MIN_RESULTS", str(DEFAULT_PROWLARR_EARLY_RETURN_MIN_RESULTS))
            ),
            prowlarr_early_return_required_priority=int(
                env.get("PROWLARR_EARLY_RETURN_REQUIRED_PRIORITY", str(DEFAULT_PROWLARR_EARLY_RETURN_REQUIRED_PRIORITY))
            ),
            search_profile_categories=search_profile_categories_from_env(env),
            search_profile_tag_labels=search_profile_tag_labels_from_env(env),
            search_profile_upstream_limits=search_profile_upstream_limits_from_env(env),
            search_profile_timeout_seconds=search_profile_timeout_seconds_from_env(env),
            search_profile_max_workers=search_profile_max_workers_from_env(env),
            llm_search_rerank_enabled=llm_search_rerank_enabled,
            llm_base_url=env.get("LLM_BASE_URL", DEFAULT_LLM_BASE_URL),
            llm_model=env.get("LLM_MODEL", DEFAULT_LLM_MODEL),
            llm_api_key=llm_api_key,
            llm_timeout_seconds=int(env.get("LLM_TIMEOUT_SECONDS", str(DEFAULT_LLM_TIMEOUT_SECONDS))),
            llm_search_rerank_limit=int(env.get("LLM_SEARCH_RERANK_LIMIT", str(DEFAULT_LLM_SEARCH_RERANK_LIMIT))),
            llm_thinking_disabled=parse_bool(env.get("LLM_THINKING_DISABLED"), True),
            asr_enabled=asr_enabled,
            asr_base_url=asr_base_url,
            asr_api_token=asr_api_token,
            asr_model=env.get("ASR_MODEL", DEFAULT_ASR_MODEL),
            asr_timeout_seconds=max(
                1,
                int(env.get("ASR_TIMEOUT_SECONDS", str(DEFAULT_ASR_TIMEOUT_SECONDS))),
            ),
            asr_translation_base_url=asr_translation_base_url,
            asr_translation_model=asr_translation_model,
            asr_translation_api_key=asr_translation_api_key,
            asr_translation_timeout_seconds=max(
                1,
                int(
                    env.get(
                        "ASR_TRANSLATION_TIMEOUT_SECONDS",
                        str(DEFAULT_ASR_TRANSLATION_TIMEOUT_SECONDS),
                    )
                ),
            ),
            asr_translation_thinking_disabled=parse_bool(
                env.get("ASR_TRANSLATION_THINKING_DISABLED"),
                parse_bool(env.get("LLM_THINKING_DISABLED"), True),
            ),
            asr_cache_dir=env.get("ASR_CACHE_DIR", "/bot-data/subtitle-asr-cache"),
            p115_cookie=(
                env.get("P115_COOKIE")
                or env.get("SHARE115_COOKIE")
                or env.get("CLOUD115_COOKIE")
                or env.get("PAN115_COOKIE")
                or ""
            ),
            internal_api_enabled=internal_api_enabled,
            internal_api_token=internal_api_token,
            internal_api_host=(env.get("INTERNAL_API_HOST") or "127.0.0.1").strip(),
            internal_api_port=max(1, int(env.get("INTERNAL_API_PORT", str(DEFAULT_INTERNAL_API_PORT)))),
            internal_api_workers=max(1, int(env.get("INTERNAL_API_WORKERS", str(DEFAULT_INTERNAL_API_WORKERS)))),
            internal_api_owner_workers=max(
                1,
                int(env.get("INTERNAL_API_OWNER_WORKERS", str(DEFAULT_INTERNAL_API_OWNER_WORKERS))),
            ),
            internal_api_search_ttl_seconds=max(
                1,
                int(
                    env.get(
                        "INTERNAL_API_SEARCH_TTL_SECONDS",
                        str(DEFAULT_INTERNAL_API_SEARCH_TTL_SECONDS),
                    )
                ),
            ),
        )


class CandidateStore:
    def __init__(self, db_path):
        self.db_path = str(db_path)
        parent = Path(self.db_path).parent
        parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    def _connect(self):
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.execute("pragma busy_timeout = 30000")
        return conn

    def _init_schema(self):
        conn = self._connect()
        try:
            conn.execute("pragma journal_mode = wal")
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
                    metadata_json text not null default '{}',
                    created_at integer not null
                )
                """
            )
            ensure_sqlite_column(conn, "search_sessions", "metadata_json", "text not null default '{}'")
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
                create table if not exists pending_actions (
                    user_id integer not null,
                    chat_id integer not null,
                    action text not null,
                    payload_json text not null,
                    created_at integer not null,
                    primary key (user_id, chat_id)
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
            conn.execute(
                """
                create table if not exists msg_trash_hide_index (
                    media_id text primary key,
                    openlist_path text not null,
                    hide_path text,
                    hide_pattern text,
                    status text not null,
                    reason text,
                    created_at integer not null,
                    updated_at integer not null
                )
                """
            )
            conn.execute(
                """
                create table if not exists candidate_submissions (
                    candidate_id integer primary key,
                    status text not null,
                    info_hash text,
                    error text,
                    created_at integer not null,
                    updated_at integer not null
                )
                """
            )
            conn.execute(
                """
                create table if not exists subtitle_backfill_index (
                    media_id text primary key,
                    adult_code text,
                    title text,
                    status text not null,
                    source text,
                    reason text,
                    error text,
                    attempt_count integer not null default 0,
                    created_at integer not null,
                    updated_at integer not null
                )
                """
            )
            conn.execute(
                """
                create table if not exists bot_settings (
                    key text primary key,
                    value text not null,
                    updated_at integer not null
                )
                """
            )
            conn.execute(
                """
                create table if not exists p115_qr_sessions (
                    id integer primary key autoincrement,
                    user_id integer not null,
                    chat_id integer not null,
                    session_json text not null,
                    status text not null,
                    created_at integer not null,
                    updated_at integer not null
                )
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

    def update_candidate(self, candidate_id, candidate):
        payload = json.dumps(candidate, ensure_ascii=False, sort_keys=True)
        conn = self._connect()
        try:
            cursor = conn.execute(
                "update candidates set candidate_json = ? where id = ?",
                (payload, int(candidate_id)),
            )
            conn.commit()
        finally:
            conn.close()
        if cursor.rowcount != 1:
            raise RuntimeError("candidate not found: %s" % candidate_id)

    def claim_candidate_submission(self, candidate_id):
        now = int(time.time())
        conn = self._connect()
        try:
            cursor = conn.execute(
                """
                insert or ignore into candidate_submissions (
                    candidate_id, status, info_hash, error, created_at, updated_at
                ) values (?, 'running', null, null, ?, ?)
                """,
                (int(candidate_id), now, now),
            )
            conn.commit()
            if cursor.rowcount == 1:
                return {"claimed": True, "status": "running", "candidate_id": int(candidate_id)}
            row = conn.execute(
                """
                select candidate_id, status, info_hash, error, created_at, updated_at
                from candidate_submissions
                where candidate_id = ?
                """,
                (int(candidate_id),),
            ).fetchone()
        finally:
            conn.close()
        if row is None:
            raise RuntimeError("candidate submission claim failed: %s" % candidate_id)
        return candidate_submission_from_row(row, claimed=False)

    def finish_candidate_submission(self, candidate_id, status, info_hash=None, error=None):
        now = int(time.time())
        conn = self._connect()
        try:
            conn.execute(
                """
                update candidate_submissions
                set status = ?, info_hash = ?, error = ?, updated_at = ?
                where candidate_id = ?
                """,
                (status, info_hash, error, now, int(candidate_id)),
            )
            conn.commit()
        finally:
            conn.close()

    def save_search_session(self, user_id, chat_id, category, query, candidate_ids, metadata=None):
        payload = json.dumps([int(candidate_id) for candidate_id in candidate_ids], sort_keys=True)
        metadata_payload = json.dumps(metadata or {}, ensure_ascii=False, sort_keys=True)
        conn = self._connect()
        try:
            cursor = conn.execute(
                """
                insert into search_sessions (user_id, chat_id, category, query, candidate_ids_json, metadata_json, created_at)
                values (?, ?, ?, ?, ?, ?, ?)
                """,
                (int(user_id), int(chat_id), category, query, payload, metadata_payload, int(time.time())),
            )
            conn.commit()
            return cursor.lastrowid
        finally:
            conn.close()

    def load_search_session(self, session_id):
        conn = self._connect()
        try:
            row = conn.execute(
                "select id, user_id, chat_id, category, query, candidate_ids_json, metadata_json from search_sessions where id = ?",
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
            "metadata": json.loads(row[6] or "{}"),
        }

    def update_search_session(self, session_id, candidate_ids, metadata=None):
        payload = json.dumps([int(candidate_id) for candidate_id in candidate_ids], sort_keys=True)
        metadata_payload = json.dumps(metadata or {}, ensure_ascii=False, sort_keys=True)
        conn = self._connect()
        try:
            cursor = conn.execute(
                """
                update search_sessions
                set candidate_ids_json = ?, metadata_json = ?
                where id = ?
                """,
                (payload, metadata_payload, int(session_id)),
            )
            conn.commit()
        finally:
            conn.close()
        if cursor.rowcount != 1:
            raise RuntimeError("search session not found: %s" % session_id)

    def find_search_session_by_candidate(self, candidate_id):
        target = int(candidate_id)
        conn = self._connect()
        try:
            rows = conn.execute(
                """
                select id, user_id, chat_id, category, query, candidate_ids_json, metadata_json
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
                    "metadata": json.loads(row[6] or "{}"),
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

    def save_pending_action(self, user_id, chat_id, action, payload=None):
        payload_json = json.dumps(payload or {}, ensure_ascii=False, sort_keys=True)
        conn = self._connect()
        try:
            conn.execute(
                """
                insert into pending_actions (user_id, chat_id, action, payload_json, created_at)
                values (?, ?, ?, ?, ?)
                on conflict(user_id, chat_id) do update set
                    action = excluded.action,
                    payload_json = excluded.payload_json,
                    created_at = excluded.created_at
                """,
                (int(user_id), int(chat_id), action, payload_json, int(time.time())),
            )
            conn.commit()
        finally:
            conn.close()

    def load_pending_action(self, user_id, chat_id):
        conn = self._connect()
        try:
            row = conn.execute(
                """
                select action, payload_json, created_at
                from pending_actions
                where user_id = ? and chat_id = ?
                """,
                (int(user_id), int(chat_id)),
            ).fetchone()
        finally:
            conn.close()
        if row is None:
            return None
        return {"action": row[0], "payload": json.loads(row[1]), "created_at": row[2]}

    def clear_pending_action(self, user_id, chat_id):
        conn = self._connect()
        try:
            cursor = conn.execute(
                "delete from pending_actions where user_id = ? and chat_id = ?",
                (int(user_id), int(chat_id)),
            )
            conn.commit()
            return cursor.rowcount > 0
        finally:
            conn.close()

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

    def set_setting(self, key, value):
        key = str(key or "").strip()
        if not key:
            raise ValueError("setting key must not be empty")
        now = int(time.time())
        conn = self._connect()
        try:
            conn.execute(
                """
                insert into bot_settings (key, value, updated_at)
                values (?, ?, ?)
                on conflict(key) do update set
                    value = excluded.value,
                    updated_at = excluded.updated_at
                """,
                (key, str(value or ""), now),
            )
            conn.commit()
        finally:
            conn.close()

    def get_setting(self, key, default=""):
        key = str(key or "").strip()
        if not key:
            return default
        conn = self._connect()
        try:
            row = conn.execute("select value from bot_settings where key = ?", (key,)).fetchone()
        finally:
            conn.close()
        if row is None:
            return default
        return row[0]

    def set_p115_cookie(self, cookie):
        cookie = str(cookie or "").strip()
        if not p115_cookie_is_valid(cookie):
            raise ValueError("invalid 115 cookie")
        self.set_setting(P115_COOKIE_SETTING_KEY, cookie)

    def get_p115_cookie(self):
        return self.get_setting(P115_COOKIE_SETTING_KEY, "")

    def create_p115_qr_session(self, user_id, chat_id, session):
        now = int(time.time())
        payload = json.dumps(session or {}, ensure_ascii=False, sort_keys=True)
        conn = self._connect()
        try:
            cursor = conn.execute(
                """
                insert into p115_qr_sessions (user_id, chat_id, session_json, status, created_at, updated_at)
                values (?, ?, ?, ?, ?, ?)
                """,
                (int(user_id), int(chat_id), payload, "pending", now, now),
            )
            conn.commit()
            return cursor.lastrowid
        finally:
            conn.close()

    def load_p115_qr_session(self, session_id):
        conn = self._connect()
        try:
            row = conn.execute(
                """
                select id, user_id, chat_id, session_json, status, created_at, updated_at
                from p115_qr_sessions
                where id = ?
                """,
                (int(session_id),),
            ).fetchone()
        finally:
            conn.close()
        if row is None:
            raise RuntimeError("p115 qr session not found: %s" % session_id)
        return {
            "id": row[0],
            "user_id": row[1],
            "chat_id": row[2],
            "session": json.loads(row[3]),
            "status": row[4],
            "created_at": row[5],
            "updated_at": row[6],
        }

    def update_p115_qr_session(self, session_id, status, session=None):
        status = str(status or "").strip()
        if not status:
            raise ValueError("p115 qr status must not be empty")
        current = self.load_p115_qr_session(session_id)
        payload = json.dumps(session if session is not None else current["session"], ensure_ascii=False, sort_keys=True)
        now = int(time.time())
        conn = self._connect()
        try:
            conn.execute(
                """
                update p115_qr_sessions
                set session_json = ?, status = ?, updated_at = ?
                where id = ?
                """,
                (payload, status, now, int(session_id)),
            )
            conn.commit()
        finally:
            conn.close()
        current["session"] = json.loads(payload)
        current["status"] = status
        current["updated_at"] = now
        return current

    def processed_trash_hide_media_ids(self, media_ids):
        normalized = [str(value or "").strip() for value in media_ids or [] if str(value or "").strip()]
        if not normalized:
            return set()
        found = set()
        conn = self._connect()
        try:
            for media_id in normalized:
                row = conn.execute(
                    "select media_id from msg_trash_hide_index where media_id = ?",
                    (media_id,),
                ).fetchone()
                if row is not None:
                    found.add(row[0])
        finally:
            conn.close()
        return found

    def save_trash_hide_result(self, result):
        media_id = str((result or {}).get("media_id") or "").strip()
        openlist_path = normalize_openlist_path((result or {}).get("openlist_path"))
        status = str((result or {}).get("status") or "").strip()
        if not media_id:
            raise ValueError("trash hide media_id must not be empty")
        if not openlist_path:
            raise ValueError("trash hide openlist_path must not be empty")
        if not status:
            raise ValueError("trash hide status must not be empty")
        now = int(time.time())
        conn = self._connect()
        try:
            conn.execute(
                """
                insert into msg_trash_hide_index (
                    media_id, openlist_path, hide_path, hide_pattern, status, reason, created_at, updated_at
                ) values (?, ?, ?, ?, ?, ?, ?, ?)
                on conflict(media_id) do update set
                    openlist_path = excluded.openlist_path,
                    hide_path = excluded.hide_path,
                    hide_pattern = excluded.hide_pattern,
                    status = excluded.status,
                    reason = excluded.reason,
                    updated_at = excluded.updated_at
                """,
                (
                    media_id,
                    openlist_path,
                    normalize_openlist_path((result or {}).get("hide_path")),
                    str((result or {}).get("hide_pattern") or "").strip(),
                    status,
                    str((result or {}).get("reason") or "").strip(),
                    now,
                    now,
                ),
            )
            conn.commit()
        finally:
            conn.close()

    def subtitle_backfill_records(self, media_ids):
        normalized = [str(value or "").strip() for value in media_ids or [] if str(value or "").strip()]
        if not normalized:
            return {}
        normalized_set = set(normalized)
        out = {}
        conn = self._connect()
        try:
            for media_id in normalized:
                row = conn.execute(
                    """
                    select media_id, adult_code, title, status, source, reason, error, attempt_count, created_at, updated_at
                    from subtitle_backfill_index
                    where media_id = ?
                    """,
                    (media_id,),
                ).fetchone()
                if row is not None:
                    out[media_id] = subtitle_backfill_record_from_row(row)
            task_rows = conn.execute(
                """
                select title, task_json
                from offline_tasks
                where category = 'adult'
                order by updated_at desc
                """
            ).fetchall()
            declared_media_ids = set()
            for task_title, task_json in task_rows:
                task = json.loads(task_json)
                media_id = str(task.get("msg_media_id") or "").strip()
                if media_id not in normalized_set or media_id in declared_media_ids:
                    continue
                if not adult_source_declares_chinese_subtitles(task_title, task):
                    continue
                previous = out.get(media_id) or {}
                declared_code = first_adult_code(
                    [
                        task.get("openlist_adult_code"),
                        task_title,
                        task.get("name"),
                        task.get("openlist_adult_format_old_path"),
                        task.get("openlist_adult_video_old_path"),
                    ]
                )
                out[media_id] = {
                    "media_id": media_id,
                    "adult_code": str(declared_code or previous.get("adult_code") or "").strip(),
                    "title": str(task_title or previous.get("title") or "").strip(),
                    "status": "declared",
                    "source": "resource_name",
                    "reason": SOURCE_DECLARED_CHINESE_SUBTITLE_REASON,
                    "error": "",
                    "attempt_count": int(previous.get("attempt_count") or 0),
                    "created_at": previous.get("created_at"),
                    "updated_at": previous.get("updated_at"),
                }
                declared_media_ids.add(media_id)
        finally:
            conn.close()
        return out

    def save_subtitle_backfill_record(self, media_id, title, adult_code, match):
        media_id = str(media_id or "").strip()
        if not media_id:
            raise ValueError("subtitle backfill media_id must not be empty")
        status = subtitle_backfill_record_status(match)
        now = int(time.time())
        source = str((match or {}).get("subtitle_match_source") or "").strip()
        reason = str((match or {}).get("subtitle_match_reason") or "").strip()
        error = str((match or {}).get("subtitle_match_error") or "").strip()
        conn = self._connect()
        try:
            conn.execute(
                """
                insert into subtitle_backfill_index (
                    media_id, adult_code, title, status, source, reason, error, attempt_count, created_at, updated_at
                ) values (?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
                on conflict(media_id) do update set
                    adult_code = excluded.adult_code,
                    title = excluded.title,
                    status = excluded.status,
                    source = excluded.source,
                    reason = excluded.reason,
                    error = excluded.error,
                    attempt_count = subtitle_backfill_index.attempt_count + 1,
                    updated_at = excluded.updated_at
                """,
                (media_id, str(adult_code or "").strip(), title or "", status, source, reason, error, now, now),
            )
            conn.commit()
        finally:
            conn.close()

    def list_msg_sync_running_tasks(self, limit=50):
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
        running = [
            record
            for record in records
            if TASK_STATE.is_offline_success(record["task"])
            and task_sync_is_running(record["task"])
            and not task_msg_synced(record["task"])
        ]
        return running[: int(limit)]

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
        active = [record for record in records if TASK_STATE.is_offline_active(record["task"])]
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

    def set_my_commands(self, commands):
        return self._request("setMyCommands", {"commands": list(commands or [])})

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


class LlmRerankBusy(RuntimeError):
    pass


class MediaStationIngestPending(RuntimeError):
    pass


class PipelineBotService:
    def __init__(self, config, p115_cookie_provider=None):
        self.config = config
        self.p115_cookie_provider = p115_cookie_provider
        self._llm_rerank_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="llm-rerank")
        self._llm_rerank_lock = threading.Lock()
        self._subtitle_matcher = None
        self._search_capabilities_lock = threading.Lock()
        self._search_capabilities_cache = None
        self._search_capabilities_cached_at = 0.0
        self._prowlarr_search_cache = ProwlarrSearchCache()

    def search_capabilities(self, cache_seconds=60):
        now = time.monotonic()
        with self._search_capabilities_lock:
            if self._search_capabilities_cache is not None and now - self._search_capabilities_cached_at < cache_seconds:
                return dict(self._search_capabilities_cache)
            capabilities = {
                "pansou": bool(self.config.pansou_enabled),
                "bt4g": False,
                "llm_rerank": bool(self.config.llm_search_rerank_enabled),
            }
            try:
                api_key = ProwlarrConfig(self.config.prowlarr_config).load_api_key()
                prowlarr = ProwlarrClient(
                    self.config.prowlarr_url,
                    api_key,
                    timeout=self.config.prowlarr_search_timeout_seconds,
                )
                capabilities["bt4g"] = any(
                    indexer_enabled(indexer) and indexer_matches_label(indexer, "BT4G")
                    for indexer in prowlarr.indexers()
                )
            except Exception as exc:
                capabilities["bt4g_error"] = str(exc)
            self._search_capabilities_cache = capabilities
            self._search_capabilities_cached_at = now
            return dict(capabilities)

    def search(self, query, category, limit=DEFAULT_SEARCH_LIMIT, profile=None):
        profile = profile or search_profile_for_query(category, query)
        stats = SearchStats()
        api_key = ProwlarrConfig(self.config.prowlarr_config).load_api_key()
        prowlarr = ProwlarrClient(
            self.config.prowlarr_url,
            api_key,
            timeout=self.config.prowlarr_search_timeout_seconds,
            search_cache=self._prowlarr_search_cache,
        )
        indexers = prowlarr.indexers()
        tags = safe_prowlarr_tags(prowlarr)
        categories_by_profile = self.config.search_profile_categories or SEARCH_PROFILE_CATEGORIES
        tag_labels_by_profile = self.config.search_profile_tag_labels or SEARCH_PROFILE_TAG_LABELS
        upstream_limit = search_profile_value(
            self.config.search_profile_upstream_limits,
            profile,
            self.config.prowlarr_upstream_search_limit,
        )
        timeout_seconds = search_profile_value(
            self.config.search_profile_timeout_seconds,
            profile,
            self.config.prowlarr_search_timeout_seconds,
        )
        max_workers = search_profile_value(
            self.config.search_profile_max_workers,
            profile,
            self.config.prowlarr_max_workers,
        )
        search_settings = {
            "upstream_limit": int(upstream_limit),
            "timeout_seconds": int(timeout_seconds),
            "max_workers": int(max_workers),
            "categories": list(categories_by_profile.get(profile, ())),
            "tag_labels": list(tag_labels_by_profile.get(profile, ())),
            "early_return_after_seconds": float(self.config.prowlarr_early_return_after_seconds),
            "early_return_min_results": int(self.config.prowlarr_early_return_min_results),
            "early_return_required_priority": int(self.config.prowlarr_early_return_required_priority),
            "llm_rerank_enabled": bool(self.config.llm_search_rerank_enabled),
            "llm_rerank_limit": int(self.config.llm_search_rerank_limit),
        }
        candidates = search_profile_indexer_results(
            prowlarr,
            query,
            profile,
            max(int(limit), int(upstream_limit)),
            indexers=indexers,
            tags=tags,
            timeout_seconds=timeout_seconds,
            stats=stats,
            categories_by_profile=categories_by_profile,
            tag_labels_by_profile=tag_labels_by_profile,
            max_workers=max_workers,
            early_return_after_seconds=self.config.prowlarr_early_return_after_seconds,
            early_return_min_results=self.config.prowlarr_early_return_min_results,
            early_return_required_priority=self.config.prowlarr_early_return_required_priority,
        )
        try:
            ranked = ResourceSelector(indexer_priorities=indexer_priority_map(indexers)).select_ranked_limited(candidates, query=query, limit=limit)
        except RuntimeError as exc:
            attach_search_metadata(
                exc,
                stats.to_metadata(profile=profile, raw_count=len(candidates), selected_count=0, settings=search_settings),
            )
            raise
        return SearchResultList(
            ranked,
            metadata=stats.to_metadata(profile=profile, raw_count=len(candidates), selected_count=len(ranked), settings=search_settings),
        )

    def rerank_search_candidates(self, query, category, candidates):
        if not self.config.llm_search_rerank_enabled:
            raise RuntimeError("LLM rerank disabled")
        return self._rerank_search_candidates_with_timeout(query, category, candidates)

    def rank_subtitle_candidates(self, media, query, candidates):
        if not self.config.llm_search_rerank_enabled:
            raise RuntimeError("LLM rerank disabled")
        return self._rank_subtitle_candidates_with_timeout(media, query, candidates)

    def search_adult(self, query, limit=DEFAULT_SEARCH_LIMIT):
        return self.search(query, "adult", limit=limit, profile=SEARCH_PROFILE_ADULT)

    def search_anime(self, query, limit=DEFAULT_SEARCH_LIMIT):
        return self.search(query, "movie", limit=limit, profile=SEARCH_PROFILE_ANIME)

    def search_bt4g(self, query, limit=DEFAULT_SEARCH_LIMIT):
        stats = SearchStats()
        api_key = ProwlarrConfig(self.config.prowlarr_config).load_api_key()
        prowlarr = ProwlarrClient(
            self.config.prowlarr_url,
            api_key,
            timeout=self.config.prowlarr_search_timeout_seconds,
            search_cache=self._prowlarr_search_cache,
        )
        indexers = prowlarr.indexers()
        bt4g_indexers = [indexer for indexer in indexers if indexer_enabled(indexer) and indexer_matches_label(indexer, "BT4G")]
        if not bt4g_indexers:
            raise RuntimeError("Prowlarr indexer not found: BT4G")

        categories_by_profile = self.config.search_profile_categories or SEARCH_PROFILE_CATEGORIES
        categories = categories_by_profile.get(SEARCH_PROFILE_GENERAL, SEARCH_PROFILE_CATEGORIES[SEARCH_PROFILE_GENERAL])
        upstream_limit = search_profile_value(
            self.config.search_profile_upstream_limits,
            SEARCH_PROFILE_GENERAL,
            self.config.prowlarr_upstream_search_limit,
        )
        request_limit = max(int(limit), int(upstream_limit))
        search_settings = {
            "upstream_limit": int(upstream_limit),
            "categories": list(categories),
            "indexers": [indexer.get("name") or indexer.get("id") for indexer in bt4g_indexers],
        }
        candidates = []
        for indexer in bt4g_indexers:
            indexer_id = indexer.get("id")
            source = indexer.get("name") or indexer_id
            candidates.extend(
                stats.measure(
                    source,
                    lambda indexer_id=indexer_id: prowlarr.search(
                        query,
                        limit=request_limit,
                        indexer_ids=[indexer_id],
                        categories=categories,
                    ),
                    phase="bt4g_indexer",
                    indexer_id=indexer_id,
                )
            )

        try:
            ranked = ResourceSelector(indexer_priorities=indexer_priority_map(indexers)).select_ranked_limited(candidates, query=query, limit=limit)
        except RuntimeError as exc:
            attach_search_metadata(
                exc,
                stats.to_metadata(profile="bt4g", raw_count=len(candidates), selected_count=0, settings=search_settings),
            )
            raise
        return SearchResultList(
            ranked,
            metadata=stats.to_metadata(profile="bt4g", raw_count=len(candidates), selected_count=len(ranked), settings=search_settings),
        )

    def search_pansou(self, query, limit=DEFAULT_SEARCH_LIMIT):
        if not self.config.pansou_enabled:
            raise RuntimeError("PanSou is not enabled")
        client = PanSouClient(
            self.config.pansou_url,
            token=self.config.pansou_token,
            timeout=self.config.pansou_timeout_seconds,
        )
        return client.search(
            query,
            limit=limit,
            cloud_types=self.config.pansou_cloud_types,
            source_type=self.config.pansou_source_type,
            plugins=self.config.pansou_plugins,
        )

    def _rerank_search_candidates_with_timeout(self, query, category, ranked):
        timeout = max(0.1, float(self.config.llm_timeout_seconds or DEFAULT_LLM_TIMEOUT_SECONDS))
        if not self._llm_rerank_lock.acquire(blocking=False):
            raise LlmRerankBusy("previous LLM rerank is still running")
        try:
            future = self._llm_rerank_executor.submit(
                self._build_search_reranker().rerank_search_candidates,
                query,
                category,
                ranked,
                max_candidates=self.config.llm_search_rerank_limit,
            )
        except Exception:
            self._llm_rerank_lock.release()
            raise
        future.add_done_callback(self._release_llm_rerank_lock)
        try:
            return future.result(timeout=timeout)
        except FutureTimeoutError:
            future.cancel()
            raise

    def _rank_subtitle_candidates_with_timeout(self, media, query, candidates):
        timeout = max(0.1, float(self.config.llm_timeout_seconds or DEFAULT_LLM_TIMEOUT_SECONDS))
        if not self._llm_rerank_lock.acquire(blocking=False):
            raise LlmRerankBusy("previous LLM rerank is still running")
        try:
            future = self._llm_rerank_executor.submit(
                self._build_search_reranker().rank_subtitle_candidates,
                media,
                query,
                candidates,
                max_candidates=self.config.llm_search_rerank_limit,
            )
        except Exception:
            self._llm_rerank_lock.release()
            raise
        future.add_done_callback(self._release_llm_rerank_lock)
        try:
            return future.result(timeout=timeout)
        except FutureTimeoutError:
            future.cancel()
            raise

    def _release_llm_rerank_lock(self, future):
        self._llm_rerank_lock.release()

    def search_migration_candidates(self, query, limit=20):
        result = self._build_msg_client().pipeline_search_migration_candidates(query, limit=limit)
        items = result.get("items") if isinstance(result, dict) else None
        if not isinstance(items, list):
            raise RuntimeError("MediaStationGo migration search returned invalid response")
        category_by_library_id = {
            root.get("library_id"): category
            for category, root in msg_library_roots().items()
            if root.get("library_id")
        }
        normalized = []
        for item in items:
            if not isinstance(item, dict):
                raise RuntimeError("MediaStationGo migration search returned invalid candidate")
            candidate = dict(item)
            candidate["category"] = category_by_library_id.get(candidate.get("library_id"), candidate.get("category") or "")
            normalized.append(candidate)
        return normalized

    def _build_search_reranker(self):
        return SearchRerankClient(
            base_url=self.config.llm_base_url,
            api_key=self.config.llm_api_key,
            model=self.config.llm_model,
            timeout=self.config.llm_timeout_seconds,
            thinking_disabled=self.config.llm_thinking_disabled,
        )

    def msg_media_diagnostics(self, media_id):
        media_id = str(media_id or "").strip()
        if not media_id:
            raise ValueError("MediaStationGo media id missing")
        return self._build_msg_client().get_media(media_id)

    def submit(self, category, download_uri):
        if is_115_share_url(download_uri):
            return self._submit_115_share(category, download_uri)
        download_uri = self._resolve_download_uri(download_uri)
        result = summarize_submit(
            self._call_115(category, lambda client: client.add_offline_urls([download_uri], category_to_folder_id(category)))
        )
        ensure_submit_result_has_task_identity(result, download_uri)
        return result

    def _resolve_download_uri(self, download_uri):
        if not is_prowlarr_download_uri(download_uri):
            return download_uri
        api_key = ProwlarrConfig(self.config.prowlarr_config).load_api_key()
        return ProwlarrClient(self.config.prowlarr_url, api_key).resolve_download_uri(download_uri)

    def _submit_115_share(self, category, download_uri):
        folder_id = category_to_folder_id(category)
        client = self._build_115_share_client()
        response = client.receive_share_url(download_uri, folder_id)
        data = response.get("data") or {}
        task_id = share_receive_task_id(data.get("share_code"))
        task = {
            "info_hash": task_id,
            "source_kind": "115_share",
            "name": ", ".join(item.get("name") or "" for item in data.get("items") or [] if item.get("name")) or data.get("share_code"),
            "status_name": "success",
            "percent_done": 100,
            "wp_path_id": folder_id,
            "file_id": first_value(data.get("save_as_top_fids")),
            "share_code": data.get("share_code"),
            "source_url": data.get("source_url") or download_uri,
            "received_item_count": len(data.get("items") or []),
        }
        if self.config.msg_enabled:
            task["msg_sync_status"] = "running"
            task["msg_error"] = None
        return {
            "state": True,
            "code": response.get("code", 0),
            "message": "115分享转存完成",
            "submit_kind": "115_share_receive",
            "tasks": [task],
            "task_status": task,
            "raw": response,
        }

    def task_status(self, category, info_hash):
        return self._call_115(category, lambda client: find_task_by_info_hash(client, info_hash, max_pages=10))

    def task_statuses(self, category, info_hashes):
        return self._call_115(category, lambda client: find_tasks_by_info_hashes(client, info_hashes, max_pages=10))

    def cancel_task(self, category, info_hash):
        return self._call_115(category, lambda client: cancel_task_if_active(client, info_hash, max_pages=10))

    def migrate_media_candidate(self, candidate, target_category):
        target = build_migration_target(candidate, target_category)
        msg_client = self._build_msg_client()
        source = {
            "category": candidate.get("category"),
            "library_id": candidate.get("library_id"),
            "library_root_id": candidate.get("library_root_id"),
            "source_openlist_path": candidate.get("source_openlist_path"),
            "source_kind": candidate.get("source_kind"),
        }
        msg_target = self._pipeline_maintenance_target(target_category)
        validation = msg_client.pipeline_validate_migration(source, msg_target)
        if (
            not isinstance(validation, dict)
            or validation.get("source_openlist_path") != candidate.get("source_openlist_path")
            or validation.get("target_openlist_path") != target.get("target_openlist_path")
        ):
            raise RuntimeError("MediaStationGo migration validation returned invalid response")

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

        result = msg_client.pipeline_apply_migration(source, msg_target)
        if (
            not isinstance(result, dict)
            or result.get("source_openlist_path") != candidate.get("source_openlist_path")
            or result.get("target_openlist_path") != target.get("target_openlist_path")
        ):
            raise RuntimeError("MediaStationGo migration apply returned invalid response")
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

    def check_duplicate(self, category, query, candidate, target=None):
        if not self.config.msg_enabled and target is None:
            return None
        queries = duplicate_media_queries(category, query, candidate)
        if not queries:
            return None
        client = self._build_msg_client()
        root = self._msg_target(category, target)
        media = find_matching_media(
            extract_media_items(client.search_media(queries[0], limit=20)),
            queries,
            library_id=root["library_id"],
        )
        codes = extract_codes(" ".join(queries))
        if media is None and category == "adult" and codes:
            media = find_matching_media(
                extract_media_items(client.list_library_media(root["library_id"], page=1, page_size=200, group_versions=0)),
                queries,
                library_id=root["library_id"],
            )
        if media is None:
            return None
        media_codes = extract_codes(media_haystack(media))
        matched_codes = sorted(codes.intersection(media_codes))
        strong_code_match = category == "adult" and bool(matched_codes)
        identity_value = matched_codes[0] if strong_code_match else (queries[0] if queries else "")
        return {
            "level": "strong" if strong_code_match else "weak",
            "reason": "mediastation_code" if strong_code_match else "mediastation_title",
            "source": "MediaStationGo",
            "title": media_display_title(media),
            "path": media_primary_path(media),
            "media_id": extract_media_id(media),
            "identity_type": "adult_code" if strong_code_match else "title_query",
            "identity_value": identity_value,
            "can_force": not strong_code_match,
        }

    def validate_upgrade_target(self, media_id, target):
        return self._validate_upgrade_target_media(self._build_msg_client(), media_id, target)

    def upgrade_duplicate_matches_target(self, upgrade_target, duplicate, category):
        duplicate_media_id = str((duplicate or {}).get("media_id") or "").strip()
        if not duplicate_media_id:
            raise ValueError("duplicate media_id is required for upgrade validation")
        if duplicate_media_id == str(extract_media_id(upgrade_target) or "").strip():
            return True
        duplicate_media = extract_media_detail(self._build_msg_client().get_media(duplicate_media_id))
        actual_duplicate_id = str(extract_media_id(duplicate_media) or "").strip()
        if actual_duplicate_id != duplicate_media_id:
            raise RuntimeError("MediaStationGo duplicate media detail is missing or mismatched")
        return media_is_same_upgrade_work(category, upgrade_target, duplicate_media)

    def _validate_upgrade_target_media(self, client, media_id, target):
        media_id = str(media_id or "").strip()
        if not media_id:
            raise ValueError("upgrade_media_id is required")
        target = dict(target or {})
        target_library_id = str(target.get("library_id") or "").strip()
        target_root_id = str(target.get("root_id") or "").strip()
        if not target_library_id or not target_root_id:
            raise ValueError("升级目标缺少媒体库或目录信息")

        media = extract_media_detail(client.get_media(media_id))
        actual_media_id = str(extract_media_id(media) or "").strip()
        if actual_media_id != media_id:
            raise ValueError("升级目标作品不存在或媒体 ID 不匹配")
        actual_library_id = str(media_first_value(media, ("library_id", "libraryId")) or "").strip()
        if actual_library_id != target_library_id:
            raise ValueError("升级目标作品不属于当前媒体库")
        actual_root_id = str(
            media_first_value(media, ("library_root_id", "libraryRootId", "root_id", "rootId")) or ""
        ).strip()
        if actual_root_id and actual_root_id != target_root_id:
            raise ValueError("升级目标作品不属于当前入库目录")
        return media

    def remove_upgrade_target(self, old_media_id, new_media_id, target, upgrade_scope="media", new_source_paths=None):
        old_media_id = str(old_media_id or "").strip()
        new_media_id = str(new_media_id or "").strip()
        if not old_media_id or not new_media_id:
            raise ValueError("升级版本处理缺少媒体 ID")
        if old_media_id == new_media_id:
            if str(upgrade_scope or "media").strip().lower() != "work":
                return {"status": "skipped", "reason": "same_media_id", "media_id": old_media_id}
        client = self._build_msg_client()

        scope = str(upgrade_scope or "media").strip().lower()
        if scope == "work":
            paths = unique_openlist_paths(new_source_paths or [])
            if not paths:
                raise ValueError("整剧升级缺少本次扫描的新片源目录")
            result = client.pipeline_replace_work_source(old_media_id, new_media_id, target, paths)
            if not isinstance(result, dict) or result.get("status") not in ("success", "already_removed"):
                raise RuntimeError("MediaStationGo整剧旧片源清理返回无效响应")
            return result
        if scope != "media":
            raise ValueError("unsupported upgrade_scope: %s" % scope)
        try:
            self._validate_upgrade_target_media(client, old_media_id, target)
        except MediaStationApiError as exc:
            if exc.status_code == 404:
                return {"status": "already_removed", "media_id": old_media_id}
            raise
        client.soft_delete_media_version(old_media_id, old_media_id)
        return {"status": "removed", "media_id": old_media_id}

    def collect_openlist_dedupe_entries(self, refresh=True):
        client = self._build_openlist_scan_client()
        entries = []
        for category in CATEGORY_LABELS:
            path = category_to_openlist_path(category)
            if refresh:
                client.list_path(path, refresh=True)
            entries.extend(openlist_dedupe_entries(client, category, path))
        return unique_dedupe_entries(entries)

    def _build_openlist_scan_client(self):
        token = OpenListPasswordTokenProvider(
            self.config.openlist_url,
            self.config.openlist_scan_username,
            self.config.openlist_scan_password,
        ).load_token()
        return OpenListClient(self.config.openlist_url, token)

    def sync_completed_task(
        self,
        category,
        title,
        task,
        progress_callback=None,
        target=None,
        preferred_scrape_queries=None,
        upgrade_media_id=None,
    ):
        out = dict(task or {})
        if not TASK_STATE.is_offline_success(out):
            return out
        if task_msg_synced(out):
            return out
        if not self.config.msg_enabled and target is None:
            return out

        def capture_progress(progress):
            out.update(progress)
            if progress_callback:
                progress_callback(dict(progress))

        try:
            result = self._sync_mediastation(
                category,
                title,
                out,
                progress_callback=capture_progress,
                target=target,
                preferred_scrape_queries=preferred_scrape_queries,
                upgrade_media_id=upgrade_media_id,
            )
        except MediaStationIngestPending:
            out["msg_sync_status"] = "running"
            out["msg_error"] = None
            return out
        except (RuntimeError, ValueError) as exc:
            mark_current_sync_stage_failed(out, str(exc))
            out["msg_sync_status"] = "failed"
            out["msg_error"] = str(exc)
            out["msg_synced_at"] = int(time.time())
            return out
        out.update(result)
        return out

    def match_task_subtitles(self, category, title, task, force=False):
        return self._build_subtitle_matcher().match_task(category, title, task, force=force)

    def subtitle_rematch_candidates_adult(self, media_id, limit=DEFAULT_SUBTITLE_REMATCH_LIMIT):
        if not self.config.msg_enabled:
            raise RuntimeError("MediaStationGo is disabled")
        media_id = str(media_id or "").strip()
        if not media_id:
            raise ValueError("media_id is required")
        client = self._build_msg_client()
        matcher = self._build_subtitle_matcher()
        media = extract_media_detail(client.get_media(media_id))
        detail_id = extract_media_id(media)
        if not detail_id:
            raise RuntimeError("MediaStationGo media detail missing id: %s" % media_id)
        if detail_id != media_id:
            raise RuntimeError("MediaStationGo media id mismatch: expected %s, got %s" % (media_id, detail_id))
        title = media_display_title(media) or media_id
        task = subtitle_backfill_task_from_media(media)
        code = task.get("openlist_adult_code") or ""
        candidates = matcher.search_task_candidates("adult", title, task, limit=limit)
        return {
            "media_id": media_id,
            "title": title,
            "code": code,
            "query": code or title,
            "task": task,
            "media": {"media_id": media_id, "title": title, "code": code},
            "candidates": candidates,
        }

    def subtitle_search_candidates(self, media_id, limit=DEFAULT_SUBTITLE_REMATCH_LIMIT):
        if not self.config.msg_enabled:
            raise RuntimeError("MediaStationGo is disabled")
        media_id = str(media_id or "").strip()
        if not media_id:
            raise ValueError("media_id is required")
        limit = max(1, min(int(limit or DEFAULT_SUBTITLE_REMATCH_LIMIT), 50))
        client = self._build_msg_client()
        matcher = self._build_subtitle_matcher()
        media = extract_media_detail(client.get_media(media_id))
        detail_id = extract_media_id(media)
        if not detail_id:
            raise RuntimeError("MediaStationGo media detail missing id: %s" % media_id)
        if detail_id != media_id:
            raise RuntimeError("MediaStationGo media id mismatch: expected %s, got %s" % (media_id, detail_id))
        category = subtitle_category_from_media(media)
        title = media_display_title(media) or media_id
        search_title = subtitle_search_title(media) or title
        task = subtitle_backfill_task_from_media(media, category=category)
        code = task.get("openlist_adult_code") or ""
        candidates = matcher.search_task_candidates(category, search_title, task, limit=limit, manual=True)
        normalized = []
        for candidate in candidates:
            item = dict(candidate)
            subtitle_title = str(item.get("title") or "").strip()
            item.update(
                {
                    "media_id": media_id,
                    "title": title,
                    "subtitle_title": subtitle_title,
                    "category": category,
                    "code": code,
                }
            )
            normalized.append(item)
        return {
            "media_id": media_id,
            "title": title,
            "category": category,
            "code": code,
            "query": code or search_title,
            "candidates": normalized,
        }

    def subtitle_find_adult(self, query, limit=DEFAULT_SUBTITLE_FIND_LIMIT):
        if not self.config.msg_enabled:
            raise RuntimeError("MediaStationGo is disabled")
        query = str(query or "").strip()
        if not query:
            raise ValueError("请输入番号或标题")
        limit = max(1, int(limit or DEFAULT_SUBTITLE_FIND_LIMIT))
        root = category_to_msg_library_root("adult")
        client = self._build_msg_client()
        matcher = self._build_subtitle_matcher()
        store = CandidateStore(self.config.state_db_path)

        media_by_id = {}

        def add_media(media):
            media_id = extract_media_id(media)
            if not media_id or media_id in media_by_id:
                return
            if not media_belongs_to_library(media, root["library_id"]):
                return
            if subtitle_find_media_matches_query(media, query):
                media_by_id[media_id] = media

        for media in extract_media_items(client.search_media(query, limit=max(limit * 3, 20))):
            add_media(media)
            if len(media_by_id) >= limit:
                break

        page = 1
        page_size = 200
        while len(media_by_id) < limit and page <= SUBTITLE_FIND_SCAN_PAGE_LIMIT:
            items = extract_media_items(client.list_library_media(root["library_id"], page=page, page_size=page_size, group_versions=0))
            if not items:
                break
            for media in items:
                add_media(media)
                if len(media_by_id) >= limit:
                    break
            if len(items) < page_size:
                break
            page += 1

        media_items = sorted(media_by_id.values(), key=lambda item: subtitle_find_media_score(item, query), reverse=True)
        records = store.subtitle_backfill_records([extract_media_id(media) for media in media_items])
        return {
            "query": query,
            "limit": limit,
            "items": [subtitle_find_result_item_from_media(media, matcher, records) for media in media_items[:limit]],
        }

    def apply_subtitle_candidate(self, candidate_record):
        candidate_record = dict(candidate_record or {})
        media_id = str(candidate_record.get("media_id") or "").strip()
        if not media_id:
            raise ValueError("media_id is required")
        title = str(candidate_record.get("title") or media_id)
        code = str(candidate_record.get("code") or "")
        result = self._build_subtitle_matcher().apply_candidate(media_id, candidate_record)
        CandidateStore(self.config.state_db_path).save_subtitle_backfill_record(media_id, title, code, result)
        out = dict(result)
        out.update({"media_id": media_id, "title": title, "code": code})
        return out

    def preview_subtitle_candidate(self, candidate_record, max_chars=8000):
        return self._build_subtitle_matcher().preview_candidate(candidate_record, max_chars=max_chars)

    def preview_subtitle_candidates(self, candidates, limit=DEFAULT_SUBTITLE_LLM_PREVIEW_LIMIT, max_chars=DEFAULT_SUBTITLE_LLM_PREVIEW_CHARS):
        matcher = self._build_subtitle_matcher()
        limit = max(0, int(limit or 0))
        out = []
        for index, candidate in enumerate(candidates or []):
            if index >= limit:
                out.append(dict(candidate))
                continue
            try:
                out.append(matcher.preview_candidate(candidate, max_chars=max_chars))
            except Exception as exc:
                item = dict(candidate)
                item["preview_error"] = str(exc)
                out.append(item)
        return out

    def subtitle_backfill_adult(self, limit=DEFAULT_SUBTITLE_BACKFILL_LIMIT, progress_callback=None, retry_attempted=False, status_filter=None):
        if not self.config.msg_enabled:
            raise RuntimeError("MediaStationGo is disabled")
        limit = normalize_subtitle_backfill_limit(limit)
        status_filter = normalize_subtitle_backfill_status_filter(status_filter)
        root = category_to_msg_library_root("adult")
        client = self._build_msg_client()
        matcher = self._build_subtitle_matcher()
        store = CandidateStore(self.config.state_db_path)
        result = new_subtitle_backfill_result(limit, retry_attempted=retry_attempted, status_filter=status_filter)

        def emit(force=False):
            if progress_callback:
                progress_callback(dict(result), force=force)

        page = 1
        page_size = 200
        while result["attempted"] < limit:
            items = extract_media_items(client.list_library_media(root["library_id"], page=page, page_size=page_size, group_versions=0))
            if not items:
                break
            backfill_records = store.subtitle_backfill_records([extract_media_id(media) for media in items])
            for media in items:
                media_status = subtitle_backfill_media_status(media, matcher, backfill_records)
                if status_filter and media_status not in SUBTITLE_BACKFILL_STATUS_FILTERS[status_filter]:
                    result["scanned"] += 1
                    continue
                attempted = process_subtitle_backfill_media(
                    media,
                    matcher,
                    store,
                    result,
                    retry_attempted=retry_attempted,
                    backfill_records=backfill_records,
                    before_match=emit,
                )
                if attempted:
                    emit()
                if result["attempted"] >= limit:
                    break
            if len(items) < page_size:
                break
            page += 1
        result["status"] = "success"
        result["current"] = {}
        emit(force=True)
        return result

    def subtitle_backfill_one_adult(self, media_id, retry_attempted=False):
        if not self.config.msg_enabled:
            raise RuntimeError("MediaStationGo is disabled")
        media_id = str(media_id or "").strip()
        if not media_id:
            raise ValueError("media_id is required")
        client = self._build_msg_client()
        matcher = self._build_subtitle_matcher()
        store = CandidateStore(self.config.state_db_path)
        media = extract_media_detail(client.get_media(media_id))
        detail_id = extract_media_id(media)
        if not detail_id:
            raise RuntimeError("MediaStationGo media detail missing id: %s" % media_id)
        if detail_id != media_id:
            raise RuntimeError("MediaStationGo media id mismatch: expected %s, got %s" % (media_id, detail_id))
        result = new_subtitle_backfill_result(1, retry_attempted=retry_attempted)
        process_subtitle_backfill_media(
            media,
            matcher,
            store,
            result,
            retry_attempted=retry_attempted,
            backfill_records=store.subtitle_backfill_records([media_id]),
        )
        result["status"] = "success"
        result["current"] = {}
        return result

    def subtitle_backfill_report_adult(self):
        if not self.config.msg_enabled:
            raise RuntimeError("MediaStationGo is disabled")
        root = category_to_msg_library_root("adult")
        client = self._build_msg_client()
        matcher = self._build_subtitle_matcher()
        store = CandidateStore(self.config.state_db_path)
        report = new_subtitle_backfill_report()
        page = 1
        page_size = 200
        while True:
            items = extract_media_items(client.list_library_media(root["library_id"], page=page, page_size=page_size, group_versions=0))
            if not items:
                break
            records = store.subtitle_backfill_records([extract_media_id(media) for media in items])
            for media in items:
                add_media_to_subtitle_backfill_report(report, matcher, records, media)
            if len(items) < page_size:
                break
            page += 1
        report["generated_at"] = int(time.time())
        return report

    def _build_subtitle_matcher(self):
        if self._subtitle_matcher is None:
            self._subtitle_matcher = build_subtitle_matcher_from_config(self.config)
        return self._subtitle_matcher

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

    def _build_115_share_client(self):
        cookie = ""
        if self.p115_cookie_provider is not None:
            cookie = str(self.p115_cookie_provider() or "").strip()
        if not cookie:
            cookie = self.config.p115_cookie
        return Share115Client(cookie)

    def _sync_mediastation(
        self,
        category,
        title,
        task,
        progress_callback=None,
        target=None,
        preferred_scrape_queries=None,
        upgrade_media_id=None,
    ):
        progress = dict(task or {})
        msg_target = self._msg_target(category, target)
        root_openlist_path = msg_target["root_openlist_path"]
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
                openlist_client = self._refresh_openlist_for_msg(root_openlist_path)
            return openlist_client

        def get_msg_client():
            nonlocal msg_client
            if msg_client is None:
                msg_client = self._build_msg_client()
            return msg_client

        format_result = prefixed_task_fields(progress, "openlist_adult_")
        if category == "adult" and self.config.openlist_adult_code_format_enabled:
            if not stage_is_complete(progress.get("openlist_adult_format_status")):
                emit({"openlist_adult_format_status": "running", "openlist_adult_format_error": None})
                format_result = self._format_openlist_adult_before_msg(
                    get_openlist_client(),
                    category,
                    title,
                    progress,
                    root_openlist_path,
                    is_upgrade=bool(str(upgrade_media_id or "").strip()),
                )
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

        trash_hide_result = prefixed_task_fields(progress, "openlist_trash_hide_")
        if self.config.msg_trash_hide_sync_enabled:
            if not stage_is_complete(progress.get("openlist_trash_hide_status")):
                emit({"openlist_trash_hide_status": "running", "openlist_trash_hide_error": None})
                trash_hide_result = self._sync_msg_trash_to_openlist_hide(get_msg_client(), get_openlist_client())
                if trash_hide_result.get("openlist_trash_hide_status") != "skipped":
                    emit(trash_hide_result)
                else:
                    apply_progress(trash_hide_result)
            else:
                apply_progress(trash_hide_result)
        else:
            trash_hide_result = {"openlist_trash_hide_status": "skipped", "openlist_trash_hide_reason": "disabled"}
            apply_progress(trash_hide_result)

        clean_result = prefixed_task_fields(progress, "openlist_clean_")
        if self.config.openlist_pre_scan_clean_enabled:
            if not stage_is_complete(progress.get("openlist_clean_status")):
                emit({"openlist_clean_status": "running", "openlist_clean_error": None})
                clean_result = self._clean_openlist_before_msg(
                    get_openlist_client(), category, title, progress, root_openlist_path
                )
                if clean_result.get("openlist_clean_status") != "skipped":
                    emit(clean_result)
                else:
                    apply_progress(clean_result)
            else:
                apply_progress(clean_result)
        else:
            clean_result = {"openlist_clean_status": "skipped", "openlist_clean_reason": "disabled"}
            apply_progress(clean_result)

        queries = media_search_queries(title, progress)
        if format_result.get("openlist_adult_code"):
            queries = [format_result["openlist_adult_code"]] + queries

        media_id = progress.get("msg_media_id")
        media_title = progress.get("msg_media_title")
        root = msg_target
        media = None
        if adult_format_changed_media_path(format_result) and progress.get("msg_scan_status") == "success":
            emit(stale_msg_media_reset_updates(progress, media_id, reason="formatted_path_changed"))
            media_id = None
            media_title = None
        if progress.get("msg_scan_status") == "success" and media_id:
            media = self._load_existing_msg_media(get_msg_client(), media_id)
            if media is None:
                emit(stale_msg_media_reset_updates(progress, media_id))
                media_id = None
                media_title = None

        adult_extra_hide_result = prefixed_task_fields(progress, "openlist_adult_extra_hide_")
        if (
            category == "adult"
            and self.config.openlist_pre_scan_clean_enabled
            and progress.get("msg_scan_status") != "success"
        ):
            if not stage_is_complete(progress.get("openlist_adult_extra_hide_status")):
                emit({"openlist_adult_extra_hide_status": "running", "openlist_adult_extra_hide_error": None})
                adult_extra_hide_result = self._hide_openlist_adult_extra_videos_before_msg(
                    get_openlist_client(), category, queries, progress, root_openlist_path
                )
                if adult_extra_hide_result.get("openlist_adult_extra_hide_status") != "skipped":
                    emit(adult_extra_hide_result)
                else:
                    apply_progress(adult_extra_hide_result)
            else:
                apply_progress(adult_extra_hide_result)

        require_target_path = bool(msg_authoritative_openlist_paths(progress))
        deleted_media_prune_result = prefixed_task_fields(progress, "msg_deleted_media_prune_")
        target_scan_result = prefixed_task_fields(progress, "msg_target_scan_")
        ingest_result = prefixed_task_fields(progress, "msg_ingest_")

        if progress.get("msg_scan_status") != "success" or not media_id:
            client = get_msg_client()
            authoritative_paths = msg_authoritative_openlist_paths(progress) if require_target_path else []
            emit({"msg_scan_status": "running", "msg_error": None})
            ingest_result = self._run_msg_pipeline_ingest(
                client,
                category,
                title,
                progress,
                queries,
                authoritative_paths,
                require_target_path,
                emit,
                msg_target,
            )
            media = ingest_result.pop("_media")
            deleted_media_prune_result = prefixed_task_fields(ingest_result, "msg_deleted_media_prune_")
            target_scan_result = prefixed_task_fields(ingest_result, "msg_target_scan_")
            apply_progress(ingest_result)
            media_id = extract_media_id(media)
            if not media_id:
                raise RuntimeError("MediaStationGo media id missing after scan")
            media_title = media_display_title(media)
            emit(
                {
                    "msg_scan_status": "success",
                    "msg_media_id": media_id,
                    "msg_media_title": media_title,
                    "msg_match_mode": media.get("_pipeline_match_mode") or "query",
                    "msg_match_path": media.get("_pipeline_match_path"),
                }
            )

        if not stage_is_complete(progress.get("msg_scrape_status")):
            if not root.get("scrape_enabled", True):
                emit(
                    {
                        "msg_scrape_status": "skipped",
                        "msg_scrape_mode": "disabled",
                        "msg_scrape_reason": "category_disabled",
                        "msg_media_id": media_id,
                        "msg_media_title": media_title,
                    }
                )
            else:
                emit({"msg_scrape_status": "running", "msg_media_id": media_id, "msg_media_title": media_title})
                scrape_result = self._scrape_msg_media(
                    get_msg_client(),
                    category,
                    media_id,
                    title,
                    progress,
                    media,
                    msg_target,
                    preferred_scrape_queries,
                )
                emit({"msg_scrape_status": "success", "msg_media_id": media_id, "msg_media_title": media_title, **scrape_result})
        if not stage_is_complete(progress.get("msg_scrape_status")):
            raise RuntimeError("MediaStationGo scrape stage incomplete")
        extras_result = prefixed_task_fields(progress, "msg_extra_cleanup_")
        movie_cleanup_touched_openlist = int(clean_result.get("openlist_hidden_count") or 0) > 0 or int(
            clean_result.get("openlist_cleaned_count") or 0
        ) > 0
        if category == "movie" and movie_cleanup_touched_openlist:
            if not stage_is_complete(progress.get("msg_extra_cleanup_status")):
                emit({"msg_extra_cleanup_status": "running", "msg_extra_cleanup_error": None})
                extras_result = self._repair_msg_movie_extras(
                    get_msg_client(), category, media_id, get_openlist_client(), msg_target
                )
                if extras_result.get("msg_extra_cleanup_status") == "skipped":
                    apply_progress(extras_result)
                else:
                    emit(extras_result)
            else:
                apply_progress(extras_result)
        visibility_result = prefixed_task_fields(progress, "msg_visibility_repair_")
        if category in ("tv", "anime"):
            if not stage_is_complete(progress.get("msg_visibility_repair_status")):
                emit({"msg_visibility_repair_status": "running", "msg_visibility_repair_error": None})
                visibility_result = self._repair_msg_episode_visibility(
                    get_msg_client(), category, media_id, msg_target
                )
                if visibility_result.get("msg_visibility_repair_status") == "skipped":
                    apply_progress(visibility_result)
                else:
                    emit(visibility_result)
            else:
                apply_progress(visibility_result)
        msg_library_id = (root or {}).get("library_id") or progress.get("msg_library_id")
        msg_root_id = (root or {}).get("root_id") or progress.get("msg_root_id")
        subtitle_result = prefixed_task_fields(progress, "subtitle_match_")
        if self.config.subtitle_auto_match_enabled:
            if not stage_is_complete(progress.get("subtitle_match_status")):
                emit({"subtitle_match_status": "running", "subtitle_match_error": None, "msg_media_id": media_id, "msg_media_title": media_title})
                subtitle_result = self._match_subtitles(get_msg_client(), category, title, progress)
                if subtitle_result.get("subtitle_match_status") == "skipped":
                    apply_progress(subtitle_result)
                else:
                    emit(subtitle_result)
            else:
                apply_progress(subtitle_result)
        else:
            subtitle_result = {"subtitle_match_status": "skipped", "subtitle_match_reason": "disabled"}
            apply_progress(subtitle_result)
        return {
            "msg_sync_status": "success",
            "msg_scan_status": "success",
            "msg_scrape_status": progress.get("msg_scrape_status"),
            "msg_library_id": msg_library_id,
            "msg_root_id": msg_root_id,
            "msg_media_id": media_id,
            "msg_media_title": media_title,
            "msg_match_mode": progress.get("msg_match_mode"),
            "msg_match_path": progress.get("msg_match_path"),
            "msg_error": None,
            "msg_synced_at": int(time.time()),
            **clean_result,
            **format_result,
            **trash_hide_result,
            **adult_extra_hide_result,
            **extras_result,
            **visibility_result,
            **ingest_result,
            **deleted_media_prune_result,
            **target_scan_result,
            **subtitle_result,
        }

    def _load_existing_msg_media(self, client, media_id):
        try:
            media = extract_media_detail(client.get_media(media_id))
        except MediaStationApiError as exc:
            if exc.status_code == 404:
                return None
            raise
        detail_id = extract_media_id(media)
        if not detail_id:
            raise RuntimeError("MediaStationGo media detail missing id: %s" % media_id)
        if str(detail_id) != str(media_id):
            raise RuntimeError("MediaStationGo media id mismatch: expected %s, got %s" % (media_id, detail_id))
        return media

    def _match_subtitles(self, client, category, title, task):
        try:
            media_id = str((task or {}).get("msg_media_id") or "").strip()
            if not media_id:
                raise RuntimeError("MediaStationGo media id missing before subtitle detection")
            presence = normalize_msg_subtitle_presence(client.pipeline_subtitle_status(media_id), media_id)
            details = {
                "subtitle_match_presence_checked": True,
                "subtitle_match_existing_external_count": len(presence["external"]),
                "subtitle_match_existing_embedded_count": len(presence["embedded"]),
                "subtitle_match_unknown_embedded_count": presence["unknown_embedded"],
            }
            if presence["has_chinese"]:
                chinese_tracks = [
                    track for track in presence["external"] + presence["embedded"] if track.get("chinese") is True
                ]
                sources = unique_nonempty_values(track.get("kind") or track.get("source") for track in chinese_tracks)
                return {
                    "subtitle_match_status": "skipped",
                    "subtitle_match_reason": "existing_chinese_subtitle",
                    "subtitle_match_source": "+".join(sources),
                    "subtitle_match_count": len(chinese_tracks),
                    **details,
                }
            if presence["unknown_embedded"] > 0:
                raise RuntimeError(
                    "MediaStationGo found %d embedded subtitle track(s) without language metadata; "
                    "cannot confirm that Chinese subtitles are absent" % presence["unknown_embedded"]
                )
            result = self.match_task_subtitles(category, title, task, force=True)
            return {**result, **details}
        except Exception as exc:
            return {"subtitle_match_status": "failed", "subtitle_match_error": str(exc)}

    def _scrape_msg_media(
        self,
        client,
        category,
        media_id,
        title,
        task,
        media=None,
        target=None,
        preferred_queries=None,
    ):
        root = self._msg_target(category, target)
        provider = root.get("provider")
        media_type = root.get("media_type")
        result = client.pipeline_scrape_media(
            media_id,
            category,
            title,
            msg_scrape_queries(title, task, media, preferred_queries),
            provider,
            media_type,
        )
        if not isinstance(result, dict) or result.get("mode") not in ("apply", "smart"):
            raise RuntimeError("MediaStationGo pipeline scrape returned invalid response")
        return {
            "msg_scrape_mode": result.get("mode"),
            "msg_scrape_query": result.get("query"),
            "msg_scrape_applied_count": int(result.get("applied_count") or 0),
        }

    def _msg_target(self, category, target=None):
        if target is None:
            root = category_to_msg_library_root(category)
            root["root_openlist_path"] = category_to_openlist_path(category)
            return root
        if not isinstance(target, dict):
            raise ValueError("MediaStationGo target must be an object")
        normalized = {}
        for key in ("library_id", "root_id", "root_openlist_path", "provider", "media_type"):
            value = str(target.get(key) or "").strip()
            if not value:
                raise ValueError("MediaStationGo target %s missing" % key)
            normalized[key] = value
        normalized["root_openlist_path"] = normalize_openlist_path(normalized["root_openlist_path"])
        if not normalized["root_openlist_path"].startswith("/"):
            raise ValueError("MediaStationGo target root_openlist_path must be absolute")
        normalized["scrape_enabled"] = bool(target.get("scrape_enabled", True))
        return normalized

    def _pipeline_maintenance_target(self, category, target=None):
        root = self._msg_target(category, target)
        return {
            "category": category,
            "library_id": root.get("library_id"),
            "root_id": root.get("root_id"),
            "root_openlist_path": root.get("root_openlist_path"),
        }

    def _repair_msg_movie_extras(self, client, category, media_id, openlist_client=None, target=None):
        result = client.pipeline_repair_movie_extras(media_id, self._pipeline_maintenance_target(category, target))
        if not isinstance(result, dict):
            raise RuntimeError("MediaStationGo movie extra cleanup returned invalid response")
        status = result.get("status")
        if status not in ("success", "skipped"):
            raise RuntimeError("MediaStationGo movie extra cleanup returned invalid status: %s" % (status or "-"))
        hide_patterns = [str(pattern) for pattern in result.get("openlist_hide_patterns") or [] if str(pattern or "").strip()]
        hide_path = str(result.get("openlist_hide_path") or "").strip()
        if hide_patterns:
            if not hide_path:
                raise RuntimeError("MediaStationGo movie extra cleanup returned hide patterns without path")
            client = openlist_client or OpenListClient(self.config.openlist_url, OpenListTokenProvider().load_token())
            client.upsert_meta_hide(hide_path, hide_patterns, h_sub=True)
        return {
            "msg_extra_cleanup_status": status,
            "msg_extra_cleanup_updated": int(result.get("updated") or 0),
            "msg_extra_cleanup_media_count": int(result.get("media_count") or 0),
            "msg_extra_cleanup_hidden_count": len(hide_patterns),
            "msg_extra_cleanup_reason": result.get("reason"),
            "msg_extra_cleanup_error": None,
        }

    def _sync_msg_trash_to_openlist_hide(self, msg_client, openlist_client):
        store = CandidateStore(self.config.state_db_path)
        response = msg_client.pipeline_list_deleted_media_hide_candidates(limit=self.config.msg_trash_hide_sync_limit)
        candidates = response.get("items") if isinstance(response, dict) else None
        if not isinstance(candidates, list):
            raise RuntimeError("MediaStationGo deleted media hide candidates returned invalid response")
        processed = store.processed_trash_hide_media_ids([candidate["media_id"] for candidate in candidates])
        pending = [candidate for candidate in candidates if candidate["media_id"] not in processed]
        if not pending:
            return {
                "openlist_trash_hide_status": "skipped",
                "openlist_trash_hide_reason": "no_pending",
                "openlist_trash_hide_scanned_count": len(candidates),
                "openlist_trash_hide_pending_count": 0,
                "openlist_trash_hide_hidden_count": 0,
                "openlist_trash_hide_skipped_count": 0,
                "openlist_trash_hide_error": None,
                "openlist_trash_hide_at": int(time.time()),
            }

        existing = []
        missing = []
        for candidate in pending:
            if openlist_child_exists_for_hide(openlist_client, candidate["target_openlist_path"]):
                existing.append(candidate)
            else:
                missing.append(candidate)

        hide_groups = defaultdict(list)
        for candidate in existing:
            pattern = str(candidate.get("hide_pattern") or "").strip()
            if pattern and pattern not in hide_groups[candidate["hide_path"]]:
                hide_groups[candidate["hide_path"]].append(pattern)

        for hide_path, patterns in sorted(hide_groups.items()):
            openlist_client.upsert_meta_hide(hide_path, patterns, h_sub=True)

        for candidate in existing:
            store.save_trash_hide_result(
                {
                    "media_id": candidate["media_id"],
                    "openlist_path": candidate["target_openlist_path"],
                    "hide_path": candidate["hide_path"],
                    "hide_pattern": candidate["hide_pattern"],
                    "status": "hidden",
                    "reason": "meta_hide",
                }
            )
        for candidate in missing:
            store.save_trash_hide_result(
                {
                    "media_id": candidate["media_id"],
                    "openlist_path": candidate["target_openlist_path"],
                    "hide_path": candidate["hide_path"],
                    "hide_pattern": candidate["hide_pattern"],
                    "status": "skipped",
                    "reason": "target_missing",
                }
            )

        return {
            "openlist_trash_hide_status": "success",
            "openlist_trash_hide_reason": "synced",
            "openlist_trash_hide_scanned_count": len(candidates),
            "openlist_trash_hide_pending_count": len(pending),
            "openlist_trash_hide_hidden_count": len(existing),
            "openlist_trash_hide_skipped_count": len(missing),
            "openlist_trash_hide_meta_count": len(hide_groups),
            "openlist_trash_hide_error": None,
            "openlist_trash_hide_at": int(time.time()),
        }

    def _hide_openlist_adult_extra_videos_before_msg(self, client, category, queries, task, root_openlist_path=None):
        result = hide_openlist_adult_extra_videos(
            client,
            root_openlist_path or category_to_openlist_path(category),
            queries,
            task=task,
            pre_scan_max_bytes=self.config.openlist_pre_scan_clean_max_bytes,
        )
        if not isinstance(result, dict):
            raise RuntimeError("OpenList adult extra video hide returned invalid response")
        status = result.get("openlist_adult_extra_hide_status")
        if status not in ("success", "skipped"):
            raise RuntimeError("OpenList adult extra video hide returned invalid status: %s" % (status or "-"))
        return result

    def _repair_msg_episode_visibility(self, client, category, media_id, target=None):
        result = client.pipeline_repair_episode_visibility(media_id, self._pipeline_maintenance_target(category, target))
        if not isinstance(result, dict):
            raise RuntimeError("MediaStationGo episode visibility repair returned invalid response")
        status = result.get("status")
        if status not in ("success", "skipped"):
            raise RuntimeError("MediaStationGo episode visibility repair returned invalid status: %s" % (status or "-"))
        return {
            "msg_visibility_repair_status": status,
            "msg_visibility_repair_updated": int(result.get("updated") or 0),
            "msg_visibility_repair_media_count": int(result.get("media_count") or 0),
            "msg_visibility_repair_reason": result.get("reason"),
            "msg_visibility_repair_error": None,
        }

    def _run_msg_pipeline_ingest(
        self,
        client,
        category,
        title,
        progress,
        queries,
        authoritative_paths,
        require_target_path,
        emit,
        target=None,
    ):
        root = self._msg_target(category, target)
        previous_failed = progress.get("msg_ingest_status") == "failed"
        job_id = "" if previous_failed else str(progress.get("msg_ingest_job_id") or "").strip()
        idempotency_key = "" if previous_failed else str(progress.get("msg_ingest_idempotency_key") or "").strip()
        if not idempotency_key:
            idempotency_key = "media-pipeline:%s" % uuid.uuid4().hex
        emit(
            {
                "msg_ingest_status": "running",
                "msg_ingest_idempotency_key": idempotency_key,
                "msg_ingest_error": None,
            }
        )

        if job_id:
            job = client.pipeline_get_ingest(job_id)
        else:
            request = {
                **self._pipeline_maintenance_target(category, root),
                "idempotency_key": idempotency_key,
                "title": title,
                "queries": list(queries or []),
                "target_openlist_paths": list(authoritative_paths or []),
                "require_target_path": bool(require_target_path),
                "prune_deleted_openlist_paths": list(authoritative_paths or []) if progress.get("msg_stale_media_id") else [],
                "scan": True,
                "repair_movie_extras": False,
                "repair_episode_visibility": False,
            }
            job = client.pipeline_start_ingest(request)

        deadline = time.monotonic() + max(0, int(self.config.msg_sync_poll_seconds))
        interval = max(1, int(self.config.msg_sync_poll_interval_seconds))
        while True:
            if not isinstance(job, dict):
                raise RuntimeError("MediaStationGo pipeline ingest returned invalid response")
            job_id = str(job.get("id") or "").strip()
            status = str(job.get("status") or "").strip()
            if not job_id or status not in ("running", "completed", "failed"):
                raise RuntimeError("MediaStationGo pipeline ingest returned invalid job")
            emit(
                {
                    "msg_ingest_job_id": job_id,
                    "msg_ingest_status": status,
                    "msg_ingest_stage": job.get("stage"),
                    "msg_ingest_message": job.get("message"),
                    "msg_ingest_error": job.get("error") or None,
                }
            )
            if status == "completed":
                break
            if status == "failed":
                raise RuntimeError("MediaStationGo pipeline ingest failed: %s" % (job.get("error") or "unknown error"))
            if time.monotonic() >= deadline:
                raise MediaStationIngestPending("MediaStationGo pipeline ingest is still running: %s" % job_id)
            time.sleep(interval)
            job = client.pipeline_get_ingest(job_id)

        result = job.get("result")
        if not isinstance(result, dict):
            raise RuntimeError("MediaStationGo pipeline ingest result missing")
        media_result = result.get("media")
        if not isinstance(media_result, dict) or not media_result.get("id"):
            raise RuntimeError("MediaStationGo pipeline ingest media missing")
        match_mode = media_result.get("match_mode") or "query"
        media = {
            "id": media_result.get("id"),
            "title": media_result.get("title"),
            "path": media_result.get("path"),
            "library_id": root.get("library_id"),
            "library_root_id": root.get("root_id"),
            "_pipeline_match_mode": match_mode,
            "_pipeline_match_path": media_result.get("path") if match_mode == "path" else media_result.get("match_path"),
        }
        updates = {
            "_media": media,
            "msg_ingest_status": "completed",
            "msg_ingest_error": None,
        }
        scan_result = result.get("scan")
        if isinstance(scan_result, dict):
            updates.update(
                {
                    "msg_ingest_scan_visited": int(scan_result.get("visited") or 0),
                    "msg_ingest_scan_added": int(scan_result.get("added") or 0),
                    "msg_ingest_scan_updated": int(scan_result.get("updated") or 0),
                    "msg_ingest_scan_removed": int(scan_result.get("removed") or 0),
                }
            )
        prune_result = result.get("deleted_media_prune")
        if isinstance(prune_result, dict):
            prune_status = prune_result.get("status")
            if prune_status not in ("success", "skipped"):
                raise RuntimeError("MediaStationGo pipeline ingest prune returned invalid status")
            updates.update(
                {
                    "msg_deleted_media_prune_status": prune_status,
                    "msg_deleted_media_prune_deleted": int(prune_result.get("deleted") or 0),
                    "msg_deleted_media_prune_reason": prune_result.get("reason"),
                    "msg_deleted_media_prune_error": None,
                }
            )
        if authoritative_paths:
            updates.update(
                {
                    "msg_target_scan_status": "success",
                    "msg_target_scan_root_id": root.get("root_id"),
                    "msg_target_scan_path": authoritative_paths[0],
                    "msg_target_scan_match_path": media_result.get("match_path") or authoritative_paths[0],
                    "msg_target_scan_error": None,
                }
            )
        else:
            updates.update(
                {
                    "msg_target_scan_status": "skipped",
                    "msg_target_scan_reason": "root_scan",
                    "msg_target_scan_error": None,
                }
            )
        return updates

    def _refresh_openlist_for_msg(self, root_openlist_path):
        path = normalize_openlist_path(root_openlist_path)
        if not path:
            raise ValueError("OpenList root path missing")
        client = OpenListClient(self.config.openlist_url, OpenListTokenProvider().load_token())
        client.list_path(path, refresh=True)
        return client

    def _clean_openlist_before_msg(self, client, category, title, task, root_openlist_path=None):
        if not self.config.openlist_pre_scan_clean_enabled:
            return {"openlist_clean_status": "skipped", "openlist_clean_reason": "disabled"}
        try:
            return clean_openlist_task_media(
                client,
                root_openlist_path or category_to_openlist_path(category),
                media_search_queries(title, task),
                task=task,
                max_bytes=self.config.openlist_pre_scan_clean_max_bytes,
                hide_extra_scan_items=category == "movie",
            )
        except (RuntimeError, ValueError) as exc:
            return {
                "openlist_clean_status": "failed",
                "openlist_clean_error": str(exc),
                "openlist_cleaned_count": 0,
                "openlist_cleaned_bytes": 0,
                "openlist_cleaned_at": int(time.time()),
            }

    def _format_openlist_adult_before_msg(
        self,
        client,
        category,
        title,
        task,
        root_openlist_path=None,
        is_upgrade=False,
    ):
        if category != "adult":
            return {}
        if not self.config.openlist_adult_code_format_enabled:
            return {"openlist_adult_format_status": "skipped", "openlist_adult_format_reason": "disabled"}
        try:
            return format_openlist_adult_code(
                client,
                root_openlist_path or category_to_openlist_path(category),
                media_search_queries(title, task),
                task=task,
                allow_existing_target=is_upgrade,
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

def msg_target_openlist_paths(category, task):
    task = task or {}
    paths = []
    for key in (
        "openlist_adult_format_new_path",
        "openlist_adult_format_path",
        "openlist_clean_target",
        "openlist_adult_extra_hide_path",
    ):
        value = normalize_openlist_path(task.get(key))
        if value:
            paths.append(value)

    root_path = normalize_openlist_path(category_to_openlist_path(category))
    if root_path:
        for name in task_openlist_target_names(task):
            paths.append(posixpath.join(root_path.rstrip("/") or "/", name))
    return unique_openlist_paths(paths)


def msg_authoritative_openlist_paths(task):
    task = task or {}
    paths = []
    for key in (
        "openlist_adult_format_new_path",
        "openlist_adult_format_path",
        "openlist_clean_target",
        "openlist_adult_extra_hide_path",
    ):
        value = normalize_openlist_path(task.get(key))
        if value:
            paths.append(value)
    return unique_openlist_paths(paths)


def task_openlist_target_names(task):
    names = []
    for key in ("name", "file_name", "filename"):
        value = str((task or {}).get(key) or "").strip().replace("\\", "/")
        if not value:
            continue
        name = posixpath.basename(value.rstrip("/"))
        if name:
            names.append(name)
    return unique_nonempty_values(names)


def unique_openlist_paths(paths):
    seen = set()
    out = []
    for path in paths or []:
        normalized = normalize_openlist_path(path)
        key = normalized.casefold()
        if normalized and key not in seen:
            seen.add(key)
            out.append(normalized)
    return out


def media_path_values(value):
    paths = []
    if isinstance(value, dict):
        for key in ("path", "file_path", "source_path", "strm_url", "url"):
            item = value.get(key)
            if isinstance(item, str) and item.strip():
                paths.append(item)
        for child in value.values():
            if isinstance(child, (dict, list)):
                paths.extend(media_path_values(child))
    elif isinstance(value, list):
        for child in value:
            paths.extend(media_path_values(child))
    return unique_nonempty_values(paths)


def media_primary_path(media):
    paths = media_path_values(media)
    return paths[0] if paths else ""


def media_item_size_bytes(item):
    if not isinstance(item, dict):
        return 0
    for key in ("size_bytes", "sizeBytes", "size", "file_size", "fileSize"):
        value = item.get(key)
        if value is None:
            continue
        try:
            return int(value)
        except (TypeError, ValueError):
            continue
    return 0


class TelegramBot:
    def __init__(self, config, telegram, store, service, internal_api=None):
        self.config = config
        self.telegram = telegram
        self.store = store
        self.service = service
        self._recovery_thread = None
        self._task_locks = {}
        self._task_locks_guard = threading.Lock()
        self._task_message_update_times = {}
        self._task_message_update_guard = threading.Lock()
        self._subtitle_backfill_lock = threading.Lock()
        self.internal_api = internal_api

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
            self.telegram.send_message(chat_id, START_TEXT, reply_markup=home_reply_markup())
            return

        command, argument = split_command(text)
        is_command = command.startswith("/")
        if command == "/cancel":
            self._handle_cancel_command(chat_id, user_id)
            return
        if is_command and command != "/migrate":
            self.store.clear_pending_action(user_id, chat_id)
        if not is_command:
            pending = self.store.load_pending_action(user_id, chat_id)
            if pending is not None:
                self._handle_pending_message(chat_id, user_id, text, pending)
                return
        if command == "/start":
            self.telegram.send_message(chat_id, START_TEXT, reply_markup=home_reply_markup())
            return
        if command == "/help":
            self.telegram.send_message(chat_id, HELP_TEXT, reply_markup=home_reply_markup())
            return
        if command == "/tasks":
            self._send_task_list(chat_id, user_id)
            return
        if command == "/status":
            with self._typing_action(chat_id):
                self._handle_status_command(chat_id, user_id, argument)
            return
        if command == "/diag":
            with self._typing_action(chat_id):
                self._handle_diag_command(chat_id, user_id, argument)
            return
        if command == "/migrate":
            with self._typing_action(chat_id):
                self._handle_migrate_command(chat_id, user_id, argument)
            return
        if command == "/subtitle_report":
            with self._typing_action(chat_id):
                self._handle_subtitle_report_command(chat_id)
            return
        if command == "/subtitle_find":
            with self._typing_action(chat_id):
                self._handle_subtitle_find_command(chat_id, argument)
            return
        if command == "/p115_cookie":
            self._handle_p115_cookie_command(chat_id)
            return
        if command == "/dedupe_refresh":
            self._handle_dedupe_refresh_command(chat_id)
            return
        if command == "/version":
            self.telegram.send_message(chat_id, format_version_info())
            return
        if text.startswith("/"):
            self.telegram.send_message(chat_id, "这个命令不再作为搜索入口。直接发送关键词、番号、磁链或 115 分享链接即可；/help 查看功能。")
            return

        subtitle_find_query = parse_subtitle_find_query(text)
        if subtitle_find_query is not None:
            with self._typing_action(chat_id):
                self._handle_subtitle_find_command(chat_id, subtitle_find_query)
            return

        direct_candidate = share115_candidate_from_text(text) or magnet_candidate_from_text(text)
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
                self._send_empty_search_page(user_id, chat_id, category, query, metadata=exception_search_metadata(exc))
                return
            self.telegram.send_message(chat_id, "搜索失败：%s" % exc)
            return
        except Exception as exc:
            self.telegram.send_message(chat_id, "搜索失败：%s" % exc)
            return
        if not candidates:
            self._send_empty_search_page(user_id, chat_id, category, query, metadata=search_result_metadata(candidates))
            return

        candidate_ids = []
        for candidate in candidates:
            candidate_id = self.store.save_candidate(user_id, chat_id, category, query, candidate)
            candidate_ids.append(candidate_id)
        session_id = self.store.save_search_session(
            user_id,
            chat_id,
            category,
            query,
            candidate_ids,
            metadata=search_result_metadata(candidates),
        )
        text, reply_markup = self._render_search_page(session_id, page=0)

        self.telegram.send_message(chat_id, text, reply_markup=reply_markup)

    def _send_empty_search_page(self, user_id, chat_id, category, query, metadata=None):
        session_id = self.store.save_search_session(user_id, chat_id, category, query, [], metadata=metadata)
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
        if action == "home":
            self._handle_home_callback(user_id, chat_id, message_id, callback_id, value)
            return
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
        if action == "bt4g_search":
            self._handle_bt4g_search_callback(user_id, chat_id, callback_id, value)
            return
        if action == "pansou_search":
            self._handle_pansou_search_callback(user_id, chat_id, callback_id, value)
            return
        if action == "llm_rerank":
            self._handle_llm_rerank_callback(user_id, chat_id, message_id, callback_id, value)
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
        if action == "subtitle":
            self._handle_subtitle_callback(user_id, chat_id, message_id, callback_id, value)
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
        if action == "p115_cookie_start":
            self._handle_p115_cookie_start_callback(user_id, chat_id, message_id, callback_id)
            return
        if action == "p115_cookie_check":
            self._handle_p115_cookie_check_callback(user_id, chat_id, message_id, callback_id, value)
            return
        if action == "p115_cookie_cancel":
            self._handle_p115_cookie_cancel_callback(user_id, chat_id, message_id, callback_id, value)
            return
        if action == "subtitle_backfill_confirm":
            self._handle_subtitle_backfill_confirm_callback(chat_id, message_id, callback_id, value, retry_attempted=False)
            return
        if action == "subtitle_backfill_retry":
            self._handle_subtitle_backfill_confirm_callback(chat_id, message_id, callback_id, value, retry_attempted=True)
            return
        if action in ("subbulk", "subbulkr"):
            bucket, limit = value
            self._handle_subtitle_backfill_confirm_callback(
                chat_id,
                message_id,
                callback_id,
                limit,
                retry_attempted=action == "subbulkr",
                status_filter=bucket,
                report_bucket=bucket,
            )
            return
        if action == "subtitle_backfill_cancel":
            self._handle_subtitle_backfill_cancel_callback(chat_id, message_id, callback_id)
            return
        if action == "subtitle_report":
            bucket, page = value
            self._handle_subtitle_report_callback(chat_id, message_id, callback_id, bucket, page)
            return
        if action == "subfind_prompt":
            self._handle_subtitle_find_prompt_callback(chat_id, message_id, callback_id)
            return
        if action in ("sub1", "sub1r"):
            bucket, page, media_id = value
            self._handle_subtitle_backfill_one_callback(
                chat_id,
                message_id,
                callback_id,
                media_id,
                bucket=bucket,
                page=page,
                retry_attempted=action == "sub1r",
            )
            return
        if action == "subrematch":
            bucket, page, media_id = value
            self._handle_subtitle_rematch_callback(user_id, chat_id, message_id, callback_id, media_id, bucket=bucket, page=page)
            return
        if action == "subpick":
            self._handle_subtitle_pick_callback(user_id, chat_id, message_id, callback_id, value)
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
        page_count = search_page_count(len(session["candidate_ids"]), page_size=self.config.search_page_size)
        safe_page = normalize_page(page, page_count)
        self.telegram.answer_callback_query(callback_id, "第 %s/%s 页" % (safe_page + 1, page_count))
        self._update_callback_message(
            chat_id,
            message_id,
            text,
            reply_markup=reply_markup,
            fallback_chat_id=session["chat_id"],
        )

    def _handle_home_callback(self, user_id, chat_id, message_id, callback_id, target):
        target = str(target or "").strip()
        if target == "menu":
            self.store.clear_pending_action(user_id, chat_id)
            self.telegram.answer_callback_query(callback_id, "功能菜单")
            self._update_callback_message(chat_id, message_id, START_TEXT, reply_markup=home_reply_markup(), fallback_chat_id=chat_id)
            return
        if target == "tasks":
            records, page, page_count, total = self._task_list_page(user_id, page=0)
            self.telegram.answer_callback_query(callback_id, "最近任务")
            if not records:
                self._update_callback_message(chat_id, message_id, "最近任务\n暂无任务", reply_markup=home_back_reply_markup(), fallback_chat_id=chat_id)
                return
            self._update_callback_message(
                chat_id,
                message_id,
                format_task_list_message(records, page=page, page_count=page_count, total=total, page_size=self.config.task_list_page_size),
                reply_markup=task_list_reply_markup(records, page=page, page_count=page_count, page_size=self.config.task_list_page_size),
                fallback_chat_id=chat_id,
            )
            return
        if target == "subtitles":
            self.telegram.answer_callback_query(callback_id, "字幕管理")
            try:
                report = self.service.subtitle_backfill_report_adult()
            except (RuntimeError, ValueError) as exc:
                self._update_callback_message(
                    chat_id,
                    message_id,
                    "字幕补齐报表生成失败：%s" % exc,
                    reply_markup=home_back_reply_markup(),
                    fallback_chat_id=chat_id,
                )
                return
            self._update_callback_message(
                chat_id,
                message_id,
                format_subtitle_backfill_report_message(report, bucket="pending", page=0),
                reply_markup=subtitle_backfill_report_reply_markup(
                    report,
                    bucket="pending",
                    page=0,
                    batch_limit=self.config.subtitle_backfill_default_limit,
                ),
                fallback_chat_id=chat_id,
            )
            return
        if target == "migrate":
            self.store.save_pending_action(user_id, chat_id, PENDING_ACTION_MIGRATE_QUERY)
            self.telegram.answer_callback_query(callback_id, "请输入迁移关键词")
            self._update_callback_message(
                chat_id,
                message_id,
                MIGRATE_PROMPT_TEXT,
                reply_markup=home_back_reply_markup(),
                fallback_chat_id=chat_id,
            )
            return
        if target == "p115":
            self.telegram.answer_callback_query(callback_id, "115 Cookie")
            self._update_callback_message(
                chat_id,
                message_id,
                format_p115_cookie_status_message(self.store.get_p115_cookie()),
                reply_markup=p115_cookie_status_reply_markup(),
                fallback_chat_id=chat_id,
            )
            return
        self.telegram.answer_callback_query(callback_id, "不支持的菜单操作")

    def _handle_llm_rerank_callback(self, user_id, chat_id, message_id, callback_id, session_id):
        try:
            session = self.store.load_search_session(session_id)
        except RuntimeError:
            self.telegram.answer_callback_query(callback_id, "搜索结果不存在")
            return
        if session["user_id"] != user_id:
            self.telegram.answer_callback_query(callback_id, "无权查看此搜索")
            return
        if not self.config.llm_search_rerank_enabled:
            self.telegram.answer_callback_query(callback_id, "LLM优选未启用")
            return

        candidate_ids = [int(value) for value in session["candidate_ids"]]
        if len(candidate_ids) <= 1:
            self.telegram.answer_callback_query(callback_id, "候选太少，无需优选")
            return

        candidates = []
        for candidate_id in candidate_ids:
            record = self.store.load_candidate(candidate_id)
            if record["user_id"] != user_id:
                self.telegram.answer_callback_query(callback_id, "无权操作此候选")
                return
            candidate = dict(record["candidate"])
            candidate["_candidate_id"] = int(candidate_id)
            candidates.append(candidate)

        self.telegram.answer_callback_query(callback_id, "正在 LLM 优选")
        started = time.monotonic()
        try:
            with self._typing_action(session["chat_id"]):
                reranked = self.service.rerank_search_candidates(session["query"], session["category"], candidates)
            new_candidate_ids = self._store_llm_reranked_candidates(candidate_ids, reranked)
            metadata = self._search_metadata_with_llm_rerank(
                session.get("metadata"),
                "success",
                time.monotonic() - started,
                result_count=len(new_candidate_ids),
            )
            self.store.update_search_session(session_id, new_candidate_ids, metadata=metadata)
            text, reply_markup = self._render_search_page(session_id, page=0)
            self._update_callback_message(
                chat_id,
                message_id,
                text,
                reply_markup=reply_markup,
                fallback_chat_id=session["chat_id"],
            )
        except FutureTimeoutError as error:
            metadata = self._search_metadata_with_llm_rerank(
                session.get("metadata"),
                "timeout",
                time.monotonic() - started,
                error=error,
            )
            self.store.update_search_session(session_id, candidate_ids, metadata=metadata)
            text, reply_markup = self._render_search_page(session_id, page=0)
            self._update_callback_message(chat_id, message_id, text, reply_markup=reply_markup, fallback_chat_id=session["chat_id"])
        except LlmRerankBusy as error:
            print("llm rerank skipped: %s" % error, file=sys.stderr)
            metadata = self._search_metadata_with_llm_rerank(
                session.get("metadata"),
                "skipped",
                time.monotonic() - started,
                error=error,
            )
            self.store.update_search_session(session_id, candidate_ids, metadata=metadata)
            text, reply_markup = self._render_search_page(session_id, page=0)
            self._update_callback_message(chat_id, message_id, text, reply_markup=reply_markup, fallback_chat_id=session["chat_id"])
        except Exception as error:
            metadata = self._search_metadata_with_llm_rerank(
                session.get("metadata"),
                "failed",
                time.monotonic() - started,
                error=error,
            )
            self.store.update_search_session(session_id, candidate_ids, metadata=metadata)
            text, reply_markup = self._render_search_page(session_id, page=0)
            self._update_callback_message(chat_id, message_id, text, reply_markup=reply_markup, fallback_chat_id=session["chat_id"])

    def _store_llm_reranked_candidates(self, original_candidate_ids, reranked):
        expected_ids = {int(candidate_id) for candidate_id in original_candidate_ids}
        new_candidate_ids = []
        seen = set()
        for index, candidate in enumerate(reranked or [], start=1):
            candidate_id = int(candidate.get("_candidate_id") or 0)
            if candidate_id not in expected_ids:
                raise RuntimeError("LLM rerank returned unknown candidate id: %s" % candidate_id)
            if candidate_id in seen:
                raise RuntimeError("LLM rerank returned duplicate candidate id: %s" % candidate_id)
            updated = dict(candidate)
            updated.pop("_candidate_id", None)
            updated["rank"] = index
            self.store.update_candidate(candidate_id, updated)
            new_candidate_ids.append(candidate_id)
            seen.add(candidate_id)
        if seen != expected_ids:
            missing = sorted(expected_ids - seen)
            raise RuntimeError("LLM rerank missed candidate ids: %s" % missing)
        return new_candidate_ids

    def _search_metadata_with_llm_rerank(self, metadata, status, duration_seconds, result_count=0, error=None):
        updated = dict(metadata or {})
        existing_sources = list(updated.get("sources") or [])
        previous_llm_duration_ms = sum(
            int(source.get("duration_ms") or 0)
            for source in existing_sources
            if source.get("phase") == "llm_rerank" or source.get("source") == "LLM rerank"
        )
        sources = [
            source
            for source in existing_sources
            if source.get("phase") != "llm_rerank" and source.get("source") != "LLM rerank"
        ]
        duration_ms = int(round(float(duration_seconds or 0) * 1000))
        entry = {
            "source": "LLM rerank",
            "status": str(status or "success"),
            "result_count": int(result_count or 0),
            "duration_ms": duration_ms,
            "phase": "llm_rerank",
        }
        if error:
            entry["error"] = str(error)
        sources.append(entry)
        updated["sources"] = sources
        updated["source_count"] = len(sources)
        updated["success_count"] = sum(1 for source in sources if source.get("status") == "success")
        updated["failed_count"] = sum(1 for source in sources if source.get("status") == "failed")
        updated["timeout_count"] = sum(1 for source in sources if source.get("status") == "timeout")
        updated["skipped_count"] = sum(1 for source in sources if source.get("status") == "skipped")
        updated["selected_count"] = int(updated.get("selected_count") or result_count or 0)
        updated["total_ms"] = max(0, int(updated.get("total_ms") or 0) - previous_llm_duration_ms) + duration_ms
        settings = dict(updated.get("settings") or {})
        settings["llm_rerank_enabled"] = bool(self.config.llm_search_rerank_enabled)
        settings["llm_rerank_manual"] = True
        updated["settings"] = settings
        return updated

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
                empty_session_id = self.store.save_search_session(
                    user_id,
                    session["chat_id"],
                    "adult",
                    session["query"],
                    [],
                    metadata=exception_search_metadata(exc),
                )
                text, reply_markup = self._render_search_page(empty_session_id, page=0)
                self.telegram.send_message(session["chat_id"], text, reply_markup=reply_markup)
                return
            self.telegram.send_message(session["chat_id"], "成人源补查失败：%s" % exc)
            return
        except Exception as exc:
            self.telegram.send_message(session["chat_id"], "成人源补查失败：%s" % exc)
            return
        if not candidates:
            empty_session_id = self.store.save_search_session(
                user_id,
                session["chat_id"],
                "adult",
                session["query"],
                [],
                metadata=search_result_metadata(candidates),
            )
            text, reply_markup = self._render_search_page(empty_session_id, page=0)
            self.telegram.send_message(session["chat_id"], text, reply_markup=reply_markup)
            return

        candidate_ids = []
        for candidate in candidates:
            candidate_id = self.store.save_candidate(user_id, session["chat_id"], "adult", session["query"], candidate)
            candidate_ids.append(candidate_id)
        adult_session_id = self.store.save_search_session(
            user_id,
            session["chat_id"],
            "adult",
            session["query"],
            candidate_ids,
            metadata=search_result_metadata(candidates),
        )
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
                empty_session_id = self.store.save_search_session(
                    user_id,
                    session["chat_id"],
                    "anime",
                    session["query"],
                    [],
                    metadata=exception_search_metadata(exc),
                )
                text, reply_markup = self._render_search_page(empty_session_id, page=0)
                self.telegram.send_message(session["chat_id"], text, reply_markup=reply_markup)
                return
            self.telegram.send_message(session["chat_id"], "动漫源补查失败：%s" % exc)
            return
        except Exception as exc:
            self.telegram.send_message(session["chat_id"], "动漫源补查失败：%s" % exc)
            return
        if not candidates:
            empty_session_id = self.store.save_search_session(
                user_id,
                session["chat_id"],
                "anime",
                session["query"],
                [],
                metadata=search_result_metadata(candidates),
            )
            text, reply_markup = self._render_search_page(empty_session_id, page=0)
            self.telegram.send_message(session["chat_id"], text, reply_markup=reply_markup)
            return

        candidate_ids = []
        for candidate in candidates:
            candidate_id = self.store.save_candidate(user_id, session["chat_id"], "anime", session["query"], candidate)
            candidate_ids.append(candidate_id)
        anime_session_id = self.store.save_search_session(
            user_id,
            session["chat_id"],
            "anime",
            session["query"],
            candidate_ids,
            metadata=search_result_metadata(candidates),
        )
        text, reply_markup = self._render_search_page(anime_session_id, page=0)
        self.telegram.send_message(session["chat_id"], text, reply_markup=reply_markup)

    def _handle_bt4g_search_callback(self, user_id, chat_id, callback_id, session_id):
        try:
            session = self.store.load_search_session(session_id)
        except RuntimeError:
            self.telegram.answer_callback_query(callback_id, "搜索结果不存在")
            return
        if session["user_id"] != user_id:
            self.telegram.answer_callback_query(callback_id, "无权查看此搜索")
            return
        if session["category"] == "bt4g":
            self.telegram.answer_callback_query(callback_id, "当前已是 BT4G 结果")
            return

        self.telegram.answer_callback_query(callback_id, "正在补查 BT4G")
        try:
            with self._typing_action(session["chat_id"]):
                candidates = self.service.search_bt4g(session["query"], limit=self.config.search_limit)
        except RuntimeError as exc:
            if "no acceptable resource" in str(exc):
                empty_session_id = self.store.save_search_session(
                    user_id,
                    session["chat_id"],
                    "bt4g",
                    session["query"],
                    [],
                    metadata=exception_search_metadata(exc),
                )
                text, reply_markup = self._render_search_page(empty_session_id, page=0)
                self.telegram.send_message(session["chat_id"], text, reply_markup=reply_markup)
                return
            self.telegram.send_message(session["chat_id"], "BT4G 补查失败：%s" % exc)
            return
        except Exception as exc:
            self.telegram.send_message(session["chat_id"], "BT4G 补查失败：%s" % exc)
            return
        if not candidates:
            empty_session_id = self.store.save_search_session(
                user_id,
                session["chat_id"],
                "bt4g",
                session["query"],
                [],
                metadata=search_result_metadata(candidates),
            )
            text, reply_markup = self._render_search_page(empty_session_id, page=0)
            self.telegram.send_message(session["chat_id"], text, reply_markup=reply_markup)
            return

        candidate_ids = []
        for candidate in candidates:
            candidate_id = self.store.save_candidate(user_id, session["chat_id"], DEFAULT_SEARCH_CATEGORY, session["query"], candidate)
            candidate_ids.append(candidate_id)
        bt4g_session_id = self.store.save_search_session(
            user_id,
            session["chat_id"],
            "bt4g",
            session["query"],
            candidate_ids,
            metadata=search_result_metadata(candidates),
        )
        text, reply_markup = self._render_search_page(bt4g_session_id, page=0)
        self.telegram.send_message(session["chat_id"], text, reply_markup=reply_markup)

    def _handle_pansou_search_callback(self, user_id, chat_id, callback_id, session_id):
        try:
            session = self.store.load_search_session(session_id)
        except RuntimeError:
            self.telegram.answer_callback_query(callback_id, "搜索结果不存在")
            return
        if session["user_id"] != user_id:
            self.telegram.answer_callback_query(callback_id, "无权查看此搜索")
            return
        if not self.config.pansou_enabled:
            self.telegram.answer_callback_query(callback_id, "PanSou 未启用")
            return

        self.telegram.answer_callback_query(callback_id, "正在搜索网盘")
        try:
            with self._typing_action(chat_id or session["chat_id"]):
                candidates = self.service.search_pansou(session["query"], limit=self.config.search_limit)
        except Exception as exc:
            self.telegram.send_message(session["chat_id"], "网盘搜索失败：%s" % exc)
            return
        if not candidates:
            self.telegram.send_message(session["chat_id"], "网盘搜索未找到可用的 115 分享")
            return

        candidate_ids = []
        for candidate in candidates:
            candidate_id = self.store.save_candidate(user_id, session["chat_id"], DEFAULT_SEARCH_CATEGORY, session["query"], candidate)
            candidate_ids.append(candidate_id)
        pansou_session_id = self.store.save_search_session(
            user_id,
            session["chat_id"],
            "pansou",
            session["query"],
            candidate_ids,
            metadata={"profile": "pansou", "raw_count": len(candidates), "selected_count": len(candidates)},
        )
        text, reply_markup = self._render_search_page(pansou_session_id, page=0)
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
        page = candidate_ids.index(int(candidate_id)) // self.config.search_page_size
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
        callback_answered = False
        if answer:
            self.telegram.answer_callback_query(callback_id, "正在处理入库")
            callback_answered = True
        with self._typing_action(chat_id or record["chat_id"]):
            if not force:
                duplicate = self._find_duplicate_before_submit(category, record, candidate)
                if duplicate:
                    if answer and not callback_answered:
                        self.telegram.answer_callback_query(callback_id, "发现重复作品")
                        callback_answered = True
                    self._update_callback_message(
                        chat_id,
                        message_id,
                        format_duplicate_message(candidate, duplicate),
                        reply_markup=duplicate_reply_markup(duplicate, category, candidate_id, content_profile=content_profile),
                        fallback_chat_id=record["chat_id"],
                    )
                    return

            submission = self.store.claim_candidate_submission(candidate_id)
            if not submission.get("claimed"):
                return
            try:
                result = self.service.submit(category, candidate["download_uri"])
            except Exception as exc:
                self.store.finish_candidate_submission(candidate_id, "failed", error=str(exc))
                raise
        self._save_tasks_from_submit(record, candidate, result, category, content_profile=content_profile)
        self.store.finish_candidate_submission(candidate_id, "submitted", info_hash=first_submit_task_info_hash(result))
        if answer and not callback_answered:
            self.telegram.answer_callback_query(callback_id, submit_callback_text(result))
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
        self._sync_successful_submit_tasks(result)

    def _find_duplicate_before_submit(self, category, record, candidate):
        local = find_local_duplicate(self.store.list_all_tasks(), category, record, candidate)
        if local:
            return local
        indexed = find_index_duplicate(self.store, category, record, candidate)
        if indexed:
            return indexed
        return self.service.check_duplicate(category, record.get("query") or "", candidate)

    def _handle_status_callback(self, user_id, chat_id, message_id, callback_id, info_hash):
        lock = self._try_acquire_task_lock(info_hash)
        if lock is None:
            self.telegram.answer_callback_query(callback_id, "任务正在处理，请稍后刷新")
            return
        try:
            return self._handle_status_callback_unlocked(user_id, chat_id, message_id, callback_id, info_hash)
        finally:
            lock.release()

    def _handle_status_callback_unlocked(self, user_id, chat_id, message_id, callback_id, info_hash):
        record = self._load_owned_task(user_id, callback_id, info_hash)
        if record is None:
            return
        record = self._remember_status_message_id(record, message_id)
        if task_is_final(record["task"]) and not TASK_STATE.is_offline_success(record["task"]):
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
        lock = self._try_acquire_task_lock(info_hash)
        if lock is None:
            self.telegram.answer_callback_query(callback_id, "任务正在处理，请稍后刷新")
            return
        try:
            return self._handle_cancel_callback_unlocked(user_id, chat_id, message_id, callback_id, info_hash)
        finally:
            lock.release()

    def _handle_cancel_callback_unlocked(self, user_id, chat_id, message_id, callback_id, info_hash):
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
        lock = self._try_acquire_task_lock(info_hash)
        if lock is None:
            self.telegram.answer_callback_query(callback_id, "任务正在处理，请稍后刷新")
            return
        try:
            return self._handle_retry_msg_callback_unlocked(user_id, chat_id, message_id, callback_id, info_hash)
        finally:
            lock.release()

    def _handle_retry_msg_callback_unlocked(self, user_id, chat_id, message_id, callback_id, info_hash):
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

    def _handle_subtitle_callback(self, user_id, chat_id, message_id, callback_id, info_hash):
        lock = self._try_acquire_task_lock(info_hash)
        if lock is None:
            self.telegram.answer_callback_query(callback_id, "任务正在处理，请稍后刷新")
            return
        try:
            return self._handle_subtitle_callback_unlocked(user_id, chat_id, message_id, callback_id, info_hash)
        finally:
            lock.release()

    def _handle_subtitle_callback_unlocked(self, user_id, chat_id, message_id, callback_id, info_hash):
        record = self._load_owned_task(user_id, callback_id, info_hash)
        if record is None:
            return
        record = self._remember_status_message_id(record, message_id)
        task = dict(record["task"] or {})
        if not task.get("msg_media_id"):
            self.telegram.answer_callback_query(callback_id, "当前任务没有MSG媒体ID")
            return
        self.telegram.answer_callback_query(callback_id, "正在查找字幕")
        task["subtitle_match_status"] = "running"
        task["subtitle_match_error"] = None
        self.store.save_task(user_id, record["chat_id"], record["category"], record["title"], task)
        self._update_callback_message(
            chat_id,
            message_id,
            format_task_status_message(record["title"], task, category=record["category"]),
            reply_markup=callback_task_reply_markup(task),
            fallback_chat_id=record["chat_id"],
        )
        result = self.service.match_task_subtitles(record["category"], record["title"], task, force=False)
        task.update(result)
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
            self.store.save_pending_action(user_id, chat_id, PENDING_ACTION_MIGRATE_QUERY)
            self.telegram.send_message(chat_id, MIGRATE_PROMPT_TEXT)
            return
        self.store.clear_pending_action(user_id, chat_id)
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

    def _handle_pending_message(self, chat_id, user_id, text, pending):
        if text in PENDING_CANCEL_TEXTS:
            self.store.clear_pending_action(user_id, chat_id)
            self.telegram.send_message(chat_id, "已退出当前等待输入")
            return
        action = pending.get("action")
        self.store.clear_pending_action(user_id, chat_id)
        if action == PENDING_ACTION_MIGRATE_QUERY:
            with self._typing_action(chat_id):
                self._handle_migrate_command(chat_id, user_id, text)
            return
        self.telegram.send_message(chat_id, "已退出未知等待输入")

    def _handle_cancel_command(self, chat_id, user_id):
        if self.store.clear_pending_action(user_id, chat_id):
            self.telegram.send_message(chat_id, "已退出当前等待输入")
            return
        self.telegram.send_message(chat_id, "当前没有等待输入的操作")

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

    def _handle_p115_cookie_command(self, chat_id):
        cookie = self.store.get_p115_cookie() or self.config.p115_cookie
        self.telegram.send_message(
            chat_id,
            format_p115_cookie_status_message(cookie),
            reply_markup=p115_cookie_status_reply_markup(),
        )

    def _handle_p115_cookie_start_callback(self, user_id, chat_id, message_id, callback_id):
        self.telegram.answer_callback_query(callback_id, "正在生成二维码")
        try:
            session = P115QRCodeLoginClient().start()
            session_id = self.store.create_p115_qr_session(user_id, chat_id, session)
        except Exception as exc:
            self._update_callback_message(
                chat_id,
                message_id,
                "生成 115 扫码登录二维码失败：%s" % exc,
                reply_markup=p115_cookie_status_reply_markup(),
            )
            return

        self._update_callback_message(
            chat_id,
            message_id,
            format_p115_qr_message(session_id, session, status_label="等待扫码"),
            reply_markup=p115_cookie_qr_reply_markup(session_id),
        )

    def _handle_p115_cookie_check_callback(self, user_id, chat_id, message_id, callback_id, session_id):
        record = self._load_owned_p115_qr_session(user_id, callback_id, session_id)
        if record is None:
            return
        if record["status"] in {"confirmed", "cancelled", "expired"}:
            self.telegram.answer_callback_query(callback_id, qrcode_session_final_label(record["status"]))
            self._update_callback_message(
                chat_id,
                message_id,
                format_p115_qr_final_message(record["status"], self.store.get_p115_cookie()),
                reply_markup=p115_cookie_status_reply_markup(),
                fallback_chat_id=record["chat_id"],
            )
            return

        client = P115QRCodeLoginClient(app=(record["session"] or {}).get("app") or "web")
        try:
            status = client.status(record["session"])
        except Exception as exc:
            self.telegram.answer_callback_query(callback_id, "状态查询失败")
            self._update_callback_message(
                chat_id,
                message_id,
                "查询 115 扫码状态失败：%s" % exc,
                reply_markup=p115_cookie_qr_reply_markup(record["id"]),
                fallback_chat_id=record["chat_id"],
            )
            return

        status_code = int(status.get("status") or 0)
        if status_code == 2:
            try:
                cookie = client.login(record["session"], require_confirmed=False)
                self.store.set_p115_cookie(cookie)
                self.store.update_p115_qr_session(record["id"], "confirmed")
            except Exception as exc:
                self.telegram.answer_callback_query(callback_id, "保存失败")
                self._update_callback_message(
                    chat_id,
                    message_id,
                    "115 扫码已确认，但保存 Cookie 失败：%s" % exc,
                    reply_markup=p115_cookie_qr_reply_markup(record["id"]),
                    fallback_chat_id=record["chat_id"],
                )
                return
            self.telegram.answer_callback_query(callback_id, "已保存 115 Cookie")
            self._update_callback_message(
                chat_id,
                message_id,
                format_p115_cookie_saved_message(cookie),
                reply_markup=p115_cookie_status_reply_markup(),
                fallback_chat_id=record["chat_id"],
            )
            return

        if status_code == -1:
            self.store.update_p115_qr_session(record["id"], "expired")
            self.telegram.answer_callback_query(callback_id, "二维码已过期")
            self._update_callback_message(
                chat_id,
                message_id,
                format_p115_qr_final_message("expired", self.store.get_p115_cookie()),
                reply_markup=p115_cookie_status_reply_markup(),
                fallback_chat_id=record["chat_id"],
            )
            return

        if status_code == -2:
            self.store.update_p115_qr_session(record["id"], "cancelled")
            self.telegram.answer_callback_query(callback_id, "已取消")
            self._update_callback_message(
                chat_id,
                message_id,
                format_p115_qr_final_message("cancelled", self.store.get_p115_cookie()),
                reply_markup=p115_cookie_status_reply_markup(),
                fallback_chat_id=record["chat_id"],
            )
            return

        status_label = qrcode_status_label(status_code)
        self.telegram.answer_callback_query(callback_id, status_label)
        self._update_callback_message(
            chat_id,
            message_id,
            format_p115_qr_message(record["id"], record["session"], status_label=status_label),
            reply_markup=p115_cookie_qr_reply_markup(record["id"]),
            fallback_chat_id=record["chat_id"],
        )

    def _handle_p115_cookie_cancel_callback(self, user_id, chat_id, message_id, callback_id, session_id):
        record = self._load_owned_p115_qr_session(user_id, callback_id, session_id)
        if record is None:
            return
        self.store.update_p115_qr_session(record["id"], "cancelled")
        self.telegram.answer_callback_query(callback_id, "已取消")
        self._update_callback_message(
            chat_id,
            message_id,
            format_p115_qr_final_message("cancelled", self.store.get_p115_cookie()),
            reply_markup=p115_cookie_status_reply_markup(),
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

    def _handle_diag_command(self, chat_id, user_id, argument):
        target = str(argument or "").strip()
        if not target:
            self.telegram.send_message(chat_id, "请输入 info_hash 或 MSG媒体ID")
            return
        try:
            record = self.store.load_task(target)
        except RuntimeError:
            record = None
        if record is not None:
            if record["user_id"] != user_id:
                self.telegram.send_message(chat_id, "无权查看该任务")
                return
            self.telegram.send_message(chat_id, format_task_diagnostics_message(record))
            return

        if not self.config.msg_enabled:
            self.telegram.send_message(chat_id, "未找到任务记录，且MSG诊断未启用")
            return
        try:
            media = self.service.msg_media_diagnostics(target)
        except (RuntimeError, ValueError) as exc:
            self.telegram.send_message(chat_id, "诊断失败：%s" % exc)
            return
        self.telegram.send_message(chat_id, format_msg_media_diagnostics_message(target, media))

    def _send_task_list(self, chat_id, user_id):
        records, page, page_count, total = self._task_list_page(user_id, page=0)
        if not records:
            self.telegram.send_message(chat_id, "暂无任务")
            return
        self.telegram.send_message(
            chat_id,
            format_task_list_message(records, page=page, page_count=page_count, total=total, page_size=self.config.task_list_page_size),
            reply_markup=task_list_reply_markup(records, page=page, page_count=page_count, page_size=self.config.task_list_page_size),
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
            format_task_list_message(records, page=page, page_count=page_count, total=total, page_size=self.config.task_list_page_size),
            reply_markup=task_list_reply_markup(records, page=page, page_count=page_count, page_size=self.config.task_list_page_size),
        )

    def _task_list_page(self, user_id, page=0):
        records = prioritized_task_records(self.store.list_tasks(user_id, limit=self.config.task_list_fetch_limit))
        total = len(records)
        page_count = task_page_count(total, page_size=self.config.task_list_page_size)
        page = normalize_page(page, page_count)
        start = page * self.config.task_list_page_size
        return records[start : start + self.config.task_list_page_size], page, page_count, total

    def _handle_dedupe_refresh_command(self, chat_id):
        self.telegram.send_message(chat_id, DEDUPE_REFRESH_WARNING_TEXT, reply_markup=dedupe_refresh_confirm_reply_markup())

    def _handle_subtitle_report_command(self, chat_id):
        try:
            report = self.service.subtitle_backfill_report_adult()
        except (RuntimeError, ValueError) as exc:
            self.telegram.send_message(chat_id, "字幕补齐报表生成失败：%s" % exc)
            return
        self.telegram.send_message(
            chat_id,
            format_subtitle_backfill_report_message(report, bucket="pending", page=0),
            reply_markup=subtitle_backfill_report_reply_markup(
                report,
                bucket="pending",
                page=0,
                batch_limit=self.config.subtitle_backfill_default_limit,
            ),
        )

    def _handle_subtitle_report_callback(self, chat_id, message_id, callback_id, bucket, page):
        bucket = normalize_subtitle_report_bucket(bucket)
        page = max(0, int(page or 0))
        self.telegram.answer_callback_query(callback_id, "正在刷新字幕统计")
        try:
            report = self.service.subtitle_backfill_report_adult()
        except (RuntimeError, ValueError) as exc:
            self._update_callback_message(
                chat_id,
                message_id,
                "字幕补齐报表生成失败：%s" % exc,
                reply_markup={"inline_keyboard": []},
                fallback_chat_id=chat_id,
            )
            return
        self._update_callback_message(
            chat_id,
            message_id,
            format_subtitle_backfill_report_message(report, bucket=bucket, page=page),
            reply_markup=subtitle_backfill_report_reply_markup(
                report,
                bucket=bucket,
                page=page,
                batch_limit=self.config.subtitle_backfill_default_limit,
            ),
            fallback_chat_id=chat_id,
        )

    def _handle_subtitle_find_prompt_callback(self, chat_id, message_id, callback_id):
        self.telegram.answer_callback_query(callback_id, "请输入字幕查找关键词")
        self._update_callback_message(
            chat_id,
            message_id,
            SUBTITLE_FIND_PROMPT_TEXT,
            reply_markup=subtitle_find_prompt_reply_markup(),
            fallback_chat_id=chat_id,
        )

    def _handle_subtitle_find_command(self, chat_id, query):
        query = str(query or "").strip()
        if not query:
            self.telegram.send_message(chat_id, SUBTITLE_FIND_PROMPT_TEXT, reply_markup=subtitle_find_prompt_reply_markup())
            return
        try:
            result = self.service.subtitle_find_adult(query, limit=DEFAULT_SUBTITLE_FIND_LIMIT)
        except (RuntimeError, ValueError) as exc:
            self.telegram.send_message(chat_id, "字幕影片查找失败：%s" % exc)
            return
        self.telegram.send_message(
            chat_id,
            format_subtitle_find_message(result),
            reply_markup=subtitle_find_reply_markup(result),
        )

    def _handle_dedupe_refresh_confirm_callback(self, chat_id, message_id, callback_id):
        self.telegram.answer_callback_query(callback_id, "开始刷新已入库记录")
        self._update_callback_message(chat_id, message_id, "正在刷新已入库记录，请稍候...", reply_markup={"inline_keyboard": []})
        with self._typing_action(chat_id):
            self._run_dedupe_refresh(chat_id, message_id=message_id)

    def _handle_dedupe_refresh_cancel_callback(self, chat_id, message_id, callback_id):
        self.telegram.answer_callback_query(callback_id, "已取消刷新")
        self._update_callback_message(chat_id, message_id, "已取消刷新已入库记录。", reply_markup={"inline_keyboard": []})

    def _handle_subtitle_backfill_confirm_callback(self, chat_id, message_id, callback_id, limit, retry_attempted=False, status_filter=None, report_bucket="pending"):
        try:
            limit = normalize_subtitle_backfill_limit(limit)
            status_filter = normalize_subtitle_backfill_status_filter(status_filter)
        except ValueError as exc:
            self.telegram.answer_callback_query(callback_id, "数量无效")
            self._update_callback_message(chat_id, message_id, "字幕补齐数量无效：%s" % exc, reply_markup={"inline_keyboard": []})
            return
        if not self._subtitle_backfill_lock.acquire(blocking=False):
            self.telegram.answer_callback_query(callback_id, "字幕补齐任务正在运行")
            return
        action_label = "重试" if retry_attempted else "补齐"
        self.telegram.answer_callback_query(callback_id, "开始%s%s字幕" % (action_label, subtitle_backfill_status_filter_label(status_filter)))
        self._update_callback_message(
            chat_id,
            message_id,
            format_subtitle_backfill_message(new_subtitle_backfill_result(limit, retry_attempted=retry_attempted, status_filter=status_filter)),
            reply_markup={"inline_keyboard": []},
        )
        thread = threading.Thread(
            target=self._run_subtitle_backfill_thread,
            args=(chat_id, message_id, limit, retry_attempted, status_filter, report_bucket),
            daemon=True,
        )
        thread.start()

    def _handle_subtitle_backfill_one_callback(self, chat_id, message_id, callback_id, media_id, bucket="pending", page=0, retry_attempted=False):
        bucket = normalize_subtitle_report_bucket(bucket)
        page = max(0, int(page or 0))
        if not str(media_id or "").strip():
            self.telegram.answer_callback_query(callback_id, "media_id 无效")
            return
        if not self._subtitle_backfill_lock.acquire(blocking=False):
            self.telegram.answer_callback_query(callback_id, "字幕补齐任务正在运行")
            return
        self.telegram.answer_callback_query(callback_id, "开始补齐单个字幕" if not retry_attempted else "开始重试单个字幕")
        self._update_callback_message(
            chat_id,
            message_id,
            "正在补齐单个字幕：%s" % media_id,
            reply_markup={"inline_keyboard": []},
        )
        thread = threading.Thread(
            target=self._run_subtitle_backfill_one_thread,
            args=(chat_id, message_id, media_id, retry_attempted, bucket, page),
            daemon=True,
        )
        thread.start()

    def _handle_subtitle_rematch_callback(self, user_id, chat_id, message_id, callback_id, media_id, bucket="cached", page=0):
        bucket = normalize_subtitle_report_bucket(bucket)
        page = max(0, int(page or 0))
        media_id = str(media_id or "").strip()
        if not media_id:
            self.telegram.answer_callback_query(callback_id, "media_id 无效")
            return
        self.telegram.answer_callback_query(callback_id, "正在重新匹配字幕")
        self._update_callback_message(
            chat_id,
            message_id,
            "正在重新匹配字幕：%s" % media_id,
            reply_markup={"inline_keyboard": []},
        )
        thread = threading.Thread(
            target=self._run_subtitle_rematch_thread,
            args=(user_id, chat_id, message_id, media_id, bucket, page),
            daemon=True,
        )
        thread.start()

    def _handle_subtitle_pick_callback(self, user_id, chat_id, message_id, callback_id, candidate_id):
        try:
            record = self.store.load_candidate(candidate_id)
        except RuntimeError:
            self.telegram.answer_callback_query(callback_id, "字幕候选不存在")
            return
        if record["user_id"] != user_id or record["category"] != "subtitle_rematch":
            self.telegram.answer_callback_query(callback_id, "无权操作此字幕候选")
            return
        candidate = dict(record["candidate"])
        bucket = normalize_subtitle_report_bucket(candidate.get("report_bucket") or "cached")
        page = max(0, int(candidate.get("report_page") or 0))
        self.telegram.answer_callback_query(callback_id, "正在应用字幕")
        try:
            with self._typing_action(record["chat_id"]):
                result = self.service.apply_subtitle_candidate(candidate)
        except Exception as exc:
            self._update_callback_message(
                chat_id,
                message_id,
                "字幕应用失败\nmedia_id：%s\n错误：%s" % (candidate.get("media_id") or "-", exc),
                reply_markup=subtitle_rematch_done_reply_markup(bucket, page),
                fallback_chat_id=record["chat_id"],
            )
            return
        self._update_callback_message(
            chat_id,
            message_id,
            format_subtitle_apply_result_message(result),
            reply_markup=subtitle_rematch_done_reply_markup(bucket, page),
            fallback_chat_id=record["chat_id"],
        )

    def _handle_subtitle_backfill_cancel_callback(self, chat_id, message_id, callback_id):
        self.telegram.answer_callback_query(callback_id, "已取消字幕补齐")
        self._update_callback_message(chat_id, message_id, "已取消批量补齐成人库字幕。", reply_markup={"inline_keyboard": []})

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

    def _run_subtitle_backfill_thread(self, chat_id, message_id, limit, retry_attempted=False, status_filter=None, report_bucket="pending"):
        try:
            last_emit_at = 0.0
            report_bucket = normalize_subtitle_report_bucket(report_bucket or status_filter or "pending")

            def progress(result, force=False):
                nonlocal last_emit_at
                now = time.monotonic()
                interval = max(0.0, float(self.config.task_message_edit_min_interval_seconds or 0))
                if not force and interval > 0 and now - last_emit_at < interval:
                    return
                last_emit_at = now
                self._update_callback_message(
                    chat_id,
                    message_id,
                    format_subtitle_backfill_message(result),
                    reply_markup={"inline_keyboard": []},
                    fallback_chat_id=chat_id,
                )

            result = self.service.subtitle_backfill_adult(
                limit=limit,
                progress_callback=progress,
                retry_attempted=retry_attempted,
                status_filter=status_filter,
            )
            try:
                report = self.service.subtitle_backfill_report_adult()
            except Exception as exc:
                self._update_callback_message(
                    chat_id,
                    message_id,
                    format_subtitle_backfill_message(result) + "\n\n报表刷新失败：%s" % exc,
                    reply_markup={"inline_keyboard": []},
                    fallback_chat_id=chat_id,
                )
                return
            self._update_callback_message(
                chat_id,
                message_id,
                format_subtitle_backfill_report_message(report, bucket=report_bucket, page=0) + "\n\n" + format_subtitle_backfill_message(result),
                reply_markup=subtitle_backfill_report_reply_markup(
                    report,
                    bucket=report_bucket,
                    page=0,
                    batch_limit=self.config.subtitle_backfill_default_limit,
                ),
                fallback_chat_id=chat_id,
            )
        except Exception as exc:
            self._update_callback_message(
                chat_id,
                message_id,
                "成人库字幕补齐：失败\n错误：%s" % exc,
                reply_markup={"inline_keyboard": []},
                fallback_chat_id=chat_id,
            )
        finally:
            self._subtitle_backfill_lock.release()

    def _run_subtitle_backfill_one_thread(self, chat_id, message_id, media_id, retry_attempted, bucket, page):
        try:
            result = self.service.subtitle_backfill_one_adult(media_id, retry_attempted=retry_attempted)
            report = self.service.subtitle_backfill_report_adult()
            self._update_callback_message(
                chat_id,
                message_id,
                format_subtitle_backfill_report_message(report, bucket=bucket, page=page) + "\n\n" + format_subtitle_backfill_message(result),
                reply_markup=subtitle_backfill_report_reply_markup(
                    report,
                    bucket=bucket,
                    page=page,
                    batch_limit=self.config.subtitle_backfill_default_limit,
                ),
                fallback_chat_id=chat_id,
            )
        except Exception as exc:
            self._update_callback_message(
                chat_id,
                message_id,
                "单个字幕补齐：失败\nmedia_id：%s\n错误：%s" % (media_id, exc),
                reply_markup={"inline_keyboard": []},
                fallback_chat_id=chat_id,
            )
        finally:
            self._subtitle_backfill_lock.release()

    def _run_subtitle_rematch_thread(self, user_id, chat_id, message_id, media_id, bucket, page):
        try:
            with self._typing_action(chat_id):
                result = self.service.subtitle_rematch_candidates_adult(media_id, limit=DEFAULT_SUBTITLE_REMATCH_LIMIT)
                candidates = list(result.get("candidates") or [])
                llm_status = "disabled"
                llm_error = ""
                if self.config.llm_search_rerank_enabled and len(candidates) > 1:
                    try:
                        previewed = self.service.preview_subtitle_candidates(
                            candidates,
                            limit=DEFAULT_SUBTITLE_LLM_PREVIEW_LIMIT,
                            max_chars=DEFAULT_SUBTITLE_LLM_PREVIEW_CHARS,
                        )
                        candidates = self.service.rank_subtitle_candidates(result.get("media") or {}, result.get("query") or "", previewed)
                        llm_status = "success"
                    except FutureTimeoutError as exc:
                        llm_status = "timeout"
                        llm_error = str(exc)
                    except LlmRerankBusy as exc:
                        llm_status = "busy"
                        llm_error = str(exc)
                    except Exception as exc:
                        llm_status = "failed"
                        llm_error = str(exc)

            candidate_ids = []
            for index, candidate in enumerate(candidates, start=1):
                record = dict(candidate)
                record["rank"] = index
                record["media_id"] = result.get("media_id") or media_id
                record["title"] = result.get("title") or media_id
                record["code"] = result.get("code") or record.get("code") or ""
                record["category"] = "adult"
                record["report_bucket"] = bucket
                record["report_page"] = page
                candidate_ids.append(
                    self.store.save_candidate(
                        user_id,
                        chat_id,
                        "subtitle_rematch",
                        result.get("query") or record.get("query") or "",
                        record,
                    )
                )

            self._update_callback_message(
                chat_id,
                message_id,
                format_subtitle_rematch_candidates_message(result, candidates, llm_status=llm_status, llm_error=llm_error),
                reply_markup=subtitle_rematch_candidates_reply_markup(candidate_ids, bucket, page),
                fallback_chat_id=chat_id,
            )
        except Exception as exc:
            self._update_callback_message(
                chat_id,
                message_id,
                "字幕重新匹配失败\nmedia_id：%s\n错误：%s" % (media_id, exc),
                reply_markup=subtitle_rematch_done_reply_markup(bucket, page),
                fallback_chat_id=chat_id,
            )

    def _render_search_page(self, session_id, page):
        session = self.store.load_search_session(session_id)
        candidate_ids = session["candidate_ids"]
        page_count = search_page_count(len(candidate_ids), page_size=self.config.search_page_size)
        page = normalize_page(page, page_count)
        start = page * self.config.search_page_size
        page_candidate_ids = candidate_ids[start : start + self.config.search_page_size]
        candidates = []
        for candidate_id in page_candidate_ids:
            record = self.store.load_candidate(candidate_id)
            candidates.append((candidate_id, record["candidate"]))
        is_adult_session = session["category"] == "adult"
        is_anime_session = session["category"] == "anime"
        is_bt4g_session = session["category"] == "bt4g"
        is_pansou_session = session["category"] == "pansou"
        title = "搜索结果"
        if not candidate_ids:
            title = "未找到可用资源"
        elif is_adult_session:
            title = "成人源搜索结果"
        elif is_anime_session:
            title = "动漫源搜索结果"
        elif is_bt4g_session:
            title = "BT4G搜索结果"
        elif is_pansou_session:
            title = "网盘搜索结果"
        return format_search_page_message(
            session["query"],
            candidates,
            page,
            page_count,
            len(candidate_ids),
            title=title,
            metadata=session.get("metadata"),
        ), search_page_reply_markup(
            session_id,
            candidates,
            page,
            page_count,
            allow_adult_retry=not is_adult_session
            and not is_anime_session
            and not is_bt4g_session
            and not is_pansou_session
            and not is_strong_adult_code_query(session["query"]),
            allow_anime_retry=not is_adult_session
            and not is_anime_session
            and not is_bt4g_session
            and not is_pansou_session
            and not is_strong_adult_code_query(session["query"])
            and not should_search_anime(DEFAULT_SEARCH_CATEGORY, session["query"]),
            allow_bt4g_retry=self._should_allow_bt4g_retry(session) and not is_pansou_session,
            allow_llm_rerank=self.config.llm_search_rerank_enabled and bool(candidate_ids) and not is_pansou_session,
            allow_pansou_search=self.config.pansou_enabled and not is_pansou_session,
        )

    def _should_allow_bt4g_retry(self, session):
        if (session or {}).get("category") != DEFAULT_SEARCH_CATEGORY:
            return False
        query = (session or {}).get("query")
        if is_strong_adult_code_query(query) or should_search_anime(DEFAULT_SEARCH_CATEGORY, query):
            return False
        if not search_metadata_has_source((session or {}).get("metadata"), "BT4G"):
            return False
        for candidate_id in (session or {}).get("candidate_ids") or []:
            try:
                record = self.store.load_candidate(candidate_id)
            except RuntimeError:
                continue
            if candidate_matches_indexer_label(record.get("candidate"), "BT4G"):
                return False
        return True

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

    def _sync_successful_submit_tasks(self, result):
        for task in result.get("tasks") or []:
            info_hash = task.get("info_hash")
            if not info_hash:
                continue
            try:
                record = self.store.load_task(info_hash)
            except RuntimeError:
                continue
            current_task = record.get("task") or {}
            if not TASK_STATE.is_offline_success(current_task):
                continue
            if task_msg_synced(current_task) or not task_sync_is_running(current_task):
                continue
            lock = self._try_acquire_task_lock(info_hash)
            if lock is None:
                continue
            try:
                try:
                    record = self.store.load_task(info_hash)
                except RuntimeError:
                    continue
                current_task = self._task_with_known_status_message_id(record, record.get("task"))
                if not TASK_STATE.is_offline_success(current_task):
                    continue
                if task_msg_synced(current_task) or not task_sync_is_running(current_task):
                    continue
                synced_task = self._sync_completed_task_with_store_progress(record, current_task)
                synced_task = self._task_with_known_status_message_id(record, synced_task)
                self.store.save_task(record["user_id"], record["chat_id"], record["category"], record["title"], synced_task)
                self._update_recovered_115_task_message(record, synced_task, force=True)
            finally:
                lock.release()

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

    def _load_owned_p115_qr_session(self, user_id, callback_id, session_id):
        try:
            record = self.store.load_p115_qr_session(session_id)
        except RuntimeError:
            self.telegram.answer_callback_query(callback_id, "扫码会话不存在")
            return None
        if record["user_id"] != user_id:
            self.telegram.answer_callback_query(callback_id, "无权操作此扫码会话")
            return None
        return record

    def _sync_completed_task(self, record, task, progress_callback=None):
        return self.service.sync_completed_task(record["category"], record["title"], task, progress_callback=progress_callback)

    def _retry_msg_sync_task(self, record, task, progress_callback=None):
        retry_task = dict(task or {})
        retry_task["msg_sync_status"] = STATUS_RUNNING
        retry_task["msg_error"] = None
        self.store.save_task(record["user_id"], record["chat_id"], record["category"], record["title"], retry_task)
        return self._sync_completed_task(record, retry_task, progress_callback=progress_callback)

    def _callback_sync_progress_updater(self, record, chat_id, message_id):
        def update(task):
            task = self._task_with_known_status_message_id(record, task, message_id=message_id)
            self.store.save_task(record["user_id"], record["chat_id"], record["category"], record["title"], task)
            if not self._should_emit_task_message_update(record, task):
                return
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
        out["msg_sync_status"] = STATUS_RUNNING
        out["msg_error"] = None
        return out

    def _should_show_syncing_status(self, task):
        return TASK_STATE.should_show_syncing_status(task, self.config.msg_enabled)

    def _delete_callback_message(self, chat_id, message_id):
        if chat_id is None or message_id is None:
            return
        try:
            self.telegram.delete_message(chat_id, message_id)
        except RuntimeError:
            return

    def _task_lock_key(self, info_hash):
        return str(info_hash or "").strip().lower()

    def _try_acquire_task_lock(self, info_hash):
        key = self._task_lock_key(info_hash)
        if not key:
            return None
        with self._task_locks_guard:
            lock = self._task_locks.get(key)
            if lock is None:
                lock = threading.Lock()
                self._task_locks[key] = lock
        if not lock.acquire(blocking=False):
            return None
        return lock

    def _should_emit_task_message_update(self, record, task=None, force=False):
        if force:
            return True
        interval = max(0.0, float(self.config.task_message_edit_min_interval_seconds or 0))
        if interval <= 0:
            return True
        message_id = task_telegram_status_message_id(task) or task_telegram_status_message_id((record or {}).get("task"))
        if not message_id:
            return True
        info_hash = self._task_lock_key((task or {}).get("info_hash") or (record or {}).get("info_hash"))
        key = "%s:%s:%s" % ((record or {}).get("chat_id"), message_id, info_hash)
        now = time.monotonic()
        with self._task_message_update_guard:
            last = self._task_message_update_times.get(key)
            if last is not None and now - last < interval:
                return False
            self._task_message_update_times[key] = now
        return True

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
        records = self.store.list_msg_sync_running_tasks()
        return self._run_task_records_parallel(records, self._recover_running_msg_sync_record)

    def recover_active_115_tasks_once(self, now=None):
        now = int(time.time() if now is None else now)
        count = 0
        records_by_category = defaultdict(list)
        for record in self.store.list_active_115_tasks():
            if active_115_task_timed_out(record, now):
                count += self._auto_cancel_timed_out_115_task(record, now)
            elif active_115_task_poll_due(record, now, normal_interval_seconds=self.config.sync_recovery_interval_seconds):
                records_by_category[record["category"]].append(record)

        active_work = []
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
                active_work.append((record, task))
        return count + self._run_task_records_parallel(active_work, self._recover_active_115_task_record)

    def _run_task_records_parallel(self, items, handler):
        items = list(items or [])
        if not items:
            return 0
        workers = max(1, int(self.config.task_workers or 1))
        if workers == 1 or len(items) == 1:
            count = 0
            for item in items:
                count += handler(item)
            return count
        count = 0
        with ThreadPoolExecutor(max_workers=min(workers, len(items)), thread_name_prefix="task-recovery") as executor:
            futures = [executor.submit(handler, item) for item in items]
            for future in futures:
                count += future.result()
        return count

    def _recover_running_msg_sync_record(self, record):
        lock = self._try_acquire_task_lock(record["info_hash"])
        if lock is None:
            return 0
        try:
            try:
                task = self._sync_completed_task(record, record["task"])
                task = self._task_with_known_status_message_id(record, task)
                self.store.save_task(record["user_id"], record["chat_id"], record["category"], record["title"], task)
                self._update_recovered_115_task_message(record, task, force=True)
                return 1
            except Exception as exc:
                print("bot sync recovery failed for %s: %s" % (record["info_hash"], exc), flush=True)
                return 0
        finally:
            lock.release()

    def _recover_active_115_task_record(self, item):
        record, task = item
        lock = self._try_acquire_task_lock(record["info_hash"])
        if lock is None:
            return 0
        try:
            try:
                task = self._sync_completed_task_with_store_progress(record, task)
                task = self._task_with_known_status_message_id(record, task)
                self.store.save_task(record["user_id"], record["chat_id"], record["category"], record["title"], task)
                self._update_recovered_115_task_message(record, task, force=True)
                return 1
            except Exception as exc:
                print("bot active task recovery failed for %s: %s" % (record["info_hash"], exc), flush=True)
                return 0
        finally:
            lock.release()

    def _update_recovered_115_task_message(self, record, task, final_fallback=True, force=False):
        if not force and not self._should_emit_task_message_update(record, task):
            return
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
        lock = self._try_acquire_task_lock(record["info_hash"])
        if lock is None:
            return 0
        try:
            try:
                result = self.service.cancel_task(record["category"], record["info_hash"])
            except Exception as exc:
                print("bot 115 timeout cancel failed for %s: %s" % (record["info_hash"], exc), flush=True)
                return 0
            task = dict(result.get("task") or record["task"] or {})
            task.setdefault("info_hash", record["info_hash"])
            if result.get("cancelled"):
                mark_task_auto_cancelled(task, now)
            if TASK_STATE.is_offline_success(task):
                task = self._sync_completed_task_with_store_progress(record, task)
            self.store.save_task(record["user_id"], record["chat_id"], record["category"], record["title"], task)
            if result.get("cancelled"):
                message = format_auto_cancel_result_message(record["title"], result, task, category=record["category"])
            else:
                message = format_cancel_result_message(record["title"], result, category=record["category"])
            self.telegram.send_message(record["chat_id"], message, reply_markup=task_reply_markup(task))
            return 1
        finally:
            lock.release()

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
            try:
                now = int(time.time())
                self.recover_active_115_tasks_once(now=now)
                if now - last_msg_recovery_at >= interval:
                    self.recover_running_msg_sync_tasks_once()
                    last_msg_recovery_at = now
            except sqlite3.Error as exc:
                print("sync recovery sqlite access failed: %s" % exc, flush=True)
            time.sleep(loop_interval)

    def configure_bot_commands(self):
        try:
            self.telegram.set_my_commands(BOT_COMMANDS)
        except Exception as exc:
            print("telegram command setup failed: %s" % exc, flush=True)

    def run_forever(self):
        if self.internal_api is not None:
            self.internal_api.start()
        try:
            self.configure_bot_commands()
            self.start_sync_recovery_thread()
            offset = None
            while True:
                offset = self.poll_updates_once(offset)
        finally:
            if self.internal_api is not None:
                self.internal_api.stop()

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
    parts = str(text or "").strip().split(None, 1)
    if not parts:
        return "", ""
    command = parts[0]
    argument = parts[1] if len(parts) > 1 else ""
    command = command.split("@", 1)[0]
    return command, argument.strip()


def parse_subtitle_find_query(text):
    value = str(text or "").strip()
    if not value:
        return None
    for prefix in ("重新匹配字幕", "字幕重配", "重配字幕", "字幕", "重配"):
        if value == prefix:
            return ""
        if value.startswith(prefix + " ") or value.startswith(prefix + "\u3000"):
            return value[len(prefix) :].strip()
        for separator in ("：", ":"):
            marker = prefix + separator
            if value.startswith(marker):
                return value[len(marker) :].strip()
    return None


def parse_bool(value, default=False):
    if value is None:
        return default
    return str(value).strip().lower() in ("1", "true", "yes", "on")


def ensure_sqlite_column(conn, table, column, definition):
    try:
        conn.execute("alter table %s add column %s %s" % (table, column, definition))
    except sqlite3.OperationalError as exc:
        if "duplicate column name" not in str(exc).lower():
            raise


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


def indexer_matches_label(indexer, label):
    return candidate_matches_indexer_label(indexer, label)


def candidate_matches_indexer_label(candidate, label):
    label = str(label or "").strip().casefold()
    if not label:
        return False
    values = [
        (candidate or {}).get("name"),
        (candidate or {}).get("indexer"),
        (candidate or {}).get("indexerName"),
        (candidate or {}).get("site"),
        (candidate or {}).get("tracker"),
        (candidate or {}).get("guid"),
        (candidate or {}).get("infoUrl"),
        (candidate or {}).get("details"),
        (candidate or {}).get("downloadUrl"),
    ]
    return any(indexer_label_text_matches(value, label) for value in values)


def search_metadata_has_source(metadata, label):
    label = str(label or "").strip().casefold()
    if not label:
        return False
    for source in (metadata or {}).get("sources") or []:
        if indexer_label_text_matches((source or {}).get("source"), label):
            return True
    return False


def indexer_label_text_matches(value, normalized_label):
    text = str(value or "").strip().casefold()
    if not text:
        return False
    if text == normalized_label:
        return True
    return re.search(r"(?<![a-z0-9])%s(?![a-z0-9])" % re.escape(normalized_label), text) is not None


def task_msg_synced(task):
    return TASK_STATE.msg_synced(task)


def task_sync_is_running(task):
    return TASK_STATE.sync_is_running(task)


def stage_is_complete(status):
    return TASK_STATE.stage_is_complete(status)


def prefixed_task_fields(task, prefix):
    return {key: value for key, value in (task or {}).items() if key.startswith(prefix)}


STALE_MSG_MEDIA_RESET_KEYS = (
    "msg_media_id",
    "msg_media_title",
    "msg_match_mode",
    "msg_match_path",
    "msg_ingest_job_id",
    "msg_ingest_idempotency_key",
    "msg_ingest_status",
    "msg_ingest_stage",
    "msg_ingest_message",
    "msg_ingest_error",
    "msg_ingest_scan_visited",
    "msg_ingest_scan_added",
    "msg_ingest_scan_updated",
    "msg_ingest_scan_removed",
    "msg_scan_status",
    "msg_scrape_status",
    "msg_scrape_mode",
    "msg_scrape_query",
    "msg_extra_cleanup_status",
    "msg_extra_cleanup_updated",
    "msg_extra_cleanup_media_count",
    "msg_extra_cleanup_hidden_count",
    "msg_extra_cleanup_reason",
    "msg_extra_cleanup_error",
    "msg_visibility_repair_status",
    "msg_visibility_repair_updated",
    "msg_visibility_repair_media_count",
    "msg_visibility_repair_reason",
    "msg_visibility_repair_error",
    "subtitle_match_status",
    "subtitle_match_source",
    "subtitle_match_reason",
    "subtitle_match_error",
    "subtitle_match_path",
)


def stale_msg_media_reset_updates(task, media_id, reason="not_found"):
    updates = {key: None for key in STALE_MSG_MEDIA_RESET_KEYS}
    updates["msg_sync_status"] = "running"
    updates["msg_error"] = None
    updates["msg_stale_media_id"] = media_id
    updates["msg_stale_media_reason"] = reason
    updates["msg_stale_media_at"] = int(time.time())
    return updates


def adult_format_changed_media_path(result):
    result = result or {}
    old_path = normalize_openlist_path(result.get("openlist_adult_video_old_path"))
    new_path = normalize_openlist_path(result.get("openlist_adult_video_new_path"))
    return bool(old_path and new_path and old_path.casefold() != new_path.casefold())


def mark_current_sync_stage_failed(task, error):
    TASK_STATE.mark_running_sync_stage_failed(task, error)


def media_search_queries(title, task):
    values = []
    for value in ((task or {}).get("openlist_adult_code"), title, (task or {}).get("name"), (task or {}).get("file_name")):
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


def msg_scrape_queries(title, task, media=None, preferred_queries=None):
    values = []
    for value in ((task or {}).get("openlist_adult_code"), title, (task or {}).get("name"), (task or {}).get("file_name")):
        if value:
            values.append(str(value))
    if isinstance(media, dict):
        for key in ("title", "name", "original_name", "file_name", "filename", "path", "file_path", "source_path", "relative_path"):
            value = media.get(key)
            if value:
                values.append(str(value))

    candidates = [str(value).strip() for value in preferred_queries or [] if str(value or "").strip()]
    preferred_title = preferred_scrape_title(title)
    if preferred_title:
        candidates.append(preferred_title)
    for value in values:
        candidates.extend(extract_codes(value))
        candidates.extend(extract_title_fragments(value, allow_english=True))

    out = []
    seen = set()
    for candidate in candidates:
        text = str(candidate or "").strip()
        if not is_useful_scrape_query(text):
            continue
        key = normalize_fragment(text)
        if key and key not in seen:
            seen.add(key)
            out.append(text)
    return out


def preferred_scrape_title(value):
    text = str(value or "").strip()
    if not is_useful_scrape_query(text) or extract_codes(text):
        return ""
    normalized = normalize_fragment(text)
    if "[" in text or "]" in text:
        return ""
    if any(marker in normalized for marker in SCRAPE_RELEASE_MARKERS):
        return ""
    return text


def duplicate_media_queries(category, query, candidate):
    values = []
    for value in (query, (candidate or {}).get("title"), (candidate or {}).get("name"), (candidate or {}).get("file_name")):
        if value:
            values.append(str(value))

    if category == "adult":
        codes = sorted(extract_codes(" ".join(values + [str((candidate or {}).get("download_uri") or "")])))
        return unique_nonempty_values(codes)

    # A broad search such as "Men in Black" also returns sequels and spin-offs.
    # Prefer the candidate's title plus release year when it is available so
    # the MSG lookup can distinguish those works before considering the query.
    queries = []
    candidate_specific_queries = []
    for value in values[1:]:
        specific = duplicate_title_year_query(value)
        if specific:
            candidate_specific_queries.append(specific)
    queries.extend(candidate_specific_queries)
    for value in values:
        cleaned = duplicate_title_query(value)
        if cleaned:
            queries.append(cleaned)
    return unique_nonempty_values(queries)


def duplicate_title_query(value):
    text = str(value or "").strip()
    if not text:
        return ""
    normalized = normalize_fragment(text)
    if len(normalized) < 4:
        return ""
    return text


def duplicate_title_year_query(value):
    text = str(value or "").strip()
    if not text:
        return ""
    match = re.search(r"(?<!\d)(?:19|20)\d{2}(?!\d)", text)
    if not match:
        return ""
    prefix = text[: match.start()].strip(" ._-[]()")
    if not prefix:
        return ""
    return "%s %s" % (prefix, match.group(0))


def unique_nonempty_values(values):
    seen = set()
    out = []
    for value in values or []:
        text = str(value or "").strip()
        key = text.lower()
        if text and key not in seen:
            seen.add(key)
            out.append(text)
    return out


def extract_title_fragments(value, allow_english=False):
    text = str(value or "")
    fragments = []
    for match in re.finditer(r"[\[\(（【](.*?)[\]\)）】]", text):
        fragment = match.group(1).strip()
        if is_useful_title_fragment(fragment, allow_english=allow_english):
            fragments.append(fragment)
    return fragments


def is_useful_title_fragment(value, allow_english=False):
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
    if allow_english and re.search(r"[A-Za-z]", value):
        if normalized in SCRAPE_QUERY_NOISE:
            return False
        words = re.findall(r"[A-Za-z][A-Za-z0-9']*", value)
        return len(words) >= 2 and len(normalized) >= 6
    return False


def is_useful_scrape_query(value):
    normalized = normalize_fragment(value)
    if not normalized or normalized in SCRAPE_QUERY_NOISE:
        return False
    if normalized in {normalize_fragment(token) for token in EXTRA_SCAN_NAME_TOKENS}:
        return False
    if re.search(r"[\u4e00-\u9fff]", value):
        return len(normalized) >= 2
    if extract_codes(value):
        return True
    words = re.findall(r"[A-Za-z][A-Za-z0-9']*", str(value or ""))
    return len(words) >= 2 and len(normalized) >= 6


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


def normalize_subtitle_backfill_limit(value):
    try:
        limit = int(str(value or "").strip())
    except (TypeError, ValueError):
        raise ValueError("请输入 1-%s 的整数" % MAX_SUBTITLE_BACKFILL_LIMIT)
    if limit < 1:
        raise ValueError("数量必须大于 0")
    if limit > MAX_SUBTITLE_BACKFILL_LIMIT:
        raise ValueError("单次最多 %s 个" % MAX_SUBTITLE_BACKFILL_LIMIT)
    return limit


def new_subtitle_backfill_result(limit, retry_attempted=False, status_filter=None):
    return {
        "status": "running",
        "limit": normalize_subtitle_backfill_limit(limit),
        "retry_attempted": bool(retry_attempted),
        "status_filter": normalize_subtitle_backfill_status_filter(status_filter),
        "scanned": 0,
        "attempted": 0,
        "with_subtitles": 0,
        "pending": 0,
        "matched": 0,
        "cached": 0,
        "declared": 0,
        "previous": 0,
        "not_found": 0,
        "failed": 0,
        "skipped": 0,
        "current": {},
        "recent": [],
    }


def subtitle_matcher_has_cached_tracks(matcher, media_id):
    cache = getattr(matcher, "cache", None)
    if cache is None:
        return False
    return bool(cache.list_tracks(media_id))


def normalize_subtitle_backfill_status_filter(value):
    text = str(value or "").strip()
    if not text:
        return None
    if text not in SUBTITLE_BACKFILL_STATUS_FILTERS:
        raise ValueError("不支持的补齐范围：%s" % text)
    return text


def subtitle_backfill_status_filter_label(value):
    return {
        "pending": "待补可尝试",
        "untried": "未尝试",
        "not_found": "未找到",
        "failed": "失败",
    }.get(str(value or ""), "")


def extract_media_detail(response):
    if not isinstance(response, dict):
        return {}
    data = response.get("data")
    if isinstance(data, dict):
        media = data.get("media")
        if isinstance(media, dict) and extract_media_id(media):
            return media
        if extract_media_id(data):
            return data
    media = response.get("media")
    if isinstance(media, dict) and extract_media_id(media):
        return media
    if extract_media_id(response):
        return response
    return {}


def normalize_msg_subtitle_presence(value, media_id):
    if not isinstance(value, dict):
        raise RuntimeError("MediaStationGo subtitle status returned invalid response")
    actual_media_id = str(value.get("media_id") or "").strip()
    if actual_media_id != str(media_id or "").strip():
        raise RuntimeError("MediaStationGo subtitle status media id mismatch")
    if not isinstance(value.get("has_chinese"), bool) or not isinstance(value.get("embedded_checked"), bool):
        raise RuntimeError("MediaStationGo subtitle status is incomplete")
    external = value.get("external")
    embedded = value.get("embedded")
    if not isinstance(external, list) or not isinstance(embedded, list):
        raise RuntimeError("MediaStationGo subtitle status tracks are invalid")
    if not value["has_chinese"] and not value["embedded_checked"]:
        raise RuntimeError("MediaStationGo subtitle status did not inspect embedded tracks")
    tracks = []
    for track in external + embedded:
        if not isinstance(track, dict) or not isinstance(track.get("chinese"), bool):
            raise RuntimeError("MediaStationGo subtitle status contains an invalid track")
        tracks.append(dict(track))
    try:
        unknown_embedded = int(value.get("unknown_embedded") or 0)
    except (TypeError, ValueError) as exc:
        raise RuntimeError("MediaStationGo subtitle status unknown count is invalid") from exc
    if unknown_embedded < 0:
        raise RuntimeError("MediaStationGo subtitle status unknown count is invalid")
    return {
        "media_id": actual_media_id,
        "has_chinese": value["has_chinese"],
        "embedded_checked": value["embedded_checked"],
        "external": tracks[: len(external)],
        "embedded": tracks[len(external) :],
        "unknown_embedded": unknown_embedded,
    }


def subtitle_backfill_task_from_media(media, category="adult"):
    media_id = extract_media_id(media)
    title = media_display_title(media)
    path = media_primary_path(media)
    haystack = media_haystack(media)
    task = {
        "msg_media_id": media_id,
        "msg_media_title": title,
        "msg_match_path": path,
    }
    if category == "adult":
        code = first_adult_code([haystack])
        if code:
            task["openlist_adult_code"] = code
    return task


def subtitle_search_title(media):
    if not isinstance(media, dict):
        return ""
    for key in ("original_name", "original_title"):
        value = media.get(key)
        if value:
            return str(value).strip()
    nested = media.get("media")
    if isinstance(nested, dict):
        return subtitle_search_title(nested)
    return media_display_title(media)


def subtitle_category_from_media(media):
    library_id = str(media_first_value(media, ("library_id", "libraryId")) or "").strip()
    if not library_id:
        raise RuntimeError("MediaStationGo media detail missing library_id")
    for category, root in msg_library_roots().items():
        if str(root.get("library_id") or "").strip() == library_id:
            return category
    raise RuntimeError("media library is not configured in pipeline: %s" % library_id)


def subtitle_backfill_media_status(media, matcher, records):
    item = subtitle_report_item_from_media(media, records)
    media_id = item.get("media_id")
    if media_id and subtitle_matcher_has_cached_tracks(matcher, media_id):
        return "cached"
    return item.get("status") or "unknown"


def process_subtitle_backfill_media(media, matcher, store, result, retry_attempted=False, backfill_records=None, before_match=None):
    result["scanned"] += 1
    media_id = extract_media_id(media)
    title = media_display_title(media) or media_id or "-"
    if not media_id:
        result["skipped"] += 1
        result["pending"] += 1
        result["current"] = {"title": title, "reason": "media_id_missing"}
        return False
    if subtitle_matcher_has_cached_tracks(matcher, media_id):
        result["cached"] += 1
        result["with_subtitles"] += 1
        result["current"] = {"media_id": media_id, "title": title, "reason": "cached"}
        update_subtitle_backfill_recent(
            result,
            {
                "media_id": media_id,
                "title": title,
                "code": "",
                "status": "success",
                "source": "cache",
                "reason": "cached",
            },
        )
        return False
    records = backfill_records if backfill_records is not None else store.subtitle_backfill_records([media_id])
    previous = records.get(media_id)
    task = subtitle_backfill_task_from_media(media)
    code = task.get("openlist_adult_code")
    if previous and previous.get("status") == "declared" and subtitle_declared_record_matches_code(previous, code):
        result["declared"] += 1
        result["with_subtitles"] += 1
        result["current"] = {
            "media_id": media_id,
            "title": title,
            "reason": SOURCE_DECLARED_CHINESE_SUBTITLE_REASON,
        }
        update_subtitle_backfill_recent(
            result,
            {
                "media_id": media_id,
                "title": title,
                "code": previous.get("adult_code") or "",
                "status": "declared",
                "source": "resource_name",
                "reason": SOURCE_DECLARED_CHINESE_SUBTITLE_REASON,
            },
        )
        return False
    if previous and not retry_attempted and previous.get("status") in SUBTITLE_BACKFILL_SKIP_STATUSES:
        result["previous"] += 1
        result["pending"] += 1
        result["current"] = {
            "media_id": media_id,
            "title": title,
            "reason": "previous_%s" % previous.get("status"),
        }
        update_subtitle_backfill_recent(
            result,
            {
                "media_id": media_id,
                "title": title,
                "code": previous.get("adult_code") or "",
                "status": previous.get("status") or "unknown",
                "source": previous.get("source") or "",
                "reason": "previous",
                "error": previous.get("error") or "",
            },
        )
        return False
    if not code:
        result["skipped"] += 1
        result["pending"] += 1
        result["current"] = {"media_id": media_id, "title": title, "reason": "query_missing"}
        update_subtitle_backfill_recent(
            result,
            {
                "media_id": media_id,
                "title": title,
                "code": "",
                "status": "skipped",
                "reason": "query_missing",
            },
        )
        return False
    result["attempted"] += 1
    result["current"] = {"media_id": media_id, "title": title, "code": code}
    if before_match:
        before_match()
    try:
        match = matcher.match_task("adult", title, task, force=False)
    except Exception as exc:
        match = {"subtitle_match_status": "failed", "subtitle_match_error": str(exc)}
    store.save_subtitle_backfill_record(media_id, title, code, match)
    update_subtitle_backfill_result(result, media_id, title, code, match)
    return True


def update_subtitle_backfill_recent(result, item):
    recent = list(result.get("recent") or [])
    recent.append(dict(item or {}))
    result["recent"] = recent[-5:]


def update_subtitle_backfill_result(result, media_id, title, code, match):
    status = (match or {}).get("subtitle_match_status")
    if status == "success":
        source = (match or {}).get("subtitle_match_source")
        if source == "cache":
            result["cached"] += 1
        else:
            result["matched"] += 1
        result["with_subtitles"] += 1
    elif status == "skipped" and (match or {}).get("subtitle_match_reason") == "not_found":
        result["not_found"] += 1
        result["pending"] += 1
    elif status == "failed":
        result["failed"] += 1
        result["pending"] += 1
    else:
        result["skipped"] += 1
        result["pending"] += 1
    update_subtitle_backfill_recent(
        result,
        {
            "media_id": media_id,
            "title": title,
            "code": code,
            "status": status or "unknown",
            "source": (match or {}).get("subtitle_match_source"),
            "reason": (match or {}).get("subtitle_match_reason"),
            "error": (match or {}).get("subtitle_match_error"),
        },
    )


def format_subtitle_backfill_message(result):
    result = result or {}
    status = result.get("status") or "running"
    title = "成人库字幕补齐：%s" % ("已完成" if status == "success" else "进行中")
    lines = [
        title,
    ]
    if result.get("status_filter"):
        lines.append("范围：%s" % subtitle_backfill_status_filter_label(result.get("status_filter")))
    lines.extend(
        [
            "扫描：%s  尝试：%s/%s" % (result.get("scanned") or 0, result.get("attempted") or 0, result.get("limit") or 0),
            "已补字幕：%s  待补：%s" % (result.get("with_subtitles") or 0, result.get("pending") or 0),
            "命中：%s  已有缓存：%s  资源已带：%s  已尝试跳过：%s  未找到：%s  失败：%s  跳过：%s"
            % (
                result.get("matched") or 0,
                result.get("cached") or 0,
                result.get("declared") or 0,
                result.get("previous") or 0,
                result.get("not_found") or 0,
                result.get("failed") or 0,
                result.get("skipped") or 0,
            ),
        ]
    )
    current = result.get("current") or {}
    if current:
        lines.append("当前：%s%s" % (current.get("code") or current.get("media_id") or "-", " / %s" % current.get("title") if current.get("title") else ""))
    recent = result.get("recent") or []
    if recent:
        lines.append("最近结果：")
        for item in recent[-3:]:
            label = subtitle_backfill_status_label(item)
            lines.append("- %s %s %s" % (item.get("code") or item.get("media_id") or "-", label, item.get("title") or ""))
    return "\n".join(lines)


def subtitle_backfill_status_label(item):
    status = (item or {}).get("status")
    source = (item or {}).get("source")
    if status == "success" and source == "cache":
        return "已有"
    if status == "success":
        return "命中%s" % ("(%s)" % source if source else "")
    if status == "declared":
        return "资源已带中文字幕"
    if status == "skipped" and (item or {}).get("reason") == "not_found":
        return "未找到"
    if status == "failed":
        return "失败"
    return status or "未知"


def new_subtitle_backfill_report():
    return {
        "total": 0,
        "with_subtitles": 0,
        "declared": 0,
        "pending": 0,
        "untried": 0,
        "not_found": 0,
        "failed": 0,
        "no_code": 0,
        "success_missing_cache": 0,
        "unknown": 0,
        "generated_at": 0,
        "buckets": {
            "pending": [],
            "cached": [],
            "untried": [],
            "not_found": [],
            "failed": [],
            "no_code": [],
        },
    }


def add_media_to_subtitle_backfill_report(report, matcher, records, media):
    report["total"] += 1
    item = subtitle_report_item_from_media(media, records)
    media_id = item.get("media_id")
    if media_id and subtitle_matcher_has_cached_tracks(matcher, media_id):
        item["status"] = "cached"
        item["status_label"] = "已补"
        report["with_subtitles"] += 1
        report["buckets"]["cached"].append(item)
        return
    if item.get("status") == "declared":
        report["declared"] += 1
        report["with_subtitles"] += 1
        report["buckets"]["cached"].append(item)
        return

    report["pending"] += 1
    report["buckets"]["pending"].append(item)
    status = item.get("status")
    if status in ("not_found", "failed", "no_code", "untried"):
        report[status] += 1
        report["buckets"][status].append(item)
    elif status == "success":
        report["success_missing_cache"] += 1
    else:
        report["unknown"] += 1


def subtitle_report_item_from_media(media, records):
    media_id = extract_media_id(media)
    title = media_display_title(media) or media_id or "-"
    task = subtitle_backfill_task_from_media(media)
    code = task.get("openlist_adult_code") or ""
    record = (records or {}).get(media_id) if media_id else None
    if record and record.get("status") == "declared" and not subtitle_declared_record_matches_code(record, code):
        record = None
    status = "untried"
    status_label = "未尝试"
    if not code:
        status = "no_code"
        status_label = "无番号"
    elif record:
        status = record.get("status") or "unknown"
        status_label = subtitle_report_status_label(status)
    return {
        "media_id": media_id,
        "title": title,
        "code": code,
        "status": status,
        "status_label": status_label,
        "attempt_count": (record or {}).get("attempt_count") or 0,
        "source": (record or {}).get("source") or "",
        "reason": (record or {}).get("reason") or "",
        "error": (record or {}).get("error") or "",
    }


def subtitle_declared_record_matches_code(record, code):
    record_code = first_adult_code([(record or {}).get("adult_code")])
    media_code = first_adult_code([code])
    return bool(record_code and media_code and record_code.casefold() == media_code.casefold())


def subtitle_report_status_label(status):
    return {
        "cached": "已补",
        "declared": "资源已标中文字幕",
        "success": "记录成功但缓存缺失",
        "not_found": "未找到",
        "failed": "失败",
        "no_code": "无番号",
        "untried": "未尝试",
        "unknown": "未知",
    }.get(str(status or ""), str(status or "未知"))


def normalize_subtitle_report_bucket(bucket):
    value = str(bucket or "").strip()
    return value if value in SUBTITLE_REPORT_BUCKET_LABELS else "pending"


def subtitle_report_bucket_items(report, bucket):
    bucket = normalize_subtitle_report_bucket(bucket)
    buckets = (report or {}).get("buckets") or {}
    return list(buckets.get(bucket) or [])


def subtitle_find_media_matches_query(media, query):
    return subtitle_find_media_score(media, query) > 0


def subtitle_find_media_score(media, query):
    query_text = str(query or "").strip()
    if not query_text or not isinstance(media, dict):
        return 0
    haystack = media_haystack(media)
    query_codes = extract_codes(query_text)
    media_codes = extract_codes(haystack)
    score = 0
    if query_codes and query_codes.intersection(media_codes):
        score += 1000
    normalized_query = normalize_fragment(query_text)
    normalized_haystack = normalize_fragment(haystack)
    title = media_display_title(media)
    normalized_title = normalize_fragment(title)
    if normalized_query and normalized_query in normalized_title:
        score += 500
    elif normalized_query and normalized_query in normalized_haystack:
        score += 250
    query_terms = [normalize_fragment(part) for part in re.split(r"\s+", query_text) if normalize_fragment(part)]
    if query_terms and all(term in normalized_haystack for term in query_terms):
        score += 100
    return score


def subtitle_find_result_item_from_media(media, matcher, records):
    item = subtitle_report_item_from_media(media, records)
    media_id = item.get("media_id")
    if media_id and subtitle_matcher_has_cached_tracks(matcher, media_id):
        item["status"] = "cached"
        item["status_label"] = "已补"
    return item


def subtitle_find_bucket_for_item(item):
    status = str((item or {}).get("status") or "")
    if status in ("cached", "declared"):
        return "cached"
    if status in ("untried", "not_found", "failed", "no_code"):
        return status
    return "pending"


def subtitle_find_item_button_label(index, item):
    status = (item or {}).get("status")
    if status in ("cached", "declared"):
        return "#%s 重配" % index
    if status in SUBTITLE_BACKFILL_SKIP_STATUSES:
        return "#%s 重试" % index
    return "#%s 补齐" % index


def format_subtitle_find_message(result):
    result = result or {}
    items = list(result.get("items") or [])
    lines = [
        "字幕影片查找：%s" % (result.get("query") or "-"),
        "结果：%s 条" % len(items),
    ]
    if not items:
        lines.append("未找到匹配影片。")
        return "\n".join(lines)
    for index, item in enumerate(items, start=1):
        code = item.get("code") or "-"
        title = item.get("title") or "-"
        line = "#%s %s / %s" % (index, code, title)
        details = []
        if item.get("status_label"):
            details.append(item["status_label"])
        if item.get("source"):
            details.append(item["source"])
        if item.get("attempt_count"):
            details.append("尝试%s次" % item.get("attempt_count"))
        if details:
            line += "\n   " + "，".join(details)
        lines.append(line)
    return "\n".join(lines)


def subtitle_find_reply_markup(result):
    items = list((result or {}).get("items") or [])
    keyboard = []
    buttons = []
    for index, item in enumerate(items, start=1):
        callback_data = subtitle_report_item_callback_data(item, subtitle_find_bucket_for_item(item), 0)
        if not callback_data:
            continue
        buttons.append({"text": subtitle_find_item_button_label(index, item), "callback_data": callback_data})
    for offset in range(0, len(buttons), 2):
        keyboard.append(buttons[offset : offset + 2])
    keyboard.extend(subtitle_find_prompt_reply_markup()["inline_keyboard"])
    return {"inline_keyboard": keyboard}


def subtitle_find_prompt_reply_markup():
    return {
        "inline_keyboard": [
            [{"text": "返回字幕报表", "callback_data": "subtitle_report:pending:0"}],
        ]
    }


def format_subtitle_backfill_report_message(report, bucket="pending", page=0):
    report = report or new_subtitle_backfill_report()
    bucket = normalize_subtitle_report_bucket(bucket)
    items = subtitle_report_bucket_items(report, bucket)
    page_count = search_page_count(len(items), page_size=SUBTITLE_REPORT_PAGE_SIZE)
    page = normalize_page(page, page_count)
    start = page * SUBTITLE_REPORT_PAGE_SIZE
    page_items = items[start : start + SUBTITLE_REPORT_PAGE_SIZE]
    lines = [
        "成人库字幕补齐报表",
        "总数：%s  已补字幕：%s  待补：%s" % (report.get("total") or 0, report.get("with_subtitles") or 0, report.get("pending") or 0),
        "资源已标中文字幕：%s" % (report.get("declared") or 0),
        "未尝试：%s  未找到：%s  失败：%s  无番号：%s"
        % (report.get("untried") or 0, report.get("not_found") or 0, report.get("failed") or 0, report.get("no_code") or 0),
        "列表：%s 第 %s/%s 页（%s 条）" % (SUBTITLE_REPORT_BUCKET_LABELS[bucket], page + 1, page_count, len(items)),
    ]
    if not page_items:
        lines.append("暂无记录")
    for index, item in enumerate(page_items, start=start + 1):
        code = item.get("code") or "-"
        title = item.get("title") or "-"
        line = "%s. %s / %s" % (index, code, title)
        details = []
        if item.get("status_label"):
            details.append(item["status_label"])
        if item.get("source"):
            details.append(item["source"])
        if item.get("attempt_count"):
            details.append("尝试%s次" % item.get("attempt_count"))
        if details:
            line += "\n   " + "，".join(details)
        lines.append(line)
    return "\n".join(lines)


def subtitle_backfill_report_reply_markup(report, bucket="pending", page=0, batch_limit=DEFAULT_SUBTITLE_BACKFILL_LIMIT):
    bucket = normalize_subtitle_report_bucket(bucket)
    items = subtitle_report_bucket_items(report, bucket)
    page_count = search_page_count(len(items), page_size=SUBTITLE_REPORT_PAGE_SIZE)
    page = normalize_page(page, page_count)
    previous_page = max(0, page - 1)
    next_page = min(page_count - 1, page + 1)
    batch_limit = normalize_subtitle_backfill_limit(batch_limit)
    start = page * SUBTITLE_REPORT_PAGE_SIZE
    page_items = items[start : start + SUBTITLE_REPORT_PAGE_SIZE]
    keyboard = []
    operation_row = subtitle_report_bulk_operation_row(report, bucket, batch_limit)
    if operation_row:
        keyboard.append(operation_row)
    keyboard.extend(
        [
            [
                {"text": "待补", "callback_data": "subtitle_report:pending:0"},
                {"text": "已补", "callback_data": "subtitle_report:cached:0"},
                {"text": "未尝试", "callback_data": "subtitle_report:untried:0"},
            ],
            [
                {"text": "未找到", "callback_data": "subtitle_report:not_found:0"},
                {"text": "失败", "callback_data": "subtitle_report:failed:0"},
                {"text": "无番号", "callback_data": "subtitle_report:no_code:0"},
            ],
        ]
    )
    item_buttons = []
    for index, item in enumerate(page_items, start=start + 1):
        callback_data = subtitle_report_item_callback_data(item, bucket, page)
        if not callback_data:
            continue
        if item.get("status") in ("cached", "declared"):
            label = "#%s 重配" % index
        else:
            label = "#%s 重试" % index if (item.get("status") in SUBTITLE_BACKFILL_SKIP_STATUSES) else "#%s 补齐" % index
        item_buttons.append({"text": label, "callback_data": callback_data})
    for offset in range(0, len(item_buttons), 2):
        keyboard.append(item_buttons[offset : offset + 2])
    keyboard.append([{"text": "查找影片", "callback_data": "subfind_prompt:1"}])
    keyboard.append(
        [
            {"text": "‹ 上一页", "callback_data": "subtitle_report:%s:%s" % (bucket, previous_page)},
            {"text": "↻ 刷新", "callback_data": "subtitle_report:%s:%s" % (bucket, page)},
            {"text": "下一页 ›", "callback_data": "subtitle_report:%s:%s" % (bucket, next_page)},
        ]
    )
    keyboard.append([{"text": "⌂ 功能菜单", "callback_data": "home:menu"}])
    return {
        "inline_keyboard": keyboard
    }


def subtitle_report_bulk_operation_row(report, bucket, batch_limit):
    bucket = normalize_subtitle_report_bucket(bucket)
    count = int((report or {}).get(bucket) or 0)
    if bucket == "pending":
        count = int((report or {}).get("pending") or 0)
        if count <= 0:
            return []
        return [{"text": "批量补齐待补可尝试 %s" % batch_limit, "callback_data": "subbulk:pending:%s" % batch_limit}]
    if bucket == "untried" and count > 0:
        return [{"text": "批量补齐未尝试 %s" % batch_limit, "callback_data": "subbulk:untried:%s" % batch_limit}]
    if bucket == "failed" and count > 0:
        return [{"text": "批量重试失败 %s" % batch_limit, "callback_data": "subbulkr:failed:%s" % batch_limit}]
    if bucket == "not_found" and count > 0:
        return [{"text": "批量重试未找到 %s" % batch_limit, "callback_data": "subbulkr:not_found:%s" % batch_limit}]
    return []


def subtitle_report_item_callback_data(item, bucket, page):
    media_id = str((item or {}).get("media_id") or "").strip()
    if not media_id:
        return None
    if not (item or {}).get("code"):
        return None
    status = (item or {}).get("status")
    if status == "no_code":
        return None
    action = "subrematch" if status in ("cached", "declared") else ("sub1r" if status in SUBTITLE_BACKFILL_SKIP_STATUSES else "sub1")
    data = "%s:%s:%s:%s" % (action, normalize_subtitle_report_bucket(bucket), max(0, int(page or 0)), media_id)
    if len(data.encode("utf-8")) > 64:
        return None
    return data


def format_subtitle_rematch_candidates_message(result, candidates, llm_status="disabled", llm_error=""):
    result = result or {}
    candidates = list(candidates or [])
    lines = [
        "字幕重新匹配候选",
        "媒体：%s / %s" % (result.get("code") or "-", result.get("title") or result.get("media_id") or "-"),
        "候选：%s 条" % len(candidates),
    ]
    if llm_status == "success":
        lines.append("LLM：已基于字幕正文预览排序")
    elif llm_status == "disabled":
        lines.append("LLM：未启用，按字幕源顺序展示")
    elif llm_status == "timeout":
        lines.append("LLM：超时，按字幕源顺序展示")
    elif llm_status == "busy":
        lines.append("LLM：已有任务运行，按字幕源顺序展示")
    elif llm_status == "failed":
        lines.append("LLM：评估失败，按字幕源顺序展示")
    if llm_error:
        lines.append("LLM错误：%s" % short_text(llm_error, 120))
    if not candidates:
        lines.append("未找到可用候选")
        return "\n".join(lines)
    for index, candidate in enumerate(candidates, start=1):
        title = candidate.get("filename") or candidate.get("title") or "-"
        details = []
        if candidate.get("provider"):
            details.append(str(candidate.get("provider")))
        if candidate.get("language"):
            details.append(str(candidate.get("language")))
        if candidate.get("source_score"):
            details.append("score=%s" % candidate.get("source_score"))
        if candidate.get("preview_error"):
            details.append("预览失败")
        line = "#%s %s" % (index, short_text(title, 64))
        if details:
            line += "\n   " + " / ".join(details)
        if candidate.get("llm_reason"):
            line += "\n   LLM：" + short_text(candidate.get("llm_reason"), 80)
        lines.append(line)
    return "\n".join(lines)


def subtitle_rematch_candidates_reply_markup(candidate_ids, bucket="cached", page=0):
    keyboard = []
    buttons = [
        {"text": "#%s 应用" % index, "callback_data": "subpick:%s" % candidate_id}
        for index, candidate_id in enumerate(candidate_ids or [], start=1)
        if len(("subpick:%s" % candidate_id).encode("utf-8")) <= 64
    ]
    for offset in range(0, len(buttons), 2):
        keyboard.append(buttons[offset : offset + 2])
    keyboard.extend(subtitle_rematch_done_reply_markup(bucket, page)["inline_keyboard"])
    return {"inline_keyboard": keyboard}


def subtitle_rematch_done_reply_markup(bucket="cached", page=0):
    bucket = normalize_subtitle_report_bucket(bucket)
    page = max(0, int(page or 0))
    return {
        "inline_keyboard": [
            [{"text": "返回字幕报表", "callback_data": "subtitle_report:%s:%s" % (bucket, page)}],
        ]
    }


def format_subtitle_apply_result_message(result):
    result = result or {}
    lines = [
        "字幕已应用",
        "媒体：%s / %s" % (result.get("code") or "-", result.get("title") or result.get("media_id") or "-"),
        "来源：%s" % (result.get("subtitle_match_source") or "-"),
    ]
    if result.get("subtitle_match_filename"):
        lines.append("文件：%s" % result.get("subtitle_match_filename"))
    if result.get("subtitle_match_status") != "success":
        lines[0] = "字幕应用未完成"
        if result.get("subtitle_match_reason"):
            lines.append("原因：%s" % result.get("subtitle_match_reason"))
    return "\n".join(lines)


def short_text(value, limit):
    text = str(value or "").replace("\r", " ").replace("\n", " ").strip()
    limit = max(4, int(limit or 80))
    if len(text) <= limit:
        return text
    return text[: limit - 1] + "…"


def clean_openlist_task_media(
    client,
    category_path,
    queries,
    task=None,
    max_bytes=DEFAULT_OPENLIST_PRE_SCAN_CLEAN_MAX_BYTES,
    hide_extra_scan_items=False,
):
    target = find_openlist_task_target(client, category_path, queries, task=task)
    if target is None:
        raise RuntimeError("OpenList target not found for cleanup")

    target_dir, target_item = target
    target_path = openlist_item_path(target_dir, target_item)
    hide_groups = defaultdict(list)
    cleaned_count = 0
    cleaned_bytes = 0
    for dir_path, item in iter_openlist_files(client, target_dir, target_item):
        if should_keep_openlist_file(item, max_bytes=max_bytes):
            continue
        name = openlist_item_name(item)
        if not name:
            continue
        hide_groups[dir_path].append("^%s$" % re.escape(name))
        cleaned_count += 1
        cleaned_bytes += openlist_item_size(item)

    if hide_extra_scan_items and openlist_item_is_dir(target_item):
        for pattern in openlist_extra_scan_hide_patterns(client, target_path):
            if pattern not in hide_groups[target_path]:
                hide_groups[target_path].append(pattern)

    hidden_count = 0
    for dir_path, patterns in sorted(hide_groups.items()):
        client.upsert_meta_hide(dir_path, patterns, h_sub=True)
        hidden_count += len(patterns)

    return {
        "openlist_clean_status": "success",
        "openlist_clean_target": target_path,
        "openlist_cleaned_count": cleaned_count,
        "openlist_cleaned_bytes": cleaned_bytes,
        "openlist_hidden_count": hidden_count,
        "openlist_cleaned_at": int(time.time()),
        "openlist_clean_error": None,
    }


def format_openlist_adult_code(client, category_path, queries, task=None, allow_existing_target=False):
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
    new_name = adult_code_formatted_name(code, old_name)
    new_path = old_path
    renamed_target = False
    target_name_occupied = False
    if new_name != old_name:
        new_path = posixpath.join(str(target_dir).rstrip("/") or "/", new_name)
        target_name_occupied = any(
            openlist_item_name(item).casefold() == new_name.casefold()
            and openlist_item_path(target_dir, item) != old_path
            for item in client.list_all(target_dir, refresh=False)
        )
        if not (allow_existing_target and target_name_occupied):
            client.rename_path(old_path, new_name)
            renamed_target = True
        else:
            new_path = old_path

    video_rename = None
    if openlist_item_is_dir(target_item):
        video_rename = format_openlist_adult_main_video(client, new_path, code)

    if target_name_occupied and allow_existing_target:
        return {
            "openlist_adult_format_status": "skipped",
            "openlist_adult_format_reason": "upgrade_target_name_occupied",
            "openlist_adult_code": code,
            "openlist_adult_format_path": old_path,
            "openlist_adult_video_old_path": (video_rename or {}).get("old_path"),
            "openlist_adult_video_new_path": (video_rename or {}).get("new_path"),
            "openlist_adult_formatted_at": int(time.time()),
        }

    if not renamed_target and not video_rename:
        return {
            "openlist_adult_format_status": "skipped",
            "openlist_adult_format_reason": "already_formatted",
            "openlist_adult_code": code,
            "openlist_adult_format_path": old_path,
            "openlist_adult_formatted_at": int(time.time()),
        }

    return {
        "openlist_adult_format_status": "success",
        "openlist_adult_code": code,
        "openlist_adult_format_old_path": old_path,
        "openlist_adult_format_new_path": new_path,
        "openlist_adult_video_old_path": (video_rename or {}).get("old_path"),
        "openlist_adult_video_new_path": (video_rename or {}).get("new_path"),
        "openlist_adult_formatted_at": int(time.time()),
        "openlist_adult_format_error": None,
    }


def hide_openlist_adult_extra_videos(
    client,
    category_path,
    queries,
    task=None,
    pre_scan_max_bytes=DEFAULT_OPENLIST_PRE_SCAN_CLEAN_MAX_BYTES,
):
    target = find_openlist_task_target(client, category_path, queries, task=task)
    if target is None:
        return {
            "openlist_adult_extra_hide_status": "skipped",
            "openlist_adult_extra_hide_reason": "target_not_found",
            "openlist_adult_extra_hidden_count": 0,
            "openlist_adult_extra_hidden_at": int(time.time()),
        }

    _target_dir, target_item = target
    if not openlist_item_is_dir(target_item):
        return {
            "openlist_adult_extra_hide_status": "skipped",
            "openlist_adult_extra_hide_reason": "not_directory",
            "openlist_adult_extra_hidden_count": 0,
            "openlist_adult_extra_hidden_at": int(time.time()),
        }

    target_path = openlist_item_path(_target_dir, target_item)
    patterns = openlist_adult_extra_video_hide_patterns(client, target_path, pre_scan_max_bytes=pre_scan_max_bytes)
    if patterns:
        client.upsert_meta_hide(target_path, patterns, h_sub=True)

    return {
        "openlist_adult_extra_hide_status": "success",
        "openlist_adult_extra_hide_path": target_path,
        "openlist_adult_extra_hidden_count": len(patterns),
        "openlist_adult_extra_hidden_at": int(time.time()),
        "openlist_adult_extra_hide_error": None,
        "openlist_adult_extra_hide_reason": "extras_hidden" if patterns else "already_clean",
    }


def openlist_adult_extra_video_hide_patterns(
    client,
    target_path,
    pre_scan_max_bytes=DEFAULT_OPENLIST_PRE_SCAN_CLEAN_MAX_BYTES,
):
    videos = []
    for item in client.list_all(target_path, refresh=False):
        if openlist_item_is_dir(item) or not is_openlist_video_file(item):
            continue
        if not should_keep_openlist_file(item, max_bytes=pre_scan_max_bytes):
            continue
        name = openlist_item_name(item)
        if name:
            videos.append((name, openlist_item_size(item)))
    if len(videos) <= 1:
        return []

    max_size = max(size for _name, size in videos)
    threshold = max(ADULT_EXTRA_VIDEO_HIDE_MIN_BYTES, int(max_size * 0.2))
    patterns = []
    for name, size in videos:
        if size < max_size and size < threshold:
            patterns.append("^%s$" % re.escape(name))
    return patterns


def openlist_target_names(client, target_dir, target_item):
    names = [openlist_item_name(target_item)]
    if openlist_item_is_dir(target_item):
        for _dir_path, item in iter_openlist_files(client, target_dir, target_item):
            names.append(openlist_item_name(item))
    return names


def adult_code_prefix_matches(name, code):
    raw_name = str(name or "").strip()
    raw_code = str(code or "").strip()
    if not raw_name or not raw_code:
        return False
    lowered_name = raw_name.casefold()
    lowered_code = raw_code.casefold()
    return lowered_name == lowered_code or lowered_name.startswith(lowered_code + " - ")


def adult_code_formatted_name(code, old_name):
    code = str(code or "").strip()
    old_name = str(old_name or "").strip()
    if not code:
        return old_name
    suffix = posixpath.splitext(old_name)[1].lower()
    if suffix in VIDEO_EXTENSIONS:
        return "%s%s" % (code, suffix)
    return code


def format_openlist_adult_main_video(client, target_path, code):
    videos = []
    for item in client.list_all(target_path, refresh=False):
        if openlist_item_is_dir(item) or not is_openlist_video_file(item):
            continue
        videos.append((openlist_item_size(item), item))
    if not videos:
        return None

    _size, item = max(videos, key=lambda pair: pair[0])
    old_name = openlist_item_name(item)
    new_name = adult_code_formatted_name(code, old_name)
    if not new_name or new_name == old_name:
        return None

    old_path = openlist_item_path(target_path, item)
    new_path = posixpath.join(str(target_path).rstrip("/") or "/", new_name)
    client.rename_path(old_path, new_name)
    return {"old_path": old_path, "new_path": new_path}


def adult_code_name_suffix(name, code):
    code_match = re.match(r"^([A-Za-z]{2,10})-(\d{3,5})$", str(code or ""))
    if not code_match:
        return None
    prefix = re.escape(code_match.group(1))
    number = re.escape(str(int(code_match.group(2))))
    raw = str(name or "").strip()
    match = re.match(r"(?i)^\s*%s[\s._-]*0*%s(?P<suffix>.*)$" % (prefix, number), raw)
    if not match:
        return None
    suffix = match.group("suffix").strip()
    if suffix.casefold() == "ch":
        return ""
    if suffix[:2].casefold() == "ch" and (len(suffix) == 2 or suffix[2] in " ._-"):
        suffix = suffix[2:].lstrip(" ._-")
    return suffix.strip(" ._-")


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


def openlist_extra_scan_hide_patterns(client, target_path):
    patterns = []
    for item in client.list_all(target_path, refresh=False):
        name = openlist_item_name(item)
        if not name or not openlist_item_looks_like_extra_scan_item(item):
            continue
        patterns.append("^%s$" % re.escape(name))
    return patterns


def openlist_item_looks_like_extra_scan_item(item):
    name = openlist_item_name(item)
    if not name:
        return False
    stem = posixpath.splitext(name)[0] if not openlist_item_is_dir(item) else name
    normalized = normalize_openlist_text(stem)
    if normalized in {normalize_openlist_text(token) for token in EXTRA_SCAN_NAME_TOKENS}:
        return True
    if not openlist_item_is_dir(item) and is_openlist_video_file(item):
        return extra_scan_name_contains_token(stem)
    return False


def extra_scan_name_contains_token(value):
    normalized = normalize_openlist_text(value)
    for token in EXTRA_SCAN_NAME_TOKENS:
        token_normalized = normalize_openlist_text(token)
        if token_normalized and token_normalized in normalized:
            return True
    return False


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


def openlist_child_exists_for_hide(client, path):
    try:
        return openlist_child_exists(client, path)
    except RuntimeError as exc:
        if openlist_missing_error(exc):
            return False
        raise


def openlist_missing_error(exc):
    text = str(exc or "").lower()
    return any(token in text for token in ("not found", "not exist", "不存在", "不存在或已删除", "已删除"))


def parse_callback_data(value):
    action, sep, payload = (value or "").partition(":")
    if not sep:
        return None, None
    if action == "home" and payload:
        return "home", payload
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
    if action == "bt4g_search":
        return "bt4g_search", int(payload)
    if action == "pansou_search":
        return "pansou_search", int(payload)
    if action == "llm_rerank":
        return "llm_rerank", int(payload)
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
    if action in ("status", "cancel", "retry_msg", "subtitle") and payload:
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
    if action == "p115_cookie_start" and payload:
        return "p115_cookie_start", payload
    if action in ("p115_cookie_check", "p115_cookie_cancel") and payload:
        return action, int(payload)
    if action in ("subtitle_backfill_confirm", "subtitle_backfill_retry") and payload:
        return action, int(payload)
    if action == "subtitle_backfill_cancel" and payload:
        return action, payload
    if action == "subtitle_report" and payload:
        bucket, sep, page = payload.partition(":")
        if sep:
            return action, (bucket, int(page))
    if action == "subfind_prompt" and payload:
        return action, payload
    if action in ("subbulk", "subbulkr") and payload:
        bucket, sep, limit = payload.partition(":")
        if sep:
            return action, (bucket, int(limit))
    if action in ("sub1", "sub1r") and payload:
        bucket, sep, rest = payload.partition(":")
        page, page_sep, media_id = rest.partition(":")
        if sep and page_sep and media_id:
            return action, (bucket, int(page), media_id)
    if action == "subrematch" and payload:
        bucket, sep, rest = payload.partition(":")
        page, page_sep, media_id = rest.partition(":")
        if sep and page_sep and media_id:
            return action, (bucket, int(page), media_id)
    if action == "subpick" and payload:
        return action, int(payload)
    return None, None


def share115_candidate_from_text(text):
    parsed = parse_115_share_url(text)
    if parsed is None:
        return None
    return {
        "title": "115分享 %s" % parsed.share_code,
        "download_uri": parsed.url,
        "indexer": "115分享",
        "seeders": None,
        "size": None,
        "rank": 1,
        "source_kind": "115_share",
        "shareCode": parsed.share_code,
    }


def format_p115_cookie_status_message(cookie):
    if p115_cookie_is_valid(cookie):
        return "115 分享转存 Cookie：可用\n%s\n\n可扫码更新；Bot 不会回显完整 Cookie。" % mask_p115_cookie(cookie)
    return "115 分享转存 Cookie：未配置或格式无效\n\n点击“扫码更新”，使用 115 App 扫码确认后保存。Bot 不会回显完整 Cookie。"


def format_p115_cookie_saved_message(cookie):
    return "115 Cookie 已保存，后续 115 分享链接转存会直接使用。\n%s" % mask_p115_cookie(cookie)


def format_p115_qr_message(session_id, session, status_label="等待扫码"):
    return "\n".join(
        [
            "115 扫码登录",
            "会话：%s" % session_id,
            "状态：%s" % status_label,
            "二维码链接：%s" % ((session or {}).get("qrcode_url") or "-"),
            "",
            "用 115 App 扫码并确认后，点击“刷新状态”。",
        ]
    )


def format_p115_qr_final_message(status, cookie):
    if status == "confirmed":
        return format_p115_cookie_saved_message(cookie)
    if status == "expired":
        return "115 扫码登录二维码已过期。\n\n当前 Cookie：%s" % (
            mask_p115_cookie(cookie) if p115_cookie_is_valid(cookie) else "未配置或格式无效"
        )
    if status == "cancelled":
        return "已取消本次 115 扫码登录。\n\n当前 Cookie：%s" % (
            mask_p115_cookie(cookie) if p115_cookie_is_valid(cookie) else "未配置或格式无效"
        )
    return format_p115_cookie_status_message(cookie)


def qrcode_session_final_label(status):
    return {
        "confirmed": "已保存",
        "expired": "二维码已过期",
        "cancelled": "已取消",
    }.get(str(status or ""), "已结束")


def p115_cookie_status_reply_markup():
    return {
        "inline_keyboard": [
            [{"text": "扫码更新", "callback_data": "p115_cookie_start:1"}],
            [{"text": "⌂ 功能菜单", "callback_data": "home:menu"}],
        ]
    }


def p115_cookie_qr_reply_markup(session_id):
    return {
        "inline_keyboard": [
            [
                {"text": "刷新状态", "callback_data": "p115_cookie_check:%s" % int(session_id)},
                {"text": "取消", "callback_data": "p115_cookie_cancel:%s" % int(session_id)},
            ]
        ]
    }


def first_value(value):
    if isinstance(value, (list, tuple)):
        for item in value:
            if item:
                return item
        return None
    return value


def subtitle_backfill_record_from_row(row):
    return {
        "media_id": row[0],
        "adult_code": row[1],
        "title": row[2],
        "status": row[3],
        "source": row[4],
        "reason": row[5],
        "error": row[6],
        "attempt_count": row[7],
        "created_at": row[8],
        "updated_at": row[9],
    }


def subtitle_backfill_record_status(match):
    status = str((match or {}).get("subtitle_match_status") or "").strip()
    if status == "success":
        return "success"
    if status == "failed":
        return "failed"
    if status == "skipped" and (match or {}).get("subtitle_match_reason") == "not_found":
        return "not_found"
    return status or "unknown"


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


def candidate_submission_from_row(row, claimed=False):
    return {
        "candidate_id": row[0],
        "status": row[1],
        "info_hash": row[2],
        "error": row[3],
        "created_at": row[4],
        "updated_at": row[5],
        "claimed": bool(claimed),
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


def ensure_submit_result_has_task_identity(result, download_uri):
    info_hash = candidate_info_hash({"download_uri": download_uri})
    if not info_hash or not submit_response_should_track(result):
        return result
    tasks = result.setdefault("tasks", [])
    for task in tasks:
        if str((task or {}).get("info_hash") or "").strip():
            return result
    tasks.append(
        {
            "info_hash": info_hash,
            "state": result.get("state"),
            "code": result.get("code"),
            "message": result.get("message"),
            "status_name": "submitted",
        }
    )
    return result


def submit_response_should_track(result):
    if (result or {}).get("state") is True:
        return True
    text = " ".join(str((result or {}).get(key) or "") for key in ("code", "message"))
    return "已存在" in text or "已添加" in text


def first_submit_task_info_hash(result):
    for task in (result or {}).get("tasks") or []:
        value = str((task or {}).get("info_hash") or "").strip()
        if value:
            return value
    return None


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


def format_task_diagnostics_message(record):
    task = (record or {}).get("task") or {}
    category = (record or {}).get("category")
    lines = ["任务诊断：%s" % ((record or {}).get("title") or task.get("name") or task.get("info_hash") or "-")]
    lines.append("库：%s" % CATEGORY_LABELS.get(category, category or "-"))
    append_task_lines(lines, task, category=category)

    target_paths = msg_target_openlist_paths(category, task) if category else []
    if target_paths:
        lines.append("目标路径候选：")
        for path in target_paths[:5]:
            lines.append("- %s" % path)

    stage_values = task_diagnostic_stage_values(task)
    if stage_values:
        lines.append("阶段：")
        for label, value in stage_values:
            lines.append("- %s：%s" % (label, msg_sync_status_label(value)))

    for key, label in (
        ("openlist_clean_error", "OpenList隐藏错误"),
        ("openlist_trash_hide_error", "回收站隐藏错误"),
        ("openlist_adult_format_error", "番号格式化错误"),
        ("openlist_adult_extra_hide_error", "成人附加隐藏错误"),
        ("msg_error", "MSG错误"),
        ("msg_extra_cleanup_error", "特典隐藏错误"),
        ("msg_visibility_repair_error", "可见性修复错误"),
    ):
        if task.get(key):
            lines.append("%s：%s" % (label, task.get(key)))
    return "\n".join(lines)


def format_msg_media_diagnostics_message(media_id, media):
    lines = ["MSG媒体诊断：%s" % media_id]
    if not isinstance(media, dict):
        lines.append("返回：非对象")
        return "\n".join(lines)

    actual_id = extract_media_id(media)
    if actual_id and actual_id != str(media_id):
        lines.append("实际ID：%s" % actual_id)
    title = media_display_title(media)
    if title:
        lines.append("标题：%s" % title)
    library_id = media_first_value(media, ("library_id", "libraryId"))
    if library_id:
        lines.append("library_id：%s" % library_id)
    root_id = media_first_value(media, ("library_root_id", "libraryRootId", "root_id", "rootId"))
    if root_id:
        lines.append("root_id：%s" % root_id)
    path = media_primary_path(media)
    if path:
        lines.append("路径：%s" % path)
    relative_path = media_first_value(media, ("relative_path", "relativePath"))
    if relative_path:
        lines.append("relative_path：%s" % relative_path)

    size = media_item_size_bytes(media)
    if size > 0:
        lines.append("大小：%s" % format_size(size))
    duration = media_first_int(media, ("duration_sec", "durationSec", "duration", "runtime_sec", "runtimeSec"))
    if duration is not None:
        lines.append("时长：%s秒" % duration)
    provider = media_first_value(media, ("provider", "metadata_provider", "metadataProvider"))
    if provider:
        lines.append("刮削源：%s" % provider)
    return "\n".join(lines)


def media_first_value(value, keys):
    if isinstance(value, dict):
        for key in keys:
            item = value.get(key)
            if item not in (None, ""):
                return item
        for child in value.values():
            if isinstance(child, (dict, list)):
                item = media_first_value(child, keys)
                if item not in (None, ""):
                    return item
    elif isinstance(value, list):
        for child in value:
            item = media_first_value(child, keys)
            if item not in (None, ""):
                return item
    return None


def media_first_int(value, keys):
    item = media_first_value(value, keys)
    if item in (None, ""):
        return None
    try:
        return int(item)
    except (TypeError, ValueError):
        return None


def media_is_same_upgrade_work(category, target, duplicate):
    if not isinstance(target, dict) or not isinstance(duplicate, dict):
        return False

    target_library_id = normalized_media_identity(target, ("library_id", "libraryId"))
    duplicate_library_id = normalized_media_identity(duplicate, ("library_id", "libraryId"))
    if not target_library_id or target_library_id != duplicate_library_id:
        return False

    for keys in (
        ("version_group_key", "versionGroupKey"),
        ("part_group_key", "partGroupKey"),
        ("series_id", "seriesId"),
    ):
        target_value = normalized_media_identity(target, keys)
        duplicate_value = normalized_media_identity(duplicate, keys)
        if target_value and target_value == duplicate_value:
            return True

    if str(category or "").strip().lower() == "adult":
        target_codes = extract_codes(media_haystack(target))
        duplicate_codes = extract_codes(media_haystack(duplicate))
        if target_codes and target_codes.intersection(duplicate_codes):
            return True

    compared_external_id = False
    for keys in (
        ("tmdb_id", "tmdbId"),
        ("bangumi_id", "bangumiId"),
        ("douban_id", "doubanId"),
        ("thetvdb_id", "thetvdbId", "tvdb_id", "tvdbId"),
    ):
        target_value = normalized_media_identity(target, keys)
        duplicate_value = normalized_media_identity(duplicate, keys)
        if not target_value or not duplicate_value:
            continue
        compared_external_id = True
        if target_value != duplicate_value:
            return False
    return compared_external_id


def normalized_media_identity(media, keys):
    value = media_first_value(media, keys)
    text = str(value or "").strip().lower()
    if text in ("", "0", "none", "null"):
        return ""
    return text


def prioritized_task_records(records):
    def sort_key(record):
        return (
            task_list_priority(record),
            -int(record.get("updated_at") or 0),
            -int(record.get("created_at") or 0),
        )

    return sorted(records or [], key=sort_key)


def build_bot(config=None):
    config = config or BotConfig.from_env()
    telegram = TelegramApi(config.token, timeout=config.telegram_timeout)
    store = CandidateStore(config.state_db_path)
    service = PipelineBotService(config, p115_cookie_provider=store.get_p115_cookie)
    internal_api = None
    if config.internal_api_enabled:
        from pipeline.internal_api import InternalApiServer

        internal_api = InternalApiServer(
            service=service,
            db_path=config.state_db_path,
            token=config.internal_api_token,
            host=config.internal_api_host,
            port=config.internal_api_port,
            workers=config.internal_api_workers,
            owner_workers=config.internal_api_owner_workers,
            search_ttl_seconds=config.internal_api_search_ttl_seconds,
        )
    return TelegramBot(config, telegram, store, service, internal_api=internal_api)


def main():
    try:
        build_bot().run_forever()
        return 0
    except (OSError, RuntimeError, TimeoutError, ValueError) as exc:
        print("error: %s" % exc, file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
