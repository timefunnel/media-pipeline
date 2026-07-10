import base64
import datetime as _datetime
import hmac
import html
import json
import os
import sqlite3
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from pipeline.task_state import STATUS_FAILED, STATUS_RUNNING, STATUS_SUCCESS, TASK_STATE


DEFAULT_ADMIN_WEB_HOST = "127.0.0.1"
DEFAULT_ADMIN_WEB_PORT = 18082
DEFAULT_ADMIN_WEB_MAX_TASKS = 2000
DEFAULT_ADMIN_WEB_BASE_PATH = ""
DEFAULT_ADMIN_WEB_AUTH_MODE = "none"
DEFAULT_ADMIN_WEB_MSG_BASE_URL = "http://127.0.0.1:18080/api"
DEFAULT_ADMIN_WEB_MSG_AUTH_CACHE_SECONDS = 60

CATEGORY_LABELS = {
    "movie": "电影",
    "tv": "剧集",
    "anime": "动漫",
    "adult": "成人",
    "other": "其他",
}

CONTENT_PROFILE_LABELS = {
    "general": "普通",
    "adult": "成人",
    "anime": "动漫",
}

STAGE_LABELS = (
    ("openlist_adult_format_status", "番号格式化"),
    ("openlist_trash_hide_status", "回收站隐藏"),
    ("openlist_clean_status", "OpenList隐藏"),
    ("openlist_adult_extra_hide_status", "成人附加隐藏"),
    ("msg_scan_status", "MSG扫描"),
    ("msg_scrape_status", "MSG刮削"),
    ("msg_extra_cleanup_status", "特典隐藏"),
    ("msg_visibility_repair_status", "可见性修复"),
    ("subtitle_match_status", "字幕匹配"),
)

ERROR_FIELDS = (
    ("openlist_adult_format_error", "番号格式化"),
    ("openlist_trash_hide_error", "回收站隐藏"),
    ("openlist_clean_error", "OpenList隐藏"),
    ("openlist_adult_extra_hide_error", "成人附加隐藏"),
    ("msg_error", "MSG"),
    ("msg_extra_cleanup_error", "特典隐藏"),
    ("msg_visibility_repair_error", "可见性修复"),
    ("subtitle_match_error", "字幕匹配"),
)


class AdminAuthError(Exception):
    def __init__(self, message, status=HTTPStatus.UNAUTHORIZED):
        super().__init__(message)
        self.status = status


class MsgTokenValidator:
    def __init__(
        self,
        base_url=DEFAULT_ADMIN_WEB_MSG_BASE_URL,
        timeout=5,
        require_admin=True,
        cache_seconds=DEFAULT_ADMIN_WEB_MSG_AUTH_CACHE_SECONDS,
    ):
        self.base_url = str(base_url or DEFAULT_ADMIN_WEB_MSG_BASE_URL).rstrip("/")
        self.timeout = max(1, int(timeout or 5))
        self.require_admin = bool(require_admin)
        self.cache_seconds = max(0, int(cache_seconds or 0))
        self._cache = {}
        self._lock = threading.Lock()

    def validate(self, token):
        token = str(token or "").strip()
        if not token:
            raise AdminAuthError("MSG token missing", HTTPStatus.UNAUTHORIZED)
        cached = self._cached(token)
        if cached is not None:
            return cached
        permissions = self._fetch_permissions(token)
        if self.require_admin and not msg_permissions_allow_admin(permissions):
            raise AdminAuthError("MSG admin permission required", HTTPStatus.FORBIDDEN)
        self._store_cache(token, permissions)
        return permissions

    def _cached(self, token):
        if self.cache_seconds <= 0:
            return None
        now = time.time()
        with self._lock:
            cached = self._cache.get(token)
            if cached and cached[0] > now:
                return cached[1]
            if cached:
                self._cache.pop(token, None)
        return None

    def _store_cache(self, token, permissions):
        if self.cache_seconds <= 0:
            return
        with self._lock:
            self._cache[token] = (time.time() + self.cache_seconds, permissions)

    def _fetch_permissions(self, token):
        url = self.base_url + "/auth/permissions"
        request = urllib.request.Request(
            url,
            headers={
                "Accept": "application/json",
                "Authorization": "Bearer %s" % token,
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                body = response.read()
        except urllib.error.HTTPError as exc:
            if exc.code in (401, 403):
                raise AdminAuthError("MSG token rejected", HTTPStatus.UNAUTHORIZED) from exc
            raise AdminAuthError("MSG auth check failed: HTTP %s" % exc.code, HTTPStatus.BAD_GATEWAY) from exc
        except urllib.error.URLError as exc:
            raise AdminAuthError("MSG auth check failed: %s" % exc.reason, HTTPStatus.BAD_GATEWAY) from exc
        try:
            payload = json.loads(body.decode("utf-8"))
        except (TypeError, ValueError, UnicodeDecodeError) as exc:
            raise AdminAuthError("MSG auth response is not valid JSON", HTTPStatus.BAD_GATEWAY) from exc
        return normalize_msg_permissions(payload)


def normalize_msg_permissions(payload):
    data = payload.get("data") if isinstance(payload, dict) else None
    if isinstance(data, dict):
        payload = data
    if not isinstance(payload, dict):
        return {}
    return {
        "permissions": payload.get("permissions") if isinstance(payload.get("permissions"), dict) else {},
        "role": str(payload.get("role") or ""),
        "tier": str(payload.get("tier") or ""),
        "is_super": bool(payload.get("is_super")),
    }


def msg_permissions_allow_admin(permissions):
    permissions = permissions or {}
    if permissions.get("is_super"):
        return True
    if permissions.get("role") == "admin":
        return True
    permission_map = permissions.get("permissions") or {}
    return bool(permission_map.get("admin") or permission_map.get("system.admin"))


def extract_bearer_token(header):
    value = str(header or "").strip()
    prefix = "Bearer "
    if not value.startswith(prefix):
        return ""
    return value[len(prefix) :].strip()


def normalize_base_path(value):
    value = str(value or "").strip()
    if not value or value == "/":
        return ""
    value = "/" + value.strip("/")
    return value


def admin_url(base_path, path="/"):
    base_path = normalize_base_path(base_path)
    path = "/" + str(path or "/").lstrip("/")
    if path == "/":
        return base_path + "/" if base_path else "/"
    return base_path + path


class AdminTaskStore:
    def __init__(self, db_path):
        self.db_path = str(db_path or "")

    def list_tasks(self, limit=DEFAULT_ADMIN_WEB_MAX_TASKS):
        if not self.db_path or not os.path.exists(self.db_path):
            return []
        rows = self._query(
            """
            select info_hash, user_id, chat_id, category, title, task_json, created_at, updated_at
            from offline_tasks
            order by updated_at desc, created_at desc
            limit ?
            """,
            (max(1, int(limit)),),
        )
        return [task_record_from_row(row) for row in rows]

    def load_task(self, info_hash):
        normalized = str(info_hash or "").strip()
        if not normalized:
            return None
        rows = self._query(
            """
            select info_hash, user_id, chat_id, category, title, task_json, created_at, updated_at
            from offline_tasks
            where lower(info_hash) = lower(?)
            limit 1
            """,
            (normalized,),
        )
        if not rows:
            return None
        return task_record_from_row(rows[0])

    def subtitle_summary(self):
        if not self.db_path or not os.path.exists(self.db_path):
            return {"total": 0, "success": 0, "failed": 0, "pending": 0}
        rows = self._query(
            """
            select status, count(*)
            from subtitle_backfill_index
            group by status
            """,
            (),
            missing_table_ok=True,
        )
        summary = {"total": 0, "success": 0, "failed": 0, "pending": 0}
        for status, count in rows:
            count = int(count or 0)
            summary["total"] += count
            if status == "success":
                summary["success"] += count
            elif status == "failed":
                summary["failed"] += count
            else:
                summary["pending"] += count
        return summary

    def _query(self, sql, params, missing_table_ok=False):
        if not self.db_path or not os.path.exists(self.db_path):
            return []
        uri = "file:%s?mode=ro" % urllib.parse.quote(os.path.abspath(self.db_path))
        conn = sqlite3.connect(uri, uri=True, timeout=10)
        try:
            try:
                return conn.execute(sql, params).fetchall()
            except sqlite3.OperationalError as exc:
                if missing_table_ok and "no such table" in str(exc).lower():
                    return []
                raise
        finally:
            conn.close()


def task_record_from_row(row):
    task = {}
    parse_error = ""
    try:
        task = json.loads(row[5] or "{}")
        if not isinstance(task, dict):
            parse_error = "task_json is not an object"
            task = {}
    except (TypeError, ValueError) as exc:
        parse_error = str(exc)
    if parse_error:
        task["_parse_error"] = parse_error
    return {
        "info_hash": row[0],
        "user_id": row[1],
        "chat_id": row[2],
        "category": row[3],
        "title": row[4],
        "task": task,
        "created_at": int(row[6] or 0),
        "updated_at": int(row[7] or 0),
    }


def filter_task_records(records, category="", status="", query=""):
    out = []
    normalized_query = str(query or "").strip().lower()
    for record in records or []:
        task = record.get("task") or {}
        if category and record.get("category") != category:
            continue
        if status and task_status_group(task) != status:
            continue
        if normalized_query and normalized_query not in task_search_text(record):
            continue
        out.append(record)
    return out


def task_search_text(record):
    task = record.get("task") or {}
    values = [
        record.get("info_hash"),
        record.get("title"),
        record.get("category"),
        task.get("name"),
        task.get("msg_media_title"),
        task.get("msg_media_id"),
        task.get("openlist_adult_code"),
        task.get("content_profile"),
    ]
    return "\n".join(str(value or "") for value in values).lower()


def task_status_group(task):
    task = task or {}
    if task.get("_parse_error"):
        return "failed"
    if TASK_STATE.sync_is_running(task) or TASK_STATE.is_offline_active(task):
        return "running"
    if task.get("msg_sync_status") == STATUS_FAILED:
        return "failed"
    if any(task.get(key) == STATUS_FAILED for key, _label in STAGE_LABELS):
        return "failed"
    if TASK_STATE.status_name(task) in ("failed", "cancelled"):
        return "failed"
    if TASK_STATE.is_offline_success(task) and task.get("msg_scrape_status") == STATUS_SUCCESS:
        return "success"
    if TASK_STATE.is_offline_success(task) and not task.get("msg_sync_status"):
        return "pending"
    if TASK_STATE.is_offline_success(task):
        return "running" if task.get("msg_sync_status") == STATUS_RUNNING else "pending"
    return "pending"


def summarize_tasks(records):
    summary = {
        "total": 0,
        "running": 0,
        "failed": 0,
        "success": 0,
        "pending": 0,
        "subtitle_failed": 0,
    }
    for record in records or []:
        summary["total"] += 1
        group = task_status_group(record.get("task"))
        summary[group] = summary.get(group, 0) + 1
        task = record.get("task") or {}
        if task.get("subtitle_match_status") == STATUS_FAILED:
            summary["subtitle_failed"] += 1
    return summary


def paginate(items, page=1, page_size=20):
    page_size = min(100, max(5, int(page_size or 20)))
    total = len(items or [])
    page_count = max(1, (total + page_size - 1) // page_size)
    page = min(max(1, int(page or 1)), page_count)
    start = (page - 1) * page_size
    return {
        "items": list(items or [])[start : start + page_size],
        "page": page,
        "page_size": page_size,
        "page_count": page_count,
        "total": total,
    }


def task_public_summary(record):
    task = record.get("task") or {}
    return {
        "info_hash": record.get("info_hash"),
        "category": record.get("category"),
        "category_label": category_label(record.get("category")),
        "title": display_task_title(record),
        "status": task_status_group(task),
        "offline_status": task.get("status_name"),
        "percent_done": task.get("percent_done"),
        "msg_sync_status": task.get("msg_sync_status"),
        "msg_media_id": task.get("msg_media_id"),
        "msg_media_title": task.get("msg_media_title"),
        "content_profile": task.get("content_profile"),
        "created_at": record.get("created_at"),
        "updated_at": record.get("updated_at"),
        "errors": task_errors(task),
    }


def display_task_title(record):
    task = record.get("task") or {}
    return str(record.get("title") or task.get("name") or task.get("msg_media_title") or record.get("info_hash") or "-")


def category_label(category):
    return CATEGORY_LABELS.get(category, category or "-")


def content_profile_label(value):
    return CONTENT_PROFILE_LABELS.get(value, value or "-")


def task_errors(task):
    task = task or {}
    out = []
    if task.get("_parse_error"):
        out.append({"stage": "任务数据", "error": task["_parse_error"]})
    for key, label in ERROR_FIELDS:
        value = str(task.get(key) or "").strip()
        if value:
            out.append({"stage": label, "error": value})
    return out


def task_stage_items(task):
    task = task or {}
    out = []
    for key, label in STAGE_LABELS:
        value = str(task.get(key) or "").strip()
        if value:
            out.append({"key": key, "label": label, "status": value})
    return out


def format_percent(value):
    if value in (None, ""):
        return "-"
    try:
        number = float(value)
    except (TypeError, ValueError):
        return str(value)
    if number <= 1:
        number *= 100
    return "%.1f%%" % number


def format_time(ts):
    try:
        ts = int(ts or 0)
    except (TypeError, ValueError):
        ts = 0
    if ts <= 0:
        return "-"
    return _datetime.datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")


def short_hash(value):
    value = str(value or "")
    if len(value) <= 14:
        return value
    return value[:8] + "..." + value[-6:]


def build_query(params, **updates):
    merged = dict(params or {})
    for key, value in updates.items():
        if value in (None, ""):
            merged.pop(key, None)
        else:
            merged[key] = str(value)
    return urllib.parse.urlencode(merged)


def render_dashboard(records, subtitle_summary, params, revision="unknown", base_path=""):
    summary = summarize_tasks(records["all"])
    page = records["page"]
    page_items = records["items"]
    filters = records["filters"]
    return html_page(
        title="Media Pipeline",
        body="""
<header class="topbar">
  <div>
    <p class="eyebrow">Media Pipeline</p>
    <h1>任务控制台</h1>
  </div>
  <div class="top-actions">
    <a class="button ghost" href="{api_url}">JSON</a>
    <span class="revision">rev {revision}</span>
  </div>
</header>
<section class="metrics" aria-label="任务统计">
  {metric_cards}
</section>
<main class="layout">
  <aside class="panel filters">
    <form method="get" action="{home_url}">
      <label>关键词<input name="q" value="{query}" placeholder="标题 / 番号 / info_hash"></label>
      <label>媒体库{category_select}</label>
      <label>状态{status_select}</label>
      <label>每页{page_size_select}</label>
      <button class="button" type="submit">筛选</button>
      <a class="button ghost" href="{home_url}">重置</a>
    </form>
  </aside>
  <section class="task-section">
    <div class="section-head">
      <div>
        <h2>最近任务</h2>
        <p>{total} 条结果，第 {page_no}/{page_count} 页</p>
      </div>
      {pager_top}
    </div>
    <div class="task-list">
      {task_cards}
    </div>
    {pager_bottom}
  </section>
</main>
""".format(
            api_url=escape(admin_url(base_path, "/api/tasks")),
            home_url=escape(admin_url(base_path, "/")),
            revision=escape(revision),
            metric_cards=render_metric_cards(summary, subtitle_summary),
            query=escape(filters.get("q")),
            category_select=render_select("category", filters.get("category"), category_options()),
            status_select=render_select("status", filters.get("status"), status_options()),
            page_size_select=render_select("page_size", str(page["page_size"]), page_size_options()),
            total=page["total"],
            page_no=page["page"],
            page_count=page["page_count"],
            pager_top=render_pager(params, page, base_path=base_path),
            task_cards=render_task_cards(page_items, base_path=base_path),
            pager_bottom=render_pager(params, page, base_path=base_path),
        ),
    )


def render_task_detail(record, revision="unknown", base_path=""):
    if record is None:
        return html_page(
            title="任务不存在",
            body='<main class="single"><section class="panel"><h1>任务不存在</h1><a class="button" href="%s">返回</a></section></main>' % escape(admin_url(base_path, "/")),
            status=HTTPStatus.NOT_FOUND,
        )
    task = record.get("task") or {}
    stages = task_stage_items(task)
    errors = task_errors(task)
    return html_page(
        title=display_task_title(record),
        body="""
<header class="topbar">
  <div>
    <p class="eyebrow">{category}</p>
    <h1>{title}</h1>
  </div>
  <div class="top-actions">
    <a class="button ghost" href="{home_url}">返回</a>
    <span class="revision">rev {revision}</span>
  </div>
</header>
<main class="detail-grid">
  <section class="panel">
    <h2>基本信息</h2>
    <dl class="kv">
      <div><dt>状态</dt><dd>{status_badge}</dd></div>
      <div><dt>115进度</dt><dd>{offline} / {percent}</dd></div>
      <div><dt>info_hash</dt><dd class="mono">{info_hash}</dd></div>
      <div><dt>内容</dt><dd>{profile}</dd></div>
      <div><dt>MSG媒体</dt><dd>{msg_media}</dd></div>
      <div><dt>更新时间</dt><dd>{updated_at}</dd></div>
    </dl>
  </section>
  <section class="panel">
    <h2>阶段</h2>
    <div class="stage-list">{stages}</div>
  </section>
  <section class="panel detail-wide">
    <h2>错误</h2>
    {errors}
  </section>
  <section class="panel detail-wide">
    <h2>原始任务</h2>
    <pre>{task_json}</pre>
  </section>
</main>
""".format(
            home_url=escape(admin_url(base_path, "/")),
            category=escape(category_label(record.get("category"))),
            title=escape(display_task_title(record)),
            revision=escape(revision),
            status_badge=render_status_badge(task_status_group(task)),
            offline=escape(task.get("status_name") or "-"),
            percent=escape(format_percent(task.get("percent_done"))),
            info_hash=escape(record.get("info_hash")),
            profile=escape(content_profile_label(task.get("content_profile"))),
            msg_media=render_msg_media(task),
            updated_at=escape(format_time(record.get("updated_at"))),
            stages=render_stage_list(stages),
            errors=render_errors(errors),
            task_json=escape(json.dumps(task, ensure_ascii=False, indent=2, sort_keys=True)),
        ),
    )


def render_metric_cards(summary, subtitle_summary):
    cards = [
        ("全部任务", summary.get("total", 0), "neutral"),
        ("进行中", summary.get("running", 0), "running"),
        ("失败", summary.get("failed", 0), "failed"),
        ("已完成", summary.get("success", 0), "success"),
        ("字幕已补", subtitle_summary.get("success", 0), "success"),
        ("字幕失败", subtitle_summary.get("failed", 0), "failed"),
    ]
    return "\n".join(
        '<article class="metric {tone}"><span>{label}</span><strong>{value}</strong></article>'.format(
            tone=escape(tone),
            label=escape(label),
            value=escape(value),
        )
        for label, value, tone in cards
    )


def render_task_cards(records, base_path=""):
    if not records:
        return '<div class="empty">没有符合条件的任务</div>'
    return "\n".join(render_task_card(record, base_path=base_path) for record in records)


def render_task_card(record, base_path=""):
    task = record.get("task") or {}
    info_hash = str(record.get("info_hash") or "")
    detail_url = admin_url(base_path, "/tasks/%s" % urllib.parse.quote(info_hash))
    errors = task_errors(task)
    error_line = ""
    if errors:
        error_line = '<p class="task-error">{}</p>'.format(escape(errors[0]["stage"] + "：" + errors[0]["error"]))
    return """
<article class="task-card">
  <div class="task-main">
    <div class="task-title-row">
      <h3>{title}</h3>
      {status}
    </div>
    <p class="task-meta">{category} · {profile} · {updated}</p>
    <p class="task-sub">115：{offline} · 进度：{percent} · MSG：{msg}</p>
    {error_line}
  </div>
  <div class="task-side">
    <span class="mono">{short_hash}</span>
    <a class="button ghost" href="{detail_url}">详情</a>
  </div>
</article>
""".format(
        title=escape(display_task_title(record)),
        status=render_status_badge(task_status_group(task)),
        category=escape(category_label(record.get("category"))),
        profile=escape(content_profile_label(task.get("content_profile"))),
        updated=escape(format_time(record.get("updated_at"))),
        offline=escape(task.get("status_name") or "-"),
        percent=escape(format_percent(task.get("percent_done"))),
        msg=escape(task.get("msg_sync_status") or task.get("msg_scrape_status") or "-"),
        error_line=error_line,
        short_hash=escape(short_hash(info_hash)),
        detail_url=escape(detail_url),
    )


def render_select(name, current, options):
    current = str(current or "")
    items = []
    for value, label in options:
        selected = " selected" if str(value) == current else ""
        items.append('<option value="{value}"{selected}>{label}</option>'.format(value=escape(value), selected=selected, label=escape(label)))
    return '<select name="{name}">{items}</select>'.format(name=escape(name), items="".join(items))


def category_options():
    return [("", "全部")] + [(key, label) for key, label in CATEGORY_LABELS.items()]


def status_options():
    return [
        ("", "全部"),
        ("running", "进行中"),
        ("failed", "失败"),
        ("pending", "待处理"),
        ("success", "已完成"),
    ]


def page_size_options():
    return [("10", "10"), ("20", "20"), ("50", "50"), ("100", "100")]


def render_pager(params, page, base_path=""):
    if page["page_count"] <= 1:
        return ""
    current = page["page"]
    prev_disabled = current <= 1
    next_disabled = current >= page["page_count"]
    prev_href = "%s?%s" % (admin_url(base_path, "/"), build_query(params, page=current - 1))
    next_href = "%s?%s" % (admin_url(base_path, "/"), build_query(params, page=current + 1))
    return """
<nav class="pager" aria-label="分页">
  {prev}
  <span>{current}/{page_count}</span>
  {next}
</nav>
""".format(
        prev='<span class="button disabled">上一页</span>' if prev_disabled else '<a class="button ghost" href="%s">上一页</a>' % escape(prev_href),
        current=current,
        page_count=page["page_count"],
        next='<span class="button disabled">下一页</span>' if next_disabled else '<a class="button ghost" href="%s">下一页</a>' % escape(next_href),
    )


def render_status_badge(status):
    labels = {
        "running": "进行中",
        "failed": "失败",
        "success": "已完成",
        "pending": "待处理",
    }
    return '<span class="badge {status}">{label}</span>'.format(status=escape(status), label=escape(labels.get(status, status or "-")))


def render_stage_list(stages):
    if not stages:
        return '<div class="empty inline">暂无阶段记录</div>'
    return "\n".join(
        '<div class="stage"><span>{label}</span>{badge}</div>'.format(
            label=escape(item["label"]),
            badge=render_stage_badge(item["status"]),
        )
        for item in stages
    )


def render_stage_badge(status):
    group = "pending"
    if status == "success" or status == "skipped":
        group = "success"
    elif status == "running":
        group = "running"
    elif status == "failed":
        group = "failed"
    return '<span class="badge {group}">{status}</span>'.format(group=escape(group), status=escape(status))


def render_errors(errors):
    if not errors:
        return '<div class="empty inline">暂无错误</div>'
    return '<div class="error-list">%s</div>' % "\n".join(
        '<div class="error-item"><strong>{stage}</strong><p>{error}</p></div>'.format(
            stage=escape(item["stage"]),
            error=escape(item["error"]),
        )
        for item in errors
    )


def render_msg_media(task):
    media_id = str(task.get("msg_media_id") or "").strip()
    title = str(task.get("msg_media_title") or "").strip()
    if not media_id and not title:
        return "-"
    if media_id:
        return '<span class="mono">{}</span>{}'.format(escape(media_id), " · " + escape(title) if title else "")
    return escape(title)


def render_msg_auth_shell(base_path="", revision="unknown"):
    script = (
        MSG_AUTH_APP_JS.replace("__BASE_PATH__", json.dumps(normalize_base_path(base_path)))
        .replace("__REVISION__", json.dumps(str(revision or "unknown")))
        .replace("__AUTH_KEY__", json.dumps("mediastationgo-auth"))
    )
    return html_page(
        title="Media Pipeline",
        body="""
<header class="topbar">
  <div>
    <p class="eyebrow">Media Pipeline</p>
    <h1>任务控制台</h1>
  </div>
  <div class="top-actions">
    <span class="revision">rev {revision}</span>
  </div>
</header>
<div id="admin-app" class="single">
  <section class="panel"><p class="loading-text">正在读取 MSG 登录态</p></section>
</div>
<script>{script}</script>
""".format(revision=escape(revision), script=script),
    )


def html_page(title, body, status=HTTPStatus.OK):
    return {
        "status": int(status),
        "headers": [("Content-Type", "text/html; charset=utf-8"), ("Cache-Control", "no-store")],
        "body": """<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{title}</title>
  <style>{css}</style>
</head>
<body>
{body}
</body>
</html>""".format(
            title=escape(title),
            css=CSS,
            body=body,
        ).encode("utf-8"),
    }


def json_page(payload, status=HTTPStatus.OK):
    return {
        "status": int(status),
        "headers": [("Content-Type", "application/json; charset=utf-8"), ("Cache-Control", "no-store")],
        "body": json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8"),
    }


def escape(value):
    return html.escape(str(value if value is not None else ""), quote=True)


class PipelineAdminHandler(BaseHTTPRequestHandler):
    server_version = "MediaPipelineAdmin/1.0"

    def do_GET(self):
        if self.server.auth_mode == "basic" and not self._authorized():
            self._send_auth_required()
            return
        parsed = urllib.parse.urlparse(self.path)
        try:
            response = self._route(parsed)
        except AdminAuthError as exc:
            response = self._auth_error_response(exc)
        except Exception as exc:
            response = html_page(
                "管理页面错误",
                '<main class="single"><section class="panel"><h1>管理页面错误</h1><pre>{}</pre></section></main>'.format(escape(exc)),
                status=HTTPStatus.INTERNAL_SERVER_ERROR,
            )
        self._write_response(response)

    def log_message(self, fmt, *args):
        if getattr(self.server, "quiet", False):
            return
        super().log_message(fmt, *args)

    def _route(self, parsed):
        path = self._strip_base_path(parsed.path or "/")
        params = first_query_values(urllib.parse.parse_qs(parsed.query, keep_blank_values=True))
        if path == "/healthz":
            return json_page({"ok": True, "time": int(time.time())})
        if self.server.auth_mode == "msg" and path in ("/", ""):
            return render_msg_auth_shell(base_path=self.server.base_path, revision=self.server.revision)
        if self.server.auth_mode == "msg" and path.startswith("/tasks/"):
            return render_msg_auth_shell(base_path=self.server.base_path, revision=self.server.revision)
        if path == "/api/tasks":
            self._require_msg_auth_if_needed()
            return self._api_tasks(params)
        if path.startswith("/api/tasks/"):
            self._require_msg_auth_if_needed()
            info_hash = urllib.parse.unquote(path[len("/api/tasks/") :])
            record = self.server.store.load_task(info_hash)
            if record is None:
                return json_page({"error": "task not found"}, status=HTTPStatus.NOT_FOUND)
            return json_page(task_public_summary(record) | {"task": record.get("task") or {}})
        if path.startswith("/tasks/"):
            info_hash = urllib.parse.unquote(path[len("/tasks/") :])
            return render_task_detail(self.server.store.load_task(info_hash), revision=self.server.revision, base_path=self.server.base_path)
        if path == "/":
            return self._dashboard(params)
        return html_page(
            "页面不存在",
            '<main class="single"><section class="panel"><h1>页面不存在</h1><a class="button" href="/">返回</a></section></main>',
            status=HTTPStatus.NOT_FOUND,
        )

    def _dashboard(self, params):
        all_records = self.server.store.list_tasks(limit=self.server.max_tasks)
        filtered = filter_task_records(
            all_records,
            category=params.get("category", ""),
            status=params.get("status", ""),
            query=params.get("q", ""),
        )
        page = paginate(filtered, page=params.get("page", 1), page_size=params.get("page_size", 20))
        records = {
            "all": all_records,
            "items": page["items"],
            "page": page,
            "filters": {
                "category": params.get("category", ""),
                "status": params.get("status", ""),
                "q": params.get("q", ""),
            },
        }
        return render_dashboard(records, self.server.store.subtitle_summary(), params, revision=self.server.revision, base_path=self.server.base_path)

    def _api_tasks(self, params):
        all_records = self.server.store.list_tasks(limit=self.server.max_tasks)
        filtered = filter_task_records(
            all_records,
            category=params.get("category", ""),
            status=params.get("status", ""),
            query=params.get("q", ""),
        )
        page = paginate(filtered, page=params.get("page", 1), page_size=params.get("page_size", 20))
        return json_page(
            {
                "summary": summarize_tasks(all_records),
                "page": {key: page[key] for key in ("page", "page_size", "page_count", "total")},
                "items": [task_public_summary(record) for record in page["items"]],
            }
        )

    def _authorized(self):
        username = getattr(self.server, "username", "")
        password = getattr(self.server, "password", "")
        if not username and not password:
            return True
        header = self.headers.get("Authorization", "")
        prefix = "Basic "
        if not header.startswith(prefix):
            return False
        try:
            decoded = base64.b64decode(header[len(prefix) :], validate=True).decode("utf-8")
        except (ValueError, UnicodeDecodeError):
            return False
        supplied_user, sep, supplied_password = decoded.partition(":")
        if not sep:
            return False
        return hmac.compare_digest(supplied_user, username) and hmac.compare_digest(supplied_password, password)

    def _require_msg_auth_if_needed(self):
        if self.server.auth_mode != "msg":
            return None
        token = extract_bearer_token(self.headers.get("Authorization"))
        return self.server.msg_validator.validate(token)

    def _strip_base_path(self, path):
        base_path = self.server.base_path
        if not base_path:
            return path or "/"
        if path == base_path:
            return "/"
        if path.startswith(base_path + "/"):
            return path[len(base_path) :] or "/"
        if path == "/" and self.server.auth_mode == "msg":
            return "/"
        raise AdminAuthError("admin web path not found", HTTPStatus.NOT_FOUND)

    def _auth_error_response(self, exc):
        status = getattr(exc, "status", HTTPStatus.UNAUTHORIZED)
        payload = {"error": str(exc)}
        if self.path.startswith(admin_url(self.server.base_path, "/api/")) or "/api/" in self.path:
            return json_page(payload, status=status)
        return html_page(
            "访问受限",
            '<main class="single"><section class="panel"><h1>访问受限</h1><p>%s</p></section></main>' % escape(exc),
            status=status,
        )

    def _send_auth_required(self):
        response = {
            "status": HTTPStatus.UNAUTHORIZED,
            "headers": [
                ("Content-Type", "text/plain; charset=utf-8"),
                ("WWW-Authenticate", 'Basic realm="Media Pipeline"'),
                ("Cache-Control", "no-store"),
            ],
            "body": "authentication required\n".encode("utf-8"),
        }
        self._write_response(response)

    def _write_response(self, response):
        self.send_response(int(response["status"]))
        for key, value in response.get("headers") or []:
            self.send_header(key, value)
        self.end_headers()
        self.wfile.write(response.get("body") or b"")


class PipelineAdminServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(
        self,
        server_address,
        RequestHandlerClass,
        store,
        revision="unknown",
        username="",
        password="",
        max_tasks=DEFAULT_ADMIN_WEB_MAX_TASKS,
        quiet=False,
        base_path=DEFAULT_ADMIN_WEB_BASE_PATH,
        auth_mode=DEFAULT_ADMIN_WEB_AUTH_MODE,
        msg_validator=None,
    ):
        super().__init__(server_address, RequestHandlerClass)
        self.store = store
        self.revision = revision
        self.username = username
        self.password = password
        self.max_tasks = max(1, int(max_tasks or DEFAULT_ADMIN_WEB_MAX_TASKS))
        self.quiet = quiet
        self.base_path = normalize_base_path(base_path)
        self.auth_mode = normalize_auth_mode(auth_mode, username=username, password=password)
        self.msg_validator = msg_validator or MsgTokenValidator()


def first_query_values(query):
    return {key: values[-1] if values else "" for key, values in (query or {}).items()}


def normalize_auth_mode(value, username="", password=""):
    value = str(value or "").strip().lower()
    if value in ("msg", "basic", "none"):
        return value
    if username or password:
        return "basic"
    return "none"


def run_admin_web(
    host=DEFAULT_ADMIN_WEB_HOST,
    port=DEFAULT_ADMIN_WEB_PORT,
    state_db_path="/bot-data/state.db",
    username="",
    password="",
    max_tasks=DEFAULT_ADMIN_WEB_MAX_TASKS,
    revision="unknown",
    quiet=False,
    base_path=DEFAULT_ADMIN_WEB_BASE_PATH,
    auth_mode=DEFAULT_ADMIN_WEB_AUTH_MODE,
    msg_base_url=DEFAULT_ADMIN_WEB_MSG_BASE_URL,
    msg_auth_cache_seconds=DEFAULT_ADMIN_WEB_MSG_AUTH_CACHE_SECONDS,
):
    auth_mode = normalize_auth_mode(auth_mode, username=username, password=password)
    server = PipelineAdminServer(
        (host, int(port)),
        PipelineAdminHandler,
        store=AdminTaskStore(state_db_path),
        revision=revision,
        username=username,
        password=password,
        max_tasks=max_tasks,
        quiet=quiet,
        base_path=base_path,
        auth_mode=auth_mode,
        msg_validator=MsgTokenValidator(base_url=msg_base_url, cache_seconds=msg_auth_cache_seconds),
    )
    print(
        "media-pipeline admin web listening on http://%s:%s%s auth=%s"
        % (host, int(port), normalize_base_path(base_path) or "/", auth_mode),
        flush=True,
    )
    server.serve_forever()


CSS = """
:root {
  color-scheme: light;
  --bg: #f5f7fb;
  --panel: #ffffff;
  --text: #172033;
  --muted: #667085;
  --line: #d8dee9;
  --accent: #2563eb;
  --accent-weak: #e7efff;
  --success: #0f766e;
  --success-bg: #dcf8f1;
  --failed: #b42318;
  --failed-bg: #fee4e2;
  --running: #8a4b00;
  --running-bg: #fff1cc;
  --pending: #475467;
  --pending-bg: #eef2f6;
  font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
}
* { box-sizing: border-box; }
body {
  margin: 0;
  min-height: 100vh;
  background: var(--bg);
  color: var(--text);
  line-height: 1.5;
}
a { color: inherit; }
.topbar {
  display: flex;
  align-items: flex-end;
  justify-content: space-between;
  gap: 16px;
  padding: 24px;
  max-width: 1240px;
  margin: 0 auto;
}
.eyebrow {
  margin: 0 0 4px;
  color: var(--muted);
  font-size: 13px;
}
h1, h2, h3, p { margin-top: 0; }
h1 { margin-bottom: 0; font-size: 30px; line-height: 1.15; }
h2 { margin-bottom: 8px; font-size: 18px; }
h3 { margin-bottom: 0; font-size: 16px; overflow-wrap: anywhere; }
.top-actions { display: flex; align-items: center; gap: 10px; flex-wrap: wrap; justify-content: flex-end; }
.revision { color: var(--muted); font-size: 13px; }
.metrics {
  max-width: 1240px;
  margin: 0 auto;
  padding: 0 24px 16px;
  display: grid;
  grid-template-columns: repeat(6, minmax(0, 1fr));
  gap: 10px;
}
.metric {
  background: var(--panel);
  border: 1px solid var(--line);
  border-radius: 8px;
  padding: 14px;
  min-width: 0;
}
.metric span { display: block; color: var(--muted); font-size: 12px; }
.metric strong { display: block; font-size: 24px; line-height: 1.2; }
.layout {
  max-width: 1240px;
  margin: 0 auto;
  padding: 0 24px 32px;
  display: grid;
  grid-template-columns: 280px minmax(0, 1fr);
  gap: 16px;
  align-items: start;
}
.panel {
  background: var(--panel);
  border: 1px solid var(--line);
  border-radius: 8px;
  padding: 16px;
}
.filters {
  position: sticky;
  top: 12px;
}
form { display: grid; gap: 12px; }
label { display: grid; gap: 6px; color: var(--muted); font-size: 13px; }
input, select {
  width: 100%;
  min-height: 44px;
  border: 1px solid var(--line);
  border-radius: 8px;
  padding: 0 12px;
  background: #fff;
  color: var(--text);
  font: inherit;
}
.button {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  min-height: 44px;
  padding: 0 14px;
  border-radius: 8px;
  border: 1px solid var(--accent);
  background: var(--accent);
  color: #fff;
  text-decoration: none;
  font-weight: 650;
  cursor: pointer;
}
.button.ghost {
  background: var(--panel);
  color: var(--accent);
}
.button.disabled {
  border-color: var(--line);
  background: var(--pending-bg);
  color: var(--muted);
}
.section-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  margin-bottom: 10px;
}
.section-head p { margin: 0; color: var(--muted); }
.task-list { display: grid; gap: 10px; }
.task-card {
  display: grid;
  grid-template-columns: minmax(0, 1fr) auto;
  gap: 12px;
  align-items: center;
  background: var(--panel);
  border: 1px solid var(--line);
  border-radius: 8px;
  padding: 14px;
}
.task-title-row {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 10px;
}
.task-meta, .task-sub, .task-error {
  margin: 6px 0 0;
  color: var(--muted);
  font-size: 13px;
  overflow-wrap: anywhere;
}
.task-error { color: var(--failed); }
.task-side {
  display: grid;
  gap: 8px;
  justify-items: end;
}
.badge {
  display: inline-flex;
  align-items: center;
  min-height: 28px;
  padding: 0 9px;
  border-radius: 999px;
  font-size: 12px;
  font-weight: 700;
  white-space: nowrap;
}
.badge.success, .metric.success { color: var(--success); background: var(--success-bg); border-color: #99ead7; }
.badge.failed, .metric.failed { color: var(--failed); background: var(--failed-bg); border-color: #f7b2ad; }
.badge.running, .metric.running { color: var(--running); background: var(--running-bg); border-color: #f7d67a; }
.badge.pending, .metric.neutral, .metric.pending { color: var(--pending); background: var(--pending-bg); border-color: var(--line); }
.mono {
  font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, "Liberation Mono", monospace;
  font-size: 12px;
  overflow-wrap: anywhere;
}
.pager {
  display: flex;
  align-items: center;
  justify-content: flex-end;
  gap: 8px;
  margin: 12px 0;
}
.empty {
  background: var(--panel);
  border: 1px dashed var(--line);
  border-radius: 8px;
  padding: 24px;
  color: var(--muted);
  text-align: center;
}
.empty.inline {
  padding: 12px;
  text-align: left;
}
.single, .detail-grid {
  max-width: 1240px;
  margin: 0 auto;
  padding: 0 24px 32px;
}
.detail-grid {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 16px;
}
.detail-wide { grid-column: 1 / -1; }
.kv {
  display: grid;
  gap: 12px;
  margin: 0;
}
.kv div {
  display: grid;
  grid-template-columns: 120px minmax(0, 1fr);
  gap: 12px;
}
.kv dt { color: var(--muted); }
.kv dd { margin: 0; overflow-wrap: anywhere; }
.stage-list { display: grid; gap: 8px; }
.stage {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  min-height: 40px;
  border-bottom: 1px solid var(--line);
}
.stage:last-child { border-bottom: 0; }
.error-list { display: grid; gap: 10px; }
.error-item {
  border: 1px solid #f7b2ad;
  background: var(--failed-bg);
  border-radius: 8px;
  padding: 12px;
}
.error-item p { margin: 4px 0 0; overflow-wrap: anywhere; }
pre {
  margin: 0;
  white-space: pre-wrap;
  overflow-wrap: anywhere;
  font-size: 12px;
  line-height: 1.45;
}
@media (max-width: 900px) {
  .topbar {
    align-items: flex-start;
    padding: 18px 14px;
    flex-direction: column;
  }
  .top-actions { justify-content: flex-start; }
  h1 { font-size: 25px; }
  .metrics {
    padding: 0 14px 12px;
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }
  .layout {
    padding: 0 14px 24px;
    grid-template-columns: 1fr;
  }
  .filters { position: static; }
  .section-head {
    align-items: flex-start;
    flex-direction: column;
  }
  .task-card {
    grid-template-columns: 1fr;
  }
  .task-title-row {
    align-items: flex-start;
    flex-direction: column;
  }
  .task-side {
    grid-template-columns: 1fr auto;
    justify-items: start;
    align-items: center;
  }
  .pager { justify-content: stretch; }
  .pager .button { flex: 1; }
  .detail-grid {
    padding: 0 14px 24px;
    grid-template-columns: 1fr;
  }
  .single { padding: 0 14px 24px; }
  .kv div { grid-template-columns: 1fr; gap: 2px; }
}
@media (max-width: 420px) {
  .metrics { grid-template-columns: 1fr; }
  .task-side { grid-template-columns: 1fr; }
  .button { width: 100%; }
}
"""


MSG_AUTH_APP_JS = r"""
(() => {
  const BASE_PATH = __BASE_PATH__;
  const REVISION = __REVISION__;
  const AUTH_KEY = __AUTH_KEY__;
  const app = document.getElementById("admin-app");

  function pathOf(route) {
    const clean = "/" + String(route || "/").replace(/^\/+/, "");
    if (clean === "/") return BASE_PATH ? BASE_PATH + "/" : "/";
    return BASE_PATH + clean;
  }

  function escapeHtml(value) {
    return String(value ?? "").replace(/[&<>"']/g, (ch) => ({
      "&": "&amp;",
      "<": "&lt;",
      ">": "&gt;",
      '"': "&quot;",
      "'": "&#39;"
    }[ch]));
  }

  function readToken() {
    const raw = window.localStorage.getItem(AUTH_KEY);
    if (!raw) return "";
    try {
      const parsed = JSON.parse(raw);
      const state = parsed && parsed.state ? parsed.state : parsed;
      return state && state.token ? String(state.token) : "";
    } catch {
      return "";
    }
  }

  async function apiJson(route) {
    const token = readToken();
    if (!token) {
      throw new Error("请先登录 MediaStationGo 管理账号");
    }
    const response = await fetch(pathOf(route), {
      headers: { "Authorization": "Bearer " + token, "Accept": "application/json" },
      cache: "no-store"
    });
    if (response.status === 401) throw new Error("MSG 登录态已失效，请刷新 MSG 后重新登录");
    if (response.status === 403) throw new Error("当前 MSG 用户没有管理权限");
    if (!response.ok) throw new Error("请求失败：HTTP " + response.status);
    return response.json();
  }

  function statusBadge(status) {
    const labels = { running: "进行中", failed: "失败", success: "已完成", pending: "待处理" };
    return `<span class="badge ${escapeHtml(status || "pending")}">${escapeHtml(labels[status] || status || "-")}</span>`;
  }

  function metric(label, value, tone) {
    return `<article class="metric ${tone}"><span>${escapeHtml(label)}</span><strong>${escapeHtml(value)}</strong></article>`;
  }

  function renderMetrics(summary) {
    return [
      metric("全部任务", summary.total || 0, "neutral"),
      metric("进行中", summary.running || 0, "running"),
      metric("失败", summary.failed || 0, "failed"),
      metric("已完成", summary.success || 0, "success"),
      metric("字幕失败", summary.subtitle_failed || 0, "failed")
    ].join("");
  }

  function currentPath() {
    const path = window.location.pathname;
    if (BASE_PATH && path.startsWith(BASE_PATH)) return path.slice(BASE_PATH.length) || "/";
    return path || "/";
  }

  function formValue(params, key) {
    return escapeHtml(params.get(key) || "");
  }

  function renderFilters(params) {
    const category = params.get("category") || "";
    const status = params.get("status") || "";
    const pageSize = params.get("page_size") || "20";
    const option = (value, label, current) => `<option value="${escapeHtml(value)}"${value === current ? " selected" : ""}>${escapeHtml(label)}</option>`;
    return `
      <aside class="panel filters">
        <form id="filter-form">
          <label>关键词<input name="q" value="${formValue(params, "q")}" placeholder="标题 / 番号 / info_hash"></label>
          <label>媒体库<select name="category">
            ${option("", "全部", category)}
            ${option("movie", "电影", category)}
            ${option("tv", "剧集", category)}
            ${option("anime", "动漫", category)}
            ${option("adult", "成人", category)}
            ${option("other", "其他", category)}
          </select></label>
          <label>状态<select name="status">
            ${option("", "全部", status)}
            ${option("running", "进行中", status)}
            ${option("failed", "失败", status)}
            ${option("pending", "待处理", status)}
            ${option("success", "已完成", status)}
          </select></label>
          <label>每页<select name="page_size">
            ${["10", "20", "50", "100"].map(v => option(v, v, pageSize)).join("")}
          </select></label>
          <button class="button" type="submit">筛选</button>
          <a class="button ghost" href="${pathOf("/")}">重置</a>
        </form>
      </aside>`;
  }

  function renderPager(page, params) {
    if (!page || page.page_count <= 1) return "";
    const prev = new URLSearchParams(params);
    const next = new URLSearchParams(params);
    prev.set("page", String(Math.max(1, page.page - 1)));
    next.set("page", String(Math.min(page.page_count, page.page + 1)));
    const prevHtml = page.page <= 1 ? '<span class="button disabled">上一页</span>' : `<a class="button ghost" href="${pathOf("/")}?${prev.toString()}">上一页</a>`;
    const nextHtml = page.page >= page.page_count ? '<span class="button disabled">下一页</span>' : `<a class="button ghost" href="${pathOf("/")}?${next.toString()}">下一页</a>`;
    return `<nav class="pager" aria-label="分页">${prevHtml}<span>${page.page}/${page.page_count}</span>${nextHtml}</nav>`;
  }

  function renderTaskCard(item) {
    const detailUrl = pathOf("/tasks/" + encodeURIComponent(item.info_hash || ""));
    const errors = Array.isArray(item.errors) ? item.errors : [];
    const error = errors.length ? `<p class="task-error">${escapeHtml(errors[0].stage)}：${escapeHtml(errors[0].error)}</p>` : "";
    const shortHash = String(item.info_hash || "").length > 14 ? String(item.info_hash).slice(0, 8) + "..." + String(item.info_hash).slice(-6) : String(item.info_hash || "");
    return `
      <article class="task-card">
        <div class="task-main">
          <div class="task-title-row"><h3>${escapeHtml(item.title || "-")}</h3>${statusBadge(item.status)}</div>
          <p class="task-meta">${escapeHtml(item.category_label || item.category || "-")} · ${escapeHtml(item.content_profile || "-")} · ${escapeHtml(formatTime(item.updated_at))}</p>
          <p class="task-sub">115：${escapeHtml(item.offline_status || "-")} · 进度：${escapeHtml(formatPercent(item.percent_done))} · MSG：${escapeHtml(item.msg_sync_status || "-")}</p>
          ${error}
        </div>
        <div class="task-side">
          <span class="mono">${escapeHtml(shortHash)}</span>
          <a class="button ghost" href="${detailUrl}">详情</a>
        </div>
      </article>`;
  }

  function formatPercent(value) {
    if (value === null || value === undefined || value === "") return "-";
    const number = Number(value);
    if (!Number.isFinite(number)) return String(value);
    return (number <= 1 ? number * 100 : number).toFixed(1) + "%";
  }

  function formatTime(value) {
    const number = Number(value || 0);
    if (!number) return "-";
    return new Date(number * 1000).toLocaleString();
  }

  async function loadDashboard() {
    const params = new URLSearchParams(window.location.search);
    const data = await apiJson("/api/tasks" + (params.toString() ? "?" + params.toString() : ""));
    const page = data.page || { page: 1, page_count: 1, total: 0 };
    const items = Array.isArray(data.items) ? data.items : [];
    app.className = "";
    app.innerHTML = `
      <section class="metrics" aria-label="任务统计">${renderMetrics(data.summary || {})}</section>
      <main class="layout">
        ${renderFilters(params)}
        <section class="task-section">
          <div class="section-head">
            <div><h2>最近任务</h2><p>${escapeHtml(page.total || 0)} 条结果，第 ${escapeHtml(page.page || 1)}/${escapeHtml(page.page_count || 1)} 页</p></div>
            ${renderPager(page, params)}
          </div>
          <div class="task-list">${items.length ? items.map(renderTaskCard).join("") : '<div class="empty">没有符合条件的任务</div>'}</div>
          ${renderPager(page, params)}
        </section>
      </main>`;
    document.getElementById("filter-form").addEventListener("submit", (event) => {
      event.preventDefault();
      const next = new URLSearchParams(new FormData(event.currentTarget));
      next.delete("page");
      for (const key of Array.from(next.keys())) {
        if (!next.get(key)) next.delete(key);
      }
      window.location.assign(pathOf("/") + (next.toString() ? "?" + next.toString() : ""));
    });
  }

  function renderStageList(task) {
    const labels = [
      ["openlist_adult_format_status", "番号格式化"],
      ["openlist_trash_hide_status", "回收站隐藏"],
      ["openlist_clean_status", "OpenList隐藏"],
      ["openlist_adult_extra_hide_status", "成人附加隐藏"],
      ["msg_scan_status", "MSG扫描"],
      ["msg_scrape_status", "MSG刮削"],
      ["msg_extra_cleanup_status", "特典隐藏"],
      ["msg_visibility_repair_status", "可见性修复"],
      ["subtitle_match_status", "字幕匹配"]
    ];
    const rows = labels.filter(([key]) => task && task[key]).map(([key, label]) => `<div class="stage"><span>${escapeHtml(label)}</span>${statusBadge(task[key])}</div>`);
    return rows.length ? rows.join("") : '<div class="empty inline">暂无阶段记录</div>';
  }

  async function loadDetail(infoHash) {
    const data = await apiJson("/api/tasks/" + encodeURIComponent(infoHash));
    const task = data.task || {};
    const errors = Array.isArray(data.errors) ? data.errors : [];
    app.className = "detail-grid";
    app.innerHTML = `
      <section class="panel">
        <h2>基本信息</h2>
        <dl class="kv">
          <div><dt>状态</dt><dd>${statusBadge(data.status)}</dd></div>
          <div><dt>115进度</dt><dd>${escapeHtml(data.offline_status || "-")} / ${escapeHtml(formatPercent(data.percent_done))}</dd></div>
          <div><dt>info_hash</dt><dd class="mono">${escapeHtml(data.info_hash || "")}</dd></div>
          <div><dt>MSG媒体</dt><dd>${escapeHtml(data.msg_media_id || "-")} ${escapeHtml(data.msg_media_title || "")}</dd></div>
          <div><dt>更新时间</dt><dd>${escapeHtml(formatTime(data.updated_at))}</dd></div>
        </dl>
      </section>
      <section class="panel"><h2>阶段</h2><div class="stage-list">${renderStageList(task)}</div></section>
      <section class="panel detail-wide"><h2>错误</h2>${errors.length ? errors.map(e => `<div class="error-item"><strong>${escapeHtml(e.stage)}</strong><p>${escapeHtml(e.error)}</p></div>`).join("") : '<div class="empty inline">暂无错误</div>'}</section>
      <section class="panel detail-wide"><h2>原始任务</h2><pre>${escapeHtml(JSON.stringify(task, null, 2))}</pre></section>`;
  }

  async function main() {
    try {
      const path = currentPath();
      if (path.startsWith("/tasks/")) {
        await loadDetail(decodeURIComponent(path.slice("/tasks/".length)));
      } else {
        await loadDashboard();
      }
    } catch (error) {
      app.className = "single";
      app.innerHTML = `<section class="panel"><h2>无法加载</h2><p class="task-error">${escapeHtml(error.message || error)}</p><a class="button ghost" href="/">返回 MSG</a></section>`;
    }
  }

  main();
})();
"""
