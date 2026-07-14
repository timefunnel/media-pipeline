import hmac
import json
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
from pipeline.openlist_utils import normalize_openlist_path
from pipeline.telegram_ui import task_from_submit_result


DEFAULT_API_HOST = "127.0.0.1"
DEFAULT_SEARCH_TTL_SECONDS = 15 * 60
MAX_JSON_BODY_BYTES = 1024 * 1024
OFFLINE_ACTIVE_STATUSES = {"submitted", "allocating", "downloading", "unknown", None, ""}
OFFLINE_SUCCESS_STATUS = "success"
OFFLINE_FAILED_STATUSES = {"failed", "cancelled", "canceled"}
FINAL_IMPORT_STATUSES = {"completed", "completed_with_warning", "failed", "canceled"}
RETRYABLE_IMPORT_STATUSES = {"completed_with_warning", "failed", "canceled"}
VALID_IMPORT_STATUSES = {"queued", "running", *FINAL_IMPORT_STATUSES}
VALID_SEARCH_SOURCES = {"default", "pansou", "bt4g"}
VALID_CATEGORIES = {"movie", "tv", "anime", "adult", "other"}
SYNC_STAGES = {"syncing", "scanning", "scraping", "subtitles"}


class ApiError(RuntimeError):
    def __init__(self, status, code, message, details=None):
        super().__init__(message)
        self.status = int(status)
        self.code = str(code)
        self.message = str(message)
        self.details = dict(details or {})


class WorkerStopping(RuntimeError):
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
        conn.execute("pragma journal_mode = wal")
        conn.execute("pragma foreign_keys = on")
        return conn

    def _init_schema(self):
        conn = self._connect()
        try:
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

    def claim_next_import(self, blocked_owners):
        conn = self._connect()
        try:
            conn.execute("begin immediate")
            rows = conn.execute(
                "select * from internal_api_imports where status = 'queued' order by created_at, id"
            ).fetchall()
            row = next((item for item in rows if item["owner_id"] not in blocked_owners), None)
            if row is None:
                conn.commit()
                return None
            now = int(time.time())
            cursor = conn.execute(
                """
                update internal_api_imports
                set status = 'running', stage = 'starting', attempt_count = attempt_count + 1,
                    started_at = ?, completed_at = null, updated_at = ?, error = null
                where id = ? and status = 'queued'
                """,
                (now, now, row["id"]),
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
            preserve_sync = bool(row["msg_media_id"] or result_task_is_offline_success(result))
            if preserve_sync:
                result = dict(result or {})
                task = dict(result.get("task") or {})
                task["msg_sync_status"] = "running"
                task["msg_error"] = None
                result["task"] = task
            else:
                result = None
            conn.execute(
                """
                update internal_api_imports
                set status = 'queued', stage = 'queued', result_json = ?, error = null,
                    info_hash = ?, msg_media_id = ?, cancel_requested = 0,
                    updated_at = ?, started_at = null, completed_at = null
                where id = ?
                """,
                (
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


class ImportTaskManager:
    def __init__(self, service, store, workers=3, owner_workers=2, poll_seconds=2.0):
        self.service = service
        self.store = store
        self.workers = max(1, int(workers))
        self.owner_workers = max(1, int(owner_workers))
        self.poll_seconds = max(0.01, float(poll_seconds))
        self._condition = threading.Condition()
        self._stop_event = threading.Event()
        self._threads = []
        self._active_by_owner = defaultdict(int)
        self._target_locks = {}
        self._target_locks_guard = threading.Lock()

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

    def stop(self):
        self._stop_event.set()
        with self._condition:
            self._condition.notify_all()
        for thread in list(self._threads):
            thread.join(timeout=5)
        self._threads = []

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
        existing = self.store.get_import_by_idempotency(owner_id, idempotency_key)
        if existing is not None:
            existing_request = existing["request"]
            incoming_identity = {
                "search_session_id": session_id,
                "candidate_id": candidate_id,
                "category": category,
                "target": target,
                "force_duplicate": force_duplicate,
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
        }
        self._check_duplicate(request)
        task, created = self.store.create_import(owner_id, idempotency_key, request)
        if created:
            self.notify()
        return task, created

    def _check_duplicate(self, request):
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
            if not (request.get("force_duplicate") and summary["can_force"]):
                raise ApiError(
                    409,
                    "duplicate_media",
                    "target MediaStationGo library already contains matching media",
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
        if current["status"] in RETRYABLE_IMPORT_STATUSES and not (
            current.get("msg_media_id") or result_task_is_offline_success(current.get("result"))
        ):
            self._check_duplicate(current["request"])
        task = self.store.retry_import(owner_id, import_id)
        self.notify()
        return task

    def _worker_loop(self):
        while not self._stop_event.is_set():
            task = self._claim_task()
            if task is None:
                with self._condition:
                    self._condition.wait(timeout=0.5)
                continue
            owner_id = task["owner_id"]
            try:
                self._execute_task(task)
            except WorkerStopping:
                self.store.requeue_running_import(task["id"])
            except ImportCanceled as exc:
                current = self.store.get_import(owner_id, task["id"])
                self.store.finish_import(
                    task["id"],
                    "canceled",
                    "canceled",
                    result=current.get("result"),
                    error=str(exc),
                    info_hash=current.get("info_hash"),
                    msg_media_id=current.get("msg_media_id"),
                )
            except Exception as exc:
                print("internal API import %s failed: %s" % (task["id"], exc), flush=True)
                current = self.store.get_import(owner_id, task["id"])
                self.store.finish_import(
                    task["id"],
                    "failed",
                    "failed",
                    result=current.get("result"),
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

    def _claim_task(self):
        with self._condition:
            blocked = {owner for owner, count in self._active_by_owner.items() if count >= self.owner_workers}
            task = self.store.claim_next_import(blocked)
            if task is not None:
                self._active_by_owner[task["owner_id"]] += 1
            return task

    def _execute_task(self, task):
        request = task["request"]
        result = dict(task.get("result") or {})
        category = request["category"]
        title = request["title"]
        info_hash = str(task.get("info_hash") or "").strip()
        offline_task = dict(result.get("task") or {})

        self._raise_if_stopping()
        self._raise_if_cancel_requested(task["owner_id"], task["id"], category, info_hash)
        if not info_hash:
            self.store.save_running(task["id"], "submitting", result=result)
            submit_result = self.service.submit(category, request["candidate"]["download_uri"])
            info_hash = first_submit_info_hash(submit_result)
            if not info_hash:
                raise RuntimeError("pipeline submit returned no info_hash")
            offline_task = task_from_submit_result(submit_result, info_hash)
            result.update({"submit": submit_result, "task": offline_task})
            self.store.save_running(task["id"], "submitted", result=result, info_hash=info_hash)

        while offline_task.get("status_name") != OFFLINE_SUCCESS_STATUS:
            status_name = offline_task.get("status_name")
            if status_name in OFFLINE_FAILED_STATUSES:
                raise RuntimeError("115 offline task ended with status: %s" % status_name)
            if status_name not in OFFLINE_ACTIVE_STATUSES:
                raise RuntimeError("115 offline task returned invalid status: %s" % (status_name or "-"))
            self._raise_if_stopping()
            self._raise_if_cancel_requested(task["owner_id"], task["id"], category, info_hash)
            self.store.save_running(task["id"], "waiting_download", result=result, info_hash=info_hash)
            offline_task = self.service.task_status(category, info_hash)
            if not isinstance(offline_task, dict):
                raise RuntimeError("pipeline task_status returned invalid response")
            offline_task = dict(offline_task)
            offline_task.setdefault("info_hash", info_hash)
            result["task"] = offline_task
            self.store.save_running(task["id"], "waiting_download", result=result, info_hash=info_hash)
            if offline_task.get("status_name") != OFFLINE_SUCCESS_STATUS:
                self._wait_or_stop()

        target = request["target"]
        self._raise_if_cancel_requested(task["owner_id"], task["id"], category, info_hash)
        target_lock = self._target_lock(target)
        self._acquire_target_lock(target_lock)
        try:
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

                self.store.save_running(task["id"], "syncing", result=result, info_hash=info_hash)
                try:
                    offline_task = self.service.sync_completed_task(
                        category,
                        title,
                        offline_task,
                        progress_callback=save_progress,
                        target=target,
                    )
                except Exception as exc:
                    current = self.store.get_import(task["owner_id"], task["id"])
                    media_id = str(current.get("msg_media_id") or "").strip()
                    if not media_id:
                        raise
                    warning_result = dict(current.get("result") or result)
                    warning_result["msg_media_id"] = media_id
                    warning_result["warnings"] = [str(exc)]
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
                    raise RuntimeError("MediaStationGo sync completed without msg_media_id")
                warnings = sync_warnings(offline_task)
                result["msg_media_id"] = media_id
                if warnings:
                    result["warnings"] = warnings
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
                    self.store.finish_import(
                        task["id"],
                        "completed",
                        "completed",
                        result=result,
                        info_hash=info_hash,
                        msg_media_id=media_id,
                    )
                return
        finally:
            target_lock.release()

    def _raise_if_cancel_requested(self, owner_id, import_id, category, info_hash):
        current = self.store.get_import(owner_id, import_id)
        if not current.get("cancel_requested"):
            return
        if info_hash:
            self.service.cancel_task(category, info_hash)
        raise ImportCanceled("import task canceled")

    def _wait_or_stop(self):
        if self._stop_event.wait(self.poll_seconds):
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


class InternalApiApplication:
    def __init__(self, service, store, manager):
        self.service = service
        self.store = store
        self.manager = manager

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
        try:
            if source == "pansou":
                result = self.service.search_pansou(query, limit=limit)
            elif source == "bt4g":
                result = self.service.search_bt4g(query, limit=limit)
            else:
                result = self.service.search(query, category, limit=limit)
        except ApiError:
            raise
        except Exception as exc:
            raise ApiError(502, "search_failed", str(exc))
        if not isinstance(result, list):
            raise ApiError(502, "search_failed", "pipeline search returned invalid response")
        for item in result:
            if not isinstance(item, dict):
                raise ApiError(502, "search_failed", "pipeline search returned an invalid candidate")
        metadata = dict(getattr(result, "metadata", {}) or {})
        try:
            capabilities = self.service.search_capabilities()
        except Exception as exc:
            raise ApiError(502, "capability_check_failed", str(exc))
        if not isinstance(capabilities, dict):
            raise ApiError(502, "capability_check_failed", "pipeline capabilities returned invalid response")
        metadata.update(
            {
                "source": source,
                "category": category,
                "selected_count": len(result),
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
        search_ttl_seconds=DEFAULT_SEARCH_TTL_SECONDS,
    ):
        self.token = require_text(token, "INTERNAL_API_TOKEN", max_length=1000)
        self.host = require_text(host, "INTERNAL_API_HOST", max_length=255)
        self.port = int(port)
        if self.port < 1 or self.port > 65535:
            raise ValueError("INTERNAL_API_PORT must be between 1 and 65535")
        self.store = InternalApiStore(db_path, search_ttl_seconds=search_ttl_seconds)
        self.manager = ImportTaskManager(service, self.store, workers=workers, owner_workers=owner_workers)
        self.application = InternalApiApplication(service, self.store, self.manager)
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
        self.manager.stop()

    def _handler_class(self):
        api_server = self

        class Handler(BaseHTTPRequestHandler):
            server_version = "MediaPipelineInternalApi/1"

            def do_GET(self):
                api_server._handle(self)

            def do_POST(self):
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


def optional_bool(value, label, default=False):
    if value is None:
        return bool(default)
    if not isinstance(value, bool):
        raise ApiError(400, "invalid_%s" % label, "%s must be a boolean" % label)
    return value


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
    if task.get("msg_scrape_status") not in {"success", "skipped"}:
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
        "submitting": "正在提交 115 任务",
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
