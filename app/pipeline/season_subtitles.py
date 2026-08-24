import json
import re
import sqlite3
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

FINAL_SEASON_SUBTITLE_STATUSES = {"completed", "failed"}
ACTIVE_SEASON_SUBTITLE_STATUSES = {"queued", "running"}
MAX_SEASON_SUBTITLE_EPISODES = 500
SEASON_SUBTITLE_DOWNLOAD_WORKERS = 2


class SeasonSubtitleTaskStore:
    def __init__(self, db_path):
        self.db_path = str(db_path)
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    def _connect(self):
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("pragma busy_timeout = 30000")
        return conn

    def _init_schema(self):
        conn = self._connect()
        try:
            conn.execute("pragma journal_mode = wal")
            conn.execute(
                """
                create table if not exists internal_api_season_subtitle_tasks (
                    id text primary key,
                    owner_id text not null,
                    media_id text not null,
                    season integer not null,
                    candidate_json text not null,
                    targets_json text not null,
                    details_json text not null default '[]',
                    status text not null,
                    stage text not null,
                    current_episode text not null default '',
                    error text not null default '',
                    created_at integer not null,
                    updated_at integer not null,
                    started_at integer,
                    completed_at integer
                )
                """
            )
            conn.execute(
                """
                create index if not exists idx_internal_api_season_subtitle_tasks_owner
                on internal_api_season_subtitle_tasks(owner_id, media_id, season, updated_at)
                """
            )
            conn.execute(
                """
                create index if not exists idx_internal_api_season_subtitle_tasks_queue
                on internal_api_season_subtitle_tasks(status, created_at)
                """
            )
            conn.commit()
        finally:
            conn.close()

    def create_task(self, owner_id, media_id, season, selection, targets):
        now = int(time.time())
        task_id = uuid.uuid4().hex
        conn = self._connect()
        try:
            conn.execute("begin immediate")
            existing = conn.execute(
                """
                select * from internal_api_season_subtitle_tasks
                where owner_id = ? and media_id = ? and season = ?
                  and status in ('queued', 'running')
                order by created_at desc
                limit 1
                """,
                (owner_id, media_id, season),
            ).fetchone()
            if existing is not None:
                conn.commit()
                return season_subtitle_task_row(existing), False
            conn.execute(
                """
                insert into internal_api_season_subtitle_tasks
                    (id, owner_id, media_id, season, candidate_json, targets_json,
                     status, stage, created_at, updated_at)
                values (?, ?, ?, ?, ?, ?, 'queued', 'queued', ?, ?)
                """,
                (
                    task_id,
                    owner_id,
                    media_id,
                    season,
                    json_dumps(selection),
                    json_dumps(targets),
                    now,
                    now,
                ),
            )
            row = conn.execute(
                "select * from internal_api_season_subtitle_tasks where id = ?", (task_id,)
            ).fetchone()
            conn.commit()
            return season_subtitle_task_row(row), True
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def get_task(self, owner_id, task_id):
        return self._get_task(owner_id, task_id, include_internal=False)

    def get_task_internal(self, owner_id, task_id):
        return self._get_task(owner_id, task_id, include_internal=True)

    def _get_task(self, owner_id, task_id, include_internal):
        conn = self._connect()
        try:
            row = conn.execute(
                """
                select * from internal_api_season_subtitle_tasks
                where id = ? and owner_id = ?
                """,
                (task_id, owner_id),
            ).fetchone()
        finally:
            conn.close()
        if row is None:
            raise RuntimeError("season subtitle task not found")
        return season_subtitle_task_row(row, include_internal=include_internal)

    def recover_running_tasks(self):
        now = int(time.time())
        conn = self._connect()
        try:
            cursor = conn.execute(
                """
                update internal_api_season_subtitle_tasks
                set status = 'failed', stage = 'failed',
                    error = 'pipeline restarted while the season subtitle task was running; start a new task to retry',
                    updated_at = ?, completed_at = ?
                where status = 'running'
                """,
                (now, now),
            )
            conn.commit()
            return int(cursor.rowcount or 0)
        finally:
            conn.close()

    def claim_next_task(self):
        conn = self._connect()
        try:
            conn.execute("begin immediate")
            row = conn.execute(
                """
                select * from internal_api_season_subtitle_tasks
                where status = 'queued'
                order by created_at, id
                limit 1
                """
            ).fetchone()
            if row is None:
                conn.commit()
                return None
            now = int(time.time())
            cursor = conn.execute(
                """
                update internal_api_season_subtitle_tasks
                set status = 'running', stage = 'download',
                    error = '', started_at = ?, completed_at = null, updated_at = ?
                where id = ? and status = 'queued'
                """,
                (now, now, row["id"]),
            )
            if cursor.rowcount != 1:
                conn.commit()
                return None
            claimed = conn.execute(
                "select * from internal_api_season_subtitle_tasks where id = ?", (row["id"],)
            ).fetchone()
            conn.commit()
            return season_subtitle_task_row(claimed, include_internal=True)
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def save_stage(self, task_id, stage, current_episode=""):
        now = int(time.time())
        conn = self._connect()
        try:
            conn.execute(
                """
                update internal_api_season_subtitle_tasks
                set stage = ?, current_episode = ?, updated_at = ?
                where id = ? and status = 'running'
                """,
                (str(stage), str(current_episode), now, task_id),
            )
            conn.commit()
        finally:
            conn.close()

    def save_episode_result(self, task_id, target, status, count=0, error=""):
        if status not in {"success", "skipped", "failed"}:
            raise RuntimeError("season subtitle episode status invalid")
        media_id = str((target or {}).get("media_id") or "").strip()
        episode_key = str((target or {}).get("episode_key") or "").strip()
        if not media_id or not episode_key:
            raise RuntimeError("season subtitle episode target invalid")
        conn = self._connect()
        try:
            conn.execute("begin immediate")
            row = conn.execute(
                "select details_json from internal_api_season_subtitle_tasks where id = ?",
                (task_id,),
            ).fetchone()
            if row is None:
                raise RuntimeError("season subtitle task not found")
            details = json_loads_list(row["details_json"])
            details = [
                item for item in details
                if str(item.get("media_id") or "") != media_id
            ]
            details.append(
                {
                    "media_id": media_id,
                    "episode_key": episode_key,
                    "status": status,
                    "count": max(0, int(count or 0)),
                    "error": str(error or ""),
                }
            )
            now = int(time.time())
            conn.execute(
                """
                update internal_api_season_subtitle_tasks
                set details_json = ?, current_episode = ?, updated_at = ?
                where id = ? and status = 'running'
                """,
                (json_dumps(details), episode_key, now, task_id),
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def save_retry_result(self, task_id, target, status, count=0, error=""):
        if status not in {"success", "skipped", "failed"}:
            raise RuntimeError("season subtitle episode status invalid")
        media_id = str((target or {}).get("media_id") or "").strip()
        episode_key = str((target or {}).get("episode_key") or "").strip()
        if not media_id or not episode_key:
            raise RuntimeError("season subtitle episode target invalid")
        conn = self._connect()
        try:
            conn.execute("begin immediate")
            row = conn.execute(
                "select details_json from internal_api_season_subtitle_tasks where id = ?",
                (task_id,),
            ).fetchone()
            if row is None:
                raise RuntimeError("season subtitle parent task not found")
            details = json_loads_list(row["details_json"])
            if not any(str(item.get("media_id") or "") == media_id for item in details):
                raise RuntimeError("season subtitle parent task does not contain retry target")
            details = [
                item for item in details
                if str(item.get("media_id") or "") != media_id
            ]
            details.append(
                {
                    "media_id": media_id,
                    "episode_key": episode_key,
                    "status": status,
                    "count": max(0, int(count or 0)),
                    "error": str(error or ""),
                }
            )
            conn.execute(
                """
                update internal_api_season_subtitle_tasks
                set details_json = ?, updated_at = ?
                where id = ?
                """,
                (json_dumps(details), int(time.time()), task_id),
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def finish_task(self, task_id, status, error=""):
        if status not in FINAL_SEASON_SUBTITLE_STATUSES:
            raise RuntimeError("season subtitle final status invalid")
        now = int(time.time())
        conn = self._connect()
        try:
            conn.execute(
                """
                update internal_api_season_subtitle_tasks
                set status = ?, stage = ?, error = ?, updated_at = ?, completed_at = ?
                where id = ?
                """,
                (status, status, str(error or ""), now, now, task_id),
            )
            conn.commit()
        finally:
            conn.close()


class SeasonSubtitleTaskManager:
    def __init__(self, service, db_path, poll_seconds=0.5):
        self.service = service
        self.store = SeasonSubtitleTaskStore(db_path)
        self.poll_seconds = max(0.05, float(poll_seconds))
        self._condition = threading.Condition()
        self._stop_event = threading.Event()
        self._thread = None

    def start(self):
        with self._condition:
            if self._thread is not None:
                return
            self._stop_event.clear()
            self.store.recover_running_tasks()
            self._thread = threading.Thread(
                target=self._worker_loop,
                name="internal-api-season-subtitles",
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

    def create_task(self, owner_id, media_id, season, targets, retry_of=""):
        owner_id = require_task_text(owner_id, "owner_id")
        media_id = require_task_text(media_id, "media_id")
        season = int(season)
        if season < 1 or season > 99:
            raise RuntimeError("season must be between 1 and 99")
        targets = normalize_targets(targets, season)
        selection = {
            "mode": "retry_failed" if retry_of else "per_episode",
            "candidate_count": len(targets),
            "retry_of": str(retry_of or ""),
        }
        task, created = self.store.create_task(owner_id, media_id, season, selection, targets)
        if created:
            with self._condition:
                self._condition.notify_all()
        return task, created

    def get_task(self, owner_id, task_id):
        return self.store.get_task(
            require_task_text(owner_id, "owner_id"),
            require_task_text(task_id, "task_id"),
        )

    def retry_failed_task(self, owner_id, task_id, media_ids=None):
        owner_id = require_task_text(owner_id, "owner_id")
        task_id = require_task_text(task_id, "task_id")
        task = self.store.get_task_internal(owner_id, task_id)
        if task["status"] not in FINAL_SEASON_SUBTITLE_STATUSES:
            raise RuntimeError("season subtitle task is still active")

        details_by_media = {
            str(item.get("media_id") or "").strip(): item
            for item in task["details"]
            if isinstance(item, dict)
        }
        retriable_ids = []
        for target in task["targets"]:
            media_id = str(target.get("media_id") or "").strip()
            detail = details_by_media.get(media_id)
            if detail is None or detail.get("status") == "failed":
                retriable_ids.append(media_id)

        requested_ids = normalize_retry_media_ids(media_ids)
        selected_ids = requested_ids or retriable_ids
        if not selected_ids:
            raise RuntimeError("season subtitle task has no failed or unfinished episodes")
        invalid_ids = [media_id for media_id in selected_ids if media_id not in retriable_ids]
        if invalid_ids:
            raise RuntimeError("season subtitle retry can only include failed or unfinished episodes")
        selected = set(selected_ids)
        targets = [target for target in task["targets"] if target.get("media_id") in selected]
        parent_task_id = task.get("retry_of") or task_id
        return self.create_task(
            owner_id,
            task["media_id"],
            task["season"],
            targets,
            retry_of=parent_task_id,
        )

    def _worker_loop(self):
        while not self._stop_event.is_set():
            try:
                task = self.store.claim_next_task()
            except sqlite3.Error as exc:
                print("internal API season subtitle worker claim failed: %s" % exc, flush=True)
                self._wait_or_stop()
                continue
            if task is None:
                self._wait_or_stop()
                continue
            try:
                self._apply_task(task)
                self.store.finish_task(task["id"], "completed")
            except Exception as exc:
                print("internal API season subtitle %s failed: %s" % (task["id"], exc), flush=True)
                self.store.finish_task(task["id"], "failed", error=str(exc))

    def _apply_task(self, task):
        targets = list(task["targets"])
        self.store.save_stage(task["id"], "download")
        with ThreadPoolExecutor(
            max_workers=min(SEASON_SUBTITLE_DOWNLOAD_WORKERS, len(targets)),
            thread_name_prefix="season-subtitle-download",
        ) as executor:
            pending = [
                (target, executor.submit(self._apply_target, target))
                for target in targets
            ]
            for target, future in pending:
                self._raise_if_stopping()
                status, count, error = future.result()
                self.store.save_episode_result(
                    task["id"],
                    target,
                    status,
                    count=count,
                    error=error,
                )
                if task.get("retry_of"):
                    self.store.save_retry_result(
                        task["retry_of"],
                        target,
                        status,
                        count=count,
                        error=error,
                    )

    def _apply_target(self, target):
        self._raise_if_stopping()
        try:
            result = self.service.apply_subtitle_candidate(target["candidate"])
        except Exception as exc:
            return "failed", 0, str(exc)
        status = str((result or {}).get("subtitle_match_status") or "").strip()
        count = max(0, int((result or {}).get("subtitle_match_count") or 0))
        if status == "success" and count > 0:
            return "success", count, ""
        if status == "skipped":
            reason = str((result or {}).get("subtitle_match_reason") or "subtitle apply skipped")
            return "skipped", count, reason
        error = str((result or {}).get("subtitle_match_error") or "subtitle apply failed")
        return "failed", count, error

    def _wait_or_stop(self):
        with self._condition:
            self._condition.wait(timeout=self.poll_seconds)

    def _raise_if_stopping(self):
        if self._stop_event.is_set():
            raise RuntimeError("season subtitle worker stopping")


def season_subtitle_task_row(row, include_internal=False):
    details = json_loads_list(row["details_json"])
    selection = json_loads_dict(row["candidate_json"])
    result = {
        "id": str(row["id"]),
        "owner_id": str(row["owner_id"]),
        "media_id": str(row["media_id"]),
        "season": int(row["season"]),
        "status": str(row["status"]),
        "stage": str(row["stage"]),
        "progress_current": len(details),
        "progress_total": len(json_loads_list(row["targets_json"])),
        "succeeded": sum(1 for item in details if item.get("status") == "success"),
        "skipped": sum(1 for item in details if item.get("status") == "skipped"),
        "failed": sum(1 for item in details if item.get("status") == "failed"),
        "current_episode": str(row["current_episode"] or ""),
        "error": str(row["error"] or ""),
        "created_at": int(row["created_at"] or 0),
        "updated_at": int(row["updated_at"] or 0),
        "started_at": int(row["started_at"] or 0),
        "completed_at": int(row["completed_at"] or 0),
        "retry_of": str(selection.get("retry_of") or ""),
        "details": details,
    }
    if include_internal:
        result["candidate"] = selection
        result["targets"] = json_loads_list(row["targets_json"])
    return result


def normalize_targets(targets, season):
    if not isinstance(targets, list) or not targets:
        raise RuntimeError("season subtitle targets are required")
    if len(targets) > MAX_SEASON_SUBTITLE_EPISODES:
        raise RuntimeError("season subtitle target count exceeds %d" % MAX_SEASON_SUBTITLE_EPISODES)
    expected_prefix = "S%02dE" % season
    seen_media_ids = set()
    normalized = []
    for item in targets:
        if not isinstance(item, dict):
            raise RuntimeError("season subtitle target is invalid")
        media_id = require_task_text(item.get("media_id"), "episode media_id")
        episode_key = str(item.get("episode_key") or "").strip().upper()
        if not re.fullmatch(r"S\d{2}E\d{2,3}", episode_key) or not episode_key.startswith(expected_prefix):
            raise RuntimeError("season subtitle episode key is invalid")
        candidate = item.get("candidate")
        if not isinstance(candidate, dict) or str(candidate.get("provider") or "") != "subhd":
            raise RuntimeError("season subtitle candidate must be a SubHD candidate")
        if str(candidate.get("media_id") or "").strip() != media_id:
            raise RuntimeError("season subtitle candidate media mismatch")
        if str(candidate.get("episode_key") or "").strip().upper() != episode_key:
            raise RuntimeError("season subtitle candidate episode mismatch")
        if media_id in seen_media_ids:
            raise RuntimeError("season subtitle targets contain duplicate media")
        seen_media_ids.add(media_id)
        normalized.append({"media_id": media_id, "episode_key": episode_key, "candidate": candidate})
    return normalized


def normalize_retry_media_ids(media_ids):
    if media_ids is None:
        return []
    if not isinstance(media_ids, list):
        raise RuntimeError("season subtitle retry media_ids must be a list")
    if len(media_ids) > MAX_SEASON_SUBTITLE_EPISODES:
        raise RuntimeError("season subtitle retry target count exceeds %d" % MAX_SEASON_SUBTITLE_EPISODES)
    normalized = []
    seen = set()
    for value in media_ids:
        media_id = require_task_text(value, "episode media_id")
        if media_id in seen:
            raise RuntimeError("season subtitle retry targets contain duplicate media")
        seen.add(media_id)
        normalized.append(media_id)
    return normalized


def require_task_text(value, field):
    text = str(value or "").strip()
    if not text or len(text) > 200:
        raise RuntimeError("%s is required" % field)
    return text


def json_dumps(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def json_loads_list(value):
    try:
        loaded = json.loads(value or "[]")
    except (TypeError, ValueError):
        return []
    return loaded if isinstance(loaded, list) else []


def json_loads_dict(value):
    try:
        loaded = json.loads(value or "{}")
    except (TypeError, ValueError):
        return {}
    return loaded if isinstance(loaded, dict) else {}
