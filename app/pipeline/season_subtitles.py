import json
import re
import sqlite3
import threading
import time
import uuid
from pathlib import Path

from pipeline.external_subtitles import subtitle_episode_key


FINAL_SEASON_SUBTITLE_STATUSES = {"completed", "failed"}
ACTIVE_SEASON_SUBTITLE_STATUSES = {"queued", "running"}
MAX_SEASON_SUBTITLE_EPISODES = 500


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

    def create_task(self, owner_id, media_id, season, candidate, targets):
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
                    json_dumps(candidate),
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
        return season_subtitle_task_row(row)

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

    def create_task(self, owner_id, media_id, season, candidate, targets):
        owner_id = require_task_text(owner_id, "owner_id")
        media_id = require_task_text(media_id, "media_id")
        season = int(season)
        if season < 1 or season > 99:
            raise RuntimeError("season must be between 1 and 99")
        if not isinstance(candidate, dict) or str(candidate.get("provider") or "") != "subhd":
            raise RuntimeError("season subtitle candidate must be a SubHD candidate")
        targets = normalize_targets(targets, season)
        task, created = self.store.create_task(owner_id, media_id, season, candidate, targets)
        if created:
            with self._condition:
                self._condition.notify_all()
        return task, created

    def get_task(self, owner_id, task_id):
        return self.store.get_task(
            require_task_text(owner_id, "owner_id"),
            require_task_text(task_id, "task_id"),
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
        self.store.save_stage(task["id"], "download")
        downloads = list(self.service.subtitle_download_season_candidate(task["candidate"]) or [])
        by_episode = {}
        for download in downloads:
            key = subtitle_episode_key(getattr(download, "filename", ""))
            if key:
                by_episode.setdefault(key, []).append(download)

        for target in task["targets"]:
            self._raise_if_stopping()
            key = target["episode_key"]
            self.store.save_stage(task["id"], "applying", key)
            matched = by_episode.get(key) or []
            if not matched:
                self.store.save_episode_result(
                    task["id"],
                    target,
                    "skipped",
                    error="the season package has no strict %s subtitle filename match" % key,
                )
                continue
            count = 0
            try:
                for download in matched:
                    self.service.subtitle_cache_season_download(target["media_id"], download)
                    count += 1
            except Exception as exc:
                self.store.save_episode_result(task["id"], target, "failed", count=count, error=str(exc))
                continue
            self.store.save_episode_result(task["id"], target, "success", count=count)

    def _wait_or_stop(self):
        with self._condition:
            self._condition.wait(timeout=self.poll_seconds)

    def _raise_if_stopping(self):
        if self._stop_event.is_set():
            raise RuntimeError("season subtitle worker stopping")


def season_subtitle_task_row(row, include_internal=False):
    details = json_loads_list(row["details_json"])
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
        "details": details,
    }
    if include_internal:
        result["candidate"] = json_loads_dict(row["candidate_json"])
        result["targets"] = json_loads_list(row["targets_json"])
    return result


def normalize_targets(targets, season):
    if not isinstance(targets, list) or not targets:
        raise RuntimeError("season subtitle targets are required")
    if len(targets) > MAX_SEASON_SUBTITLE_EPISODES:
        raise RuntimeError("season subtitle target count exceeds %d" % MAX_SEASON_SUBTITLE_EPISODES)
    expected_prefix = "S%02dE" % season
    seen_media_ids = set()
    seen_episode_keys = set()
    normalized = []
    for item in targets:
        if not isinstance(item, dict):
            raise RuntimeError("season subtitle target is invalid")
        media_id = require_task_text(item.get("media_id"), "episode media_id")
        episode_key = str(item.get("episode_key") or "").strip().upper()
        if not re.fullmatch(r"S\d{2}E\d{2,3}", episode_key) or not episode_key.startswith(expected_prefix):
            raise RuntimeError("season subtitle episode key is invalid")
        if media_id in seen_media_ids or episode_key in seen_episode_keys:
            raise RuntimeError("season subtitle targets contain duplicate episodes")
        seen_media_ids.add(media_id)
        seen_episode_keys.add(episode_key)
        normalized.append({"media_id": media_id, "episode_key": episode_key})
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
