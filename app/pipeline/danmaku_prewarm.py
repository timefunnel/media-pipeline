"""整季弹幕预热。

预热会逐集调用 ``DanmakuMatcher``，把匹配结果与弹幕写进磁盘缓存，这样用户
打开某一集时不必等第三方回源。

两条刻意的约束（都来自弹弹play 开放弹幕网络的使用约定：按需调用、禁止规模化
抓取）：

* **只由用户/管理员显式触发**，绝不自动排期整库预热；
* **串行 + 集间延迟**，并把单次任务限制在固定集数以内。

进度只保存在内存里：真正的产物是 ``DanmakuCache`` 的磁盘缓存，进程重启后留存的
也是缓存本身；重启丢掉的只是任务历史。
"""

import threading
import time
import uuid
from collections import deque

DEFAULT_DANMAKU_PREWARM_MAX_EPISODES = 50
DEFAULT_DANMAKU_PREWARM_DELAY_SECONDS = 2.0
MAX_TRACKED_DANMAKU_PREWARM_TASKS = 20

STATUS_QUEUED = "queued"
STATUS_RUNNING = "running"
STATUS_COMPLETED = "completed"
STATUS_FAILED = "failed"
STATUS_CANCELED = "canceled"

FINAL_STATUSES = (STATUS_COMPLETED, STATUS_FAILED, STATUS_CANCELED)


def normalize_prewarm_episodes(value, max_episodes=DEFAULT_DANMAKU_PREWARM_MAX_EPISODES):
    """校验并去重调用方给出的分集列表。

    返回 ``[{"media_id": ..., "episode_key": ...}]``；空列表或超限都直接报错，
    不静默截断 —— 截断会让调用方以为整季都预热过了。
    """
    if not isinstance(value, (list, tuple)):
        raise ValueError("episodes must be a list")
    episodes = []
    seen = set()
    for raw in value:
        if isinstance(raw, dict):
            media_id = str(raw.get("media_id") or "").strip()
            episode_key = str(raw.get("episode_key") or "").strip()
        else:
            media_id = str(raw or "").strip()
            episode_key = ""
        if not media_id:
            raise ValueError("episode media_id is required")
        if len(media_id) > 200:
            raise ValueError("episode media_id is too long")
        if media_id in seen:
            continue
        seen.add(media_id)
        episodes.append({"media_id": media_id, "episode_key": episode_key})
    if not episodes:
        raise ValueError("episodes must not be empty")
    if len(episodes) > int(max_episodes):
        raise ValueError("episodes must not exceed %d entries" % int(max_episodes))
    return episodes


def prewarm_task_payload(task):
    """对外结构：进度 + 逐集结果（错误必须能看见）。"""
    if task is None:
        return None
    return {
        "task_id": task["task_id"],
        "status": task["status"],
        "owner_id": task["owner_id"],
        "media_id": task["media_id"],
        "season": task["season"],
        "total": task["total"],
        "processed": task["processed"],
        "matched": task["matched"],
        "empty": task["empty"],
        "cached": task["cached"],
        "failed": task["failed"],
        "current_episode": task["current_episode"],
        "error": task["error"],
        "details": list(task["details"]),
        "created_at": task["created_at"],
        "updated_at": task["updated_at"],
    }


class DanmakuPrewarmManager:
    """单 worker、串行执行的整季预热任务队列。"""

    def __init__(
        self,
        service,
        delay_seconds=DEFAULT_DANMAKU_PREWARM_DELAY_SECONDS,
        max_episodes=DEFAULT_DANMAKU_PREWARM_MAX_EPISODES,
        sleep=time.sleep,
        now=time.time,
    ):
        self.service = service
        self.delay_seconds = max(0.0, float(delay_seconds))
        self.max_episodes = max(1, int(max_episodes))
        self._sleep = sleep
        self._now = now
        self._lock = threading.Lock()
        self._tasks = {}
        self._order = deque()
        self._queue = deque()
        self._wake = threading.Event()
        self._stop = threading.Event()
        self._worker = None

    def start(self):
        if self._worker is not None:
            return
        self._stop.clear()
        worker = threading.Thread(target=self._worker_loop, name="danmaku-prewarm", daemon=True)
        self._worker = worker
        worker.start()

    def stop(self):
        self._stop.set()
        self._wake.set()
        worker = self._worker
        self._worker = None
        if worker is not None:
            worker.join(timeout=5)

    def submit(self, owner_id, media_id, season, episodes):
        """入队一个整季预热任务；同一媒体同一季已有进行中的任务时直接复用。"""
        episodes = normalize_prewarm_episodes(episodes, max_episodes=self.max_episodes)
        media_id = str(media_id or "").strip()
        if not media_id:
            raise ValueError("media_id is required")
        try:
            season_number = int(season)
        except (TypeError, ValueError):
            raise ValueError("season must be an integer")
        with self._lock:
            for task_id in self._order:
                task = self._tasks.get(task_id)
                if task is None or task["status"] in FINAL_STATUSES:
                    continue
                if task["media_id"] == media_id and task["season"] == season_number:
                    # 已有同季任务在排队/执行：复用，避免重复回源。
                    return prewarm_task_payload(task)
            task = {
                "task_id": uuid.uuid4().hex,
                "status": STATUS_QUEUED,
                "owner_id": str(owner_id or ""),
                "media_id": media_id,
                "season": season_number,
                "episodes": episodes,
                "total": len(episodes),
                "processed": 0,
                "matched": 0,
                "empty": 0,
                "cached": 0,
                "failed": 0,
                "current_episode": "",
                "error": "",
                "details": [],
                "created_at": self._now(),
                "updated_at": self._now(),
            }
            self._tasks[task["task_id"]] = task
            self._order.append(task["task_id"])
            self._queue.append(task["task_id"])
            self._trim_locked()
            self._wake.set()
            return prewarm_task_payload(task)

    def get(self, task_id):
        with self._lock:
            return prewarm_task_payload(self._tasks.get(str(task_id or "")))

    def recent(self, limit=10):
        with self._lock:
            ids = list(self._order)[-max(1, int(limit)) :]
            return [prewarm_task_payload(self._tasks[task_id]) for task_id in reversed(ids)]

    def _trim_locked(self):
        while len(self._order) > MAX_TRACKED_DANMAKU_PREWARM_TASKS:
            oldest = self._order.popleft()
            task = self._tasks.get(oldest)
            if task is not None and task["status"] not in FINAL_STATUSES:
                # 还在跑的任务不能丢，放回去继续跟踪。
                self._order.append(oldest)
                return
            self._tasks.pop(oldest, None)

    def _worker_loop(self):
        while not self._stop.is_set():
            task_id = None
            with self._lock:
                if self._queue:
                    task_id = self._queue.popleft()
            if task_id is None:
                self._wake.wait(timeout=0.5)
                self._wake.clear()
                continue
            self._run_task(task_id)

    def _run_task(self, task_id):
        with self._lock:
            task = self._tasks.get(task_id)
            if task is None:
                return
            task["status"] = STATUS_RUNNING
            task["updated_at"] = self._now()
            episodes = list(task["episodes"])
        for index, episode in enumerate(episodes):
            if self._stop.is_set():
                self._finish(task_id, STATUS_CANCELED, "pipeline is shutting down")
                return
            if index:
                # 集间延迟：把一次整季预热摊开，避免对第三方形成突发流量。
                self._sleep(self.delay_seconds)
                if self._stop.is_set():
                    self._finish(task_id, STATUS_CANCELED, "pipeline is shutting down")
                    return
            self._run_episode(task_id, episode)
        self._finish(task_id, STATUS_COMPLETED, "")

    def _run_episode(self, task_id, episode):
        media_id = episode["media_id"]
        with self._lock:
            task = self._tasks.get(task_id)
            if task is None:
                return
            task["current_episode"] = episode.get("episode_key") or media_id
            task["updated_at"] = self._now()
        try:
            payload = self.service.danmaku_comments(media_id)
        except (RuntimeError, ValueError) as exc:
            self._record(
                task_id,
                episode,
                status=STATUS_FAILED,
                count=0,
                cached=False,
                error=str(exc),
            )
            return
        count = int(payload.get("count") or 0)
        self._record(
            task_id,
            episode,
            status=STATUS_COMPLETED,
            count=count,
            cached=bool(payload.get("cached")),
            error="",
        )

    def _record(self, task_id, episode, status, count, cached, error):
        with self._lock:
            task = self._tasks.get(task_id)
            if task is None:
                return
            task["processed"] += 1
            if status == STATUS_FAILED:
                task["failed"] += 1
            elif count <= 0:
                task["empty"] += 1
            else:
                task["matched"] += 1
            if cached:
                task["cached"] += 1
            task["details"].append(
                {
                    "media_id": episode["media_id"],
                    "episode_key": episode.get("episode_key") or "",
                    "status": status,
                    "count": count,
                    "cached": cached,
                    "error": error,
                }
            )
            task["updated_at"] = self._now()

    def _finish(self, task_id, status, error):
        with self._lock:
            task = self._tasks.get(task_id)
            if task is None:
                return
            task["status"] = status
            task["error"] = error
            task["current_episode"] = ""
            task["updated_at"] = self._now()
