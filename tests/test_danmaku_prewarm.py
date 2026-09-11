import sys
import threading
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "app"))

from pipeline.danmaku_prewarm import (
    STATUS_CANCELED,
    STATUS_COMPLETED,
    STATUS_QUEUED,
    STATUS_RUNNING,
    DanmakuPrewarmManager,
    normalize_prewarm_episodes,
    prewarm_task_payload,
)


class FakeService:
    """按 media_id 返回预置结果，并记录调用顺序。"""

    def __init__(self, results=None, raise_for=None):
        self.results = dict(results or {})
        self.raise_for = dict(raise_for or {})
        self.calls = []
        self._lock = threading.Lock()

    def danmaku_comments(self, media_id, *args, **kwargs):
        with self._lock:
            self.calls.append(media_id)
        if media_id in self.raise_for:
            raise self.raise_for[media_id]
        return self.results.get(media_id, {"count": 0, "cached": False})


def wait_for_status(manager, task_id, statuses, timeout=3.0):
    deadline = __import__("time").monotonic() + timeout
    while __import__("time").monotonic() < deadline:
        task = manager.get(task_id)
        if task and task["status"] in statuses:
            return task
        __import__("time").sleep(0.01)
    raise AssertionError("task did not reach %s: %s" % (statuses, manager.get(task_id)))


class NormalizeEpisodesTest(unittest.TestCase):
    def test_accepts_dicts_and_bare_ids_and_dedupes(self):
        episodes = normalize_prewarm_episodes(
            [{"media_id": "a", "episode_key": "S01E01"}, "b", {"media_id": "a"}]
        )
        self.assertEqual([item["media_id"] for item in episodes], ["a", "b"])
        self.assertEqual(episodes[0]["episode_key"], "S01E01")

    def test_rejects_empty_missing_and_oversized_lists(self):
        with self.assertRaises(ValueError):
            normalize_prewarm_episodes([])
        with self.assertRaises(ValueError):
            normalize_prewarm_episodes([{"episode_key": "S01E01"}])
        with self.assertRaises(ValueError):
            normalize_prewarm_episodes("not-a-list")
        with self.assertRaises(ValueError):
            normalize_prewarm_episodes([{"media_id": "x" * 201}])
        # 超限必须报错而不是静默截断，否则调用方会以为整季都预热过了。
        with self.assertRaises(ValueError):
            normalize_prewarm_episodes([{"media_id": str(i)} for i in range(4)], max_episodes=3)


class DanmakuPrewarmManagerTest(unittest.TestCase):
    def manager(self, service, **kwargs):
        options = {"delay_seconds": 0.0, "sleep": lambda _seconds: None}
        options.update(kwargs)
        manager = DanmakuPrewarmManager(service, **options)
        manager.start()
        self.addCleanup(manager.stop)
        return manager

    def test_prewarms_every_episode_and_counts_outcomes(self):
        service = FakeService(
            {
                "a": {"count": 12, "cached": False},
                "b": {"count": 0, "cached": False},
                "c": {"count": 5, "cached": True},
            }
        )
        manager = self.manager(service)
        task = manager.submit(
            "admin",
            "season-1",
            1,
            [{"media_id": "a"}, {"media_id": "b"}, {"media_id": "c"}],
        )
        finished = wait_for_status(manager, task["task_id"], {STATUS_COMPLETED})
        self.assertEqual(finished["total"], 3)
        self.assertEqual(finished["processed"], 3)
        self.assertEqual(finished["matched"], 2)
        self.assertEqual(finished["empty"], 1)
        self.assertEqual(finished["cached"], 1)
        self.assertEqual(finished["failed"], 0)
        self.assertEqual(service.calls, ["a", "b", "c"])
        self.assertEqual([detail["status"] for detail in finished["details"]], [STATUS_COMPLETED] * 3)

    def test_a_failing_episode_does_not_stop_the_season(self):
        service = FakeService(
            {"c": {"count": 3, "cached": False}},
            raise_for={"b": RuntimeError("dandanplay API error 3: 应用不存在")},
        )
        manager = self.manager(service)
        task = manager.submit("admin", "season-1", 1, [{"media_id": "a"}, {"media_id": "b"}, {"media_id": "c"}])
        finished = wait_for_status(manager, task["task_id"], {STATUS_COMPLETED})
        self.assertEqual(finished["processed"], 3)
        self.assertEqual(finished["failed"], 1)
        self.assertEqual(finished["matched"], 1)
        self.assertEqual(service.calls, ["a", "b", "c"], "一集失败后必须继续处理后面的集")
        failed = [detail for detail in finished["details"] if detail["status"] == "failed"]
        self.assertEqual(len(failed), 1)
        self.assertIn("应用不存在", failed[0]["error"])

    def test_same_season_is_not_queued_twice(self):
        service = FakeService({"a": {"count": 1, "cached": False}})
        manager = self.manager(service)
        first = manager.submit("admin", "season-1", 1, [{"media_id": "a"}])
        second = manager.submit("admin", "season-1", 1, [{"media_id": "a"}])
        self.assertEqual(first["task_id"], second["task_id"], "同季进行中的任务必须复用")

    def test_different_season_of_the_same_media_runs_separately(self):
        service = FakeService({"a": {"count": 1, "cached": False}})
        manager = self.manager(service)
        first = manager.submit("admin", "series-1", 1, [{"media_id": "a"}])
        wait_for_status(manager, first["task_id"], {STATUS_COMPLETED})
        second = manager.submit("admin", "series-1", 2, [{"media_id": "a"}])
        self.assertNotEqual(first["task_id"], second["task_id"])

    def test_episodes_are_processed_sequentially_with_a_delay(self):
        service = FakeService({"a": {"count": 1}, "b": {"count": 1}})
        sleeps = []
        manager = self.manager(service, delay_seconds=2.5, sleep=lambda seconds: sleeps.append(seconds))
        task = manager.submit("admin", "season-1", 1, [{"media_id": "a"}, {"media_id": "b"}])
        wait_for_status(manager, task["task_id"], {STATUS_COMPLETED})
        # 第一集不等待，第二集前必须等一次；这是"不形成突发流量"的保证。
        self.assertEqual(sleeps, [2.5])

    def test_task_is_queued_before_the_worker_picks_it_up(self):
        service = FakeService({"a": {"count": 1}})
        manager = DanmakuPrewarmManager(service, delay_seconds=0.0, sleep=lambda _seconds: None)
        # 没 start()：只入队，不该有 worker 动它。
        task = manager.submit("admin", "season-1", 1, [{"media_id": "a"}])
        self.assertEqual(task["status"], STATUS_QUEUED)
        self.assertEqual(service.calls, [])
        self.assertEqual(manager.get(task["task_id"])["status"], STATUS_QUEUED)
        manager.start()
        self.addCleanup(manager.stop)
        wait_for_status(manager, task["task_id"], {STATUS_COMPLETED})

    def test_stop_cancels_a_running_task(self):
        service = FakeService()
        release = threading.Event()

        class BlockingService(FakeService):
            def danmaku_comments(self, media_id, *args, **kwargs):
                release.wait(timeout=5)
                return super().danmaku_comments(media_id, *args, **kwargs)

        service = BlockingService({"a": {"count": 1}, "b": {"count": 1}})
        manager = DanmakuPrewarmManager(service, delay_seconds=0.0, sleep=lambda _seconds: None)
        manager.start()
        task = manager.submit("admin", "season-1", 1, [{"media_id": "a"}, {"media_id": "b"}])
        wait_for_status(manager, task["task_id"], {STATUS_RUNNING})
        manager.stop()
        release.set()
        canceled = wait_for_status(manager, task["task_id"], {STATUS_CANCELED})
        self.assertIn("shutting down", canceled["error"])

    def test_recent_lists_newest_first(self):
        service = FakeService({"a": {"count": 1}})
        manager = self.manager(service)
        first = manager.submit("admin", "s1", 1, [{"media_id": "a"}])
        wait_for_status(manager, first["task_id"], {STATUS_COMPLETED})
        second = manager.submit("admin", "s2", 1, [{"media_id": "a"}])
        wait_for_status(manager, second["task_id"], {STATUS_COMPLETED})
        recent = manager.recent(limit=2)
        self.assertEqual([task["task_id"] for task in recent], [second["task_id"], first["task_id"]])

    def test_payload_reports_missing_task_as_none(self):
        self.assertIsNone(prewarm_task_payload(None))
        manager = self.manager(FakeService())
        self.assertIsNone(manager.get("does-not-exist"))


if __name__ == "__main__":
    unittest.main()
