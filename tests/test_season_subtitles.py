import io
import tempfile
import time
import unittest
import zipfile

from pipeline.external_subtitles import SubHDProvider
from pipeline.season_subtitles import SeasonSubtitleTaskManager


class SeasonSubtitleTaskManagerTest(unittest.TestCase):
    def _candidate(self):
        return {
            "provider": "subhd",
            "query": "Alien: Earth S01",
            "candidate": {"id": "candidate-1", "title": "Alien Earth Season One"},
        }

    def _targets(self):
        return [
            {"media_id": "episode-1", "episode_key": "S01E01"},
            {"media_id": "episode-2", "episode_key": "S01E02"},
            {"media_id": "episode-3", "episode_key": "S01E03"},
        ]

    def test_downloads_one_season_package_and_persists_per_episode_progress(self):
        class Download:
            def __init__(self, filename):
                self.filename = filename

        class Service:
            def __init__(self):
                self.download_calls = 0
                self.saved = []

            def subtitle_download_season_candidate(self, candidate):
                self.download_calls += 1
                self.asserted_candidate = candidate
                return [Download("Alien.Earth.S01E01.zh.srt"), Download("Alien.Earth.S01E02.zh.srt")]

            def subtitle_cache_season_download(self, media_id, download):
                self.saved.append((media_id, download.filename))
                return {"filename": download.filename}

        with tempfile.TemporaryDirectory() as tmp:
            service = Service()
            manager = SeasonSubtitleTaskManager(service, tmp + "/state.db", poll_seconds=0.01)
            manager.start()
            try:
                task, created = manager.create_task("admin", "series-anchor", 1, self._candidate(), self._targets())
                self.assertTrue(created)
                completed = self._wait_for_final(manager, task["id"])
            finally:
                manager.stop()

        self.assertEqual(service.download_calls, 1)
        self.assertEqual(
            service.saved,
            [
                ("episode-1", "Alien.Earth.S01E01.zh.srt"),
                ("episode-2", "Alien.Earth.S01E02.zh.srt"),
            ],
        )
        self.assertEqual(completed["status"], "completed")
        self.assertEqual(completed["progress_current"], 3)
        self.assertEqual(completed["succeeded"], 2)
        self.assertEqual(completed["skipped"], 1)
        self.assertEqual(completed["failed"], 0)
        self.assertEqual(completed["details"][-1]["episode_key"], "S01E03")
        self.assertIn("strict S01E03", completed["details"][-1]["error"])

    def test_running_task_is_explicitly_failed_on_recovery(self):
        class Service:
            def subtitle_download_season_candidate(self, candidate):
                return []

            def subtitle_cache_season_download(self, media_id, download):
                return {}

        with tempfile.TemporaryDirectory() as tmp:
            manager = SeasonSubtitleTaskManager(Service(), tmp + "/state.db")
            task, created = manager.create_task("admin", "series-anchor", 1, self._candidate(), self._targets())
            self.assertTrue(created)
            claimed = manager.store.claim_next_task()
            self.assertEqual(claimed["status"], "running")
            self.assertEqual(manager.store.recover_running_tasks(), 1)
            recovered = manager.get_task("admin", task["id"])

        self.assertEqual(recovered["status"], "failed")
        self.assertIn("pipeline restarted", recovered["error"])

    def _wait_for_final(self, manager, task_id):
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            task = manager.get_task("admin", task_id)
            if task["status"] in {"completed", "failed"}:
                return task
            time.sleep(0.01)
        self.fail("season subtitle task did not finish")


class SubHDSeasonDownloadTest(unittest.TestCase):
    def test_downloads_the_package_once_and_extracts_strict_episode_files(self):
        subtitle = "1\n00:00:01,000 --> 00:00:02,000\n\u4f60\u597d\u4e16\u754c\n".encode("utf-8")
        archive_buffer = io.BytesIO()
        with zipfile.ZipFile(archive_buffer, "w") as archive:
            archive.writestr("Alien.Earth.S01E01.zh.srt", subtitle)
            archive.writestr("Alien.Earth.S01E02.zh.srt", subtitle)

        class Transport:
            def __init__(self):
                self.download_calls = 0

            def text_request(self, *args, **kwargs):
                return "<html></html>"

            def json_request(self, method, url, headers=None, data=None, timeout=None):
                if url.endswith("prepare-download"):
                    return {"success": True, "url": "/down/season-1"}
                return {"success": True, "pass": True, "url": "https://dl.subhd.me/season-1.zip"}

            def download(self, *args, **kwargs):
                self.download_calls += 1
                return archive_buffer.getvalue()

        transport = Transport()
        provider = SubHDProvider(transport=transport)
        downloads = provider.download_season(
            {"id": "season-1", "title": "Alien Earth Season One", "language": "zh-CN"},
            "Alien: Earth S01",
        )

        self.assertEqual(transport.download_calls, 1)
        self.assertEqual(
            [item.filename for item in downloads],
            ["Alien.Earth.S01E01.zh.srt", "Alien.Earth.S01E02.zh.srt"],
        )


if __name__ == "__main__":
    unittest.main()
