import io
import sys
import tempfile
import threading
import time
import unittest
import zipfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "app"))

from pipeline.external_subtitles import SubtitleCache, SubtitleMatcher, SubHDProvider, extract_subhd_detail_results
from pipeline.season_subtitles import SeasonSubtitleTaskManager


class SeasonSubtitleTaskManagerTest(unittest.TestCase):
    def _targets(self):
        return [
            self._target("episode-1", "S01E01", "candidate-1"),
            self._target("episode-2", "S01E02", "candidate-2"),
            self._target("episode-3", "S01E03", "candidate-3"),
        ]

    def _target(self, media_id, episode_key, candidate_id):
        return {
            "media_id": media_id,
            "episode_key": episode_key,
            "candidate": {
                "candidate_id": candidate_id,
                "provider": "subhd",
                "media_id": media_id,
                "episode_key": episode_key,
                "query": episode_key,
                "candidate": {"id": candidate_id, "title": episode_key},
            },
        }

    def _collection_target(self, media_id, episode_key, provider_id="complete"):
        target = self._target(media_id, episode_key, provider_id)
        target["candidate"]["provider_id"] = provider_id
        target["candidate"]["candidate"].update(
            {"provider_id": provider_id, "scope": "collection"}
        )
        return target

    def test_applies_the_selected_candidate_for_each_episode(self):
        class Service:
            def __init__(self):
                self.applied = []

            def apply_subtitle_candidate(self, candidate):
                self.applied.append((candidate["media_id"], candidate["candidate_id"]))
                if candidate["media_id"] == "episode-3":
                    return {
                        "subtitle_match_status": "skipped",
                        "subtitle_match_count": 0,
                        "subtitle_match_reason": "no subtitle body",
                    }
                return {"subtitle_match_status": "success", "subtitle_match_count": 1}

        with tempfile.TemporaryDirectory() as tmp:
            service = Service()
            manager = SeasonSubtitleTaskManager(service, tmp + "/state.db", poll_seconds=0.01)
            manager.start()
            try:
                task, created = manager.create_task("admin", "series-anchor", 1, self._targets())
                self.assertTrue(created)
                completed = self._wait_for_final(manager, task["id"])
            finally:
                manager.stop()

        self.assertEqual(
            sorted(service.applied),
            [
                ("episode-1", "candidate-1"),
                ("episode-2", "candidate-2"),
                ("episode-3", "candidate-3"),
            ],
        )
        self.assertEqual(completed["status"], "completed")
        self.assertEqual(completed["progress_current"], 3)
        self.assertEqual(completed["succeeded"], 2)
        self.assertEqual(completed["skipped"], 1)
        self.assertEqual(completed["failed"], 0)
        self.assertEqual(completed["details"][-1]["episode_key"], "S01E03")
        self.assertEqual(completed["details"][-1]["error"], "no subtitle body")

    def test_applies_two_episode_candidates_concurrently(self):
        class Service:
            def __init__(self):
                self.lock = threading.Lock()
                self.release = threading.Event()
                self.two_started = threading.Event()
                self.active = 0
                self.max_active = 0

            def apply_subtitle_candidate(self, _candidate):
                with self.lock:
                    self.active += 1
                    self.max_active = max(self.max_active, self.active)
                    if self.active >= 2:
                        self.two_started.set()
                self.release.wait(timeout=2)
                with self.lock:
                    self.active -= 1
                return {"subtitle_match_status": "success", "subtitle_match_count": 1}

        with tempfile.TemporaryDirectory() as tmp:
            service = Service()
            manager = SeasonSubtitleTaskManager(service, tmp + "/state.db", poll_seconds=0.01)
            manager.start()
            try:
                task, _created = manager.create_task("admin", "series-anchor", 1, self._targets())
                self.assertTrue(service.two_started.wait(timeout=1))
                service.release.set()
                completed = self._wait_for_final(manager, task["id"])
            finally:
                service.release.set()
                manager.stop()

        self.assertEqual(service.max_active, 2)
        self.assertEqual(completed["succeeded"], 3)

    def test_applies_one_collection_to_sixteen_episodes_with_one_batch_download(self):
        class Service:
            def __init__(self):
                self.collection_calls = []

            def apply_subtitle_candidate(self, _candidate):
                raise AssertionError("collection must use the batch path")

            def apply_season_subtitle_collection(self, candidates):
                self.collection_calls.append([item["media_id"] for item in candidates])
                return [
                    {"subtitle_match_status": "success", "subtitle_match_count": 1}
                    for _item in candidates
                ]

        targets = [
            self._collection_target("episode-%d" % episode, "S05E%02d" % episode)
            for episode in range(1, 17)
        ]
        with tempfile.TemporaryDirectory() as tmp:
            service = Service()
            manager = SeasonSubtitleTaskManager(service, tmp + "/state.db", poll_seconds=0.01)
            manager.start()
            try:
                task, _created = manager.create_task("admin", "series-anchor", 5, targets)
                completed = self._wait_for_final(manager, task["id"])
            finally:
                manager.stop()

        self.assertEqual(len(service.collection_calls), 1)
        self.assertEqual(len(service.collection_calls[0]), 16)
        self.assertEqual(completed["succeeded"], 16)
        self.assertEqual(completed["failed"], 0)

    def test_downloads_each_selected_collection_once(self):
        class Service:
            def __init__(self):
                self.collection_calls = []

            def apply_subtitle_candidate(self, _candidate):
                raise AssertionError("collection must use the batch path")

            def apply_season_subtitle_collection(self, candidates):
                self.collection_calls.append(
                    (
                        candidates[0]["provider_id"],
                        [item["media_id"] for item in candidates],
                    )
                )
                return [
                    {"subtitle_match_status": "success", "subtitle_match_count": 1}
                    for _item in candidates
                ]

        targets = [
            self._collection_target("episode-1", "S05E01", "complete-a"),
            self._collection_target("episode-2", "S05E02", "complete-b"),
            self._collection_target("episode-3", "S05E03", "complete-a"),
            self._collection_target("episode-4", "S05E04", "complete-b"),
        ]
        with tempfile.TemporaryDirectory() as tmp:
            service = Service()
            manager = SeasonSubtitleTaskManager(service, tmp + "/state.db", poll_seconds=0.01)
            manager.start()
            try:
                task, _created = manager.create_task("admin", "series-anchor", 5, targets)
                completed = self._wait_for_final(manager, task["id"])
            finally:
                manager.stop()

        self.assertEqual(
            sorted(service.collection_calls),
            [
                ("complete-a", ["episode-1", "episode-3"]),
                ("complete-b", ["episode-2", "episode-4"]),
            ],
        )
        self.assertEqual(completed["succeeded"], 4)
        self.assertEqual(completed["failed"], 0)

    def test_retry_reuses_only_failed_episode_candidates(self):
        class Service:
            def __init__(self):
                self.calls = []
                self.attempts = {}

            def apply_subtitle_candidate(self, candidate):
                media_id = candidate["media_id"]
                self.calls.append((media_id, candidate["candidate_id"]))
                self.attempts[media_id] = self.attempts.get(media_id, 0) + 1
                if media_id == "episode-2" and self.attempts[media_id] == 1:
                    raise RuntimeError("temporary TLS EOF")
                return {"subtitle_match_status": "success", "subtitle_match_count": 1}

        with tempfile.TemporaryDirectory() as tmp:
            service = Service()
            manager = SeasonSubtitleTaskManager(service, tmp + "/state.db", poll_seconds=0.01)
            manager.start()
            try:
                original, _created = manager.create_task("admin", "series-anchor", 1, self._targets())
                completed = self._wait_for_final(manager, original["id"])
                self.assertEqual(completed["failed"], 1)

                with self.assertRaisesRegex(RuntimeError, "only include failed"):
                    manager.retry_failed_task("admin", original["id"], ["episode-1"])

                retried, created = manager.retry_failed_task("admin", original["id"])
                self.assertTrue(created)
                self.assertNotEqual(retried["id"], original["id"])
                self.assertEqual(retried["retry_of"], original["id"])
                self.assertEqual(retried["progress_total"], 1)
                retry_completed = self._wait_for_final(manager, retried["id"])
                updated_original = manager.get_task("admin", original["id"])
            finally:
                manager.stop()

        self.assertEqual(retry_completed["succeeded"], 1)
        self.assertEqual(retry_completed["failed"], 0)
        self.assertEqual(updated_original["succeeded"], 3)
        self.assertEqual(updated_original["failed"], 0)
        self.assertEqual(service.calls.count(("episode-1", "candidate-1")), 1)
        self.assertEqual(service.calls.count(("episode-2", "candidate-2")), 2)
        self.assertEqual(service.calls.count(("episode-3", "candidate-3")), 1)

    def test_retry_rejects_active_task(self):
        class Service:
            def apply_subtitle_candidate(self, candidate):
                return {"subtitle_match_status": "success", "subtitle_match_count": 1}

        with tempfile.TemporaryDirectory() as tmp:
            manager = SeasonSubtitleTaskManager(Service(), tmp + "/state.db")
            task, _created = manager.create_task("admin", "series-anchor", 1, self._targets())
            with self.assertRaisesRegex(RuntimeError, "still active"):
                manager.retry_failed_task("admin", task["id"])

    def test_allows_multiple_media_versions_for_the_same_episode(self):
        class Service:
            def apply_subtitle_candidate(self, candidate):
                return {"subtitle_match_status": "success", "subtitle_match_count": 1}

        targets = [
            self._target("episode-1-4k", "S01E01", "candidate-1-4k"),
            self._target("episode-1-1080p", "S01E01", "candidate-1-1080p"),
        ]
        with tempfile.TemporaryDirectory() as tmp:
            manager = SeasonSubtitleTaskManager(Service(), tmp + "/state.db")
            _task, created = manager.create_task("admin", "series-anchor", 1, targets)
            claimed = manager.store.claim_next_task()

        self.assertTrue(created)
        self.assertEqual([item["episode_key"] for item in claimed["targets"]], ["S01E01", "S01E01"])

    def test_running_task_is_explicitly_failed_on_recovery(self):
        class Service:
            def apply_subtitle_candidate(self, candidate):
                return {"subtitle_match_status": "success", "subtitle_match_count": 1}

        with tempfile.TemporaryDirectory() as tmp:
            manager = SeasonSubtitleTaskManager(Service(), tmp + "/state.db")
            task, created = manager.create_task("admin", "series-anchor", 1, self._targets())
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


class SubHDSeasonDetailTest(unittest.TestCase):
    def test_parses_episode_rows_and_keeps_collections_separate(self):
        html = """
        <div><span>字幕信息</span></div>
        <div class="text-danger fw-bold">第 10 集</div>
        <div class="row pt-2 mb-2">
          <a class="link-dark" href="/a/abc123">Star.Trek.S01E10.CHS-ENG</a>
          <div class="view-text"><a href="/zu/42">星际迷航中国字幕组</a></div>
          <div class="pt-1 f11">
            <span class="bg-success text-white">原创翻译</span>
            <span class="fw-bold">双语</span><span class="fw-bold">简体</span><span class="fw-bold">英语</span>
            <span class="text-secondary">ASS</span>
            <span class="text-primary">7</span>
            <span class="text-danger">11</span>
          </div>
          <div class="px-3 py-2 text-end text-secondary">11883</div>
          <a href="/u/teclast">Teclast</a>
          <time datetime="2022-07-11T08:30:00+08:00">2022-07-11</time>
          <hr class="my-0">
        </div>
        <div class="text-danger fw-bold">合集</div>
        <div class="row pt-2 mb-2">
          <a class="link-dark" href="/a/seasonpack">Season Pack</a>
          <div class="pt-1 f11"><span class="fw-bold">简体</span><span class="text-secondary">SRT</span></div>
          <div class="px-3 py-2 text-end text-secondary">99999</div>
          <hr class="my-0">
        </div>
        <aside>同系列作品</aside>
        """

        candidates = extract_subhd_detail_results(html, 1)

        self.assertEqual(len(candidates), 2)
        candidate = candidates[0]
        self.assertEqual(candidate["episode_key"], "S01E10")
        self.assertEqual(candidate["title"], "Star.Trek.S01E10.CHS-ENG")
        self.assertEqual(candidate["subtitle_group"], "星际迷航中国字幕组")
        self.assertEqual(candidate["source_type"], "原创翻译")
        self.assertEqual(candidate["language_tags"], ["双语", "简体", "英语"])
        self.assertEqual(candidate["formats"], ["ASS"])
        self.assertEqual(candidate["like_count"], 11)
        self.assertEqual(candidate["download_count"], 11883)
        self.assertEqual(candidate["uploader"], "Teclast")
        self.assertEqual(candidate["uploaded_date"], "2022-07-11")
        self.assertEqual(candidate["scope"], "episode")
        self.assertNotIn("comment_count", candidate)
        collection = candidates[1]
        self.assertEqual(collection["provider_id"], "seasonpack")
        self.assertEqual(collection["episode_key"], "")
        self.assertEqual(collection["scope"], "collection")

    def test_collection_candidates_are_available_to_each_target_episode(self):
        class Provider:
            name = "subhd"

            def enabled(self):
                return True

            def search_season(self, _query, _season):
                return {
                    "detail_url": "https://subhd.tv/d/34148546",
                    "detail_title": "路西法 第五季 Lucifer",
                    "candidates": [
                        {
                            "id": "single",
                            "provider_id": "single",
                            "title": "Lucifer.S05E01",
                            "filename": "Lucifer.S05E01.ass",
                            "language": "简体",
                            "formats": ["ASS"],
                            "episode_key": "S05E01",
                            "scope": "episode",
                            "download_count": 100,
                        },
                        {
                            "id": "complete",
                            "provider_id": "complete",
                            "title": "Lucifer.S05.COMPLETE",
                            "filename": "Lucifer.S05.COMPLETE.ass",
                            "language": "简体",
                            "formats": ["ASS"],
                            "episode_key": "",
                            "scope": "collection",
                            "download_count": 200,
                        },
                    ],
                }

        result = SubtitleMatcher(None, providers=[Provider()]).search_season_candidates(
            "Lucifer S05",
            5,
            [
                {"media_id": "episode-1", "episode_key": "S05E01"},
                {"media_id": "episode-2", "episode_key": "S05E02"},
            ],
        )

        self.assertEqual(
            [(item["media_id"], item["provider_id"]) for item in result["candidates"]],
            [("episode-1", "complete"), ("episode-1", "single"), ("episode-2", "complete")],
        )
        collection = result["candidates"][2]
        self.assertEqual(collection["episode_key"], "S05E02")
        self.assertEqual(collection["candidate"]["episode_key"], "S05E02")

    def test_collection_download_requires_the_requested_archive_member(self):
        archive_body = io.BytesIO()
        with zipfile.ZipFile(archive_body, "w") as archive:
            archive.writestr("Lucifer.S05E01.ass", "[Script Info]\nDialogue: 0,0:00:01.00,0:00:02.00,Default,,0,0,0,,第一集中文字幕")
            archive.writestr("Lucifer.S05E02.ass", "[Script Info]\nDialogue: 0,0:00:01.00,0:00:02.00,Default,,0,0,0,,第二集中文字幕")

        class Transport:
            def __init__(self):
                self.download_calls = 0

            def text_request(self, _url, **_kwargs):
                return ""

            def json_request(self, _method, url, **_kwargs):
                if url.endswith("/api/sub/prepare-download"):
                    return {"success": True, "url": "/down/complete"}
                return {
                    "success": True,
                    "pass": True,
                    "url": "https://download.subhd.me/Lucifer.S05.COMPLETE.zip",
                }

            def download(self, _url, **_kwargs):
                self.download_calls += 1
                return archive_body.getvalue()

        candidate = {
            "id": "complete",
            "provider_id": "complete",
            "title": "Lucifer.S05.COMPLETE",
            "release": "Lucifer.S05.COMPLETE",
            "language": "双语 简体 英语",
            "formats": ["ASS"],
            "scope": "collection",
        }
        transport = Transport()
        provider = SubHDProvider(transport=transport)

        download = provider.download_for_review(candidate, "S05E02")

        self.assertEqual(download.filename, "Lucifer.S05E02.ass")
        with self.assertRaisesRegex(RuntimeError, "does not contain S05E03"):
            provider.download_for_review(candidate, "S05E03")

    def test_collection_batch_downloads_the_archive_once_and_applies_each_member(self):
        archive_body = io.BytesIO()
        with zipfile.ZipFile(archive_body, "w") as archive:
            archive.writestr("Lucifer.S05E01.ass", "[Script Info]\nDialogue: 0,0:00:01.00,0:00:02.00,Default,,0,0,0,,第一集中文字幕")
            archive.writestr("Lucifer.S05E02.ass", "[Script Info]\nDialogue: 0,0:00:01.00,0:00:02.00,Default,,0,0,0,,第二集中文字幕")

        class Transport:
            def __init__(self):
                self.download_calls = 0

            def text_request(self, _url, **_kwargs):
                return ""

            def json_request(self, _method, url, **_kwargs):
                if url.endswith("/api/sub/prepare-download"):
                    return {"success": True, "url": "/down/complete"}
                return {
                    "success": True,
                    "pass": True,
                    "url": "https://download.subhd.me/Lucifer.S05.COMPLETE.zip",
                }

            def download(self, _url, **_kwargs):
                self.download_calls += 1
                return archive_body.getvalue()

        candidate = {
            "id": "complete",
            "provider_id": "complete",
            "title": "Lucifer.S05.COMPLETE",
            "release": "Lucifer.S05.COMPLETE",
            "language": "双语 简体 英语",
            "formats": ["ASS"],
            "scope": "collection",
        }
        records = [
            {
                "provider": "subhd",
                "provider_id": "complete",
                "media_id": "episode-1",
                "query": "S05E01",
                "candidate": {**candidate, "episode_key": "S05E01"},
            },
            {
                "provider": "subhd",
                "provider_id": "complete",
                "media_id": "episode-2",
                "query": "S05E02",
                "candidate": {**candidate, "episode_key": "S05E02"},
            },
            {
                "provider": "subhd",
                "provider_id": "complete",
                "media_id": "episode-3",
                "query": "S05E03",
                "candidate": {**candidate, "episode_key": "S05E03"},
            },
        ]
        transport = Transport()
        with tempfile.TemporaryDirectory() as tmp:
            matcher = SubtitleMatcher(SubtitleCache(tmp), providers=[SubHDProvider(transport=transport)])
            results = matcher.apply_collection_candidates(records)

            self.assertEqual(results[0]["subtitle_match_status"], "success")
            self.assertEqual(results[1]["subtitle_match_status"], "success")
            self.assertEqual(results[2]["subtitle_match_status"], "failed")
            self.assertIn("does not contain S05E03", results[2]["subtitle_match_error"])
            self.assertEqual(len(SubtitleCache(tmp).list_tracks("episode-1")), 1)
            self.assertEqual(len(SubtitleCache(tmp).list_tracks("episode-2")), 1)
            self.assertEqual(SubtitleCache(tmp).list_tracks("episode-3"), [])
        self.assertEqual(transport.download_calls, 1)

    def test_searches_then_reads_the_best_matching_detail_page(self):
        search_html = """
        <div class="bg-white shadow-sm mb-4">
          <a href="/d/35069688"><img alt="星际迷航：奇异新世界 第一季"></a>
        </div>
        <div class="bg-white shadow-sm mb-4">
          <a href="/d/999"><img alt="另一个节目 第一季"></a>
        </div>
        """
        detail_html = """
        <div>字幕信息</div>
        <div class="text-danger fw-bold">第 1 集</div>
        <div class="row pt-2 mb-2">
          <a class="link-dark" href="/a/one">S01E01</a>
          <div class="pt-1 f11"><span class="fw-bold">简体</span><span class="text-secondary">SRT</span></div>
          <div class="px-3 py-2 text-end text-secondary">100</div>
          <hr class="my-0">
        </div>
        <aside>同系列作品</aside>
        """

        class Transport:
            def __init__(self):
                self.urls = []

            def text_request(self, url, **kwargs):
                self.urls.append(url)
                return detail_html if "/d/" in url else search_html

        transport = Transport()
        result = SubHDProvider(transport=transport).search_season("星际迷航：奇异新世界 S01", 1)

        self.assertEqual(result["detail_url"], "https://subhd.tv/d/35069688")
        self.assertEqual(result["detail_title"], "星际迷航：奇异新世界 第一季")
        self.assertEqual(result["candidates"][0]["episode_key"], "S01E01")
        self.assertEqual(len(transport.urls), 2)

    def test_keeps_highest_download_sup_candidate_for_display(self):
        html = """
        <div>字幕信息</div>
        <div class="text-danger fw-bold">第 1 集</div>
        <div class="row pt-2 mb-2">
          <a class="link-dark" href="/a/first-sup">星际迷航.新世界.S01E01.HDR.2160p.WEBRip.HEVC.chs.eng-STCFanSub</a>
          <div class="view-text"><a href="/zu/42">星际迷航中国字幕组</a></div>
          <div class="pt-1 f11">
            <span class="bg-success text-white">原创翻译</span>
            <span class="fw-bold">双语</span><span class="fw-bold">简体</span><span class="fw-bold">英语</span>
            <span class="text-secondary">SUP</span>
            <span class="text-danger">16</span>
          </div>
          <div class="px-3 py-2 text-end text-secondary">23474</div>
          <a href="/u/teclast">Teclast</a>
          <time datetime="2022-07-11T08:30:00+08:00">2022-07-11</time>
          <hr class="my-0">
        </div>
        <aside>同系列作品</aside>
        """

        candidates = extract_subhd_detail_results(html, 1)

        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0]["title"], "星际迷航.新世界.S01E01.HDR.2160p.WEBRip.HEVC.chs.eng-STCFanSub")
        self.assertEqual(candidates[0]["formats"], ["SUP"])
        self.assertEqual(candidates[0]["download_count"], 23474)
        self.assertEqual(candidates[0]["subtitle_group"], "星际迷航中国字幕组")


class SubHDMovieDetailTest(unittest.TestCase):
    def test_search_enriches_movie_candidates_from_the_matching_detail_page(self):
        search_html = """
        <div class="bg-white shadow-sm mb-4">
          <a href="/d/35267208"><img alt="流浪地球2"></a>
          <a class="link-dark align-middle" href="/a/movie-one">流浪地球2</a>
          <div class="view-text text-secondary"><a href="/a/movie-one">The.Wandering.Earth.2</a></div>
          <div class="text-truncate py-2 f11">
            <span class="text-white">官方字幕</span><span class="fw-bold">简体</span><span class="text-secondary">ASS</span>
          </div>
          <div class="pt-2 text-secondary f12"><span class="align-text-top me-3">190k</span><span class="align-text-top me-3">842</span><span class="align-text-top me-3"><time datetime="2025-07-16T02:14:06.000Z">2025-07-16</time></span></div>
          <a href="/u/SubKing">SubKing</a>
        </div>
        """
        detail_html = """
        <div>字幕信息</div>
        <div class="row pt-2 mb-2">
          <a class="link-dark" href="/a/movie-one">The.Wandering.Earth.2.IMAX</a>
          <div class="view-text"><a href="/zu/88">电影字幕组</a></div>
          <div class="pt-1 f11">
            <span class="text-white">官方字幕</span><span class="fw-bold">简体</span><span class="text-secondary">ASS</span>
            <span class="text-primary">9</span><span class="text-danger">6</span>
          </div>
          <div class="px-3 py-2 text-end text-secondary">10838</div>
          <a href="/u/Coritz">Coritz</a>
          <time datetime="2023-07-13T22:36:11.000Z">2023-07-13</time>
          <hr class="my-0">
        </div>
        <aside>同系列作品</aside>
        """

        class Transport:
            def text_request(self, url, **kwargs):
                return detail_html if "/d/" in url else search_html

        candidates = SubHDProvider(transport=Transport()).search("流浪地球2")

        self.assertEqual(len(candidates), 1)
        candidate = candidates[0]
        self.assertEqual(candidate["subtitle_group"], "电影字幕组")
        self.assertEqual(candidate["source_type"], "官方字幕")
        self.assertEqual(candidate["language_tags"], ["简体"])
        self.assertEqual(candidate["formats"], ["ASS"])
        self.assertEqual(candidate["like_count"], 6)
        self.assertEqual(candidate["download_count"], 10838)
        self.assertEqual(candidate["uploader"], "Coritz")
        self.assertEqual(candidate["uploaded_date"], "2023-07-13")
        self.assertNotIn("comment_count", candidate)


if __name__ == "__main__":
    unittest.main()
