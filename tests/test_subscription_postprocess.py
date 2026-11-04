import copy
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))

from pipeline.bot import BotConfig, PipelineBotService, require_msg_scrape_coverage
from pipeline.internal_api import subscription_sync_warnings
from pipeline.mediastation import MediaStationClient, MediaStationApiError


class SubscriptionPostprocessTest(unittest.TestCase):
    def setUp(self):
        self.target = {"library_id": "requested-library", "root_id": "requested-root",
                       "root_openlist_path": "/115/anime", "provider": "tmdb", "media_type": "anime"}
        self.path = "/115/anime/Show/Season 1"
        self.identities = [{"openlist_path": self.path + "/Show.S01E197." + ext,
                            "season_num": 1, "episode_num": 197} for ext in ("mkv", "mp4")]
        self.rows = {str(i): {"id": str(i), "path": "cloud://openlist" + item["openlist_path"],
                             "season_num": 1, "episode_num": 197, "title": "Show", "scrape_status": "pending",
                             "tmdb_id": 0, "library_id": "requested-library", "library_root_id": "requested-root"}
                     for i, item in enumerate(self.identities)}
        self.reads, self.scrapes, self.verifications = [], [], []
        test = self

        class FakeMsg:
            def get_media(self, media_id):
                test.reads.append(media_id)
                return copy.deepcopy(test.rows[media_id])

            def pipeline_scrape_media(self, media_id, category, title, queries, provider, media_type, **kwargs):
                if kwargs.get("verify_only"):
                    test.verifications.append((media_id, kwargs))
                    return {"mode": "verified", "scrape_status": "matched", "verified_count": 2}
                test.scrapes.append((media_id, kwargs))
                return {"mode": "apply", "scrape_status": "matched", "applied_count": 2}

        self.service = PipelineBotService(BotConfig(token="token", allowed_user_ids={1}, msg_enabled=True,
                                                    subtitle_auto_match_enabled=False))
        self.service._build_msg_client = lambda: FakeMsg()
        self.service._build_openlist_client = lambda: self.fail("post-processing unexpectedly called OpenList")
        self.task = {"status_name": "success", "msg_sync_status": "failed", "msg_error": "old scrape failure",
                     "msg_scrape_status": "failed", "msg_scan_status": "success", "msg_ingest_status": "completed",
                     "msg_ingest_job_id": "existing-ingest", "msg_media_id": "historical",
                     "msg_ingest_media_count": 3,
                     "msg_ingest_media_items": [{"id": "historical", "path": "cloud://openlist" + self.path + "/Show.S01E170.mkv",
                                                 "season_num": 1, "episode_num": 170}] + list(self.rows.values()),
                     "subscription_target_media_identities": self.identities,
                     "subscription_target_openlist_path": self.path,
                     "openlist_clean_status": "failed", "openlist_clean_error": "old missing target"}

    def refresh(self):
        return self.service.refresh_subscription_media_receipt("anime", self.target, self.task, self.identities)

    def test_legacy_directory_receipt_is_restricted_before_retry_without_scan(self):
        task = self.refresh()
        self.assertEqual(task["msg_ingest_media_count"], 2)
        self.assertIn(task["msg_media_id"], self.rows)
        self.assertEqual(task["msg_ingest_job_id"], "existing-ingest")
        result = self.service.sync_completed_task("anime", "Show release title", task, target=self.target)
        self.assertEqual(result["msg_sync_status"], "success", result.get("msg_error"))
        self.assertEqual(result["openlist_clean_status"], "skipped")
        self.assertEqual(result["msg_visibility_repair_reason"], "explicit_episode_identities")
        self.assertEqual(self.scrapes[0][1]["media_ids"], ["0", "1"])
        self.assertEqual(self.scrapes[0][1]["use_existing_episode_identity"], True)
        self.assertNotIn("historical", self.reads)
        self.assertEqual(subscription_sync_warnings(result, {}), [])

    def test_completed_ingest_from_old_release_is_scoped_before_scraping(self):
        test = self

        class OldIngest:
            def pipeline_get_ingest(self, job_id):
                test.assertEqual(job_id, "old-ingest")
                return {"id": job_id, "status": "completed", "result": {
                    "media": test.task["msg_ingest_media_items"][0],
                    "media_items": test.task["msg_ingest_media_items"],
                    "applied_media_identities": test.identities,
                }}

        task = {**self.task, "msg_ingest_job_id": "old-ingest"}
        result = self.service._run_msg_pipeline_ingest(OldIngest(), "anime", "Show", task, [],
                                                      [self.path], True, lambda updates: None, self.target)
        self.assertEqual(result["msg_ingest_media_ids"], ["0", "1"])
        self.assertEqual(result["msg_ingest_media_count"], 2)
        self.assertIn(result["_media"]["id"], self.rows)

    def test_single_planned_episode_requires_actual_scrape_coverage(self):
        task = {"msg_ingest_media_count": 1, "subscription_target_media_identities": self.identities[:1]}
        with self.assertRaisesRegex(RuntimeError, "covered 0 of 1"):
            require_msg_scrape_coverage("anime", task, {"msg_scrape_applied_count": 0})

    def test_postprocess_race_with_deleted_media_fails_without_rescanning(self):
        task = self.refresh()

        class DeletedMedia:
            def get_media(self, media_id):
                raise MediaStationApiError(404, "media not found")

        self.service._build_msg_client = lambda: DeletedMedia()
        result = self.service.sync_completed_task("anime", "Show", task, target=self.target)
        self.assertEqual(result["msg_sync_status"], "failed")
        self.assertIn("removed before post-processing", result["msg_error"])
        self.assertEqual(self.scrapes, [])

    def test_manual_scrape_is_reused_only_when_both_formats_have_episode_metadata(self):
        for row in self.rows.values():
            row.update(scrape_status="matched", tmdb_id=12345, episode_title="Episode title")
        task = self.refresh()
        self.assertEqual(task["msg_scrape_status"], "success")
        result = self.service.sync_completed_task("anime", "Show", task, target=self.target)
        self.assertEqual(result["msg_sync_status"], "success")
        self.assertEqual(self.scrapes, [])
        self.assertEqual(len(self.verifications), 1)
        self.assertEqual(subscription_sync_warnings(result, {}), [])
        self.rows["1"].update(scrape_status="pending", tmdb_id=0, episode_title="")
        task = self.refresh()
        self.assertEqual(task["msg_scrape_status"], "failed")
        result = self.service.sync_completed_task("anime", "Show", task, target=self.target)
        self.assertEqual(result["msg_scrape_applied_count"], 2)
        self.assertEqual(len(self.scrapes), 1)

    def test_changed_identity_root_and_conflicting_provider_are_rejected(self):
        for updates in ({"library_root_id": "other-root"}, {"episode_num": 198}, {"path": "cloud://openlist/another/file.mkv"}):
            with self.subTest(updates=updates):
                before = dict(self.rows["0"])
                self.rows["0"].update(updates)
                with self.assertRaisesRegex(RuntimeError, "changed or is outside"):
                    self.refresh()
                self.rows["0"] = before
        self.rows["0"].update(tmdb_id=12345, scrape_status="matched")
        self.rows["1"].update(tmdb_id=54321, scrape_status="matched")
        with self.assertRaisesRegex(RuntimeError, "conflicting provider"):
            self.refresh()

    def test_successful_old_task_cannot_hide_missing_current_metadata(self):
        self.task.update(msg_scrape_status="success", msg_sync_status="success")
        self.rows["0"].update(tmdb_id=12345, scrape_status="matched", episode_title="Episode")
        self.rows["1"].update(tmdb_id=0, scrape_status="matched", episode_title="Episode")
        result = self.refresh()
        self.assertEqual(result["msg_scrape_status"], "pending")
        self.assertEqual(result["msg_sync_status"], "running")

    def test_verification_reads_only_requested_media_in_requested_root(self):
        result = self.service.verify_subscription_msg_episodes("anime", self.path, 1, [197],
                                                               self.target, list(self.rows.values()))
        self.assertEqual(result["verified_episodes"], [197])
        self.assertEqual(result["missing_episodes"], [])
        self.assertEqual(result["duplicate_episodes"], {})
        self.assertEqual(self.reads, ["0", "1"])

    def test_warnings_use_current_stages_and_report_cleanup_separately(self):
        task = {"msg_sync_status": "failed", "msg_scrape_status": "failed", "msg_error": "metadata failed",
                "msg_scrape_error": "metadata failed", "warnings": ["stale failure"]}
        warnings = subscription_sync_warnings(task, {"staging_cleanup": {"status": "failed", "error": "permission denied"}})
        self.assertEqual(warnings, ["MSG error: metadata failed", "subscription staging cleanup failed: permission denied"])

    def test_scrape_client_serializes_identity_reuse_flag(self):
        client = MediaStationClient("http://unused", "unused", "unused")
        captured = []
        client._pipeline_request = lambda method, path, data: captured.append(data)
        client.pipeline_scrape_media("0", "anime", "Show", [], "tmdb", "anime",
                                     media_ids=["0", "1"], use_existing_episode_identity=True, verify_only=True)
        self.assertTrue(captured[0]["use_existing_episode_identity"])
        self.assertTrue(captured[0]["verify_only"])


if __name__ == "__main__":
    unittest.main()
