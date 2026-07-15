import json
import os
import socket
import sqlite3
import sys
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "app"))

for category in ("movie", "tv", "anime", "adult", "other"):
    prefix = "MEDIA_PIPELINE_%s" % category.upper()
    os.environ.setdefault(prefix + "_MSG_LIBRARY_ID", "test-%s-library" % category)
    os.environ.setdefault(prefix + "_MSG_ROOT_ID", "test-%s-root" % category)

from pipeline.bot import BotConfig, PipelineBotService
from pipeline.config import category_to_folder_id, category_to_openlist_path
from pipeline.internal_api import ApiError, ImportTaskManager, InternalApiApplication, InternalApiServer, InternalApiStore


TARGET = {
    "library_id": "library-current",
    "root_id": "root-current",
    "root_openlist_path": category_to_openlist_path("movie"),
    "provider": "tmdb-current",
    "media_type": "movie-current",
}


def target_for(category, **updates):
    target = {**TARGET, "root_openlist_path": category_to_openlist_path(category)}
    target.update(updates)
    return target


class ResultList(list):
    def __init__(self, items, metadata=None):
        super().__init__(items)
        self.metadata = dict(metadata or {})


class FakePipelineService:
    def __init__(
        self,
        warning=False,
        missing_media_id=False,
        sync_error="",
        sync_delay=0,
        download_delay=0,
        duplicate=None,
    ):
        self.warning = warning
        self.missing_media_id = missing_media_id
        self.sync_error = sync_error
        self.sync_delay = sync_delay
        self.download_delay = download_delay
        self.duplicate = duplicate
        self.submit_uris = []
        self.task_status_calls = []
        self.sync_targets = []
        self.sync_input_tasks = []
        self.cancel_calls = []
        self.duplicate_calls = []
        self._sequence = 0
        self._lock = threading.Lock()
        self.active = 0
        self.max_active = 0
        self.active_by_owner_target = {}
        self.max_by_target = {}
        self.active_by_owner = {}
        self.max_by_owner = {}
        self.download_active = 0
        self.max_download_active = 0

    def search(self, query, category, limit=20):
        return ResultList(
            [{"title": query, "download_uri": "magnet:?xt=urn:btih:%s" % query.upper(), "rank": 1}],
            metadata={"profile": category},
        )

    def search_capabilities(self):
        return {"pansou": True, "bt4g": True, "llm_rerank": False}

    def search_pansou(self, query, limit=20):
        return [{"title": query, "download_uri": "https://115.com/s/%s" % query, "rank": 1}]

    def search_bt4g(self, query, limit=20):
        return ResultList(
            [{"title": query, "download_uri": "magnet:?xt=urn:btih:BT4G%s" % query.upper(), "rank": 1}],
            metadata={"profile": "bt4g"},
        )

    def submit(self, category, download_uri):
        with self._lock:
            self._sequence += 1
            info_hash = "HASH%03d" % self._sequence
            self.submit_uris.append(download_uri)
        status_name = "submitted" if self.download_delay else "success"
        task = {"info_hash": info_hash, "status_name": status_name, "percent_done": 0 if self.download_delay else 100}
        return {"state": True, "tasks": [task], "task_status": task}

    def check_duplicate(self, category, query, candidate, target=None):
        self.duplicate_calls.append((category, query, dict(candidate), dict(target or {})))
        return dict(self.duplicate) if self.duplicate is not None else None

    def task_status(self, category, info_hash):
        self.task_status_calls.append((category, info_hash))
        with self._lock:
            self.download_active += 1
            self.max_download_active = max(self.max_download_active, self.download_active)
        try:
            if self.download_delay:
                time.sleep(self.download_delay)
        finally:
            with self._lock:
                self.download_active -= 1
        return {"info_hash": info_hash, "status_name": "success", "percent_done": 100}

    def cancel_task(self, category, info_hash):
        self.cancel_calls.append((category, info_hash))
        return {"info_hash": info_hash, "status_name": "cancelled"}

    def sync_completed_task(self, category, title, task, progress_callback=None, target=None):
        target = dict(target or {})
        self.sync_targets.append(target)
        self.sync_input_tasks.append(dict(task or {}))
        if progress_callback:
            progress_callback({**task, "msg_sync_status": "running", "msg_scan_status": "running"})
        target_key = (target.get("library_id"), target.get("root_id"))
        owner = str(title).split("|", 1)[0] if "|" in str(title) else "unknown"
        with self._lock:
            self.active += 1
            self.max_active = max(self.max_active, self.active)
            current = self.active_by_owner_target.get(target_key, 0) + 1
            self.active_by_owner_target[target_key] = current
            self.max_by_target[target_key] = max(self.max_by_target.get(target_key, 0), current)
            owner_active = self.active_by_owner.get(owner, 0) + 1
            self.active_by_owner[owner] = owner_active
            self.max_by_owner[owner] = max(self.max_by_owner.get(owner, 0), owner_active)
        try:
            if self.sync_delay:
                time.sleep(self.sync_delay)
        finally:
            with self._lock:
                self.active -= 1
                self.active_by_owner_target[target_key] -= 1
                self.active_by_owner[owner] -= 1
        result = {
            **task,
            "msg_sync_status": "success",
            "msg_scan_status": "success",
            "msg_scrape_status": "success",
            "subtitle_match_status": "success",
        }
        if self.sync_error:
            result["msg_sync_status"] = "failed"
            result["msg_scan_status"] = "failed"
            result["msg_error"] = self.sync_error
        elif not self.missing_media_id:
            result["msg_media_id"] = "media-" + task["info_hash"]
            result["msg_media_title"] = "MSG " + title
        if self.warning:
            result["subtitle_match_status"] = "failed"
            result["subtitle_match_error"] = "subtitle provider failed"
        return result


class InternalApiTestCase(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = str(Path(self.tempdir.name) / "state.db")

    def tearDown(self):
        self.tempdir.cleanup()

    def build_components(self, service=None, workers=3, owner_workers=2, poll_seconds=0.01):
        service = service or FakePipelineService()
        store = InternalApiStore(self.db_path)
        manager = ImportTaskManager(
            service,
            store,
            workers=workers,
            owner_workers=owner_workers,
            poll_seconds=poll_seconds,
        )
        application = InternalApiApplication(service, store, manager)
        return service, store, manager, application

    def search_candidate(self, application, owner_id="owner-a", query="sintel", category="movie"):
        response = application.search(
            {"owner_id": owner_id, "query": query, "category": category, "source": "default", "limit": 5}
        )
        return response["session_id"], response["items"][0]["candidate_id"], response

    def import_payload(self, session_id, candidate_id, category="movie", target=None):
        return {
            "owner_id": "owner-a",
            "search_session_id": session_id,
            "candidate_id": candidate_id,
            "category": category,
            **dict(target or TARGET),
        }

    def wait_final(self, manager, owner_id, import_id, timeout=3):
        deadline = time.time() + timeout
        while time.time() < deadline:
            task = manager.get_import(owner_id, import_id)
            if task["status"] in {"completed", "completed_with_warning", "failed", "canceled"}:
                return task
            time.sleep(0.01)
        self.fail("import task did not reach a final state")


class BotApiConfigTest(InternalApiTestCase):
    def test_config_requires_token_when_internal_api_is_enabled(self):
        with self.assertRaisesRegex(RuntimeError, "INTERNAL_API_TOKEN missing"):
            BotConfig.from_env(
                {
                    "TG_BOT_TOKEN": "123:token",
                    "TG_ALLOWED_USER_IDS": "1",
                    "INTERNAL_API_ENABLED": "1",
                }
            )

    def test_config_reads_internal_api_worker_limits(self):
        config = BotConfig.from_env(
            {
                "TG_BOT_TOKEN": "123:token",
                "TG_ALLOWED_USER_IDS": "1",
                "INTERNAL_API_ENABLED": "1",
                "INTERNAL_API_TOKEN": "secret",
                "INTERNAL_API_HOST": "0.0.0.0",
                "INTERNAL_API_PORT": "9876",
                "INTERNAL_API_WORKERS": "3",
                "INTERNAL_API_OWNER_WORKERS": "2",
                "INTERNAL_API_SEARCH_TTL_SECONDS": "900",
            }
        )
        self.assertTrue(config.internal_api_enabled)
        self.assertEqual(config.internal_api_token, "secret")
        self.assertEqual(config.internal_api_host, "0.0.0.0")
        self.assertEqual(config.internal_api_port, 9876)
        self.assertEqual(config.internal_api_workers, 3)
        self.assertEqual(config.internal_api_owner_workers, 2)
        self.assertEqual(config.internal_api_search_ttl_seconds, 900)


class SearchResponseTest(InternalApiTestCase):
    def test_empty_search_is_a_successful_response_with_pansou_capability(self):
        class EmptySearchService(FakePipelineService):
            def search(self, query, category, limit=20):
                return ResultList([], metadata={"profile": category})

        _, _, _, application = self.build_components(EmptySearchService())

        response = application.search(
            {"owner_id": "owner-a", "query": "missing", "category": "movie", "source": "default"}
        )

        self.assertEqual(response["items"], [])
        self.assertTrue(response["capabilities"]["pansou"])

    def test_search_failure_keeps_error_semantics_and_exposes_safe_capabilities(self):
        class FailingSearchService(FakePipelineService):
            def search(self, query, category, limit=20):
                raise TimeoutError("BT4G timed out")

        _, _, _, application = self.build_components(FailingSearchService())

        with self.assertRaises(ApiError) as raised:
            application.search(
                {"owner_id": "owner-a", "query": "missing", "category": "movie", "source": "default"}
            )

        self.assertEqual(raised.exception.status, 502)
        self.assertEqual(raised.exception.code, "search_failed")
        self.assertTrue(raised.exception.details["capabilities"]["pansou"])


class PipelineTargetOverrideTest(InternalApiTestCase):
    def test_service_uses_explicit_target_for_scrape_and_maintenance(self):
        service = PipelineBotService(BotConfig(token="token", allowed_user_ids={1}))

        class Client:
            def __init__(self):
                self.call = None

            def pipeline_scrape_media(self, *args):
                self.call = args
                return {"mode": "smart", "query": "Sintel", "applied_count": 1}

        client = Client()
        service._scrape_msg_media(client, "movie", "media-1", "Sintel", {}, target=TARGET)

        self.assertEqual(client.call[-2:], (TARGET["provider"], TARGET["media_type"]))
        self.assertEqual(
            service._pipeline_maintenance_target("movie", TARGET),
            {
                "category": "movie",
                "library_id": TARGET["library_id"],
                "root_id": TARGET["root_id"],
                "root_openlist_path": TARGET["root_openlist_path"],
            },
        )

    def test_duplicate_check_filters_by_explicit_target_library(self):
        config = BotConfig(token="token", allowed_user_ids={1}, msg_enabled=True)
        service = PipelineBotService(config)

        class Client:
            def search_media(self, query, limit=20):
                return {
                    "items": [
                        {"id": "wrong-media", "title": "Sintel", "library_id": "configured-library"},
                        {"id": "target-media", "title": "Sintel", "library_id": TARGET["library_id"]},
                    ]
                }

        service._build_msg_client = lambda: Client()
        duplicate = service.check_duplicate(
            "movie",
            "Sintel",
            {"title": "Sintel", "download_uri": "magnet:?xt=urn:btih:ABC"},
            target=TARGET,
        )

        self.assertEqual(duplicate["media_id"], "target-media")
        self.assertTrue(duplicate["can_force"])

    def test_submit_uses_configured_category_folder_id(self):
        service = PipelineBotService(BotConfig(token="token", allowed_user_ids={1}))

        class Client:
            def __init__(self):
                self.folder_id = None

            def add_offline_urls(self, urls, folder_id):
                self.folder_id = folder_id
                return {"state": True, "data": [{"info_hash": "ABC", "state": True, "code": 0}]}

        client = Client()
        service._call_115 = lambda category, callback: callback(client)
        service.submit("movie", "magnet:?xt=urn:btih:ABC")

        self.assertEqual(client.folder_id, category_to_folder_id("movie"))


class ImportPersistenceTest(InternalApiTestCase):
    def test_persisted_candidate_controls_download_uri_and_target_is_forwarded(self):
        service, store, manager, application = self.build_components()
        session_id, candidate_id, search = self.search_candidate(application)
        payload = self.import_payload(session_id, candidate_id)
        payload["download_uri"] = "magnet:?xt=urn:btih:BROWSER_FORGED"
        task, created = manager.create_import("owner-a", "key-1", payload)
        duplicate, duplicate_created = manager.create_import("owner-a", "key-1", payload)

        self.assertTrue(created)
        self.assertFalse(duplicate_created)
        self.assertEqual(duplicate["id"], task["id"])
        manager.start()
        try:
            completed = self.wait_final(manager, "owner-a", task["id"])
        finally:
            manager.stop()

        self.assertEqual(completed["status"], "completed")
        self.assertEqual(completed["result"]["msg_media_id"], completed["msg_media_id"])
        self.assertEqual(completed["message"], "导入完成")
        self.assertEqual(completed["msg_media_title"], "MSG sintel")
        self.assertEqual(service.submit_uris, [search["items"][0]["download_uri"]])
        self.assertNotEqual(service.submit_uris[0], payload["download_uri"])
        self.assertEqual(service.sync_targets, [TARGET])
        self.assertEqual(completed["request"]["target"], TARGET)
        self.assertEqual(
            search["capabilities"],
            {"pansou": True, "bt4g": True, "llm_rerank": False},
        )
        self.assertEqual(search["metadata"]["capabilities"], search["capabilities"])
        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute(
                "update internal_api_search_sessions set expires_at = ? where id = ?",
                (int(time.time()) - 1, session_id),
            )
            conn.commit()
        finally:
            conn.close()
        repeated, repeated_created = manager.create_import("owner-a", "key-1", payload)
        self.assertFalse(repeated_created)
        self.assertEqual(repeated["id"], task["id"])
        self.assertEqual(len(service.duplicate_calls), 1)

    def test_candidate_and_import_access_are_owner_isolated(self):
        service, store, manager, application = self.build_components()
        session_id, candidate_id, _ = self.search_candidate(application, owner_id="owner-a")
        payload = self.import_payload(session_id, candidate_id)
        with self.assertRaisesRegex(ApiError, "search session not found"):
            manager.create_import("owner-b", "key-b", payload)

        task, _ = manager.create_import("owner-a", "key-a", payload)
        with self.assertRaisesRegex(ApiError, "import task not found"):
            manager.get_import("owner-b", task["id"])
        with self.assertRaisesRegex(ApiError, "import task not found"):
            manager.cancel_import("owner-b", task["id"])
        with self.assertRaisesRegex(ApiError, "import task not found"):
            manager.retry_import("owner-b", task["id"])

    def test_expired_session_and_candidate_from_another_session_are_rejected(self):
        service = FakePipelineService()
        store = InternalApiStore(self.db_path, search_ttl_seconds=1)
        manager = ImportTaskManager(service, store, poll_seconds=0.01)
        application = InternalApiApplication(service, store, manager)
        first_session, first_candidate, _ = self.search_candidate(application, query="first")
        second_session, second_candidate, _ = self.search_candidate(application, query="second")

        wrong_session_payload = self.import_payload(first_session, second_candidate)
        with self.assertRaisesRegex(ApiError, "search candidate not found"):
            manager.create_import("owner-a", "wrong-session", wrong_session_payload)

        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute(
                "update internal_api_search_sessions set expires_at = ? where id = ?",
                (int(time.time()) - 1, first_session),
            )
            conn.commit()
        finally:
            conn.close()
        with self.assertRaisesRegex(ApiError, "search session expired"):
            manager.create_import("owner-a", "expired", self.import_payload(first_session, first_candidate))

    def test_idempotency_conflict_is_explicit(self):
        service, store, manager, application = self.build_components()
        session_id, candidate_id, _ = self.search_candidate(application)
        manager.create_import("owner-a", "same-key", self.import_payload(session_id, candidate_id))
        changed = self.import_payload(session_id, candidate_id, target={**TARGET, "root_id": "other-root"})
        with self.assertRaisesRegex(ApiError, "different request"):
            manager.create_import("owner-a", "same-key", changed)

    def test_unconfigured_target_path_is_rejected_before_duplicate_or_submit(self):
        service, store, manager, application = self.build_components()
        session_id, candidate_id, _ = self.search_candidate(application)
        payload = self.import_payload(
            session_id,
            candidate_id,
            target={**TARGET, "root_openlist_path": "/115/not-configured"},
        )

        with self.assertRaises(ApiError) as raised:
            manager.create_import("owner-a", "bad-target", payload)
        self.assertEqual(raised.exception.status, 409)
        self.assertEqual(raised.exception.code, "target_transfer_not_configured")
        self.assertEqual(service.duplicate_calls, [])
        self.assertEqual(service.submit_uris, [])

    def test_weak_duplicate_requires_force_then_submits(self):
        duplicate = {
            "level": "weak",
            "reason": "mediastation_title",
            "source": "MediaStationGo",
            "title": "Sintel",
            "media_id": "media-existing",
            "can_force": True,
            "path": "/sensitive/path",
        }
        service, store, manager, application = self.build_components(FakePipelineService(duplicate=duplicate))
        session_id, candidate_id, _ = self.search_candidate(application)
        payload = self.import_payload(session_id, candidate_id)

        with self.assertRaises(ApiError) as raised:
            manager.create_import("owner-a", "weak-duplicate", payload)
        self.assertEqual(raised.exception.status, 409)
        self.assertEqual(
            raised.exception.details["duplicate"],
            {
                "level": "weak",
                "reason": "mediastation_title",
                "source": "MediaStationGo",
                "title": "Sintel",
                "media_id": "media-existing",
                "can_force": True,
            },
        )
        self.assertEqual(service.submit_uris, [])
        payload["force_duplicate"] = True
        task, _ = manager.create_import("owner-a", "weak-duplicate-forced", payload)
        manager.start()
        try:
            completed = self.wait_final(manager, "owner-a", task["id"])
        finally:
            manager.stop()
        self.assertEqual(completed["status"], "completed")
        self.assertEqual(len(service.submit_uris), 1)
        self.assertEqual(service.duplicate_calls[-1][3], TARGET)

    def test_strong_duplicate_cannot_be_forced_and_never_submits(self):
        duplicate = {
            "level": "strong",
            "reason": "mediastation_code",
            "source": "MediaStationGo",
            "title": "SSIS-218",
            "media_id": "media-strong",
            "can_force": False,
        }
        service, store, manager, application = self.build_components(FakePipelineService(duplicate=duplicate))
        session_id, candidate_id, _ = self.search_candidate(application, query="SSIS-218", category="adult")
        payload = self.import_payload(session_id, candidate_id, category="adult", target=target_for("adult"))
        payload["force_duplicate"] = True

        with self.assertRaises(ApiError) as raised:
            manager.create_import("owner-a", "strong-duplicate", payload)
        self.assertEqual(raised.exception.status, 409)
        self.assertFalse(raised.exception.details["duplicate"]["can_force"])
        self.assertEqual(raised.exception.details["duplicate"]["media_id"], "media-strong")
        self.assertEqual(service.submit_uris, [])

    def test_running_tasks_are_recovered_to_queue(self):
        service, store, manager, application = self.build_components()
        session_id, candidate_id, _ = self.search_candidate(application)
        task, _ = manager.create_import("owner-a", "recover-key", self.import_payload(session_id, candidate_id))
        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute(
                "update internal_api_imports set status = 'running', stage = 'scanning' where id = ?",
                (task["id"],),
            )
            conn.commit()
        finally:
            conn.close()

        self.assertEqual(store.recover_running_imports(), 1)
        recovered = manager.get_import("owner-a", task["id"])
        self.assertEqual((recovered["status"], recovered["stage"]), ("queued", "queued"))

    def test_download_recovery_reuses_info_hash_without_resubmit(self):
        service, store, manager, application = self.build_components()
        session_id, candidate_id, _ = self.search_candidate(application)
        task, _ = manager.create_import("owner-a", "download-recovery", self.import_payload(session_id, candidate_id))
        recovered_result = {
            "task": {"info_hash": "RECOVER-HASH", "status_name": "downloading", "percent_done": 30}
        }
        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute(
                """
                update internal_api_imports
                set status = 'running', stage = 'waiting_download', result_json = ?, info_hash = ?
                where id = ?
                """,
                (json.dumps(recovered_result), "RECOVER-HASH", task["id"]),
            )
            conn.commit()
        finally:
            conn.close()

        manager.start()
        try:
            completed = self.wait_final(manager, "owner-a", task["id"])
        finally:
            manager.stop()

        self.assertEqual(completed["status"], "completed")
        self.assertEqual(service.submit_uris, [])
        self.assertEqual(service.task_status_calls, [("movie", "RECOVER-HASH")])

    def test_msg_sync_recovery_reuses_progress_and_ingest_idempotency(self):
        service, store, manager, application = self.build_components()
        session_id, candidate_id, _ = self.search_candidate(application)
        task, _ = manager.create_import("owner-a", "msg-recovery", self.import_payload(session_id, candidate_id))
        recovered_task = {
            "info_hash": "SYNC-HASH",
            "status_name": "success",
            "msg_sync_status": "running",
            "msg_scan_status": "running",
            "msg_ingest_status": "running",
            "msg_ingest_job_id": "job-existing",
            "msg_ingest_idempotency_key": "media-pipeline:existing",
        }
        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute(
                """
                update internal_api_imports
                set status = 'running', stage = 'scanning', result_json = ?, info_hash = ?
                where id = ?
                """,
                (json.dumps({"task": recovered_task}), "SYNC-HASH", task["id"]),
            )
            conn.commit()
        finally:
            conn.close()

        manager.start()
        try:
            completed = self.wait_final(manager, "owner-a", task["id"])
        finally:
            manager.stop()

        self.assertEqual(completed["status"], "completed")
        self.assertEqual(service.submit_uris, [])
        self.assertEqual(service.task_status_calls, [])
        self.assertEqual(service.sync_input_tasks[0]["msg_ingest_job_id"], "job-existing")
        self.assertEqual(
            service.sync_input_tasks[0]["msg_ingest_idempotency_key"],
            "media-pipeline:existing",
        )

    def test_recovery_preserves_pending_cancel_and_cancels_original_task(self):
        service, store, manager, application = self.build_components()
        session_id, candidate_id, _ = self.search_candidate(application)
        task, _ = manager.create_import("owner-a", "cancel-recovery", self.import_payload(session_id, candidate_id))
        recovered_result = {
            "task": {"info_hash": "CANCEL-HASH", "status_name": "downloading", "percent_done": 20}
        }
        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute(
                """
                update internal_api_imports
                set status = 'running', stage = 'waiting_download', result_json = ?,
                    info_hash = ?, cancel_requested = 1
                where id = ?
                """,
                (json.dumps(recovered_result), "CANCEL-HASH", task["id"]),
            )
            conn.commit()
        finally:
            conn.close()

        self.assertEqual(store.recover_running_imports(), 1)
        recovered = manager.get_import("owner-a", task["id"])
        self.assertTrue(recovered["cancel_requested"])
        manager.start()
        try:
            canceled = self.wait_final(manager, "owner-a", task["id"])
        finally:
            manager.stop()

        self.assertEqual(canceled["status"], "canceled")
        self.assertEqual(service.cancel_calls, [("movie", "CANCEL-HASH")])
        self.assertEqual(service.submit_uris, [])

    def test_queued_cancel_and_retry_remain_owner_scoped(self):
        service, store, manager, application = self.build_components()
        session_id, candidate_id, _ = self.search_candidate(application)
        task, _ = manager.create_import("owner-a", "cancel-key", self.import_payload(session_id, candidate_id))

        canceled = manager.cancel_import("owner-a", task["id"])
        self.assertEqual(canceled["status"], "canceled")
        retried = manager.retry_import("owner-a", task["id"])
        self.assertEqual(retried["status"], "queued")


class ImportResultSemanticsTest(InternalApiTestCase):
    def test_media_id_with_subtitle_failure_is_completed_with_warning(self):
        service, store, manager, application = self.build_components(FakePipelineService(warning=True))
        session_id, candidate_id, _ = self.search_candidate(application)
        task, _ = manager.create_import("owner-a", "warning-key", self.import_payload(session_id, candidate_id))
        manager.start()
        try:
            completed = self.wait_final(manager, "owner-a", task["id"])
        finally:
            manager.stop()

        self.assertEqual(completed["status"], "completed_with_warning")
        self.assertTrue(completed["msg_media_id"])
        self.assertIn("subtitle_match failed", "; ".join(completed["result"]["warnings"]))

    def test_missing_media_id_is_failed_not_completed(self):
        service, store, manager, application = self.build_components(FakePipelineService(missing_media_id=True))
        session_id, candidate_id, _ = self.search_candidate(application)
        task, _ = manager.create_import("owner-a", "missing-media-key", self.import_payload(session_id, candidate_id))
        manager.start()
        try:
            completed = self.wait_final(manager, "owner-a", task["id"])
        finally:
            manager.stop()

        self.assertEqual(completed["status"], "failed")
        self.assertIsNone(completed["msg_media_id"])
        self.assertIn("without msg_media_id", completed["error"])

    def test_msg_sync_error_is_preserved_as_import_error(self):
        service, store, manager, application = self.build_components(
            FakePipelineService(sync_error="target OpenList scan failed")
        )
        session_id, candidate_id, _ = self.search_candidate(application)
        task, _ = manager.create_import(
            "owner-a", "sync-error-key", self.import_payload(session_id, candidate_id)
        )
        manager.start()
        try:
            completed = self.wait_final(manager, "owner-a", task["id"])
        finally:
            manager.stop()

        self.assertEqual(completed["status"], "failed")
        self.assertEqual(completed["error"], "target OpenList scan failed")


class ImportConcurrencyTest(InternalApiTestCase):
    def create_task(self, manager, application, owner, key, query, target):
        session_id, candidate_id, _ = self.search_candidate(
            application,
            owner_id=owner,
            query="%s|%s" % (owner, query),
        )
        payload = self.import_payload(session_id, candidate_id, target=target)
        payload["owner_id"] = owner
        return manager.create_import(owner, key, payload)[0]

    def test_global_and_owner_worker_limits(self):
        service = FakePipelineService(sync_delay=0.08)
        service, store, manager, application = self.build_components(service, workers=3, owner_workers=2)
        tasks = [
            ("owner-a", self.create_task(manager, application, "owner-a", "a1", "a1", {**TARGET, "root_id": "r1"})),
            ("owner-a", self.create_task(manager, application, "owner-a", "a2", "a2", {**TARGET, "root_id": "r2"})),
            ("owner-a", self.create_task(manager, application, "owner-a", "a3", "a3", {**TARGET, "root_id": "r3"})),
            ("owner-b", self.create_task(manager, application, "owner-b", "b1", "b1", {**TARGET, "root_id": "r4"})),
            ("owner-c", self.create_task(manager, application, "owner-c", "c1", "c1", {**TARGET, "root_id": "r5"})),
        ]
        manager.start()
        try:
            for owner, task in tasks:
                self.assertEqual(self.wait_final(manager, owner, task["id"], timeout=5)["status"], "completed")
        finally:
            manager.stop()

        self.assertEqual(service.max_active, 3)
        self.assertEqual(service.max_by_owner["owner-a"], 2)

    def test_same_library_and_root_only_serializes_msg_sync(self):
        service = FakePipelineService(sync_delay=0.08, download_delay=0.08)
        service, store, manager, application = self.build_components(service, workers=3, owner_workers=2)
        first = self.create_task(manager, application, "owner-a", "same-1", "same1", TARGET)
        second = self.create_task(manager, application, "owner-b", "same-2", "same2", TARGET)
        manager.start()
        try:
            self.wait_final(manager, "owner-a", first["id"], timeout=5)
            self.wait_final(manager, "owner-b", second["id"], timeout=5)
        finally:
            manager.stop()

        self.assertEqual(service.max_by_target[(TARGET["library_id"], TARGET["root_id"])], 1)
        self.assertEqual(service.max_download_active, 2)

    def test_different_roots_can_sync_concurrently(self):
        service = FakePipelineService(sync_delay=0.08)
        service, store, manager, application = self.build_components(service, workers=3, owner_workers=2)
        first_target = {**TARGET, "root_id": "root-one"}
        second_target = {**TARGET, "root_id": "root-two"}
        first = self.create_task(manager, application, "owner-a", "different-1", "different1", first_target)
        second = self.create_task(manager, application, "owner-b", "different-2", "different2", second_target)
        manager.start()
        try:
            self.wait_final(manager, "owner-a", first["id"], timeout=5)
            self.wait_final(manager, "owner-b", second["id"], timeout=5)
        finally:
            manager.stop()

        self.assertEqual(service.max_active, 2)


class HttpAuthenticationTest(InternalApiTestCase):
    def test_health_is_public_but_other_endpoints_require_bearer(self):
        service = FakePipelineService()
        port = free_tcp_port()
        server = InternalApiServer(service, self.db_path, token="secret", port=port, workers=1, owner_workers=1)
        server.start()
        try:
            with urllib.request.urlopen("http://127.0.0.1:%d/health" % port, timeout=2) as response:
                body = json.loads(response.read().decode("utf-8"))
            self.assertEqual(body, {"status": "ok"})

            search_request = urllib.request.Request(
                "http://127.0.0.1:%d/v1/search" % port,
                data=json.dumps({"owner_id": "owner-a"}).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with self.assertRaises(urllib.error.HTTPError) as raised:
                urllib.request.urlopen(search_request, timeout=2)
            self.assertEqual(raised.exception.code, 401)
            unauthorized = json.loads(raised.exception.read().decode("utf-8"))
            self.assertEqual(unauthorized["error"]["code"], "unauthorized")
        finally:
            server.stop()

    def test_http_duplicate_response_contains_safe_summary(self):
        service = FakePipelineService(
            duplicate={
                "level": "weak",
                "reason": "mediastation_title",
                "source": "MediaStationGo",
                "title": "Sintel",
                "media_id": "media-existing",
                "can_force": True,
                "path": "/sensitive/path",
            }
        )
        port = free_tcp_port()
        server = InternalApiServer(service, self.db_path, token="secret", port=port, workers=1, owner_workers=1)
        server.start()
        try:
            search = http_json(
                "http://127.0.0.1:%d/v1/search" % port,
                {"owner_id": "owner-a", "query": "Sintel", "category": "movie", "source": "default"},
                token="secret",
            )
            payload = {
                "owner_id": "owner-a",
                "search_session_id": search["session_id"],
                "candidate_id": search["items"][0]["candidate_id"],
                "category": "movie",
                **TARGET,
            }
            with self.assertRaises(urllib.error.HTTPError) as raised:
                http_json(
                    "http://127.0.0.1:%d/v1/imports" % port,
                    payload,
                    token="secret",
                    headers={"Idempotency-Key": "http-duplicate"},
                )
            self.assertEqual(raised.exception.code, 409)
            error = json.loads(raised.exception.read().decode("utf-8"))["error"]
            self.assertEqual(error["code"], "duplicate_media")
            self.assertEqual(error["duplicate"]["media_id"], "media-existing")
            self.assertTrue(error["duplicate"]["can_force"])
            self.assertNotIn("path", error["duplicate"])
            self.assertEqual(service.submit_uris, [])
        finally:
            server.stop()


def free_tcp_port():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]
    finally:
        sock.close()


def http_json(url, payload, token=None, headers=None):
    request_headers = {"Content-Type": "application/json", **dict(headers or {})}
    if token:
        request_headers["Authorization"] = "Bearer " + token
    request = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers=request_headers,
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=2) as response:
        return json.loads(response.read().decode("utf-8"))


if __name__ == "__main__":
    unittest.main()
