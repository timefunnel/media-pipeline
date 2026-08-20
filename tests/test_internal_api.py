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
from pipeline.bot import (
    parse_follow_episode,
    wait_openlist_directory,
    wait_openlist_offline_result_names,
    wait_openlist_receive_entry_count,
    wait_openlist_receive_move,
    wait_openlist_receive_root_entries,
)
from pipeline.config import category_to_folder_id, category_to_openlist_path
from pipeline.internal_api import (
    ApiError,
    ImportTaskManager,
    InternalApiApplication,
    InternalApiServer,
    InternalApiStore,
    subscription_source_block_key,
    upgrade_target_scrape_queries,
)


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


class OpenListReceiveMoveWaitTest(unittest.TestCase):
    def test_retries_only_the_receive_pending_error_until_move_succeeds(self):
        class FakeOpenList:
            def __init__(self):
                self.calls = 0

            def move_names(self, src_dir, dst_dir, names):
                self.calls += 1
                if self.calls == 1:
                    raise RuntimeError("OpenList move failed: code: 990007, message: receive still running")

        client = FakeOpenList()
        wait_openlist_receive_move(
            client,
            "/115/temp",
            "/115/temp/staging",
            ["received-folder"],
            timeout_seconds=1,
            poll_seconds=0,
            wait_fn=lambda _seconds: None,
        )
        self.assertEqual(client.calls, 2)

    def test_does_not_retry_other_move_failures(self):
        class FakeOpenList:
            def __init__(self):
                self.calls = 0

            def move_names(self, src_dir, dst_dir, names):
                self.calls += 1
                raise RuntimeError("OpenList move failed: permission denied")

        client = FakeOpenList()
        with self.assertRaisesRegex(RuntimeError, "permission denied"):
            wait_openlist_receive_move(client, "/115/temp", "/115/temp/staging", ["received-folder"])
        self.assertEqual(client.calls, 1)

    def test_waits_until_upgrade_directory_is_visible(self):
        class FakeOpenList:
            def __init__(self):
                self.calls = 0

            def list_path(self, path, refresh=False):
                self.calls += 1
                if self.calls == 1:
                    raise RuntimeError("OpenList list failed: object not found")
                return {"code": 200, "data": {"content": []}}

        client = FakeOpenList()
        result = wait_openlist_directory(
            client,
            "/115/anime",
            "Show [upgrade-task]",
            timeout_seconds=1,
            poll_seconds=0,
            wait_fn=lambda _seconds: None,
        )

        self.assertEqual(result["name"], "Show [upgrade-task]")
        self.assertEqual(client.calls, 2)

    def test_retries_receive_root_read_timeout_until_listing_succeeds(self):
        class FakeOpenList:
            def __init__(self):
                self.calls = 0

            def list_all(self, path, refresh=False):
                self.calls += 1
                if path != "/115/temp":
                    raise AssertionError("unexpected receive root path")
                if not refresh:
                    raise AssertionError("receive root listing must refresh")
                if self.calls == 1:
                    raise RuntimeError("OpenList request failed: The read operation timed out")
                return [{"name": "received-folder"}]

        client = FakeOpenList()
        entries = wait_openlist_receive_root_entries(
            client,
            "/115/temp",
            timeout_seconds=1,
            poll_seconds=0,
            wait_fn=lambda _seconds: None,
        )
        self.assertEqual(entries, [{"name": "received-folder"}])
        self.assertEqual(client.calls, 2)

    def test_does_not_retry_other_receive_root_listing_failures(self):
        class FakeOpenList:
            def __init__(self):
                self.calls = 0

            def list_all(self, _path, refresh=False):
                self.calls += 1
                raise RuntimeError("OpenList list failed: permission denied")

        client = FakeOpenList()
        with self.assertRaisesRegex(RuntimeError, "permission denied"):
            wait_openlist_receive_root_entries(client, "/115/temp")
        self.assertEqual(client.calls, 1)

    def test_waits_for_direct_receive_entry_count_without_matching_names(self):
        class FakeOpenList:
            def __init__(self):
                self.calls = 0

            def list_all(self, path, refresh=False):
                self.calls += 1
                self.assert_request = (path, refresh)
                if self.calls == 1:
                    return [{"name": "01.mkv"}]
                return [{"name": "01.mkv"}, {"name": "02.mkv"}]

        client = FakeOpenList()
        entries = wait_openlist_receive_entry_count(
            client,
            "/115/临时/追更任务/series/task",
            2,
            timeout_seconds=1,
            poll_seconds=0,
            wait_fn=lambda _seconds: None,
        )

        self.assertEqual(len(entries), 2)
        self.assertEqual(client.calls, 2)
        self.assertEqual(client.assert_request, ("/115/临时/追更任务/series/task", True))

    def test_direct_receive_entry_count_rejects_unexpected_extra_items(self):
        class FakeOpenList:
            def list_all(self, _path, refresh=False):
                return [{"name": "01.mkv"}, {"name": "02.mkv"}]

        with self.assertRaisesRegex(RuntimeError, "exceeds 115 result: expected=1 actual=2"):
            wait_openlist_receive_entry_count(FakeOpenList(), "/115/temp/task", 1, timeout_seconds=0)

    def test_offline_result_wait_matches_the_completed_task_name(self):
        class FakeOpenList:
            def __init__(self):
                self.calls = 0

            def list_all(self, path, refresh=False):
                self.calls += 1
                self.assert_request = (path, refresh)
                if self.calls == 1:
                    return [{"name": "追更任务", "is_dir": True}]
                return [
                    {"name": "追更任务", "is_dir": True},
                    {"name": "凡人修仙传.115-120", "is_dir": True},
                ]

        client = FakeOpenList()
        names = wait_openlist_offline_result_names(
            client,
            "/115/临时",
            "凡人修仙传.115-120",
            timeout_seconds=1,
            poll_seconds=0,
            wait_fn=lambda _seconds: None,
        )

        self.assertEqual(names, ["凡人修仙传.115-120"])
        self.assertEqual(client.assert_request, ("/115/临时", True))
        self.assertEqual(client.calls, 2)

    def test_offline_result_wait_rejects_a_different_result_name(self):
        class FakeOpenList:
            def list_all(self, _path, refresh=False):
                return [{"name": "另一部动画", "is_dir": True}]

        with self.assertRaisesRegex(RuntimeError, "does not match task"):
            wait_openlist_offline_result_names(
                FakeOpenList(), "/115/临时", "凡人修仙传", timeout_seconds=0
            )

    def test_offline_result_wait_rejects_multiple_unclaimed_entries(self):
        class FakeOpenList:
            def list_all(self, _path, refresh=False):
                return [
                    {"name": "凡人修仙传", "is_dir": True},
                    {"name": "未知目录", "is_dir": True},
                ]

        with self.assertRaisesRegex(RuntimeError, "multiple offline results"):
            wait_openlist_offline_result_names(
                FakeOpenList(), "/115/临时", "凡人修仙传", timeout_seconds=0
            )

    def test_offline_result_wait_times_out_without_an_unclaimed_entry(self):
        class FakeOpenList:
            def list_all(self, _path, refresh=False):
                return [{"name": "追更任务", "is_dir": True}]

        with self.assertRaisesRegex(RuntimeError, "did not appear"):
            wait_openlist_offline_result_names(
                FakeOpenList(), "/115/临时", "凡人修仙传", timeout_seconds=0
            )

    def test_offline_result_wait_honors_stop_check_before_listing(self):
        class FakeOpenList:
            def list_all(self, _path, refresh=False):
                raise AssertionError("listing should not run after cancellation")

        with self.assertRaisesRegex(RuntimeError, "canceled"):
            wait_openlist_offline_result_names(
                FakeOpenList(),
                "/115/临时",
                "凡人修仙传",
                stop_check=lambda: (_ for _ in ()).throw(RuntimeError("canceled")),
            )

    def test_direct_receive_count_honors_stop_check_before_listing(self):
        class FakeOpenList:
            def list_all(self, _path, refresh=False):
                raise AssertionError("listing should not run after cancellation")

        with self.assertRaisesRegex(RuntimeError, "canceled"):
            wait_openlist_receive_entry_count(
                FakeOpenList(),
                "/115/临时/追更任务/series/task",
                1,
                stop_check=lambda: (_ for _ in ()).throw(RuntimeError("canceled")),
            )


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
        upgrade_target_error=None,
        upgrade_remove_error=None,
        upgrade_duplicate_match=False,
        upgrade_duplicate_match_error=None,
        defer_enhancements=False,
        post_enhancement_error="",
    ):
        self.warning = warning
        self.missing_media_id = missing_media_id
        self.sync_error = sync_error
        self.sync_delay = sync_delay
        self.download_delay = download_delay
        self.duplicate = duplicate
        self.upgrade_target_error = upgrade_target_error
        self.upgrade_remove_error = upgrade_remove_error
        self.upgrade_duplicate_match = upgrade_duplicate_match
        self.upgrade_duplicate_match_error = upgrade_duplicate_match_error
        self.defer_enhancements = bool(defer_enhancements)
        self.post_enhancement_error = post_enhancement_error
        self.submit_uris = []
        self.task_status_calls = []
        self.sync_targets = []
        self.sync_titles = []
        self.sync_scrape_queries = []
        self.sync_upgrade_media_ids = []
        self.sync_input_tasks = []
        self.sync_results = []
        self.post_enhancement_calls = []
        self.cancel_calls = []
        self.duplicate_calls = []
        self.upgrade_target_calls = []
        self.upgrade_duplicate_match_calls = []
        self.upgrade_remove_calls = []
        self.upgrade_prepare_calls = []
        self.import_prepare_calls = []
        self.submit_error = None
        self.submit_target_folder_ids = []
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
        self.subtitle_preview_calls = []
        self.subtitle_apply_calls = []
        self.subscription_staging = None
        self.subscription_stagings = {}
        self.subscription_entries = []
        self.subscription_prepare_calls = []
        self.subscription_validate_root_calls = []
        self.subscription_claim_calls = []
        self.subscription_inspect_hints = []
        self.subscription_cleanup_calls = []
        self.subscription_settle_calls = []
        self.subscription_cleanup_error = None
        self.subscription_verify_result = None
        self.subscription_receive_active = 0
        self.subscription_receive_max_active = 0
        self.reset_resubmit_calls = []
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


    def subtitle_search_candidates(self, media_id, limit=20):
        return {
            "media_id": media_id,
            "title": "MIDE-605",
            "category": "adult",
            "code": "MIDE-605",
            "query": "MIDE-605",
            "candidates": [
                {
                    "media_id": media_id,
                    "title": "MIDE-605",
                    "subtitle_title": "MIDE-605 简体中文",
                    "provider": "subtitlecat",
                    "provider_id": "https://subtitle.invalid/private-result",
                    "filename": "MIDE-605.zh-CN.srt",
                    "language": "zh-CN",
                    "source_score": 120,
                    "rank": 1,
                    "query": "MIDE-605",
                    "code": "MIDE-605",
                    "candidate": {"url": "https://subtitle.invalid/private-result"},
                }
            ][:limit],
        }

    def preview_subtitle_candidate(self, candidate, max_chars=8000):
        self.subtitle_preview_calls.append((dict(candidate), max_chars))
        return {
            **candidate,
            "content_sample": "第一行字幕\n第二行字幕",
            "preview_char_count": 11,
            "preview_line_count": 2,
        }

    def apply_subtitle_candidate(self, candidate):
        self.subtitle_apply_calls.append(dict(candidate))
        return {
            "subtitle_match_status": "success",
            "subtitle_match_source": candidate.get("provider"),
            "subtitle_match_filename": candidate.get("filename"),
            "subtitle_match_count": 1,
        }

    def submit(self, category, download_uri, target_folder_id=None):
        if self.submit_error is not None:
            raise self.submit_error
        with self._lock:
            self._sequence += 1
            info_hash = "HASH%03d" % self._sequence
            self.submit_uris.append(download_uri)
            self.last_submit_target_folder_id = target_folder_id
            self.submit_target_folder_ids.append(target_folder_id)
        status_name = "submitted" if self.download_delay else "success"
        task = {
            "info_hash": info_hash,
            "name": "120集全",
            "status_name": status_name,
            "percent_done": 0 if self.download_delay else 100,
        }
        return {
            "state": True,
            "submit_kind": "115_offline",
            "tasks": [task],
            "task_status": task,
            "raw": {"data": {"items": [{"name": "120集全"}]}},
        }

    def prepare_import_target(self, category, import_id, title, purpose="import"):
        self.import_prepare_calls.append((category, import_id, title, purpose))
        if purpose == "upgrade":
            self.upgrade_prepare_calls.append((category, import_id, title))
        prefix = "Upgrade" if purpose == "upgrade" else "Import"
        return {
            "folder_id": purpose + "-folder-" + import_id,
            "openlist_path": category_to_openlist_path(category).rstrip("/") + "/" + prefix + "-" + import_id,
            "folder_name": prefix + "-" + import_id,
            "purpose": purpose,
        }

    def prepare_subscription_staging(self, category, import_id, work_key):
        self.subscription_prepare_calls.append((category, import_id, work_key))
        if import_id not in self.subscription_stagings:
            self.subscription_stagings[import_id] = {
                "receive_root_folder_id": "temporary-root-cid",
                "receive_root_path": "/115/临时",
                "openlist_path": "/115/临时/追更任务/%s/%s" % (work_key, import_id),
            }
        self.subscription_staging = self.subscription_stagings[import_id]
        return dict(self.subscription_staging)

    def validate_subscription_receive_root(self, staging):
        self.subscription_validate_root_calls.append(dict(staging))
        self.subscription_receive_active += 1
        self.subscription_receive_max_active = max(
            self.subscription_receive_max_active,
            self.subscription_receive_active,
        )
        time.sleep(0.03)
        self.subscription_receive_active -= 1
        return {"receive_root_path": staging["receive_root_path"], "entries": ["追更任务"]}

    def claim_subscription_transfer(
        self, staging, submit_result, completed_task=None, stop_check=None
    ):
        if stop_check is not None:
            stop_check()
        self.subscription_claim_calls.append(
            (dict(staging), dict(submit_result), dict(completed_task or {}))
        )
        claimed = dict(staging)
        claimed.update({"received_names": ["120集全"], "claimed_at": int(time.time())})
        self.subscription_staging = dict(claimed)
        for import_id, candidate in self.subscription_stagings.items():
            if candidate.get("openlist_path") == staging.get("openlist_path"):
                self.subscription_stagings[import_id] = dict(claimed)
                break
        return claimed

    def inspect_subscription_staging(self, category, staging, season, episode_hints=None):
        self.subscription_inspect_hints.append(sorted(episode_hints or []))
        entries = [dict(item) for item in self.subscription_entries] if staging.get("claimed_at") else []
        by_episode = {}
        unknown = []
        for item in entries:
            if item.get("kind") != "video":
                continue
            episode = item.get("episode")
            if not episode:
                unknown.append(item.get("fn"))
                continue
            by_episode.setdefault(int(episode), []).append(item.get("fn"))
        return {
            "entries": entries,
            "videos": [],
            "verified_episodes": sorted(by_episode),
            "unknown_videos": unknown,
            "duplicate_episodes": {str(key): value for key, value in by_episode.items() if len(value) > 1},
        }

    def inspect_subscription_submit_result(
        self,
        category,
        submit_result,
        completed_task,
        season,
        episode_hints=None,
    ):
        by_episode = {}
        unknown = []
        for item in self.subscription_entries:
            if item.get("kind") != "video":
                continue
            episode = item.get("episode")
            if not episode:
                unknown.append(item.get("fn"))
                continue
            by_episode.setdefault(int(episode), []).append(item.get("fn"))
        return {
            "source_kind": (submit_result or {}).get("submit_kind") or "115_offline",
            "inspection_timing": "before_transfer"
            if (submit_result or {}).get("submit_kind") == "115_share_receive"
            else "after_download",
            "top_level_item_count": 1,
            "request_count": 1,
            "entry_count": len(self.subscription_entries),
            "verified_episodes": sorted(by_episode),
            "unknown_videos": sorted(unknown),
            "duplicate_episodes": {
                str(key): value for key, value in by_episode.items() if len(value) > 1
            },
        }

    def plan_subscription_promotion(self, staging, selected_episodes, season):
        selected = set(selected_episodes)
        files = [
            {
                "path": item.get("path") or "/115/anime/temp/test/" + item["fn"],
                "name": item["fn"],
                "episode": item.get("episode"),
                "kind": item.get("kind"),
            }
            for item in staging.get("entries") or []
            if item.get("episode") in selected or item.get("sidecar_episode") in selected
        ]
        return {"files": files, "episodes": sorted(selected)}

    def promote_subscription_episodes(self, category, staging, target_openlist_path, selected_episodes, season, plan=None):
        return {
            "target_openlist_path": target_openlist_path,
            "moved_paths": [item["path"] for item in (plan or {}).get("files") or []],
            "moved_names": [item["name"] for item in (plan or {}).get("files") or []],
            "reused_names": [],
            "moved_episodes": sorted(selected_episodes),
        }

    def refresh_subscription_target(self, target_openlist_path):
        return {"path": target_openlist_path}

    def verify_subscription_msg_episodes(self, category, target_openlist_path, season, selected_episodes):
        if self.subscription_verify_result is not None:
            return dict(self.subscription_verify_result)
        return {"verified_episodes": sorted(selected_episodes), "missing_episodes": [], "duplicate_episodes": {}}

    def cleanup_subscription_staging(self, category, staging):
        self.subscription_cleanup_calls.append((category, dict(staging)))
        if self.subscription_cleanup_error is not None:
            raise self.subscription_cleanup_error

    def settle_subscription_staging(self, category, staging, terminal_status, promoted=False):
        self.subscription_settle_calls.append(
            (category, dict(staging), terminal_status, bool(promoted))
        )
        if terminal_status == "failed" and not promoted and self.subscription_entries:
            return {
                "status": "retained",
                "reason": "retryable_content_before_promotion",
                "path": staging["openlist_path"],
                "entry_count": len(self.subscription_entries),
            }
        self.cleanup_subscription_staging(category, staging)
        return {
            "status": "cleaned",
            "reason": "task_canceled"
            if terminal_status == "canceled"
            else (
                "promoted_files_no_longer_need_staging"
                if promoted
                else "empty_failed_staging"
            ),
            "path": staging["openlist_path"],
            "cleaned_at": int(time.time()),
        }

    def check_duplicate(self, category, query, candidate, target=None):
        self.duplicate_calls.append((category, query, dict(candidate), dict(target or {})))
        return dict(self.duplicate) if self.duplicate is not None else None

    def validate_upgrade_target(self, media_id, target):
        self.upgrade_target_calls.append((media_id, dict(target or {})))
        if self.upgrade_target_error:
            raise ValueError(self.upgrade_target_error)
        return {"id": media_id, "library_id": (target or {}).get("library_id"), "title": "Upgrade Target"}

    def upgrade_duplicate_matches_target(self, upgrade_target, duplicate, category):
        self.upgrade_duplicate_match_calls.append((dict(upgrade_target or {}), dict(duplicate or {}), category))
        if self.upgrade_duplicate_match_error:
            raise RuntimeError(self.upgrade_duplicate_match_error)
        return self.upgrade_duplicate_match

    def remove_upgrade_target(
        self,
        old_media_id,
        new_media_id,
        target,
        upgrade_scope="media",
        new_source_paths=None,
        category=None,
    ):
        self.upgrade_remove_calls.append(
            (old_media_id, new_media_id, dict(target or {}), upgrade_scope, list(new_source_paths or []), category)
        )
        if self.upgrade_remove_error:
            raise RuntimeError(self.upgrade_remove_error)
        if upgrade_scope == "work":
            return {"status": "success", "removed": 2, "preserved": 2}
        return {"status": "removed", "media_id": old_media_id}

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
        return {
            "info_hash": info_hash,
            "name": "120集全",
            "status_name": "success",
            "percent_done": 100,
        }

    def cancel_task(self, category, info_hash):
        self.cancel_calls.append((category, info_hash))
        return {"info_hash": info_hash, "status_name": "cancelled"}

    def reset_task_for_resubmit(self, category, info_hash):
        self.reset_resubmit_calls.append((category, info_hash))
        return {"deleted": True, "task": {"info_hash": info_hash}, "response": {"state": True}}

    def sync_completed_task(
        self,
        category,
        title,
        task,
        progress_callback=None,
        target=None,
        preferred_scrape_queries=None,
        upgrade_media_id=None,
    ):
        target = dict(target or {})
        self.sync_targets.append(target)
        self.sync_titles.append(title)
        self.sync_scrape_queries.append(list(preferred_scrape_queries or []))
        self.sync_upgrade_media_ids.append(upgrade_media_id)
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
            "msg_ingest_scan_added": len(
                {
                    item.get("episode")
                    for item in self.subscription_entries
                    if item.get("kind") == "video" and item.get("episode") and int(item.get("episode")) > 114
                }
            ),
        }
        if self.defer_enhancements and task.get("defer_enhancements") and not task.get("post_enhancement_execution"):
            result.update(
                {
                    "msg_scrape_status": "deferred",
                    "msg_scrape_reason": "post_ingest_deferred",
                    "subtitle_match_status": "deferred",
                    "subtitle_match_reason": "post_ingest_deferred",
                    "post_enhancement_status": "pending",
                    "post_enhancement_stage": "scrape",
                }
            )
        elif self.defer_enhancements and task.get("post_enhancement_execution"):
            self.post_enhancement_calls.append(dict(task))
            if self.post_enhancement_error:
                result.update(
                    {
                        "msg_sync_status": "failed",
                        "msg_scan_status": "success",
                        "msg_scrape_status": "failed",
                        "msg_scrape_error": self.post_enhancement_error,
                        "post_enhancement_status": "failed",
                        "post_enhancement_stage": "scrape",
                        "post_enhancement_error": self.post_enhancement_error,
                    }
                )
            else:
                result.update(
                    {
                        "msg_scrape_status": "success",
                        "subtitle_match_status": "success",
                        "post_enhancement_status": "success",
                        "post_enhancement_stage": "completed",
                    }
                )
        if self.sync_error:
            result["msg_sync_status"] = "failed"
            result["msg_scan_status"] = "failed"
            result["msg_error"] = self.sync_error
        elif not self.missing_media_id:
            result["msg_media_id"] = "media-" + task["info_hash"]
            result["msg_media_title"] = "MSG " + title
            if category in ("tv", "anime"):
                result["msg_target_scan_status"] = "success"
                result["msg_target_scan_path"] = task.get("import_target_openlist_path") or (
                    target["root_openlist_path"].rstrip("/") + "/Imported-Show"
                )
        if self.warning:
            result["subtitle_match_status"] = "failed"
            result["subtitle_match_error"] = "subtitle provider failed"
        self.sync_results.append(dict(result))
        return result


class InternalApiTestCase(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = str(Path(self.tempdir.name) / "state.db")

    def tearDown(self):
        self.tempdir.cleanup()

    def build_components(
        self,
        service=None,
        workers=3,
        owner_workers=2,
        poll_seconds=0.01,
        offline_wait_slice_seconds=300,
    ):
        service = service or FakePipelineService()
        store = InternalApiStore(self.db_path)
        manager = ImportTaskManager(
            service,
            store,
            workers=workers,
            owner_workers=owner_workers,
            poll_seconds=poll_seconds,
            offline_wait_slice_seconds=offline_wait_slice_seconds,
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
                "INTERNAL_API_OFFLINE_WAIT_SLICE_SECONDS": "120",
                "INTERNAL_API_SEARCH_TTL_SECONDS": "900",
            }
        )
        self.assertTrue(config.internal_api_enabled)
        self.assertEqual(config.internal_api_token, "secret")
        self.assertEqual(config.internal_api_host, "0.0.0.0")
        self.assertEqual(config.internal_api_port, 9876)
        self.assertEqual(config.internal_api_workers, 3)
        self.assertEqual(config.internal_api_owner_workers, 2)
        self.assertEqual(config.internal_api_offline_wait_slice_seconds, 120)
        self.assertEqual(config.internal_api_search_ttl_seconds, 900)


class SubtitleApiTest(InternalApiTestCase):
    def test_search_preview_and_apply_keep_raw_candidate_server_side(self):
        service, _store, manager, application = self.build_components()
        try:
            search = application.search_subtitles({"owner_id": "admin", "media_id": "media-1", "limit": 10})
            self.assertEqual(search["media_id"], "media-1")
            self.assertEqual(search["items"][0]["provider"], "subtitlecat")
            self.assertNotIn("candidate", search["items"][0])
            self.assertNotIn("provider_id", search["items"][0])

            selection = {
                "owner_id": "admin",
                "media_id": "media-1",
                "search_session_id": search["session_id"],
                "candidate_id": search["items"][0]["candidate_id"],
            }
            preview = application.preview_subtitle(selection)
            self.assertEqual(preview["content_sample"], "第一行字幕\n第二行字幕")
            self.assertEqual(preview["candidate_id"], selection["candidate_id"])
            applied = application.apply_subtitle(selection)
            self.assertEqual(applied["status"], "success")
            self.assertEqual(applied["filename"], "MIDE-605.zh-CN.srt")
            self.assertEqual(len(service.subtitle_preview_calls), 1)
            self.assertEqual(len(service.subtitle_apply_calls), 1)
            self.assertEqual(service.subtitle_apply_calls[0]["candidate"]["url"], "https://subtitle.invalid/private-result")
        finally:
            manager.stop()

    def test_candidate_rejects_media_mismatch(self):
        _service, _store, manager, application = self.build_components()
        try:
            search = application.search_subtitles({"owner_id": "admin", "media_id": "media-1"})
            with self.assertRaises(ApiError) as raised:
                application.preview_subtitle(
                    {
                        "owner_id": "admin",
                        "media_id": "media-2",
                        "search_session_id": search["session_id"],
                        "candidate_id": search["items"][0]["candidate_id"],
                    }
                )
            self.assertEqual(raised.exception.status, 409)
            self.assertEqual(raised.exception.code, "subtitle_media_mismatch")
        finally:
            manager.stop()


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

    def test_manual_candidate_uses_user_title_without_resource_inspection(self):
        class NoSearchService(FakePipelineService):
            def search(self, query, category, limit=20):
                raise AssertionError("manual candidate must not call search")

        service, _, _, application = self.build_components(NoSearchService())
        response = application.prepare_manual_candidate(
            {
                "owner_id": "owner-a",
                "title": "Manual share task",
                "input": "https://115cdn.com/s/swabc123?password=xy99",
                "category": "movie",
            }
        )

        self.assertEqual(len(response["items"]), 1)
        candidate = response["items"][0]
        self.assertEqual(candidate["source_kind"], "115_share")
        self.assertEqual(candidate["resource_type"], "115_share")
        self.assertEqual(candidate["title"], "Manual share task")
        self.assertEqual(candidate["download_uri"], "https://115cdn.com/s/swabc123?password=xy99")
        self.assertIsNone(candidate["size"])
        self.assertEqual(response["metadata"]["manual_kind"], "115_share")

    def test_manual_candidate_accepts_valid_btih_magnet_without_dn(self):
        _, _, _, application = self.build_components()
        info_hash = "0123456789abcdef0123456789abcdef01234567"
        response = application.prepare_manual_candidate(
            {
                "owner_id": "owner-a",
                "title": "Manual magnet task",
                "input": "magnet:?xt=urn:btih:%s" % info_hash,
                "category": "movie",
            }
        )

        candidate = response["items"][0]
        self.assertEqual(candidate["title"], "Manual magnet task")
        self.assertEqual(candidate["infoHash"], info_hash)
        self.assertEqual(candidate["resource_type"], "magnet")

    def test_manual_candidate_preserves_magnet_display_name_with_spaces(self):
        _, _, _, application = self.build_components()
        info_hash = "cba9085cfbe940ae873dfdf20ab635c064fd274f"
        response = application.prepare_manual_candidate(
            {
                "owner_id": "owner-a",
                "title": "吞噬星空",
                "input": (
                    "magnet:?xt=urn:btih:%s&dn=【高清剧集网发布 www.BPHDTV.com】"
                    "吞噬星空 第5季[第139集]"
                )
                % info_hash,
                "category": "anime",
            }
        )

        candidate = response["items"][0]
        self.assertEqual(candidate["infoHash"], info_hash)
        self.assertNotIn(" ", candidate["download_uri"])
        self.assertIn("第139集", urllib.parse.unquote(candidate["download_uri"]))

    def test_manual_candidate_accepts_valid_ed2k_file_link(self):
        _, _, _, application = self.build_components()
        file_hash = "0123456789abcdef0123456789abcdef"
        uri = "ed2k://|file|Fanren.S01E115.mkv|123456789|%s|/" % file_hash
        response = application.prepare_manual_candidate(
            {
                "owner_id": "owner-a",
                "title": "Manual ED2K task",
                "input": uri,
                "category": "anime",
            }
        )

        candidate = response["items"][0]
        self.assertEqual(candidate["title"], "Manual ED2K task")
        self.assertEqual(candidate["download_uri"], uri)
        self.assertEqual(candidate["infoHash"], file_hash)
        self.assertEqual(candidate["resource_type"], "ed2k")
        self.assertEqual(candidate["size"], 123456789)

    def test_manual_candidate_requires_user_title(self):
        _, _, _, application = self.build_components()
        with self.assertRaises(ApiError) as raised:
            application.prepare_manual_candidate(
                {"owner_id": "owner-a", "input": "https://115.com/s/swabc123", "category": "movie"}
            )
        self.assertEqual(raised.exception.status, 400)
        self.assertEqual(raised.exception.code, "missing_title")

    def test_manual_candidate_rejects_invalid_or_unsupported_input(self):
        _, _, _, application = self.build_components()
        with self.assertRaises(ApiError) as invalid_magnet:
            application.prepare_manual_candidate(
                {"owner_id": "owner-a", "title": "Manual task", "input": "magnet:?xt=urn:btih:short", "category": "movie"}
            )
        self.assertEqual(invalid_magnet.exception.code, "invalid_magnet")

        with self.assertRaises(ApiError) as invalid_ed2k:
            application.prepare_manual_candidate(
                {"owner_id": "owner-a", "title": "Manual task", "input": "ed2k://|file|bad.mkv|12|short|/", "category": "movie"}
            )
        self.assertEqual(invalid_ed2k.exception.code, "invalid_ed2k")

        with self.assertRaises(ApiError) as unsupported:
            application.prepare_manual_candidate(
                {"owner_id": "owner-a", "title": "Manual task", "input": "ordinary text", "category": "movie"}
            )
        self.assertEqual(unsupported.exception.code, "unsupported_manual_input")


class PipelineTargetOverrideTest(InternalApiTestCase):
    def test_service_uses_explicit_target_for_scrape_and_maintenance(self):
        service = PipelineBotService(BotConfig(token="token", allowed_user_ids={1}))

        class Client:
            def __init__(self):
                self.call = None

            def pipeline_scrape_media(self, *args):
                self.call = args
                return {"mode": "smart", "query": "Sintel", "applied_count": 1, "scrape_status": "matched"}

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

    def test_work_upgrade_cleanup_sends_explicit_category(self):
        service = PipelineBotService(BotConfig(token="token", allowed_user_ids={1}))

        class Client:
            def __init__(self):
                self.call = None

            def pipeline_replace_work_source(self, *args):
                self.call = args
                return {"status": "success", "removed": 150, "preserved": 99}

        client = Client()
        service._build_msg_client = lambda: client
        target = target_for("anime", provider="tmdb", media_type="anime")

        result = service.remove_upgrade_target(
            "old-episode",
            "new-episode",
            target,
            upgrade_scope="work",
            new_source_paths=["/115/动漫/吞噬星空-new"],
            category="anime",
        )

        self.assertEqual(result["status"], "success")
        self.assertEqual(
            client.call[2],
            {
                "category": "anime",
                "library_id": target["library_id"],
                "root_id": target["root_id"],
                "root_openlist_path": target["root_openlist_path"],
            },
        )

    def test_import_target_creates_and_verifies_dedicated_folder(self):
        service = PipelineBotService(
            BotConfig(
                token="token",
                allowed_user_ids={1},
                subscription_move_timeout_seconds=1,
                subscription_move_poll_seconds=0,
            )
        )

        class Client115:
            def __init__(self):
                self.created = []
                self.list_calls = []

            def list_all_files(self, folder_id):
                self.list_calls.append(folder_id)
                return []

            def create_folder(self, name, parent_id):
                self.created.append((name, parent_id))
                return {"state": True, "data": {"file_id": "upgrade-folder"}}

            def get_folder_info(self, folder_id):
                return {"state": True, "data": {"file_id": folder_id, "file_name": self.created[0][0]}}

        class OpenList:
            def list_path(self, path, refresh=False):
                return {"code": 200, "data": {"content": []}}

        client = Client115()
        service._build_115_client = lambda _category: client
        service._build_openlist_client = lambda: OpenList()

        staging = service.prepare_import_target("anime", "abcdef1234567890", "吞噬星空", purpose="upgrade")

        self.assertEqual(staging["folder_id"], "upgrade-folder")
        self.assertEqual(staging["folder_name"], "吞噬星空 [upgrade-abcdef123456]")
        self.assertEqual(staging["openlist_path"], "/115/动漫/吞噬星空 [upgrade-abcdef123456]")
        self.assertEqual(client.created[0][1], category_to_folder_id("anime"))
        self.assertEqual(client.list_calls, [])

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

    def test_upgrade_target_must_match_explicit_library_and_root(self):
        config = BotConfig(token="token", allowed_user_ids={1}, msg_enabled=True)
        service = PipelineBotService(config)

        class Client:
            def get_media(self, media_id):
                return {
                    "id": media_id,
                    "library_id": TARGET["library_id"],
                    "library_root_id": TARGET["root_id"],
                    "title": "Sintel",
                }

        service._build_msg_client = lambda: Client()
        media = service.validate_upgrade_target("media-existing", TARGET)
        self.assertEqual(media["id"], "media-existing")

        with self.assertRaisesRegex(ValueError, "不属于当前媒体库"):
            service.validate_upgrade_target("media-existing", {**TARGET, "library_id": "other-library"})
        with self.assertRaisesRegex(ValueError, "不属于当前入库目录"):
            service.validate_upgrade_target("media-existing", {**TARGET, "root_id": "other-root"})

    def test_submit_requires_explicit_target_folder_id(self):
        service = PipelineBotService(BotConfig(token="token", allowed_user_ids={1}))

        class Client:
            def __init__(self):
                self.folder_id = None

            def add_offline_urls(self, urls, folder_id):
                self.folder_id = folder_id
                return {"state": True, "data": [{"info_hash": "ABC", "state": True, "code": 0}]}

        client = Client()
        service._call_115 = lambda category, callback: callback(client)
        with self.assertRaisesRegex(ValueError, "目标目录 ID 缺失"):
            service.submit("movie", "magnet:?xt=urn:btih:ABC")
        service.submit("movie", "magnet:?xt=urn:btih:ABC", target_folder_id="task-folder")

        self.assertEqual(client.folder_id, "task-folder")


class ImportPersistenceTest(InternalApiTestCase):
    def test_manual_magnet_candidate_creates_existing_import_task(self):
        service, _store, manager, application = self.build_components()
        info_hash = "0123456789abcdef0123456789abcdef01234567"
        magnet = "magnet:?xt=urn:btih:%s" % info_hash
        preview = application.prepare_manual_candidate(
            {"owner_id": "owner-a", "title": "Manual magnet task", "input": magnet, "category": "movie"}
        )
        payload = self.import_payload(
            preview["session_id"],
            preview["items"][0]["candidate_id"],
        )
        payload["download_uri"] = "magnet:?xt=urn:btih:ffffffffffffffffffffffffffffffffffffffff"
        task, created = manager.create_import("owner-a", "manual-magnet", payload)

        self.assertTrue(created)
        self.assertEqual(task["request"]["query"], "Manual magnet task")
        self.assertEqual(task["request"]["candidate"]["title"], "Manual magnet task")
        manager.start()
        try:
            completed = self.wait_final(manager, "owner-a", task["id"])
        finally:
            manager.stop()

        self.assertEqual(completed["status"], "completed")
        self.assertEqual(service.submit_uris, [magnet])

    def test_manual_share_candidate_persists_user_title_before_task_creation(self):
        service, _store, manager, application = self.build_components()
        preview = application.prepare_manual_candidate(
            {"owner_id": "owner-a", "title": "Manual share task", "input": "https://115.com/s/swabc123", "category": "tv"}
        )
        task, created = manager.create_import(
            "owner-a",
            "manual-share",
            self.import_payload(
                preview["session_id"],
                preview["items"][0]["candidate_id"],
                category="tv",
                target=target_for("tv"),
            ),
        )

        self.assertTrue(created)
        self.assertEqual(task["request"]["query"], "Manual share task")
        self.assertEqual(task["request"]["candidate"]["title"], "Manual share task")

    def test_upgrade_target_scrape_queries_prefer_exact_tmdb_id(self):
        self.assertEqual(
            upgrade_target_scrape_queries(
                {"tmdb_id": 608, "title": "黑衣人2", "original_name": "Men in Black II"},
                {"provider": "tmdb"},
            ),
            ["[tmdbid-608]", "黑衣人2", "Men in Black II"],
        )

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
        self.assertEqual(service.import_prepare_calls, [("movie", task["id"], "sintel", "import")])
        self.assertEqual(service.submit_target_folder_ids, ["import-folder-" + task["id"]])
        self.assertEqual(
            completed["result"]["task"]["import_target_openlist_path"],
            category_to_openlist_path("movie").rstrip("/") + "/Import-" + task["id"],
        )
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

    def test_post_ingest_enhancement_runs_after_core_import_and_reuses_download(self):
        service, store, manager, application = self.build_components(
            FakePipelineService(defer_enhancements=True)
        )
        session_id, candidate_id, _ = self.search_candidate(application)
        task, _ = manager.create_import("owner-a", "deferred-enhancement", self.import_payload(session_id, candidate_id))
        manager.start()
        try:
            completed = self.wait_final(manager, "owner-a", task["id"])
            deadline = time.time() + 3
            while time.time() < deadline:
                completed = manager.get_import("owner-a", task["id"])
                task_result = (completed.get("result") or {}).get("task") or {}
                if task_result.get("post_enhancement_status") == "success":
                    break
                time.sleep(0.01)
        finally:
            manager.stop()

        task_result = (completed.get("result") or {}).get("task") or {}
        self.assertEqual(completed["status"], "completed")
        self.assertEqual(task_result.get("post_enhancement_status"), "success")
        self.assertEqual(len(service.submit_uris), 1)
        self.assertEqual(len(service.sync_input_tasks), 2)
        self.assertTrue(service.sync_input_tasks[0].get("defer_enhancements"))
        self.assertTrue(service.sync_input_tasks[1].get("post_enhancement_execution"))
        self.assertEqual(service.sync_results[0]["msg_scrape_status"], "deferred")
        self.assertEqual(service.sync_results[1]["msg_scrape_status"], "success")
        self.assertEqual(len(service.post_enhancement_calls), 1)

    def test_historical_post_enhancement_false_success_is_queued_for_repair(self):
        from pipeline.internal_api import post_enhancement_needs_run, restore_deferred_post_enhancement_stages

        service, store, manager, _application = self.build_components()
        task, _ = store.create_import(
            "owner-a", "historical-false-success", {"category": "movie", "title": "Historical False Success"}
        )
        historical = {
            "info_hash": "historical-hash",
            "status_name": "success",
            "msg_sync_status": "success",
            "msg_scrape_status": "skipped",
            "msg_scrape_reason": "post_ingest_deferred",
            "post_enhancement_status": "success",
        }
        store.finish_import(task["id"], "completed", "completed", result={"task": historical})

        pending = store.list_pending_post_enhancements()
        self.assertEqual([item["id"] for item in pending], [task["id"]])
        self.assertTrue(post_enhancement_needs_run(historical))
        restore_deferred_post_enhancement_stages(historical)
        self.assertEqual(historical["msg_scrape_status"], "deferred")
        self.assertNotIn("msg_scrape_reason", historical)

        manager._run_post_enhancement(task)
        repaired = (store.get_import("owner-a", task["id"])["result"] or {}).get("task") or {}
        self.assertEqual(repaired.get("post_enhancement_status"), "success")
        self.assertEqual(repaired.get("msg_scrape_status"), "success")
        self.assertNotIn("msg_scrape_reason", repaired)
        self.assertEqual(service.submit_uris, [])

    def test_failed_post_ingest_enhancement_is_retryable_without_resubmit(self):
        service, store, manager, application = self.build_components(
            FakePipelineService(defer_enhancements=True, post_enhancement_error="scrape unavailable")
        )
        session_id, candidate_id, _ = self.search_candidate(application)
        task, _ = manager.create_import("owner-a", "deferred-enhancement-fail", self.import_payload(session_id, candidate_id))
        manager.start()
        try:
            completed = self.wait_final(manager, "owner-a", task["id"])
            deadline = time.time() + 3
            while time.time() < deadline:
                completed = manager.get_import("owner-a", task["id"])
                task_result = (completed.get("result") or {}).get("task") or {}
                if task_result.get("post_enhancement_status") == "failed":
                    break
                time.sleep(0.01)
        finally:
            manager.stop()

        self.assertEqual(completed["status"], "completed_with_warning")
        self.assertEqual((completed["result"] or {}).get("task", {}).get("post_enhancement_status"), "failed")
        service.post_enhancement_error = ""
        retried = manager.retry_import("owner-a", task["id"])
        self.assertEqual((retried["status"], retried["stage"]), ("queued", "post_enhancement"))
        manager.start()
        try:
            resolved = self.wait_final(manager, "owner-a", task["id"])
        finally:
            manager.stop()
        self.assertEqual(resolved["status"], "completed")
        self.assertEqual(len(service.submit_uris), 1)
        self.assertEqual(len(service.post_enhancement_calls), 2)

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

    def test_upgrade_allows_only_the_selected_duplicate_media(self):
        duplicate = {
            "level": "strong",
            "reason": "mediastation_code",
            "source": "MediaStationGo",
            "title": "SSIS-218",
            "media_id": "media-upgrade-target",
            "can_force": False,
        }
        service, store, manager, application = self.build_components(FakePipelineService(duplicate=duplicate))
        session_id, candidate_id, _ = self.search_candidate(application, query="SSIS-218", category="adult")
        payload = self.import_payload(session_id, candidate_id, category="adult", target=target_for("adult"))
        payload["upgrade_media_id"] = "media-upgrade-target"

        task, _ = manager.create_import("owner-a", "adult-upgrade", payload)
        manager.start()
        try:
            completed = self.wait_final(manager, "owner-a", task["id"])
        finally:
            manager.stop()

        self.assertEqual(completed["status"], "completed")
        self.assertEqual(service.upgrade_target_calls[-1], ("media-upgrade-target", target_for("adult")))
        self.assertEqual(service.sync_titles[-1], "Upgrade Target")
        self.assertEqual(service.sync_scrape_queries[-1], ["Upgrade Target"])
        self.assertEqual(service.sync_upgrade_media_ids[-1], "media-upgrade-target")
        self.assertEqual(len(service.submit_uris), 1)

    def test_upgrade_can_move_selected_old_version_to_recycle_bin(self):
        duplicate = {
            "level": "strong",
            "reason": "mediastation_code",
            "source": "MediaStationGo",
            "title": "SSIS-218",
            "media_id": "media-upgrade-target",
            "can_force": False,
        }
        service, store, manager, application = self.build_components(FakePipelineService(duplicate=duplicate))
        session_id, candidate_id, _ = self.search_candidate(application, query="SSIS-218", category="adult")
        payload = self.import_payload(session_id, candidate_id, category="adult", target=target_for("adult"))
        payload["upgrade_media_id"] = "media-upgrade-target"
        payload["keep_old_version"] = False

        task, _ = manager.create_import("owner-a", "adult-upgrade-replace", payload)
        manager.start()
        try:
            completed = self.wait_final(manager, "owner-a", task["id"])
        finally:
            manager.stop()

        self.assertEqual(completed["status"], "completed")
        self.assertEqual(
            service.upgrade_remove_calls,
            [("media-upgrade-target", completed["msg_media_id"], target_for("adult"), "media", [], "adult")],
        )
        self.assertEqual(completed["result"]["upgrade_cleanup"]["status"], "removed")

    def test_upgrade_cleanup_failure_keeps_old_version_and_reports_warning(self):
        duplicate = {
            "level": "strong",
            "reason": "mediastation_code",
            "source": "MediaStationGo",
            "title": "SSIS-218",
            "media_id": "media-upgrade-target",
            "can_force": False,
        }
        service, store, manager, application = self.build_components(
            FakePipelineService(duplicate=duplicate, upgrade_remove_error="MSG recycle unavailable")
        )
        session_id, candidate_id, _ = self.search_candidate(application, query="SSIS-218", category="adult")
        payload = self.import_payload(session_id, candidate_id, category="adult", target=target_for("adult"))
        payload["upgrade_media_id"] = "media-upgrade-target"
        payload["keep_old_version"] = False

        task, _ = manager.create_import("owner-a", "adult-upgrade-warning", payload)
        manager.start()
        try:
            completed = self.wait_final(manager, "owner-a", task["id"])
        finally:
            manager.stop()

        self.assertEqual(completed["status"], "completed_with_warning")
        self.assertIn("旧片源移入回收站失败", completed["error"])
        self.assertTrue(completed["msg_media_id"])

    def test_warning_retry_starts_at_subtitle_stage_without_resubmitting(self):
        service, store, manager, application = self.build_components(FakePipelineService(warning=True))
        session_id, candidate_id, _ = self.search_candidate(application)
        task, _ = manager.create_import("owner-a", "subtitle-warning-retry", self.import_payload(session_id, candidate_id))
        manager.start()
        try:
            completed = self.wait_final(manager, "owner-a", task["id"])
        finally:
            manager.stop()

        self.assertEqual(completed["status"], "completed_with_warning")
        self.assertEqual(completed["result"]["task"]["subtitle_match_status"], "failed")
        self.assertEqual(len(service.submit_uris), 1)
        self.assertEqual(len(service.sync_input_tasks), 1)

        service.warning = False
        retried = manager.retry_import("owner-a", task["id"])
        self.assertEqual((retried["status"], retried["stage"]), ("queued", "subtitles"))

        manager.start()
        try:
            resolved = self.wait_final(manager, "owner-a", task["id"])
        finally:
            manager.stop()

        self.assertEqual(resolved["status"], "completed")
        self.assertEqual(len(service.submit_uris), 1)
        self.assertEqual(len(service.sync_input_tasks), 2)
        self.assertEqual(service.sync_input_tasks[-1]["subtitle_match_status"], "failed")

    def test_upgrade_warning_retry_starts_at_cleanup_without_resyncing(self):
        duplicate = {
            "level": "strong",
            "reason": "mediastation_code",
            "source": "MediaStationGo",
            "title": "SSIS-218",
            "media_id": "media-upgrade-target",
            "can_force": False,
        }
        service, store, manager, application = self.build_components(
            FakePipelineService(duplicate=duplicate, upgrade_remove_error="MSG recycle unavailable")
        )
        session_id, candidate_id, _ = self.search_candidate(application, query="SSIS-218", category="adult")
        payload = self.import_payload(session_id, candidate_id, category="adult", target=target_for("adult"))
        payload["upgrade_media_id"] = "media-upgrade-target"
        payload["keep_old_version"] = False
        task, _ = manager.create_import("owner-a", "upgrade-warning-retry", payload)
        manager.start()
        try:
            completed = self.wait_final(manager, "owner-a", task["id"])
        finally:
            manager.stop()

        self.assertEqual(completed["status"], "completed_with_warning")
        self.assertEqual(len(service.submit_uris), 1)
        self.assertEqual(len(service.sync_input_tasks), 1)
        self.assertEqual(len(service.upgrade_remove_calls), 1)

        service.upgrade_remove_error = None
        retried = manager.retry_import("owner-a", task["id"])
        self.assertEqual((retried["status"], retried["stage"]), ("queued", "removing_old_version"))

        manager.start()
        try:
            resolved = self.wait_final(manager, "owner-a", task["id"])
        finally:
            manager.stop()

        self.assertEqual(resolved["status"], "completed")
        self.assertEqual(len(service.submit_uris), 1)
        self.assertEqual(len(service.sync_input_tasks), 1)
        self.assertEqual(len(service.upgrade_remove_calls), 2)
    def test_upgrade_rejects_duplicate_that_belongs_to_another_media(self):
        duplicate = {
            "level": "weak",
            "reason": "mediastation_title",
            "source": "MediaStationGo",
            "title": "Other Sintel",
            "media_id": "media-other",
            "can_force": True,
        }
        service, store, manager, application = self.build_components(FakePipelineService(duplicate=duplicate))
        session_id, candidate_id, _ = self.search_candidate(application)
        payload = self.import_payload(session_id, candidate_id)
        payload["upgrade_media_id"] = "media-upgrade-target"
        payload["force_duplicate"] = True

        with self.assertRaises(ApiError) as raised:
            manager.create_import("owner-a", "wrong-upgrade-duplicate", payload)

        self.assertEqual(raised.exception.code, "duplicate_media")
        self.assertFalse(raised.exception.details["duplicate"]["can_force"])
        self.assertEqual(raised.exception.details["duplicate"]["media_id"], "media-other")
        self.assertEqual(service.submit_uris, [])

    def test_upgrade_allows_duplicate_episode_from_the_same_tv_work(self):
        duplicate = {
            "level": "weak",
            "reason": "mediastation_title",
            "source": "MediaStationGo",
            "title": "My Royal Enemy S01E01",
            "media_id": "episode-1",
            "can_force": True,
        }
        service, store, manager, application = self.build_components(
            FakePipelineService(duplicate=duplicate, upgrade_duplicate_match=True)
        )
        session_id, candidate_id, _ = self.search_candidate(
            application,
            query="My Royal Enemy",
            category="tv",
        )
        payload = self.import_payload(session_id, candidate_id, category="tv", target=target_for("tv"))
        payload["upgrade_media_id"] = "episode-7"

        task, created = manager.create_import("owner-a", "tv-series-upgrade", payload)

        self.assertTrue(created)
        self.assertEqual(task["status"], "queued")
        self.assertEqual(len(service.upgrade_duplicate_match_calls), 1)
        self.assertEqual(service.upgrade_duplicate_match_calls[0][1]["media_id"], "episode-1")
        self.assertEqual(service.upgrade_duplicate_match_calls[0][2], "tv")

    def test_tv_upgrade_replaces_only_the_dedicated_work_source(self):
        service, store, manager, application = self.build_components()
        session_id, candidate_id, _ = self.search_candidate(
            application,
            query="My Royal Enemy",
            category="tv",
        )
        payload = self.import_payload(session_id, candidate_id, category="tv", target=target_for("tv"))
        payload["upgrade_media_id"] = "episode-7"
        payload["keep_old_version"] = False

        task, _ = manager.create_import("owner-a", "tv-work-replace", payload)
        manager.start()
        try:
            completed = self.wait_final(manager, "owner-a", task["id"])
        finally:
            manager.stop()

        self.assertEqual(completed["status"], "completed")
        self.assertEqual(completed["result"]["upgrade_cleanup"]["removed"], 2)
        staging_path = category_to_openlist_path("tv").rstrip("/") + "/Upgrade-" + task["id"]
        self.assertEqual(
            service.upgrade_prepare_calls,
            [("tv", task["id"], "Upgrade Target")],
        )
        self.assertEqual(service.submit_target_folder_ids, ["upgrade-folder-" + task["id"]])
        self.assertEqual(service.sync_input_tasks[-1]["import_target_openlist_path"], staging_path)
        self.assertEqual(
            service.upgrade_remove_calls,
            [
                (
                    "episode-7",
                    completed["msg_media_id"],
                    target_for("tv"),
                    "work",
                    [staging_path],
                    "tv",
                )
            ],
        )

    def test_tv_upgrade_recovery_preserves_legacy_staging_purpose(self):
        service, store, manager, application = self.build_components()
        session_id, candidate_id, _ = self.search_candidate(
            application,
            query="My Royal Enemy",
            category="tv",
        )
        payload = self.import_payload(session_id, candidate_id, category="tv", target=target_for("tv"))
        payload["upgrade_media_id"] = "episode-7"
        task, _ = manager.create_import("owner-a", "tv-legacy-staging", payload)
        staging_path = category_to_openlist_path("tv").rstrip("/") + "/Legacy-" + task["id"]
        legacy_result = {
            "upgrade_staging": {
                "folder_id": "legacy-upgrade-folder",
                "openlist_path": staging_path,
                "folder_name": "Legacy-" + task["id"],
            }
        }
        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute(
                "update internal_api_imports set result_json = ? where id = ?",
                (json.dumps(legacy_result), task["id"]),
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
        self.assertEqual(service.import_prepare_calls, [])
        self.assertEqual(service.submit_target_folder_ids, ["legacy-upgrade-folder"])
        self.assertEqual(service.sync_input_tasks[-1]["import_target_openlist_path"], staging_path)
        self.assertEqual(service.sync_input_tasks[-1]["import_target_purpose"], "upgrade")
        self.assertEqual(completed["result"]["import_target"]["purpose"], "upgrade")
        self.assertNotIn("upgrade_staging", completed["result"])

    def test_tv_upgrade_rejects_single_media_scope(self):
        service, store, manager, application = self.build_components()
        session_id, candidate_id, _ = self.search_candidate(application, query="Show", category="tv")
        payload = self.import_payload(session_id, candidate_id, category="tv", target=target_for("tv"))
        payload["upgrade_media_id"] = "episode-1"
        payload["upgrade_scope"] = "media"

        with self.assertRaises(ApiError) as raised:
            manager.create_import("owner-a", "tv-invalid-media-scope", payload)

        self.assertEqual(raised.exception.code, "invalid_upgrade_scope")
        self.assertEqual(service.submit_uris, [])

    def test_upgrade_rejects_invalid_target_before_duplicate_check(self):
        service, store, manager, application = self.build_components(
            FakePipelineService(upgrade_target_error="升级目标作品不属于当前媒体库")
        )
        session_id, candidate_id, _ = self.search_candidate(application)
        payload = self.import_payload(session_id, candidate_id)
        payload["upgrade_media_id"] = "media-invalid"

        with self.assertRaises(ApiError) as raised:
            manager.create_import("owner-a", "invalid-upgrade-target", payload)

        self.assertEqual(raised.exception.code, "invalid_upgrade_target")
        self.assertEqual(service.duplicate_calls, [])
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


class SubscriptionFollowImportTest(InternalApiTestCase):
    def test_episode_filename_parser_does_not_treat_resolution_as_episode(self):
        self.assertEqual(parse_follow_episode("凡人修仙传.S01E115.1080p.mkv", 1), 115)
        self.assertEqual(parse_follow_episode("第120集.mp4", 1), 120)
        self.assertIsNone(parse_follow_episode("凡人修仙传.1080p.mkv", 1))
        self.assertEqual(
            parse_follow_episode("[Hall_of_C] FanRenXiuXianZhuan_115_XHFC_39.mkv", 1, {115}),
            115,
        )
        self.assertEqual(parse_follow_episode("吞噬星空132.mp4", 1, {132}), 132)
        self.assertIsNone(parse_follow_episode("吞噬星空2132.mp4", 1, {132}))
        self.assertIsNone(
            parse_follow_episode("[Hall_of_C] FanRenXiuXianZhuan_115_XHFC_39.mkv", 1, {114})
        )
        self.assertIsNone(
            parse_follow_episode("[Hall_of_C] FanRenXiuXianZhuan_115_XHFC_39.mkv", 1, {39, 115})
        )
        self.assertIsNone(parse_follow_episode("吞噬星空.S05E139.mkv", 1))
        self.assertEqual(
            parse_follow_episode(
                "吞噬星空.S05E139.mkv", 1, allow_season_mismatch=True
            ),
            139,
        )

    def test_unclaimed_direct_staging_retry_requires_resubmit(self):
        from pipeline.internal_api import subscription_retry_resubmit

        result = {
            "task": {"status_name": "success"},
            "subscription_follow": {
                "staging": {"receive_mode": "direct_task_directory"}
            },
        }
        request = {"subscription_follow": {"manual_replenish": True}}

        self.assertTrue(subscription_retry_resubmit(result, request))

    def test_manual_replenishment_uses_known_missing_episodes_as_filename_hints(self):
        service, _store, manager, application = self.build_components()
        session_id, candidate_id, _ = self.search_candidate(
            application,
            query="吞噬星空",
            category="anime",
        )
        payload = self.import_payload(
            session_id,
            candidate_id,
            category="anime",
            target=target_for("anime"),
        )
        payload.update(
            {
                "subscription_follow": True,
                "manual_replenish": True,
                "work_key": "series:4790edb7",
                "season": 1,
                "existing_episodes": [138, 140],
                "reserved_episodes": [],
                "target_openlist_path": "/115/动漫/吞噬星空/Season 1",
                "title_class": "unknown",
            }
        )
        service.subscription_entries = [
            {
                "fid": "video-139",
                "fn": "吞噬星空.Swallowed.Star.S05E139.mkv",
                "kind": "video",
                "episode": 139,
            }
        ]
        task, _ = manager.create_import("owner-a", "swallowed-star-139", payload)
        manager.start()
        try:
            completed = self.wait_final(manager, "owner-a", task["id"])
        finally:
            manager.stop()

        self.assertEqual(completed["status"], "completed")
        self.assertTrue(service.subscription_inspect_hints)
        self.assertTrue(all(139 in value for value in service.subscription_inspect_hints))
        self.assertEqual(completed["result"]["subscription_follow"]["selected_episodes"], [139])

    def test_subscription_follow_rejects_expected_episode_input(self):
        from pipeline.internal_api import ApiError, normalize_subscription_follow

        target = target_for("anime")
        payload = {
            "subscription_follow": True,
            "manual_replenish": True,
            "work_key": "series:4790edb7",
            "season": 1,
            "existing_episodes": [1, 2, 4],
            "reserved_episodes": [],
            "expected_episodes": [3],
            "target_openlist_path": "/115/动漫/吞噬星空/Season 1",
            "title_class": "unknown",
        }
        with self.assertRaises(ApiError) as raised:
            normalize_subscription_follow(payload, "anime", target, False, "")
        self.assertEqual(raised.exception.code, "invalid_expected_episodes")

    def test_subscription_source_block_key_uses_115_share_code_and_subdirectory(self):
        self.assertEqual(
            subscription_source_block_key("https://115.com/s/swhsc313zrk?password=first"),
            subscription_source_block_key("https://115.com/s/swhsc313zrk?password=second"),
        )

    def test_subscription_source_block_key_normalizes_magnet_trackers(self):
        first = "magnet:?xt=urn:btih:ABCDEF&tr=https%3A%2F%2Ftracker-one.invalid"
        second = "magnet:?tr=https%3A%2F%2Ftracker-two.invalid&xt=urn:btih:abcdef"

        self.assertEqual(subscription_source_block_key(first), subscription_source_block_key(second))

    def test_subscription_search_excludes_blocked_source_and_returns_an_alternative(self):
        service, store, _manager, application = self.build_components()
        blocked_uri = "magnet:?xt=urn:btih:BLOCKED"
        alternative_uri = "magnet:?xt=urn:btih:ALTERNATIVE"
        requested_limits = []

        def search(_query, _category, limit=20):
            requested_limits.append(limit)
            return ResultList(
                [
                    {"title": "已拉黑资源", "download_uri": blocked_uri, "rank": 1},
                    {"title": "可用单集", "download_uri": alternative_uri, "rank": 2},
                ],
                metadata={"provider_total": 2},
            )

        service.search = search
        store.block_subscription_source(
            subscription_source_block_key(blocked_uri),
            reason="offline_failed",
            origin_import_id="failed-import",
        )

        response = application.search(
            {
                "owner_id": "owner-a",
                "query": "凡人修仙传 115",
                "category": "anime",
                "source": "default",
                "limit": 1,
                "subscription_follow": True,
            }
        )

        self.assertEqual(requested_limits, [200])
        self.assertEqual([item["title"] for item in response["items"]], ["可用单集"])
        self.assertEqual(response["metadata"]["blocked_count"], 1)
        self.assertEqual(response["metadata"]["provider_total"], 2)

    def payload(self, application, service, existing=None, reserved=None):
        session_id, candidate_id, _ = self.search_candidate(
            application,
            query="凡人修仙传 更新至120集",
            category="anime",
        )
        payload = self.import_payload(
            session_id,
            candidate_id,
            category="anime",
            target=target_for("anime"),
        )
        payload.update(
            {
                "subscription_follow": True,
                "subscription_id": "subscription-fanren",
                "work_key": "凡人修仙传",
                "season": 1,
                "existing_episodes": list(existing if existing is not None else range(1, 115)),
                "reserved_episodes": list(reserved or []),
                "target_openlist_path": category_to_openlist_path("anime").rstrip("/")
                + "/凡人修仙传 (2020)/Season 1",
                "title_class": "cumulative_pack",
            }
        )
        return payload

    def test_cumulative_pack_moves_only_missing_episodes_and_cleans_after_verification(self):
        service, store, manager, application = self.build_components()
        service.subscription_entries = [
            {"fid": "video-%03d" % episode, "fn": "凡人修仙传.S01E%03d.mkv" % episode, "kind": "video", "episode": episode}
            for episode in range(1, 121)
        ] + [
            {"fid": "subtitle-115", "fn": "凡人修仙传.S01E115.zh-CN.srt", "kind": "sidecar", "sidecar_episode": 115}
        ]
        payload = self.payload(application, service)
        task, _ = manager.create_import("owner-a", "fanren-cumulative", payload)

        manager.start()
        try:
            completed = self.wait_final(manager, "owner-a", task["id"])
        finally:
            manager.stop()

        self.assertEqual(completed["status"], "completed")
        audit = completed["result"]["subscription_follow"]
        self.assertEqual(audit["selected_episodes"], [115, 116, 117, 118, 119, 120])
        self.assertEqual(audit["moved_episodes"], [115, 116, 117, 118, 119, 120])
        self.assertEqual(audit["scan_added"], 6)
        self.assertEqual(audit["outcome"], "imported")
        self.assertEqual(service.last_submit_target_folder_id, "temporary-root-cid")
        self.assertEqual(len(service.subscription_claim_calls), 1)
        self.assertEqual(len(service.subscription_cleanup_calls), 1)
        self.assertEqual(service.subscription_cleanup_calls[0][0], "anime")
        self.assertEqual(service.subscription_cleanup_calls[0][1]["openlist_path"], audit["staging"]["openlist_path"])
        self.assertEqual(service.duplicate_calls, [])

    def test_direct_subscription_receive_submits_to_the_task_directory(self):
        service, _store, manager, application = self.build_components()
        service.subscription_entries = [
            {"fid": "video-115", "fn": "凡人修仙传.S01E115.mkv", "kind": "video", "episode": 115}
        ]
        original_prepare = service.prepare_subscription_staging

        def prepare_direct(category, import_id, work_key):
            staging = original_prepare(category, import_id, work_key)
            staging.update({"receive_mode": "direct_task_directory", "receive_folder_id": "task-folder-cid"})
            return staging

        service.prepare_subscription_staging = prepare_direct
        task, _ = manager.create_import("owner-a", "fanren-direct-receive", self.payload(application, service))

        manager.start()
        try:
            completed = self.wait_final(manager, "owner-a", task["id"])
        finally:
            manager.stop()

        self.assertEqual(completed["status"], "completed")
        self.assertEqual(service.submit_target_folder_ids, ["task-folder-cid"])
        self.assertEqual(completed["result"]["subscription_follow"]["staging"]["receive_mode"], "direct_task_directory")

    def test_running_subscription_cancel_cleans_staging(self):
        service, store, manager, application = self.build_components()
        task, _ = manager.create_import(
            "owner-a",
            "fanren-running-cancel",
            self.payload(application, service),
        )
        staging = service.prepare_subscription_staging("anime", task["id"], "凡人修仙传")
        recovered_result = {
            "task": {
                "info_hash": "CANCEL-SUBSCRIPTION-HASH",
                "status_name": "downloading",
                "percent_done": 20,
            },
            "subscription_follow": {"staging": staging},
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
                (
                    json.dumps(recovered_result),
                    "CANCEL-SUBSCRIPTION-HASH",
                    task["id"],
                ),
            )
            conn.commit()
        finally:
            conn.close()

        self.assertEqual(store.recover_running_imports(), 1)
        manager.start()
        try:
            canceled = self.wait_final(manager, "owner-a", task["id"])
        finally:
            manager.stop()

        self.assertEqual(canceled["status"], "canceled")
        self.assertEqual(service.cancel_calls, [("anime", "CANCEL-SUBSCRIPTION-HASH")])
        self.assertEqual(len(service.subscription_cleanup_calls), 1)
        cleanup = canceled["result"]["subscription_follow"]["staging_cleanup"]
        self.assertEqual(cleanup["status"], "cleaned")
        self.assertEqual(cleanup["reason"], "task_canceled")

    def test_single_episode_without_actual_episode_marker_is_rejected(self):
        service, _store, manager, application = self.build_components()
        service.subscription_entries = [
            {"fid": "video-115", "fn": "show_115_release.mkv", "kind": "video"}
        ]
        payload = self.payload(application, service)
        payload["title_class"] = "single"
        task, _ = manager.create_import("owner-a", "fanren-single", payload)

        manager.start()
        try:
            completed = self.wait_final(manager, "owner-a", task["id"])
        finally:
            manager.stop()

        self.assertEqual(completed["status"], "failed")
        self.assertTrue(service.subscription_inspect_hints)
        self.assertTrue(all(value == [] for value in service.subscription_inspect_hints))
        self.assertIn("unrecognized video names", completed["error"])

    def test_no_new_episodes_cleans_staging_and_blocks_the_source(self):
        service, store, manager, application = self.build_components()
        service.subscription_entries = [
            {"fid": "video-%03d" % episode, "fn": "凡人修仙传.S01E%03d.mkv" % episode, "kind": "video", "episode": episode}
            for episode in range(1, 115)
        ]
        payload = self.payload(application, service)
        task, _ = manager.create_import("owner-a", "fanren-no-new", payload)
        manager.start()
        try:
            completed = self.wait_final(manager, "owner-a", task["id"])
        finally:
            manager.stop()

        self.assertEqual(completed["status"], "failed")
        audit = completed["result"]["subscription_follow"]
        self.assertEqual(audit["outcome"], "no_new_episodes")
        self.assertIsInstance(audit["staging_cleaned_at"], int)
        self.assertEqual(len(service.subscription_cleanup_calls), 1)
        self.assertEqual(service.subscription_cleanup_calls[0][0], "anime")
        self.assertEqual(service.subscription_cleanup_calls[0][1]["openlist_path"], audit["staging"]["openlist_path"])
        source_key = subscription_source_block_key(completed["request"]["candidate"]["download_uri"])
        block = store.get_subscription_source_block(source_key)
        self.assertEqual(block["reason"], "no_new_episodes")
        self.assertEqual(block["origin_import_id"], completed["id"])
        with self.assertRaises(ApiError) as retry_blocked:
            manager.retry_import("owner-a", completed["id"])
        self.assertEqual(retry_blocked.exception.code, "subscription_source_blocked")
        with self.assertRaises(ApiError) as create_blocked:
            manager.create_import("owner-a", "fanren-no-new-second", payload)
        self.assertEqual(create_blocked.exception.code, "subscription_source_blocked")

    def test_manual_replenish_no_new_episodes_completes_without_blocking_source(self):
        service, store, manager, application = self.build_components()
        service.subscription_entries = [
            {"fid": "video-%03d" % episode, "fn": "凡人修仙传.S01E%03d.mkv" % episode, "kind": "video", "episode": episode}
            for episode in range(1, 115)
        ]
        payload = self.payload(application, service)
        payload["manual_replenish"] = True
        payload.pop("subscription_id")
        task, _ = manager.create_import("owner-a", "fanren-manual-no-new", payload)
        manager.start()
        try:
            completed = self.wait_final(manager, "owner-a", task["id"])
        finally:
            manager.stop()

        self.assertEqual(completed["status"], "completed")
        self.assertIsNone(completed["msg_media_id"])
        audit = completed["result"]["subscription_follow"]
        self.assertTrue(audit["manual_replenish"])
        self.assertEqual(audit["outcome"], "no_new_episodes")
        self.assertIsInstance(audit["staging_cleaned_at"], int)
        self.assertEqual(len(service.subscription_cleanup_calls), 1)
        source_key = subscription_source_block_key(completed["request"]["candidate"]["download_uri"])
        self.assertIsNone(store.get_subscription_source_block(source_key))

        next_task, created = manager.create_import(
            "owner-a", "fanren-manual-no-new-second", payload
        )
        self.assertTrue(created)
        self.assertEqual(next_task["status"], "queued")

    def test_no_new_episodes_does_not_block_source_when_staging_cleanup_fails(self):
        service, store, manager, application = self.build_components()
        service.subscription_entries = [
            {"fid": "video-%03d" % episode, "fn": "凡人修仙传.S01E%03d.mkv", "kind": "video", "episode": episode}
            for episode in range(1, 115)
        ]
        service.subscription_cleanup_error = RuntimeError("OpenList cleanup failed")
        payload = self.payload(application, service)
        task, _ = manager.create_import("owner-a", "fanren-no-new-cleanup-fails", payload)
        manager.start()
        try:
            completed = self.wait_final(manager, "owner-a", task["id"])
        finally:
            manager.stop()

        self.assertEqual(completed["status"], "failed")
        audit = completed["result"]["subscription_follow"]
        self.assertEqual(audit["outcome"], "no_new_episodes")
        self.assertNotIn("staging_cleaned_at", audit)
        self.assertEqual(len(service.subscription_cleanup_calls), 1)
        source_key = subscription_source_block_key(completed["request"]["candidate"]["download_uri"])
        self.assertIsNone(store.get_subscription_source_block(source_key))
        self.assertEqual(manager.retry_import("owner-a", completed["id"])["status"], "queued")

    def test_expired_share_cleans_staging_and_blocks_the_source(self):
        service, store, manager, application = self.build_components()
        service.submit_error = RuntimeError("115 share receive failed: 链接已过期")
        payload = self.payload(application, service)
        task, _ = manager.create_import("owner-a", "fanren-expired-share", payload)
        manager.start()
        try:
            completed = self.wait_final(manager, "owner-a", task["id"])
        finally:
            manager.stop()

        self.assertEqual(completed["status"], "failed")
        audit = completed["result"]["subscription_follow"]
        self.assertEqual(audit["outcome"], "source_unavailable")
        self.assertIsInstance(audit["staging_cleaned_at"], int)
        self.assertEqual(len(service.subscription_cleanup_calls), 1)
        source_key = subscription_source_block_key(completed["request"]["candidate"]["download_uri"])
        block = store.get_subscription_source_block(source_key)
        self.assertEqual(block["reason"], "share_expired")

    def test_transient_share_failure_cleans_empty_staging_without_blocking_source(self):
        service, store, manager, application = self.build_components()
        service.submit_error = RuntimeError("115 share receive failed: connection timed out")
        payload = self.payload(application, service)
        task, _ = manager.create_import("owner-a", "fanren-transient-share", payload)
        manager.start()
        try:
            completed = self.wait_final(manager, "owner-a", task["id"])
        finally:
            manager.stop()

        self.assertEqual(completed["status"], "failed")
        source_key = subscription_source_block_key(completed["request"]["candidate"]["download_uri"])
        self.assertIsNone(store.get_subscription_source_block(source_key))
        self.assertEqual(len(service.subscription_cleanup_calls), 1)
        cleanup = completed["result"]["subscription_follow"]["staging_cleanup"]
        self.assertEqual(cleanup["status"], "cleaned")
        self.assertEqual(cleanup["reason"], "empty_failed_staging")

    def test_terminal_cleanup_failure_is_recorded_without_replacing_import_error(self):
        service, _store, manager, application = self.build_components()
        service.submit_error = RuntimeError("115 share receive failed: connection timed out")
        service.subscription_cleanup_error = RuntimeError("OpenList cleanup failed: permission denied")
        task, _ = manager.create_import(
            "owner-a",
            "fanren-terminal-cleanup-failure",
            self.payload(application, service),
        )
        manager.start()
        try:
            completed = self.wait_final(manager, "owner-a", task["id"])
        finally:
            manager.stop()

        self.assertEqual(completed["status"], "failed")
        self.assertEqual(completed["error"], "115 share receive failed: connection timed out")
        cleanup = completed["result"]["subscription_follow"]["staging_cleanup"]
        self.assertEqual(cleanup["status"], "failed")
        self.assertEqual(cleanup["reason"], "terminal_failed_cleanup")
        self.assertEqual(cleanup["error"], "OpenList cleanup failed: permission denied")

    def test_failed_offline_task_cleans_staging_and_blocks_the_magnet(self):
        service = FakePipelineService(download_delay=0.01)
        service, store, manager, application = self.build_components(service)

        def failed_status(category, info_hash):
            service.task_status_calls.append((category, info_hash))
            return {
                "info_hash": info_hash,
                "name": "凡人修仙传.115",
                "status_name": "failed",
                "percent_done": 0,
            }

        service.task_status = failed_status
        payload = self.payload(application, service)
        task, _ = manager.create_import("owner-a", "fanren-offline-failed", payload)
        manager.start()
        try:
            completed = self.wait_final(manager, "owner-a", task["id"])
        finally:
            manager.stop()

        self.assertEqual(completed["status"], "failed")
        audit = completed["result"]["subscription_follow"]
        self.assertEqual(audit["outcome"], "source_unavailable")
        self.assertEqual(len(service.subscription_cleanup_calls), 1)
        source_key = subscription_source_block_key(completed["request"]["candidate"]["download_uri"])
        self.assertEqual(store.get_subscription_source_block(source_key)["reason"], "offline_failed")

    def test_offline_status_network_failure_cleans_empty_staging_without_blocking(self):
        service = FakePipelineService(download_delay=0.01)
        service, store, manager, application = self.build_components(service)

        def failed_status(_category, _info_hash):
            raise RuntimeError("115 offline task list failed: connection timed out")

        service.task_status = failed_status
        payload = self.payload(application, service)
        task, _ = manager.create_import("owner-a", "fanren-offline-network", payload)
        manager.start()
        try:
            completed = self.wait_final(manager, "owner-a", task["id"])
        finally:
            manager.stop()

        self.assertEqual(completed["status"], "failed")
        source_key = subscription_source_block_key(completed["request"]["candidate"]["download_uri"])
        self.assertIsNone(store.get_subscription_source_block(source_key))
        self.assertEqual(len(service.subscription_cleanup_calls), 1)
        cleanup = completed["result"]["subscription_follow"]["staging_cleanup"]
        self.assertEqual(cleanup["status"], "cleaned")
        self.assertEqual(cleanup["reason"], "empty_failed_staging")

    def test_transient_failure_retains_nonempty_staging_before_promotion(self):
        service, _store, manager, application = self.build_components()
        service.subscription_entries = [
            {"fid": "video-115", "fn": "凡人修仙传.S01E115.mkv", "kind": "video", "episode": 115}
        ]

        def fail_inspection(_category, _staging, _season, _episode_hints=None):
            raise RuntimeError("OpenList staging list failed: connection timed out")

        service.inspect_subscription_staging = fail_inspection
        task, _ = manager.create_import(
            "owner-a",
            "fanren-retain-before-promotion",
            self.payload(application, service),
        )
        manager.start()
        try:
            completed = self.wait_final(manager, "owner-a", task["id"])
        finally:
            manager.stop()

        self.assertEqual(completed["status"], "failed")
        self.assertEqual(service.subscription_cleanup_calls, [])
        cleanup = completed["result"]["subscription_follow"]["staging_cleanup"]
        self.assertEqual(cleanup["status"], "retained")
        self.assertEqual(cleanup["reason"], "retryable_content_before_promotion")
        self.assertEqual(cleanup["entry_count"], 1)

    def test_unknown_video_is_rejected_cleaned_and_blocked(self):
        service, store, manager, application = self.build_components()
        service.subscription_entries = [{"fid": "unknown", "fn": "final-video.mkv", "kind": "video"}]
        payload = self.payload(application, service)
        task, _ = manager.create_import("owner-a", "fanren-unknown", payload)
        manager.start()
        try:
            completed = self.wait_final(manager, "owner-a", task["id"])
        finally:
            manager.stop()

        self.assertEqual(completed["status"], "failed")
        self.assertEqual(completed["result"]["subscription_follow"]["outcome"], "rejected")
        self.assertEqual(len(service.subscription_cleanup_calls), 1)
        source_key = subscription_source_block_key(completed["request"]["candidate"]["download_uri"])
        self.assertEqual(store.get_subscription_source_block(source_key)["reason"], "invalid_episode_layout")

    def test_unknown_video_is_ignored_when_known_missing_episode_is_selected(self):
        service, store, manager, application = self.build_components()
        service.subscription_entries = [
            {"fid": "video-115", "fn": "凡人修仙传.S01E115.mkv", "kind": "video", "episode": 115},
            {"fid": "advertisement", "fn": "更多高清剧集请访问发布站.mkv", "kind": "video"},
        ]
        payload = self.payload(
            application,
            service,
            existing=list(range(1, 115)) + [116],
        )
        payload["manual_replenish"] = True
        payload.pop("subscription_id")
        task, _ = manager.create_import("owner-a", "fanren-known-with-advertisement", payload)
        manager.start()
        try:
            completed = self.wait_final(manager, "owner-a", task["id"])
        finally:
            manager.stop()

        self.assertEqual(completed["status"], "completed")
        audit = completed["result"]["subscription_follow"]
        self.assertEqual(audit["selected_episodes"], [115])
        self.assertEqual(audit["moved_episodes"], [115])
        self.assertEqual(audit["unknown_videos"], ["更多高清剧集请访问发布站.mkv"])
        self.assertEqual(audit["ignored_unknown_videos"], ["更多高清剧集请访问发布站.mkv"])
        self.assertEqual(len(service.subscription_cleanup_calls), 1)
        source_key = subscription_source_block_key(completed["request"]["candidate"]["download_uri"])
        self.assertIsNone(store.get_subscription_source_block(source_key))

    def test_retry_reuses_import_record_and_resubmits_after_staging_cleanup(self):
        service, _store, manager, application = self.build_components()
        service.subscription_entries = [{"fid": "unknown", "fn": "final-video.mkv", "kind": "video"}]
        payload = self.payload(
            application,
            service,
            existing=list(range(1, 115)) + [116],
        )
        payload["manual_replenish"] = True
        payload.pop("subscription_id")
        task, _ = manager.create_import("owner-a", "fanren-retry-cleaned-staging", payload)
        manager.start()
        try:
            failed = self.wait_final(manager, "owner-a", task["id"])
            service.subscription_stagings.pop(task["id"], None)
            service.subscription_staging = None
            service.subscription_entries = [
                {"fid": "video-115", "fn": "凡人修仙传.S01E115.mkv", "kind": "video", "episode": 115}
            ]
            retried = manager.retry_import("owner-a", failed["id"])
            completed = self.wait_final(manager, "owner-a", failed["id"])
        finally:
            manager.stop()

        self.assertEqual(retried["id"], failed["id"])
        self.assertEqual(completed["id"], failed["id"])
        self.assertEqual(completed["status"], "completed")
        self.assertEqual(service.reset_resubmit_calls, [("anime", "HASH001")])
        self.assertEqual(len(service.submit_uris), 2)
        self.assertEqual(service.submit_uris[0], service.submit_uris[1])
        self.assertEqual(completed["info_hash"], "HASH002")

    def test_duplicate_episode_videos_are_rejected_cleaned_and_blocked(self):
        service, store, manager, application = self.build_components()
        service.subscription_entries = [
            {"fid": "video-a", "fn": "凡人修仙传.S01E115.1080p.mkv", "kind": "video", "episode": 115},
            {"fid": "video-b", "fn": "凡人修仙传.S01E115.2160p.mkv", "kind": "video", "episode": 115},
        ]
        payload = self.payload(application, service)
        task, _ = manager.create_import("owner-a", "fanren-duplicate-episode", payload)
        manager.start()
        try:
            completed = self.wait_final(manager, "owner-a", task["id"])
        finally:
            manager.stop()

        self.assertEqual(completed["status"], "failed")
        self.assertEqual(completed["result"]["subscription_follow"]["outcome"], "rejected")
        self.assertEqual(len(service.subscription_cleanup_calls), 1)
        source_key = subscription_source_block_key(completed["request"]["candidate"]["download_uri"])
        self.assertEqual(store.get_subscription_source_block(source_key)["reason"], "invalid_episode_layout")

    def test_scan_mismatch_cleans_promoted_staging_and_retries_from_scan(self):
        service, store, manager, application = self.build_components()
        service.subscription_entries = [
            {"fid": "video-115", "fn": "凡人修仙传.S01E115.mkv", "kind": "video", "episode": 115}
        ]
        service.subscription_verify_result = {
            "verified_episodes": [],
            "missing_episodes": [115],
            "duplicate_episodes": {},
        }
        payload = self.payload(application, service)
        task, _ = manager.create_import("owner-a", "fanren-scan-mismatch", payload)
        manager.start()
        try:
            failed = self.wait_final(manager, "owner-a", task["id"])
            failed_cleanup = failed["result"]["subscription_follow"]["staging_cleanup"]
            service.subscription_verify_result = None
            retried = manager.retry_import("owner-a", failed["id"])
            completed = self.wait_final(manager, "owner-a", failed["id"])
        finally:
            manager.stop()

        self.assertEqual(failed["status"], "failed")
        self.assertEqual(failed_cleanup["status"], "cleaned")
        self.assertEqual(failed_cleanup["reason"], "promoted_files_no_longer_need_staging")
        self.assertEqual(retried["id"], failed["id"])
        self.assertEqual(completed["status"], "completed")
        self.assertEqual(len(service.submit_uris), 1)
        self.assertEqual(service.sync_input_tasks[-1]["subscription_target_season"], 1)
        self.assertEqual(len(service.subscription_cleanup_calls), 1)

    def test_restart_reuses_existing_staging_without_resubmitting_share(self):
        service, store, manager, application = self.build_components()
        service.subscription_entries = [
            {"fid": "video-115", "fn": "凡人修仙传.S01E115.mkv", "kind": "video", "episode": 115}
        ]
        payload = self.payload(application, service)
        task, _ = manager.create_import("owner-a", "fanren-staging-recovery", payload)
        staging = {
            "receive_root_folder_id": "temporary-root-cid",
            "receive_root_path": "/115/临时",
            "openlist_path": "/115/临时/追更任务/凡人修仙传/%s" % task["id"],
            "claimed_at": int(time.time()),
        }
        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute(
                """
                update internal_api_imports
                set status = 'running', stage = 'staging', result_json = ?, info_hash = null
                where id = ?
                """,
                (json.dumps({"subscription_follow": {"staging": staging}}), task["id"]),
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
        self.assertEqual(service.subscription_prepare_calls, [])
        self.assertEqual(completed["result"]["subscription_follow"]["staging"], staging)

    def test_received_share_is_claimed_after_restart_without_resubmitting(self):
        service, store, manager, application = self.build_components()
        service.subscription_entries = [
            {"fid": "video-115", "fn": "凡人修仙传.S01E115.mkv", "kind": "video", "episode": 115}
        ]
        payload = self.payload(application, service)
        task, _ = manager.create_import("owner-a", "fanren-unclaimed-recovery", payload)
        staging = {
            "receive_root_folder_id": "temporary-root-cid",
            "receive_root_path": "/115/临时",
            "openlist_path": "/115/临时/追更任务/凡人修仙传/%s" % task["id"],
        }
        submit_result = {
            "tasks": [{"info_hash": "share:abc", "status_name": "success"}],
            "task_status": {"info_hash": "share:abc", "status_name": "success"},
            "raw": {"data": {"items": [{"name": "120集全"}]}},
        }
        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute(
                """
                update internal_api_imports
                set status = 'running', stage = 'received_unclaimed', result_json = ?, info_hash = ?
                where id = ?
                """,
                (
                    json.dumps({"submit": submit_result, "subscription_follow": {"staging": staging}}),
                    "share:abc",
                    task["id"],
                ),
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
        self.assertEqual(len(service.subscription_claim_calls), 1)

    def test_restart_resubmits_completed_offline_task_when_direct_staging_is_empty(self):
        service, _store, manager, application = self.build_components()
        service.subscription_entries = [
            {"fid": "video-115", "fn": "凡人修仙传.S01E115.mkv", "kind": "video", "episode": 115}
        ]
        payload = self.payload(application, service)
        task, _ = manager.create_import("owner-a", "fanren-empty-direct-recovery", payload)
        staging = {
            "receive_mode": "direct_task_directory",
            "receive_root_folder_id": "temporary-root-cid",
            "receive_root_path": "/115/临时",
            "receive_folder_id": "task-folder-cid",
            "openlist_path": "/115/临时/追更任务/凡人修仙传/%s" % task["id"],
        }
        submit_result = {
            "submit_kind": "115_offline",
            "tasks": [{"info_hash": "OLDHASH", "status_name": "success"}],
            "task_status": {"info_hash": "OLDHASH", "status_name": "success"},
        }
        result = {
            "submit": submit_result,
            "task": {"info_hash": "OLDHASH", "status_name": "success"},
            "subscription_follow": {"staging": staging},
        }
        original_inspect = service.inspect_subscription_staging
        inspect_calls = []

        def inspect_after_redelivery(category, current_staging, season, episode_hints=None):
            inspect_calls.append(dict(current_staging))
            if len(inspect_calls) <= 2:
                return {
                    "entries": [],
                    "videos": [],
                    "verified_episodes": [],
                    "unknown_videos": [],
                    "duplicate_episodes": {},
                }
            return original_inspect(category, current_staging, season, episode_hints)

        service.inspect_subscription_staging = inspect_after_redelivery
        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute(
                """
                update internal_api_imports
                set status = 'running', stage = 'received_unclaimed', result_json = ?, info_hash = ?
                where id = ?
                """,
                (json.dumps(result), "OLDHASH", task["id"]),
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
        self.assertEqual(service.reset_resubmit_calls, [("anime", "OLDHASH")])
        self.assertEqual(len(service.submit_uris), 1)
        self.assertEqual(service.submit_target_folder_ids, ["task-folder-cid"])
        self.assertGreaterEqual(len(inspect_calls), 3)

    def test_retry_preserves_received_subscription_share_without_resubmitting(self):
        service, store, manager, application = self.build_components()
        task, _ = manager.create_import("owner-a", "fanren-retry-unclaimed", self.payload(application, service))
        staging = {
            "receive_root_folder_id": "temporary-root-cid",
            "receive_root_path": "/115/temp",
            "openlist_path": "/115/temp/staging/retry-task",
        }
        submit_result = {
            "task_status": {"info_hash": "share:abc", "status_name": "success"},
            "raw": {"data": {"items": [{"name": "received-folder"}]}},
        }
        result = {
            "submit": submit_result,
            "task": {"info_hash": "share:abc", "status_name": "success"},
            "subscription_follow": {"staging": staging},
        }
        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute(
                """
                update internal_api_imports
                set status = 'failed', stage = 'failed', result_json = ?, error = ?, info_hash = ?
                where id = ?
                """,
                (json.dumps(result), "OpenList move failed: code: 990007", "share:abc", task["id"]),
            )
            conn.commit()
        finally:
            conn.close()

        retried = manager.retry_import("owner-a", task["id"])

        self.assertEqual(retried["status"], "queued")
        self.assertEqual(retried["info_hash"], "share:abc")
        self.assertEqual(retried["result"]["submit"], submit_result)
        self.assertEqual(retried["result"]["subscription_follow"]["staging"], staging)

    def test_received_share_with_unknown_root_item_fails_without_resubmitting(self):
        service, store, manager, application = self.build_components()
        payload = self.payload(application, service)
        task, _ = manager.create_import("owner-a", "fanren-unclaimed-conflict", payload)
        staging = {
            "receive_root_folder_id": "temporary-root-cid",
            "receive_root_path": "/115/临时",
            "openlist_path": "/115/临时/追更任务/凡人修仙传/%s" % task["id"],
        }
        submit_result = {
            "tasks": [{"info_hash": "share:abc", "status_name": "success"}],
            "task_status": {"info_hash": "share:abc", "status_name": "success"},
            "raw": {"data": {"items": [{"name": "120集全"}]}},
        }

        def reject_claim(_staging, _submit_result, completed_task=None, stop_check=None):
            raise RuntimeError("subscription receive root cannot be claimed: unexpected=未知目录")

        service.claim_subscription_transfer = reject_claim
        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute(
                """
                update internal_api_imports
                set status = 'running', stage = 'received_unclaimed', result_json = ?, info_hash = ?
                where id = ?
                """,
                (
                    json.dumps({"submit": submit_result, "subscription_follow": {"staging": staging}}),
                    "share:abc",
                    task["id"],
                ),
            )
            conn.commit()
        finally:
            conn.close()

        manager.start()
        try:
            failed = self.wait_final(manager, "owner-a", task["id"])
        finally:
            manager.stop()

        self.assertEqual(failed["status"], "failed")
        self.assertIn("cannot be claimed", failed["error"])
        self.assertEqual(service.submit_uris, [])

    def test_force_duplicate_is_rejected_for_subscription_follow(self):
        service, store, manager, application = self.build_components()
        payload = self.payload(application, service)
        payload["force_duplicate"] = True
        with self.assertRaises(ApiError) as raised:
            manager.create_import("owner-a", "fanren-force", payload)
        self.assertEqual(raised.exception.code, "invalid_subscription_follow")

    def test_different_subscription_works_serialize_shared_receive_root(self):
        service, store, manager, application = self.build_components(workers=2, owner_workers=2)
        service.subscription_entries = [
            {"fid": "video-115", "fn": "凡人修仙传.S01E115.mkv", "kind": "video", "episode": 115}
        ]
        first_payload = self.payload(application, service)
        session_id, candidate_id, _ = self.search_candidate(
            application,
            owner_id="owner-b",
            query="另一部动画 更新至120集",
            category="anime",
        )
        second_payload = self.import_payload(
            session_id,
            candidate_id,
            category="anime",
            target=target_for("anime"),
        )
        second_payload.update(first_payload)
        second_payload["search_session_id"] = session_id
        second_payload["candidate_id"] = candidate_id
        second_payload["work_key"] = "另一部动画"
        second_payload["subscription_id"] = "subscription-other"
        first, _ = manager.create_import("owner-a", "fanren-shared-root-1", first_payload)
        second, _ = manager.create_import("owner-b", "fanren-shared-root-2", second_payload)

        manager.start()
        try:
            self.wait_final(manager, "owner-a", first["id"], timeout=5)
            self.wait_final(manager, "owner-b", second["id"], timeout=5)
        finally:
            manager.stop()

        self.assertEqual(service.subscription_receive_max_active, 1)

    def test_different_direct_subscription_works_do_not_serialize_receive(self):
        service, _store, manager, application = self.build_components(workers=2, owner_workers=2)
        service.subscription_entries = [
            {"fid": "video-115", "fn": "凡人修仙传.S01E115.mkv", "kind": "video", "episode": 115}
        ]
        original_prepare = service.prepare_subscription_staging

        def prepare_direct(category, import_id, work_key):
            staging = original_prepare(category, import_id, work_key)
            staging.update({"receive_mode": "direct_task_directory", "receive_folder_id": "task-" + import_id})
            return staging

        service.prepare_subscription_staging = prepare_direct
        first_payload = self.payload(application, service)
        session_id, candidate_id, _ = self.search_candidate(
            application,
            owner_id="owner-b",
            query="另一部动画 更新至120集",
            category="anime",
        )
        second_payload = self.import_payload(
            session_id,
            candidate_id,
            category="anime",
            target=target_for("anime"),
        )
        second_payload.update(first_payload)
        second_payload["search_session_id"] = session_id
        second_payload["candidate_id"] = candidate_id
        second_payload["work_key"] = "另一部动画"
        second_payload["subscription_id"] = "subscription-other"
        first, _ = manager.create_import("owner-a", "fanren-direct-1", first_payload)
        second, _ = manager.create_import("owner-b", "fanren-direct-2", second_payload)

        manager.start()
        try:
            self.wait_final(manager, "owner-a", first["id"], timeout=5)
            self.wait_final(manager, "owner-b", second["id"], timeout=5)
        finally:
            manager.stop()

        self.assertGreaterEqual(service.subscription_receive_max_active, 2)


class SubscriptionPromotionTest(unittest.TestCase):
    def test_partial_openlist_move_is_detected_by_target_verification(self):
        service = PipelineBotService(
            BotConfig(token="token", allowed_user_ids={1}, subscription_move_timeout_seconds=0)
        )
        target_path = category_to_openlist_path("anime").rstrip("/") + "/凡人修仙传 (2020)/Season 1"
        staging_path = category_to_openlist_path("anime").rstrip("/") + "/temp/凡人修仙传/task-1"

        class PartialMoveClient:
            def __init__(self):
                self.moved = []
                self.target_items = []

            def get_path(self, path):
                return {"code": 200, "data": {"name": "Season 1", "is_dir": True}}

            def list_all(self, path, refresh=False):
                if path == target_path:
                    return list(self.target_items)
                return []

            def move_names(self, src_dir, dst_dir, names):
                self.moved.append((src_dir, dst_dir, list(names)))
                self.target_items.append({"name": "凡人修仙传.S01E115.mkv", "is_dir": False})
                return {"code": 200, "data": {"message": "completed"}}

        client = PartialMoveClient()
        service._build_openlist_client = lambda: client
        service._build_115_client = lambda _category: (_ for _ in ()).throw(AssertionError("115 Open API must not be used"))
        staging = {
            "openlist_path": staging_path,
            "entries": [
                {"path": staging_path + "/凡人修仙传.S01E115.mkv", "name": "凡人修仙传.S01E115.mkv", "is_dir": False},
                {"path": staging_path + "/凡人修仙传.S01E115.zh-CN.srt", "name": "凡人修仙传.S01E115.zh-CN.srt", "is_dir": False},
            ]
        }
        plan = {
            "files": [
                {"path": staging_path + "/凡人修仙传.S01E115.mkv", "name": "凡人修仙传.S01E115.mkv", "episode": 115, "kind": "video"},
                {"path": staging_path + "/凡人修仙传.S01E115.zh-CN.srt", "name": "凡人修仙传.S01E115.zh-CN.srt", "kind": "sidecar"},
            ],
            "episodes": [115],
        }

        with self.assertRaisesRegex(RuntimeError, "OpenList move verification failed.*zh-(?:CN|cn)\\.srt"):
            service.promote_subscription_episodes("anime", staging, target_path, [115], 1, plan=plan)
        self.assertEqual(
            client.moved,
            [(staging_path, target_path, ["凡人修仙传.S01E115.mkv", "凡人修仙传.S01E115.zh-CN.srt"])],
        )

    def test_prepare_subscription_staging_uses_a_dedicated_115_receive_directory(self):
        service = PipelineBotService(
            BotConfig(token="token", allowed_user_ids={1}, subscription_staging_folder_id="temporary-root-cid")
        )

        class FakeClient115:
            def __init__(self):
                self.created = []
                self.names = {}

            def create_folder(self, name, parent_id):
                folder_id = "cid-%d" % (len(self.created) + 1)
                self.created.append((name, parent_id, folder_id))
                self.names[folder_id] = name
                return {"state": True, "data": {"file_id": folder_id}}

            def get_folder_info(self, folder_id):
                return {"state": True, "data": {"file_id": folder_id, "file_name": self.names[folder_id]}}

        client = FakeClient115()
        service._build_115_client = lambda _category: client
        service._build_openlist_client = lambda: (_ for _ in ()).throw(AssertionError("OpenList mkdir must not be used"))

        staging = service.prepare_subscription_staging("anime", "import-1", "凡人修仙传")

        self.assertEqual(staging["receive_mode"], "direct_task_directory")
        self.assertEqual(staging["receive_root_folder_id"], "temporary-root-cid")
        self.assertEqual(staging["receive_folder_id"], "cid-3")
        self.assertEqual(staging["receive_root_path"], "/115/临时")
        self.assertTrue(staging["openlist_path"].startswith("/115/临时/追更任务/凡人修仙传/"))
        self.assertEqual(
            client.created,
            [
                ("追更任务", "temporary-root-cid", "cid-1"),
                ("凡人修仙传", "cid-1", "cid-2"),
                ("import-1", "cid-2", "cid-3"),
            ],
        )

    def test_direct_subscription_receive_waits_for_count_without_moving_top_level_names(self):
        service = PipelineBotService(
            BotConfig(
                token="token",
                allowed_user_ids={1},
                subscription_move_timeout_seconds=1,
                subscription_move_poll_seconds=0,
            )
        )
        staging = {
            "receive_mode": "direct_task_directory",
            "receive_root_path": "/115/临时",
            "receive_folder_id": "task-cid",
            "openlist_path": "/115/临时/追更任务/show/import-1",
        }

        class FakeOpenList:
            def __init__(self):
                self.list_calls = 0
                self.moves = []

            def list_all(self, path, refresh=False):
                self.list_calls += 1
                self.last_list = (path, refresh)
                if self.list_calls == 1:
                    return [{"name": "01.mkv"}]
                return [{"name": "01.mkv"}, {"name": "02.mkv"}]

            def move_names(self, src_dir, dst_dir, names):
                self.moves.append((src_dir, dst_dir, names))

        client = FakeOpenList()
        service._build_openlist_client = lambda: client
        claimed = service.claim_subscription_transfer(
            staging,
            {
                "submit_kind": "115_share_receive",
                "raw": {"data": {"items": [{"name": "original-a"}, {"name": "original-b"}]}},
            },
        )

        self.assertEqual(claimed["received_item_count"], 2)
        self.assertIsInstance(claimed["claimed_at"], int)
        self.assertEqual(client.last_list, (staging["openlist_path"], True))
        self.assertEqual(client.moves, [])

    def test_prepare_subscription_staging_reuses_verified_existing_115_ancestors(self):
        root = "/115/temp"
        service = PipelineBotService(
            BotConfig(token="token", allowed_user_ids={1}, subscription_staging_root=root, subscription_staging_folder_id="root-cid")
        )

        class FakeClient115:
            def __init__(self):
                self.created = []
                self.listed = []

            def create_folder(self, name, parent_id):
                self.created.append((name, parent_id))
                raise RuntimeError("folder already exists")

            def list_all_files(self, folder_id):
                self.listed.append(folder_id)
                entries = {
                    "root-cid": [{"cid": "reserved-cid", "fn": "追更任务", "fc": "0"}],
                    "reserved-cid": [{"cid": "show-cid", "fn": "show", "fc": "0"}],
                    "show-cid": [{"cid": "task-cid", "fn": "import-1", "fc": "0"}],
                }
                return entries[folder_id]

            def get_folder_info(self, folder_id):
                names = {"reserved-cid": "追更任务", "show-cid": "show", "task-cid": "import-1"}
                return {"state": True, "data": {"file_id": folder_id, "file_name": names[folder_id]}}

        client = FakeClient115()
        service._build_115_client = lambda _category: client
        staging = service.prepare_subscription_staging("anime", "import-1", "show")

        self.assertEqual(staging["receive_folder_id"], "task-cid")
        self.assertEqual(client.created, [("追更任务", "root-cid"), ("show", "reserved-cid"), ("import-1", "show-cid")])
        self.assertEqual(client.listed, ["root-cid", "reserved-cid", "show-cid"])
        self.assertEqual(staging["openlist_path"], root + "/追更任务/show/import-1")

    def test_staging_inspection_and_cleanup_use_only_openlist_paths(self):
        service = PipelineBotService(BotConfig(token="token", allowed_user_ids={1}))
        staging = {
            "receive_root_folder_id": "temporary-root-cid",
            "receive_root_path": "/115/临时",
            "openlist_path": "/115/临时/追更任务/凡人修仙传/import-1",
        }

        class FakeOpenList:
            def __init__(self):
                self.removed = []
                self.listed = []

            def list_all(self, path, refresh=False):
                self.listed.append((path, refresh))
                self.last_list = (path, refresh)
                return [{"name": "凡人修仙传.S01E115.mkv", "is_dir": False}]

            def list_path(self, path, refresh=False, page=1, per_page=1):
                self.listed.append((path, refresh))
                return {
                    "code": 200,
                    "data": {
                        "content": [{"name": "existing-task", "is_dir": True}],
                        "total": 1,
                    },
                }

            def remove_names(self, parent, names):
                self.removed.append((parent, list(names)))

        client = FakeOpenList()
        service._build_openlist_client = lambda: client
        service._build_115_client = lambda _category: (_ for _ in ()).throw(AssertionError("115 Open API must not be used"))

        inspected = service.inspect_subscription_staging("anime", staging, 1)
        cleanup = service.cleanup_subscription_staging("anime", staging)

        self.assertEqual(inspected["verified_episodes"], [115])
        self.assertEqual(
            client.listed,
            [
                (staging["openlist_path"], True),
                ("/115/临时/追更任务/凡人修仙传", True),
            ],
        )
        self.assertEqual(client.removed, [("/115/临时/追更任务/凡人修仙传", ["import-1"])])
        self.assertFalse(cleanup["parent_pruned"])

    def test_staging_cleanup_prunes_empty_work_parent(self):
        service = PipelineBotService(BotConfig(token="token", allowed_user_ids={1}))
        staging = {
            "openlist_path": "/115/临时/追更任务/series-4790edb7/import-1",
        }

        class FakeOpenList:
            def __init__(self):
                self.removed = []
                self.listed = []

            def remove_names(self, parent, names):
                self.removed.append((parent, list(names)))

            def list_path(self, path, refresh=False, page=1, per_page=1):
                self.listed.append((path, refresh, page, per_page))
                return {"code": 200, "data": {"content": [], "total": 0}}

        client = FakeOpenList()
        service._build_openlist_client = lambda: client

        cleanup = service.cleanup_subscription_staging("anime", staging)

        self.assertEqual(
            client.removed,
            [
                ("/115/临时/追更任务/series-4790edb7", ["import-1"]),
                ("/115/临时/追更任务", ["series-4790edb7"]),
            ],
        )
        self.assertEqual(
            client.listed,
            [("/115/临时/追更任务/series-4790edb7", True, 1, 1)],
        )
        self.assertTrue(cleanup["parent_pruned"])

    def test_staging_cleanup_is_idempotent_when_task_and_parent_are_absent(self):
        service = PipelineBotService(BotConfig(token="token", allowed_user_ids={1}))
        staging = {
            "openlist_path": "/115/临时/追更任务/series-4790edb7/import-1",
        }

        class MissingOpenList:
            def remove_names(self, _parent, _names):
                raise RuntimeError("OpenList remove failed: object not found")

            def list_path(self, _path, refresh=False, page=1, per_page=1):
                raise RuntimeError("OpenList list failed: object not found")

        service._build_openlist_client = lambda: MissingOpenList()

        cleanup = service.cleanup_subscription_staging("anime", staging)

        self.assertTrue(cleanup["task_already_absent"])
        self.assertTrue(cleanup["parent_already_absent"])
        self.assertFalse(cleanup["parent_pruned"])

    def test_parent_prune_failure_does_not_undo_task_cleanup(self):
        service = PipelineBotService(BotConfig(token="token", allowed_user_ids={1}))
        staging = {
            "openlist_path": "/115/临时/追更任务/series-4790edb7/import-1",
        }

        class ParentListFailureOpenList:
            def __init__(self):
                self.removed = []

            def remove_names(self, parent, names):
                self.removed.append((parent, list(names)))

            def list_path(self, _path, refresh=False, page=1, per_page=1):
                raise RuntimeError("OpenList list failed: connection timed out")

        client = ParentListFailureOpenList()
        service._build_openlist_client = lambda: client

        cleanup = service.cleanup_subscription_staging("anime", staging)

        self.assertEqual(
            client.removed,
            [("/115/临时/追更任务/series-4790edb7", ["import-1"])],
        )
        self.assertEqual(
            cleanup["parent_prune_error"],
            "OpenList list failed: connection timed out",
        )
        self.assertFalse(cleanup["parent_pruned"])

    def test_failed_staging_with_content_is_retained_before_promotion(self):
        service = PipelineBotService(BotConfig(token="token", allowed_user_ids={1}))
        staging = {
            "openlist_path": "/115/临时/追更任务/series-4790edb7/import-1",
        }

        class NonEmptyOpenList:
            def __init__(self):
                self.removed = []

            def list_path(self, path, refresh=False, page=1, per_page=1):
                self.last_list = (path, refresh)
                return {
                    "code": 200,
                    "data": {
                        "content": [{"name": "吞噬星空.S05E139.mkv", "is_dir": False}],
                        "total": 1,
                    },
                }

            def remove_names(self, parent, names):
                self.removed.append((parent, list(names)))

        client = NonEmptyOpenList()
        service._build_openlist_client = lambda: client

        settlement = service.settle_subscription_staging(
            "anime",
            staging,
            "failed",
            promoted=False,
        )

        self.assertEqual(settlement["status"], "retained")
        self.assertEqual(settlement["reason"], "retryable_content_before_promotion")
        self.assertEqual(settlement["entry_count"], 1)
        self.assertEqual(client.removed, [])

    def test_staging_cleanup_rejects_paths_outside_task_root(self):
        service = PipelineBotService(BotConfig(token="token", allowed_user_ids={1}))
        service._build_openlist_client = lambda: object()

        with self.assertRaisesRegex(RuntimeError, "outside the task root"):
            service.cleanup_subscription_staging(
                "anime",
                {"openlist_path": "/115/临时/other/import-1"},
            )

    def test_subscription_staging_does_not_create_directories_through_openlist(self):
        service = PipelineBotService(
            BotConfig(token="token", allowed_user_ids={1}, subscription_staging_folder_id="temporary-root-cid")
        )

        class FakeClient115:
            def __init__(self):
                self.created = []

            def create_folder(self, name, parent_id):
                folder_id = "cid-%d" % (len(self.created) + 1)
                self.created.append((name, parent_id, folder_id))
                return {"state": True, "data": {"file_id": folder_id}}

            def get_folder_info(self, folder_id):
                return {"state": True, "data": {"file_id": folder_id, "file_name": self.created[int(folder_id.split("-")[-1]) - 1][0]}}

        service._build_115_client = lambda _category: FakeClient115()
        service._build_openlist_client = lambda: (_ for _ in ()).throw(AssertionError("OpenList mkdir must not be used"))

        staging = service.prepare_subscription_staging("anime", "import-1", "凡人修仙传")

        self.assertEqual(staging["receive_mode"], "direct_task_directory")


class ImportWorkerRecoveryTest(InternalApiTestCase):
    def test_worker_retries_after_transient_sqlite_claim_error(self):
        service, store, manager, application = self.build_components(workers=1, owner_workers=1)
        session_id, candidate_id, _ = self.search_candidate(application)
        task, _ = manager.create_import(
            "owner-a", "sqlite-retry-key", self.import_payload(session_id, candidate_id)
        )
        original_claim = store.claim_next_import
        claim_calls = []

        def flaky_claim(blocked_owners):
            claim_calls.append(set(blocked_owners))
            if len(claim_calls) == 1:
                raise sqlite3.OperationalError("disk I/O error")
            return original_claim(blocked_owners)

        store.claim_next_import = flaky_claim
        manager.start()
        try:
            completed = self.wait_final(manager, "owner-a", task["id"])
        finally:
            manager.stop()

        self.assertEqual(completed["status"], "completed")
        self.assertGreaterEqual(len(claim_calls), 2)


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

    def test_deferred_offline_wait_is_claimed_after_newer_normal_task(self):
        service, store, manager, application = self.build_components(workers=1, owner_workers=1)
        first = self.create_task(manager, application, "owner-a", "first", "first", {**TARGET, "root_id": "r1"})
        second = self.create_task(manager, application, "owner-a", "second", "second", {**TARGET, "root_id": "r2"})

        claimed = store.claim_next_import(set())
        other_id = second["id"] if claimed["id"] == first["id"] else first["id"]
        store.save_running(claimed["id"], "waiting_download")
        store.defer_waiting_import(claimed["id"])

        next_task = store.claim_next_import(set())
        self.assertEqual(next_task["id"], other_id)
        deferred = store.get_import("owner-a", claimed["id"])
        self.assertEqual((deferred["status"], deferred["stage"]), ("queued", "waiting_download"))

    def test_long_offline_wait_yields_worker_to_later_task(self):
        class WaitingFirstService(FakePipelineService):
            def task_status(self, category, info_hash):
                self.task_status_calls.append((category, info_hash))
                if info_hash == "HASH001":
                    return {
                        "info_hash": info_hash,
                        "name": "slow",
                        "status_name": "downloading",
                        "percent_done": 25,
                    }
                return {
                    "info_hash": info_hash,
                    "name": "ready",
                    "status_name": "success",
                    "percent_done": 100,
                }

        service = WaitingFirstService(download_delay=0.001)
        service, store, manager, application = self.build_components(
            service,
            workers=1,
            owner_workers=1,
            poll_seconds=0.005,
            offline_wait_slice_seconds=0.02,
        )
        slow = self.create_task(manager, application, "owner-a", "slow", "slow", {**TARGET, "root_id": "r1"})
        ready = self.create_task(manager, application, "owner-a", "ready", "ready", {**TARGET, "root_id": "r2"})
        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute("update internal_api_imports set created_at = 1 where id = ?", (slow["id"],))
            conn.execute("update internal_api_imports set created_at = 2 where id = ?", (ready["id"],))
            conn.commit()
        finally:
            conn.close()

        manager.start()
        try:
            deadline = time.time() + 2
            while len(service.submit_uris) < 2 and time.time() < deadline:
                time.sleep(0.01)
            self.assertEqual(len(service.submit_uris), 2)
            self.assertTrue(service.submit_uris[0].endswith("SLOW"))
            self.assertTrue(service.submit_uris[1].endswith("READY"))
        finally:
            manager.stop()

    def test_global_and_owner_worker_limits(self):
        service = FakePipelineService(sync_delay=0.2)
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

    def test_http_manual_candidate_endpoint_uses_bearer_and_returns_session(self):
        service = FakePipelineService()
        port = free_tcp_port()
        server = InternalApiServer(service, self.db_path, token="secret", port=port, workers=1, owner_workers=1)
        server.start()
        try:
            info_hash = "0123456789abcdef0123456789abcdef01234567"
            result = http_json(
                "http://127.0.0.1:%d/v1/manual-candidates" % port,
                {
                    "owner_id": "owner-a",
                    "input": "magnet:?xt=urn:btih:%s&dn=Sintel" % info_hash,
                    "title": "Sintel",
                    "category": "movie",
                },
                token="secret",
            )
            self.assertTrue(result["session_id"])
            self.assertEqual(result["items"][0]["resource_type"], "magnet")
            self.assertEqual(result["metadata"]["manual_kind"], "magnet")
        finally:
            server.stop()

    def test_http_subtitle_search_uses_bearer_and_hides_raw_candidate(self):
        service = FakePipelineService()
        port = free_tcp_port()
        server = InternalApiServer(service, self.db_path, token="secret", port=port, workers=1, owner_workers=1)
        server.start()
        try:
            result = http_json(
                "http://127.0.0.1:%d/v1/subtitles/search" % port,
                {"owner_id": "admin", "media_id": "media-1"},
                token="secret",
            )
            self.assertEqual(result["items"][0]["provider"], "subtitlecat")
            self.assertNotIn("candidate", result["items"][0])
            self.assertNotIn("provider_id", result["items"][0])
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
