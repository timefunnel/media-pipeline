import json
import hashlib
import io
import sqlite3
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "app"))

from pipeline.client115 import Client115
from pipeline.cli import public_resource_summary
from pipeline.cli import main as cli_main
from pipeline.cli import run as cli_run
from pipeline.cli import summarize_offline_submit
from pipeline.config import category_to_folder_id, category_to_msg_library_root, category_to_openlist_path
from pipeline.mediastation import (
    MediaStationClient,
    adult_artwork_repair_patch,
    extract_codes,
    extract_library_items,
    extract_media_id,
    extract_media_items,
    find_matching_media,
    is_bad_adult_artwork_url,
    iter_dmm_cids,
    iter_mgstage_poster_candidates,
    reachable_image_url,
)
from pipeline.offline_tasks import cancel_task_if_active, find_task_by_info_hash, find_tasks_by_info_hashes, normalize_task, task_can_cancel, wait_for_task
from pipeline.openlist import OpenListClient, OpenListPasswordTokenProvider, OpenListTokenProvider, extract_openlist_login_token
from pipeline.openlist_tokens import OpenListTokenStore
from pipeline.prowlarr import ProwlarrClient, ProwlarrConfig, torrent_bytes_to_magnet
from pipeline.resource_selector import ResourceSelector
from pipeline.subtitle_proxy import (
    MsgApiAuthenticator,
    inject_emby_subtitle_streams,
    inject_subtitle_track_bootstrap,
    normalize_webvtt_timestamps,
    patch_emby_playback_info_runtime,
    patch_emby_resume_runtime_fields,
    parse_emby_subtitle_stream_path,
    parse_emby_item_media_id,
    redact_sensitive_query_values,
    should_normalize_subtitle,
    subtitle_body_to_vtt,
)


class FakeTransport:
    def __init__(self, payload):
        self.payload = payload
        self.calls = []

    def request(self, method, url, headers=None, data=None, timeout=None):
        self.calls.append(
            {
                "method": method,
                "url": url,
                "headers": headers or {},
                "data": data,
                "timeout": timeout,
            }
        )
        return self.payload


class DownloadRedirectTransport(FakeTransport):
    def __init__(self, magnet_uri):
        super().__init__([])
        self.magnet_uri = magnet_uri
        self.resolve_calls = []

    def resolve_magnet_redirect(self, url, timeout=None):
        self.resolve_calls.append({"url": url, "timeout": timeout})
        return self.magnet_uri


class SequenceTransport:
    def __init__(self, payloads):
        self.payloads = list(payloads)
        self.calls = []

    def request(self, method, url, headers=None, data=None, timeout=None):
        self.calls.append(
            {
                "method": method,
                "url": url,
                "headers": headers or {},
                "data": data,
                "timeout": timeout,
            }
        )
        if not self.payloads:
            raise RuntimeError("unexpected request: %s %s" % (method, url))
        return self.payloads.pop(0)


class Fake115TaskClient:
    def __init__(self, pages):
        self.pages = list(pages)
        self.calls = []

    def get_offline_tasks(self, page=1):
        self.calls.append(page)
        if len(self.pages) == 1:
            return self.pages[0]
        return self.pages.pop(0)


class Fake115CancelClient(Fake115TaskClient):
    def __init__(self, task_page, delete_response):
        super().__init__([task_page])
        self.delete_response = delete_response
        self.deleted = []

    def delete_offline_task(self, info_hash, delete_files=False):
        self.deleted.append((info_hash, delete_files))
        return self.delete_response


class StepClock:
    def __init__(self, step=1):
        self.value = 0
        self.step = step

    def __call__(self):
        current = self.value
        self.value += self.step
        return current


class FakeProwlarr:
    def __init__(self, results, indexers=None, indexer_results=None, indexer_errors=None):
        self.results = results
        self.indexer_rows = indexers or []
        self.indexer_results = indexer_results or {}
        self.indexer_errors = indexer_errors or {}
        self.search_calls = []

    def search(self, query, limit=20, indexer_ids=None, categories=None):
        key = tuple(indexer_ids or [])
        if categories is None:
            self.search_calls.append((query, limit, key))
        else:
            self.search_calls.append((query, limit, key, tuple(categories or [])))
        if key in self.indexer_errors:
            raise self.indexer_errors[key]
        if indexer_ids:
            return self.indexer_results.get(key, [])
        return self.results

    def indexers(self):
        return self.indexer_rows

    def tags(self):
        return []


class FakeToken:
    access_token = "access-token-value"
    storage_id = 1
    mount_path = "/115"


class FakeTokenStore:
    def __init__(self, db_path):
        self.db_path = db_path
        self.load_count = 0

    def load_access_token(self):
        self.load_count += 1
        return FakeToken()


class Fake115SubmitClient:
    def __init__(self, response):
        self.response = response
        self.urls = None
        self.folder_id = None

    def add_offline_urls(self, urls, folder_id):
        self.urls = urls
        self.folder_id = folder_id
        return self.response


class FakeOpenList:
    def __init__(self):
        self.paths = []

    def list_path(self, path):
        self.paths.append(path)
        return {"code": 200, "message": "success", "data": {"content": []}}


class FakeOpenListTokenProvider:
    def load_token(self):
        return "openlist-token-value"


class FakeOpenListPasswordTokenProvider:
    def __init__(self, base_url, username, password):
        self.base_url = base_url
        self.username = username
        self.password = password

    def load_token(self):
        if not self.username or not self.password:
            raise RuntimeError("OpenList media scan credentials missing")
        return "media-scan-token-value"


class RetryOpenList:
    def __init__(self, events):
        self.events = events

    def list_path(self, path, refresh=False):
        self.events.append(("openlist", path, refresh))
        return {"code": 200, "message": "success", "data": {"content": []}}


class CleaningOpenList:
    def __init__(self, tree, events=None):
        self.tree = tree
        self.events = events if events is not None else []
        self.rename_calls = []
        self.move_calls = []
        self.meta_hide_calls = []

    def list_path(self, path, refresh=False):
        self.events.append(("openlist", path, refresh))
        return {"code": 200, "message": "success", "data": {"content": self.tree.get(path, []), "total": len(self.tree.get(path, []))}}

    def list_all(self, path, refresh=False):
        self.events.append(("list_all", path, refresh))
        return list(self.tree.get(path, []))

    def rename_path(self, path, name):
        self.rename_calls.append((path, name))
        self.events.append(("rename", path, name))
        parent = str(path).rstrip("/").rsplit("/", 1)[0] or "/"
        new_path = parent.rstrip("/") + "/" + str(name)
        if parent in self.tree:
            for item in self.tree[parent]:
                if str(item.get("name") or "") == str(path).rstrip("/").rsplit("/", 1)[-1]:
                    item["name"] = str(name)
        if path in self.tree:
            moves = []
            for key, value in list(self.tree.items()):
                if key == path or key.startswith(path.rstrip("/") + "/"):
                    moves.append((key, new_path + key[len(path) :], value))
            for old_key, new_key, value in moves:
                self.tree[new_key] = value
                if old_key != new_key:
                    self.tree.pop(old_key, None)
        return {"code": 200, "message": "success"}

    def move_names(self, src_dir, dst_dir, names):
        self.move_calls.append((src_dir, dst_dir, list(names)))
        self.events.append(("move", src_dir, dst_dir, tuple(names)))
        return {"code": 200, "message": "success"}

    def upsert_meta_hide(self, path, hide_patterns, h_sub=True):
        self.meta_hide_calls.append((path, list(hide_patterns), h_sub))
        self.events.append(("meta_hide", path, tuple(hide_patterns), h_sub))
        return {"code": 200, "message": "success"}

    def get_path(self, path):
        self.events.append(("get_path", path))
        return {"code": 200, "message": "success", "data": {"sign": "sign-%s" % str(path).rsplit("/", 1)[-1]}}


class RetryTokenStore:
    def __init__(self):
        self.tokens = []

    def load_access_token(self):
        token = "expired-token" if not self.tokens else "fresh-token"
        self.tokens.append(token)
        return type("Token", (), {"access_token": token})()


class Retry115Client:
    def __init__(self, token, events):
        self.token = token
        self.events = events

    def get_offline_tasks(self, page=1):
        self.events.append(("115_tasks", self.token, page))
        if self.token == "expired-token":
            return {"state": False, "code": 40140125, "message": "access_token 无效"}
        return {"state": True, "data": {"page_count": 1, "tasks": [{"info_hash": "ABC", "status": 2, "percentDone": 100}]}}


class FakeMediaStationClient:
    def __init__(self, search_response=None, list_response=None, events=None, artwork_repair_response=None, scrape_search_responses=None):
        self.search_response = search_response or {"data": {"items": []}}
        self.list_response = list_response or {"data": {"items": []}}
        self.events = events
        self.artwork_repair_response = artwork_repair_response or {"status": "skipped", "updated": 0, "reason": "not_needed"}
        self.scrape_search_responses = scrape_search_responses or {}
        self.scan_calls = []
        self.search_calls = []
        self.list_calls = []
        self.scrape_calls = []
        self.scrape_search_calls = []
        self.scrape_apply_calls = []
        self.artwork_repair_calls = []

    def scan_root(self, library_id, root_id):
        self.scan_calls.append((library_id, root_id))
        if self.events is not None:
            self.events.append(("scan",))
        return {"ok": True}

    def search_media(self, query, limit=20):
        self.search_calls.append((query, limit))
        return self.search_response

    def list_library_media(self, library_id, page=1, page_size=200, group_versions=0):
        self.list_calls.append((library_id, page, page_size, group_versions))
        return self.list_response

    def scrape_media(self, media_id):
        self.scrape_calls.append(media_id)
        return {"ok": True}

    def get_media(self, media_id):
        return {}

    def search_scrape_matches(self, media_id, query, provider, media_type):
        self.scrape_search_calls.append((media_id, query, provider, media_type))
        return self.scrape_search_responses.get(query, {"items": []})

    def apply_scrape_match(self, media_id, match):
        self.scrape_apply_calls.append((media_id, match))
        return {"ok": True}

    def repair_adult_artwork(self, media_id):
        self.artwork_repair_calls.append(media_id)
        if self.events is not None:
            self.events.append(("artwork_repair",))
        return self.artwork_repair_response


class EventOpenList:
    def __init__(self, events):
        self.events = events

    def list_path(self, path):
        self.events.append("warm:%s" % path)
        return {"code": 200, "message": "success", "data": {"content": []}}


class EventTokenStore:
    def __init__(self, db_path, events):
        self.db_path = db_path
        self.events = events

    def load_access_token(self):
        self.events.append("load_token")
        return FakeToken()


class Fake115StatusClient:
    def get_offline_tasks(self, page=1):
        return {"state": True, "data": {"page_count": 1, "tasks": [{"info_hash": "ABC", "status": 2}]}}


class FakeTelegram:
    def __init__(self, chat_action_error=None):
        self.messages = []
        self.answers = []
        self.edits = []
        self.deletes = []
        self.chat_actions = []
        self.commands = []
        self.chat_action_error = chat_action_error

    def send_message(self, chat_id, text, reply_markup=None):
        self.messages.append({"chat_id": chat_id, "text": text, "reply_markup": reply_markup})
        return {"ok": True, "result": {"message_id": 1000 + len(self.messages)}}

    def send_chat_action(self, chat_id, action="typing"):
        if self.chat_action_error:
            raise self.chat_action_error
        self.chat_actions.append({"chat_id": chat_id, "action": action})

    def set_my_commands(self, commands):
        self.commands.append(list(commands or []))
        return {"ok": True}

    def edit_message_text(self, chat_id, message_id, text, reply_markup=None):
        self.edits.append({"chat_id": chat_id, "message_id": message_id, "text": text, "reply_markup": reply_markup})

    def delete_message(self, chat_id, message_id):
        self.deletes.append({"chat_id": chat_id, "message_id": message_id})

    def answer_callback_query(self, callback_query_id, text=None):
        self.answers.append({"callback_query_id": callback_query_id, "text": text})


class FakeBotService:
    def __init__(
        self,
        search_results=None,
        adult_search_results=None,
        anime_search_results=None,
        submit_response=None,
        search_error=None,
        status_response=None,
        cancel_response=None,
        sync_response=None,
        sync_progress=None,
        duplicate_response=None,
        dedupe_entries=None,
        statuses_response=None,
        migration_candidates=None,
        migration_response=None,
        msg_diag_response=None,
    ):
        self.search_results = search_results or []
        self.adult_search_results = adult_search_results or []
        self.anime_search_results = anime_search_results or []
        self.submit_response = submit_response or {"state": True, "tasks": []}
        self.search_error = search_error
        self.status_response = status_response or {"info_hash": "ABC", "status_name": "success", "percent_done": 100}
        self.statuses_response = statuses_response or {}
        self.cancel_response = cancel_response or {
            "cancelled": False,
            "task": {"info_hash": "ABC", "status_name": "success", "percent_done": 100},
            "response": None,
            "reason": "task is not cancellable: success",
        }
        self.search_calls = []
        self.adult_search_calls = []
        self.anime_search_calls = []
        self.submit_calls = []
        self.status_calls = []
        self.statuses_calls = []
        self.cancel_calls = []
        self.sync_response = sync_response or {}
        self.sync_progress = sync_progress or []
        self.sync_calls = []
        self.duplicate_response = duplicate_response
        self.duplicate_calls = []
        self.dedupe_entries = dedupe_entries or []
        self.dedupe_refresh_calls = []
        self.migration_candidates = migration_candidates or []
        self.migration_response = migration_response or {
            "source_openlist_path": "/115/剧集/成龙历险记",
            "target_openlist_path": "/115/动漫/成龙历险记",
            "target_category": "anime",
            "media_count": 95,
            "series_count": 1,
        }
        self.migration_search_calls = []
        self.migration_calls = []
        self.msg_diag_response = msg_diag_response or {}
        self.msg_diag_calls = []

    def search(self, query, category, limit=5):
        self.search_calls.append((query, category, limit))
        if self.search_error:
            raise self.search_error
        return self.search_results

    def search_adult(self, query, limit=5):
        self.adult_search_calls.append((query, limit))
        if self.search_error:
            raise self.search_error
        return self.adult_search_results

    def search_anime(self, query, limit=5):
        self.anime_search_calls.append((query, limit))
        if self.search_error:
            raise self.search_error
        return self.anime_search_results

    def submit(self, category, download_uri):
        self.submit_calls.append((category, download_uri))
        return self.submit_response

    def task_status(self, category, info_hash):
        self.status_calls.append((category, info_hash))
        return self.status_response

    def task_statuses(self, category, info_hashes):
        hashes = tuple(info_hashes)
        self.statuses_calls.append((category, hashes))
        responses = {str(key).lower(): value for key, value in self.statuses_response.items()}
        return {str(info_hash).lower(): responses[str(info_hash).lower()] for info_hash in hashes if str(info_hash).lower() in responses}

    def cancel_task(self, category, info_hash):
        self.cancel_calls.append((category, info_hash))
        return self.cancel_response

    def check_duplicate(self, category, query, candidate):
        self.duplicate_calls.append((category, query, candidate.get("title")))
        return self.duplicate_response

    def collect_openlist_dedupe_entries(self, refresh=True):
        self.dedupe_refresh_calls.append(refresh)
        return list(self.dedupe_entries)

    def search_migration_candidates(self, query, limit=20):
        self.migration_search_calls.append((query, limit))
        if self.search_error:
            raise self.search_error
        return list(self.migration_candidates)

    def migrate_media_candidate(self, candidate, target_category):
        self.migration_calls.append((candidate.get("source_openlist_path"), target_category))
        return dict(self.migration_response)

    def msg_media_diagnostics(self, media_id):
        self.msg_diag_calls.append(media_id)
        if self.search_error:
            raise self.search_error
        return dict(self.msg_diag_response)

    def sync_completed_task(self, category, title, task, progress_callback=None):
        self.sync_calls.append((category, title, (task or {}).get("info_hash")))
        out = dict(task or {})
        for progress in self.sync_progress:
            out.update(progress)
            if progress_callback:
                progress_callback(dict(out))
        out.update(self.sync_response)
        return out


__all__ = [name for name in globals() if not name.startswith("_")]


SPLIT_TEST_MODULES = (
    "tests.pipeline_test_bot",
    "tests.pipeline_test_services",
)


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    for module_name in SPLIT_TEST_MODULES:
        suite.addTests(loader.loadTestsFromName(module_name))
    return suite


if __name__ == "__main__":
    unittest.main()
