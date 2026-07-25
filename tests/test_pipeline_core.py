import json
import hashlib
import io
import os
import sqlite3
import sys
import tempfile
import time
import urllib.parse
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "app"))

for _category, _prefix in {
    "movie": "MEDIA_PIPELINE_MOVIE",
    "tv": "MEDIA_PIPELINE_TV",
    "anime": "MEDIA_PIPELINE_ANIME",
    "adult": "MEDIA_PIPELINE_ADULT",
    "other": "MEDIA_PIPELINE_OTHER",
}.items():
    for _suffix, _value in {
        "MSG_LIBRARY_ID": "test-%s-library" % _category,
        "MSG_ROOT_ID": "test-%s-root" % _category,
    }.items():
        _key = _prefix + "_" + _suffix
        if not os.environ.get(_key):
            os.environ[_key] = _value

from pipeline.client115 import Client115
from pipeline.config import category_to_folder_id, category_to_msg_library_root, category_to_openlist_path
from pipeline.mediastation import (
    MediaStationClient,
    MediaStationApiError,
    extract_codes,
    extract_media_id,
    extract_media_items,
    find_matching_media,
)
from pipeline.offline_tasks import cancel_task_if_active, find_task_by_info_hash, find_tasks_by_info_hashes, normalize_task, task_can_cancel, wait_for_task
from pipeline.openlist import OpenListClient, OpenListPasswordTokenProvider, OpenListTokenProvider, extract_openlist_login_token
from pipeline.openlist_tokens import OpenListTokenStore
from pipeline.prowlarr import ProwlarrClient, ProwlarrConfig, torrent_bytes_to_magnet
from pipeline.resource_selector import ResourceSelector


def find_fake_media_by_openlist_paths(items, openlist_paths, library_id=None):
    targets = [("cloud://openlist" + str(path or "")).rstrip("/") for path in openlist_paths or []]
    best = None
    best_score = 0
    for item in items or []:
        if library_id and str((item or {}).get("library_id") or "") != str(library_id):
            continue
        media_path = urllib.parse.unquote(str((item or {}).get("path") or "")).replace("\\", "/").rstrip("/")
        for target in targets:
            if media_path == target:
                score = 2000
            elif media_path.startswith(target + "/"):
                score = 1000
            else:
                continue
            score += min(int((item or {}).get("size_bytes") or 0) // (100 * 1024 * 1024), 100)
            if score > best_score:
                best = item
                best_score = score
    return best
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
        self.source_delete_calls = []

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

    def delete_path(self, path):
        self.source_delete_calls.append(("delete_path", path))
        raise AssertionError("OpenList source files must be hidden with Meta Hide, not deleted")

    def remove_path(self, path):
        self.source_delete_calls.append(("remove_path", path))
        raise AssertionError("OpenList source files must be hidden with Meta Hide, not deleted")

    def trash_path(self, path):
        self.source_delete_calls.append(("trash_path", path))
        raise AssertionError("OpenList source files must be hidden with Meta Hide, not deleted")

    def delete_names(self, src_dir, names):
        self.source_delete_calls.append(("delete_names", src_dir, list(names)))
        raise AssertionError("OpenList source files must be hidden with Meta Hide, not deleted")

    def remove_names(self, src_dir, names):
        self.source_delete_calls.append(("remove_names", src_dir, list(names)))
        raise AssertionError("OpenList source files must be hidden with Meta Hide, not deleted")

    def trash_names(self, src_dir, names):
        self.source_delete_calls.append(("trash_names", src_dir, list(names)))
        raise AssertionError("OpenList source files must be hidden with Meta Hide, not deleted")

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
    def __init__(
        self,
        search_response=None,
        list_response=None,
        get_response=None,
        events=None,
        scrape_search_responses=None,
        pipeline_scrape_response=None,
        movie_extra_response=None,
        episode_visibility_response=None,
        prune_deleted_response=None,
        deleted_hide_candidates_response=None,
        migration_search_response=None,
        migration_validate_response=None,
        migration_apply_response=None,
        pipeline_ingest_response=None,
        subtitle_status_response=None,
    ):
        self.search_response = search_response or {"data": {"items": []}}
        self.list_response = list_response or {"data": {"items": []}}
        self.get_response = get_response
        self.events = events
        self.scrape_search_responses = scrape_search_responses or {}
        self.pipeline_scrape_response = pipeline_scrape_response
        self.movie_extra_response = movie_extra_response or {"status": "success", "updated": 0, "media_count": 1, "reason": "already_clean"}
        self.episode_visibility_response = episode_visibility_response or {
            "status": "success",
            "updated": 0,
            "media_count": 1,
            "reason": "already_valid",
        }
        self.prune_deleted_response = prune_deleted_response or {
            "status": "skipped",
            "deleted": 0,
            "reason": "no_deleted_media",
            "media_ids": [],
        }
        self.deleted_hide_candidates_response = deleted_hide_candidates_response or {"items": []}
        self.migration_search_response = migration_search_response or {"items": []}
        self.migration_validate_response = migration_validate_response
        self.migration_apply_response = migration_apply_response
        self.pipeline_ingest_response = pipeline_ingest_response
        self.subtitle_status_response = subtitle_status_response or {
            "media_id": "",
            "has_chinese": False,
            "embedded_checked": True,
            "embedded": [],
            "external": [],
            "unknown_embedded": 0,
        }
        self.scan_calls = []
        self.search_calls = []
        self.list_calls = []
        self.get_calls = []
        self.scrape_calls = []
        self.scrape_search_calls = []
        self.scrape_apply_calls = []
        self.pipeline_scrape_calls = []
        self.pipeline_maintenance_calls = []
        self.pipeline_ingest_calls = []
        self.pipeline_ingest_jobs = {}
        self.subtitle_status_calls = []
        self.deleted_version_calls = []

    def search_media(self, query, limit=20):
        self.search_calls.append((query, limit))
        return self.search_response

    def list_library_media(self, library_id, page=1, page_size=200, group_versions=0):
        self.list_calls.append((library_id, page, page_size, group_versions))
        return self.list_response

    def pipeline_scrape_media(self, media_id, category, title, queries, provider, media_type):
        self.pipeline_scrape_calls.append((media_id, category, title, list(queries or []), provider, media_type))
        if self.pipeline_scrape_response is not None:
            return self.pipeline_scrape_response
        for query in queries or []:
            self.scrape_search_calls.append((media_id, query, provider, media_type))
            response = self.scrape_search_responses.get(query, {"items": []})
            matches = response.get("items") or (response.get("data") or {}).get("items") or []
            if len(matches) == 1:
                self.scrape_apply_calls.append((media_id, matches[0]))
                return {"mode": "apply", "query": query, "applied_count": 1}
        self.scrape_calls.append(media_id)
        return {"mode": "smart", "applied_count": 1}

    def pipeline_repair_movie_extras(self, media_id, target):
        self.pipeline_maintenance_calls.append(("repair_movie_extras", media_id, dict(target or {})))
        return self.movie_extra_response

    def pipeline_repair_episode_visibility(self, media_id, target):
        self.pipeline_maintenance_calls.append(("repair_episode_visibility", media_id, dict(target or {})))
        return self.episode_visibility_response

    def pipeline_subtitle_status(self, media_id):
        self.subtitle_status_calls.append(media_id)
        result = dict(self.subtitle_status_response)
        if not result.get("media_id"):
            result["media_id"] = media_id
        return result

    def pipeline_prune_deleted_media(self, target, openlist_paths):
        self.pipeline_maintenance_calls.append(("prune_deleted_media", dict(target or {}), list(openlist_paths or [])))
        return self.prune_deleted_response

    def pipeline_list_deleted_media_hide_candidates(self, limit=100):
        self.pipeline_maintenance_calls.append(("list_deleted_media_hide_candidates", int(limit)))
        return self.deleted_hide_candidates_response

    def pipeline_search_migration_candidates(self, query, limit=20):
        self.pipeline_maintenance_calls.append(("search_migration_candidates", query, int(limit)))
        return self.migration_search_response

    def pipeline_validate_migration(self, source, target):
        self.pipeline_maintenance_calls.append(("validate_migration", dict(source or {}), dict(target or {})))
        if self.events is not None:
            self.events.append(("msg_validate_migration", source.get("source_openlist_path"), target.get("category")))
        if self.migration_validate_response is None:
            raise AssertionError("migration_validate_response missing")
        return self.migration_validate_response

    def pipeline_apply_migration(self, source, target):
        self.pipeline_maintenance_calls.append(("apply_migration", dict(source or {}), dict(target or {})))
        if self.events is not None:
            self.events.append(("msg_apply_migration", source.get("source_openlist_path"), target.get("category")))
        if self.migration_apply_response is None:
            raise AssertionError("migration_apply_response missing")
        return self.migration_apply_response

    def pipeline_start_ingest(self, request):
        request = dict(request or {})
        self.pipeline_ingest_calls.append(request)
        self.scan_calls.append((request.get("library_id"), request.get("root_id")))
        if self.events is not None:
            self.events.append(("scan",))
        if self.pipeline_ingest_response is not None:
            job = dict(self.pipeline_ingest_response)
        else:
            queries = list(request.get("queries") or [])
            target_paths = list(request.get("target_openlist_paths") or [])
            library_id = request.get("library_id")
            media = None
            if target_paths:
                items = extract_media_items(self.list_response) + extract_media_items(self.search_response)
                media = find_fake_media_by_openlist_paths(items, target_paths, library_id=library_id)
            if media is None and (not target_paths or not request.get("require_target_path")):
                for query in queries:
                    items = extract_media_items(self.search_media(query, limit=20))
                    media = find_matching_media(items, queries, library_id=library_id)
                    if media:
                        break
                if media is None:
                    items = extract_media_items(self.list_library_media(library_id, page=1, page_size=200, group_versions=0))
                    media = find_matching_media(items, queries, library_id=library_id)
            job_id = "ingest-%d" % (len(self.pipeline_ingest_jobs) + 1)
            if media is None:
                job = {
                    "id": job_id,
                    "status": "failed",
                    "stage": "failed",
                    "error": "MediaStationGo media not found after root scan",
                    "result": {},
                }
            else:
                prune = self.prune_deleted_response if request.get("prune_deleted_openlist_paths") else None
                job = {
                    "id": job_id,
                    "status": "completed",
                    "stage": "completed",
                    "message": "completed",
                    "result": {
                        "scan": {"library_id": library_id, "visited": 1, "added": 1, "updated": 0, "removed": 0},
                        "media": {
                            "id": media.get("id"),
                            "title": media.get("title"),
                            "path": media.get("path"),
                            "match_mode": "path" if target_paths else "query",
                            "match_path": target_paths[0] if target_paths else "",
                        },
                        "deleted_media_prune": prune,
                    },
                }
        job_id = str(job.get("id") or "ingest-%d" % (len(self.pipeline_ingest_jobs) + 1))
        job["id"] = job_id
        self.pipeline_ingest_jobs[job_id] = job
        return job

    def pipeline_get_ingest(self, job_id):
        return self.pipeline_ingest_jobs[job_id]

    def get_media(self, media_id):
        self.get_calls.append(media_id)
        if isinstance(self.get_response, Exception):
            raise self.get_response
        if self.get_response is None:
            return {"id": media_id}
        return self.get_response

    def soft_delete_media_version(self, anchor_media_id, version_media_id):
        self.deleted_version_calls.append((anchor_media_id, version_media_id))
        return {"deleted_id": version_media_id}















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
        bt4g_search_results=None,
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
        subtitle_response=None,
        subtitle_backfill_response=None,
        subtitle_backfill_one_response=None,
        subtitle_report_response=None,
        subtitle_find_response=None,
        subtitle_rematch_response=None,
        subtitle_apply_response=None,
        rerank_results=None,
        rerank_error=None,
    ):
        self.search_results = search_results or []
        self.adult_search_results = adult_search_results or []
        self.anime_search_results = anime_search_results or []
        self.bt4g_search_results = bt4g_search_results or []
        self.rerank_results = rerank_results
        self.rerank_error = rerank_error
        self.subtitle_rematch_response = subtitle_rematch_response or {
            "media_id": "media-1",
            "title": "SSIS-218",
            "code": "SSIS-218",
            "query": "SSIS-218",
            "media": {"media_id": "media-1", "title": "SSIS-218", "code": "SSIS-218"},
            "candidates": [
                {
                    "provider": "subtitlecat",
                    "query": "SSIS-218",
                    "code": "SSIS-218",
                    "title": "SSIS-218 zh",
                    "filename": "SSIS-218.zh.srt",
                    "source_score": 100,
                    "candidate": {"id": "sub-1"},
                    "rank": 1,
                },
                {
                    "provider": "assrt",
                    "query": "SSIS-218",
                    "code": "SSIS-218",
                    "title": "SSIS-218 chs",
                    "filename": "SSIS-218.chs.ass",
                    "source_score": 80,
                    "candidate": {"id": "sub-2"},
                    "rank": 2,
                },
            ],
        }
        self.subtitle_apply_response = subtitle_apply_response or {
            "media_id": "media-1",
            "title": "SSIS-218",
            "code": "SSIS-218",
            "subtitle_match_status": "success",
            "subtitle_match_source": "assrt",
            "subtitle_match_filename": "assrt-123.srt",
        }
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
        self.bt4g_search_calls = []
        self.rerank_calls = []
        self.submit_calls = []
        self.status_calls = []
        self.statuses_calls = []
        self.cancel_calls = []
        self.sync_response = sync_response or {}
        self.sync_progress = sync_progress or []
        self.subtitle_response = subtitle_response or {"subtitle_match_status": "success", "subtitle_match_count": 1, "subtitle_match_source": "cache"}
        self.subtitle_backfill_response = subtitle_backfill_response or {
            "status": "success",
            "limit": 1,
            "scanned": 1,
            "attempted": 1,
            "with_subtitles": 1,
            "pending": 0,
            "matched": 1,
            "cached": 0,
            "previous": 0,
            "not_found": 0,
            "failed": 0,
            "skipped": 0,
            "recent": [{"media_id": "media-1", "code": "SSIS-218", "status": "success", "source": "assrt", "title": "SSIS-218"}],
            "current": {},
        }
        self.subtitle_backfill_one_response = subtitle_backfill_one_response or dict(self.subtitle_backfill_response)
        self.subtitle_report_response = subtitle_report_response or {
            "total": 2,
            "with_subtitles": 1,
            "pending": 1,
            "untried": 1,
            "not_found": 0,
            "failed": 0,
            "no_code": 0,
            "success_missing_cache": 0,
            "unknown": 0,
            "buckets": {
                "pending": [{"media_id": "media-2", "title": "MIDE-882", "code": "MIDE-882", "status": "untried", "status_label": "未尝试"}],
                "cached": [{"media_id": "media-1", "title": "SSIS-218", "code": "SSIS-218", "status": "cached", "status_label": "已补"}],
                "untried": [{"media_id": "media-2", "title": "MIDE-882", "code": "MIDE-882", "status": "untried", "status_label": "未尝试"}],
                "not_found": [],
                "failed": [],
                "no_code": [],
            },
        }
        self.subtitle_find_response = subtitle_find_response or {
            "query": "SSIS-218",
            "limit": 8,
            "items": [{"media_id": "media-1", "title": "SSIS-218", "code": "SSIS-218", "status": "cached", "status_label": "已补"}],
        }
        self.sync_calls = []
        self.subtitle_calls = []
        self.subtitle_backfill_calls = []
        self.subtitle_backfill_one_calls = []
        self.subtitle_report_calls = 0
        self.subtitle_find_calls = []
        self.subtitle_rematch_calls = []
        self.subtitle_preview_calls = []
        self.subtitle_apply_calls = []
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

    def search_bt4g(self, query, limit=5):
        self.bt4g_search_calls.append((query, limit))
        if self.search_error:
            raise self.search_error
        return self.bt4g_search_results

    def rerank_search_candidates(self, query, category, candidates):
        self.rerank_calls.append((query, category, [item.get("title") for item in candidates]))
        if self.rerank_error:
            raise self.rerank_error
        if self.rerank_results is not None:
            return self.rerank_results
        return list(candidates)

    def rank_subtitle_candidates(self, media, query, candidates):
        self.rerank_calls.append((query, "subtitle", [item.get("title") for item in candidates]))
        if self.rerank_error:
            raise self.rerank_error
        if self.rerank_results is not None:
            return self.rerank_results
        ranked = list(reversed([dict(item) for item in candidates]))
        for index, item in enumerate(ranked, start=1):
            item["rank"] = index
            item["llm_reason"] = "正文更匹配"
        return ranked

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

    def match_task_subtitles(self, category, title, task, force=False):
        self.subtitle_calls.append((category, title, (task or {}).get("info_hash"), force))
        return dict(self.subtitle_response)

    def subtitle_backfill_adult(self, limit=20, progress_callback=None, retry_attempted=False, status_filter=None):
        self.subtitle_backfill_calls.append((limit, retry_attempted, status_filter))
        result = dict(self.subtitle_backfill_response)
        result["limit"] = limit
        result["retry_attempted"] = bool(retry_attempted)
        result["status_filter"] = status_filter
        if progress_callback:
            progress_callback(dict(result), force=True)
        return result

    def subtitle_backfill_one_adult(self, media_id, retry_attempted=False):
        self.subtitle_backfill_one_calls.append((media_id, retry_attempted))
        result = dict(self.subtitle_backfill_one_response)
        result["limit"] = 1
        result["retry_attempted"] = bool(retry_attempted)
        return result

    def subtitle_backfill_report_adult(self):
        self.subtitle_report_calls += 1
        return dict(self.subtitle_report_response)

    def subtitle_find_adult(self, query, limit=8):
        self.subtitle_find_calls.append((query, limit))
        result = dict(self.subtitle_find_response)
        result["query"] = query
        result["limit"] = limit
        return result

    def subtitle_rematch_candidates_adult(self, media_id, limit=10):
        self.subtitle_rematch_calls.append((media_id, limit))
        return dict(self.subtitle_rematch_response)

    def preview_subtitle_candidates(self, candidates, limit=5, max_chars=2000):
        self.subtitle_preview_calls.append((len(candidates or []), limit, max_chars))
        out = []
        for item in candidates or []:
            candidate = dict(item)
            candidate["content_sample"] = "这是中文字幕正文预览"
            candidate["preview_char_count"] = len(candidate["content_sample"])
            candidate["preview_line_count"] = 1
            out.append(candidate)
        return out

    def apply_subtitle_candidate(self, candidate_record):
        self.subtitle_apply_calls.append(dict(candidate_record or {}))
        return dict(self.subtitle_apply_response)


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
