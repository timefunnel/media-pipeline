import os
import sys
import tempfile
import unittest
import urllib.parse
from http.cookiejar import CookieJar
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
        os.environ[_prefix + "_" + _suffix] = _value

from pipeline.bot import (
    BotConfig,
    CandidateStore,
    PipelineBotService,
    TelegramBot,
    build_transfer_manifest,
    inspect_115_offline_result,
    probe_import_transfer_visibility,
    share115_candidate_from_text,
)
from pipeline.client115 import P115QRCodeLoginClient, Share115Client, p115_cookie_is_valid, parse_115_share_url
from pipeline.config import category_to_folder_id


class FakeShareTransport:
    def __init__(self):
        self.calls = []

    def new_cookie_jar(self):
        return CookieJar()

    def request(self, method, url, headers=None, data=None, timeout=None, cookie_jar=None, parse_json=True):
        self.calls.append(
            {
                "method": method,
                "url": url,
                "headers": headers or {},
                "data": data,
                "parse_json": parse_json,
            }
        )
        if parse_json is False:
            return "<html></html>"
        if "share/snap" in url:
            query = urllib.parse.parse_qs(urllib.parse.urlsplit(url).query)
            cid = (query.get("cid") or [""])[0]
            if cid == "folder-1":
                return {
                    "state": True,
                    "data": {
                        "list": [
                            {"fid": "file-2", "cid": "folder-1", "n": "ABF-002.mp4", "s": 456},
                        ]
                    },
                }
            return {
                "state": True,
                "data": {
                    "list": [
                        {"fid": "file-1", "cid": "0", "n": "ABF-001.mp4", "s": 123},
                        {"cid": "folder-1", "n": "ABF-001", "fc": 1},
                    ]
                },
            }
        if "share/receive" in url:
            return {"state": True, "data": {"save_as_top_fids": ["saved-1"]}}
        raise AssertionError("unexpected request: %s" % url)


class FakeQRTransport:
    def __init__(self):
        self.calls = []

    def request(self, method, url, headers=None, data=None, timeout=None, **_kwargs):
        self.calls.append({"method": method, "url": url, "headers": headers or {}, "data": data})
        if "token" in url:
            return {
                "state": 1,
                "data": {
                    "uid": "qr-uid",
                    "sign": "qr-sign",
                    "time": 123,
                    "qrcode": "qr-content",
                },
            }
        if "get/status" in url:
            return {"state": 1, "data": {"status": 2, "msg": "ok"}}
        if "login/qrcode" in url:
            return {
                "state": 1,
                "data": {
                    "cookie": {
                        "UID": "uid-value",
                        "CID": "cid-value",
                        "SEID": "seid-value",
                        "KID": "kid-value",
                    }
                },
            }
        raise AssertionError("unexpected request: %s" % url)


class FakeTelegram:
    def __init__(self):
        self.messages = []
        self.edits = []
        self.answers = []

    def send_message(self, chat_id, text, reply_markup=None):
        self.messages.append({"chat_id": chat_id, "text": text, "reply_markup": reply_markup})
        return {"ok": True, "result": {"message_id": 1000 + len(self.messages)}}

    def edit_message_text(self, chat_id, message_id, text, reply_markup=None):
        self.edits.append({"chat_id": chat_id, "message_id": message_id, "text": text, "reply_markup": reply_markup})

    def answer_callback_query(self, callback_query_id, text=None):
        self.answers.append({"callback_query_id": callback_query_id, "text": text})

    def send_chat_action(self, chat_id, action="typing"):
        return None


class FakeQRCodeLoginClient:
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

    def start(self):
        return {
            "uid": "qr-uid",
            "sign": "qr-sign",
            "time": 123,
            "qrcode": "qr-content",
            "qrcode_url": "https://qrcode.example/qr-uid",
            "app": "web",
        }

    def status(self, session):
        return {"status": 2, "message": "confirmed"}

    def login(self, session, require_confirmed=True):
        return "UID=uid-value;CID=cid-value;SEID=seid-value;KID=kid-value"


class Share115ReceiveTest(unittest.TestCase):
    def test_completed_offline_folder_needs_one_list_request_for_flat_resource(self):
        class Fake115:
            def __init__(self):
                self.calls = []

            def list_all_files_with_request_count(self, folder_id, page_size=1000):
                self.calls.append((folder_id, page_size))
                return (
                    [
                        {"fid": "video-139", "fn": "吞噬星空.S05E139.mkv", "fc": "1"},
                        {"fid": "advert", "fn": "更多资源请访问发布站.mkv", "fc": "1"},
                    ],
                    1,
                )

        client = Fake115()
        result = inspect_115_offline_result(
            client,
            {"file_id": "result-folder", "wp_path_id": "task-folder"},
            1,
            episode_hints={139},
            allow_season_mismatch=True,
        )

        self.assertEqual(client.calls, [("result-folder", 7000)])
        self.assertEqual(result["request_count"], 1)
        self.assertEqual(result["verified_episodes"], [139])
        self.assertEqual(result["unknown_videos"], ["更多资源请访问发布站.mkv"])

    def test_parse_115_share_url_reads_code_password_and_subdir(self):
        parsed = parse_115_share_url(
            "https://115.com/s/swabc123?password=xy99#/list/share/987654321"
        )

        self.assertEqual(parsed.share_code, "swabc123")
        self.assertEqual(parsed.receive_code, "xy99")
        self.assertEqual(parsed.pdir_fid, "987654321")

    def test_share_client_receives_all_top_level_items(self):
        transport = FakeShareTransport()
        client = Share115Client("UID=u; CID=c; SEID=s", transport=transport)

        result = client.receive_share_url("https://115.com/s/swabc123?password=xy99", "target-cid")

        receive_call = transport.calls[-1]
        self.assertEqual(receive_call["method"], "POST")
        self.assertIn("/webapi/share/receive", receive_call["url"])
        self.assertEqual(
            receive_call["data"],
            {
                "cid": "target-cid",
                "share_code": "swabc123",
                "receive_code": "xy99",
                "file_id": "file-1,folder-1",
            },
        )
        self.assertIn("UID=u", receive_call["headers"]["Cookie"])
        self.assertEqual(result["data"]["save_as_top_fids"], ["saved-1"])
        self.assertEqual(
            [item["name"] for item in result["data"]["manifest_items"]],
            ["ABF-001.mp4", "ABF-001", "ABF-002.mp4"],
        )
        self.assertEqual(result["data"]["manifest_request_count"], 2)
        self.assertEqual(
            [item["relative_path"] for item in result["data"]["manifest_items"]],
            ["ABF-001.mp4", "ABF-001", "ABF-001/ABF-002.mp4"],
        )

    def test_share_client_does_not_persist_acw_cookie_between_snap_requests(self):
        class AntiBotCookieJar:
            def __init__(self):
                self.has_acw_tc = False

            def __iter__(self):
                return iter(())

        class AntiBotPageTransport:
            def __init__(self):
                self.calls = []

            def new_cookie_jar(self):
                return AntiBotCookieJar()

            def request(self, method, url, headers=None, data=None, timeout=None, cookie_jar=None, parse_json=True):
                self.calls.append(
                    {
                        "method": method,
                        "url": url,
                        "headers": headers or {},
                        "data": data,
                        "parse_json": parse_json,
                        "cookie_jar": cookie_jar,
                    }
                )
                if parse_json is False:
                    raise AssertionError("share page must not be requested")
                if "share/snap" not in url:
                    raise AssertionError("unexpected request: %s" % url)
                if cookie_jar is not None and cookie_jar.has_acw_tc:
                    raise AssertionError("ACW_TC must not be sent to share/snap")
                query = urllib.parse.parse_qs(urllib.parse.urlsplit(url).query)
                cid = (query.get("cid") or [""])[0]
                if cookie_jar is not None:
                    cookie_jar.has_acw_tc = True
                if cid == "folder-1":
                    return {
                        "state": True,
                        "data": {"list": [{"fid": "file-1", "n": "movie.mkv", "s": 123}]},
                    }
                return {
                    "state": True,
                    "data": {"list": [{"cid": "folder-1", "n": "collection", "fc": 0}]},
                }

        transport = AntiBotPageTransport()
        client = Share115Client("UID=u; CID=c; SEID=s", transport=transport)

        items, manifest, request_count = client._inspect_share_tree(
            client._create_share_session("swabc123", "xy99"),
            "swabc123",
            "xy99",
        )

        self.assertEqual(request_count, 2)
        self.assertEqual([item["name"] for item in items], ["collection"])
        self.assertEqual([item["name"] for item in manifest], ["collection", "movie.mkv"])
        self.assertEqual(len(transport.calls), 2)
        self.assertTrue(all(call["method"] == "GET" for call in transport.calls))
        self.assertTrue(all(call["cookie_jar"] is None for call in transport.calls))

    def test_pipeline_submit_115_share_uses_share_client_not_offline_open_api(self):
        class FakeShareClient:
            def __init__(self):
                self.calls = []

            def receive_share_url(self, url, folder_id):
                self.calls.append((url, folder_id))
                return {
                    "state": True,
                    "code": 0,
                    "data": {
                        "share_code": "swabc123",
                        "source_url": url,
                        "items": [{"name": "ABF-001.mp4"}],
                        "save_as_top_fids": ["saved-1"],
                    },
                }

        fake = FakeShareClient()
        service = PipelineBotService(BotConfig(token="token", allowed_user_ids={1}, p115_cookie="UID=u"))
        with patch.object(service, "_build_115_share_client", return_value=fake):
            result = service.submit(
                "adult",
                "https://115.com/s/swabc123?password=xy99",
                target_folder_id="task-folder",
            )

        self.assertEqual(fake.calls, [("https://115.com/s/swabc123?password=xy99", "task-folder")])
        self.assertEqual(result["submit_kind"], "115_share_receive")
        self.assertEqual(result["task_status"]["status_name"], "success")
        self.assertEqual(result["task_status"]["source_kind"], "115_share")
        self.assertEqual(result["task_status"]["share_code"], "swabc123")
        self.assertEqual(result["task_status"]["received_file_ids"], ["saved-1"])
        self.assertEqual(result["task_status"]["received_item_names"], ["ABF-001.mp4"])
        self.assertEqual(result["task_status"]["source_manifest"]["entry_count"], 1)

    def test_pipeline_share_submit_uses_top_level_names_when_115_returns_no_saved_ids(self):
        class FakeShareClient:
            def receive_share_url(self, url, folder_id):
                return {
                    "state": True,
                    "code": 0,
                    "data": {
                        "share_code": "swabc123",
                        "source_url": url,
                        "items": [{"name": "Show", "is_dir": True}],
                        "manifest_items": [
                            {"name": "Show", "relative_path": "Show", "is_dir": True},
                            {
                                "name": "01.mkv",
                                "relative_path": "Show/01.mkv",
                                "is_dir": False,
                                "size": 100,
                            },
                        ],
                        "save_as_top_fids": [],
                    },
                }

        service = PipelineBotService(BotConfig(token="token", allowed_user_ids={1}, p115_cookie="UID=u"))
        with patch.object(service, "_build_115_share_client", return_value=FakeShareClient()):
            result = service.submit(
                "anime",
                "https://115.com/s/swabc123",
                target_folder_id="task-folder",
            )

        task = result["task_status"]
        self.assertIsNone(task["file_id"])
        self.assertEqual(task["received_file_ids"], [])
        self.assertEqual(task["received_item_names"], ["Show"])
        self.assertEqual(task["source_manifest"]["entry_count"], 2)

    def test_share_transfer_uses_source_manifest_and_backs_off_openlist_refresh(self):
        source_entries = [
            {"name": "Show", "relative_path": "Show", "is_dir": True, "size": 0},
            {"name": "01.mkv", "relative_path": "Show/01.mkv", "is_dir": False, "size": 100},
            {"name": "02.mkv", "relative_path": "Show/02.mkv", "is_dir": False, "size": 200},
        ]
        expected = build_transfer_manifest(
            source_entries,
            item_name=lambda item: item["name"],
            item_is_dir=lambda item: item["is_dir"],
            item_size=lambda item: item["size"],
            item_relative_path=lambda item: item["relative_path"],
        )

        class FakeOpenList:
            def __init__(self):
                self.complete = False
                self.calls = []

            def list_all(self, path, refresh=False):
                self.calls.append((path, refresh))
                if path == "/115/anime/import-task":
                    return [{"name": "Show", "is_dir": True, "size": 0}]
                if path == "/115/anime/import-task/Show":
                    files = [{"name": "01.mkv", "is_dir": False, "size": 100}]
                    if self.complete:
                        files.append({"name": "02.mkv", "is_dir": False, "size": 200})
                    return files
                raise AssertionError("unexpected OpenList path: %s" % path)

        task = {
            "source_kind": "115_share",
            "source_manifest": expected,
            "received_file_ids": [],
            "received_item_names": ["Show"],
            "file_id": None,
            "wp_path_id": "target-folder",
            "import_target_openlist_path": "/115/anime/import-task",
        }
        openlist = FakeOpenList()

        waiting_openlist = probe_import_transfer_visibility(None, openlist, task, now_fn=lambda: 100)
        self.assertEqual(waiting_openlist["status"], "running")
        self.assertEqual(waiting_openlist["reason"], "waiting_openlist_manifest")
        self.assertEqual(waiting_openlist["direct_request_count"], 0)
        self.assertEqual(waiting_openlist["next_probe_at"], 115)
        self.assertTrue(waiting_openlist["openlist_refresh_performed"])

        task["transfer_verification"] = waiting_openlist
        openlist.complete = True
        calls_before_backoff = len(openlist.calls)
        deferred = probe_import_transfer_visibility(None, openlist, task, now_fn=lambda: 101)
        self.assertEqual(deferred, waiting_openlist)
        self.assertEqual(len(openlist.calls), calls_before_backoff)

        verified = probe_import_transfer_visibility(None, openlist, task, now_fn=lambda: 115)
        self.assertEqual(verified["status"], "success")
        self.assertEqual(verified["direct_manifest"]["entry_count"], 3)
        self.assertEqual(verified["openlist_manifest"]["entry_count"], 3)
        self.assertEqual(verified["openlist_query_mode"], "cache_hit")
        self.assertIn(False, [refresh for _path, refresh in openlist.calls])
        self.assertIn(True, [refresh for _path, refresh in openlist.calls])

    def test_stale_openlist_cache_refreshes_only_after_manifest_mismatch(self):
        expected = build_transfer_manifest(
            [
                {"name": "Show", "relative_path": "Show", "is_dir": True, "size": 0},
                {"name": "01.mkv", "relative_path": "Show/01.mkv", "is_dir": False, "size": 100},
                {"name": "02.mkv", "relative_path": "Show/02.mkv", "is_dir": False, "size": 200},
            ],
            item_name=lambda item: item["name"],
            item_is_dir=lambda item: item["is_dir"],
            item_size=lambda item: item["size"],
            item_relative_path=lambda item: item["relative_path"],
        )

        class FakeOpenList:
            def __init__(self):
                self.calls = []

            def list_all(self, path, refresh=False):
                self.calls.append((path, refresh))
                if path == "/115/anime/import-task":
                    return [{"name": "Show", "is_dir": True, "size": 0}]
                if path == "/115/anime/import-task/Show":
                    files = [{"name": "01.mkv", "is_dir": False, "size": 100}]
                    if refresh:
                        files.append({"name": "02.mkv", "is_dir": False, "size": 200})
                    return files
                raise AssertionError("unexpected OpenList path: %s" % path)

        openlist = FakeOpenList()
        result = probe_import_transfer_visibility(
            None,
            openlist,
            {
                "source_kind": "115_share",
                "source_manifest": expected,
                "import_target_openlist_path": "/115/anime/import-task",
            },
            now_fn=lambda: 100,
        )

        self.assertEqual(result["status"], "success")
        self.assertEqual(result["openlist_query_mode"], "refreshed")
        self.assertEqual(result["openlist_refresh_request_count"], 2)
        self.assertEqual(
            openlist.calls,
            [
                ("/115/anime/import-task", False),
                ("/115/anime/import-task/Show", False),
                ("/115/anime/import-task", True),
                ("/115/anime/import-task/Show", True),
            ],
        )

    def test_openlist_target_error_triggers_one_refreshed_read(self):
        expected = build_transfer_manifest(
            [{"name": "movie.mkv", "is_dir": False, "size": 100}],
            item_name=lambda item: item["name"],
            item_is_dir=lambda item: item["is_dir"],
            item_size=lambda item: item["size"],
            item_relative_path=lambda item: item["name"],
        )

        class FakeOpenList:
            def __init__(self):
                self.calls = []

            def list_all(self, path, refresh=False):
                self.calls.append((path, refresh))
                if not refresh:
                    raise RuntimeError("OpenList list failed: object not found")
                return [{"name": "movie.mkv", "is_dir": False, "size": 100}]

        openlist = FakeOpenList()
        result = probe_import_transfer_visibility(
            None,
            openlist,
            {
                "source_kind": "115_share",
                "source_manifest": expected,
                "import_target_openlist_path": "/115/movie/import-task",
            },
            now_fn=lambda: 100,
        )

        self.assertEqual(result["status"], "success")
        self.assertEqual(result["openlist_query_mode"], "refreshed")
        self.assertEqual(
            openlist.calls,
            [
                ("/115/movie/import-task", False),
                ("/115/movie/import-task", True),
            ],
        )

    def test_offline_transfer_reads_115_once_then_reuses_locked_manifest(self):
        class Fake115:
            def __init__(self):
                self.calls = 0

            def list_all_files_with_request_count(self, folder_id, page_size=1000):
                self.calls += 1
                if folder_id != "target-folder":
                    raise AssertionError("unexpected 115 folder: %s" % folder_id)
                return ([{"fid": "video-1", "fn": "movie.mkv", "fc": "1", "fs": 100}], 1)

        class FakeOpenList:
            def __init__(self):
                self.complete = False
                self.calls = 0

            def list_all(self, path, refresh=False):
                self.calls += 1
                if path != "/115/movie/import-task":
                    raise AssertionError("unexpected OpenList request")
                if not self.complete:
                    return []
                return [{"name": "movie.mkv", "is_dir": False, "size": 100}]

        task = {
            "file_id": "video-1",
            "wp_path_id": "target-folder",
            "import_target_openlist_path": "/115/movie/import-task",
        }
        p115 = Fake115()
        openlist = FakeOpenList()
        first = probe_import_transfer_visibility(p115, openlist, task, now_fn=lambda: 100)
        self.assertEqual(first["status"], "running")
        self.assertTrue(first["direct_manifest_locked"])
        self.assertEqual(first["direct_request_count"], 1)
        self.assertEqual(p115.calls, 1)
        task["transfer_verification"] = first
        openlist.complete = True
        openlist_calls = openlist.calls
        second = probe_import_transfer_visibility(None, openlist, task, now_fn=lambda: 105)
        self.assertEqual(second, first)
        self.assertEqual(openlist.calls, openlist_calls)
        third = probe_import_transfer_visibility(None, openlist, task, now_fn=lambda: 115)
        self.assertEqual(third["status"], "success")
        self.assertEqual(third["openlist_query_mode"], "cache_hit")
        self.assertEqual(third["direct_request_count"], 1)
        self.assertEqual(p115.calls, 1)

    def test_transfer_backoff_skips_client_creation_before_next_probe(self):
        service = PipelineBotService(BotConfig(token="token", allowed_user_ids={1}))
        previous = {
            "status": "running",
            "reason": "waiting_openlist_manifest",
            "next_probe_at": 115,
        }
        task = {
            "import_target_openlist_path": "/115/movie/import-task",
            "transfer_verification": previous,
        }
        with patch("pipeline.bot.time.time", return_value=100), patch.object(
            service,
            "_build_openlist_scan_client",
            side_effect=AssertionError("OpenList client must not be created during backoff"),
        ), patch.object(
            service,
            "_call_115",
            side_effect=AssertionError("115 client must not be created during backoff"),
        ):
            result = service.verify_import_transfer("movie", task)

        self.assertEqual(result, previous)

    def test_share_verification_never_calls_direct_115_api(self):
        expected = build_transfer_manifest(
            [{"name": "movie.mkv", "is_dir": False, "size": 100}],
            item_name=lambda item: item["name"],
            item_is_dir=lambda item: item["is_dir"],
            item_size=lambda item: item["size"],
            item_relative_path=lambda item: item["name"],
        )

        class FakeOpenList:
            def list_all(self, path, refresh=False):
                self.request = (path, refresh)
                return [{"name": "movie.mkv", "is_dir": False, "size": 100}]

        service = PipelineBotService(BotConfig(token="token", allowed_user_ids={1}))
        openlist = FakeOpenList()
        with patch.object(service, "_build_openlist_scan_client", return_value=openlist), patch.object(
            service,
            "_call_115",
            side_effect=AssertionError("share verification must not call direct 115 API"),
        ):
            result = service.verify_import_transfer(
                "movie",
                {
                    "source_kind": "115_share",
                    "source_manifest": expected,
                    "import_target_openlist_path": "/115/movie/import-task",
                },
            )

        self.assertEqual(result["status"], "success")
        self.assertEqual(result["direct_request_count"], 0)
        self.assertEqual(result["openlist_query_mode"], "cache_hit")
        self.assertEqual(openlist.request, ("/115/movie/import-task", False))

    def test_msg_sync_does_not_start_before_transfer_verification(self):
        service = PipelineBotService(BotConfig(token="token", allowed_user_ids={1}))
        task = {
            "info_hash": "HASH",
            "status_name": "success",
            "import_target_openlist_path": "/115/anime/import-task",
            "msg_sync_status": "running",
        }
        with patch.object(
            service,
            "verify_import_transfer",
            return_value={
                "status": "running",
                "reason": "waiting_openlist_manifest",
                "direct_manifest": {"entry_count": 283},
                "openlist_manifest": {"entry_count": 172},
            },
        ), patch.object(
            service,
            "finalize_import_target",
            side_effect=AssertionError("target must not finalize before transfer verification"),
        ):
            result = service.sync_completed_task("anime", "完美世界", task)

        self.assertEqual(result["transfer_verify_status"], "running")
        self.assertEqual(result["msg_sync_status"], "running")
        self.assertNotIn("import_target_finalize_status", result)

    def test_direct_message_can_create_115_share_candidate(self):
        candidate = share115_candidate_from_text("转存 https://115cdn.com/s/swabc123 提取码 xy99")

        self.assertEqual(candidate["source_kind"], "115_share")
        self.assertEqual(candidate["shareCode"], "swabc123")
        self.assertEqual(candidate["download_uri"], "https://115cdn.com/s/swabc123?password=xy99")

    def test_qrcode_login_client_builds_cookie_after_confirmed_scan(self):
        transport = FakeQRTransport()
        client = P115QRCodeLoginClient(transport=transport)

        session = client.start()
        status = client.status(session)
        cookie = client.login(session)

        self.assertEqual(session["uid"], "qr-uid")
        self.assertEqual(status["status"], 2)
        self.assertTrue(p115_cookie_is_valid(cookie))
        self.assertIn("UID=uid-value", cookie)

    def test_candidate_store_persists_p115_cookie_and_qr_session(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = CandidateStore(os.path.join(tmp, "state.db"))
            store.set_p115_cookie("UID=uid-value;CID=cid-value;SEID=seid-value")
            session_id = store.create_p115_qr_session(1, 2, {"uid": "qr-uid", "sign": "qr-sign", "time": 123})
            session = store.load_p115_qr_session(session_id)
            updated = store.update_p115_qr_session(session_id, "confirmed")
            cookie = store.get_p115_cookie()

        self.assertEqual(cookie, "UID=uid-value;CID=cid-value;SEID=seid-value")
        self.assertEqual(session["session"]["uid"], "qr-uid")
        self.assertEqual(updated["status"], "confirmed")

    def test_pipeline_share_client_reads_cookie_from_msg_storage(self):
        class FakeMediaStationClient:
            def get_cloud115_cookie(self):
                return "UID=msg;CID=msg;SEID=msg"

        service = PipelineBotService(
            BotConfig(
                token="token",
                allowed_user_ids={1},
                msg_admin_user="admin",
                msg_admin_password="secret",
            )
        )

        with patch("pipeline.bot.MediaStationClient", return_value=FakeMediaStationClient()), patch(
            "pipeline.bot.Share115Client"
        ) as client_cls:
            service._build_115_share_client()

        client_cls.assert_called_once_with("UID=msg;CID=msg;SEID=msg")

    def test_p115_cookie_bot_command_points_to_msg_management(self):
        with tempfile.TemporaryDirectory() as tmp:
            config = BotConfig(token="token", allowed_user_ids={1}, state_db_path=os.path.join(tmp, "state.db"))
            store = CandidateStore(config.state_db_path)
            telegram = FakeTelegram()
            bot = TelegramBot(config, telegram, store, PipelineBotService(config))

            bot.handle_update({"message": {"chat": {"id": 10}, "from": {"id": 1}, "text": "/p115_cookie"}})
            saved_cookie = store.get_p115_cookie()

        self.assertIn("MSG 管理后台", telegram.messages[0]["text"])
        self.assertEqual(saved_cookie, "")


if __name__ == "__main__":
    unittest.main()
