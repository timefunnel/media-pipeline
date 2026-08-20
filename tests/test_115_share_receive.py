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
    inspect_115_offline_result,
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

    def test_pipeline_share_client_prefers_state_cookie_provider(self):
        service = PipelineBotService(
            BotConfig(token="token", allowed_user_ids={1}, p115_cookie="UID=env;CID=env;SEID=env"),
            p115_cookie_provider=lambda: "UID=db;CID=db;SEID=db",
        )

        with patch("pipeline.bot.Share115Client") as client_cls:
            service._build_115_share_client()

        client_cls.assert_called_once_with("UID=db;CID=db;SEID=db")

    def test_p115_cookie_bot_flow_saves_cookie_after_scan_confirmed(self):
        with tempfile.TemporaryDirectory() as tmp:
            config = BotConfig(token="token", allowed_user_ids={1}, state_db_path=os.path.join(tmp, "state.db"))
            store = CandidateStore(config.state_db_path)
            telegram = FakeTelegram()
            bot = TelegramBot(config, telegram, store, PipelineBotService(config))

            bot.handle_update({"message": {"chat": {"id": 10}, "from": {"id": 1}, "text": "/p115_cookie"}})
            with patch("pipeline.bot.P115QRCodeLoginClient", FakeQRCodeLoginClient):
                bot.handle_update(
                    {
                        "callback_query": {
                            "id": "cb-start",
                            "from": {"id": 1},
                            "message": {"chat": {"id": 10}, "message_id": 11},
                            "data": "p115_cookie_start:1",
                        }
                    }
                )
                session_id = store.load_p115_qr_session(1)["id"]
                bot.handle_update(
                    {
                        "callback_query": {
                            "id": "cb-check",
                            "from": {"id": 1},
                            "message": {"chat": {"id": 10}, "message_id": 11},
                            "data": "p115_cookie_check:%s" % session_id,
                        }
                    }
                )
                saved_cookie = store.get_p115_cookie()

        self.assertIn("未配置", telegram.messages[0]["text"])
        self.assertIn("二维码链接", telegram.edits[0]["text"])
        self.assertIn("已保存", telegram.edits[-1]["text"])
        self.assertTrue(p115_cookie_is_valid(saved_cookie))


if __name__ == "__main__":
    unittest.main()
