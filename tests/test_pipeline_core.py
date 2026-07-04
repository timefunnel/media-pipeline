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
from pipeline.openlist import OpenListClient, OpenListTokenProvider
from pipeline.openlist_tokens import OpenListTokenStore
from pipeline.prowlarr import ProwlarrClient, ProwlarrConfig, torrent_bytes_to_magnet
from pipeline.resource_selector import ResourceSelector


class BotConfigTest(unittest.TestCase):
    def test_bot_config_rejects_missing_allowed_user_ids_without_fallback(self):
        from pipeline.bot import BotConfig

        with self.assertRaisesRegex(RuntimeError, "TG_ALLOWED_USER_IDS missing"):
            BotConfig.from_env({"TG_BOT_TOKEN": "123:token"})

    def test_bot_config_reads_token_allowed_user_and_state_db(self):
        from pipeline.bot import BotConfig

        config = BotConfig.from_env(
            {
                "TG_BOT_TOKEN": "123:token",
                "TG_ALLOWED_USER_IDS": "700656624",
                "BOT_STATE_DB": "/bot/state.db",
            }
        )

        self.assertEqual(config.token, "123:token")
        self.assertEqual(config.allowed_user_ids, {700656624})
        self.assertEqual(config.state_db_path, "/bot/state.db")
        self.assertEqual(config.telegram_timeout, 90)

    def test_bot_config_enables_mediastation_when_credentials_exist(self):
        from pipeline.bot import BotConfig

        config = BotConfig.from_env(
            {
                "TG_BOT_TOKEN": "123:token",
                "TG_ALLOWED_USER_IDS": "700656624",
                "MSG_BASE_URL": "http://127.0.0.1:18080/api",
                "MSG_DATABASE_DSN": "postgresql://mediastation:mediastation@127.0.0.1:15432/mediastation",
                "MSG_ADMIN_USER": "admin",
                "MSG_ADMIN_PASSWORD": "secret",
                "MSG_SYNC_POLL_SECONDS": "0",
            }
        )

        self.assertTrue(config.msg_enabled)
        self.assertEqual(config.msg_base_url, "http://127.0.0.1:18080/api")
        self.assertEqual(config.msg_database_dsn, "postgresql://mediastation:mediastation@127.0.0.1:15432/mediastation")
        self.assertEqual(config.msg_admin_user, "admin")
        self.assertEqual(config.msg_admin_password, "secret")
        self.assertEqual(config.msg_sync_poll_seconds, 0)
        self.assertTrue(config.openlist_pre_scan_clean_enabled)
        self.assertEqual(config.openlist_pre_scan_clean_max_bytes, 20 * 1024 * 1024)
        self.assertTrue(config.openlist_adult_code_format_enabled)

    def test_bot_config_reads_openlist_pre_scan_clean_switch(self):
        from pipeline.bot import BotConfig

        config = BotConfig.from_env(
            {
                "TG_BOT_TOKEN": "123:token",
                "TG_ALLOWED_USER_IDS": "700656624",
                "OPENLIST_PRE_SCAN_CLEAN_ENABLED": "1",
                "OPENLIST_PRE_SCAN_CLEAN_MAX_BYTES": "12345",
            }
        )

        self.assertTrue(config.openlist_pre_scan_clean_enabled)
        self.assertEqual(config.openlist_pre_scan_clean_max_bytes, 12345)

    def test_bot_config_can_disable_openlist_pre_scan_clean(self):
        from pipeline.bot import BotConfig

        config = BotConfig.from_env(
            {
                "TG_BOT_TOKEN": "123:token",
                "TG_ALLOWED_USER_IDS": "700656624",
                "OPENLIST_PRE_SCAN_CLEAN_ENABLED": "0",
            }
        )

        self.assertFalse(config.openlist_pre_scan_clean_enabled)

    def test_bot_config_reads_adult_code_format_switch(self):
        from pipeline.bot import BotConfig

        config = BotConfig.from_env(
            {
                "TG_BOT_TOKEN": "123:token",
                "TG_ALLOWED_USER_IDS": "700656624",
                "OPENLIST_ADULT_CODE_FORMAT_ENABLED": "0",
            }
        )

        self.assertFalse(config.openlist_adult_code_format_enabled)

    def test_bot_config_reads_sync_recovery_interval(self):
        from pipeline.bot import BotConfig

        config = BotConfig.from_env(
            {
                "TG_BOT_TOKEN": "123:token",
                "TG_ALLOWED_USER_IDS": "700656624",
                "BOT_SYNC_RECOVERY_INTERVAL_SECONDS": "120",
            }
        )

        self.assertEqual(config.sync_recovery_interval_seconds, 120)

class CandidateStoreTest(unittest.TestCase):
    def test_candidate_store_persists_candidate_for_callback(self):
        from pipeline.bot import CandidateStore

        with tempfile.TemporaryDirectory() as tmp:
            store = CandidateStore(str(Path(tmp) / "state.db"))
            candidate_id = store.save_candidate(
                user_id=700656624,
                chat_id=9001,
                category="movie",
                query="sintel",
                candidate={"title": "Sintel", "download_uri": "magnet:?xt=urn:btih:ABC", "rank": 1},
            )

            loaded = store.load_candidate(candidate_id)

        self.assertEqual(loaded["user_id"], 700656624)
        self.assertEqual(loaded["chat_id"], 9001)
        self.assertEqual(loaded["category"], "movie")
        self.assertEqual(loaded["query"], "sintel")
        self.assertEqual(loaded["candidate"]["download_uri"], "magnet:?xt=urn:btih:ABC")

    def test_candidate_store_persists_recent_offline_tasks(self):
        from pipeline.bot import CandidateStore

        with tempfile.TemporaryDirectory() as tmp:
            store = CandidateStore(str(Path(tmp) / "state.db"))
            store.save_task(
                user_id=700656624,
                chat_id=9001,
                category="movie",
                title="Sintel",
                task={"info_hash": "ABC", "status_name": "downloading", "percent_done": 10},
            )

            loaded = store.load_task("abc")
            recent = store.list_tasks(700656624, limit=10)

        self.assertEqual(loaded["info_hash"], "ABC")
        self.assertEqual(loaded["category"], "movie")
        self.assertEqual(loaded["task"]["status_name"], "downloading")
        self.assertEqual([item["info_hash"] for item in recent], ["ABC"])

    def test_candidate_store_persists_search_session_candidate_ids(self):
        from pipeline.bot import CandidateStore

        with tempfile.TemporaryDirectory() as tmp:
            store = CandidateStore(str(Path(tmp) / "state.db"))
            first_id = store.save_candidate(700656624, 9001, "movie", "sintel", {"title": "Sintel 720p"})
            second_id = store.save_candidate(700656624, 9001, "movie", "sintel", {"title": "Sintel 1080p"})
            session_id = store.save_search_session(700656624, 9001, "movie", "sintel", [first_id, second_id])

            session = store.load_search_session(session_id)

        self.assertEqual(session["user_id"], 700656624)
        self.assertEqual(session["chat_id"], 9001)
        self.assertEqual(session["query"], "sintel")
        self.assertEqual(session["candidate_ids"], [first_id, second_id])

    def test_candidate_store_persists_migration_candidate(self):
        from pipeline.bot import CandidateStore

        with tempfile.TemporaryDirectory() as tmp:
            store = CandidateStore(str(Path(tmp) / "state.db"))
            candidate_id = store.save_migration_candidate(
                700656624,
                9001,
                "成龙历险记",
                {
                    "title": "成龙历险记",
                    "category": "tv",
                    "source_openlist_path": "/115/剧集/成龙历险记",
                    "media_count": 95,
                },
            )

            loaded = store.load_migration_candidate(candidate_id)

        self.assertEqual(loaded["user_id"], 700656624)
        self.assertEqual(loaded["chat_id"], 9001)
        self.assertEqual(loaded["query"], "成龙历险记")
        self.assertEqual(loaded["candidate"]["source_openlist_path"], "/115/剧集/成龙历险记")

    def test_candidate_store_lists_running_msg_sync_tasks(self):
        from pipeline.bot import CandidateStore

        with tempfile.TemporaryDirectory() as tmp:
            store = CandidateStore(str(Path(tmp) / "state.db"))
            store.save_task(
                700656624,
                9001,
                "movie",
                "Running",
                {"info_hash": "AAA", "status_name": "success", "msg_sync_status": "running"},
            )
            store.save_task(
                700656624,
                9001,
                "movie",
                "Synced",
                {"info_hash": "BBB", "status_name": "success", "msg_sync_status": "success", "msg_scrape_status": "success"},
            )
            store.save_task(
                700656624,
                9001,
                "movie",
                "Downloading",
                {"info_hash": "CCC", "status_name": "downloading", "msg_sync_status": "running"},
            )

            running = store.list_msg_sync_running_tasks(limit=10)

        self.assertEqual([record["info_hash"] for record in running], ["AAA"])

    def test_candidate_store_lists_active_115_tasks(self):
        from pipeline.bot import CandidateStore

        with tempfile.TemporaryDirectory() as tmp:
            store = CandidateStore(str(Path(tmp) / "state.db"))
            store.save_task(700656624, 9001, "movie", "Submitted", {"info_hash": "AAA", "status_name": "submitted"})
            store.save_task(700656624, 9001, "movie", "Allocating", {"info_hash": "BBB", "status_name": "allocating"})
            store.save_task(700656624, 9001, "movie", "Downloading", {"info_hash": "CCC", "status_name": "downloading"})
            store.save_task(700656624, 9001, "movie", "Success", {"info_hash": "DDD", "status_name": "success"})
            store.save_task(700656624, 9001, "movie", "Failed", {"info_hash": "EEE", "status_name": "failed"})

            active = store.list_active_115_tasks(limit=10)

        self.assertEqual([record["info_hash"] for record in active], ["AAA", "BBB", "CCC"])

    def test_candidate_store_replaces_openlist_dedupe_entries(self):
        from pipeline.bot import CandidateStore

        with tempfile.TemporaryDirectory() as tmp:
            store = CandidateStore(str(Path(tmp) / "state.db"))
            store.replace_dedupe_entries(
                "openlist",
                [
                    {
                        "category": "adult",
                        "identity_type": "adult_code",
                        "identity_value": "SSIS-450",
                        "title": "SSIS-450 Existing",
                        "path": "/115/成人/SSIS-450 Existing",
                    },
                    {
                        "category": "movie",
                        "identity_type": "normalized_title",
                        "identity_value": "sintel",
                        "title": "Sintel",
                        "path": "/115/电影/Sintel",
                    },
                ],
            )

            adult_match = store.find_dedupe_entries("adult", [{"identity_type": "adult_code", "identity_value": "SSIS-450"}])
            movie_match = store.find_dedupe_entries("movie", [{"identity_type": "normalized_title", "identity_value": "sintel"}])

            store.replace_dedupe_entries(
                "openlist",
                [
                    {
                        "category": "adult",
                        "identity_type": "adult_code",
                        "identity_value": "IPX-789",
                        "title": "IPX-789 Existing",
                        "path": "/115/成人/IPX-789 Existing",
                    }
                ],
            )
            old_match = store.find_dedupe_entries("adult", [{"identity_type": "adult_code", "identity_value": "SSIS-450"}])
            new_match = store.find_dedupe_entries("adult", [{"identity_type": "adult_code", "identity_value": "IPX-789"}])

        self.assertEqual(adult_match[0]["title"], "SSIS-450 Existing")
        self.assertEqual(movie_match[0]["path"], "/115/电影/Sintel")
        self.assertEqual(old_match, [])
        self.assertEqual(new_match[0]["title"], "IPX-789 Existing")


    def test_candidate_store_migrates_openlist_dedupe_entries(self):
        from pipeline.bot import CandidateStore

        with tempfile.TemporaryDirectory() as tmp:
            store = CandidateStore(str(Path(tmp) / "state.db"))
            store.replace_dedupe_entries(
                "openlist",
                [
                    {
                        "category": "tv",
                        "identity_type": "normalized_title",
                        "identity_value": "jackie",
                        "title": "Jackie",
                        "path": "/115/tv/Jackie",
                    }
                ],
            )

            changed = store.migrate_dedupe_entries("/115/tv/Jackie", "/115/anime/Jackie", "tv", "anime")
            old_match = store.find_dedupe_entries("tv", [{"identity_type": "normalized_title", "identity_value": "jackie"}])
            new_match = store.find_dedupe_entries("anime", [{"identity_type": "normalized_title", "identity_value": "jackie"}])

        self.assertEqual(changed, 1)
        self.assertEqual(old_match, [])
        self.assertEqual(new_match[0]["path"], "/115/anime/Jackie")


class TelegramBotTest(unittest.TestCase):
    def test_telegram_api_send_chat_action_uses_send_chat_action_endpoint(self):
        from pipeline.bot import TelegramApi

        class FakeTelegramTransport:
            def __init__(self):
                self.calls = []

            def request(self, url, payload, timeout=None):
                self.calls.append({"url": url, "payload": payload, "timeout": timeout})
                return {"ok": True}

        transport = FakeTelegramTransport()
        api = TelegramApi("token", transport=transport, timeout=12)

        api.send_chat_action(9001, "typing")

        self.assertEqual(
            transport.calls,
            [
                {
                    "url": "https://api.telegram.org/bottoken/sendChatAction",
                    "payload": {"chat_id": 9001, "action": "typing"},
                    "timeout": 12,
                }
            ],
        )

    def test_telegram_transport_converts_http_error_body_to_runtime_error(self):
        import urllib.error

        from pipeline.bot import TelegramTransport

        error = urllib.error.HTTPError(
            "https://api.telegram.org/bottoken/editMessageText",
            400,
            "Bad Request",
            {},
            io.BytesIO(b'{"ok":false,"description":"Bad Request: message is not modified"}'),
        )

        with patch("pipeline.bot.urllib.request.urlopen", side_effect=error):
            with self.assertRaisesRegex(RuntimeError, "message is not modified"):
                TelegramTransport().request("https://api.telegram.org/bottoken/editMessageText", {"chat_id": 9001}, timeout=3)

    def test_typing_action_pulse_repeats_until_context_exits(self):
        from pipeline.bot import TypingActionPulse

        class FakeBot:
            def __init__(self):
                self.actions = []

            def _send_typing_action(self, chat_id):
                self.actions.append(chat_id)

        bot = FakeBot()

        with TypingActionPulse(bot, 9001, interval_seconds=0.01):
            time.sleep(0.035)
        sent_count = len(bot.actions)
        time.sleep(0.03)

        self.assertGreaterEqual(sent_count, 2)
        self.assertEqual(bot.actions, [9001] * sent_count)

    def test_message_version_reports_runtime_version(self):
        from pipeline.bot import BotConfig, CandidateStore, TelegramBot

        with tempfile.TemporaryDirectory() as tmp:
            store = CandidateStore(str(Path(tmp) / "state.db"))
            telegram = FakeTelegram()
            service = FakeBotService()
            bot = TelegramBot(BotConfig("token", {700656624}, store.db_path), telegram, store, service)

            with patch.dict("os.environ", {"MEDIA_PIPELINE_VERSION": "9.9.9", "MEDIA_PIPELINE_REVISION": "abc123"}):
                bot.handle_update(
                    {
                        "message": {"chat": {"id": 9001}, "from": {"id": 700656624}, "text": "/version"}
                    }
                )

        self.assertEqual(service.search_calls, [])
        self.assertEqual(telegram.messages[0]["text"], "media-pipeline 9.9.9\nrevision: abc123")

    def test_message_help_describes_direct_keyword_flow(self):
        from pipeline.bot import BotConfig, CandidateStore, TelegramBot

        with tempfile.TemporaryDirectory() as tmp:
            store = CandidateStore(str(Path(tmp) / "state.db"))
            telegram = FakeTelegram()
            service = FakeBotService()
            bot = TelegramBot(BotConfig("token", {700656624}, store.db_path), telegram, store, service)

            bot.handle_update({"message": {"chat": {"id": 9001}, "from": {"id": 700656624}, "text": "/help"}})

        self.assertEqual(service.search_calls, [])
        self.assertIn("直接发送关键词、番号或磁链即可", telegram.messages[0]["text"])
        self.assertIn("/tasks 查看最近任务", telegram.messages[0]["text"])
        self.assertIn("/migrate <关键词>", telegram.messages[0]["text"])

    def test_migrate_command_searches_msg_and_prompts_candidates(self):
        from pipeline.bot import BotConfig, CandidateStore, TelegramBot

        migration_candidate = {
            "title": "成龙历险记",
            "category": "tv",
            "library_name": "剧集",
            "source_openlist_path": "/115/剧集/成龙历险记",
            "source_kind": "folder",
            "media_count": 95,
            "total_size": 10 * 1024 * 1024 * 1024,
        }
        with tempfile.TemporaryDirectory() as tmp:
            store = CandidateStore(str(Path(tmp) / "state.db"))
            telegram = FakeTelegram()
            service = FakeBotService(migration_candidates=[migration_candidate])
            bot = TelegramBot(BotConfig("token", {700656624}, store.db_path), telegram, store, service)

            bot.handle_update({"message": {"chat": {"id": 9001}, "from": {"id": 700656624}, "text": "/migrate 成龙历险记"}})

            button = telegram.messages[0]["reply_markup"]["inline_keyboard"][0][0]
            candidate_id = int(button["callback_data"].split(":")[-1])
            stored = store.load_migration_candidate(candidate_id)

        self.assertEqual(service.migration_search_calls, [("成龙历险记", 20)])
        self.assertEqual(service.search_calls, [])
        self.assertIn("媒体迁移搜索：成龙历险记", telegram.messages[0]["text"])
        self.assertIn("/115/剧集/成龙历险记", telegram.messages[0]["text"])
        self.assertEqual(button["text"], "迁移 1")
        self.assertEqual(stored["candidate"]["title"], "成龙历险记")

    def test_migrate_callbacks_confirm_and_execute(self):
        from pipeline.bot import BotConfig, CandidateStore, TelegramBot

        migration_candidate = {
            "title": "成龙历险记",
            "category": "tv",
            "library_name": "剧集",
            "source_openlist_path": "/115/剧集/成龙历险记",
            "source_kind": "folder",
            "media_count": 95,
            "total_size": 10 * 1024 * 1024 * 1024,
        }
        with tempfile.TemporaryDirectory() as tmp:
            store = CandidateStore(str(Path(tmp) / "state.db"))
            telegram = FakeTelegram()
            service = FakeBotService(migration_candidates=[migration_candidate])
            bot = TelegramBot(BotConfig("token", {700656624}, store.db_path), telegram, store, service)

            bot.handle_update({"message": {"chat": {"id": 9001}, "from": {"id": 700656624}, "text": "/migrate 成龙历险记"}})
            select_button = telegram.messages[0]["reply_markup"]["inline_keyboard"][0][0]
            bot.handle_update(
                {
                    "callback_query": {
                        "id": "cb-migrate-select",
                        "from": {"id": 700656624},
                        "message": {"chat": {"id": 9001}, "message_id": 701},
                        "data": select_button["callback_data"],
                    }
                }
            )
            anime_button = [
                button
                for row in telegram.edits[-1]["reply_markup"]["inline_keyboard"]
                for button in row
                if button["callback_data"].startswith("migrate_to:anime:")
            ][0]
            bot.handle_update(
                {
                    "callback_query": {
                        "id": "cb-migrate-to",
                        "from": {"id": 700656624},
                        "message": {"chat": {"id": 9001}, "message_id": 701},
                        "data": anime_button["callback_data"],
                    }
                }
            )
            confirm_button = telegram.edits[-1]["reply_markup"]["inline_keyboard"][0][0]
            bot.handle_update(
                {
                    "callback_query": {
                        "id": "cb-migrate-confirm",
                        "from": {"id": 700656624},
                        "message": {"chat": {"id": 9001}, "message_id": 701},
                        "data": confirm_button["callback_data"],
                    }
                }
            )

        self.assertEqual(telegram.answers[0], {"callback_query_id": "cb-migrate-select", "text": "请选择目标库"})
        self.assertEqual(telegram.answers[1], {"callback_query_id": "cb-migrate-to", "text": "请确认迁移"})
        self.assertEqual(telegram.answers[2], {"callback_query_id": "cb-migrate-confirm", "text": "开始迁移"})
        self.assertIn("目标路径：/115/动漫/成龙历险记", telegram.edits[1]["text"])
        self.assertEqual(service.migration_calls, [("/115/剧集/成龙历险记", "anime")])
        self.assertIn("迁移完成：成龙历险记", telegram.edits[-1]["text"])
        self.assertIn("目标路径：/115/动漫/成龙历险记", telegram.edits[-1]["text"])

    def test_legacy_search_command_is_not_used_as_search_entry(self):
        from pipeline.bot import BotConfig, CandidateStore, TelegramBot

        with tempfile.TemporaryDirectory() as tmp:
            store = CandidateStore(str(Path(tmp) / "state.db"))
            telegram = FakeTelegram()
            service = FakeBotService(search_results=[{"title": "Sintel", "download_uri": "magnet:?xt=urn:btih:AAA"}])
            bot = TelegramBot(BotConfig("token", {700656624}, store.db_path), telegram, store, service)

            bot.handle_update({"message": {"chat": {"id": 9001}, "from": {"id": 700656624}, "text": "/movie sintel"}})

        self.assertEqual(service.search_calls, [])
        self.assertIn("不再作为搜索入口", telegram.messages[0]["text"])

    def test_message_search_saves_candidates_and_sends_rank_buttons(self):
        from pipeline.bot import BotConfig, CandidateStore, TelegramBot

        with tempfile.TemporaryDirectory() as tmp:
            store = CandidateStore(str(Path(tmp) / "state.db"))
            telegram = FakeTelegram()
            service = FakeBotService(
                search_results=[
                    {"title": "Sintel 720p", "download_uri": "magnet:?xt=urn:btih:AAA", "rank": 1, "seeders": 10, "size": 100},
                    {"title": "Sintel 1080p", "download_uri": "magnet:?xt=urn:btih:BBB", "rank": 2, "seeders": 8, "size": 200},
                ]
            )
            bot = TelegramBot(BotConfig("token", {700656624}, store.db_path), telegram, store, service)

            bot.handle_update(
                {
                    "message": {
                        "chat": {"id": 9001},
                        "from": {"id": 700656624},
                        "text": "sintel",
                    }
                }
            )

            sent = telegram.messages[0]
            self.assertEqual(service.search_calls, [("sintel", "movie", 100)])
            self.assertIn("1. Sintel 720p", sent["text"])
            self.assertIn("第 1/1 页，共 2 条", sent["text"])
            self.assertEqual(sent["reply_markup"]["inline_keyboard"][0][0]["text"], "#1 入库")
            self.assertRegex(sent["reply_markup"]["inline_keyboard"][0][0]["callback_data"], r"^choose:\d+$")

    def test_message_search_sends_typing_action_before_search(self):
        from pipeline.bot import BotConfig, CandidateStore, TelegramBot

        with tempfile.TemporaryDirectory() as tmp:
            store = CandidateStore(str(Path(tmp) / "state.db"))
            telegram = FakeTelegram()
            service = FakeBotService(search_results=[{"title": "Sintel", "download_uri": "magnet:?xt=urn:btih:AAA", "rank": 1}])
            bot = TelegramBot(BotConfig("token", {700656624}, store.db_path), telegram, store, service)

            bot.handle_update(
                {
                    "message": {
                        "chat": {"id": 9001},
                        "from": {"id": 700656624},
                        "text": "sintel",
                    }
                }
            )

        self.assertEqual(telegram.chat_actions, [{"chat_id": 9001, "action": "typing"}])
        self.assertEqual(service.search_calls, [("sintel", "movie", 100)])

    def test_typing_action_failure_does_not_block_search(self):
        from pipeline.bot import BotConfig, CandidateStore, TelegramBot

        with tempfile.TemporaryDirectory() as tmp:
            store = CandidateStore(str(Path(tmp) / "state.db"))
            telegram = FakeTelegram(chat_action_error=RuntimeError("typing failed"))
            service = FakeBotService(search_results=[{"title": "Sintel", "download_uri": "magnet:?xt=urn:btih:AAA", "rank": 1}])
            bot = TelegramBot(BotConfig("token", {700656624}, store.db_path), telegram, store, service)

            bot.handle_update(
                {
                    "message": {
                        "chat": {"id": 9001},
                        "from": {"id": 700656624},
                        "text": "sintel",
                    }
                }
            )

        self.assertEqual(service.search_calls, [("sintel", "movie", 100)])
        self.assertIn("搜索结果：sintel", telegram.messages[0]["text"])

    def test_message_magnet_prompts_library_without_searching(self):
        from pipeline.bot import BotConfig, CandidateStore, TelegramBot

        magnet = "magnet:?xt=urn:btih:ABCDEF1234567890&dn=Sintel%201080p"
        with tempfile.TemporaryDirectory() as tmp:
            store = CandidateStore(str(Path(tmp) / "state.db"))
            telegram = FakeTelegram()
            service = FakeBotService()
            bot = TelegramBot(BotConfig("token", {700656624}, store.db_path), telegram, store, service)

            bot.handle_update(
                {
                    "message": {
                        "chat": {"id": 9001},
                        "from": {"id": 700656624},
                        "text": magnet,
                    }
                }
            )

            self.assertEqual(service.search_calls, [])
            sent = telegram.messages[0]
            self.assertIn("已选择：Sintel 1080p", sent["text"])
            self.assertIn("站点：磁链", sent["text"])
            self.assertIn("链接类型：磁链", sent["text"])
            self.assertIn("info_hash：ABCDEF1234567890", sent["text"])
            buttons = [button for row in sent["reply_markup"]["inline_keyboard"] for button in row]
            self.assertEqual([button["text"] for button in buttons], ["电影", "剧集", "动漫", "成人", "其他"])
            self.assertRegex(buttons[0]["callback_data"], r"^profile:movie:\d+$")
            self.assertRegex(buttons[1]["callback_data"], r"^profile:tv:\d+$")
            self.assertRegex(buttons[2]["callback_data"], r"^profile:anime:\d+$")
            self.assertRegex(buttons[3]["callback_data"], r"^profile:adult:\d+$")
            self.assertRegex(buttons[4]["callback_data"], r"^profile:other:\d+$")
            candidate_id = int(buttons[0]["callback_data"].split(":")[-1])
            record = store.load_candidate(candidate_id)
            self.assertEqual(record["candidate"]["title"], "Sintel 1080p")
            self.assertEqual(record["candidate"]["download_uri"], magnet)
            self.assertEqual(record["candidate"]["infoHash"], "ABCDEF1234567890")

    def test_search_results_can_page_without_researching(self):
        from pipeline.bot import BotConfig, CandidateStore, TelegramBot

        with tempfile.TemporaryDirectory() as tmp:
            store = CandidateStore(str(Path(tmp) / "state.db"))
            telegram = FakeTelegram()
            service = FakeBotService(
                search_results=[
                    {
                        "title": "Sintel %02d 1080p" % rank,
                        "download_uri": "magnet:?xt=urn:btih:%02d" % rank,
                        "rank": rank,
                        "seeders": 40 - rank,
                        "size": 1024 * 1024 * 1024,
                    }
                    for rank in range(1, 8)
                ]
            )
            bot = TelegramBot(BotConfig("token", {700656624}, store.db_path), telegram, store, service)

            bot.handle_update(
                {
                    "message": {
                        "chat": {"id": 9001},
                        "from": {"id": 700656624},
                        "text": "sintel",
                    }
                }
            )

            first = telegram.messages[0]
            self.assertEqual(service.search_calls, [("sintel", "movie", 100)])
            self.assertIn("第 1/2 页，共 7 条", first["text"])
            self.assertIn("5. Sintel 05 1080p", first["text"])
            self.assertNotIn("6. Sintel 06 1080p", first["text"])
            nav = first["reply_markup"]["inline_keyboard"][-4]
            self.assertEqual([button["text"] for button in nav], ["下一页"])
            self.assertEqual(
                [(button["text"], button["callback_data"]) for button in first["reply_markup"]["inline_keyboard"][-3]],
                [("[1]", "page:1:0"), ("2", "page:1:1")],
            )
            self.assertEqual(
                [(button["text"], button["callback_data"].split(":", 1)[0]) for button in first["reply_markup"]["inline_keyboard"][-2]],
                [("🔞", "adult_search"), ("动漫", "anime_search")],
            )
            self.assertEqual(first["reply_markup"]["inline_keyboard"][-1][0]["text"], "关闭")

            bot.handle_update(
                {
                    "callback_query": {
                        "id": "cb-page",
                        "from": {"id": 700656624},
                        "message": {"chat": {"id": 9001}, "message_id": 701},
                        "data": nav[0]["callback_data"],
                    }
                }
            )

            self.assertEqual(service.search_calls, [("sintel", "movie", 100)])
            self.assertEqual(telegram.answers[-1], {"callback_query_id": "cb-page", "text": "第 2/2 页"})
            self.assertEqual(telegram.messages[1:], [])
            self.assertEqual(telegram.edits[0]["chat_id"], 9001)
            self.assertEqual(telegram.edits[0]["message_id"], 701)
            self.assertIn("第 2/2 页，共 7 条", telegram.edits[0]["text"])
            self.assertIn("6. Sintel 06 1080p", telegram.edits[0]["text"])
            self.assertNotIn("1. Sintel 01 1080p", telegram.edits[0]["text"])
            self.assertEqual([button["text"] for button in telegram.edits[0]["reply_markup"]["inline_keyboard"][-4]], ["上一页"])
            self.assertEqual(
                [(button["text"], button["callback_data"]) for button in telegram.edits[0]["reply_markup"]["inline_keyboard"][-3]],
                [("1", "page:1:0"), ("[2]", "page:1:1")],
            )
            self.assertEqual(
                [
                    (button["text"], button["callback_data"].split(":", 1)[0])
                    for button in telegram.edits[0]["reply_markup"]["inline_keyboard"][-2]
                ],
                [("🔞", "adult_search"), ("动漫", "anime_search")],
            )
            self.assertEqual(telegram.edits[0]["reply_markup"]["inline_keyboard"][-1][0]["text"], "关闭")

    def test_search_results_can_jump_directly_to_page_number(self):
        from pipeline.bot import BotConfig, CandidateStore, TelegramBot

        with tempfile.TemporaryDirectory() as tmp:
            store = CandidateStore(str(Path(tmp) / "state.db"))
            telegram = FakeTelegram()
            service = FakeBotService(
                search_results=[
                    {
                        "title": "Sintel %03d 1080p" % rank,
                        "download_uri": "magnet:?xt=urn:btih:%03d" % rank,
                        "rank": rank,
                    }
                    for rank in range(1, 101)
                ]
            )
            bot = TelegramBot(BotConfig("token", {700656624}, store.db_path), telegram, store, service)

            bot.handle_update({"message": {"chat": {"id": 9001}, "from": {"id": 700656624}, "text": "sintel"}})
            page_jump_row = telegram.messages[0]["reply_markup"]["inline_keyboard"][-3]
            last_page_button = [button for button in page_jump_row if button["text"] == "20"][0]

            bot.handle_update(
                {
                    "callback_query": {
                        "id": "cb-page-20",
                        "from": {"id": 700656624},
                        "message": {"chat": {"id": 9001}, "message_id": 701},
                        "data": last_page_button["callback_data"],
                    }
                }
            )

            self.assertEqual(service.search_calls, [("sintel", "movie", 100)])
            self.assertEqual(telegram.answers[-1], {"callback_query_id": "cb-page-20", "text": "第 20/20 页"})
            self.assertIn("第 20/20 页，共 100 条", telegram.edits[0]["text"])
            self.assertIn("96. Sintel 096 1080p", telegram.edits[0]["text"])
            self.assertIn("100. Sintel 100 1080p", telegram.edits[0]["text"])
            self.assertNotIn("95. Sintel 095 1080p", telegram.edits[0]["text"])

    def test_callback_adult_search_sends_separate_adult_result_message(self):
        from pipeline.bot import BotConfig, CandidateStore, TelegramBot

        with tempfile.TemporaryDirectory() as tmp:
            store = CandidateStore(str(Path(tmp) / "state.db"))
            telegram = FakeTelegram()
            service = FakeBotService(
                search_results=[{"title": "Sintel", "download_uri": "magnet:?xt=urn:btih:AAA", "rank": 1, "seeders": 10}],
                adult_search_results=[{"title": "Sintel adult", "download_uri": "magnet:?xt=urn:btih:BBB", "rank": 1, "seeders": 0}],
            )
            bot = TelegramBot(BotConfig("token", {700656624}, store.db_path), telegram, store, service)

            bot.handle_update({"message": {"chat": {"id": 9001}, "from": {"id": 700656624}, "text": "sintel"}})
            adult_button = telegram.messages[0]["reply_markup"]["inline_keyboard"][-2][0]

            bot.handle_update(
                {
                    "callback_query": {
                        "id": "cb-adult-search",
                        "from": {"id": 700656624},
                        "message": {"chat": {"id": 9001}, "message_id": 701},
                        "data": adult_button["callback_data"],
                    }
                }
            )

            self.assertEqual(service.search_calls, [("sintel", "movie", 100)])
            self.assertEqual(service.adult_search_calls, [("sintel", 100)])
            self.assertEqual(telegram.answers[-1], {"callback_query_id": "cb-adult-search", "text": "正在补查成人源"})
            self.assertEqual(len(telegram.messages), 2)
            self.assertIn("成人源搜索结果：sintel", telegram.messages[1]["text"])
            self.assertNotEqual(telegram.messages[0]["text"], telegram.messages[1]["text"])
            self.assertNotIn(
                "adult_search",
                json.dumps(telegram.messages[1]["reply_markup"], ensure_ascii=False),
            )
            self.assertEqual(telegram.edits, [])

    def test_callback_anime_search_sends_separate_anime_result_message(self):
        from pipeline.bot import BotConfig, CandidateStore, TelegramBot

        with tempfile.TemporaryDirectory() as tmp:
            store = CandidateStore(str(Path(tmp) / "state.db"))
            telegram = FakeTelegram()
            service = FakeBotService(
                search_results=[{"title": "Frieren", "download_uri": "magnet:?xt=urn:btih:AAA", "rank": 1, "seeders": 10}],
                anime_search_results=[{"title": "Frieren anime", "download_uri": "magnet:?xt=urn:btih:BBB", "rank": 1, "seeders": 3}],
            )
            bot = TelegramBot(BotConfig("token", {700656624}, store.db_path), telegram, store, service)

            bot.handle_update({"message": {"chat": {"id": 9001}, "from": {"id": 700656624}, "text": "frieren"}})
            anime_button = telegram.messages[0]["reply_markup"]["inline_keyboard"][-2][1]

            bot.handle_update(
                {
                    "callback_query": {
                        "id": "cb-anime-search",
                        "from": {"id": 700656624},
                        "message": {"chat": {"id": 9001}, "message_id": 701},
                        "data": anime_button["callback_data"],
                    }
                }
            )

            self.assertEqual(service.search_calls, [("frieren", "movie", 100)])
            self.assertEqual(service.anime_search_calls, [("frieren", 100)])
            self.assertEqual(telegram.answers[-1], {"callback_query_id": "cb-anime-search", "text": "正在补查动漫源"})
            self.assertEqual(len(telegram.messages), 2)
            self.assertIn("动漫源搜索结果：frieren", telegram.messages[1]["text"])
            self.assertNotEqual(telegram.messages[0]["text"], telegram.messages[1]["text"])
            self.assertNotIn("adult_search", json.dumps(telegram.messages[1]["reply_markup"], ensure_ascii=False))
            self.assertNotIn("anime_search", json.dumps(telegram.messages[1]["reply_markup"], ensure_ascii=False))
            self.assertEqual(telegram.edits, [])

    def test_message_strong_adult_code_uses_adult_search_without_retry_button(self):
        from pipeline.bot import BotConfig, CandidateStore, TelegramBot

        with tempfile.TemporaryDirectory() as tmp:
            store = CandidateStore(str(Path(tmp) / "state.db"))
            telegram = FakeTelegram()
            service = FakeBotService(search_results=[{"title": "MIDE-882", "download_uri": "magnet:?xt=urn:btih:AAA", "rank": 1, "seeders": 0}])
            bot = TelegramBot(BotConfig("token", {700656624}, store.db_path), telegram, store, service)

            bot.handle_update({"message": {"chat": {"id": 9001}, "from": {"id": 700656624}, "text": "MIDE-882"}})

            self.assertEqual(service.search_calls, [("MIDE-882", "adult", 100)])
            self.assertIn("成人源搜索结果：MIDE-882", telegram.messages[0]["text"])
            self.assertNotIn("adult_search", json.dumps(telegram.messages[0]["reply_markup"], ensure_ascii=False))

    def test_callback_close_search_deletes_search_message(self):
        from pipeline.bot import BotConfig, CandidateStore, TelegramBot

        with tempfile.TemporaryDirectory() as tmp:
            store = CandidateStore(str(Path(tmp) / "state.db"))
            telegram = FakeTelegram()
            service = FakeBotService(search_results=[{"title": "Sintel", "download_uri": "magnet:?xt=urn:btih:AAA", "rank": 1}])
            bot = TelegramBot(BotConfig("token", {700656624}, store.db_path), telegram, store, service)

            bot.handle_update({"message": {"chat": {"id": 9001}, "from": {"id": 700656624}, "text": "sintel"}})
            close_button = telegram.messages[0]["reply_markup"]["inline_keyboard"][-1][0]

            bot.handle_update(
                {
                    "callback_query": {
                        "id": "cb-close-search",
                        "from": {"id": 700656624},
                        "message": {"chat": {"id": 9001}, "message_id": 701},
                        "data": close_button["callback_data"],
                    }
                }
            )

            self.assertEqual(telegram.answers[-1], {"callback_query_id": "cb-close-search", "text": "已关闭搜索结果"})
            self.assertEqual(telegram.deletes, [{"chat_id": 9001, "message_id": 701}])

    def test_callback_choose_candidate_prompts_library_without_submitting(self):
        from pipeline.bot import BotConfig, CandidateStore, TelegramBot

        with tempfile.TemporaryDirectory() as tmp:
            store = CandidateStore(str(Path(tmp) / "state.db"))
            candidate_id = store.save_candidate(
                user_id=700656624,
                chat_id=9001,
                category="movie",
                query="sintel",
                candidate={"title": "Sintel 1080p", "download_uri": "magnet:?xt=urn:btih:BBB", "rank": 2},
            )
            telegram = FakeTelegram()
            service = FakeBotService(submit_response={"state": True, "tasks": [{"info_hash": "BBB", "state": True, "code": 0}]})
            bot = TelegramBot(BotConfig("token", {700656624}, store.db_path), telegram, store, service)

            bot.handle_update(
                {
                    "callback_query": {
                        "id": "cb1",
                        "from": {"id": 700656624},
                        "message": {"chat": {"id": 9001}, "message_id": 501},
                        "data": "choose:%s" % candidate_id,
                    }
                }
            )

            self.assertEqual(service.search_calls, [])
            self.assertEqual(service.submit_calls, [])
            self.assertEqual(telegram.answers, [{"callback_query_id": "cb1", "text": "请选择入库分类"}])
            self.assertEqual(telegram.edits, [])
            self.assertEqual(len(telegram.messages), 1)
            self.assertEqual(telegram.messages[0]["chat_id"], 9001)
            self.assertIn("已选择：Sintel 1080p", telegram.messages[0]["text"])
            self.assertIn("候选：#2", telegram.messages[0]["text"])
            self.assertIn("链接类型：磁链", telegram.messages[0]["text"])
            self.assertIn("info_hash：BBB", telegram.messages[0]["text"])
            buttons = [button for row in telegram.messages[0]["reply_markup"]["inline_keyboard"] for button in row]
            self.assertEqual([button["text"] for button in buttons], ["电影", "剧集", "动漫", "成人", "其他", "返回结果"])
            self.assertEqual(buttons[0]["callback_data"], "profile:movie:%s" % candidate_id)
            self.assertEqual(buttons[1]["callback_data"], "profile:tv:%s" % candidate_id)
            self.assertEqual(buttons[2]["callback_data"], "profile:anime:%s" % candidate_id)
            self.assertEqual(buttons[3]["callback_data"], "profile:adult:%s" % candidate_id)
            self.assertEqual(buttons[4]["callback_data"], "profile:other:%s" % candidate_id)
            self.assertEqual(buttons[5]["callback_data"], "close_choice:%s" % candidate_id)

    def test_callback_back_from_library_choice_closes_choice_message_without_researching(self):
        from pipeline.bot import BotConfig, CandidateStore, TelegramBot

        with tempfile.TemporaryDirectory() as tmp:
            store = CandidateStore(str(Path(tmp) / "state.db"))
            telegram = FakeTelegram()
            service = FakeBotService(
                search_results=[
                    {"title": "Sintel 720p", "download_uri": "magnet:?xt=urn:btih:AAA", "rank": 1},
                    {"title": "Sintel 1080p", "download_uri": "magnet:?xt=urn:btih:BBB", "rank": 2},
                ]
            )
            bot = TelegramBot(BotConfig("token", {700656624}, store.db_path), telegram, store, service)

            bot.handle_update(
                {
                    "message": {
                        "chat": {"id": 9001},
                        "from": {"id": 700656624},
                        "text": "sintel",
                    }
                }
            )
            choose_button = telegram.messages[0]["reply_markup"]["inline_keyboard"][1][0]
            self.assertEqual(choose_button["text"], "#2 入库")

            bot.handle_update(
                {
                    "callback_query": {
                        "id": "cb-choose",
                        "from": {"id": 700656624},
                        "message": {"chat": {"id": 9001}, "message_id": 501},
                        "data": choose_button["callback_data"],
                    }
                }
            )
            back_button = [
                button
                for row in telegram.messages[1]["reply_markup"]["inline_keyboard"]
                for button in row
                if button["callback_data"].startswith("close_choice:")
            ][0]

            bot.handle_update(
                {
                    "callback_query": {
                        "id": "cb-back",
                        "from": {"id": 700656624},
                        "message": {"chat": {"id": 9001}, "message_id": 1002},
                        "data": back_button["callback_data"],
                    }
                }
            )

            self.assertEqual(service.search_calls, [("sintel", "movie", 100)])
            self.assertEqual(service.submit_calls, [])
            self.assertEqual(telegram.answers[-1], {"callback_query_id": "cb-back", "text": "返回结果"})
            self.assertEqual(telegram.edits, [])
            self.assertEqual(telegram.deletes, [{"chat_id": 9001, "message_id": 1002}])
            self.assertIn("搜索结果：sintel", telegram.messages[0]["text"])
            self.assertIn("第 1/1 页，共 2 条", telegram.messages[0]["text"])
            self.assertIn("2. Sintel 1080p", telegram.messages[0]["text"])
            self.assertEqual(telegram.messages[0]["reply_markup"]["inline_keyboard"][1][0]["text"], "#2 入库")

    def test_callback_profile_submit_saves_manual_content_profile(self):
        from pipeline.bot import BotConfig, CandidateStore, TelegramBot

        with tempfile.TemporaryDirectory() as tmp:
            store = CandidateStore(str(Path(tmp) / "state.db"))
            candidate_id = store.save_candidate(
                user_id=700656624,
                chat_id=9001,
                category="tv",
                query="series",
                candidate={"title": "Series 1080p", "download_uri": "magnet:?xt=urn:btih:SERIES", "rank": 1},
            )
            telegram = FakeTelegram()
            service = FakeBotService(submit_response={"state": True, "tasks": [{"info_hash": "SERIES", "state": True, "code": 0}]})
            bot = TelegramBot(BotConfig("token", {700656624}, store.db_path), telegram, store, service)

            bot.handle_update(
                {
                    "callback_query": {
                        "id": "cb1",
                        "from": {"id": 700656624},
                        "message": {"chat": {"id": 9001}, "message_id": 502},
                        "data": "profile:tv:%s" % candidate_id,
                    }
                }
            )
            record = store.load_task("SERIES")

        self.assertEqual(service.submit_calls, [("tv", "magnet:?xt=urn:btih:SERIES")])
        self.assertEqual(record["category"], "tv")
        self.assertEqual(record["task"]["content_profile"], "tv")
        self.assertIn("入库目录：剧集库", telegram.messages[0]["text"])
        self.assertIn("内容分类：剧集", telegram.messages[0]["text"])

    def test_callback_profile_submit_allows_anime_profile(self):
        from pipeline.bot import BotConfig, CandidateStore, TelegramBot

        with tempfile.TemporaryDirectory() as tmp:
            store = CandidateStore(str(Path(tmp) / "state.db"))
            candidate_id = store.save_candidate(
                user_id=700656624,
                chat_id=9001,
                category="movie",
                query="anime",
                candidate={"title": "Anime 1080p", "download_uri": "magnet:?xt=urn:btih:ANIME", "rank": 1},
            )
            telegram = FakeTelegram()
            service = FakeBotService(submit_response={"state": True, "tasks": [{"info_hash": "ANIME", "state": True, "code": 0}]})
            bot = TelegramBot(BotConfig("token", {700656624}, store.db_path), telegram, store, service)

            bot.handle_update(
                {
                    "callback_query": {
                        "id": "cb-anime",
                        "from": {"id": 700656624},
                        "message": {"chat": {"id": 9001}, "message_id": 502},
                        "data": "profile:anime:%s" % candidate_id,
                    }
                }
            )
            record = store.load_task("ANIME")

        self.assertEqual(service.submit_calls, [("anime", "magnet:?xt=urn:btih:ANIME")])
        self.assertEqual(record["category"], "anime")
        self.assertEqual(record["task"]["content_profile"], "anime")
        self.assertIn("入库目录：动漫库", telegram.messages[0]["text"])
        self.assertIn("内容分类：动漫", telegram.messages[0]["text"])

    def test_callback_submit_uses_selected_library_and_saved_magnet_without_researching(self):
        from pipeline.bot import BotConfig, CandidateStore, TelegramBot

        with tempfile.TemporaryDirectory() as tmp:
            store = CandidateStore(str(Path(tmp) / "state.db"))
            candidate_id = store.save_candidate(
                user_id=700656624,
                chat_id=9001,
                category="movie",
                query="sintel",
                candidate={"title": "Sintel 1080p", "download_uri": "magnet:?xt=urn:btih:BBB", "rank": 2},
            )
            telegram = FakeTelegram()
            service = FakeBotService(submit_response={"state": True, "tasks": [{"info_hash": "BBB", "state": True, "code": 0}]})
            bot = TelegramBot(BotConfig("token", {700656624}, store.db_path), telegram, store, service)

            bot.handle_update(
                {
                    "callback_query": {
                        "id": "cb1",
                        "from": {"id": 700656624},
                        "message": {"chat": {"id": 9001}, "message_id": 502},
                        "data": "submit:adult:%s" % candidate_id,
                    }
                }
            )

            self.assertEqual(service.search_calls, [])
            self.assertEqual(service.submit_calls, [("adult", "magnet:?xt=urn:btih:BBB")])
            self.assertEqual(telegram.chat_actions, [{"chat_id": 9001, "action": "typing"}])
            self.assertEqual(telegram.answers, [{"callback_query_id": "cb1", "text": "已提交 115 离线"}])
            self.assertEqual(telegram.deletes, [{"chat_id": 9001, "message_id": 502}])
            self.assertIn("BBB", telegram.messages[0]["text"])
            self.assertIn("入库目录：成人库", telegram.messages[0]["text"])
            self.assertEqual(store.load_task("BBB")["task"]["status_name"], "submitted")
            self.assertEqual(store.load_task("BBB")["task"]["telegram_status_message_id"], 1001)
            self.assertEqual(store.load_task("BBB")["category"], "adult")

    def test_callback_submit_blocks_same_info_hash_duplicate_without_submitting(self):
        from pipeline.bot import BotConfig, CandidateStore, TelegramBot

        with tempfile.TemporaryDirectory() as tmp:
            store = CandidateStore(str(Path(tmp) / "state.db"))
            store.save_task(
                700656624,
                9001,
                "movie",
                "Existing Sintel",
                {"info_hash": "BBB", "status_name": "success", "msg_sync_status": "success", "msg_scrape_status": "success"},
            )
            candidate_id = store.save_candidate(
                user_id=700656624,
                chat_id=9001,
                category="movie",
                query="sintel",
                candidate={"title": "Sintel 1080p", "download_uri": "magnet:?xt=urn:btih:BBB", "infoHash": "BBB", "rank": 1},
            )
            telegram = FakeTelegram()
            service = FakeBotService(submit_response={"state": True, "tasks": [{"info_hash": "BBB"}]})
            bot = TelegramBot(BotConfig("token", {700656624}, store.db_path), telegram, store, service)

            bot.handle_update(
                {
                    "callback_query": {
                        "id": "cb1",
                        "from": {"id": 700656624},
                        "message": {"chat": {"id": 9001}, "message_id": 502},
                        "data": "submit:movie:%s" % candidate_id,
                    }
                }
            )

            self.assertEqual(service.submit_calls, [])
            self.assertEqual(telegram.answers, [{"callback_query_id": "cb1", "text": "发现重复作品"}])
            self.assertIn("重复入库拦截", telegram.edits[0]["text"])
            self.assertIn("相同info_hash", telegram.edits[0]["text"])
            buttons = [button for row in telegram.edits[0]["reply_markup"]["inline_keyboard"] for button in row]
            self.assertEqual([button["text"] for button in buttons], ["查看已有任务"])

    def test_callback_submit_blocks_adult_code_duplicate_without_submitting(self):
        from pipeline.bot import BotConfig, CandidateStore, TelegramBot

        with tempfile.TemporaryDirectory() as tmp:
            store = CandidateStore(str(Path(tmp) / "state.db"))
            store.save_task(
                700656624,
                9001,
                "adult",
                "SSIS-450 Existing",
                {"info_hash": "OLDHASH", "status_name": "success", "openlist_adult_code": "SSIS-450"},
            )
            candidate_id = store.save_candidate(
                user_id=700656624,
                chat_id=9001,
                category="movie",
                query="SSIS-450",
                candidate={"title": "ssis 450 1080p", "download_uri": "magnet:?xt=urn:btih:NEWHASH", "infoHash": "NEWHASH", "rank": 1},
            )
            telegram = FakeTelegram()
            service = FakeBotService()
            bot = TelegramBot(BotConfig("token", {700656624}, store.db_path), telegram, store, service)

            bot.handle_update(
                {
                    "callback_query": {
                        "id": "cb1",
                        "from": {"id": 700656624},
                        "message": {"chat": {"id": 9001}, "message_id": 502},
                        "data": "submit:adult:%s" % candidate_id,
                    }
                }
            )

            self.assertEqual(service.submit_calls, [])
            self.assertIn("成人番号重复", telegram.edits[0]["text"])
            self.assertIn("SSIS-450", telegram.edits[0]["text"])

    def test_callback_submit_blocks_openlist_adult_code_duplicate_without_submitting(self):
        from pipeline.bot import BotConfig, CandidateStore, TelegramBot

        with tempfile.TemporaryDirectory() as tmp:
            store = CandidateStore(str(Path(tmp) / "state.db"))
            store.replace_dedupe_entries(
                "openlist",
                [
                    {
                        "category": "adult",
                        "identity_type": "adult_code",
                        "identity_value": "SSIS-450",
                        "title": "SSIS-450 Existing",
                        "path": "/115/成人/SSIS-450 Existing",
                    }
                ],
            )
            candidate_id = store.save_candidate(
                user_id=700656624,
                chat_id=9001,
                category="adult",
                query="SSIS-450",
                candidate={"title": "ssis 450 1080p", "download_uri": "magnet:?xt=urn:btih:NEWHASH", "infoHash": "NEWHASH", "rank": 1},
            )
            telegram = FakeTelegram()
            service = FakeBotService()
            bot = TelegramBot(BotConfig("token", {700656624}, store.db_path), telegram, store, service)

            bot.handle_update(
                {
                    "callback_query": {
                        "id": "cb1",
                        "from": {"id": 700656624},
                        "message": {"chat": {"id": 9001}, "message_id": 502},
                        "data": "submit:adult:%s" % candidate_id,
                    }
                }
            )

            self.assertEqual(service.submit_calls, [])
            self.assertEqual(service.duplicate_calls, [])
            self.assertIn("成人番号重复", telegram.edits[0]["text"])
            self.assertIn("OpenList基线", telegram.edits[0]["text"])
            self.assertIn("/115/成人/SSIS-450 Existing", telegram.edits[0]["text"])

    def test_callback_submit_allows_force_submit_after_weak_duplicate_warning(self):
        from pipeline.bot import BotConfig, CandidateStore, TelegramBot

        with tempfile.TemporaryDirectory() as tmp:
            store = CandidateStore(str(Path(tmp) / "state.db"))
            candidate_id = store.save_candidate(
                user_id=700656624,
                chat_id=9001,
                category="movie",
                query="sintel",
                candidate={"title": "Sintel 1080p", "download_uri": "magnet:?xt=urn:btih:BBB", "infoHash": "BBB", "rank": 1},
            )
            telegram = FakeTelegram()
            service = FakeBotService(
                duplicate_response={
                    "level": "weak",
                    "reason": "mediastation_title",
                    "source": "MediaStationGo",
                    "title": "Sintel",
                    "media_id": "media-1",
                },
                submit_response={"state": True, "tasks": [{"info_hash": "BBB", "state": True, "code": 0}]},
            )
            bot = TelegramBot(BotConfig("token", {700656624}, store.db_path, msg_enabled=True), telegram, store, service)

            bot.handle_update(
                {
                    "callback_query": {
                        "id": "cb1",
                        "from": {"id": 700656624},
                        "message": {"chat": {"id": 9001}, "message_id": 502},
                        "data": "submit:movie:%s" % candidate_id,
                    }
                }
            )
            bot.handle_update(
                {
                    "callback_query": {
                        "id": "cb2",
                        "from": {"id": 700656624},
                        "message": {"chat": {"id": 9001}, "message_id": 502},
                        "data": "force_submit:movie:%s" % candidate_id,
                    }
                }
            )

            self.assertIn("可能重复入库", telegram.edits[0]["text"])
            self.assertEqual(telegram.edits[0]["reply_markup"]["inline_keyboard"][-1][0]["text"], "仍然入库")
            self.assertEqual(service.submit_calls, [("movie", "magnet:?xt=urn:btih:BBB")])
            self.assertEqual(telegram.answers[-1], {"callback_query_id": "cb2", "text": "已确认仍然入库"})
            self.assertEqual(store.load_task("BBB")["task"]["status_name"], "submitted")

    def test_callback_submit_warns_openlist_title_duplicate_and_allows_force_submit(self):
        from pipeline.bot import BotConfig, CandidateStore, TelegramBot

        with tempfile.TemporaryDirectory() as tmp:
            store = CandidateStore(str(Path(tmp) / "state.db"))
            store.replace_dedupe_entries(
                "openlist",
                [
                    {
                        "category": "movie",
                        "identity_type": "normalized_title",
                        "identity_value": "sintel",
                        "title": "Sintel",
                        "path": "/115/电影/Sintel",
                    }
                ],
            )
            candidate_id = store.save_candidate(
                user_id=700656624,
                chat_id=9001,
                category="movie",
                query="sintel",
                candidate={"title": "Sintel 1080p", "download_uri": "magnet:?xt=urn:btih:BBB", "infoHash": "BBB", "rank": 1},
            )
            telegram = FakeTelegram()
            service = FakeBotService(submit_response={"state": True, "tasks": [{"info_hash": "BBB", "state": True, "code": 0}]})
            bot = TelegramBot(BotConfig("token", {700656624}, store.db_path), telegram, store, service)

            bot.handle_update(
                {
                    "callback_query": {
                        "id": "cb1",
                        "from": {"id": 700656624},
                        "message": {"chat": {"id": 9001}, "message_id": 502},
                        "data": "submit:movie:%s" % candidate_id,
                    }
                }
            )
            bot.handle_update(
                {
                    "callback_query": {
                        "id": "cb2",
                        "from": {"id": 700656624},
                        "message": {"chat": {"id": 9001}, "message_id": 502},
                        "data": "force_submit:movie:%s" % candidate_id,
                    }
                }
            )

            self.assertIn("可能重复入库", telegram.edits[0]["text"])
            self.assertIn("OpenList基线", telegram.edits[0]["text"])
            self.assertEqual(telegram.edits[0]["reply_markup"]["inline_keyboard"][-1][0]["text"], "仍然入库")
            self.assertEqual(service.submit_calls, [("movie", "magnet:?xt=urn:btih:BBB")])

    def test_dedupe_refresh_command_requires_confirmation(self):
        from pipeline.bot import BotConfig, CandidateStore, TelegramBot

        with tempfile.TemporaryDirectory() as tmp:
            store = CandidateStore(str(Path(tmp) / "state.db"))
            telegram = FakeTelegram()
            service = FakeBotService(dedupe_entries=[{"category": "movie", "source": "openlist", "identity_type": "normalized_title", "identity_value": "sintel"}])
            bot = TelegramBot(BotConfig("token", {700656624}, store.db_path), telegram, store, service)

            bot.handle_update({"message": {"chat": {"id": 9001}, "from": {"id": 700656624}, "text": "/dedupe_refresh"}})

            self.assertEqual(service.dedupe_refresh_calls, [])
            self.assertIn("刷新已入库记录？", telegram.messages[0]["text"])
            self.assertIn("可能增加网盘侧请求量", telegram.messages[0]["text"])
            buttons = telegram.messages[0]["reply_markup"]["inline_keyboard"][0]
            self.assertEqual([button["text"] for button in buttons], ["确认刷新", "取消"])
            self.assertEqual(buttons[0]["callback_data"], "dedupe_refresh_confirm:1")
            self.assertEqual(buttons[1]["callback_data"], "dedupe_refresh_cancel:1")

    def test_dedupe_refresh_confirm_replaces_openlist_index(self):
        from pipeline.bot import BotConfig, CandidateStore, TelegramBot

        with tempfile.TemporaryDirectory() as tmp:
            store = CandidateStore(str(Path(tmp) / "state.db"))
            store.replace_dedupe_entries(
                "openlist",
                [
                    {
                        "category": "adult",
                        "identity_type": "adult_code",
                        "identity_value": "OLD-001",
                        "title": "Old",
                        "path": "/115/成人/Old",
                    }
                ],
            )
            telegram = FakeTelegram()
            service = FakeBotService(
                dedupe_entries=[
                    {
                        "category": "adult",
                        "source": "openlist",
                        "identity_type": "adult_code",
                        "identity_value": "SSIS-450",
                        "title": "SSIS-450 Existing",
                        "path": "/115/成人/SSIS-450 Existing",
                    },
                    {
                        "category": "movie",
                        "source": "openlist",
                        "identity_type": "normalized_title",
                        "identity_value": "sintel",
                        "title": "Sintel",
                        "path": "/115/电影/Sintel",
                    },
                ]
            )
            bot = TelegramBot(BotConfig("token", {700656624}, store.db_path), telegram, store, service)

            bot.handle_update({"message": {"chat": {"id": 9001}, "from": {"id": 700656624}, "text": "/dedupe_refresh"}})
            bot.handle_update(
                {
                    "callback_query": {
                        "id": "cb-dedupe",
                        "from": {"id": 700656624},
                        "message": {"chat": {"id": 9001}, "message_id": 1001},
                        "data": "dedupe_refresh_confirm:1",
                    }
                }
            )

            self.assertEqual(service.dedupe_refresh_calls, [True])
            self.assertEqual(telegram.answers, [{"callback_query_id": "cb-dedupe", "text": "开始刷新已入库记录"}])
            self.assertEqual(telegram.edits[0]["text"], "正在刷新已入库记录，请稍候...")
            self.assertEqual(store.find_dedupe_entries("adult", [{"identity_type": "adult_code", "identity_value": "OLD-001"}]), [])
            self.assertEqual(
                store.find_dedupe_entries("adult", [{"identity_type": "adult_code", "identity_value": "SSIS-450"}])[0]["title"],
                "SSIS-450 Existing",
            )
            self.assertIn("OpenList已入库记录刷新完成", telegram.edits[-1]["text"])
            self.assertIn("写入：2", telegram.edits[-1]["text"])

    def test_dedupe_refresh_cancel_does_not_refresh_index(self):
        from pipeline.bot import BotConfig, CandidateStore, TelegramBot

        with tempfile.TemporaryDirectory() as tmp:
            store = CandidateStore(str(Path(tmp) / "state.db"))
            telegram = FakeTelegram()
            service = FakeBotService(dedupe_entries=[{"category": "movie", "source": "openlist", "identity_type": "normalized_title", "identity_value": "sintel"}])
            bot = TelegramBot(BotConfig("token", {700656624}, store.db_path), telegram, store, service)

            bot.handle_update({"message": {"chat": {"id": 9001}, "from": {"id": 700656624}, "text": "/dedupe_refresh"}})
            bot.handle_update(
                {
                    "callback_query": {
                        "id": "cb-dedupe-cancel",
                        "from": {"id": 700656624},
                        "message": {"chat": {"id": 9001}, "message_id": 1001},
                        "data": "dedupe_refresh_cancel:1",
                    }
                }
            )

            self.assertEqual(service.dedupe_refresh_calls, [])
            self.assertEqual(telegram.answers, [{"callback_query_id": "cb-dedupe-cancel", "text": "已取消刷新"}])
            self.assertEqual(telegram.edits[-1]["text"], "已取消刷新已入库记录。")

    def test_callback_submit_reports_current_task_status(self):
        from pipeline.bot import BotConfig, CandidateStore, TelegramBot

        with tempfile.TemporaryDirectory() as tmp:
            store = CandidateStore(str(Path(tmp) / "state.db"))
            candidate_id = store.save_candidate(
                user_id=700656624,
                chat_id=9001,
                category="movie",
                query="sintel",
                candidate={"title": "Sintel 1080p", "download_uri": "magnet:?xt=urn:btih:BBB", "rank": 2},
            )
            telegram = FakeTelegram()
            service = FakeBotService(
                submit_response={
                    "state": True,
                    "tasks": [{"info_hash": "BBB", "state": True, "code": 0}],
                    "task_status": {"status_name": "downloading", "percent_done": 0, "file_id": "", "wp_path_id": "3464134653584082023"},
                }
            )
            bot = TelegramBot(BotConfig("token", {700656624}, store.db_path), telegram, store, service)

            bot.handle_update(
                {
                    "callback_query": {
                        "id": "cb1",
                        "from": {"id": 700656624},
                        "message": {"chat": {"id": 9001}},
                        "data": "submit:movie:%s" % candidate_id,
                    }
                }
            )

            self.assertIn("当前状态：downloading", telegram.messages[0]["text"])
            self.assertIn("完成进度：0", telegram.messages[0]["text"])
            buttons = telegram.messages[0]["reply_markup"]["inline_keyboard"][0]
            self.assertEqual([button["text"] for button in buttons], ["刷新进度", "取消任务"])

    def test_tasks_command_lists_recent_tasks_with_action_buttons(self):
        from pipeline.bot import BotConfig, CandidateStore, TelegramBot

        with tempfile.TemporaryDirectory() as tmp:
            store = CandidateStore(str(Path(tmp) / "state.db"))
            store.save_task(700656624, 9001, "movie", "Sintel", {"info_hash": "ABC", "status_name": "downloading", "percent_done": 5})
            telegram = FakeTelegram()
            service = FakeBotService()
            bot = TelegramBot(BotConfig("token", {700656624}, store.db_path), telegram, store, service)

            bot.handle_update({"message": {"chat": {"id": 9001}, "from": {"id": 700656624}, "text": "/tasks"}})

            self.assertIn("最近任务：第 1/1 页，共 1 条", telegram.messages[0]["text"])
            self.assertIn("Sintel", telegram.messages[0]["text"])
            self.assertIn("入库：电影库  状态：downloading  进度：5", telegram.messages[0]["text"])
            buttons = telegram.messages[0]["reply_markup"]["inline_keyboard"][0]
            self.assertEqual([button["text"] for button in buttons], ["刷新 1", "取消 1"])

    def test_tasks_command_prioritizes_actionable_tasks_and_paginates(self):
        from pipeline.bot import BotConfig, CandidateStore, TelegramBot

        with tempfile.TemporaryDirectory() as tmp:
            store = CandidateStore(str(Path(tmp) / "state.db"))
            store.save_task(700656624, 9001, "movie", "Done 1", {"info_hash": "DONE1", "status_name": "success", "percent_done": 100})
            store.save_task(700656624, 9001, "movie", "Done 2", {"info_hash": "DONE2", "status_name": "success", "percent_done": 100})
            store.save_task(700656624, 9001, "adult", "Retry", {"info_hash": "RETRY", "status_name": "success", "msg_sync_status": "failed", "msg_scan_status": "success"})
            store.save_task(700656624, 9001, "tv", "Downloading", {"info_hash": "RUN", "status_name": "downloading", "percent_done": 12, "content_profile": "tv"})
            store.save_task(700656624, 9001, "other", "Failed", {"info_hash": "FAILED", "status_name": "failed"})
            store.save_task(700656624, 9001, "movie", "Done 3", {"info_hash": "DONE3", "status_name": "success", "percent_done": 100})
            telegram = FakeTelegram()
            service = FakeBotService()
            bot = TelegramBot(BotConfig("token", {700656624}, store.db_path), telegram, store, service)

            bot.handle_update({"message": {"chat": {"id": 9001}, "from": {"id": 700656624}, "text": "/tasks"}})

            self.assertIn("最近任务：第 1/2 页，共 6 条", telegram.messages[0]["text"])
            self.assertLess(telegram.messages[0]["text"].index("1. Retry"), telegram.messages[0]["text"].index("2. Downloading"))
            self.assertLess(telegram.messages[0]["text"].index("2. Downloading"), telegram.messages[0]["text"].index("3. Failed"))
            self.assertIn("入库：成人库  状态：success  进度：-", telegram.messages[0]["text"])
            self.assertIn("MSG：失败", telegram.messages[0]["text"])
            self.assertIn("入库：剧集库  状态：downloading  进度：12", telegram.messages[0]["text"])
            self.assertIn("内容：剧集", telegram.messages[0]["text"])
            self.assertEqual(telegram.messages[0]["reply_markup"]["inline_keyboard"][0][0]["text"], "重试MSG 1")
            self.assertEqual(telegram.messages[0]["reply_markup"]["inline_keyboard"][1][0]["text"], "刷新 2")
            self.assertEqual(telegram.messages[0]["reply_markup"]["inline_keyboard"][-1][0]["text"], "下一页")

            bot.handle_update(
                {
                    "callback_query": {
                        "id": "cb-tasks-page",
                        "from": {"id": 700656624},
                        "message": {"chat": {"id": 9001}, "message_id": 901},
                        "data": "tasks_page:1",
                    }
                }
            )

            self.assertEqual(telegram.answers[-1], {"callback_query_id": "cb-tasks-page", "text": "第 2/2 页"})
            self.assertIn("最近任务：第 2/2 页，共 6 条", telegram.edits[-1]["text"])
            self.assertIn("6. Done", telegram.edits[-1]["text"])
            self.assertEqual(telegram.edits[-1]["reply_markup"]["inline_keyboard"][-1][0]["text"], "上一页")

    def test_status_callback_refreshes_task_status(self):
        from pipeline.bot import BotConfig, CandidateStore, TelegramBot

        with tempfile.TemporaryDirectory() as tmp:
            store = CandidateStore(str(Path(tmp) / "state.db"))
            store.save_task(700656624, 9001, "movie", "Sintel", {"info_hash": "ABC", "status_name": "downloading", "percent_done": 5})
            telegram = FakeTelegram()
            service = FakeBotService(status_response={"info_hash": "ABC", "status_name": "success", "percent_done": 100, "file_id": "file1"})
            bot = TelegramBot(BotConfig("token", {700656624}, store.db_path), telegram, store, service)

            bot.handle_update(
                {
                    "callback_query": {
                        "id": "cb1",
                        "from": {"id": 700656624},
                        "message": {"chat": {"id": 9001}, "message_id": 101},
                        "data": "status:ABC",
                    }
                }
            )

            self.assertEqual(service.status_calls, [("movie", "ABC")])
            self.assertEqual(telegram.chat_actions, [{"chat_id": 9001, "action": "typing"}])
            self.assertEqual(telegram.answers, [{"callback_query_id": "cb1", "text": "正在刷新进度"}])
            self.assertEqual(telegram.messages, [])
            self.assertEqual(telegram.edits[0]["chat_id"], 9001)
            self.assertEqual(telegram.edits[0]["message_id"], 101)
            self.assertIn("当前状态：success", telegram.edits[0]["text"])
            self.assertEqual(store.load_task("ABC")["task"]["status_name"], "success")

    def test_status_callback_remembers_message_id_for_background_updates(self):
        from pipeline.bot import BotConfig, CandidateStore, TelegramBot

        with tempfile.TemporaryDirectory() as tmp:
            store = CandidateStore(str(Path(tmp) / "state.db"))
            store.save_task(700656624, 9001, "movie", "Sintel", {"info_hash": "ABC", "status_name": "downloading", "percent_done": 5})
            telegram = FakeTelegram()
            service = FakeBotService(status_response={"info_hash": "ABC", "status_name": "downloading", "percent_done": 30})
            bot = TelegramBot(BotConfig("token", {700656624}, store.db_path), telegram, store, service)

            bot.handle_update(
                {
                    "callback_query": {
                        "id": "cb1",
                        "from": {"id": 700656624},
                        "message": {"chat": {"id": 9001}, "message_id": 101},
                        "data": "status:ABC",
                    }
                }
            )
            saved_after_refresh = store.load_task("ABC")["task"]

            telegram.edits.clear()
            service.statuses_response = {"ABC": {"info_hash": "ABC", "status_name": "success", "percent_done": 100, "file_id": "file1"}}
            count = bot.recover_active_115_tasks_once()
            saved_after_recovery = store.load_task("ABC")["task"]

        self.assertEqual(saved_after_refresh["telegram_status_message_id"], 101)
        self.assertEqual(count, 1)
        self.assertEqual(saved_after_recovery["telegram_status_message_id"], 101)
        self.assertEqual(telegram.messages, [])
        self.assertEqual(telegram.edits[-1]["message_id"], 101)
        self.assertIn("当前状态：success", telegram.edits[-1]["text"])

    def test_status_callback_syncs_completed_task_to_mediastation(self):
        from pipeline.bot import BotConfig, CandidateStore, TelegramBot

        with tempfile.TemporaryDirectory() as tmp:
            store = CandidateStore(str(Path(tmp) / "state.db"))
            store.save_task(700656624, 9001, "movie", "Sintel", {"info_hash": "ABC", "status_name": "downloading", "percent_done": 5})
            telegram = FakeTelegram()
            service = FakeBotService(
                status_response={"info_hash": "ABC", "status_name": "success", "percent_done": 100, "file_id": "file1"},
                sync_response={"msg_sync_status": "success", "msg_scrape_status": "success", "msg_media_id": "media-1"},
            )
            bot = TelegramBot(BotConfig("token", {700656624}, store.db_path), telegram, store, service)

            bot.handle_update(
                {
                    "callback_query": {
                        "id": "cb1",
                        "from": {"id": 700656624},
                        "message": {"chat": {"id": 9001}, "message_id": 101},
                        "data": "status:ABC",
                    }
                }
            )

            self.assertEqual(service.sync_calls, [("movie", "Sintel", "ABC")])
            saved = store.load_task("ABC")["task"]
            self.assertEqual(saved["msg_sync_status"], "success")
            self.assertEqual(saved["msg_media_id"], "media-1")
            self.assertIn("MSG同步：已完成", telegram.edits[-1]["text"])
            self.assertIn("MSG媒体ID：media-1", telegram.edits[-1]["text"])

    def test_status_callback_shows_sync_stage_progress(self):
        from pipeline.bot import BotConfig, CandidateStore, TelegramBot

        with tempfile.TemporaryDirectory() as tmp:
            store = CandidateStore(str(Path(tmp) / "state.db"))
            store.save_task(700656624, 9001, "adult", "MIDA-304", {"info_hash": "ABC", "status_name": "downloading", "percent_done": 5})
            telegram = FakeTelegram()
            service = FakeBotService(
                status_response={"info_hash": "ABC", "status_name": "success", "percent_done": 100, "file_id": "file1"},
                sync_progress=[
                    {"msg_sync_status": "running", "openlist_clean_status": "running"},
                    {"msg_sync_status": "running", "openlist_clean_status": "success", "openlist_cleaned_count": 2},
                    {"msg_sync_status": "running", "openlist_adult_format_status": "running"},
                    {"msg_sync_status": "running", "openlist_adult_format_status": "success", "openlist_adult_code": "MIDA-304"},
                    {"msg_sync_status": "running", "msg_scan_status": "running"},
                    {"msg_sync_status": "running", "msg_scan_status": "success", "msg_media_id": "media-1"},
                    {"msg_sync_status": "running", "msg_scrape_status": "running", "msg_media_id": "media-1"},
                ],
                sync_response={"msg_sync_status": "success", "msg_scrape_status": "success", "msg_media_id": "media-1"},
            )
            bot = TelegramBot(BotConfig("token", {700656624}, store.db_path, msg_enabled=True), telegram, store, service)

            bot.handle_update(
                {
                    "callback_query": {
                        "id": "cb1",
                        "from": {"id": 700656624},
                        "message": {"chat": {"id": 9001}, "message_id": 101},
                        "data": "status:ABC",
                    }
                }
            )

            texts = [edit["text"] for edit in telegram.edits]
            self.assertEqual(telegram.answers, [{"callback_query_id": "cb1", "text": "正在刷新进度"}])
            self.assertTrue(any("OpenList清理：进行中" in text for text in texts))
            self.assertTrue(any("OpenList清理：已完成（2 个）" in text for text in texts))
            self.assertTrue(any("番号格式化：进行中" in text for text in texts))
            self.assertTrue(any("番号格式化：已完成（MIDA-304）" in text for text in texts))
            self.assertTrue(any("MSG扫描：进行中" in text for text in texts))
            self.assertTrue(any("MSG刮削：进行中" in text for text in texts))
            self.assertIn("MSG同步：已完成", texts[-1])

    def test_status_callback_keeps_syncing_when_progress_message_update_times_out(self):
        from pipeline.bot import BotConfig, CandidateStore, TelegramBot

        class TimeoutOnSecondEditTelegram(FakeTelegram):
            def send_message(self, chat_id, text, reply_markup=None):
                raise RuntimeError("timed out")

            def edit_message_text(self, chat_id, message_id, text, reply_markup=None):
                if len(self.edits) == 1:
                    raise RuntimeError("timed out")
                super().edit_message_text(chat_id, message_id, text, reply_markup=reply_markup)

        with tempfile.TemporaryDirectory() as tmp:
            store = CandidateStore(str(Path(tmp) / "state.db"))
            store.save_task(700656624, 9001, "adult", "MIDA-304", {"info_hash": "ABC", "status_name": "downloading", "percent_done": 5})
            telegram = TimeoutOnSecondEditTelegram()
            service = FakeBotService(
                status_response={"info_hash": "ABC", "status_name": "success", "percent_done": 100, "file_id": "file1"},
                sync_progress=[{"msg_sync_status": "running", "openlist_clean_status": "running"}],
                sync_response={"msg_sync_status": "success", "msg_scrape_status": "success", "msg_media_id": "media-1"},
            )
            bot = TelegramBot(BotConfig("token", {700656624}, store.db_path, msg_enabled=True), telegram, store, service)

            bot.handle_update(
                {
                    "callback_query": {
                        "id": "cb1",
                        "from": {"id": 700656624},
                        "message": {"chat": {"id": 9001}, "message_id": 101},
                        "data": "status:ABC",
                    }
                }
            )

            self.assertEqual(service.sync_calls, [("adult", "MIDA-304", "ABC")])
            saved = store.load_task("ABC")["task"]
            self.assertEqual(saved["msg_sync_status"], "success")
            self.assertEqual(saved["msg_scrape_status"], "success")
            self.assertEqual(saved["msg_media_id"], "media-1")

    def test_movie_status_message_omits_adult_code_format_fields(self):
        from pipeline.bot import format_task_status_message

        task = {
            "info_hash": "ABC",
            "status_name": "success",
            "openlist_adult_format_status": "success",
            "openlist_adult_code": "MIDA-304",
        }

        movie_text = format_task_status_message("Movie", task, category="movie")
        adult_text = format_task_status_message("Adult", task, category="adult")

        self.assertNotIn("番号格式化", movie_text)
        self.assertIn("番号格式化：已完成（MIDA-304）", adult_text)

    def test_failed_msg_sync_task_shows_retry_button_and_retries_from_callback(self):
        from pipeline.bot import BotConfig, CandidateStore, TelegramBot, task_reply_markup

        task = {
            "info_hash": "ABC",
            "status_name": "success",
            "percent_done": 100,
            "msg_sync_status": "failed",
            "msg_scan_status": "failed",
            "msg_error": "MediaStationGo media not found after root scan: ABC",
        }
        with tempfile.TemporaryDirectory() as tmp:
            store = CandidateStore(str(Path(tmp) / "state.db"))
            store.save_task(700656624, 9001, "movie", "Sintel", task)
            telegram = FakeTelegram()
            service = FakeBotService(sync_response={"msg_sync_status": "success", "msg_scrape_status": "success", "msg_media_id": "media-1"})
            bot = TelegramBot(BotConfig("token", {700656624}, store.db_path, msg_enabled=True), telegram, store, service)

            markup = task_reply_markup(task)
            bot.handle_update(
                {
                    "callback_query": {
                        "id": "cb1",
                        "from": {"id": 700656624},
                        "message": {"chat": {"id": 9001}, "message_id": 104},
                        "data": "retry_msg:ABC",
                    }
                }
            )

            saved = store.load_task("ABC")["task"]

        self.assertEqual(markup["inline_keyboard"][0][0]["text"], "重试MSG同步")
        self.assertEqual(markup["inline_keyboard"][0][0]["callback_data"], "retry_msg:ABC")
        self.assertEqual(telegram.answers, [{"callback_query_id": "cb1", "text": "正在重试MSG同步"}])
        self.assertEqual(service.sync_calls, [("movie", "Sintel", "ABC")])
        self.assertEqual(saved["msg_sync_status"], "success")
        self.assertIn("MSG同步：已完成", telegram.edits[-1]["text"])

    def test_recover_running_msg_sync_tasks_once_updates_store_without_user_refresh(self):
        from pipeline.bot import BotConfig, CandidateStore, TelegramBot

        with tempfile.TemporaryDirectory() as tmp:
            store = CandidateStore(str(Path(tmp) / "state.db"))
            store.save_task(
                700656624,
                9001,
                "movie",
                "Sintel",
                {"info_hash": "ABC", "status_name": "success", "msg_sync_status": "running", "msg_scan_status": "running"},
            )
            telegram = FakeTelegram()
            service = FakeBotService(sync_response={"msg_sync_status": "success", "msg_scrape_status": "success", "msg_media_id": "media-1"})
            bot = TelegramBot(BotConfig("token", {700656624}, store.db_path, msg_enabled=True), telegram, store, service)

            count = bot.recover_running_msg_sync_tasks_once()
            saved = store.load_task("ABC")["task"]

        self.assertEqual(count, 1)
        self.assertEqual(service.sync_calls, [("movie", "Sintel", "ABC")])
        self.assertEqual(saved["msg_sync_status"], "success")
        self.assertEqual(telegram.messages[0]["chat_id"], 9001)
        self.assertIn("MSG同步：已完成", telegram.messages[0]["text"])

    def test_recover_active_115_tasks_once_updates_status_and_syncs_completed_task(self):
        from pipeline.bot import BotConfig, CandidateStore, TelegramBot

        with tempfile.TemporaryDirectory() as tmp:
            store = CandidateStore(str(Path(tmp) / "state.db"))
            store.save_task(
                700656624,
                9001,
                "movie",
                "Sintel",
                {"info_hash": "ABC", "status_name": "downloading", "percent_done": 5},
            )
            telegram = FakeTelegram()
            service = FakeBotService(
                statuses_response={"ABC": {"info_hash": "ABC", "status_name": "success", "percent_done": 100, "file_id": "file1"}},
                sync_response={"msg_sync_status": "success", "msg_scrape_status": "success", "msg_media_id": "media-1"},
            )
            bot = TelegramBot(BotConfig("token", {700656624}, store.db_path, msg_enabled=True), telegram, store, service)

            count = bot.recover_active_115_tasks_once()
            saved = store.load_task("ABC")["task"]

        self.assertEqual(count, 1)
        self.assertEqual(service.statuses_calls, [("movie", ("ABC",))])
        self.assertEqual(service.sync_calls, [("movie", "Sintel", "ABC")])
        self.assertEqual(saved["status_name"], "success")
        self.assertEqual(saved["msg_sync_status"], "success")
        self.assertEqual(saved["msg_media_id"], "media-1")
        self.assertEqual(telegram.messages[0]["chat_id"], 9001)
        self.assertIn("MSG同步：已完成", telegram.messages[0]["text"])

    def test_recover_active_115_tasks_skips_fast_poll_before_two_seconds(self):
        from pipeline.bot import BotConfig, CandidateStore, TelegramBot

        with tempfile.TemporaryDirectory() as tmp:
            store = CandidateStore(str(Path(tmp) / "state.db"))
            with patch("pipeline.bot.time.time", return_value=1000):
                store.save_task(
                    700656624,
                    9001,
                    "movie",
                    "Sintel",
                    {"info_hash": "ABC", "status_name": "downloading", "poll_count": 0, "last_polled_at": 1000},
                )
            telegram = FakeTelegram()
            service = FakeBotService(statuses_response={"ABC": {"info_hash": "ABC", "status_name": "downloading", "percent_done": 30}})
            bot = TelegramBot(BotConfig("token", {700656624}, store.db_path), telegram, store, service)

            count = bot.recover_active_115_tasks_once(now=1001)

        self.assertEqual(count, 0)
        self.assertEqual(service.statuses_calls, [])
        self.assertEqual(telegram.messages, [])

    def test_recover_active_115_tasks_polls_fast_task_after_two_seconds(self):
        from pipeline.bot import BotConfig, CandidateStore, TelegramBot

        with tempfile.TemporaryDirectory() as tmp:
            store = CandidateStore(str(Path(tmp) / "state.db"))
            with patch("pipeline.bot.time.time", return_value=1000):
                store.save_task(
                    700656624,
                    9001,
                    "movie",
                    "Sintel",
                    {"info_hash": "ABC", "status_name": "downloading", "poll_count": 0, "last_polled_at": 1000},
                )
            telegram = FakeTelegram()
            service = FakeBotService(statuses_response={"ABC": {"info_hash": "ABC", "status_name": "downloading", "percent_done": 30}})
            bot = TelegramBot(BotConfig("token", {700656624}, store.db_path), telegram, store, service)

            count = bot.recover_active_115_tasks_once(now=1002)
            saved = store.load_task("ABC")["task"]

        self.assertEqual(count, 1)
        self.assertEqual(service.statuses_calls, [("movie", ("ABC",))])
        self.assertEqual(saved["poll_count"], 0)
        self.assertEqual(saved["last_polled_at"], 1002)
        self.assertEqual(saved["percent_done"], 30)
        self.assertEqual(telegram.messages, [])

    def test_recover_active_115_tasks_edits_saved_status_message_during_fast_poll(self):
        from pipeline.bot import BotConfig, CandidateStore, TelegramBot

        with tempfile.TemporaryDirectory() as tmp:
            store = CandidateStore(str(Path(tmp) / "state.db"))
            with patch("pipeline.bot.time.time", return_value=1000):
                store.save_task(
                    700656624,
                    9001,
                    "movie",
                    "Sintel",
                    {
                        "info_hash": "ABC",
                        "status_name": "downloading",
                        "percent_done": 5,
                        "poll_count": 0,
                        "last_polled_at": 1000,
                        "telegram_status_message_id": 777,
                    },
                )
            telegram = FakeTelegram()
            service = FakeBotService(statuses_response={"ABC": {"info_hash": "ABC", "status_name": "downloading", "percent_done": 30}})
            bot = TelegramBot(BotConfig("token", {700656624}, store.db_path), telegram, store, service)

            count = bot.recover_active_115_tasks_once(now=1002)

        self.assertEqual(count, 1)
        self.assertEqual(telegram.messages, [])
        self.assertEqual(telegram.edits[0]["chat_id"], 9001)
        self.assertEqual(telegram.edits[0]["message_id"], 777)
        self.assertIn("完成进度：30", telegram.edits[0]["text"])
        self.assertEqual(telegram.edits[0]["reply_markup"]["inline_keyboard"][0][0]["text"], "刷新进度")

    def test_recover_active_115_tasks_preserves_message_id_saved_after_record_snapshot(self):
        from pipeline.bot import BotConfig, CandidateStore, TelegramBot

        with tempfile.TemporaryDirectory() as tmp:
            store = CandidateStore(str(Path(tmp) / "state.db"))
            with patch("pipeline.bot.time.time", return_value=1000):
                store.save_task(
                    700656624,
                    9001,
                    "movie",
                    "Sintel",
                    {"info_hash": "ABC", "status_name": "downloading", "percent_done": 5, "poll_count": 0, "last_polled_at": 1000},
                )

            class MessageIdRaceService(FakeBotService):
                def task_statuses(self, category, info_hashes):
                    current = store.load_task("ABC")["task"]
                    current["telegram_status_message_id"] = 888
                    store.save_task(700656624, 9001, "movie", "Sintel", current)
                    return super().task_statuses(category, info_hashes)

            telegram = FakeTelegram()
            service = MessageIdRaceService(
                statuses_response={"ABC": {"info_hash": "ABC", "status_name": "success", "percent_done": 100, "file_id": "file1"}}
            )
            bot = TelegramBot(BotConfig("token", {700656624}, store.db_path), telegram, store, service)

            count = bot.recover_active_115_tasks_once(now=1002)
            saved = store.load_task("ABC")["task"]

        self.assertEqual(count, 1)
        self.assertEqual(saved["telegram_status_message_id"], 888)
        self.assertEqual(telegram.messages, [])
        self.assertEqual(telegram.edits[-1]["message_id"], 888)
        self.assertIn("当前状态：success", telegram.edits[-1]["text"])

    def test_recover_active_115_tasks_edits_saved_status_message_during_msg_sync_progress(self):
        from pipeline.bot import BotConfig, CandidateStore, TelegramBot

        with tempfile.TemporaryDirectory() as tmp:
            store = CandidateStore(str(Path(tmp) / "state.db"))
            with patch("pipeline.bot.time.time", return_value=1000):
                store.save_task(
                    700656624,
                    9001,
                    "movie",
                    "Sintel",
                    {
                        "info_hash": "ABC",
                        "status_name": "downloading",
                        "percent_done": 95,
                        "poll_count": 0,
                        "last_polled_at": 1000,
                        "telegram_status_message_id": 777,
                    },
                )
            telegram = FakeTelegram()
            service = FakeBotService(
                statuses_response={"ABC": {"info_hash": "ABC", "status_name": "success", "percent_done": 100, "file_id": "file1"}},
                sync_progress=[
                    {"msg_sync_status": "running", "openlist_clean_status": "running"},
                    {"msg_sync_status": "running", "openlist_clean_status": "success", "openlist_cleaned_count": 2},
                    {"msg_sync_status": "running", "msg_scan_status": "running"},
                    {"msg_sync_status": "running", "msg_scrape_status": "running", "msg_media_id": "media-1"},
                ],
                sync_response={"msg_sync_status": "success", "msg_scrape_status": "success", "msg_media_id": "media-1"},
            )
            bot = TelegramBot(BotConfig("token", {700656624}, store.db_path, msg_enabled=True), telegram, store, service)

            count = bot.recover_active_115_tasks_once(now=1002)
            texts = [edit["text"] for edit in telegram.edits]

        self.assertEqual(count, 1)
        self.assertEqual(telegram.messages, [])
        self.assertTrue(all(edit["message_id"] == 777 for edit in telegram.edits))
        self.assertTrue(any("OpenList清理：进行中" in text for text in texts))
        self.assertTrue(any("OpenList清理：已完成（2 个）" in text for text in texts))
        self.assertTrue(any("MSG扫描：进行中" in text for text in texts))
        self.assertTrue(any("MSG刮削：进行中" in text for text in texts))
        self.assertIn("MSG同步：已完成", texts[-1])

    def test_recover_active_115_tasks_skips_slow_task_before_six_hundred_seconds(self):
        from pipeline.bot import BotConfig, CandidateStore, TelegramBot

        with tempfile.TemporaryDirectory() as tmp:
            store = CandidateStore(str(Path(tmp) / "state.db"))
            with patch("pipeline.bot.time.time", return_value=1000):
                store.save_task(
                    700656624,
                    9001,
                    "movie",
                    "Sintel",
                    {"info_hash": "ABC", "status_name": "downloading", "poll_count": 10, "last_polled_at": 1100},
                )
            telegram = FakeTelegram()
            service = FakeBotService(statuses_response={"ABC": {"info_hash": "ABC", "status_name": "downloading", "percent_done": 50}})
            bot = TelegramBot(BotConfig("token", {700656624}, store.db_path), telegram, store, service)

            count = bot.recover_active_115_tasks_once(now=1600)

        self.assertEqual(count, 0)
        self.assertEqual(service.statuses_calls, [])

    def test_recover_active_115_tasks_polls_slow_task_after_six_hundred_seconds(self):
        from pipeline.bot import BotConfig, CandidateStore, TelegramBot

        with tempfile.TemporaryDirectory() as tmp:
            store = CandidateStore(str(Path(tmp) / "state.db"))
            with patch("pipeline.bot.time.time", return_value=1000):
                store.save_task(
                    700656624,
                    9001,
                    "movie",
                    "Sintel",
                    {"info_hash": "ABC", "status_name": "downloading", "poll_count": 10, "last_polled_at": 1100},
                )
            telegram = FakeTelegram()
            service = FakeBotService(statuses_response={"ABC": {"info_hash": "ABC", "status_name": "downloading", "percent_done": 50}})
            bot = TelegramBot(BotConfig("token", {700656624}, store.db_path), telegram, store, service)

            count = bot.recover_active_115_tasks_once(now=1700)
            saved = store.load_task("ABC")["task"]

        self.assertEqual(count, 1)
        self.assertEqual(service.statuses_calls, [("movie", ("ABC",))])
        self.assertEqual(saved["poll_count"], 11)
        self.assertEqual(saved["last_polled_at"], 1700)
        self.assertEqual(saved["percent_done"], 50)

    def test_recover_active_115_tasks_auto_cancels_timeout_and_notifies(self):
        from pipeline.bot import BotConfig, CandidateStore, TelegramBot

        with tempfile.TemporaryDirectory() as tmp:
            store = CandidateStore(str(Path(tmp) / "state.db"))
            with patch("pipeline.bot.time.time", return_value=1000):
                store.save_task(
                    700656624,
                    9001,
                    "movie",
                    "Sintel",
                    {"info_hash": "ABC", "status_name": "downloading", "percent_done": 80, "poll_count": 10, "last_polled_at": 1500},
                )
            telegram = FakeTelegram()
            service = FakeBotService(
                cancel_response={
                    "cancelled": True,
                    "task": {"info_hash": "ABC", "status_name": "cancelled", "percent_done": 80},
                    "response": {"state": True},
                    "reason": "",
                }
            )
            bot = TelegramBot(BotConfig("token", {700656624}, store.db_path), telegram, store, service)

            count = bot.recover_active_115_tasks_once(now=8200)
            saved = store.load_task("ABC")["task"]

        self.assertEqual(count, 1)
        self.assertEqual(service.cancel_calls, [("movie", "ABC")])
        self.assertEqual(service.statuses_calls, [])
        self.assertEqual(saved["status_name"], "cancelled")
        self.assertEqual(saved["auto_cancelled_at"], 8200)
        self.assertIn("超过7200秒", saved["auto_cancel_reason"])
        self.assertEqual(telegram.messages[0]["chat_id"], 9001)
        self.assertIn("任务超时已自动取消", telegram.messages[0]["text"])
        self.assertIn("当前状态：cancelled", telegram.messages[0]["text"])

    def test_recover_active_115_tasks_does_not_mark_auto_cancel_when_timeout_task_already_finished(self):
        from pipeline.bot import BotConfig, CandidateStore, TelegramBot

        with tempfile.TemporaryDirectory() as tmp:
            store = CandidateStore(str(Path(tmp) / "state.db"))
            with patch("pipeline.bot.time.time", return_value=1000):
                store.save_task(
                    700656624,
                    9001,
                    "movie",
                    "Sintel",
                    {"info_hash": "ABC", "status_name": "downloading", "percent_done": 80, "poll_count": 10, "last_polled_at": 1500},
                )
            telegram = FakeTelegram()
            service = FakeBotService(
                cancel_response={
                    "cancelled": False,
                    "task": {"info_hash": "ABC", "status_name": "success", "percent_done": 100, "file_id": "file1"},
                    "response": None,
                    "reason": "task is not cancellable: success",
                },
                sync_response={"msg_sync_status": "success", "msg_scrape_status": "success", "msg_media_id": "media-1"},
            )
            bot = TelegramBot(BotConfig("token", {700656624}, store.db_path, msg_enabled=True), telegram, store, service)

            count = bot.recover_active_115_tasks_once(now=8200)
            saved = store.load_task("ABC")["task"]

        self.assertEqual(count, 1)
        self.assertEqual(service.cancel_calls, [("movie", "ABC")])
        self.assertEqual(service.sync_calls, [("movie", "Sintel", "ABC")])
        self.assertEqual(saved["status_name"], "success")
        self.assertEqual(saved["msg_sync_status"], "success")
        self.assertNotIn("auto_cancelled_at", saved)
        self.assertIn("任务未取消", telegram.messages[0]["text"])

    def test_openlist_and_adult_format_failures_show_manual_handling_text(self):
        from pipeline.bot import format_task_status_message, task_reply_markup

        task = {
            "info_hash": "ABC",
            "status_name": "success",
            "msg_sync_status": "failed",
            "openlist_clean_status": "failed",
            "openlist_clean_error": "target not found",
            "openlist_adult_format_status": "failed",
            "openlist_adult_format_error": "rename failed",
            "msg_error": "MediaStationGo media not found after root scan: ABC",
        }

        text = format_task_status_message("MIDA-304", task, category="adult")
        markup = task_reply_markup(task)

        self.assertIn("OpenList处理：请手动进入目标目录检查并删除广告/样片等无效小文件，然后点击重试MSG同步", text)
        self.assertIn("番号处理：请手动将目录重命名为“标准番号 - 原名称”，然后点击重试MSG同步", text)
        self.assertEqual(markup["inline_keyboard"][0][0]["text"], "重试MSG同步")

    def test_cancel_callback_cancels_active_task_through_button(self):
        from pipeline.bot import BotConfig, CandidateStore, TelegramBot

        with tempfile.TemporaryDirectory() as tmp:
            store = CandidateStore(str(Path(tmp) / "state.db"))
            store.save_task(700656624, 9001, "movie", "Sintel", {"info_hash": "ABC", "status_name": "downloading", "percent_done": 5})
            telegram = FakeTelegram()
            service = FakeBotService(
                cancel_response={
                    "cancelled": True,
                    "task": {"info_hash": "ABC", "status_name": "cancelled", "percent_done": 5},
                    "response": {"state": True},
                    "reason": "",
                }
            )
            bot = TelegramBot(BotConfig("token", {700656624}, store.db_path), telegram, store, service)

            bot.handle_update(
                {
                    "callback_query": {
                        "id": "cb1",
                        "from": {"id": 700656624},
                        "message": {"chat": {"id": 9001}, "message_id": 102},
                        "data": "cancel:ABC",
                    }
                }
            )

            self.assertEqual(service.cancel_calls, [("movie", "ABC")])
            self.assertEqual(telegram.answers, [{"callback_query_id": "cb1", "text": "已取消任务"}])
            self.assertEqual(telegram.messages, [])
            self.assertEqual(telegram.edits[0]["chat_id"], 9001)
            self.assertEqual(telegram.edits[0]["message_id"], 102)
            self.assertIn("已取消任务", telegram.edits[0]["text"])
            self.assertEqual(telegram.edits[0]["reply_markup"], {"inline_keyboard": []})
            self.assertEqual(store.load_task("ABC")["task"]["status_name"], "cancelled")

    def test_status_callback_for_cancelled_task_does_not_query_115(self):
        from pipeline.bot import BotConfig, CandidateStore, TelegramBot

        with tempfile.TemporaryDirectory() as tmp:
            store = CandidateStore(str(Path(tmp) / "state.db"))
            store.save_task(700656624, 9001, "movie", "Sintel", {"info_hash": "ABC", "status_name": "cancelled", "percent_done": 5})
            telegram = FakeTelegram()
            service = FakeBotService()
            bot = TelegramBot(BotConfig("token", {700656624}, store.db_path), telegram, store, service)

            bot.handle_update(
                {
                    "callback_query": {
                        "id": "cb1",
                        "from": {"id": 700656624},
                        "message": {"chat": {"id": 9001}, "message_id": 103},
                        "data": "status:ABC",
                    }
                }
            )

            self.assertEqual(service.status_calls, [])
            self.assertEqual(telegram.answers, [{"callback_query_id": "cb1", "text": "任务已结束"}])
            self.assertIn("当前状态：cancelled", telegram.edits[0]["text"])
            self.assertEqual(telegram.edits[0]["reply_markup"], {"inline_keyboard": []})

    def test_tasks_command_omits_buttons_for_finished_tasks(self):
        from pipeline.bot import BotConfig, CandidateStore, TelegramBot

        with tempfile.TemporaryDirectory() as tmp:
            store = CandidateStore(str(Path(tmp) / "state.db"))
            store.save_task(700656624, 9001, "movie", "Cancelled", {"info_hash": "ABC", "status_name": "cancelled"})
            store.save_task(700656624, 9001, "movie", "Success", {"info_hash": "DEF", "status_name": "success", "percent_done": 100})
            telegram = FakeTelegram()
            service = FakeBotService()
            bot = TelegramBot(BotConfig("token", {700656624}, store.db_path), telegram, store, service)

            bot.handle_update({"message": {"chat": {"id": 9001}, "from": {"id": 700656624}, "text": "/tasks"}})

            self.assertIn("Cancelled", telegram.messages[0]["text"])
            self.assertIn("Success", telegram.messages[0]["text"])
            self.assertIsNone(telegram.messages[0]["reply_markup"])

    def test_unauthorized_user_is_rejected_before_search(self):
        from pipeline.bot import BotConfig, CandidateStore, TelegramBot

        with tempfile.TemporaryDirectory() as tmp:
            store = CandidateStore(str(Path(tmp) / "state.db"))
            telegram = FakeTelegram()
            service = FakeBotService(search_results=[{"title": "Sintel", "download_uri": "magnet:?xt=urn:btih:AAA", "rank": 1}])
            bot = TelegramBot(BotConfig("token", {700656624}, store.db_path), telegram, store, service)

            bot.handle_update({"message": {"chat": {"id": 9001}, "from": {"id": 1}, "text": "sintel"}})

            self.assertEqual(service.search_calls, [])
            self.assertEqual(telegram.messages[0]["text"], "未授权用户")

    def test_search_error_replies_to_user_without_silent_failure(self):
        from pipeline.bot import BotConfig, CandidateStore, TelegramBot

        with tempfile.TemporaryDirectory() as tmp:
            store = CandidateStore(str(Path(tmp) / "state.db"))
            telegram = FakeTelegram()
            service = FakeBotService(search_error=RuntimeError("no acceptable resource"))
            bot = TelegramBot(BotConfig("token", {700656624}, store.db_path), telegram, store, service)

            bot.handle_update({"message": {"chat": {"id": 9001}, "from": {"id": 700656624}, "text": "missing movie"}})

            self.assertIn("未找到可用资源：missing movie", telegram.messages[0]["text"])
            self.assertEqual(
                [(button["text"], button["callback_data"].split(":", 1)[0]) for button in telegram.messages[0]["reply_markup"]["inline_keyboard"][-2]],
                [("🔞", "adult_search"), ("动漫", "anime_search")],
            )

    def test_empty_search_result_keeps_retry_buttons(self):
        from pipeline.bot import BotConfig, CandidateStore, TelegramBot

        with tempfile.TemporaryDirectory() as tmp:
            store = CandidateStore(str(Path(tmp) / "state.db"))
            telegram = FakeTelegram()
            service = FakeBotService(
                search_results=[],
                adult_search_results=[{"title": "Missing adult", "download_uri": "magnet:?xt=urn:btih:BBB", "rank": 1, "seeders": 0}],
            )
            bot = TelegramBot(BotConfig("token", {700656624}, store.db_path), telegram, store, service)

            bot.handle_update({"message": {"chat": {"id": 9001}, "from": {"id": 700656624}, "text": "missing movie"}})
            retry_row = telegram.messages[0]["reply_markup"]["inline_keyboard"][-2]

            self.assertIn("未找到可用资源：missing movie", telegram.messages[0]["text"])
            self.assertEqual([(button["text"], button["callback_data"].split(":", 1)[0]) for button in retry_row], [("🔞", "adult_search"), ("动漫", "anime_search")])

            bot.handle_update(
                {
                    "callback_query": {
                        "id": "cb-empty-adult-search",
                        "from": {"id": 700656624},
                        "message": {"chat": {"id": 9001}, "message_id": 701},
                        "data": retry_row[0]["callback_data"],
                    }
                }
            )

            self.assertEqual(service.adult_search_calls, [("missing movie", 100)])
            self.assertEqual(len(telegram.messages), 2)
            self.assertIn("成人源搜索结果：missing movie", telegram.messages[1]["text"])

    def test_search_timeout_replies_to_user_without_silent_failure(self):
        from pipeline.bot import BotConfig, CandidateStore, TelegramBot

        with tempfile.TemporaryDirectory() as tmp:
            store = CandidateStore(str(Path(tmp) / "state.db"))
            telegram = FakeTelegram()
            service = FakeBotService(search_error=TimeoutError("timed out"))
            bot = TelegramBot(BotConfig("token", {700656624}, store.db_path), telegram, store, service)

            bot.handle_update({"message": {"chat": {"id": 9001}, "from": {"id": 700656624}, "text": "流浪地球1"}})

            self.assertEqual(service.search_calls, [("流浪地球1", "movie", 100)])
            self.assertEqual(telegram.messages[0]["chat_id"], 9001)
            self.assertIn("搜索失败", telegram.messages[0]["text"])
            self.assertIn("timed out", telegram.messages[0]["text"])

    def test_poll_updates_once_keeps_running_when_polling_fails(self):
        from pipeline.bot import BotConfig, CandidateStore, TelegramBot

        class PollingErrorTelegram(FakeTelegram):
            def get_updates(self, offset=None, timeout=30):
                raise RuntimeError("HTTP Error 409: Conflict")

        with tempfile.TemporaryDirectory() as tmp, patch("pipeline.bot.time.sleep") as sleep:
            store = CandidateStore(str(Path(tmp) / "state.db"))
            telegram = PollingErrorTelegram()
            service = FakeBotService()
            bot = TelegramBot(BotConfig("token", {700656624}, store.db_path), telegram, store, service)

            offset = bot.poll_updates_once(None)

        self.assertIsNone(offset)
        sleep.assert_called_once_with(5)

    def test_poll_updates_once_replies_when_update_handling_fails(self):
        from pipeline.bot import BotConfig, CandidateStore, TelegramBot

        class OneUpdateTelegram(FakeTelegram):
            def get_updates(self, offset=None, timeout=30):
                return {
                    "result": [
                        {
                            "update_id": 100,
                            "callback_query": {
                                "id": "cb1",
                                "from": {"id": 700656624},
                                "message": {"chat": {"id": 9001}, "message_id": 501},
                                "data": "submit:movie:999999",
                            },
                        }
                    ]
                }

        with tempfile.TemporaryDirectory() as tmp:
            store = CandidateStore(str(Path(tmp) / "state.db"))
            telegram = OneUpdateTelegram()
            service = FakeBotService()
            bot = TelegramBot(BotConfig("token", {700656624}, store.db_path), telegram, store, service)

            offset = bot.poll_updates_once(None)

        self.assertEqual(offset, 101)
        self.assertEqual(telegram.answers[0], {"callback_query_id": "cb1", "text": "处理失败"})
        self.assertEqual(telegram.messages[0]["chat_id"], 9001)
        self.assertIn("处理失败", telegram.messages[0]["text"])


class PipelineBotServiceTest(unittest.TestCase):
    def test_submit_resolves_prowlarr_download_uri_before_115_offline(self):
        from pipeline.bot import BotConfig, PipelineBotService

        class SubmitService(PipelineBotService):
            def _call_115(self, category, callback):
                self.fake_115 = Fake115SubmitClient({"state": True, "data": [{}]})
                return callback(self.fake_115)

        class FakeProwlarrConfig:
            def __init__(self, config_path):
                self.config_path = config_path

            def load_api_key(self):
                return "prowlarr-key-value"

        class FakeResolvingProwlarr:
            calls = []

            def __init__(self, base_url, api_key):
                self.base_url = base_url
                self.api_key = api_key

            def resolve_download_uri(self, download_uri):
                self.calls.append((self.base_url, self.api_key, download_uri))
                return "magnet:?xt=urn:btih:ABC"

        with patch("pipeline.bot.ProwlarrConfig", FakeProwlarrConfig), patch("pipeline.bot.ProwlarrClient", FakeResolvingProwlarr):
            service = SubmitService(
                BotConfig(
                    "token",
                    {700656624},
                    "/tmp/state.db",
                    prowlarr_url="http://127.0.0.1:9696",
                    prowlarr_config="/prowlarr-config/config.xml",
                )
            )
            service.submit("movie", "prowlarr-download://9?link=ABC%2BDEF")

        self.assertEqual(service.fake_115.urls, ["magnet:?xt=urn:btih:ABC"])
        self.assertEqual(
            FakeResolvingProwlarr.calls,
            [("http://127.0.0.1:9696", "prowlarr-key-value", "prowlarr-download://9?link=ABC%2BDEF")],
        )

    def test_task_status_refreshes_openlist_and_retries_when_115_token_is_invalid(self):
        from pipeline.bot import BotConfig, PipelineBotService

        events = []
        token_store = RetryTokenStore()

        with patch("pipeline.bot.OpenListTokenProvider", return_value=FakeOpenListTokenProvider()), patch(
            "pipeline.bot.OpenListClient", side_effect=lambda url, token: RetryOpenList(events)
        ), patch("pipeline.bot.OpenListTokenStore", return_value=token_store), patch(
            "pipeline.bot.Client115", side_effect=lambda token: Retry115Client(token, events)
        ):
            service = PipelineBotService(BotConfig("token", {700656624}, "/tmp/state.db"))
            task = service.task_status("movie", "ABC")

        self.assertEqual(task["status_name"], "success")
        self.assertIn(("openlist", "/115/电影", False), events)
        self.assertIn(("openlist", "/115/电影", True), events)
        self.assertEqual(token_store.tokens, ["expired-token", "fresh-token"])

    def test_sync_completed_task_scans_root_finds_media_and_scrapes_one_item(self):
        from pipeline.bot import BotConfig, PipelineBotService

        events = []
        fake_msg = FakeMediaStationClient(
            search_response={"data": {"items": [{"id": "media-1", "library_id": "d150a96c-b467-4c60-82f1-207ae5949045", "title": "Sintel"}]}}
        )

        with patch("pipeline.bot.MediaStationClient", return_value=fake_msg), patch(
            "pipeline.bot.OpenListTokenProvider", return_value=FakeOpenListTokenProvider()
        ), patch("pipeline.bot.OpenListClient", side_effect=lambda url, token: RetryOpenList(events)):
            service = PipelineBotService(
                BotConfig(
                    "token",
                    {700656624},
                    "/tmp/state.db",
                    msg_admin_user="admin",
                    msg_admin_password="secret",
                    msg_enabled=True,
                    msg_sync_poll_seconds=0,
                    openlist_pre_scan_clean_enabled=False,
                )
            )
            task = service.sync_completed_task(
                "movie",
                "Sintel",
                {"info_hash": "ABC", "status_name": "success", "name": "Sintel.mkv", "msg_error": "old error"},
            )

        self.assertEqual(events, [("openlist", "/115/电影", True)])
        self.assertEqual(fake_msg.scan_calls, [("d150a96c-b467-4c60-82f1-207ae5949045", "0c1dda42-29ef-4069-b051-c9549a8d4440")])
        self.assertEqual(fake_msg.scrape_calls, ["media-1"])
        self.assertEqual(fake_msg.artwork_repair_calls, [])
        self.assertEqual(task["msg_sync_status"], "success")
        self.assertEqual(task["msg_scrape_status"], "success")
        self.assertEqual(task["msg_media_id"], "media-1")
        self.assertIsNone(task["msg_error"])

    def test_sync_completed_task_cleans_openlist_before_mediastation_scan(self):
        from pipeline.bot import BotConfig, PipelineBotService

        events = []
        fake_openlist = CleaningOpenList(
            {
                "/115/电影": [{"name": "Movie", "is_dir": True, "size": 0}],
                "/115/电影/Movie": [
                    {"name": "Movie.mkv", "is_dir": False, "size": 800 * 1024 * 1024},
                    {"name": "trailer.mp4", "is_dir": False, "size": 20 * 1024 * 1024},
                    {"name": "poster.jpg", "is_dir": False, "size": 300 * 1024},
                    {"name": "Movie.srt", "is_dir": False, "size": 20 * 1024},
                    {"name": "Extras", "is_dir": True, "size": 0},
                ],
                "/115/电影/Movie/Extras": [
                    {"name": "sample.mp4", "is_dir": False, "size": 10 * 1024 * 1024},
                ],
            },
            events=events,
        )
        fake_msg = FakeMediaStationClient(
            search_response={"data": {"items": [{"id": "media-1", "library_id": "d150a96c-b467-4c60-82f1-207ae5949045", "title": "Movie"}]}},
            events=events,
        )

        with patch("pipeline.bot.MediaStationClient", return_value=fake_msg), patch(
            "pipeline.bot.OpenListTokenProvider", return_value=FakeOpenListTokenProvider()
        ), patch("pipeline.bot.OpenListClient", return_value=fake_openlist):
            service = PipelineBotService(
                BotConfig(
                    "token",
                    {700656624},
                    "/tmp/state.db",
                    msg_admin_user="admin",
                    msg_admin_password="secret",
                    msg_enabled=True,
                    msg_sync_poll_seconds=0,
                    openlist_pre_scan_clean_enabled=True,
                    openlist_pre_scan_clean_max_bytes=100 * 1024 * 1024,
                )
            )
            task = service.sync_completed_task(
                "movie",
                "Movie",
                {"info_hash": "ABC", "status_name": "success", "name": "Movie"},
            )

        self.assertEqual(
            fake_openlist.remove_calls,
            [
                ("/115/电影/Movie", ["trailer.mp4", "poster.jpg"]),
                ("/115/电影/Movie/Extras", ["sample.mp4"]),
            ],
        )
        self.assertLess(events.index(("remove", "/115/电影/Movie", ("trailer.mp4", "poster.jpg"))), events.index(("scan",)))
        self.assertEqual(task["openlist_clean_status"], "success")
        self.assertEqual(task["openlist_cleaned_count"], 3)
        self.assertEqual(task["openlist_cleaned_bytes"], 30 * 1024 * 1024 + 300 * 1024)
        self.assertEqual(task["msg_sync_status"], "success")

    def test_sync_completed_task_keeps_scanning_when_openlist_clean_target_is_missing(self):
        from pipeline.bot import BotConfig, PipelineBotService

        fake_openlist = CleaningOpenList({"/115/电影": [{"name": "Other", "is_dir": True, "size": 0}]})
        fake_msg = FakeMediaStationClient(
            search_response={"data": {"items": [{"id": "media-1", "library_id": "d150a96c-b467-4c60-82f1-207ae5949045", "title": "Movie"}]}}
        )

        with patch("pipeline.bot.MediaStationClient", return_value=fake_msg), patch(
            "pipeline.bot.OpenListTokenProvider", return_value=FakeOpenListTokenProvider()
        ), patch("pipeline.bot.OpenListClient", return_value=fake_openlist):
            service = PipelineBotService(
                BotConfig(
                    "token",
                    {700656624},
                    "/tmp/state.db",
                    msg_admin_user="admin",
                    msg_admin_password="secret",
                    msg_enabled=True,
                    msg_sync_poll_seconds=0,
                    openlist_pre_scan_clean_enabled=True,
                )
            )
            task = service.sync_completed_task(
                "movie",
                "Movie",
                {"info_hash": "ABC", "status_name": "success", "name": "Movie"},
            )

        self.assertEqual(fake_openlist.remove_calls, [])
        self.assertEqual(fake_msg.scan_calls, [("d150a96c-b467-4c60-82f1-207ae5949045", "0c1dda42-29ef-4069-b051-c9549a8d4440")])
        self.assertEqual(task["openlist_clean_status"], "failed")
        self.assertIn("target not found", task["openlist_clean_error"])
        self.assertEqual(task["msg_sync_status"], "success")

    def test_sync_completed_task_keeps_scanning_when_openlist_remove_times_out(self):
        from pipeline.bot import BotConfig, PipelineBotService

        class TimeoutCleaningOpenList(CleaningOpenList):
            def remove_names(self, dir_path, names):
                raise RuntimeError("OpenList request failed: timed out")

        fake_openlist = TimeoutCleaningOpenList(
            {
                "/115/电影": [{"name": "Movie", "is_dir": True, "size": 0}],
                "/115/电影/Movie": [{"name": "ad.mp4", "is_dir": False, "size": 1 * 1024 * 1024}],
            }
        )
        fake_msg = FakeMediaStationClient(
            search_response={"data": {"items": [{"id": "media-1", "library_id": "d150a96c-b467-4c60-82f1-207ae5949045", "title": "Movie"}]}}
        )

        with patch("pipeline.bot.MediaStationClient", return_value=fake_msg), patch(
            "pipeline.bot.OpenListTokenProvider", return_value=FakeOpenListTokenProvider()
        ), patch("pipeline.bot.OpenListClient", return_value=fake_openlist):
            service = PipelineBotService(
                BotConfig(
                    "token",
                    {700656624},
                    "/tmp/state.db",
                    msg_admin_user="admin",
                    msg_admin_password="secret",
                    msg_enabled=True,
                    msg_sync_poll_seconds=0,
                    openlist_pre_scan_clean_enabled=True,
                )
            )
            task = service.sync_completed_task(
                "movie",
                "Movie",
                {"info_hash": "ABC", "status_name": "success", "name": "Movie"},
            )

        self.assertEqual(fake_msg.scan_calls, [("d150a96c-b467-4c60-82f1-207ae5949045", "0c1dda42-29ef-4069-b051-c9549a8d4440")])
        self.assertEqual(task["openlist_clean_status"], "failed")
        self.assertIn("timed out", task["openlist_clean_error"])
        self.assertEqual(task["msg_sync_status"], "success")

    def test_clean_openlist_target_prefers_raw_exact_name_when_normalized_names_tie(self):
        from pipeline.bot import clean_openlist_task_media

        fake_openlist = CleaningOpenList(
            {
                "/root": [
                    {"name": "Movie A", "is_dir": True, "size": 0},
                    {"name": "Movie-A", "is_dir": True, "size": 0},
                ],
                "/root/Movie A": [{"name": "ad.mp4", "is_dir": False, "size": 3 * 1024 * 1024}],
                "/root/Movie-A": [{"name": "other-ad.mp4", "is_dir": False, "size": 3 * 1024 * 1024}],
            }
        )

        result = clean_openlist_task_media(fake_openlist, "/root", ["Movie A"], task={"size": 20 * 1024 * 1024})

        self.assertEqual(result["openlist_clean_target"], "/root/Movie A")
        self.assertEqual(fake_openlist.remove_calls, [("/root/Movie A", ["ad.mp4"])])

    def test_clean_openlist_default_threshold_keeps_episode_videos_and_deletes_small_non_episode_videos(self):
        from pipeline.bot import clean_openlist_task_media

        fake_openlist = CleaningOpenList(
            {
                "/root": [{"name": "Jackie Chan Adventures", "is_dir": True, "size": 0}],
                "/root/Jackie Chan Adventures": [
                    {"name": "成龙历险记 第42集.mp4", "is_dir": False, "size": 10 * 1024 * 1024},
                    {"name": "S01E01.mkv", "is_dir": False, "size": 10 * 1024 * 1024},
                    {"name": "EP01.mp4", "is_dir": False, "size": 10 * 1024 * 1024},
                    {"name": "01.mp4", "is_dir": False, "size": 10 * 1024 * 1024},
                    {"name": "001.mkv", "is_dir": False, "size": 10 * 1024 * 1024},
                    {"name": "成龙历险记 第42集.srt", "is_dir": False, "size": 20 * 1024},
                    {"name": "ad.mp4", "is_dir": False, "size": 50 * 1024 * 1024},
                    {"name": "bonus.mp4", "is_dir": False, "size": 10 * 1024 * 1024},
                    {"name": "poster.jpg", "is_dir": False, "size": 300 * 1024},
                ],
            }
        )

        result = clean_openlist_task_media(
            fake_openlist,
            "/root",
            ["Jackie Chan Adventures"],
            task={"size": 10 * 1024 * 1024},
        )

        self.assertEqual(fake_openlist.remove_calls, [("/root/Jackie Chan Adventures", ["ad.mp4", "bonus.mp4", "poster.jpg"])])
        self.assertEqual(result["openlist_cleaned_count"], 3)

    def test_clean_openlist_target_uses_task_size_to_break_remaining_ties(self):
        from pipeline.bot import clean_openlist_task_media

        fake_openlist = CleaningOpenList(
            {
                "/root": [
                    {"name": "Movie-A", "is_dir": True, "size": 0},
                    {"name": "Movie A", "is_dir": True, "size": 0},
                ],
                "/root/Movie-A": [{"name": "small.mp4", "is_dir": False, "size": 20 * 1024 * 1024}],
                "/root/Movie A": [
                    {"name": "main.mp4", "is_dir": False, "size": 100 * 1024 * 1024},
                    {"name": "ad.mp4", "is_dir": False, "size": 1 * 1024 * 1024},
                ],
            }
        )

        result = clean_openlist_task_media(
            fake_openlist,
            "/root",
            ["MovieA"],
            task={"size": 101 * 1024 * 1024},
            max_bytes=100 * 1024 * 1024,
        )

        self.assertEqual(result["openlist_clean_target"], "/root/Movie A")
        self.assertEqual(fake_openlist.remove_calls, [("/root/Movie A", ["ad.mp4"])])

    def test_sync_completed_adult_task_formats_code_before_mediastation_scan(self):
        from pipeline.bot import BotConfig, PipelineBotService

        events = []
        fake_openlist = CleaningOpenList(
            {
                "/115/成人": [{"name": "downloaded folder", "is_dir": True, "size": 0}],
                "/115/成人/downloaded folder": [
                    {"name": "mida.304.mp4", "is_dir": False, "size": 800 * 1024 * 1024},
                    {"name": "ad.mp4", "is_dir": False, "size": 3 * 1024 * 1024},
                    {"name": "mida.304.srt", "is_dir": False, "size": 20 * 1024},
                ],
            },
            events=events,
        )
        fake_msg = FakeMediaStationClient(
            search_response={"data": {"items": [{"id": "media-1", "library_id": "26768071-73bb-4b5c-85f3-ad0dd84f9fd9", "title": "MIDA-304"}]}},
            events=events,
            artwork_repair_response={"status": "success", "updated": 1, "fields": ["poster_url"]},
        )

        with patch("pipeline.bot.MediaStationClient", return_value=fake_msg), patch(
            "pipeline.bot.OpenListTokenProvider", return_value=FakeOpenListTokenProvider()
        ), patch("pipeline.bot.OpenListClient", return_value=fake_openlist):
            service = PipelineBotService(
                BotConfig(
                    "token",
                    {700656624},
                    "/tmp/state.db",
                    msg_admin_user="admin",
                    msg_admin_password="secret",
                    msg_enabled=True,
                    msg_sync_poll_seconds=0,
                    openlist_pre_scan_clean_enabled=True,
                    openlist_adult_code_format_enabled=True,
                )
            )
            task = service.sync_completed_task(
                "adult",
                "downloaded folder",
                {"info_hash": "ABC", "status_name": "success", "name": "downloaded folder"},
            )

        self.assertEqual(fake_openlist.remove_calls, [("/115/成人/downloaded folder", ["ad.mp4"])])
        self.assertEqual(fake_openlist.rename_calls, [("/115/成人/downloaded folder", "MIDA-304 - downloaded folder")])
        self.assertLess(events.index(("remove", "/115/成人/downloaded folder", ("ad.mp4",))), events.index(("rename", "/115/成人/downloaded folder", "MIDA-304 - downloaded folder")))
        self.assertLess(events.index(("rename", "/115/成人/downloaded folder", "MIDA-304 - downloaded folder")), events.index(("scan",)))
        self.assertLess(events.index(("scan",)), events.index(("artwork_repair",)))
        self.assertEqual(fake_msg.search_calls[0], ("MIDA-304", 20))
        self.assertEqual(fake_msg.artwork_repair_calls, ["media-1"])
        self.assertEqual(task["openlist_adult_format_status"], "success")
        self.assertEqual(task["openlist_adult_code"], "MIDA-304")
        self.assertEqual(task["msg_artwork_repair_status"], "success")
        self.assertEqual(task["msg_artwork_repair_updated"], 1)
        self.assertEqual(task["msg_artwork_repair_fields"], "poster_url")
        self.assertEqual(task["msg_sync_status"], "success")

    def test_first_adult_code_recognizes_standard_and_fc2_codes_only(self):
        from pipeline.bot import first_adult_code

        cases = [
            ("ssis 450", "SSIS-450"),
            ("ipx_789", "IPX-789"),
            ("FSDSS.567", "FSDSS-567"),
            ("fc2 ppv 12345678", "FC2-PPV-12345678"),
            ("fc2ppv1234567", "FC2-PPV-1234567"),
            ("xxx-123ch", "XXX-123"),
        ]
        for value, expected in cases:
            with self.subTest(value=value):
                self.assertEqual(first_adult_code([value]), expected)

        self.assertIsNone(
            first_adult_code(
                [
                    "Tokyo Hot n0680",
                    "carib-020913-001",
                    "HEYDOUGA-1234-56",
                    "10musume 061234_01",
                    "10mu-022525_01",
                ]
            )
        )

    def test_sync_completed_adult_task_clears_format_running_when_format_is_skipped(self):
        from pipeline.bot import BotConfig, PipelineBotService

        progress = []
        fake_openlist = CleaningOpenList(
            {
                "/115/成人": [{"name": "downloaded folder", "is_dir": True, "size": 0}],
                "/115/成人/downloaded folder": [{"name": "main.mp4", "is_dir": False, "size": 800 * 1024 * 1024}],
            }
        )
        fake_msg = FakeMediaStationClient(
            search_response={"data": {"items": [{"id": "media-1", "library_id": "26768071-73bb-4b5c-85f3-ad0dd84f9fd9", "title": "downloaded folder"}]}}
        )

        with patch("pipeline.bot.MediaStationClient", return_value=fake_msg), patch(
            "pipeline.bot.OpenListTokenProvider", return_value=FakeOpenListTokenProvider()
        ), patch("pipeline.bot.OpenListClient", return_value=fake_openlist):
            service = PipelineBotService(
                BotConfig(
                    "token",
                    {700656624},
                    "/tmp/state.db",
                    msg_admin_user="admin",
                    msg_admin_password="secret",
                    msg_enabled=True,
                    msg_sync_poll_seconds=0,
                    openlist_pre_scan_clean_enabled=True,
                    openlist_adult_code_format_enabled=True,
                )
            )
            task = service.sync_completed_task(
                "adult",
                "downloaded folder",
                {"info_hash": "ABC", "status_name": "success", "name": "downloaded folder"},
                progress_callback=lambda item: progress.append(item),
            )

        scan_running = [item for item in progress if item.get("msg_scan_status") == "running"]
        self.assertTrue(scan_running)
        self.assertNotEqual(scan_running[-1].get("openlist_adult_format_status"), "running")
        self.assertEqual(task["openlist_adult_format_status"], "skipped")
        self.assertEqual(task["openlist_adult_format_reason"], "code_not_found")
        self.assertEqual(task["msg_sync_status"], "success")

    def test_sync_completed_movie_task_does_not_format_adult_code(self):
        from pipeline.bot import BotConfig, PipelineBotService

        fake_openlist = CleaningOpenList({"/115/电影": [{"name": "Movie", "is_dir": True, "size": 0}]})
        fake_msg = FakeMediaStationClient(
            search_response={"data": {"items": [{"id": "media-1", "library_id": "d150a96c-b467-4c60-82f1-207ae5949045", "title": "Movie"}]}}
        )

        with patch("pipeline.bot.MediaStationClient", return_value=fake_msg), patch(
            "pipeline.bot.OpenListTokenProvider", return_value=FakeOpenListTokenProvider()
        ), patch("pipeline.bot.OpenListClient", return_value=fake_openlist):
            service = PipelineBotService(
                BotConfig(
                    "token",
                    {700656624},
                    "/tmp/state.db",
                    msg_admin_user="admin",
                    msg_admin_password="secret",
                    msg_enabled=True,
                    msg_sync_poll_seconds=0,
                    openlist_adult_code_format_enabled=True,
                )
            )
            task = service.sync_completed_task(
                "movie",
                "Movie",
                {"info_hash": "ABC", "status_name": "success", "name": "Movie"},
            )

        self.assertEqual(fake_openlist.rename_calls, [])
        self.assertNotIn("openlist_adult_format_status", task)
        self.assertEqual(task["msg_sync_status"], "success")

    def test_sync_completed_other_task_uses_other_root_without_adult_code_format(self):
        from pipeline.bot import BotConfig, PipelineBotService

        fake_openlist = CleaningOpenList({"/115/其他": [{"name": "Unmatched", "is_dir": True, "size": 0}]})
        fake_msg = FakeMediaStationClient(
            search_response={"data": {"items": [{"id": "media-1", "library_id": "60067bc7-eb34-466c-8bf9-5654297a609f", "title": "Unmatched"}]}},
            artwork_repair_response={"status": "skipped", "updated": 0, "reason": "not_needed"},
        )

        with patch("pipeline.bot.MediaStationClient", return_value=fake_msg), patch(
            "pipeline.bot.OpenListTokenProvider", return_value=FakeOpenListTokenProvider()
        ), patch("pipeline.bot.OpenListClient", return_value=fake_openlist):
            service = PipelineBotService(
                BotConfig(
                    "token",
                    {700656624},
                    "/tmp/state.db",
                    msg_admin_user="admin",
                    msg_admin_password="secret",
                    msg_enabled=True,
                    msg_sync_poll_seconds=0,
                    openlist_pre_scan_clean_enabled=True,
                    openlist_adult_code_format_enabled=True,
                )
            )
            task = service.sync_completed_task(
                "other",
                "Unmatched",
                {"info_hash": "ABC", "status_name": "success", "name": "Unmatched"},
            )

        self.assertEqual(fake_msg.scan_calls, [("60067bc7-eb34-466c-8bf9-5654297a609f", "1f889ec1-b34d-40b6-b3ca-f4372170a42b")])
        self.assertEqual(fake_msg.artwork_repair_calls, [])
        self.assertEqual(fake_openlist.rename_calls, [])
        self.assertNotIn("openlist_adult_format_status", task)
        self.assertEqual(task["msg_library_id"], "60067bc7-eb34-466c-8bf9-5654297a609f")
        self.assertEqual(task["msg_root_id"], "1f889ec1-b34d-40b6-b3ca-f4372170a42b")
        self.assertEqual(task["msg_sync_status"], "success")

    def test_sync_completed_task_skips_when_already_synced(self):
        from pipeline.bot import BotConfig, PipelineBotService

        fake_msg = FakeMediaStationClient()

        with patch("pipeline.bot.MediaStationClient", return_value=fake_msg):
            service = PipelineBotService(
                BotConfig(
                    "token",
                    {700656624},
                    "/tmp/state.db",
                    msg_admin_user="admin",
                    msg_admin_password="secret",
                    msg_enabled=True,
                )
            )
            task = service.sync_completed_task(
                "movie",
                "Sintel",
                {
                    "info_hash": "ABC",
                    "status_name": "success",
                    "msg_sync_status": "success",
                    "msg_scrape_status": "success",
                    "msg_media_id": "media-1",
                },
            )

        self.assertEqual(fake_msg.scan_calls, [])
        self.assertEqual(fake_msg.scrape_calls, [])
        self.assertEqual(task["msg_media_id"], "media-1")

    def test_sync_completed_task_retries_from_failed_scrape_step(self):
        from pipeline.bot import BotConfig, PipelineBotService

        fake_msg = FakeMediaStationClient()

        with patch("pipeline.bot.MediaStationClient", return_value=fake_msg), patch("pipeline.bot.OpenListClient") as openlist_cls:
            service = PipelineBotService(
                BotConfig(
                    "token",
                    {700656624},
                    "/tmp/state.db",
                    msg_admin_user="admin",
                    msg_admin_password="secret",
                    msg_enabled=True,
                    openlist_pre_scan_clean_enabled=True,
                )
            )
            task = service.sync_completed_task(
                "movie",
                "Sintel",
                {
                    "info_hash": "ABC",
                    "status_name": "success",
                    "msg_sync_status": "failed",
                    "msg_scan_status": "success",
                    "msg_media_id": "media-1",
                    "msg_media_title": "Sintel",
                    "msg_scrape_status": "failed",
                    "msg_error": "scrape failed",
                    "openlist_clean_status": "success",
                    "openlist_cleaned_count": 0,
                },
            )

        self.assertFalse(openlist_cls.called)
        self.assertEqual(fake_msg.scan_calls, [])
        self.assertEqual(fake_msg.search_calls, [])
        self.assertEqual(fake_msg.scrape_calls, ["media-1"])
        self.assertEqual(task["msg_sync_status"], "success")
        self.assertEqual(task["msg_scrape_status"], "success")
        self.assertIsNone(task["msg_error"])

    def test_sync_completed_adult_task_retries_from_failed_artwork_repair_step(self):
        from pipeline.bot import BotConfig, PipelineBotService

        fake_msg = FakeMediaStationClient(artwork_repair_response={"status": "success", "updated": 2, "fields": ["poster_url", "backdrop_url"]})

        with patch("pipeline.bot.MediaStationClient", return_value=fake_msg), patch("pipeline.bot.OpenListClient") as openlist_cls:
            service = PipelineBotService(
                BotConfig(
                    "token",
                    {700656624},
                    "/tmp/state.db",
                    msg_admin_user="admin",
                    msg_admin_password="secret",
                    msg_enabled=True,
                    openlist_pre_scan_clean_enabled=True,
                )
            )
            task = service.sync_completed_task(
                "adult",
                "SSIS-450",
                {
                    "info_hash": "ABC",
                    "status_name": "success",
                    "msg_sync_status": "failed",
                    "msg_scan_status": "success",
                    "msg_media_id": "media-1",
                    "msg_media_title": "SSIS-450",
                    "msg_scrape_status": "success",
                    "msg_artwork_repair_status": "failed",
                    "msg_artwork_repair_error": "metadata patch failed",
                    "openlist_clean_status": "success",
                    "openlist_cleaned_count": 0,
                    "openlist_adult_format_status": "success",
                    "openlist_adult_code": "SSIS-450",
                },
            )

        self.assertFalse(openlist_cls.called)
        self.assertEqual(fake_msg.scan_calls, [])
        self.assertEqual(fake_msg.scrape_calls, [])
        self.assertEqual(fake_msg.artwork_repair_calls, ["media-1"])
        self.assertEqual(task["msg_sync_status"], "success")
        self.assertEqual(task["msg_artwork_repair_status"], "success")
        self.assertEqual(task["msg_artwork_repair_updated"], 2)

    def test_check_duplicate_marks_adult_code_match_as_strong(self):
        from pipeline.bot import BotConfig, PipelineBotService

        fake_msg = FakeMediaStationClient(
            search_response={
                "data": {
                    "items": [
                        {
                            "id": "media-1",
                            "library_id": "26768071-73bb-4b5c-85f3-ad0dd84f9fd9",
                            "title": "SSIS-450",
                        }
                    ]
                }
            }
        )

        with patch("pipeline.bot.MediaStationClient", return_value=fake_msg):
            service = PipelineBotService(
                BotConfig(
                    "token",
                    {700656624},
                    "/tmp/state.db",
                    msg_admin_user="admin",
                    msg_admin_password="secret",
                    msg_enabled=True,
                )
            )
            duplicate = service.check_duplicate("adult", "SSIS-450", {"title": "ssis 450 1080p"})

        self.assertEqual(duplicate["level"], "strong")
        self.assertEqual(duplicate["reason"], "mediastation_code")
        self.assertEqual(duplicate["media_id"], "media-1")

    def test_check_duplicate_marks_movie_title_match_as_weak(self):
        from pipeline.bot import BotConfig, PipelineBotService

        fake_msg = FakeMediaStationClient(
            search_response={
                "data": {
                    "items": [
                        {
                            "id": "media-1",
                            "library_id": "d150a96c-b467-4c60-82f1-207ae5949045",
                            "title": "Sintel",
                        }
                    ]
                }
            }
        )

        with patch("pipeline.bot.MediaStationClient", return_value=fake_msg):
            service = PipelineBotService(
                BotConfig(
                    "token",
                    {700656624},
                    "/tmp/state.db",
                    msg_admin_user="admin",
                    msg_admin_password="secret",
                    msg_enabled=True,
                )
            )
            duplicate = service.check_duplicate("movie", "Sintel", {"title": "Sintel 1080p"})

        self.assertEqual(duplicate["level"], "weak")
        self.assertEqual(duplicate["reason"], "mediastation_title")
        self.assertEqual(duplicate["media_id"], "media-1")

    def test_check_duplicate_does_not_use_generic_release_fragments(self):
        from pipeline.bot import BotConfig, PipelineBotService

        fake_msg = FakeMediaStationClient(
            search_response={"data": {"items": []}},
            list_response={
                "data": {
                    "items": [
                        {
                            "id": "media-adult",
                            "library_id": "26768071-73bb-4b5c-85f3-ad0dd84f9fd9",
                            "title": "無防備すぎる幼馴染のノーブラぽろりに胸キュン勃起！ びんびんビーチクに我慢できず乳首こねくりラブ 石川澪",
                        }
                    ]
                }
            },
        )

        with patch("pipeline.bot.MediaStationClient", return_value=fake_msg):
            service = PipelineBotService(
                BotConfig(
                    "token",
                    {700656624},
                    "/tmp/state.db",
                    msg_admin_user="admin",
                    msg_admin_password="secret",
                    msg_enabled=True,
                )
            )
            duplicate = service.check_duplicate(
                "adult",
                "秋色之空",
                {"title": "[Seed-Raws] 秋色之空 Aki Sora - 全3話+特典 (乳) (BD 720p AVC AAC).mp4 [一般向]"},
            )

        self.assertIsNone(duplicate)
        self.assertEqual(fake_msg.search_calls, [])
        self.assertEqual(fake_msg.list_calls, [])

    def test_check_duplicate_does_not_fallback_to_recent_library_page_for_non_adult_titles(self):
        from pipeline.bot import BotConfig, PipelineBotService

        fake_msg = FakeMediaStationClient(
            search_response={"data": {"items": []}},
            list_response={
                "data": {
                    "items": [
                        {
                            "id": "media-1",
                            "library_id": "e1333358-17ff-4b90-82f0-663cec26c0df",
                            "title": "成龙历险记",
                        }
                    ]
                }
            },
        )

        with patch("pipeline.bot.MediaStationClient", return_value=fake_msg):
            service = PipelineBotService(
                BotConfig(
                    "token",
                    {700656624},
                    "/tmp/state.db",
                    msg_admin_user="admin",
                    msg_admin_password="secret",
                    msg_enabled=True,
                )
            )
            duplicate = service.check_duplicate("anime", "秋色之空", {"title": "秋色之空 Aki Sora"})

        self.assertIsNone(duplicate)
        self.assertEqual(fake_msg.search_calls, [("秋色之空", 20)])
        self.assertEqual(fake_msg.list_calls, [])

    def test_collect_openlist_dedupe_entries_refreshes_each_library_once(self):
        from pipeline.bot import BotConfig, PipelineBotService

        events = []
        fake_openlist = CleaningOpenList(
            {
                "/115/电影": [{"name": "Sintel", "is_dir": True, "size": 0}],
                "/115/电影/Sintel": [{"name": "Sintel 1080p.mkv", "is_dir": False, "size": 1024}],
                "/115/剧集": [{"name": "Series", "is_dir": True, "size": 0}],
                "/115/剧集/Series": [{"name": "S01E01.mkv", "is_dir": False, "size": 1024}],
                "/115/成人": [{"name": "SSIS-450 Existing", "is_dir": True, "size": 0}],
                "/115/成人/SSIS-450 Existing": [{"name": "ssis 450 1080p.mp4", "is_dir": False, "size": 1024}],
                "/115/其他": [{"name": "Unmatched Pack", "is_dir": True, "size": 0}],
                "/115/其他/Unmatched Pack": [{"name": "main.mp4", "is_dir": False, "size": 1024}],
            },
            events=events,
        )

        with patch("pipeline.bot.OpenListTokenProvider", return_value=FakeOpenListTokenProvider()), patch(
            "pipeline.bot.OpenListClient", return_value=fake_openlist
        ):
            service = PipelineBotService(BotConfig("token", {700656624}, "/tmp/state.db"))
            entries = service.collect_openlist_dedupe_entries(refresh=True)

        keys = {(entry["category"], entry["identity_type"], entry["identity_value"]) for entry in entries}
        self.assertIn(("movie", "normalized_title", "sintel"), keys)
        self.assertIn(("tv", "normalized_title", "series"), keys)
        self.assertIn(("adult", "adult_code", "SSIS-450"), keys)
        self.assertIn(("adult", "normalized_title", "ssis450existing"), keys)
        self.assertIn(("other", "normalized_title", "unmatchedpack"), keys)
        self.assertEqual(events.count(("openlist", "/115/电影", True)), 1)
        self.assertEqual(events.count(("openlist", "/115/剧集", True)), 1)
        self.assertEqual(events.count(("openlist", "/115/成人", True)), 1)
        self.assertEqual(events.count(("openlist", "/115/其他", True)), 1)

    def test_collect_openlist_dedupe_entries_indexes_work_units_without_path_rows(self):
        from pipeline.bot import BotConfig, PipelineBotService

        fake_openlist = CleaningOpenList(
            {
                "/115/电影": [
                    {"name": "Sintel", "is_dir": True, "size": 0},
                    {"name": "Root Movie.mkv", "is_dir": False, "size": 900 * 1024 * 1024},
                    {"name": "poster.jpg", "is_dir": False, "size": 300 * 1024},
                ],
                "/115/电影/Sintel": [
                    {"name": "Sintel 1080p.mkv", "is_dir": False, "size": 900 * 1024 * 1024},
                    {"name": "sample.mp4", "is_dir": False, "size": 20 * 1024 * 1024},
                    {"name": "poster.jpg", "is_dir": False, "size": 300 * 1024},
                ],
                "/115/成人": [{"name": "Downloaded Pack", "is_dir": True, "size": 0}],
                "/115/成人/Downloaded Pack": [
                    {"name": "SSIS-450 1080p.mp4", "is_dir": False, "size": 900 * 1024 * 1024},
                    {"name": "cover.jpg", "is_dir": False, "size": 300 * 1024},
                ],
                "/115/剧集": [],
                "/115/其他": [{"name": "Unmatched Pack", "is_dir": True, "size": 0}],
                "/115/其他/Unmatched Pack": [{"name": "main.mp4", "is_dir": False, "size": 900 * 1024 * 1024}],
            }
        )

        with patch("pipeline.bot.OpenListTokenProvider", return_value=FakeOpenListTokenProvider()), patch(
            "pipeline.bot.OpenListClient", return_value=fake_openlist
        ):
            service = PipelineBotService(BotConfig("token", {700656624}, "/tmp/state.db"))
            entries = service.collect_openlist_dedupe_entries(refresh=True)

        keys = {(entry["category"], entry["identity_type"], entry["identity_value"]) for entry in entries}
        identity_types = {entry["identity_type"] for entry in entries}
        self.assertNotIn("openlist_path", identity_types)
        self.assertIn(("movie", "normalized_title", "sintel"), keys)
        self.assertIn(("movie", "normalized_title", "rootmovie"), keys)
        self.assertIn(("adult", "normalized_title", "downloadedpack"), keys)
        self.assertIn(("adult", "adult_code", "SSIS-450"), keys)
        self.assertIn(("other", "normalized_title", "unmatchedpack"), keys)
        self.assertNotIn(("movie", "normalized_title", "sintel1080p"), keys)
        self.assertNotIn(("movie", "normalized_title", "sample"), keys)
        self.assertNotIn(("movie", "normalized_title", "poster"), keys)
        self.assertNotIn(("adult", "normalized_title", "ssis4501080p"), keys)

    def test_migrate_media_candidate_moves_openlist_then_updates_msg_db(self):
        from pipeline.bot import BotConfig, PipelineBotService

        events = []

        class FakeMigrationDb:
            def search_migration_candidates(self, query, limit=20):
                raise AssertionError("not used")

            def validate_migration_target_available(self, candidate, target_category):
                events.append(("db_validate", candidate["source_openlist_path"], target_category))

            def validate_migration_source_ready(self, candidate):
                events.append(("db_preflight", candidate["source_openlist_path"]))

            def migrate_media_group(self, candidate, target_category):
                events.append(("db_migrate", candidate["source_openlist_path"], target_category))
                return {
                    "source_openlist_path": candidate["source_openlist_path"],
                    "target_openlist_path": "/115/动漫/成龙历险记",
                    "target_category": target_category,
                    "media_count": 95,
                    "series_count": 1,
                }

        fake_openlist = CleaningOpenList(
            {
                "/115/剧集": [{"name": "成龙历险记", "is_dir": True, "size": 0}],
                "/115/动漫": [],
            },
            events=events,
        )

        with patch("pipeline.bot.MediaStationDbClient", return_value=FakeMigrationDb()), patch(
            "pipeline.bot.OpenListTokenProvider", return_value=FakeOpenListTokenProvider()
        ), patch("pipeline.bot.OpenListClient", return_value=fake_openlist):
            service = PipelineBotService(BotConfig("token", {700656624}, "/tmp/state.db"))
            result = service.migrate_media_candidate(
                {
                    "title": "成龙历险记",
                    "category": "tv",
                    "source_openlist_path": "/115/剧集/成龙历险记",
                    "source_kind": "folder",
                    "media_count": 95,
                },
                "anime",
            )

        self.assertEqual(events[1], ("db_preflight", result["source_openlist_path"]))
        self.assertEqual(
            events[:1] + events[2:],
            [
                ("db_validate", "/115/剧集/成龙历险记", "anime"),
                ("list_all", "/115/剧集", False),
                ("list_all", "/115/动漫", False),
                ("move", "/115/剧集", "/115/动漫", ("成龙历险记",)),
                ("openlist", "/115/剧集", True),
                ("openlist", "/115/动漫", True),
                ("db_migrate", "/115/剧集/成龙历险记", "anime"),
            ],
        )
        self.assertEqual(result["target_openlist_path"], "/115/动漫/成龙历险记")
        self.assertTrue(result["openlist_moved"])

    def test_sync_completed_task_marks_failed_scan_stage(self):
        from pipeline.bot import BotConfig, PipelineBotService

        fake_msg = FakeMediaStationClient(search_response={"data": {"items": []}}, list_response={"data": {"items": []}})

        with patch("pipeline.bot.MediaStationClient", return_value=fake_msg), patch(
            "pipeline.bot.OpenListTokenProvider", return_value=FakeOpenListTokenProvider()
        ), patch("pipeline.bot.OpenListClient", return_value=CleaningOpenList({"/115/电影": []})):
            service = PipelineBotService(
                BotConfig(
                    "token",
                    {700656624},
                    "/tmp/state.db",
                    msg_admin_user="admin",
                    msg_admin_password="secret",
                    msg_enabled=True,
                    msg_sync_poll_seconds=0,
                    openlist_pre_scan_clean_enabled=False,
                )
            )
            task = service.sync_completed_task(
                "movie",
                "Missing",
                {"info_hash": "ABC", "status_name": "success", "name": "Missing"},
            )

        self.assertEqual(task["msg_sync_status"], "failed")
        self.assertEqual(task["msg_scan_status"], "failed")
        self.assertIn("MediaStationGo media not found", task["msg_error"])

    def test_media_search_queries_uses_title_fragment_without_codec_noise(self):
        from pipeline.bot import media_search_queries

        queries = media_search_queries(
            "[DBD-Raws][4K_HDR][流浪地球2][IMAX版][正片+花絮][2160P][UHDBDRip][HEVC-10bit][简体内封][FLAC][MKV]",
            {"info_hash": "ABC", "status_name": "success", "file_id": "3464429394146100471"},
        )

        self.assertIn("流浪地球2", queries)
        self.assertIn("3464429394146100471", queries)
        self.assertNotIn("HEVC-10", queries)


    def test_media_search_queries_ignores_single_character_chinese_fragment(self):
        from pipeline.bot import media_search_queries

        queries = media_search_queries(
            "[Seed-Raws] \u79cb\u8272\u4e4b\u7a7a Aki Sora - \u51683\u8a71+\u7279\u5178 (\u4e73) (BD 720p AVC AAC).mp4 [\u4e00\u822c\u5411]",
            {
                "file_name": "[Seed-Raws] \u79cb\u8272\u4e4b\u7a7a Aki Sora - \u51683\u8a71+\u7279\u5178 (\u4e73) (BD 720p AVC AAC).mp4 [\u4e00\u822c\u5411]"
            },
        )

        self.assertIn("\u79cb\u8272\u4e4b\u7a7a", " ".join(queries))
        self.assertNotIn("\u4e73", queries)


class CategoryConfigTest(unittest.TestCase):
    def test_msgdb_groups_episode_rows_into_one_migration_candidate(self):
        from pipeline.msgdb import build_migration_candidates, build_migration_target, cloud_path_to_openlist_path

        rows = [
            {
                "id": "m1",
                "library_id": "b6c58f40-76dc-46b5-8f27-9e74d22e5e3d",
                "library_root_id": "3d2e0cb4-3537-4f7d-8d79-9d4d5f1800df",
                "title": "成龙历险记",
                "path": "cloud://openlist/115/剧集/成龙历险记/成龙历险记 第01集.mp4",
                "root_path": "cloud://openlist/115%2F%E5%89%A7%E9%9B%86",
                "size_bytes": 100,
                "library_name": "剧集",
                "library_type": "tv",
            },
            {
                "id": "m2",
                "library_id": "b6c58f40-76dc-46b5-8f27-9e74d22e5e3d",
                "library_root_id": "3d2e0cb4-3537-4f7d-8d79-9d4d5f1800df",
                "title": "成龙历险记",
                "path": "cloud://openlist/115/剧集/成龙历险记/成龙历险记 第02集.mp4",
                "root_path": "cloud://openlist/115%2F%E5%89%A7%E9%9B%86",
                "size_bytes": 200,
                "library_name": "剧集",
                "library_type": "tv",
            },
        ]

        candidates = build_migration_candidates(rows, limit=20)
        target = build_migration_target(candidates[0], "anime")

        self.assertEqual(cloud_path_to_openlist_path("cloud://openlist/115%2F%E5%89%A7%E9%9B%86"), "/115/剧集")
        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0]["source_openlist_path"], "/115/剧集/成龙历险记")
        self.assertEqual(candidates[0]["source_kind"], "folder")
        self.assertEqual(candidates[0]["category"], "tv")
        self.assertEqual(candidates[0]["media_count"], 2)
        self.assertEqual(candidates[0]["total_size"], 300)
        self.assertEqual(target["target_openlist_path"], "/115/动漫/成龙历险记")

    def test_msgdb_rewrites_cloud_play_strm_url_when_migrating_paths(self):
        from pipeline.msgdb import replace_strm_url_prefix

        old_path = "/115/\u5267\u96c6/\u6210\u9f99\u5386\u9669\u8bb0"
        new_path = "/115/\u52a8\u6f2b/\u6210\u9f99\u5386\u9669\u8bb0"
        url = (
            "/api/cloud/play/openlist?ref="
            "%2F115%2F%E5%89%A7%E9%9B%86%2F%E6%88%90%E9%BE%99%E5%8E%86%E9%99%A9%E8%AE%B0"
            "%2F%E6%88%90%E9%BE%99%E5%8E%86%E9%99%A9%E8%AE%B0+%E7%AC%AC01%E9%9B%86.mp4"
        )

        rewritten = replace_strm_url_prefix(url, old_path, new_path)

        self.assertIn("%2F115%2F%E5%8A%A8%E6%BC%AB%2F%E6%88%90%E9%BE%99%E5%8E%86%E9%99%A9%E8%AE%B0", rewritten)
        self.assertNotIn("%2F115%2F%E5%89%A7%E9%9B%86%2F%E6%88%90%E9%BE%99%E5%8E%86%E9%99%A9%E8%AE%B0", rewritten)

    def test_routes_movie_tv_anime_adult_and_other_to_separate_115_folders(self):
        self.assertEqual(category_to_folder_id("movie"), "3464134653584082023")
        self.assertEqual(category_to_folder_id("tv"), "3465137076394001831")
        self.assertEqual(category_to_folder_id("anime"), "3465784028030830531")
        self.assertEqual(category_to_folder_id("adult"), "3464134590896014943")
        self.assertEqual(category_to_folder_id("other"), "3465205291639899794")

    def test_routes_movie_tv_anime_adult_and_other_to_openlist_paths(self):
        self.assertEqual(category_to_openlist_path("movie"), "/115/电影")
        self.assertEqual(category_to_openlist_path("tv"), "/115/剧集")
        self.assertEqual(category_to_openlist_path("anime"), "/115/动漫")
        self.assertEqual(category_to_openlist_path("adult"), "/115/成人")
        self.assertEqual(category_to_openlist_path("other"), "/115/其他")

    def test_routes_movie_tv_anime_adult_and_other_to_mediastation_roots(self):
        movie = category_to_msg_library_root("movie")
        tv = category_to_msg_library_root("tv")
        anime = category_to_msg_library_root("anime")
        adult = category_to_msg_library_root("adult")
        other = category_to_msg_library_root("other")

        self.assertEqual(movie["library_id"], "d150a96c-b467-4c60-82f1-207ae5949045")
        self.assertEqual(movie["root_id"], "0c1dda42-29ef-4069-b051-c9549a8d4440")
        self.assertEqual(tv["library_id"], "b6c58f40-76dc-46b5-8f27-9e74d22e5e3d")
        self.assertEqual(tv["root_id"], "3d2e0cb4-3537-4f7d-8d79-9d4d5f1800df")
        self.assertEqual(tv["media_type"], "tv")
        self.assertEqual(anime["library_id"], "e1333358-17ff-4b90-82f0-663cec26c0df")
        self.assertEqual(anime["root_id"], "fc7058d6-0b32-4536-bb92-4755c488be55")
        self.assertEqual(anime["provider"], "tmdb")
        self.assertEqual(anime["media_type"], "anime")
        self.assertEqual(adult["library_id"], "26768071-73bb-4b5c-85f3-ad0dd84f9fd9")
        self.assertEqual(adult["root_id"], "3fe479e8-4a96-4e61-9f69-fa802e448446")
        self.assertEqual(other["library_id"], "60067bc7-eb34-466c-8bf9-5654297a609f")
        self.assertEqual(other["root_id"], "1f889ec1-b34d-40b6-b3ca-f4372170a42b")
        self.assertEqual(other["provider"], "tmdb")
        self.assertEqual(other["media_type"], "movie")

    def test_rejects_unknown_category_without_fallback(self):
        with self.assertRaisesRegex(ValueError, "unsupported category"):
            category_to_folder_id("unknown")


class OpenListTokenStoreTest(unittest.TestCase):
    def test_reads_access_token_from_enabled_115_open_storage(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "data.db"
            conn = sqlite3.connect(db_path)
            conn.execute(
                "create table x_storages (id integer, mount_path text, driver text, disabled integer, addition text)"
            )
            conn.execute(
                "insert into x_storages values (?, ?, ?, ?, ?)",
                (
                    1,
                    "/115_audit_movie",
                    "115 Open",
                    0,
                    json.dumps(
                        {
                            "access_token": "access-token-value",
                            "refresh_token": "refresh-token-value",
                            "root_folder_id": "3462843402402399378",
                        }
                    ),
                ),
            )
            conn.commit()
            conn.close()

            token = OpenListTokenStore(db_path).load_access_token()

        self.assertEqual(token.storage_id, 1)
        self.assertEqual(token.mount_path, "/115_audit_movie")
        self.assertEqual(token.access_token, "access-token-value")

    def test_rejects_missing_access_token_without_refreshing(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "data.db"
            conn = sqlite3.connect(db_path)
            conn.execute(
                "create table x_storages (id integer, mount_path text, driver text, disabled integer, addition text)"
            )
            conn.execute(
                "insert into x_storages values (?, ?, ?, ?, ?)",
                (1, "/115_audit_movie", "115 Open", 0, json.dumps({"refresh_token": "refresh"})),
            )
            conn.commit()
            conn.close()

            with self.assertRaisesRegex(RuntimeError, "access_token missing"):
                OpenListTokenStore(db_path).load_access_token()


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


class ProwlarrConfigTest(unittest.TestCase):
    def test_reads_api_key_from_config_xml(self):
        with tempfile.TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "config.xml"
            config_path.write_text("<Config><ApiKey>prowlarr-key-value</ApiKey></Config>", encoding="utf-8")

            self.assertEqual(ProwlarrConfig(config_path).load_api_key(), "prowlarr-key-value")

    def test_rejects_missing_api_key_without_default(self):
        with tempfile.TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "config.xml"
            config_path.write_text("<Config></Config>", encoding="utf-8")

            with self.assertRaisesRegex(RuntimeError, "Prowlarr ApiKey missing"):
                ProwlarrConfig(config_path).load_api_key()


class OpenListTokenProviderTest(unittest.TestCase):
    def test_reads_openlist_token_from_environment(self):
        provider = OpenListTokenProvider(env={"OPENLIST_TOKEN": "openlist-token-value"})

        self.assertEqual(provider.load_token(), "openlist-token-value")

    def test_reads_openlist_token_from_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            token_path = Path(tmp) / "openlist_token"
            token_path.write_text("openlist-token-value\n", encoding="utf-8")
            provider = OpenListTokenProvider(env={"OPENLIST_TOKEN_FILE": str(token_path)})

            self.assertEqual(provider.load_token(), "openlist-token-value")

    def test_rejects_missing_openlist_token_without_default(self):
        provider = OpenListTokenProvider(env={"OPENLIST_TOKEN_FILE": ""})

        with self.assertRaisesRegex(RuntimeError, "OpenList token missing"):
            provider.load_token()


class OpenListClientTest(unittest.TestCase):
    def test_list_path_uses_plain_authorization_header(self):
        transport = FakeTransport({"code": 200, "message": "success", "data": {"content": []}})
        client = OpenListClient("http://127.0.0.1:5244", "openlist-token-value", transport=transport)

        result = client.list_path("/115/电影")

        self.assertEqual(result["code"], 200)
        call = transport.calls[0]
        self.assertEqual(call["method"], "POST")
        self.assertEqual(call["url"], "http://127.0.0.1:5244/api/fs/list")
        self.assertEqual(call["headers"]["Authorization"], "openlist-token-value")
        self.assertEqual(call["data"]["path"], "/115/电影")
        self.assertNotIn("Bearer", call["headers"]["Authorization"])
        self.assertFalse(call["data"]["refresh"])

    def test_list_path_can_force_refresh(self):
        transport = FakeTransport({"code": 200, "message": "success", "data": {"content": []}})
        client = OpenListClient("http://127.0.0.1:5244", "openlist-token-value", transport=transport)

        client.list_path("/115/电影", refresh=True)

        self.assertTrue(transport.calls[0]["data"]["refresh"])

    def test_list_path_rejects_non_success_code(self):
        transport = FakeTransport({"code": 401, "message": "token is invalidated", "data": None})
        client = OpenListClient("http://127.0.0.1:5244", "openlist-token-value", transport=transport)

        with self.assertRaisesRegex(RuntimeError, "OpenList list failed"):
            client.list_path("/115/电影")

    def test_remove_names_uses_openlist_batch_remove_endpoint(self):
        transport = FakeTransport({"code": 200, "message": "success", "data": None})
        client = OpenListClient("http://127.0.0.1:5244", "openlist-token-value", transport=transport)

        client.remove_names("/115/电影/Movie", ["trailer.mp4", "poster.jpg"])

        call = transport.calls[0]
        self.assertEqual(call["method"], "POST")
        self.assertEqual(call["url"], "http://127.0.0.1:5244/api/fs/remove")
        self.assertEqual(call["headers"]["Authorization"], "openlist-token-value")
        self.assertEqual(call["data"], {"dir": "/115/电影/Movie", "names": ["trailer.mp4", "poster.jpg"]})

    def test_transport_converts_timeout_to_runtime_error(self):
        from pipeline.openlist import OpenListTransport

        with patch("pipeline.openlist.urllib.request.urlopen", side_effect=TimeoutError("timed out")):
            with self.assertRaisesRegex(RuntimeError, "OpenList request failed: timed out"):
                OpenListTransport().request("POST", "http://127.0.0.1:5244/api/fs/remove", data={"dir": "/root"}, timeout=1)

    def test_rename_path_uses_openlist_rename_endpoint(self):
        transport = FakeTransport({"code": 200, "message": "success", "data": None})
        client = OpenListClient("http://127.0.0.1:5244", "openlist-token-value", transport=transport)

        client.rename_path("/115/成人/old", "MIDA-304 - old")

        call = transport.calls[0]
        self.assertEqual(call["method"], "POST")
        self.assertEqual(call["url"], "http://127.0.0.1:5244/api/fs/rename")
        self.assertEqual(call["headers"]["Authorization"], "openlist-token-value")
        self.assertEqual(call["data"], {"path": "/115/成人/old", "name": "MIDA-304 - old"})

    def test_move_names_uses_openlist_move_endpoint(self):
        transport = FakeTransport({"code": 200, "message": "success", "data": None})
        client = OpenListClient("http://127.0.0.1:5244", "openlist-token-value", transport=transport)

        client.move_names("/115/剧集", "/115/动漫", ["成龙历险记"])

        call = transport.calls[0]
        self.assertEqual(call["method"], "POST")
        self.assertEqual(call["url"], "http://127.0.0.1:5244/api/fs/move")
        self.assertEqual(call["headers"]["Authorization"], "openlist-token-value")
        self.assertEqual(call["data"], {"src_dir": "/115/剧集", "dst_dir": "/115/动漫", "names": ["成龙历险记"]})


class MediaStationClientTest(unittest.TestCase):
    def test_list_libraries_uses_libraries_endpoint(self):
        transport = SequenceTransport(
            [
                {"tokens": {"access_token": "msg-token"}},
                {"data": {"items": [{"id": "lib-1", "name": "电影", "type": "movie"}]}},
            ]
        )
        client = MediaStationClient("http://127.0.0.1:18080/api", "admin", "secret", transport=transport)

        response = client.list_libraries(include_hidden=True)

        self.assertEqual(extract_library_items(response), [{"id": "lib-1", "name": "电影", "type": "movie"}])
        self.assertEqual(transport.calls[1]["method"], "GET")
        self.assertEqual(transport.calls[1]["url"], "http://127.0.0.1:18080/api/libraries?include_hidden=1")
        self.assertEqual(transport.calls[1]["headers"]["Authorization"], "Bearer msg-token")

    def test_scan_root_logs_in_and_uses_bearer_token(self):
        transport = SequenceTransport([{"tokens": {"access_token": "msg-token"}}, {"ok": True}])
        client = MediaStationClient("http://127.0.0.1:18080/api", "admin", "secret", transport=transport)

        result = client.scan_root("library-1", "root-1")

        self.assertEqual(result, {"ok": True})
        self.assertEqual(transport.calls[0]["method"], "POST")
        self.assertEqual(transport.calls[0]["url"], "http://127.0.0.1:18080/api/auth/login")
        self.assertEqual(transport.calls[0]["data"], {"username": "admin", "password": "secret"})
        self.assertEqual(transport.calls[1]["method"], "POST")
        self.assertEqual(transport.calls[1]["url"], "http://127.0.0.1:18080/api/libraries/library-1/roots/root-1/scan")
        self.assertEqual(transport.calls[1]["headers"]["Authorization"], "Bearer msg-token")

    def test_scrape_media_uses_single_item_scrape_body(self):
        transport = SequenceTransport([{"tokens": {"access_token": "msg-token"}}, {"ok": True}])
        client = MediaStationClient("http://127.0.0.1:18080/api", "admin", "secret", transport=transport)

        client.scrape_media("media-1")

        call = transport.calls[1]
        self.assertEqual(call["method"], "POST")
        self.assertEqual(call["url"], "http://127.0.0.1:18080/api/media/media-1/scrape")
        self.assertEqual(
            call["data"],
            {
                "episode_images": False,
                "refresh_matched": True,
                "include_matched": True,
            },
        )

    def test_update_media_metadata_patches_artwork_fields(self):
        transport = SequenceTransport([{"tokens": {"access_token": "msg-token"}}, {"id": "media-1"}])
        client = MediaStationClient("http://127.0.0.1:18080/api", "admin", "secret", transport=transport)

        client.update_media_metadata("media-1", {"poster_url": "https://img/poster.jpg", "title": "ignored"})

        call = transport.calls[1]
        self.assertEqual(call["method"], "PATCH")
        self.assertEqual(call["url"], "http://127.0.0.1:18080/api/media/media-1/metadata")
        self.assertEqual(call["data"], {"poster_url": "https://img/poster.jpg"})

    def test_adult_artwork_repair_patch_prefers_mgstage_poster(self):
        media = {
            "title": "ABF-159",
            "poster_url": "https://www.javbus.com/pics/cover/avrb_b.jpg",
            "backdrop_url": "https://image.mgstage.com/images/prestige/abf/159/cap_e_0_abf-159.jpg",
        }

        patch = adult_artwork_repair_patch(
            media,
            verifier=lambda url: url == "https://image.mgstage.com/images/prestige/abf/159/pf_o1_abf-159.jpg",
        )

        self.assertEqual(
            patch,
            {"poster_url": "https://image.mgstage.com/images/prestige/abf/159/pf_o1_abf-159.jpg"},
        )

    def test_adult_artwork_repair_patch_uses_dmm_candidates(self):
        media = {
            "title": "STARS-590",
            "poster_url": "https://www.javbus.com/pics/cover/8xio_b.jpg",
            "backdrop_url": "https://www.javbus.com/pics/sample/8xio_1.jpg",
        }

        patch = adult_artwork_repair_patch(
            media,
            verifier=lambda url: url
            in {
                "https://pics.dmm.co.jp/digital/video/1stars00590/1stars00590pl.jpg",
                "https://pics.dmm.co.jp/digital/video/1stars00590/1stars00590jp-1.jpg",
            },
        )

        self.assertEqual(patch["poster_url"], "https://pics.dmm.co.jp/digital/video/1stars00590/1stars00590pl.jpg")
        self.assertEqual(patch["backdrop_url"], "https://pics.dmm.co.jp/digital/video/1stars00590/1stars00590jp-1.jpg")
        self.assertTrue(is_bad_adult_artwork_url("https://www.javbus.com/pics/cover/8xio_b.jpg"))
        self.assertIn("stars00450", list(iter_dmm_cids("STARS-450")))
        self.assertEqual(
            list(
                iter_mgstage_poster_candidates(
                    "https://image.mgstage.com/images/prestige/abf/159/cap_e_0_abf-159.jpg"
                )
            )[0],
            "https://image.mgstage.com/images/prestige/abf/159/pf_o1_abf-159.jpg",
        )

    def test_adult_artwork_repair_skips_dmm_now_printing_placeholder(self):
        media = {
            "title": "STARS-590",
            "poster_url": "https://pics.dmm.co.jp/digital/video/stars590/stars590pl.jpg",
            "backdrop_url": "https://pics.dmm.co.jp/digital/video/stars590/stars590jp-1.jpg",
        }

        patch = adult_artwork_repair_patch(
            media,
            verifier=lambda url: url
            in {
                "https://pics.dmm.co.jp/digital/video/1stars00590/1stars00590pl.jpg",
                "https://pics.dmm.co.jp/digital/video/1stars00590/1stars00590jp-1.jpg",
            },
        )

        self.assertEqual(
            patch,
            {
                "poster_url": "https://pics.dmm.co.jp/digital/video/1stars00590/1stars00590pl.jpg",
                "backdrop_url": "https://pics.dmm.co.jp/digital/video/1stars00590/1stars00590jp-1.jpg",
            },
        )

    def test_adult_artwork_repair_clears_placeholder_when_replacement_is_missing(self):
        media = {
            "title": "STCV-017",
            "poster_url": "https://pics.dmm.co.jp/digital/video/stcv017/stcv017pl.jpg",
            "backdrop_url": "https://pics.dmm.co.jp/digital/video/stcv017/stcv017jp-1.jpg",
        }

        patch = adult_artwork_repair_patch(media, verifier=lambda url: False)

        self.assertEqual(patch, {"poster_url": "", "backdrop_url": ""})

    def test_reachable_image_url_rejects_dmm_now_printing_redirect(self):
        class PlaceholderImageResponse:
            status = 200
            headers = {"Content-Type": "image/jpeg"}

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def geturl(self):
                return "https://imgsrc.dmm.com/pics/mono/movie/n/now_printing/now_printing.jpg?w=800"

            def read(self, size=-1):
                return b"\xff\xd8\xff"

        with patch("pipeline.mediastation.urllib.request.urlopen", return_value=PlaceholderImageResponse()):
            self.assertFalse(reachable_image_url("https://pics.dmm.co.jp/digital/video/stcv017/stcv017pl.jpg"))

    def test_extracts_and_matches_media_items_flexibly(self):
        response = {"data": {"items": [{"id": "media-1", "library_id": "library-1", "title": "GANA-2525"}]}}

        items = extract_media_items(response)
        media = find_matching_media(items, ["GANA-2525"], library_id="library-1")

        self.assertEqual(extract_media_id(media), "media-1")

    def test_matching_prefers_main_feature_over_extras(self):
        items = [
            {
                "id": "menu-1",
                "library_id": "library-1",
                "title": "menu",
                "path": "cloud://openlist/115/电影/流浪地球2/menu/menu.mkv",
                "size_bytes": 175 * 1024 * 1024,
            },
            {
                "id": "extra-1",
                "library_id": "library-1",
                "title": "花絮",
                "path": "cloud://openlist/115/电影/流浪地球2/花絮/extra.mkv",
                "size_bytes": 5900 * 1024 * 1024,
            },
            {
                "id": "main-1",
                "library_id": "library-1",
                "title": "[DBD-Raws][4K_HDR][流浪地球2]",
                "path": "cloud://openlist/115/电影/流浪地球2/main.mkv",
                "size_bytes": 10122 * 1024 * 1024,
            },
        ]

        media = find_matching_media(items, ["流浪地球2"], library_id="library-1")

        self.assertEqual(extract_media_id(media), "main-1")

    def test_extract_codes_ignores_codec_tags_and_years(self):
        codes = extract_codes("[HEVC-10bit][H264-1080][Sintel.2010][ABF-363][GANA-2525]")

        self.assertEqual(codes, {"ABF-363", "GANA-2525"})

    def test_extract_codes_recognizes_standard_and_fc2_codes_only(self):
        codes = extract_codes(
            "SSIS-450 IPX_789 FSDSS.567 FC2-PPV-12345678 fc2ppv1234567 XXX-123ch "
            "n0680 k1234 Carib-020913-001 HEYDOUGA-1234-56 10musume 061234_01 10mu-022525_01"
        )

        self.assertEqual(
            codes,
            {
                "SSIS-450",
                "IPX-789",
                "FSDSS-567",
                "FC2-PPV-12345678",
                "FC2-PPV-1234567",
                "XXX-123",
            },
        )

    def test_strong_adult_code_query_accepts_exact_codes_and_excludes_long_titles(self):
        from pipeline.bot import is_strong_adult_code_query

        self.assertTrue(is_strong_adult_code_query("MIDE-882"))
        self.assertTrue(is_strong_adult_code_query("FC2-PPV-1234567"))
        self.assertTrue(is_strong_adult_code_query("BDMV-001"))
        self.assertFalse(is_strong_adult_code_query("电影名 MIDE-882 1080p"))


class ProwlarrClientTest(unittest.TestCase):
    def test_search_calls_prowlarr_api_with_query_limit_and_api_key(self):
        transport = FakeTransport([{"title": "Sintel 1080p", "seeders": 10, "magnetUrl": "magnet:?xt=urn:btih:abc"}])
        client = ProwlarrClient("http://127.0.0.1:9696", "prowlarr-key-value", transport=transport)

        results = client.search("sintel", limit=20)

        self.assertEqual(len(results), 1)
        call = transport.calls[0]
        self.assertEqual(call["method"], "GET")
        self.assertEqual(call["headers"]["X-Api-Key"], "prowlarr-key-value")
        self.assertEqual(call["url"], "http://127.0.0.1:9696/api/v1/search?query=sintel&limit=20")

    def test_search_can_limit_to_indexer_ids(self):
        transport = FakeTransport([])
        client = ProwlarrClient("http://127.0.0.1:9696", "prowlarr-key-value", transport=transport)

        client.search("ATFB-309", limit=1000, indexer_ids=[8])

        call = transport.calls[0]
        self.assertEqual(call["url"], "http://127.0.0.1:9696/api/v1/search?query=ATFB-309&limit=1000&indexerIds=8")

    def test_search_can_limit_to_categories(self):
        transport = FakeTransport([])
        client = ProwlarrClient("http://127.0.0.1:9696", "prowlarr-key-value", transport=transport)

        client.search("MIDE-882", limit=30, indexer_ids=[8], categories=[6000])

        call = transport.calls[0]
        self.assertEqual(call["url"], "http://127.0.0.1:9696/api/v1/search?query=MIDE-882&limit=30&categories=6000&indexerIds=8")

    def test_indexers_calls_prowlarr_indexer_api(self):
        transport = FakeTransport([{"id": 8, "name": "sukebei.nyaa.si"}])
        client = ProwlarrClient("http://127.0.0.1:9696", "prowlarr-key-value", transport=transport)

        results = client.indexers()

        self.assertEqual(results, [{"id": 8, "name": "sukebei.nyaa.si"}])
        self.assertEqual(transport.calls[0]["url"], "http://127.0.0.1:9696/api/v1/indexer")

    def test_resolve_download_uri_rebuilds_prowlarr_download_url_with_api_key(self):
        transport = DownloadRedirectTransport("magnet:?xt=urn:btih:ABC")
        client = ProwlarrClient("http://127.0.0.1:9696", "prowlarr-key-value", transport=transport)

        resolved = client.resolve_download_uri("prowlarr-download://9?link=ABC%2BDEF&file=SSIS-450")

        self.assertEqual(resolved, "magnet:?xt=urn:btih:ABC")
        self.assertEqual(
            transport.resolve_calls,
            [
                {
                    "url": "http://127.0.0.1:9696/9/download?apikey=prowlarr-key-value&link=ABC%2BDEF&file=SSIS-450",
                    "timeout": 30,
                }
            ],
        )

    def test_torrent_bytes_to_magnet_uses_bencoded_info_hash(self):
        info = (
            b"d"
            b"6:lengthi123e"
            b"4:name8:test.mkv"
            b"12:piece lengthi16384e"
            b"6:pieces20:aaaaaaaaaaaaaaaaaaaa"
            b"e"
        )
        torrent = b"d8:announce14:http://tracker4:info" + info + b"e"

        magnet = torrent_bytes_to_magnet(torrent)

        self.assertIn("xt=urn:btih:%s" % hashlib.sha1(info).hexdigest(), magnet)
        self.assertIn("dn=test.mkv", magnet)
        self.assertIn("tr=http:%2F%2Ftracker", magnet)


class ResourceSelectorTest(unittest.TestCase):
    def test_select_ranked_orders_candidates_and_assigns_one_based_rank(self):
        selector = ResourceSelector()

        ranked = selector.select_ranked(
            [
                {"title": "Sintel 720p", "seeders": 90, "infoHash": "BBB"},
                {"title": "Sintel 1080p BluRay", "seeders": 60, "infoHash": "AAA"},
                {"title": "Sintel CAM", "seeders": 500, "infoHash": "BAD"},
                {"title": "Sintel Dead", "seeders": 0, "infoHash": "DDD"},
            ]
        )

        self.assertEqual([item["rank"] for item in ranked], [1, 2, 3])
        self.assertEqual([item["title"] for item in ranked], ["Sintel 1080p BluRay", "Sintel 720p", "Sintel CAM"])
        self.assertEqual(ranked[0]["download_uri"], "magnet:?xt=urn:btih:AAA")

    def test_select_ranked_filters_unrelated_high_seed_results_by_query(self):
        selector = ResourceSelector()

        ranked = selector.select_ranked(
            [
                {"title": "流浪地球 2019 电影 国语中字 高清", "seeders": 3, "infoHash": "GOOD"},
                {"title": "Obsession.2026.1080p.AMZN.WEB-DL", "seeders": 9582, "infoHash": "BAD"},
            ],
            query="流浪地球",
        )

        self.assertEqual([item["title"] for item in ranked], ["流浪地球 2019 电影 国语中字 高清"])
        self.assertEqual(ranked[0]["download_uri"], "magnet:?xt=urn:btih:GOOD")

    def test_select_ranked_accepts_prowlarr_download_proxy_without_storing_api_key(self):
        selector = ResourceSelector()

        ranked = selector.select_ranked(
            [
                {
                    "title": "SSIS-450",
                    "seeders": 1,
                    "indexer": "0Magnet",
                    "downloadUrl": "http://127.0.0.1:9696/9/download?apikey=secret-key&link=ABC%2BDEF&file=SSIS-450",
                }
            ],
            query="SSIS-450",
        )

        self.assertEqual(ranked[0]["download_uri"], "prowlarr-download://9?link=ABC%2BDEF&file=SSIS-450")
        self.assertNotIn("secret-key", ranked[0]["download_uri"])

    def test_select_ranked_matches_code_queries_across_punctuation(self):
        selector = ResourceSelector()

        ranked = selector.select_ranked(
            [
                {"title": "MIDE-882 1080p", "seeders": 10, "infoHash": "GOOD"},
                {"title": "MIDE 777 1080p", "seeders": 100, "infoHash": "BAD"},
            ],
            query="MIDE-882",
        )

        self.assertEqual([item["title"] for item in ranked], ["MIDE-882 1080p"])

    def test_select_ranked_adds_uncensored_bonus_on_existing_score(self):
        selector = ResourceSelector()

        ranked = selector.select_ranked(
            [
                {"title": "STAR-646 1080p", "seeders": 10, "infoHash": "CEN"},
                {"title": "STAR-646 720p UC", "seeders": 10, "infoHash": "UNC"},
            ],
            query="STAR-646",
        )

        self.assertEqual([item["title"] for item in ranked], ["STAR-646 720p UC", "STAR-646 1080p"])

    def test_select_ranked_adds_chinese_subtitle_bonus_on_existing_score(self):
        selector = ResourceSelector()

        ranked = selector.select_ranked(
            [
                {"title": "MIDE-882 1080p", "seeders": 10, "infoHash": "NOSUB"},
                {"title": "MIDE-882 1080p 中字", "seeders": 10, "infoHash": "CHS"},
            ],
            query="MIDE-882",
        )

        self.assertEqual([item["title"] for item in ranked], ["MIDE-882 1080p 中字", "MIDE-882 1080p"])

    def test_select_ranked_prefers_exact_code_match_over_high_seed_suffix_title(self):
        selector = ResourceSelector()

        ranked = selector.select_ranked(
            [
                {"title": "SSIS-450", "seeders": 1, "infoHash": "EXACT"},
                {"title": "SSIS-450-C", "seeders": 100, "infoHash": "SUFFIX"},
            ],
            query="SSIS-450",
        )

        self.assertEqual([item["title"] for item in ranked], ["SSIS-450", "SSIS-450-C"])

    def test_select_ranked_uses_seeders_as_tiebreaker_not_primary_weight(self):
        selector = ResourceSelector()

        ranked = selector.select_ranked(
            [
                {"title": "MIDE-882 1080p 中字", "seeders": 10, "infoHash": "CHS"},
                {"title": "MIDE-882 1080p", "seeders": 11, "infoHash": "MORE"},
            ],
            query="MIDE-882",
        )

        self.assertEqual([item["title"] for item in ranked], ["MIDE-882 1080p 中字", "MIDE-882 1080p"])

    def test_select_ranked_adds_sukebei_indexer_bonus(self):
        selector = ResourceSelector()

        ranked = selector.select_ranked(
            [
                {"title": "ATFB-309 1080p", "indexer": "TorrentKitty", "seeders": 10, "infoHash": "OTHER"},
                {"title": "ATFB-309 720p", "indexer": "sukebei.nyaa.si", "seeders": 10, "infoHash": "SUKEBEI"},
            ],
            query="ATFB-309",
        )

        self.assertEqual([item["indexer"] for item in ranked], ["sukebei.nyaa.si", "TorrentKitty"])

    def test_select_ranked_uses_prowlarr_indexer_priority_when_available(self):
        selector = ResourceSelector(indexer_priorities={1: 25, 8: 1})

        ranked = selector.select_ranked(
            [
                {"title": "MIDE-882 1080p", "indexer": "Knaben", "indexerId": 1, "seeders": 30, "infoHash": "K1"},
                {"title": "MIDE-882 720p", "indexer": "sukebei.nyaa.si", "indexerId": 8, "seeders": 0, "infoHash": "S1"},
            ],
            query="MIDE-882",
        )

        self.assertEqual([item["indexer"] for item in ranked], ["sukebei.nyaa.si", "Knaben"])

    def test_select_ranked_does_not_let_one_extra_seeder_override_sukebei_bonus(self):
        selector = ResourceSelector()

        ranked = selector.select_ranked(
            [
                {"title": "ATFB-309 1080p", "indexer": "sukebei.nyaa.si", "seeders": 10, "infoHash": "SUKEBEI"},
                {"title": "ATFB-309 1080p", "indexer": "TorrentKitty", "seeders": 11, "infoHash": "OTHER"},
            ],
            query="ATFB-309",
        )

        self.assertEqual([item["indexer"] for item in ranked], ["sukebei.nyaa.si", "TorrentKitty"])

    def test_select_ranked_keeps_zero_seed_sukebei_candidates(self):
        selector = ResourceSelector()

        ranked = selector.select_ranked(
            [
                {"title": "ATFB-309 1080p", "indexer": "sukebei.nyaa.si", "seeders": 0, "infoHash": "SUKEBEI"},
            ],
            query="ATFB-309",
        )

        self.assertEqual([item["indexer"] for item in ranked], ["sukebei.nyaa.si"])

    def test_select_ranked_keeps_zero_seed_dht_candidates_but_penalizes_them(self):
        selector = ResourceSelector()

        ranked = selector.select_ranked(
            [
                {"title": "Sintel 1080p", "indexer": "Knaben", "seeders": 0, "infoHash": "DHT"},
                {"title": "Sintel 1080p", "indexer": "LimeTorrents", "seeders": 1, "infoHash": "SEEDED"},
                {"title": "Sintel 1080p", "indexer": "Unknown", "seeders": 0, "infoHash": "DROP"},
            ],
            query="Sintel",
        )

        self.assertEqual([item["infoHash"] for item in ranked], ["SEEDED", "DHT"])

    def test_select_ranked_limited_preserves_all_sukebei_candidates(self):
        selector = ResourceSelector()

        ranked = selector.select_ranked_limited(
            [
                {"title": "ATFB-309 other 1", "indexer": "TorrentKitty", "seeders": 20, "infoHash": "O1"},
                {"title": "ATFB-309 other 2", "indexer": "TorrentKitty", "seeders": 19, "infoHash": "O2"},
                {"title": "ATFB-309 sukebei 1", "indexer": "sukebei.nyaa.si", "seeders": 0, "infoHash": "S1"},
                {"title": "ATFB-309 sukebei 2", "indexer": "sukebei.nyaa.si", "seeders": 0, "infoHash": "S2"},
            ],
            query="ATFB-309",
            limit=2,
        )

        self.assertEqual([item["infoHash"] for item in ranked], ["S1", "S2"])
        self.assertEqual([item["rank"] for item in ranked], [1, 2])

    def test_select_ranked_deduplicates_same_info_hash(self):
        selector = ResourceSelector()

        ranked = selector.select_ranked(
            [
                {"title": "流浪地球 1080p", "indexer": "Knaben", "seeders": 7, "infoHash": "ABC"},
                {"title": "流浪地球 1080p", "indexer": "Nyaa.si", "seeders": 8, "magnetUrl": "magnet:?xt=urn:btih:abc"},
            ],
            query="流浪地球",
        )

        self.assertEqual(len(ranked), 1)
        self.assertEqual(ranked[0]["indexer"], "Nyaa.si")

    def test_select_ranked_prefers_sukebei_when_deduplicating_same_info_hash(self):
        selector = ResourceSelector()

        ranked = selector.select_ranked(
            [
                {"title": "ATFB-309", "indexer": "TorrentKitty", "seeders": 10, "infoHash": "ABC"},
                {"title": "ATFB-309", "indexer": "sukebei.nyaa.si", "seeders": 0, "infoHash": "ABC"},
            ],
            query="ATFB-309",
        )

        self.assertEqual(len(ranked), 1)
        self.assertEqual(ranked[0]["indexer"], "sukebei.nyaa.si")

    def test_select_ranked_does_not_filter_by_reported_size(self):
        selector = ResourceSelector()

        ranked = selector.select_ranked(
            [
                {"title": "ATFB-309 trailer", "seeders": 100, "size": 50 * 1024 * 1024, "infoHash": "SMALL"},
                {"title": "ATFB-309 full", "seeders": 2, "size": 1024 * 1024 * 1024, "infoHash": "GOOD"},
                {"title": "ATFB-309 1080p", "seeders": 1, "size": 0, "infoHash": "ZERO"},
            ],
            query="ATFB-309",
        )

        self.assertEqual([item["title"] for item in ranked], ["ATFB-309 1080p", "ATFB-309 full", "ATFB-309 trailer"])

    def test_select_rank_rejects_out_of_range_without_fallback(self):
        selector = ResourceSelector()

        with self.assertRaisesRegex(RuntimeError, "resource rank out of range"):
            selector.select_rank([{"title": "Sintel 1080p", "seeders": 10, "infoHash": "ABC"}], rank=2)

    def test_prefers_high_seeded_1080p_candidate_with_usable_uri(self):
        selector = ResourceSelector()
        candidates = [
            {"title": "Sintel CAM", "seeders": 500, "size": 1000000000, "magnetUrl": "magnet:?xt=urn:btih:bad"},
            {"title": "Sintel 1080p BluRay", "seeders": 60, "size": 3000000000, "magnetUrl": "magnet:?xt=urn:btih:good"},
            {"title": "Sintel 720p", "seeders": 90, "size": 1200000000, "magnetUrl": ""},
        ]

        selected = selector.select_best(candidates)

        self.assertEqual(selected["title"], "Sintel 1080p BluRay")
        self.assertEqual(selected["download_uri"], "magnet:?xt=urn:btih:good")

    def test_builds_magnet_from_info_hash_when_url_is_missing(self):
        selector = ResourceSelector()

        selected = selector.select_best([{"title": "Sintel 1080p", "seeders": 10, "infoHash": "ABCDEF"}])

        self.assertEqual(selected["download_uri"], "magnet:?xt=urn:btih:ABCDEF")

    def test_uses_info_hash_when_magnet_url_is_local_http_proxy(self):
        selector = ResourceSelector()

        selected = selector.select_best(
            [
                {
                    "title": "Sintel 1080p",
                    "seeders": 10,
                    "magnetUrl": "http://127.0.0.1:9696/1/download?apikey=secret",
                    "infoHash": "ABCDEF",
                }
            ]
        )

        self.assertEqual(selected["download_uri"], "magnet:?xt=urn:btih:ABCDEF")

    def test_rejects_local_download_url_without_magnet_or_info_hash(self):
        selector = ResourceSelector()

        with self.assertRaisesRegex(RuntimeError, "no acceptable resource"):
            selector.select_best(
                [
                    {
                        "title": "Sintel 1080p",
                        "seeders": 10,
                        "downloadUrl": "http://127.0.0.1:9696/1/download?apikey=secret",
                    }
                ]
            )

    def test_rejects_candidates_without_positive_seeders(self):
        selector = ResourceSelector()

        with self.assertRaisesRegex(RuntimeError, "no acceptable resource"):
            selector.select_best([{"title": "Sintel 1080p", "seeders": 0, "magnetUrl": "magnet:?xt=urn:btih:abc"}])

    def test_public_summary_redacts_sensitive_query_parameters(self):
        summary = public_resource_summary(
            {
                "title": "Sintel",
                "download_uri": "http://127.0.0.1:9696/1/download?apikey=secret&link=value",
            }
        )

        self.assertEqual(summary["download_uri"], "http://127.0.0.1:9696/1/download?apikey=REDACTED&link=value")


class Client115Test(unittest.TestCase):
    def test_add_offline_task_uses_official_endpoint_and_target_folder(self):
        transport = FakeTransport({"state": True, "data": [{"info_hash": "abc", "url": "magnet:?xt=urn:btih:abc"}]})
        client = Client115("access-token-value", transport=transport)

        result = client.add_offline_urls(["magnet:?xt=urn:btih:abc"], "3464134653584082023")

        self.assertEqual(result["state"], True)
        self.assertEqual(len(transport.calls), 1)
        call = transport.calls[0]
        self.assertEqual(call["method"], "POST")
        self.assertEqual(call["url"], "https://proapi.115.com/open/offline/add_task_urls")
        self.assertEqual(call["headers"]["Authorization"], "Bearer access-token-value")
        self.assertEqual(call["data"], {"urls": "magnet:?xt=urn:btih:abc", "wp_path_id": "3464134653584082023"})

    def test_delete_offline_task_uses_official_endpoint_without_deleting_files(self):
        transport = FakeTransport({"state": True, "data": []})
        client = Client115("access-token-value", transport=transport)

        result = client.delete_offline_task("ABC", delete_files=False)

        self.assertEqual(result["state"], True)
        call = transport.calls[0]
        self.assertEqual(call["method"], "POST")
        self.assertEqual(call["url"], "https://proapi.115.com/open/offline/del_task")
        self.assertEqual(call["headers"]["Authorization"], "Bearer access-token-value")
        self.assertEqual(call["data"], {"info_hash": "ABC", "del_source_file": "0"})

    def test_client_never_calls_refresh_token_endpoint(self):
        transport = FakeTransport({"state": False, "code": 40140125, "message": "access_token invalid"})
        client = Client115("expired-token", transport=transport)

        result = client.get_offline_tasks(page=1)

        self.assertEqual(result["code"], 40140125)
        self.assertNotIn("refreshToken", transport.calls[0]["url"])

    def test_get_folder_info_uses_official_folder_info_endpoint(self):
        transport = FakeTransport({"state": True, "data": {"file_id": "3464134653584082023", "file_name": "影视库-电影"}})
        client = Client115("access-token-value", transport=transport)

        result = client.get_folder_info("3464134653584082023")

        self.assertEqual(result["state"], True)
        call = transport.calls[0]
        self.assertEqual(call["method"], "GET")
        self.assertEqual(
            call["url"],
            "https://proapi.115.com/open/folder/get_info?file_id=3464134653584082023",
        )


class OfflineTaskTest(unittest.TestCase):
    def test_normalizes_115_status_code(self):
        task = normalize_task({"info_hash": "ABC", "status": 2, "percentDone": 100, "name": "Movie"})

        self.assertEqual(task["info_hash"], "ABC")
        self.assertEqual(task["status_name"], "success")
        self.assertEqual(task["percent_done"], 100)

    def test_finds_task_by_info_hash_case_insensitively(self):
        client = Fake115TaskClient(
            [
                {"state": True, "data": {"page_count": 2, "tasks": [{"info_hash": "AAA", "status": 1}]}},
                {"state": True, "data": {"page_count": 2, "tasks": [{"info_hash": "BbB", "status": 2}]}},
            ]
        )

        task = find_task_by_info_hash(client, "bbb")

        self.assertEqual(task["info_hash"], "BbB")
        self.assertEqual(task["status_name"], "success")
        self.assertEqual(client.calls, [1, 2])

    def test_finds_multiple_tasks_by_info_hashes_with_one_page_scan(self):
        client = Fake115TaskClient(
            [
                {
                    "state": True,
                    "data": {
                        "page_count": 1,
                        "tasks": [
                            {"info_hash": "AAA", "status": 1},
                            {"info_hash": "BbB", "status": 2},
                            {"info_hash": "CCC", "status": -1},
                        ],
                    },
                }
            ]
        )

        tasks = find_tasks_by_info_hashes(client, ["bbb", "ccc", "missing"])

        self.assertEqual(set(tasks.keys()), {"bbb", "ccc"})
        self.assertEqual(tasks["bbb"]["info_hash"], "BbB")
        self.assertEqual(tasks["bbb"]["status_name"], "success")
        self.assertEqual(tasks["ccc"]["status_name"], "failed")
        self.assertEqual(client.calls, [1])

    def test_rejects_missing_task_without_fallback(self):
        client = Fake115TaskClient([{"state": True, "data": {"page_count": 1, "tasks": []}}])

        with self.assertRaisesRegex(RuntimeError, "offline task not found"):
            find_task_by_info_hash(client, "missing")

    def test_wait_for_task_returns_when_success(self):
        client = Fake115TaskClient(
            [
                {"state": True, "data": {"page_count": 1, "tasks": [{"info_hash": "ABC", "status": 1}]}},
                {"state": True, "data": {"page_count": 1, "tasks": [{"info_hash": "ABC", "status": 2}]}},
            ]
        )
        sleeps = []

        task = wait_for_task(client, "ABC", timeout_seconds=30, interval_seconds=5, sleep=sleeps.append, now=StepClock())

        self.assertEqual(task["status_name"], "success")
        self.assertEqual(sleeps, [5])

    def test_wait_for_task_fails_fast_on_failed_status(self):
        client = Fake115TaskClient([{"state": True, "data": {"page_count": 1, "tasks": [{"info_hash": "ABC", "status": -1}]}}])

        with self.assertRaisesRegex(RuntimeError, "offline task failed"):
            wait_for_task(client, "ABC", timeout_seconds=30, interval_seconds=5, sleep=lambda seconds: None, now=StepClock())

    def test_wait_for_task_times_out_without_fallback(self):
        client = Fake115TaskClient([{"state": True, "data": {"page_count": 1, "tasks": [{"info_hash": "ABC", "status": 1}]}}])

        with self.assertRaisesRegex(TimeoutError, "offline task wait timeout"):
            wait_for_task(client, "ABC", timeout_seconds=1, interval_seconds=5, sleep=lambda seconds: None, now=StepClock(step=2))

    def test_task_can_cancel_only_active_statuses(self):
        self.assertTrue(task_can_cancel({"status_name": "downloading"}))
        self.assertTrue(task_can_cancel({"status_name": "allocating"}))
        self.assertFalse(task_can_cancel({"status_name": "success"}))
        self.assertFalse(task_can_cancel({"status_name": "failed"}))

    def test_cancel_task_if_active_deletes_offline_task_without_files(self):
        client = Fake115CancelClient(
            {"state": True, "data": {"page_count": 1, "tasks": [{"info_hash": "ABC", "status": 1, "percentDone": 10}]}},
            {"state": True, "data": []},
        )

        result = cancel_task_if_active(client, "ABC")

        self.assertTrue(result["cancelled"])
        self.assertEqual(result["task"]["status_name"], "cancelled")
        self.assertEqual(client.deleted, [("ABC", False)])

    def test_cancel_task_if_active_skips_finished_task(self):
        client = Fake115CancelClient(
            {"state": True, "data": {"page_count": 1, "tasks": [{"info_hash": "ABC", "status": 2, "percentDone": 100}]}},
            {"state": True, "data": []},
        )

        result = cancel_task_if_active(client, "ABC")

        self.assertFalse(result["cancelled"])
        self.assertEqual(result["task"]["status_name"], "success")
        self.assertEqual(client.deleted, [])

    def test_offline_submit_summary_keeps_info_hash_and_drops_url(self):
        summary = summarize_offline_submit(
            {
                "state": True,
                "data": [
                    {
                        "info_hash": "ABC",
                        "state": True,
                        "code": 0,
                        "url": "magnet:?xt=urn:btih:ABC",
                    }
                ],
            }
        )

        self.assertEqual(summary["tasks"][0]["info_hash"], "ABC")
        self.assertNotIn("url", summary["tasks"][0])


class CliSubmitSearchTest(unittest.TestCase):
    def test_search_prints_ranked_candidate_list(self):
        fake_prowlarr = FakeProwlarr(
            [
                {"title": "Sintel 720p", "seeders": 90, "size": 1200000000, "infoHash": "BBB"},
                {"title": "Sintel 1080p", "seeders": 60, "size": 3000000000, "infoHash": "AAA"},
            ]
        )
        stdout = io.StringIO()

        with patch("pipeline.cli.build_prowlarr_client", return_value=fake_prowlarr), patch("sys.stdout", stdout):
            code = cli_main(["search", "--query", "sintel", "--limit", "10"])

        payload = json.loads(stdout.getvalue())
        self.assertEqual(code, 0)
        self.assertEqual(payload["query"], "sintel")
        self.assertEqual(payload["count"], 2)
        self.assertEqual([item["rank"] for item in payload["results"]], [1, 2])
        self.assertEqual(payload["results"][0]["download_uri"], "magnet:?xt=urn:btih:AAA")

    def test_search_limits_ranked_candidate_list_locally(self):
        fake_prowlarr = FakeProwlarr(
            [
                {"title": "Sintel 720p", "seeders": 90, "infoHash": "BBB"},
                {"title": "Sintel 1080p", "seeders": 60, "infoHash": "AAA"},
            ]
        )
        stdout = io.StringIO()

        with patch("pipeline.cli.build_prowlarr_client", return_value=fake_prowlarr), patch("sys.stdout", stdout):
            code = cli_main(["search", "--query", "sintel", "--limit", "1"])

        payload = json.loads(stdout.getvalue())
        self.assertEqual(code, 0)
        self.assertEqual(payload["count"], 1)
        self.assertEqual([item["rank"] for item in payload["results"]], [1])

    def test_search_includes_all_sukebei_results_even_beyond_limit(self):
        fake_prowlarr = FakeProwlarr(
            [{"title": "ATFB-309 other", "indexer": "TorrentKitty", "seeders": 20, "infoHash": "OTHER"}],
            indexers=[{"id": 8, "name": "sukebei.nyaa.si"}],
            indexer_results={
                (8,): [
                    {"title": "ATFB-309 sukebei 1", "indexer": "sukebei.nyaa.si", "seeders": 0, "infoHash": "S1"},
                    {"title": "ATFB-309 sukebei 2", "indexer": "sukebei.nyaa.si", "seeders": 0, "infoHash": "S2"},
                ]
            },
        )
        stdout = io.StringIO()

        with patch("pipeline.cli.build_prowlarr_client", return_value=fake_prowlarr), patch("sys.stdout", stdout):
            code = cli_main(["search", "--query", "ATFB-309", "--limit", "1"])

        payload = json.loads(stdout.getvalue())
        self.assertEqual(code, 0)
        self.assertEqual(payload["count"], 2)
        self.assertEqual([item["indexer"] for item in payload["results"]], ["sukebei.nyaa.si", "sukebei.nyaa.si"])
        self.assertEqual(fake_prowlarr.search_calls, [("ATFB-309", 1000, (8,))])

    def test_search_skips_sukebei_supplement_for_non_adult_non_code_query(self):
        fake_prowlarr = FakeProwlarr(
            [],
            indexers=[
                {"id": 1, "name": "Knaben", "enable": True},
                {"id": 8, "name": "sukebei.nyaa.si", "enable": True},
                {"id": 10, "name": "Mikan", "enable": True},
            ],
            indexer_results={
                (1,): [{"title": "鬼灭之刃 1080p", "indexer": "Knaben", "seeders": 20, "infoHash": "K1"}],
                (8,): [{"title": "鬼灭之刃 sukebei", "indexer": "sukebei.nyaa.si", "seeders": 20, "infoHash": "S1"}],
                (10,): [{"title": "鬼灭之刃 Mikan", "indexer": "Mikan", "seeders": 1, "infoHash": "M1"}],
            },
        )
        stdout = io.StringIO()

        with patch("pipeline.cli.build_prowlarr_client", return_value=fake_prowlarr), patch("sys.stdout", stdout):
            code = cli_main(["search", "--query", "鬼灭之刃", "--limit", "10"])

        payload = json.loads(stdout.getvalue())
        self.assertEqual(code, 0)
        self.assertEqual({item["indexer"] for item in payload["results"]}, {"Knaben", "Mikan"})
        self.assertEqual(fake_prowlarr.search_calls, [("鬼灭之刃", 100, (1,)), ("鬼灭之刃", 100, (10,))])

    def test_search_skips_anime_supplements_for_plain_movie_query(self):
        fake_prowlarr = FakeProwlarr(
            [],
            indexers=[
                {"id": 1, "name": "Knaben", "enable": True},
                {"id": 5, "name": "Nyaa.si", "enable": True},
                {"id": 6, "name": "ACG.RIP", "enable": True},
                {"id": 8, "name": "sukebei.nyaa.si", "enable": True},
                {"id": 10, "name": "Mikan", "enable": True},
                {"id": 11, "name": "Bangumi Moe", "enable": True},
            ],
            indexer_results={
                (1,): [{"title": "Sintel 1080p", "indexer": "Knaben", "seeders": 20, "infoHash": "K1"}],
                (5,): [{"title": "Sintel Nyaa", "indexer": "Nyaa.si", "seeders": 20, "infoHash": "N1"}],
                (6,): [{"title": "Sintel ACG", "indexer": "ACG.RIP", "seeders": 20, "infoHash": "A1"}],
                (10,): [{"title": "Sintel Mikan", "indexer": "Mikan", "seeders": 20, "infoHash": "M1"}],
                (11,): [{"title": "Sintel Bangumi", "indexer": "Bangumi Moe", "seeders": 20, "infoHash": "B1"}],
            },
        )
        stdout = io.StringIO()

        with patch("pipeline.cli.build_prowlarr_client", return_value=fake_prowlarr), patch("sys.stdout", stdout):
            code = cli_main(["search", "--query", "Sintel", "--limit", "10"])

        payload = json.loads(stdout.getvalue())
        self.assertEqual(code, 0)
        self.assertEqual([item["indexer"] for item in payload["results"]], ["Knaben"])
        self.assertEqual(fake_prowlarr.search_calls, [("Sintel", 100, (1,))])

    def test_search_excludes_anime_specialized_indexers_from_primary_call_and_adds_them_as_supplements(self):
        fake_prowlarr = FakeProwlarr(
            [],
            indexers=[
                {"id": 1, "name": "Knaben", "enable": True},
                {"id": 5, "name": "Nyaa.si", "enable": True},
                {"id": 6, "name": "ACG.RIP", "enable": True},
                {"id": 10, "name": "Mikan", "enable": True},
                {"id": 11, "name": "Bangumi Moe", "enable": True},
            ],
            indexer_results={
                (1,): [{"title": "葬送的芙莉莲 1080p", "indexer": "Knaben", "seeders": 20, "infoHash": "K1"}],
                (5,): [{"title": "葬送的芙莉莲 Nyaa", "indexer": "Nyaa.si", "seeders": 2, "infoHash": "N1"}],
                (6,): [{"title": "葬送的芙莉莲 ACG", "indexer": "ACG.RIP", "seeders": 3, "infoHash": "A1"}],
                (10,): [{"title": "葬送的芙莉莲 Mikan", "indexer": "Mikan", "seeders": 1, "infoHash": "M1"}],
                (11,): [{"title": "葬送的芙莉莲 Bangumi", "indexer": "Bangumi Moe", "seeders": 1, "infoHash": "B1"}],
            },
        )
        stdout = io.StringIO()

        with patch("pipeline.cli.build_prowlarr_client", return_value=fake_prowlarr), patch("sys.stdout", stdout):
            code = cli_main(["search", "--query", "葬送的芙莉莲", "--limit", "10"])

        payload = json.loads(stdout.getvalue())
        self.assertEqual(code, 0)
        self.assertEqual(payload["count"], 5)
        self.assertEqual({item["indexer"] for item in payload["results"]}, {"Knaben", "Nyaa.si", "ACG.RIP", "Mikan", "Bangumi Moe"})
        self.assertEqual(
            fake_prowlarr.search_calls,
            [
                ("葬送的芙莉莲", 100, (1,)),
                ("葬送的芙莉莲", 100, (5,)),
                ("葬送的芙莉莲", 100, (6,)),
                ("葬送的芙莉莲", 100, (10,)),
                ("葬送的芙莉莲", 100, (11,)),
            ],
        )

    def test_search_skips_disabled_supplement_indexers(self):
        fake_prowlarr = FakeProwlarr(
            [],
            indexers=[
                {"id": 1, "name": "Knaben", "enable": True},
                {"id": 8, "name": "sukebei.nyaa.si", "enable": False},
                {"id": 10, "name": "Mikan", "enable": False},
            ],
            indexer_results={
                (1,): [{"title": "ATFB-309 1080p", "indexer": "Knaben", "seeders": 20, "infoHash": "K1"}],
                (8,): [{"title": "ATFB-309 sukebei", "indexer": "sukebei.nyaa.si", "seeders": 0, "infoHash": "S1"}],
                (10,): [{"title": "ATFB-309 Mikan", "indexer": "Mikan", "seeders": 1, "infoHash": "M1"}],
            },
        )
        stdout = io.StringIO()

        with patch("pipeline.cli.build_prowlarr_client", return_value=fake_prowlarr), patch("sys.stdout", stdout):
            code = cli_main(["search", "--query", "ATFB-309", "--limit", "10"])

        payload = json.loads(stdout.getvalue())
        self.assertEqual(code, 0)
        self.assertEqual([item["indexer"] for item in payload["results"]], ["Knaben"])
        self.assertEqual(fake_prowlarr.search_calls, [("ATFB-309", 100, (1,))])

    def test_primary_search_falls_back_to_single_indexers_when_aggregate_times_out(self):
        from pipeline.bot import search_primary_indexer_results

        fake_prowlarr = FakeProwlarr(
            [],
            indexers=[
                {"id": 1, "name": "Knaben", "enable": True},
                {"id": 2, "name": "SlowIndexer", "enable": True},
            ],
            indexer_results={
                (1,): [{"title": "Sintel 1080p", "indexer": "Knaben", "seeders": 20, "infoHash": "K1"}],
            },
            indexer_errors={
                (1, 2): TimeoutError("timed out"),
                (2,): TimeoutError("timed out"),
            },
        )

        results = search_primary_indexer_results(fake_prowlarr, "sintel", 100, indexers=fake_prowlarr.indexers())

        self.assertEqual([item["infoHash"] for item in results], ["K1"])
        self.assertEqual(
            fake_prowlarr.search_calls,
            [("sintel", 100, (1, 2)), ("sintel", 100, (1,)), ("sintel", 100, (2,))],
        )

    def test_submit_search_commit_uses_requested_rank(self):
        fake_prowlarr = FakeProwlarr(
            [
                {"title": "Sintel 720p", "seeders": 90, "infoHash": "BBB"},
                {"title": "Sintel 1080p", "seeders": 60, "infoHash": "AAA"},
            ]
        )
        fake_115 = Fake115SubmitClient({"state": True, "data": [{"info_hash": "AAA", "state": True, "code": 0}]})
        stdout = io.StringIO()

        with patch("pipeline.cli.build_prowlarr_client", return_value=fake_prowlarr), patch(
            "pipeline.cli.build_openlist_client", return_value=FakeOpenList()
        ), patch("pipeline.cli.OpenListTokenStore", return_value=FakeTokenStore("unused.db")), patch(
            "pipeline.cli.Client115", return_value=fake_115
        ), patch("sys.stdout", stdout):
            code = cli_main(
                [
                    "--openlist-db",
                    "unused.db",
                    "submit-search",
                    "--query",
                    "sintel",
                    "--category",
                    "movie",
                    "--rank",
                    "2",
                    "--commit",
                ]
            )

        payload = json.loads(stdout.getvalue())
        self.assertEqual(code, 0)
        self.assertEqual(fake_115.urls, ["magnet:?xt=urn:btih:BBB"])
        self.assertEqual(payload["selected"]["rank"], 2)

    def test_submit_search_rejects_rank_out_of_range_without_fallback(self):
        fake_prowlarr = FakeProwlarr([{"title": "Sintel 720p", "seeders": 90, "infoHash": "BBB"}])
        stderr = io.StringIO()

        with patch("pipeline.cli.build_prowlarr_client", return_value=fake_prowlarr), patch("sys.stderr", stderr):
            code = cli_run(["submit-search", "--query", "sintel", "--category", "movie", "--rank", "2"])

        self.assertEqual(code, 1)
        self.assertEqual(stderr.getvalue().strip(), "error: resource rank out of range: 2")

    def test_submit_search_commit_prints_info_hash_from_115_response(self):
        fake_prowlarr = FakeProwlarr([{"title": "Sintel 1080p", "seeders": 10, "infoHash": "ABC"}])
        fake_115 = Fake115SubmitClient(
            {
                "state": True,
                "data": [{"info_hash": "ABC", "state": True, "code": 0, "url": "magnet:?xt=urn:btih:ABC"}],
            }
        )
        fake_openlist = FakeOpenList()
        fake_token_store = FakeTokenStore("unused.db")
        stdout = io.StringIO()

        with patch("pipeline.cli.build_prowlarr_client", return_value=fake_prowlarr), patch(
            "pipeline.cli.build_openlist_client", return_value=fake_openlist
        ), patch(
            "pipeline.cli.OpenListTokenStore", return_value=fake_token_store
        ), patch("pipeline.cli.Client115", return_value=fake_115), patch("sys.stdout", stdout):
            code = cli_main(["--openlist-db", "unused.db", "submit-search", "--query", "sintel", "--category", "movie", "--commit"])

        payload = json.loads(stdout.getvalue())
        self.assertEqual(code, 0)
        self.assertEqual(fake_openlist.paths, ["/115/电影"])
        self.assertEqual(fake_token_store.load_count, 1)
        self.assertEqual(fake_115.urls, ["magnet:?xt=urn:btih:ABC"])
        self.assertEqual(fake_115.folder_id, "3464134653584082023")
        self.assertEqual(payload["submit"]["tasks"][0]["info_hash"], "ABC")
        self.assertNotIn("url", payload["submit"]["tasks"][0])

    def test_submit_search_adult_commit_warms_adult_openlist_path(self):
        fake_prowlarr = FakeProwlarr([{"title": "Adult 1080p", "seeders": 10, "infoHash": "DEF"}])
        fake_openlist = FakeOpenList()

        with patch("pipeline.cli.build_prowlarr_client", return_value=fake_prowlarr), patch(
            "pipeline.cli.build_openlist_client", return_value=fake_openlist
        ), patch("pipeline.cli.OpenListTokenStore", return_value=FakeTokenStore("unused.db")), patch(
            "pipeline.cli.Client115", return_value=Fake115SubmitClient({"state": True, "data": [{"info_hash": "DEF"}]})
        ), patch("sys.stdout", io.StringIO()):
            cli_main(["--openlist-db", "unused.db", "submit-search", "--query", "adult", "--category", "adult", "--commit"])

        self.assertEqual(fake_openlist.paths, ["/115/成人"])

    def test_cli_run_prints_business_error_without_traceback(self):
        stderr = io.StringIO()

        with patch("pipeline.cli.main", side_effect=RuntimeError("115 offline task list failed: access_token 无效")), patch(
            "sys.stderr", stderr
        ):
            code = cli_run(["task-status"])

        self.assertEqual(code, 1)
        self.assertEqual(stderr.getvalue().strip(), "error: 115 offline task list failed: access_token 无效")
        self.assertNotIn("Traceback", stderr.getvalue())

    def test_task_status_warms_openlist_before_loading_115_token(self):
        events = []
        fake_openlist = EventOpenList(events)
        fake_token_store = EventTokenStore("unused.db", events)
        stdout = io.StringIO()

        with patch("pipeline.cli.build_openlist_client", return_value=fake_openlist), patch(
            "pipeline.cli.OpenListTokenStore", return_value=fake_token_store
        ), patch("pipeline.cli.Client115", return_value=Fake115StatusClient()), patch("sys.stdout", stdout):
            code = cli_main(["--openlist-db", "unused.db", "task-status", "--info-hash", "ABC"])

        self.assertEqual(code, 0)
        self.assertEqual(events, ["warm:/115/电影", "load_token"])

    def test_add_offline_adult_commit_warms_adult_before_loading_115_token(self):
        events = []
        fake_openlist = EventOpenList(events)
        fake_token_store = EventTokenStore("unused.db", events)

        with patch("pipeline.cli.build_openlist_client", return_value=fake_openlist), patch(
            "pipeline.cli.OpenListTokenStore", return_value=fake_token_store
        ), patch("pipeline.cli.Client115", return_value=Fake115SubmitClient({"state": True, "data": [{"info_hash": "DEF"}]})), patch(
            "sys.stdout", io.StringIO()
        ):
            cli_main(
                [
                    "--openlist-db",
                    "unused.db",
                    "add-offline",
                    "--category",
                    "adult",
                    "--url",
                    "magnet:?xt=urn:btih:DEF",
                    "--commit",
                ]
            )

        self.assertEqual(events, ["warm:/115/成人", "load_token"])


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
        self.remove_calls = []
        self.rename_calls = []
        self.move_calls = []

    def list_path(self, path, refresh=False):
        self.events.append(("openlist", path, refresh))
        return {"code": 200, "message": "success", "data": {"content": self.tree.get(path, []), "total": len(self.tree.get(path, []))}}

    def list_all(self, path, refresh=False):
        self.events.append(("list_all", path, refresh))
        return list(self.tree.get(path, []))

    def remove_names(self, dir_path, names):
        self.remove_calls.append((dir_path, list(names)))
        self.events.append(("remove", dir_path, tuple(names)))
        return [{"code": 200, "message": "success"}]

    def rename_path(self, path, name):
        self.rename_calls.append((path, name))
        self.events.append(("rename", path, name))
        return {"code": 200, "message": "success"}

    def move_names(self, src_dir, dst_dir, names):
        self.move_calls.append((src_dir, dst_dir, list(names)))
        self.events.append(("move", src_dir, dst_dir, tuple(names)))
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
    def __init__(self, search_response=None, list_response=None, events=None, artwork_repair_response=None):
        self.search_response = search_response or {"data": {"items": []}}
        self.list_response = list_response or {"data": {"items": []}}
        self.events = events
        self.artwork_repair_response = artwork_repair_response or {"status": "skipped", "updated": 0, "reason": "not_needed"}
        self.scan_calls = []
        self.search_calls = []
        self.list_calls = []
        self.scrape_calls = []
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
        self.chat_action_error = chat_action_error

    def send_message(self, chat_id, text, reply_markup=None):
        self.messages.append({"chat_id": chat_id, "text": text, "reply_markup": reply_markup})
        return {"ok": True, "result": {"message_id": 1000 + len(self.messages)}}

    def send_chat_action(self, chat_id, action="typing"):
        if self.chat_action_error:
            raise self.chat_action_error
        self.chat_actions.append({"chat_id": chat_id, "action": action})

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

    def sync_completed_task(self, category, title, task, progress_callback=None):
        self.sync_calls.append((category, title, (task or {}).get("info_hash")))
        out = dict(task or {})
        for progress in self.sync_progress:
            out.update(progress)
            if progress_callback:
                progress_callback(dict(out))
        out.update(self.sync_response)
        return out


if __name__ == "__main__":
    unittest.main()
