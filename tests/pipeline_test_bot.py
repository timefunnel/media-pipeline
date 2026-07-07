from tests.test_pipeline_core import *


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
                "OPENLIST_MEDIA_SCAN_USERNAME": "media_scan",
                "OPENLIST_MEDIA_SCAN_PASSWORD": "scan-secret",
            }
        )

        self.assertEqual(config.token, "123:token")
        self.assertEqual(config.allowed_user_ids, {700656624})
        self.assertEqual(config.state_db_path, "/bot/state.db")
        self.assertEqual(config.telegram_timeout, 90)
        self.assertEqual(config.openlist_scan_username, "media_scan")
        self.assertEqual(config.openlist_scan_password, "scan-secret")

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
                "BOT_TASK_WORKERS": "3",
                "BOT_TASK_MESSAGE_EDIT_MIN_INTERVAL_SECONDS": "1.5",
                "SUBTITLE_AUTO_MATCH_ENABLED": "1",
                "SUBTITLE_AUTO_MATCH_ADULT_ONLY": "1",
                "SUBTITLE_CACHE_DIR": "/subtitle-cache",
                "SUBTITLE_PROVIDERS": "assrt,opensubtitles",
                "SUBTITLE_SEARCH_TIMEOUT_SECONDS": "9",
                "SUBTITLE_DOWNLOAD_MAX_BYTES": "123456",
                "SUBTITLE_BACKFILL_DEFAULT_LIMIT": "7",
                "ASSRT_API_TOKEN": "assrt-token",
                "OPENSUBTITLES_API_KEY": "opensubtitles-key",
            }
        )

        self.assertEqual(config.sync_recovery_interval_seconds, 120)
        self.assertEqual(config.task_workers, 3)
        self.assertEqual(config.task_message_edit_min_interval_seconds, 1.5)
        self.assertTrue(config.subtitle_auto_match_enabled)
        self.assertTrue(config.subtitle_auto_match_adult_only)
        self.assertEqual(config.subtitle_cache_dir, "/subtitle-cache")
        self.assertEqual(config.subtitle_providers, ("assrt", "opensubtitles"))
        self.assertEqual(config.subtitle_search_timeout_seconds, 9)
        self.assertEqual(config.subtitle_download_max_bytes, 123456)
        self.assertEqual(config.subtitle_backfill_default_limit, 7)
        self.assertEqual(config.assrt_api_token, "assrt-token")
        self.assertEqual(config.opensubtitles_api_key, "opensubtitles-key")

    def test_bot_config_reads_msg_trash_hide_sync_settings(self):
        from pipeline.bot import BotConfig

        config = BotConfig.from_env(
            {
                "TG_BOT_TOKEN": "123:token",
                "TG_ALLOWED_USER_IDS": "700656624",
                "MSG_TRASH_HIDE_SYNC_ENABLED": "1",
                "MSG_TRASH_HIDE_SYNC_LIMIT": "25",
            }
        )

        self.assertTrue(config.msg_trash_hide_sync_enabled)
        self.assertEqual(config.msg_trash_hide_sync_limit, 25)

    def test_bot_config_reads_externalized_search_and_prowlarr_settings(self):
        from pipeline.bot import BotConfig

        config = BotConfig.from_env(
            {
                "TG_BOT_TOKEN": "123:token",
                "TG_ALLOWED_USER_IDS": "700656624",
                "BOT_SEARCH_PAGE_SIZE": "10",
                "BOT_TASK_LIST_PAGE_SIZE": "7",
                "BOT_TASK_LIST_FETCH_LIMIT": "77",
                "PROWLARR_UPSTREAM_SEARCH_LIMIT": "150",
                "PROWLARR_MAX_WORKERS": "3",
                "PROWLARR_EARLY_RETURN_AFTER_SECONDS": "1.5",
                "PROWLARR_EARLY_RETURN_MIN_RESULTS": "55",
                "PROWLARR_EARLY_RETURN_REQUIRED_PRIORITY": "10",
                "PROWLARR_PROFILE_GENERAL_UPSTREAM_LIMIT": "120",
                "PROWLARR_PROFILE_ADULT_UPSTREAM_LIMIT": "200",
                "PROWLARR_PROFILE_ANIME_UPSTREAM_LIMIT": "80",
                "PROWLARR_PROFILE_GENERAL_TIMEOUT_SECONDS": "3",
                "PROWLARR_PROFILE_ADULT_TIMEOUT_SECONDS": "5",
                "PROWLARR_PROFILE_ANIME_TIMEOUT_SECONDS": "2",
                "PROWLARR_PROFILE_GENERAL_MAX_WORKERS": "4",
                "PROWLARR_PROFILE_ADULT_MAX_WORKERS": "6",
                "PROWLARR_PROFILE_ANIME_MAX_WORKERS": "2",
                "PROWLARR_PROFILE_GENERAL_CATEGORIES": "1000,2000",
                "PROWLARR_PROFILE_ADULT_CATEGORIES": "6000",
                "PROWLARR_PROFILE_ANIME_CATEGORIES": "5070,5080",
                "PROWLARR_PROFILE_GENERAL_TAG_LABELS": "general,public",
                "PROWLARR_PROFILE_ADULT_TAG_LABELS": "adult,sukebei",
                "PROWLARR_PROFILE_ANIME_TAG_LABELS": "anime,nyaa",
            }
        )

        self.assertEqual(config.search_page_size, 10)
        self.assertEqual(config.task_list_page_size, 7)
        self.assertEqual(config.task_list_fetch_limit, 77)
        self.assertEqual(config.prowlarr_upstream_search_limit, 150)
        self.assertEqual(config.prowlarr_max_workers, 3)
        self.assertEqual(config.prowlarr_early_return_after_seconds, 1.5)
        self.assertEqual(config.prowlarr_early_return_min_results, 55)
        self.assertEqual(config.prowlarr_early_return_required_priority, 10)
        self.assertEqual(config.search_profile_upstream_limits["general"], 120)
        self.assertEqual(config.search_profile_upstream_limits["adult"], 200)
        self.assertEqual(config.search_profile_upstream_limits["anime"], 80)
        self.assertEqual(config.search_profile_timeout_seconds["general"], 3)
        self.assertEqual(config.search_profile_timeout_seconds["adult"], 5)
        self.assertEqual(config.search_profile_timeout_seconds["anime"], 2)
        self.assertEqual(config.search_profile_max_workers["general"], 4)
        self.assertEqual(config.search_profile_max_workers["adult"], 6)
        self.assertEqual(config.search_profile_max_workers["anime"], 2)
        self.assertEqual(config.search_profile_categories["general"], (1000, 2000))
        self.assertEqual(config.search_profile_categories["anime"], (5070, 5080))
        self.assertEqual(config.search_profile_tag_labels["adult"], ("adult", "sukebei"))

    def test_bot_config_reads_llm_rerank_settings(self):
        from pipeline.bot import BotConfig

        config = BotConfig.from_env(
            {
                "TG_BOT_TOKEN": "123:token",
                "TG_ALLOWED_USER_IDS": "700656624",
                "LLM_SEARCH_RERANK_ENABLED": "1",
                "LLM_API_KEY": "llm-key",
                "LLM_BASE_URL": "https://api.deepseek.com/v1",
                "LLM_MODEL": "deepseek-v4-flash",
                "LLM_TIMEOUT_SECONDS": "3",
                "LLM_SEARCH_RERANK_LIMIT": "25",
                "LLM_THINKING_DISABLED": "0",
            }
        )

        self.assertTrue(config.llm_search_rerank_enabled)
        self.assertEqual(config.llm_api_key, "llm-key")
        self.assertEqual(config.llm_base_url, "https://api.deepseek.com/v1")
        self.assertEqual(config.llm_model, "deepseek-v4-flash")
        self.assertEqual(config.llm_timeout_seconds, 3)
        self.assertEqual(config.llm_search_rerank_limit, 25)
        self.assertFalse(config.llm_thinking_disabled)

    def test_bot_config_rejects_enabled_llm_rerank_without_api_key(self):
        from pipeline.bot import BotConfig

        with self.assertRaisesRegex(RuntimeError, "LLM_API_KEY missing"):
            BotConfig.from_env(
                {
                    "TG_BOT_TOKEN": "123:token",
                    "TG_ALLOWED_USER_IDS": "700656624",
                    "LLM_SEARCH_RERANK_ENABLED": "1",
                }
            )


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

    def test_candidate_store_claims_submission_once(self):
        from pipeline.bot import CandidateStore

        with tempfile.TemporaryDirectory() as tmp:
            store = CandidateStore(str(Path(tmp) / "state.db"))
            candidate_id = store.save_candidate(700656624, 9001, "movie", "sintel", {"title": "Sintel"})

            first = store.claim_candidate_submission(candidate_id)
            second = store.claim_candidate_submission(candidate_id)
            store.finish_candidate_submission(candidate_id, "submitted", info_hash="ABC")
            third = store.claim_candidate_submission(candidate_id)

        self.assertTrue(first["claimed"])
        self.assertFalse(second["claimed"])
        self.assertEqual(second["status"], "running")
        self.assertFalse(third["claimed"])
        self.assertEqual(third["status"], "submitted")
        self.assertEqual(third["info_hash"], "ABC")

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
            session_id = store.save_search_session(
                700656624,
                9001,
                "movie",
                "sintel",
                [first_id, second_id],
                metadata={"source_count": 2, "total_ms": 1234},
            )

            session = store.load_search_session(session_id)
            found = store.find_search_session_by_candidate(second_id)

        self.assertEqual(session["user_id"], 700656624)
        self.assertEqual(session["chat_id"], 9001)
        self.assertEqual(session["query"], "sintel")
        self.assertEqual(session["candidate_ids"], [first_id, second_id])
        self.assertEqual(session["metadata"], {"source_count": 2, "total_ms": 1234})
        self.assertEqual(found["metadata"], {"source_count": 2, "total_ms": 1234})

    def test_candidate_store_records_subtitle_backfill_attempts(self):
        from pipeline.bot import CandidateStore

        with tempfile.TemporaryDirectory() as tmp:
            store = CandidateStore(str(Path(tmp) / "state.db"))
            store.save_subtitle_backfill_record(
                "media-1",
                "MIDE-882",
                "MIDE-882",
                {"subtitle_match_status": "skipped", "subtitle_match_reason": "not_found"},
            )
            first = store.subtitle_backfill_records(["media-1"])["media-1"]
            store.save_subtitle_backfill_record(
                "media-1",
                "MIDE-882",
                "MIDE-882",
                {"subtitle_match_status": "failed", "subtitle_match_error": "upstream"},
            )
            second = store.subtitle_backfill_records(["media-1", "missing"])["media-1"]

        self.assertEqual(first["status"], "not_found")
        self.assertEqual(first["attempt_count"], 1)
        self.assertEqual(second["status"], "failed")
        self.assertEqual(second["error"], "upstream")
        self.assertEqual(second["attempt_count"], 2)

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

    def test_candidate_store_tracks_processed_msg_trash_hide_items(self):
        from pipeline.bot import CandidateStore

        with tempfile.TemporaryDirectory() as tmp:
            store = CandidateStore(str(Path(tmp) / "state.db"))
            self.assertEqual(store.processed_trash_hide_media_ids(["media-1"]), set())

            store.save_trash_hide_result(
                {
                    "media_id": "media-1",
                    "openlist_path": "/115/其他/Old",
                    "hide_path": "/115/其他",
                    "hide_pattern": r"^Old$",
                    "status": "hidden",
                    "reason": "meta_hide",
                }
            )

            self.assertEqual(store.processed_trash_hide_media_ids(["media-1", "media-2"]), {"media-1"})

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


class TaskStateMachineTest(unittest.TestCase):
    def test_task_state_machine_marks_first_running_stage_failed(self):
        from pipeline.task_state import TASK_STATE

        task = {
            "openlist_clean_status": "success",
            "msg_scan_status": "running",
            "msg_scrape_status": "running",
        }

        stage = TASK_STATE.mark_running_sync_stage_failed(task, "scan failed")

        self.assertEqual(stage, "msg_scan_status")
        self.assertEqual(task["msg_scan_status"], "failed")
        self.assertEqual(task["msg_scrape_status"], "running")

    def test_task_state_machine_centralizes_offline_and_sync_checks(self):
        from pipeline.task_state import TASK_STATE

        self.assertTrue(TASK_STATE.is_offline_active({"status_name": "submitted"}))
        self.assertTrue(TASK_STATE.is_offline_final({"status_name": "cancelled"}))
        self.assertTrue(TASK_STATE.can_refresh_offline_status({"info_hash": "ABC", "status_name": "downloading"}))
        self.assertFalse(TASK_STATE.can_refresh_offline_status({"info_hash": "ABC", "status_name": "cancelled"}))
        self.assertTrue(TASK_STATE.can_cancel_offline_task({"status_name": "downloading"}))
        self.assertFalse(TASK_STATE.can_cancel_offline_task({"status_name": "success"}))
        self.assertTrue(TASK_STATE.stage_is_complete("skipped"))
        self.assertTrue(
            TASK_STATE.can_retry_msg_sync(
                {"info_hash": "ABC", "status_name": "success", "msg_sync_status": "failed", "msg_scrape_status": "running"}
            )
        )
        self.assertEqual(
            TASK_STATE.task_list_priority({"info_hash": "ABC", "status_name": "success", "msg_sync_status": "failed"}),
            0,
        )
        self.assertEqual(TASK_STATE.task_list_priority({"info_hash": "ABC", "status_name": "downloading"}), 1)
        self.assertEqual(TASK_STATE.task_list_priority({"info_hash": "ABC", "status_name": "cancelled"}), 2)
        self.assertEqual(TASK_STATE.task_list_priority({"info_hash": "ABC", "status_name": "success"}), 3)
        self.assertFalse(
            TASK_STATE.should_show_syncing_status(
                {"status_name": "success", "msg_sync_status": "running"},
                msg_enabled=True,
            )
        )


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

    def test_telegram_api_set_my_commands_uses_set_my_commands_endpoint(self):
        from pipeline.bot import BOT_COMMANDS, TelegramApi

        class FakeTelegramTransport:
            def __init__(self):
                self.calls = []

            def request(self, url, payload, timeout=None):
                self.calls.append({"url": url, "payload": payload, "timeout": timeout})
                return {"ok": True}

        transport = FakeTelegramTransport()
        api = TelegramApi("token", transport=transport, timeout=12)

        api.set_my_commands(BOT_COMMANDS)

        self.assertEqual(
            transport.calls,
            [
                {
                    "url": "https://api.telegram.org/bottoken/setMyCommands",
                    "payload": {"commands": BOT_COMMANDS},
                    "timeout": 12,
                }
            ],
        )

    def test_configure_bot_commands_sets_current_command_descriptions(self):
        from pipeline.bot import BOT_COMMANDS, BotConfig, CandidateStore, TelegramBot

        with tempfile.TemporaryDirectory() as tmp:
            store = CandidateStore(str(Path(tmp) / "state.db"))
            telegram = FakeTelegram()
            service = FakeBotService()
            bot = TelegramBot(BotConfig("token", {700656624}, store.db_path), telegram, store, service)

            bot.configure_bot_commands()

        self.assertEqual(telegram.commands, [BOT_COMMANDS])
        command_names = [item["command"] for item in BOT_COMMANDS]
        self.assertIn("subtitle_report", command_names)
        self.assertNotIn("subtitle_backfill", command_names)

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
        self.assertIn("/diag <info_hash|media_id>", telegram.messages[0]["text"])
        self.assertIn("/migrate <关键词>", telegram.messages[0]["text"])
        self.assertIn("搜索统计会显示来源、耗时、返回/展示数量和 LLM 重排状态", telegram.messages[0]["text"])

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

    def test_message_search_shows_bt4g_retry_when_bt4g_was_skipped(self):
        from pipeline.bot import BotConfig, CandidateStore, TelegramBot
        from pipeline.search_stats import SearchResultList

        with tempfile.TemporaryDirectory() as tmp:
            store = CandidateStore(str(Path(tmp) / "state.db"))
            telegram = FakeTelegram()
            service = FakeBotService(
                search_results=SearchResultList(
                    [{"title": "Sintel Knaben", "download_uri": "magnet:?xt=urn:btih:AAA", "rank": 1, "indexer": "Knaben"}],
                    metadata={
                        "source_count": 2,
                        "raw_count": 1,
                        "selected_count": 1,
                        "sources": [
                            {"source": "Knaben", "status": "success", "result_count": 1},
                            {"source": "BT4G", "status": "skipped", "result_count": 0},
                        ],
                    },
                )
            )
            bot = TelegramBot(BotConfig("token", {700656624}, store.db_path), telegram, store, service)

            bot.handle_update({"message": {"chat": {"id": 9001}, "from": {"id": 700656624}, "text": "sintel"}})

        markup = json.dumps(telegram.messages[0]["reply_markup"], ensure_ascii=False)
        self.assertIn("bt4g_search", markup)
        self.assertIn("BT4G", markup)

    def test_message_search_hides_bt4g_retry_when_bt4g_result_is_present(self):
        from pipeline.bot import BotConfig, CandidateStore, TelegramBot
        from pipeline.search_stats import SearchResultList

        with tempfile.TemporaryDirectory() as tmp:
            store = CandidateStore(str(Path(tmp) / "state.db"))
            telegram = FakeTelegram()
            service = FakeBotService(
                search_results=SearchResultList(
                    [{"title": "Sintel BT4G", "download_uri": "magnet:?xt=urn:btih:BBB", "rank": 1, "indexer": "BT4G"}],
                    metadata={
                        "source_count": 1,
                        "raw_count": 1,
                        "selected_count": 1,
                        "sources": [{"source": "BT4G", "status": "success", "result_count": 1}],
                    },
                )
            )
            bot = TelegramBot(BotConfig("token", {700656624}, store.db_path), telegram, store, service)

            bot.handle_update({"message": {"chat": {"id": 9001}, "from": {"id": 700656624}, "text": "sintel"}})

        markup = json.dumps(telegram.messages[0]["reply_markup"], ensure_ascii=False)
        self.assertNotIn("bt4g_search", markup)

    def test_message_search_shows_manual_llm_rerank_button_when_enabled(self):
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
            config = BotConfig("token", {700656624}, store.db_path, llm_search_rerank_enabled=True, llm_api_key="llm-key")
            bot = TelegramBot(config, telegram, store, service)

            bot.handle_update({"message": {"chat": {"id": 9001}, "from": {"id": 700656624}, "text": "sintel"}})

        buttons = telegram.messages[0]["reply_markup"]["inline_keyboard"]
        self.assertEqual(service.rerank_calls, [])
        self.assertTrue(any(row[0]["text"] == "LLM优选" and row[0]["callback_data"].startswith("llm_rerank:") for row in buttons))

    def test_llm_rerank_callback_updates_same_search_message(self):
        from pipeline.bot import BotConfig, CandidateStore, TelegramBot

        with tempfile.TemporaryDirectory() as tmp:
            store = CandidateStore(str(Path(tmp) / "state.db"))
            telegram = FakeTelegram()
            service = FakeBotService(
                search_results=[
                    {"title": "Sintel 720p", "download_uri": "magnet:?xt=urn:btih:AAA", "rank": 1},
                    {"title": "Sintel 1080p", "download_uri": "magnet:?xt=urn:btih:BBB", "rank": 2},
                ],
            )
            config = BotConfig("token", {700656624}, store.db_path, llm_search_rerank_enabled=True, llm_api_key="llm-key")
            bot = TelegramBot(config, telegram, store, service)

            bot.handle_update({"message": {"chat": {"id": 9001}, "from": {"id": 700656624}, "text": "sintel"}})
            first_session = store.find_search_session_by_candidate(1)
            service.rerank_results = [
                {
                    "_candidate_id": 2,
                    "title": "Sintel 1080p",
                    "download_uri": "magnet:?xt=urn:btih:BBB",
                    "rank": 1,
                },
                {
                    "_candidate_id": 1,
                    "title": "Sintel 720p",
                    "download_uri": "magnet:?xt=urn:btih:AAA",
                    "rank": 2,
                },
            ]

            bot.handle_update(
                {
                    "callback_query": {
                        "id": "cb-llm",
                        "from": {"id": 700656624},
                        "message": {"chat": {"id": 9001}, "message_id": 1001},
                        "data": "llm_rerank:%s" % first_session["id"],
                    }
                }
            )

            session = store.load_search_session(first_session["id"])
            candidate_2_rank = store.load_candidate(2)["candidate"]["rank"]
            candidate_1_rank = store.load_candidate(1)["candidate"]["rank"]

        self.assertEqual(telegram.answers[-1], {"callback_query_id": "cb-llm", "text": "正在 LLM 优选"})
        self.assertEqual(service.rerank_calls, [("sintel", "movie", ["Sintel 720p", "Sintel 1080p"])])
        self.assertEqual(session["candidate_ids"], [2, 1])
        self.assertEqual(candidate_2_rank, 1)
        self.assertEqual(candidate_1_rank, 2)
        self.assertEqual(len(telegram.edits), 1)
        self.assertEqual(telegram.edits[0]["chat_id"], 9001)
        self.assertEqual(telegram.edits[0]["message_id"], 1001)
        self.assertIn("1. Sintel 1080p", telegram.edits[0]["text"])
        self.assertIn("LLM重排成功", telegram.edits[0]["text"])

    def test_llm_rerank_metadata_replaces_previous_llm_timing(self):
        from pipeline.bot import BotConfig, CandidateStore, TelegramBot

        with tempfile.TemporaryDirectory() as tmp:
            store = CandidateStore(str(Path(tmp) / "state.db"))
            bot = TelegramBot(
                BotConfig("token", {700656624}, store.db_path, llm_search_rerank_enabled=True, llm_api_key="llm-key"),
                FakeTelegram(),
                store,
                FakeBotService(),
            )

            metadata = bot._search_metadata_with_llm_rerank(
                {
                    "total_ms": 1500,
                    "sources": [
                        {"source": "Indexer", "status": "success", "result_count": 10, "duration_ms": 1000},
                        {"source": "LLM rerank", "status": "timeout", "duration_ms": 500, "phase": "llm_rerank"},
                    ],
                },
                "success",
                0.2,
                result_count=5,
            )

        self.assertEqual(metadata["total_ms"], 1200)
        self.assertEqual([source["source"] for source in metadata["sources"]], ["Indexer", "LLM rerank"])
        self.assertEqual(metadata["sources"][-1]["status"], "success")

    def test_message_search_shows_source_timing_summary(self):
        from pipeline.bot import BotConfig, CandidateStore, TelegramBot
        from pipeline.search_stats import SearchResultList

        with tempfile.TemporaryDirectory() as tmp:
            store = CandidateStore(str(Path(tmp) / "state.db"))
            telegram = FakeTelegram()
            service = FakeBotService(
                search_results=SearchResultList(
                    [{"title": "Sintel 720p", "download_uri": "magnet:?xt=urn:btih:AAA", "rank": 1, "seeders": 10}],
                    metadata={
                        "source_count": 2,
                        "total_ms": 1234,
                        "raw_count": 7,
                        "selected_count": 1,
                        "failed_count": 1,
                        "settings": {"llm_rerank_enabled": True},
                        "sources": [
                            {"source": "Indexer", "status": "success", "result_count": 7, "duration_ms": 900},
                            {
                                "source": "LLM rerank",
                                "status": "success",
                                "result_count": 1,
                                "duration_ms": 321,
                                "phase": "llm_rerank",
                            },
                        ],
                    },
                )
            )
            bot = TelegramBot(BotConfig("token", {700656624}, store.db_path), telegram, store, service)

            bot.handle_update({"message": {"chat": {"id": 9001}, "from": {"id": 700656624}, "text": "sintel"}})

            session = store.find_search_session_by_candidate(1)

        self.assertIn("搜索统计：来源2个，耗时1.2s，返回7条，展示1条，失败1个", telegram.messages[0]["text"])
        self.assertIn("LLM重排成功0.3s", telegram.messages[0]["text"])
        self.assertEqual(session["metadata"]["source_count"], 2)

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

    def test_callback_bt4g_search_sends_separate_bt4g_result_message(self):
        from pipeline.bot import BotConfig, CandidateStore, TelegramBot
        from pipeline.search_stats import SearchResultList

        with tempfile.TemporaryDirectory() as tmp:
            store = CandidateStore(str(Path(tmp) / "state.db"))
            telegram = FakeTelegram()
            service = FakeBotService(
                search_results=SearchResultList(
                    [{"title": "Sintel Knaben", "download_uri": "magnet:?xt=urn:btih:AAA", "rank": 1, "indexer": "Knaben"}],
                    metadata={
                        "source_count": 2,
                        "raw_count": 1,
                        "selected_count": 1,
                        "sources": [
                            {"source": "Knaben", "status": "success", "result_count": 1},
                            {"source": "BT4G", "status": "skipped", "result_count": 0},
                        ],
                    },
                ),
                bt4g_search_results=[
                    {"title": "Sintel BT4G", "download_uri": "magnet:?xt=urn:btih:BBB", "rank": 1, "seeders": 2, "indexer": "BT4G"}
                ],
            )
            bot = TelegramBot(BotConfig("token", {700656624}, store.db_path), telegram, store, service)

            bot.handle_update({"message": {"chat": {"id": 9001}, "from": {"id": 700656624}, "text": "sintel"}})
            bt4g_button = next(
                button
                for row in telegram.messages[0]["reply_markup"]["inline_keyboard"]
                for button in row
                if button["callback_data"].startswith("bt4g_search:")
            )

            bot.handle_update(
                {
                    "callback_query": {
                        "id": "cb-bt4g-search",
                        "from": {"id": 700656624},
                        "message": {"chat": {"id": 9001}, "message_id": 701},
                        "data": bt4g_button["callback_data"],
                    }
                }
            )

            self.assertEqual(service.search_calls, [("sintel", "movie", 100)])
            self.assertEqual(service.bt4g_search_calls, [("sintel", 100)])
            self.assertEqual(telegram.answers[-1], {"callback_query_id": "cb-bt4g-search", "text": "正在补查 BT4G"})
            self.assertEqual(len(telegram.messages), 2)
            self.assertIn("BT4G搜索结果：sintel", telegram.messages[1]["text"])
            self.assertNotEqual(telegram.messages[0]["text"], telegram.messages[1]["text"])
            self.assertNotIn("bt4g_search", json.dumps(telegram.messages[1]["reply_markup"], ensure_ascii=False))
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
            self.assertEqual(telegram.answers, [{"callback_query_id": "cb1", "text": "正在处理入库"}])
            self.assertEqual(telegram.deletes, [{"chat_id": 9001, "message_id": 502}])
            self.assertIn("BBB", telegram.messages[0]["text"])
            self.assertIn("入库目录：成人库", telegram.messages[0]["text"])
            self.assertEqual(store.load_task("BBB")["task"]["status_name"], "submitted")
            self.assertEqual(store.load_task("BBB")["task"]["telegram_status_message_id"], 1001)
            self.assertEqual(store.load_task("BBB")["category"], "adult")

    def test_callback_submit_claims_candidate_before_115_submit(self):
        from pipeline.bot import BotConfig, CandidateStore, TelegramBot

        with tempfile.TemporaryDirectory() as tmp:
            store = CandidateStore(str(Path(tmp) / "state.db"))
            candidate_id = store.save_candidate(
                user_id=700656624,
                chat_id=9001,
                category="adult",
                query="cawd-773",
                candidate={"title": "CAWD-773", "download_uri": "magnet:?xt=urn:btih:D00D7132F75BEB644A19E6A1CC011AA3523CF233", "rank": 1},
            )
            telegram = FakeTelegram()
            service = FakeBotService(submit_response={"state": True, "tasks": []})
            bot = TelegramBot(BotConfig("token", {700656624}, store.db_path), telegram, store, service)
            update = {
                "callback_query": {
                    "id": "cb1",
                    "from": {"id": 700656624},
                    "message": {"chat": {"id": 9001}, "message_id": 502},
                    "data": "submit:adult:%s" % candidate_id,
                }
            }

            bot.handle_update(update)
            update["callback_query"]["id"] = "cb2"
            bot.handle_update(update)

            self.assertEqual(service.submit_calls, [("adult", "magnet:?xt=urn:btih:D00D7132F75BEB644A19E6A1CC011AA3523CF233")])
            self.assertEqual([item["text"] for item in telegram.answers], ["正在处理入库", "正在处理入库"])

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
            self.assertEqual(telegram.answers, [{"callback_query_id": "cb1", "text": "正在处理入库"}])
            self.assertIn("重复入库拦截", telegram.edits[0]["text"])
            self.assertIn("判定：强重复", telegram.edits[0]["text"])
            self.assertIn("相同info_hash", telegram.edits[0]["text"])
            self.assertIn("命中规则：info_hash", telegram.edits[0]["text"])
            self.assertIn("当前info_hash：BBB", telegram.edits[0]["text"])
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
                {
                    "info_hash": "OLDHASH",
                    "status_name": "success",
                    "msg_sync_status": "success",
                    "msg_scrape_status": "success",
                    "openlist_adult_code": "SSIS-450",
                },
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

    def test_callback_submit_ignores_failed_local_duplicate_record(self):
        from pipeline.bot import BotConfig, CandidateStore, TelegramBot

        with tempfile.TemporaryDirectory() as tmp:
            store = CandidateStore(str(Path(tmp) / "state.db"))
            store.save_task(700656624, 9001, "movie", "Failed Sintel", {"info_hash": "BBB", "status_name": "failed"})
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

            self.assertEqual(service.submit_calls, [("movie", "magnet:?xt=urn:btih:BBB")])
            self.assertEqual(telegram.edits, [])

    def test_callback_submit_ignores_unsynced_success_local_duplicate_record(self):
        from pipeline.bot import BotConfig, CandidateStore, TelegramBot

        with tempfile.TemporaryDirectory() as tmp:
            store = CandidateStore(str(Path(tmp) / "state.db"))
            store.save_task(
                700656624,
                9001,
                "movie",
                "Unsynced Sintel",
                {"info_hash": "BBB", "status_name": "success", "msg_sync_status": "failed"},
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

            self.assertEqual(service.submit_calls, [("movie", "magnet:?xt=urn:btih:BBB")])
            self.assertEqual(telegram.edits, [])

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
            self.assertIn("命中规则：成人番号", telegram.edits[0]["text"])
            self.assertIn("命中值：SSIS-450", telegram.edits[0]["text"])
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
                    "identity_type": "title_query",
                    "identity_value": "sintel",
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
            self.assertIn("判定：弱重复", telegram.edits[0]["text"])
            self.assertIn("命中规则：标题查询", telegram.edits[0]["text"])
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

    def test_subtitle_backfill_command_is_removed(self):
        from pipeline.bot import BotConfig, CandidateStore, TelegramBot

        with tempfile.TemporaryDirectory() as tmp:
            store = CandidateStore(str(Path(tmp) / "state.db"))
            telegram = FakeTelegram()
            service = FakeBotService()
            bot = TelegramBot(
                BotConfig("token", {700656624}, store.db_path, task_message_edit_min_interval_seconds=0, subtitle_backfill_default_limit=3),
                telegram,
                store,
                service,
            )

            bot.handle_update({"message": {"chat": {"id": 9001}, "from": {"id": 700656624}, "text": "/subtitle_backfill"}})

            self.assertEqual(service.subtitle_backfill_calls, [])
            self.assertEqual(service.subtitle_report_calls, 0)
            self.assertIn("这个命令不再作为搜索入口", telegram.messages[0]["text"])

    def test_subtitle_report_bulk_buttons_run_backfill_and_retry(self):
        from pipeline.bot import BotConfig, CandidateStore, TelegramBot

        class ImmediateThread:
            def __init__(self, target=None, args=(), kwargs=None, daemon=None):
                self.target = target
                self.args = args
                self.kwargs = kwargs or {}
                self.daemon = daemon

            def start(self):
                self.target(*self.args, **self.kwargs)

        with tempfile.TemporaryDirectory() as tmp:
            store = CandidateStore(str(Path(tmp) / "state.db"))
            telegram = FakeTelegram()
            service = FakeBotService(
                subtitle_backfill_response={
                    "status": "success",
                    "limit": 3,
                    "scanned": 4,
                    "attempted": 3,
                    "with_subtitles": 3,
                    "pending": 1,
                    "matched": 2,
                    "cached": 1,
                    "previous": 0,
                    "not_found": 0,
                    "failed": 0,
                    "skipped": 1,
                    "recent": [{"media_id": "media-1", "code": "SSIS-218", "status": "success", "source": "assrt", "title": "SSIS-218"}],
                    "current": {},
                },
                subtitle_report_response={
                    "total": 3,
                    "with_subtitles": 1,
                    "pending": 2,
                    "untried": 1,
                    "not_found": 1,
                    "failed": 0,
                    "no_code": 0,
                    "success_missing_cache": 0,
                    "unknown": 0,
                    "buckets": {
                        "pending": [
                            {"media_id": "media-2", "title": "MIDE-882", "code": "MIDE-882", "status": "untried", "status_label": "未尝试"},
                            {"media_id": "media-3", "title": "SSIS-218", "code": "SSIS-218", "status": "not_found", "status_label": "未找到"},
                        ],
                        "cached": [{"media_id": "media-1", "title": "SSIS-001", "code": "SSIS-001", "status": "cached", "status_label": "已补"}],
                        "untried": [{"media_id": "media-2", "title": "MIDE-882", "code": "MIDE-882", "status": "untried", "status_label": "未尝试"}],
                        "not_found": [{"media_id": "media-3", "title": "SSIS-218", "code": "SSIS-218", "status": "not_found", "status_label": "未找到"}],
                        "failed": [],
                        "no_code": [],
                    },
                },
            )
            bot = TelegramBot(
                BotConfig("token", {700656624}, store.db_path, task_message_edit_min_interval_seconds=0, subtitle_backfill_default_limit=3),
                telegram,
                store,
                service,
            )

            bot.handle_update({"message": {"chat": {"id": 9001}, "from": {"id": 700656624}, "text": "/subtitle_report"}})
            operation_row = telegram.messages[0]["reply_markup"]["inline_keyboard"][0]
            self.assertEqual(operation_row[0]["callback_data"], "subtitle_backfill_confirm:3")
            self.assertEqual(operation_row[1]["callback_data"], "subtitle_backfill_retry:3")

            with patch("pipeline.bot.threading.Thread", ImmediateThread):
                bot.handle_update(
                    {
                        "callback_query": {
                            "id": "cb-subtitle-backfill",
                            "from": {"id": 700656624},
                            "message": {"chat": {"id": 9001}, "message_id": 1001},
                            "data": operation_row[0]["callback_data"],
                        }
                    }
                )
                bot.handle_update(
                    {
                        "callback_query": {
                            "id": "cb-subtitle-backfill-retry",
                            "from": {"id": 700656624},
                            "message": {"chat": {"id": 9001}, "message_id": 1001},
                            "data": operation_row[1]["callback_data"],
                        }
                    }
                )

        self.assertEqual(service.subtitle_backfill_calls, [(3, False), (3, True)])
        self.assertEqual(telegram.answers[-2], {"callback_query_id": "cb-subtitle-backfill", "text": "开始补齐字幕"})
        self.assertEqual(telegram.answers[-1], {"callback_query_id": "cb-subtitle-backfill-retry", "text": "开始重试字幕补齐"})
        self.assertIn("成人库字幕补齐报表", telegram.edits[-1]["text"])
        self.assertIn("成人库字幕补齐：已完成", telegram.edits[-1]["text"])

    def test_subtitle_report_command_shows_counts_and_switches_bucket(self):
        from pipeline.bot import BotConfig, CandidateStore, TelegramBot

        with tempfile.TemporaryDirectory() as tmp:
            store = CandidateStore(str(Path(tmp) / "state.db"))
            telegram = FakeTelegram()
            service = FakeBotService()
            bot = TelegramBot(
                BotConfig("token", {700656624}, store.db_path),
                telegram,
                store,
                service,
            )

            bot.handle_update({"message": {"chat": {"id": 9001}, "from": {"id": 700656624}, "text": "/subtitle_report"}})

            self.assertEqual(service.subtitle_report_calls, 1)
            self.assertIn("成人库字幕补齐报表", telegram.messages[0]["text"])
            self.assertIn("总数：2", telegram.messages[0]["text"])
            self.assertIn("已补字幕：1", telegram.messages[0]["text"])
            self.assertIn("待补：1", telegram.messages[0]["text"])
            self.assertIn("列表：待补", telegram.messages[0]["text"])
            keyboard = telegram.messages[0]["reply_markup"]["inline_keyboard"]
            self.assertEqual(keyboard[0][0]["callback_data"], "subtitle_backfill_confirm:20")
            self.assertEqual(keyboard[1][1]["callback_data"], "subtitle_report:cached:0")
            self.assertEqual(keyboard[3][0]["text"], "补齐 1")
            self.assertTrue(keyboard[3][0]["callback_data"].startswith("sub1:pending:0:media-2"))

            bot.handle_update(
                {
                    "callback_query": {
                        "id": "cb-subtitle-report",
                        "from": {"id": 700656624},
                        "message": {"chat": {"id": 9001}, "message_id": 1001},
                        "data": "subtitle_report:cached:0",
                    }
                }
            )

        self.assertEqual(service.subtitle_report_calls, 2)
        self.assertEqual(telegram.answers[-1], {"callback_query_id": "cb-subtitle-report", "text": "正在刷新字幕统计"})
        self.assertIn("列表：已补", telegram.edits[-1]["text"])
        self.assertIn("SSIS-218", telegram.edits[-1]["text"])

    def test_subtitle_report_single_button_runs_one_backfill(self):
        from pipeline.bot import BotConfig, CandidateStore, TelegramBot

        class ImmediateThread:
            def __init__(self, target=None, args=(), kwargs=None, daemon=None):
                self.target = target
                self.args = args
                self.kwargs = kwargs or {}
                self.daemon = daemon

            def start(self):
                self.target(*self.args, **self.kwargs)

        with tempfile.TemporaryDirectory() as tmp:
            store = CandidateStore(str(Path(tmp) / "state.db"))
            telegram = FakeTelegram()
            service = FakeBotService(
                subtitle_backfill_one_response={
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
                    "recent": [{"media_id": "media-2", "code": "MIDE-882", "status": "success", "source": "subtitlecat", "title": "MIDE-882"}],
                    "current": {},
                }
            )
            bot = TelegramBot(
                BotConfig("token", {700656624}, store.db_path, task_message_edit_min_interval_seconds=0),
                telegram,
                store,
                service,
            )

            bot.handle_update({"message": {"chat": {"id": 9001}, "from": {"id": 700656624}, "text": "/subtitle_report"}})
            single_button = next(
                button
                for row in telegram.messages[0]["reply_markup"]["inline_keyboard"]
                for button in row
                if button["callback_data"].startswith("sub1:")
            )

            with patch("pipeline.bot.threading.Thread", ImmediateThread):
                bot.handle_update(
                    {
                        "callback_query": {
                            "id": "cb-subtitle-one",
                            "from": {"id": 700656624},
                            "message": {"chat": {"id": 9001}, "message_id": 1001},
                            "data": single_button["callback_data"],
                        }
                    }
                )

        self.assertEqual(service.subtitle_backfill_one_calls, [("media-2", False)])
        self.assertEqual(service.subtitle_report_calls, 2)
        self.assertEqual(telegram.answers[-1], {"callback_query_id": "cb-subtitle-one", "text": "开始补齐单个字幕"})
        self.assertIn("成人库字幕补齐报表", telegram.edits[-1]["text"])
        self.assertIn("成人库字幕补齐：已完成", telegram.edits[-1]["text"])
        self.assertIn("MIDE-882", telegram.edits[-1]["text"])

    def test_subtitle_report_single_retry_button_retries_previous_attempt(self):
        from pipeline.bot import BotConfig, CandidateStore, TelegramBot

        class ImmediateThread:
            def __init__(self, target=None, args=(), kwargs=None, daemon=None):
                self.target = target
                self.args = args
                self.kwargs = kwargs or {}
                self.daemon = daemon

            def start(self):
                self.target(*self.args, **self.kwargs)

        report = {
            "total": 1,
            "with_subtitles": 0,
            "pending": 1,
            "untried": 0,
            "not_found": 1,
            "failed": 0,
            "no_code": 0,
            "success_missing_cache": 0,
            "unknown": 0,
            "buckets": {
                "pending": [{"media_id": "media-3", "title": "SSIS-218", "code": "SSIS-218", "status": "not_found", "status_label": "未找到"}],
                "cached": [],
                "untried": [],
                "not_found": [{"media_id": "media-3", "title": "SSIS-218", "code": "SSIS-218", "status": "not_found", "status_label": "未找到"}],
                "failed": [],
                "no_code": [],
            },
        }
        with tempfile.TemporaryDirectory() as tmp:
            store = CandidateStore(str(Path(tmp) / "state.db"))
            telegram = FakeTelegram()
            service = FakeBotService(subtitle_report_response=report)
            bot = TelegramBot(
                BotConfig("token", {700656624}, store.db_path, task_message_edit_min_interval_seconds=0),
                telegram,
                store,
                service,
            )

            bot.handle_update({"message": {"chat": {"id": 9001}, "from": {"id": 700656624}, "text": "/subtitle_report"}})
            retry_button = next(
                button
                for row in telegram.messages[0]["reply_markup"]["inline_keyboard"]
                for button in row
                if button["callback_data"].startswith("sub1r:")
            )

            with patch("pipeline.bot.threading.Thread", ImmediateThread):
                bot.handle_update(
                    {
                        "callback_query": {
                            "id": "cb-subtitle-one-retry",
                            "from": {"id": 700656624},
                            "message": {"chat": {"id": 9001}, "message_id": 1001},
                            "data": retry_button["callback_data"],
                        }
                    }
                )

        self.assertEqual(retry_button["text"], "重试 1")
        self.assertEqual(service.subtitle_backfill_one_calls, [("media-3", True)])
        self.assertEqual(telegram.answers[-1], {"callback_query_id": "cb-subtitle-one-retry", "text": "开始重试单个字幕"})

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
            bot = TelegramBot(
                BotConfig(
                    "token",
                    {700656624},
                    store.db_path,
                    msg_enabled=True,
                    task_message_edit_min_interval_seconds=0,
                ),
                telegram,
                store,
                service,
            )

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
            self.assertTrue(any("OpenList隐藏：进行中" in text for text in texts))
            self.assertTrue(any("OpenList隐藏：已完成（2 个）" in text for text in texts))
            self.assertTrue(any("番号格式化：进行中" in text for text in texts))
            self.assertTrue(any("番号格式化：已完成（MIDA-304）" in text for text in texts))
            self.assertTrue(any("MSG扫描：进行中" in text for text in texts))
            self.assertTrue(any("MSG刮削：进行中" in text for text in texts))
            self.assertIn("MSG同步：已完成", texts[-1])

    def test_status_callback_throttles_nonfinal_progress_message_edits(self):
        from pipeline.bot import BotConfig, CandidateStore, TelegramBot, format_task_status_message

        with tempfile.TemporaryDirectory() as tmp:
            store = CandidateStore(str(Path(tmp) / "state.db"))
            store.save_task(700656624, 9001, "movie", "Sintel", {"info_hash": "ABC", "status_name": "downloading", "percent_done": 5})
            telegram = FakeTelegram()
            service = FakeBotService(
                status_response={"info_hash": "ABC", "status_name": "success", "percent_done": 100, "file_id": "file1"},
                sync_progress=[
                    {"msg_sync_status": "running", "openlist_clean_status": "running"},
                    {"msg_sync_status": "running", "openlist_clean_status": "success", "openlist_cleaned_count": 2},
                    {"msg_sync_status": "running", "msg_scan_status": "running"},
                    {"msg_sync_status": "running", "msg_scrape_status": "running", "msg_media_id": "media-1"},
                ],
                sync_response={"msg_sync_status": "success", "msg_scrape_status": "success", "msg_media_id": "media-1"},
            )
            bot = TelegramBot(
                BotConfig(
                    "token",
                    {700656624},
                    store.db_path,
                    msg_enabled=True,
                    task_message_edit_min_interval_seconds=60,
                ),
                telegram,
                store,
                service,
            )

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
            saved = store.load_task("ABC")["task"]

        self.assertLess(len(telegram.edits), 6)
        self.assertEqual(saved["msg_sync_status"], "success")
        self.assertEqual(saved["msg_media_id"], "media-1")
        self.assertEqual(telegram.edits[-1]["text"], format_task_status_message("Sintel", saved, category="movie"))

    def test_status_callback_skips_when_same_task_is_already_running(self):
        from pipeline.bot import BotConfig, CandidateStore, TelegramBot

        with tempfile.TemporaryDirectory() as tmp:
            store = CandidateStore(str(Path(tmp) / "state.db"))
            store.save_task(700656624, 9001, "movie", "Sintel", {"info_hash": "ABC", "status_name": "downloading", "percent_done": 5})
            telegram = FakeTelegram()
            service = FakeBotService(status_response={"info_hash": "ABC", "status_name": "success", "percent_done": 100})
            bot = TelegramBot(BotConfig("token", {700656624}, store.db_path), telegram, store, service)
            lock = bot._try_acquire_task_lock("ABC")
            try:
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
            finally:
                lock.release()

        self.assertEqual(service.status_calls, [])
        self.assertEqual(telegram.answers, [{"callback_query_id": "cb1", "text": "任务正在处理，请稍后刷新"}])

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

    def test_status_message_shows_msg_match_diagnostics(self):
        from pipeline.bot import format_task_status_message

        task = {
            "info_hash": "ABC",
            "status_name": "success",
            "msg_sync_status": "success",
            "msg_scrape_status": "success",
            "msg_media_id": "media-1",
            "msg_match_mode": "path",
            "msg_match_path": "cloud://openlist/115/电影/Sintel/main.mkv",
        }

        text = format_task_status_message("Sintel", task, category="movie")

        self.assertIn("MSG匹配：路径命中", text)
        self.assertIn("MSG路径：cloud://openlist/115/电影/Sintel/main.mkv", text)

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

    def test_completed_msg_task_shows_subtitle_button_and_matches_from_callback(self):
        from pipeline.bot import BotConfig, CandidateStore, TelegramBot, task_reply_markup

        task = {
            "info_hash": "ABC",
            "status_name": "success",
            "percent_done": 100,
            "msg_sync_status": "success",
            "msg_scrape_status": "success",
            "msg_media_id": "media-1",
        }
        with tempfile.TemporaryDirectory() as tmp:
            store = CandidateStore(str(Path(tmp) / "state.db"))
            store.save_task(700656624, 9001, "adult", "SSIS-218", task)
            telegram = FakeTelegram()
            service = FakeBotService(
                subtitle_response={
                    "subtitle_match_status": "success",
                    "subtitle_match_count": 1,
                    "subtitle_match_source": "assrt",
                    "subtitle_match_filename": "assrt-123.srt",
                }
            )
            bot = TelegramBot(BotConfig("token", {700656624}, store.db_path, msg_enabled=True), telegram, store, service)

            markup = task_reply_markup(task)
            bot.handle_update(
                {
                    "callback_query": {
                        "id": "cb1",
                        "from": {"id": 700656624},
                        "message": {"chat": {"id": 9001}, "message_id": 104},
                        "data": "subtitle:ABC",
                    }
                }
            )
            saved = store.load_task("ABC")["task"]

        self.assertEqual(markup["inline_keyboard"][0][0]["text"], "查找字幕")
        self.assertEqual(markup["inline_keyboard"][0][0]["callback_data"], "subtitle:ABC")
        self.assertEqual(service.subtitle_calls, [("adult", "SSIS-218", "ABC", False)])
        self.assertEqual(saved["subtitle_match_status"], "success")
        self.assertEqual(saved["subtitle_match_source"], "assrt")
        self.assertEqual(telegram.answers, [{"callback_query_id": "cb1", "text": "正在查找字幕"}])
        self.assertIn("字幕匹配：已完成", telegram.edits[-1]["text"])
        self.assertEqual(telegram.messages, [])

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

    def test_recover_active_115_tasks_batches_115_lookup_before_parallel_sync(self):
        from pipeline.bot import BotConfig, CandidateStore, TelegramBot

        class CapturingExecutor:
            instances = []

            def __init__(self, max_workers=None, thread_name_prefix=None):
                self.max_workers = max_workers
                self.thread_name_prefix = thread_name_prefix
                self.submitted = []
                self.__class__.instances.append(self)

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def submit(self, fn, item):
                self.submitted.append(item)

                class DoneFuture:
                    def result(self_inner):
                        return fn(item)

                return DoneFuture()

        with tempfile.TemporaryDirectory() as tmp:
            store = CandidateStore(str(Path(tmp) / "state.db"))
            with patch("pipeline.bot.time.time", return_value=1000):
                store.save_task(
                    700656624,
                    9001,
                    "movie",
                    "Sintel A",
                    {"info_hash": "AAA", "status_name": "downloading", "poll_count": 0, "last_polled_at": 1000},
                )
                store.save_task(
                    700656624,
                    9001,
                    "movie",
                    "Sintel B",
                    {"info_hash": "BBB", "status_name": "downloading", "poll_count": 0, "last_polled_at": 1000},
                )
            telegram = FakeTelegram()
            service = FakeBotService(
                statuses_response={
                    "AAA": {"info_hash": "AAA", "status_name": "success", "percent_done": 100},
                    "BBB": {"info_hash": "BBB", "status_name": "success", "percent_done": 100},
                },
                sync_response={"msg_sync_status": "success", "msg_scrape_status": "success"},
            )
            bot = TelegramBot(BotConfig("token", {700656624}, store.db_path, msg_enabled=True, task_workers=2), telegram, store, service)

            with patch("pipeline.bot.ThreadPoolExecutor", CapturingExecutor):
                count = bot.recover_active_115_tasks_once(now=1002)
            saved_a = store.load_task("AAA")["task"]
            saved_b = store.load_task("BBB")["task"]

        self.assertEqual(count, 2)
        self.assertEqual(len(service.statuses_calls), 1)
        self.assertEqual(service.statuses_calls[0][0], "movie")
        self.assertEqual(set(service.statuses_calls[0][1]), {"AAA", "BBB"})
        self.assertEqual({call[2] for call in service.sync_calls}, {"AAA", "BBB"})
        self.assertEqual(CapturingExecutor.instances[0].max_workers, 2)
        self.assertEqual(len(CapturingExecutor.instances[0].submitted), 2)
        self.assertEqual(saved_a["msg_sync_status"], "success")
        self.assertEqual(saved_b["msg_sync_status"], "success")

    def test_recover_active_115_tasks_skips_locked_task_after_batch_status_lookup(self):
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
            telegram = FakeTelegram()
            service = FakeBotService(statuses_response={"ABC": {"info_hash": "ABC", "status_name": "success", "percent_done": 100}})
            bot = TelegramBot(BotConfig("token", {700656624}, store.db_path, msg_enabled=True), telegram, store, service)
            lock = bot._try_acquire_task_lock("ABC")
            try:
                count = bot.recover_active_115_tasks_once(now=1002)
            finally:
                lock.release()
            saved = store.load_task("ABC")["task"]

        self.assertEqual(count, 0)
        self.assertEqual(len(service.statuses_calls), 1)
        self.assertEqual(service.sync_calls, [])
        self.assertEqual(saved["status_name"], "downloading")
        self.assertEqual(saved["percent_done"], 5)
        self.assertEqual(telegram.messages, [])

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
            bot = TelegramBot(
                BotConfig(
                    "token",
                    {700656624},
                    store.db_path,
                    msg_enabled=True,
                    task_message_edit_min_interval_seconds=0,
                ),
                telegram,
                store,
                service,
            )

            count = bot.recover_active_115_tasks_once(now=1002)
            texts = [edit["text"] for edit in telegram.edits]

        self.assertEqual(count, 1)
        self.assertEqual(telegram.messages, [])
        self.assertTrue(all(edit["message_id"] == 777 for edit in telegram.edits))
        self.assertTrue(any("OpenList隐藏：进行中" in text for text in texts))
        self.assertTrue(any("OpenList隐藏：已完成（2 个）" in text for text in texts))
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

        self.assertIn("OpenList处理：请手动为目标目录添加 Meta Hide，隐藏广告/样片等无效小文件，然后点击重试MSG同步", text)
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

    def test_diag_command_shows_local_task_diagnostics_without_querying_115(self):
        from pipeline.bot import BotConfig, CandidateStore, TelegramBot

        with tempfile.TemporaryDirectory() as tmp:
            store = CandidateStore(str(Path(tmp) / "state.db"))
            store.save_task(
                700656624,
                9001,
                "movie",
                "Sintel",
                {
                    "info_hash": "ABC",
                    "status_name": "success",
                    "percent_done": 100,
                    "name": "Sintel",
                    "msg_sync_status": "success",
                    "msg_scan_status": "success",
                    "msg_scrape_status": "success",
                    "msg_media_id": "media-1",
                    "msg_match_mode": "path",
                    "msg_match_path": "cloud://openlist/115/电影/Sintel/main.mkv",
                    "openlist_clean_target": "/115/电影/Sintel",
                },
            )
            telegram = FakeTelegram()
            service = FakeBotService()
            bot = TelegramBot(BotConfig("token", {700656624}, store.db_path), telegram, store, service)

            bot.handle_update({"message": {"chat": {"id": 9001}, "from": {"id": 700656624}, "text": "/diag ABC"}})

            self.assertEqual(service.status_calls, [])
            self.assertEqual(service.msg_diag_calls, [])
            text = telegram.messages[0]["text"]
            self.assertIn("任务诊断：Sintel", text)
            self.assertIn("MSG匹配：路径命中", text)
            self.assertIn("MSG路径：cloud://openlist/115/电影/Sintel/main.mkv", text)
            self.assertIn("目标路径候选：", text)
            self.assertIn("/115/电影/Sintel", text)

    def test_diag_command_falls_back_to_msg_media_diagnostics(self):
        from pipeline.bot import BotConfig, CandidateStore, TelegramBot

        with tempfile.TemporaryDirectory() as tmp:
            store = CandidateStore(str(Path(tmp) / "state.db"))
            telegram = FakeTelegram()
            service = FakeBotService(
                msg_diag_response={
                    "id": "media-1",
                    "title": "Sintel",
                    "library_id": "library-1",
                    "library_root_id": "root-1",
                    "path": "cloud://openlist/115/电影/Sintel/main.mkv",
                    "size_bytes": 800 * 1024 * 1024,
                    "duration_sec": 3823,
                }
            )
            bot = TelegramBot(BotConfig("token", {700656624}, store.db_path, msg_enabled=True), telegram, store, service)

            bot.handle_update({"message": {"chat": {"id": 9001}, "from": {"id": 700656624}, "text": "/diag media-1"}})

            self.assertEqual(service.msg_diag_calls, ["media-1"])
            text = telegram.messages[0]["text"]
            self.assertIn("MSG媒体诊断：media-1", text)
            self.assertIn("标题：Sintel", text)
            self.assertIn("library_id：library-1", text)
            self.assertIn("root_id：root-1", text)
            self.assertIn("路径：cloud://openlist/115/电影/Sintel/main.mkv", text)
            self.assertIn("时长：3823秒", text)

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


class LlmSearchRerankClientTest(unittest.TestCase):
    def test_rerank_search_candidates_sends_deepseek_json_request_and_reorders(self):
        from pipeline.llm import SearchRerankClient

        class FakeTransport:
            def __init__(self):
                self.calls = []

            def request(self, url, payload, headers=None, timeout=None):
                self.calls.append({"url": url, "payload": payload, "headers": headers, "timeout": timeout})
                return {
                    "choices": [
                        {
                            "message": {
                                "content": json.dumps(
                                    {
                                        "selected_ids": ["c2", "c1"],
                                        "best_id": "c2",
                                        "confidence": 0.95,
                                        "reason": "exact match",
                                    }
                                )
                            }
                        }
                    ]
                }

        transport = FakeTransport()
        client = SearchRerankClient(
            base_url="https://api.deepseek.com/v1",
            api_key="llm-key",
            model="deepseek-v4-flash",
            timeout=3,
            thinking_disabled=True,
            transport=transport,
        )

        ranked = client.rerank_search_candidates(
            "sintel 1080p",
            "movie",
            [
                {"title": "Sintel 720p", "seeders": 90, "rank": 1},
                {"title": "Sintel 1080p", "seeders": 20, "rank": 2},
            ],
            max_candidates=2,
        )

        self.assertEqual([item["title"] for item in ranked], ["Sintel 1080p", "Sintel 720p"])
        self.assertEqual([item["rank"] for item in ranked], [1, 2])
        call = transport.calls[0]
        self.assertEqual(call["url"], "https://api.deepseek.com/v1/chat/completions")
        self.assertEqual(call["headers"]["Authorization"], "Bearer llm-key")
        self.assertEqual(call["timeout"], 3)
        self.assertEqual(call["payload"]["model"], "deepseek-v4-flash")
        self.assertEqual(call["payload"]["response_format"], {"type": "json_object"})
        self.assertEqual(call["payload"]["thinking"], {"type": "disabled"})
        self.assertIn("json", call["payload"]["messages"][0]["content"].lower())
        self.assertNotIn("download_uri", call["payload"]["messages"][1]["content"])

    def test_rerank_search_candidates_rejects_invalid_llm_confidence(self):
        from pipeline.llm import SearchRerankClient

        class FakeTransport:
            def request(self, url, payload, headers=None, timeout=None):
                return {
                    "choices": [
                        {
                            "message": {
                                "content": json.dumps(
                                    {
                                        "selected_ids": ["c1"],
                                        "best_id": "c1",
                                        "confidence": "high",
                                        "reason": "exact match",
                                    }
                                )
                            }
                        }
                    ]
                }

        client = SearchRerankClient(api_key="llm-key", transport=FakeTransport())

        with self.assertRaisesRegex(RuntimeError, "confidence must be numeric"):
            client.rerank_search_candidates(
                "sintel",
                "movie",
                [{"title": "Sintel 720p", "rank": 1}, {"title": "Sintel 1080p", "rank": 2}],
                max_candidates=2,
            )


class PipelineBotServiceTest(unittest.TestCase):
    def test_search_does_not_run_llm_rerank_automatically(self):
        from pipeline.bot import BotConfig, PipelineBotService

        class FakeProwlarrConfig:
            def __init__(self, config_path):
                self.config_path = config_path

            def load_api_key(self):
                return "prowlarr-key-value"

        fake_prowlarr = FakeProwlarr(
            [
                {"title": "Sintel 720p", "seeders": 90, "infoHash": "LOW"},
                {"title": "Sintel 1080p", "seeders": 20, "infoHash": "HIGH"},
            ]
        )

        class FakeReranker:
            calls = []

            def __init__(self, **kwargs):
                self.kwargs = kwargs

            def rerank_search_candidates(self, query, category, candidates, max_candidates=None):
                self.calls.append((query, category, [item["title"] for item in candidates], max_candidates))
                return list(reversed(candidates))

        config = BotConfig(
            "token",
            {700656624},
            "/tmp/state.db",
            llm_search_rerank_enabled=True,
            llm_api_key="llm-key",
            llm_search_rerank_limit=25,
            llm_timeout_seconds=3,
        )

        with patch("pipeline.bot.ProwlarrConfig", FakeProwlarrConfig), patch(
            "pipeline.bot.ProwlarrClient", return_value=fake_prowlarr
        ), patch("pipeline.bot.SearchRerankClient", FakeReranker):
            results = PipelineBotService(config).search("sintel", "movie", limit=10)

        self.assertEqual([item["title"] for item in results], ["Sintel 1080p", "Sintel 720p"])
        self.assertEqual(FakeReranker.calls, [])
        self.assertFalse(any(source.get("phase") == "llm_rerank" for source in results.metadata["sources"]))
        self.assertTrue(results.metadata["settings"]["llm_rerank_enabled"])

    def test_rerank_search_candidates_uses_llm_when_enabled(self):
        from pipeline.bot import BotConfig, PipelineBotService

        class FakeReranker:
            calls = []

            def __init__(self, **kwargs):
                self.kwargs = kwargs

            def rerank_search_candidates(self, query, category, candidates, max_candidates=None):
                self.calls.append((query, category, [item["title"] for item in candidates], max_candidates, self.kwargs))
                ordered = [dict(candidates[1]), dict(candidates[0])]
                for index, item in enumerate(ordered, start=1):
                    item["rank"] = index
                return ordered

        config = BotConfig(
            "token",
            {700656624},
            "/tmp/state.db",
            llm_search_rerank_enabled=True,
            llm_api_key="llm-key",
            llm_search_rerank_limit=25,
            llm_timeout_seconds=3,
        )

        with patch("pipeline.bot.SearchRerankClient", FakeReranker):
            results = PipelineBotService(config).rerank_search_candidates(
                "sintel",
                "movie",
                [
                    {"title": "Sintel 720p", "rank": 1},
                    {"title": "Sintel 1080p", "rank": 2},
                ],
            )

        self.assertEqual([item["title"] for item in results], ["Sintel 1080p", "Sintel 720p"])
        self.assertEqual([item["rank"] for item in results], [1, 2])
        self.assertEqual(FakeReranker.calls[0][0], "sintel")
        self.assertEqual(FakeReranker.calls[0][1], "movie")
        self.assertEqual(FakeReranker.calls[0][3], 25)
        self.assertEqual(FakeReranker.calls[0][4]["api_key"], "llm-key")

    def test_rerank_search_candidates_raises_llm_error(self):
        from pipeline.bot import BotConfig, PipelineBotService

        class FailingReranker:
            def __init__(self, **kwargs):
                pass

            def rerank_search_candidates(self, query, category, candidates, max_candidates=None):
                raise RuntimeError("invalid llm response")

        config = BotConfig(
            "token",
            {700656624},
            "/tmp/state.db",
            llm_search_rerank_enabled=True,
            llm_api_key="llm-key",
        )

        with patch("pipeline.bot.SearchRerankClient", FailingReranker):
            with self.assertRaisesRegex(RuntimeError, "invalid llm response"):
                PipelineBotService(config).rerank_search_candidates(
                    "sintel",
                    "movie",
                    [{"title": "Sintel 720p", "rank": 1}, {"title": "Sintel 1080p", "rank": 2}],
                )

    def test_rerank_search_candidates_times_out(self):
        from concurrent.futures import TimeoutError as FutureTimeoutError

        from pipeline.bot import BotConfig, PipelineBotService

        class FakeReranker:
            def __init__(self, **kwargs):
                pass

            def rerank_search_candidates(self, query, category, candidates, max_candidates=None):
                return list(reversed(candidates))

        class TimeoutFuture:
            def __init__(self):
                self.timeouts = []
                self.callbacks = []
                self.cancel_called = False

            def result(self, timeout=None):
                self.timeouts.append(timeout)
                raise FutureTimeoutError()

            def add_done_callback(self, callback):
                self.callbacks.append(callback)

            def cancel(self):
                self.cancel_called = True
                return False

        class TimeoutExecutor:
            instances = []

            def __init__(self, max_workers=None, thread_name_prefix=None):
                self.max_workers = max_workers
                self.thread_name_prefix = thread_name_prefix
                self.future = TimeoutFuture()
                self.shutdown_args = None
                self.__class__.instances.append(self)

            def submit(self, *args, **kwargs):
                self.submitted = (args, kwargs)
                return self.future

            def shutdown(self, wait=False, cancel_futures=False):
                self.shutdown_args = (wait, cancel_futures)

        config = BotConfig(
            "token",
            {700656624},
            "/tmp/state.db",
            llm_search_rerank_enabled=True,
            llm_api_key="llm-key",
            llm_timeout_seconds=2,
        )

        with patch("pipeline.bot.SearchRerankClient", FakeReranker), patch(
            "pipeline.bot.ThreadPoolExecutor", TimeoutExecutor
        ):
            with self.assertRaises(FutureTimeoutError):
                PipelineBotService(config).rerank_search_candidates(
                    "sintel",
                    "movie",
                    [{"title": "Sintel 720p", "rank": 1}, {"title": "Sintel 1080p", "rank": 2}],
                )

        self.assertEqual(TimeoutExecutor.instances[0].future.timeouts, [2.0])
        self.assertTrue(TimeoutExecutor.instances[0].future.cancel_called)
        self.assertIsNone(TimeoutExecutor.instances[0].shutdown_args)

    def test_rerank_search_candidates_rejects_parallel_run(self):
        from pipeline.bot import BotConfig, LlmRerankBusy, PipelineBotService

        class GuardedExecutor:
            instances = []

            def __init__(self, max_workers=None, thread_name_prefix=None):
                self.max_workers = max_workers
                self.thread_name_prefix = thread_name_prefix
                self.submit_called = False
                self.__class__.instances.append(self)

            def submit(self, *args, **kwargs):
                self.submit_called = True
                raise AssertionError("LLM rerank should not be submitted while a previous rerank is running")

        config = BotConfig(
            "token",
            {700656624},
            "/tmp/state.db",
            llm_search_rerank_enabled=True,
            llm_api_key="llm-key",
            llm_timeout_seconds=2,
        )

        with patch("pipeline.bot.ThreadPoolExecutor", GuardedExecutor):
            service = PipelineBotService(config)
            service._llm_rerank_lock.acquire()
            try:
                with self.assertRaisesRegex(LlmRerankBusy, "previous LLM rerank"):
                    service.rerank_search_candidates(
                        "sintel",
                        "movie",
                        [{"title": "Sintel 720p", "rank": 1}, {"title": "Sintel 1080p", "rank": 2}],
                    )
            finally:
                service._llm_rerank_lock.release()

        self.assertFalse(GuardedExecutor.instances[0].submit_called)

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

    def test_submit_does_not_wait_for_115_task_list_after_add(self):
        from pipeline.bot import BotConfig, PipelineBotService

        class SubmitOnly115Client(Fake115SubmitClient):
            def get_offline_tasks(self, page=1):
                raise AssertionError("submit should not poll task list")

        class SubmitService(PipelineBotService):
            def _call_115(self, category, callback):
                self.fake_115 = SubmitOnly115Client({"state": True, "data": [{"info_hash": "ABC"}]})
                return callback(self.fake_115)

        service = SubmitService(BotConfig("token", {700656624}, "/tmp/state.db"))

        result = service.submit("movie", "magnet:?xt=urn:btih:ABC")

        self.assertEqual(service.fake_115.urls, ["magnet:?xt=urn:btih:ABC"])
        self.assertEqual(result["tasks"][0]["info_hash"], "ABC")
        self.assertNotIn("task_status", result)

    def test_submit_fills_task_identity_from_magnet_when_115_omits_data(self):
        from pipeline.bot import BotConfig, PipelineBotService

        class SubmitService(PipelineBotService):
            def _call_115(self, category, callback):
                self.fake_115 = Fake115SubmitClient({"state": True, "data": [], "message": "ok"})
                return callback(self.fake_115)

        service = SubmitService(BotConfig("token", {700656624}, "/tmp/state.db"))

        result = service.submit("adult", "magnet:?xt=urn:btih:D00D7132F75BEB644A19E6A1CC011AA3523CF233")

        self.assertEqual(result["tasks"], [
            {
                "info_hash": "D00D7132F75BEB644A19E6A1CC011AA3523CF233",
                "state": True,
                "code": None,
                "message": "ok",
                "status_name": "submitted",
            }
        ])
        self.assertEqual(service.fake_115.urls, ["magnet:?xt=urn:btih:D00D7132F75BEB644A19E6A1CC011AA3523CF233"])

    def test_submit_does_not_fill_task_identity_for_rejected_115_response(self):
        from pipeline.bot import BotConfig, PipelineBotService

        class SubmitService(PipelineBotService):
            def _call_115(self, category, callback):
                self.fake_115 = Fake115SubmitClient({"state": False, "code": 400, "message": "invalid url"})
                return callback(self.fake_115)

        service = SubmitService(BotConfig("token", {700656624}, "/tmp/state.db"))

        result = service.submit("adult", "magnet:?xt=urn:btih:D00D7132F75BEB644A19E6A1CC011AA3523CF233")

        self.assertEqual(result["tasks"], [])
        self.assertEqual(result["state"], False)

    def test_submit_fills_task_identity_for_existing_115_task_response(self):
        from pipeline.bot import BotConfig, PipelineBotService

        class SubmitService(PipelineBotService):
            def _call_115(self, category, callback):
                self.fake_115 = Fake115SubmitClient({"state": False, "code": 10008, "message": "任务已存在"})
                return callback(self.fake_115)

        service = SubmitService(BotConfig("token", {700656624}, "/tmp/state.db"))

        result = service.submit("adult", "magnet:?xt=urn:btih:D00D7132F75BEB644A19E6A1CC011AA3523CF233")

        self.assertEqual(result["tasks"][0]["info_hash"], "D00D7132F75BEB644A19E6A1CC011AA3523CF233")
        self.assertEqual(result["tasks"][0]["message"], "任务已存在")

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

    def test_sync_completed_movie_task_applies_unique_clean_scrape_match(self):
        from pipeline.bot import BotConfig, PipelineBotService

        title = "[DBD-Raws][4K_SDR][哥斯拉之终极战役][正片+特典映像][2160P][UHDBDRip][HEVC-10bit][简繁外挂][FLACx3][MKV]"
        media = {
            "id": "media-1",
            "library_id": "d150a96c-b467-4c60-82f1-207ae5949045",
            "title": title,
            "path": "cloud://openlist/115/电影/%s/[DBD-Raws][4K_SDR][Godzilla Final Wars][Ver.A][2160P].mkv" % title,
        }
        fake_msg = FakeMediaStationClient(
            search_response={"data": {"items": [media]}},
            scrape_search_responses={"Godzilla Final Wars": {"items": [{"source": "tmdb", "tmdb_id": 15767, "title": "哥斯拉之终极战役"}]}},
        )

        with patch("pipeline.bot.MediaStationClient", return_value=fake_msg), patch(
            "pipeline.bot.OpenListTokenProvider", return_value=FakeOpenListTokenProvider()
        ), patch("pipeline.bot.OpenListClient", side_effect=lambda url, token: RetryOpenList([])), patch(
            "pipeline.bot.MediaStationDbClient"
        ) as db_cls:
            db_cls.return_value.repair_movie_extras.return_value = {"status": "success", "updated": 0, "media_count": 1}
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
            task = service.sync_completed_task("movie", title, {"info_hash": "ABC", "status_name": "success", "name": title})

        self.assertEqual(fake_msg.scrape_calls, [])
        self.assertEqual(fake_msg.scrape_apply_calls[0][0], "media-1")
        self.assertEqual(task["msg_scrape_mode"], "apply")
        self.assertEqual(task["msg_scrape_query"], "Godzilla Final Wars")

    def test_sync_completed_anime_task_repairs_episode_visibility_after_scrape(self):
        from pipeline.bot import BotConfig, PipelineBotService

        events = []
        fake_msg = FakeMediaStationClient(
            search_response={"data": {"items": [{"id": "media-1", "library_id": "e1333358-17ff-4b90-82f0-663cec26c0df", "title": "Aki Sora"}]}}
        )

        class FakeMsgDb:
            def __init__(self):
                self.calls = []

            def repair_episode_visibility(self, category, media_id=None):
                self.calls.append((category, media_id))
                return {"status": "success", "updated": 3, "media_count": 5, "reason": "repaired"}

        fake_db = FakeMsgDb()

        with patch("pipeline.bot.MediaStationClient", return_value=fake_msg), patch(
            "pipeline.bot.MediaStationDbClient", return_value=fake_db
        ), patch("pipeline.bot.OpenListTokenProvider", return_value=FakeOpenListTokenProvider()), patch(
            "pipeline.bot.OpenListClient", side_effect=lambda url, token: RetryOpenList(events)
        ):
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
                "anime",
                "Aki Sora",
                {"info_hash": "ABC", "status_name": "success", "name": "Aki Sora.mkv"},
            )

        self.assertEqual(fake_msg.scrape_calls, ["media-1"])
        self.assertEqual(fake_db.calls, [("anime", "media-1")])
        self.assertEqual(task["msg_visibility_repair_status"], "success")
        self.assertEqual(task["msg_visibility_repair_updated"], 3)
        self.assertEqual(task["msg_visibility_repair_media_count"], 5)
        self.assertEqual(task["msg_sync_status"], "success")

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
        ), patch("pipeline.bot.OpenListClient", return_value=fake_openlist), patch("pipeline.bot.MediaStationDbClient") as db_cls:
            db_cls.return_value.repair_movie_extras.return_value = {"status": "success", "updated": 1, "media_count": 2}
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
            fake_openlist.meta_hide_calls,
            [
                ("/115/电影/Movie", [r"^trailer\.mp4$", r"^poster\.jpg$", "^Extras$"], True),
                ("/115/电影/Movie/Extras", [r"^sample\.mp4$"], True),
            ],
        )
        self.assertEqual(fake_openlist.source_delete_calls, [])
        self.assertLess(
            events.index(("meta_hide", "/115/电影/Movie", (r"^trailer\.mp4$", r"^poster\.jpg$", "^Extras$"), True)),
            events.index(("scan",)),
        )
        self.assertEqual(task["openlist_clean_status"], "success")
        self.assertEqual(task["openlist_cleaned_count"], 3)
        self.assertEqual(task["openlist_cleaned_bytes"], 30 * 1024 * 1024 + 300 * 1024)
        self.assertEqual(task["msg_sync_status"], "success")

    def test_sync_completed_task_hides_msg_trash_before_mediastation_scan(self):
        from pipeline.bot import BotConfig, CandidateStore, PipelineBotService

        events = []
        fake_openlist = CleaningOpenList(
            {
                "/115/电影": [{"name": "Movie", "is_dir": True, "size": 0}],
                "/115/其他": [{"name": "Old", "is_dir": True, "size": 0}],
            },
            events=events,
        )
        fake_msg = FakeMediaStationClient(
            search_response={"data": {"items": [{"id": "media-1", "library_id": "d150a96c-b467-4c60-82f1-207ae5949045", "title": "Movie"}]}},
            events=events,
        )

        class FakeMsgDb:
            def list_deleted_openlist_media_for_hide(self, limit=100):
                return [
                    {
                        "media_id": "trash-1",
                        "target_openlist_path": "/115/其他/Old",
                        "hide_path": "/115/其他",
                        "hide_pattern": "^Old$",
                    },
                    {
                        "media_id": "trash-2",
                        "target_openlist_path": "/115/其他/Gone",
                        "hide_path": "/115/其他",
                        "hide_pattern": "^Gone$",
                    },
                ]

        with tempfile.TemporaryDirectory() as tmp, patch("pipeline.bot.MediaStationClient", return_value=fake_msg), patch(
            "pipeline.bot.OpenListTokenProvider", return_value=FakeOpenListTokenProvider()
        ), patch("pipeline.bot.OpenListClient", return_value=fake_openlist), patch("pipeline.bot.MediaStationDbClient", return_value=FakeMsgDb()):
            state_db = str(Path(tmp) / "state.db")
            service = PipelineBotService(
                BotConfig(
                    "token",
                    {700656624},
                    state_db,
                    msg_admin_user="admin",
                    msg_admin_password="secret",
                    msg_enabled=True,
                    msg_sync_poll_seconds=0,
                    msg_trash_hide_sync_enabled=True,
                    openlist_pre_scan_clean_enabled=False,
                )
            )
            task = service.sync_completed_task(
                "movie",
                "Movie",
                {"info_hash": "ABC", "status_name": "success", "name": "Movie"},
            )
            processed = CandidateStore(state_db).processed_trash_hide_media_ids(["trash-1", "trash-2"])

        self.assertEqual(fake_openlist.meta_hide_calls, [("/115/其他", ["^Old$"], True)])
        self.assertEqual(fake_openlist.source_delete_calls, [])
        self.assertLess(events.index(("meta_hide", "/115/其他", ("^Old$",), True)), events.index(("scan",)))
        self.assertEqual(processed, {"trash-1", "trash-2"})
        self.assertEqual(task["openlist_trash_hide_status"], "success")
        self.assertEqual(task["openlist_trash_hide_hidden_count"], 1)
        self.assertEqual(task["openlist_trash_hide_skipped_count"], 1)
        self.assertEqual(task["msg_sync_status"], "success")

    def test_repair_msg_movie_extras_writes_openlist_meta_hide(self):
        from pipeline.bot import BotConfig, PipelineBotService

        fake_openlist = CleaningOpenList({})
        with patch("pipeline.bot.MediaStationDbClient") as db_cls:
            db_cls.return_value.repair_movie_extras.return_value = {
                "status": "success",
                "updated": 1,
                "media_count": 2,
                "openlist_hide_path": "/115/movie/Movie",
                "openlist_hide_patterns": [r"^Extras$"],
                "reason": "extras_hidden",
            }
            service = PipelineBotService(BotConfig("token", {700656624}, "/tmp/state.db"))
            result = service._repair_msg_movie_extras("movie", "media-1", fake_openlist)

        self.assertEqual(fake_openlist.meta_hide_calls, [("/115/movie/Movie", [r"^Extras$"], True)])
        self.assertEqual(fake_openlist.source_delete_calls, [])
        self.assertEqual(result["msg_extra_cleanup_status"], "success")
        self.assertEqual(result["msg_extra_cleanup_updated"], 1)
        self.assertEqual(result["msg_extra_cleanup_hidden_count"], 1)

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

        self.assertEqual(fake_openlist.meta_hide_calls, [])
        self.assertEqual(fake_msg.scan_calls, [("d150a96c-b467-4c60-82f1-207ae5949045", "0c1dda42-29ef-4069-b051-c9549a8d4440")])
        self.assertEqual(task["openlist_clean_status"], "failed")
        self.assertIn("target not found", task["openlist_clean_error"])
        self.assertEqual(task["msg_sync_status"], "success")

    def test_sync_completed_task_keeps_scanning_when_openlist_meta_hide_times_out(self):
        from pipeline.bot import BotConfig, PipelineBotService

        class TimeoutCleaningOpenList(CleaningOpenList):
            def upsert_meta_hide(self, path, hide_patterns, h_sub=True):
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
        self.assertEqual(fake_openlist.meta_hide_calls, [("/root/Movie A", [r"^ad\.mp4$"], True)])
        self.assertEqual(fake_openlist.source_delete_calls, [])

    def test_clean_openlist_default_threshold_keeps_episode_videos_and_hides_small_non_episode_videos(self):
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

        self.assertEqual(
            fake_openlist.meta_hide_calls,
            [("/root/Jackie Chan Adventures", [r"^ad\.mp4$", r"^bonus\.mp4$", r"^poster\.jpg$"], True)],
        )
        self.assertEqual(fake_openlist.source_delete_calls, [])
        self.assertEqual(result["openlist_cleaned_count"], 3)
        self.assertEqual(result["openlist_hidden_count"], 3)

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
        self.assertEqual(fake_openlist.meta_hide_calls, [("/root/Movie A", [r"^ad\.mp4$"], True)])
        self.assertEqual(fake_openlist.source_delete_calls, [])

    def test_clean_openlist_movie_pack_hides_extra_directories_without_deleting_them(self):
        from pipeline.bot import clean_openlist_task_media

        fake_openlist = CleaningOpenList(
            {
                "/root": [{"name": "Godzilla Pack", "is_dir": True, "size": 0}],
                "/root/Godzilla Pack": [
                    {"name": "main.mkv", "is_dir": False, "size": 8 * 1024 * 1024 * 1024},
                    {"name": "PV", "is_dir": True, "size": 0},
                    {"name": "特典映像", "is_dir": True, "size": 0},
                    {"name": "图集", "is_dir": True, "size": 0},
                ],
            }
        )

        result = clean_openlist_task_media(
            fake_openlist,
            "/root",
            ["Godzilla Pack"],
            hide_extra_scan_items=True,
        )

        self.assertEqual(result["openlist_hidden_count"], 3)
        self.assertEqual(fake_openlist.meta_hide_calls[0][0], "/root/Godzilla Pack")
        self.assertIn("^PV$", fake_openlist.meta_hide_calls[0][1])
        self.assertIn("^特典映像$", fake_openlist.meta_hide_calls[0][1])
        self.assertIn("^图集$", fake_openlist.meta_hide_calls[0][1])
        self.assertEqual(fake_openlist.source_delete_calls, [])

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
        class FakeSubtitleMatcher:
            def match_task(self, category, title, task, force=False):
                events.append(("subtitle_match", category, title, task.get("msg_media_id"), task.get("openlist_adult_code")))
                return {
                    "subtitle_match_status": "success",
                    "subtitle_match_count": 1,
                    "subtitle_match_source": "assrt",
                    "subtitle_match_filename": "assrt-123.srt",
                }

        with patch("pipeline.bot.MediaStationClient", return_value=fake_msg), patch(
            "pipeline.bot.OpenListTokenProvider", return_value=FakeOpenListTokenProvider()
        ), patch("pipeline.bot.OpenListClient", return_value=fake_openlist), patch(
            "pipeline.bot.build_subtitle_matcher_from_config", return_value=FakeSubtitleMatcher()
        ):
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
                    subtitle_auto_match_enabled=True,
                )
            )
            task = service.sync_completed_task(
                "adult",
                "downloaded folder",
                {"info_hash": "ABC", "status_name": "success", "name": "downloaded folder"},
            )

        self.assertEqual(fake_openlist.meta_hide_calls, [("/115/成人/MIDA-304 - downloaded folder", [r"^ad\.mp4$"], True)])
        self.assertEqual(fake_openlist.source_delete_calls, [])
        self.assertEqual(fake_openlist.rename_calls, [("/115/成人/downloaded folder", "MIDA-304 - downloaded folder")])
        self.assertLess(
            events.index(("rename", "/115/成人/downloaded folder", "MIDA-304 - downloaded folder")),
            events.index(("meta_hide", "/115/成人/MIDA-304 - downloaded folder", (r"^ad\.mp4$",), True)),
        )
        self.assertLess(events.index(("meta_hide", "/115/成人/MIDA-304 - downloaded folder", (r"^ad\.mp4$",), True)), events.index(("scan",)))
        self.assertLess(events.index(("scan",)), events.index(("artwork_repair",)))
        self.assertLess(events.index(("artwork_repair",)), events.index(("subtitle_match", "adult", "downloaded folder", "media-1", "MIDA-304")))
        self.assertEqual(fake_msg.search_calls[0], ("MIDA-304", 20))
        self.assertEqual(fake_msg.artwork_repair_calls, ["media-1"])
        self.assertEqual(task["openlist_adult_format_status"], "success")
        self.assertEqual(task["openlist_adult_code"], "MIDA-304")
        self.assertEqual(task["msg_artwork_repair_status"], "success")
        self.assertEqual(task["msg_artwork_repair_updated"], 1)
        self.assertEqual(task["msg_artwork_repair_fields"], "poster_url")
        self.assertEqual(task["subtitle_match_status"], "success")
        self.assertEqual(task["subtitle_match_source"], "assrt")
        self.assertEqual(task["msg_sync_status"], "success")

    def test_sync_completed_adult_task_hides_secondary_videos_after_format(self):
        import posixpath

        from pipeline.bot import BotConfig, PipelineBotService
        from pipeline.config import category_to_openlist_path

        events = []
        adult_root = category_to_openlist_path("adult")
        old_path = posixpath.join(adult_root, "ssis-218ch")
        new_path = posixpath.join(adult_root, "SSIS-218")
        fake_openlist = CleaningOpenList(
            {
                adult_root: [{"name": "ssis-218ch", "is_dir": True, "size": 0}],
                old_path: [
                    {"name": "ssis-218ch.mp4", "is_dir": False, "size": 5 * 1024 * 1024 * 1024},
                    {"name": "side.mp4", "is_dir": False, "size": 150 * 1024 * 1024},
                ],
            },
            events=events,
        )
        fake_msg = FakeMediaStationClient(
            search_response={"data": {"items": [{"id": "media-1", "library_id": "26768071-73bb-4b5c-85f3-ad0dd84f9fd9", "title": "SSIS-218"}]}},
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
                    openlist_adult_code_format_enabled=True,
                )
            )
            task = service.sync_completed_task(
                "adult",
                "ssis-218ch",
                {"info_hash": "ABC", "status_name": "success", "name": "ssis-218ch"},
            )

        self.assertEqual(fake_openlist.rename_calls, [(old_path, "SSIS-218")])
        self.assertEqual(fake_openlist.meta_hide_calls, [(new_path, [r"^side\.mp4$"], True)])
        self.assertEqual(fake_openlist.source_delete_calls, [])
        self.assertLess(events.index(("rename", old_path, "SSIS-218")), events.index(("meta_hide", new_path, (r"^side\.mp4$",), True)))
        self.assertLess(events.index(("meta_hide", new_path, (r"^side\.mp4$",), True)), events.index(("scan",)))
        self.assertEqual(task["openlist_adult_extra_hide_status"], "success")
        self.assertEqual(task["openlist_adult_extra_hidden_count"], 1)
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

    def test_adult_code_formatting_strips_ch_noise_suffix(self):
        from pipeline.bot import adult_code_formatted_name, adult_code_prefix_matches

        self.assertFalse(adult_code_prefix_matches("ssis-152ch", "SSIS-152"))
        self.assertTrue(adult_code_prefix_matches("SSIS-152", "SSIS-152"))
        self.assertTrue(adult_code_prefix_matches("SSIS-152 - title", "SSIS-152"))
        self.assertEqual(adult_code_formatted_name("SSIS-152", "ssis-152ch"), "SSIS-152")
        self.assertEqual(adult_code_formatted_name("SSIS-152", "SSIS-152CH title"), "SSIS-152 - title")

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

    def test_subtitle_backfill_reads_msg_adult_library_and_skips_cached_or_uncoded_media(self):
        from pipeline.bot import BotConfig, PipelineBotService
        from pipeline.config import category_to_msg_library_root

        class FakeSubtitleCache:
            def list_tracks(self, media_id):
                if media_id == "media-cached":
                    return [{"path": "local-subtitle://media-cached/cached.srt"}]
                return []

        class FakeSubtitleMatcher:
            def __init__(self):
                self.cache = FakeSubtitleCache()
                self.calls = []

            def match_task(self, category, title, task, force=False):
                self.calls.append((category, title, task.get("msg_media_id"), task.get("openlist_adult_code"), force))
                return {
                    "subtitle_match_status": "success",
                    "subtitle_match_count": 1,
                    "subtitle_match_source": "assrt",
                    "subtitle_match_filename": "assrt-123.srt",
                }

        root = category_to_msg_library_root("adult")
        fake_msg = FakeMediaStationClient(
            list_response={
                "data": {
                    "items": [
                        {"id": "media-cached", "library_id": root["library_id"], "title": "SSIS-001"},
                        {"id": "media-no-code", "library_id": root["library_id"], "title": "No Code Title"},
                        {
                            "id": "media-target",
                            "library_id": root["library_id"],
                            "title": "MIDE-882",
                            "path": "cloud://openlist/115/成人/MIDE-882/MIDE-882.mp4",
                        },
                    ]
                }
            }
        )
        matcher = FakeSubtitleMatcher()
        progress = []

        with tempfile.TemporaryDirectory() as tmp, patch("pipeline.bot.MediaStationClient", return_value=fake_msg), patch(
            "pipeline.bot.build_subtitle_matcher_from_config", return_value=matcher
        ), patch("pipeline.bot.OpenListClient", side_effect=AssertionError("OpenList must not be called")), patch(
            "pipeline.bot.Client115", side_effect=AssertionError("115 must not be called")
        ):
            service = PipelineBotService(
                BotConfig(
                    "token",
                    {700656624},
                    str(Path(tmp) / "state.db"),
                    msg_admin_user="admin",
                    msg_admin_password="secret",
                    msg_enabled=True,
                    subtitle_auto_match_enabled=True,
                )
            )
            result = service.subtitle_backfill_adult(limit=1, progress_callback=lambda item, force=False: progress.append((item, force)))

        self.assertEqual(fake_msg.list_calls, [(root["library_id"], 1, 200, 0)])
        self.assertEqual(matcher.calls, [("adult", "MIDE-882", "media-target", "MIDE-882", False)])
        self.assertEqual(result["scanned"], 3)
        self.assertEqual(result["attempted"], 1)
        self.assertEqual(result["matched"], 1)
        self.assertEqual(result["cached"], 1)
        self.assertEqual(result["with_subtitles"], 2)
        self.assertEqual(result["pending"], 1)
        self.assertEqual(result["skipped"], 1)
        self.assertTrue(progress)
        self.assertTrue(progress[-1][1])

    def test_subtitle_backfill_one_reads_single_msg_media_without_openlist_or_115(self):
        from pipeline.bot import BotConfig, PipelineBotService
        from pipeline.config import category_to_msg_library_root

        class FakeSubtitleCache:
            def list_tracks(self, media_id):
                return []

        class FakeSubtitleMatcher:
            def __init__(self):
                self.cache = FakeSubtitleCache()
                self.calls = []

            def match_task(self, category, title, task, force=False):
                self.calls.append((category, title, task.get("msg_media_id"), task.get("openlist_adult_code"), force))
                return {
                    "subtitle_match_status": "success",
                    "subtitle_match_count": 1,
                    "subtitle_match_source": "subtitlecat",
                    "subtitle_match_filename": "MIDE-882.srt",
                }

        root = category_to_msg_library_root("adult")
        fake_msg = FakeMediaStationClient(
            get_response={
                "data": {
                    "id": "media-target",
                    "library_id": root["library_id"],
                    "title": "MIDE-882",
                    "path": "cloud://openlist/115/成人/MIDE-882/MIDE-882.mp4",
                }
            }
        )
        matcher = FakeSubtitleMatcher()

        with tempfile.TemporaryDirectory() as tmp, patch("pipeline.bot.MediaStationClient", return_value=fake_msg), patch(
            "pipeline.bot.build_subtitle_matcher_from_config", return_value=matcher
        ), patch("pipeline.bot.OpenListClient", side_effect=AssertionError("OpenList must not be called")), patch(
            "pipeline.bot.Client115", side_effect=AssertionError("115 must not be called")
        ):
            service = PipelineBotService(
                BotConfig(
                    "token",
                    {700656624},
                    str(Path(tmp) / "state.db"),
                    msg_admin_user="admin",
                    msg_admin_password="secret",
                    msg_enabled=True,
                    subtitle_auto_match_enabled=True,
                )
            )
            result = service.subtitle_backfill_one_adult("media-target")

        self.assertEqual(fake_msg.get_calls, ["media-target"])
        self.assertEqual(fake_msg.list_calls, [])
        self.assertEqual(matcher.calls, [("adult", "MIDE-882", "media-target", "MIDE-882", False)])
        self.assertEqual(result["scanned"], 1)
        self.assertEqual(result["attempted"], 1)
        self.assertEqual(result["matched"], 1)
        self.assertEqual(result["with_subtitles"], 1)

    def test_subtitle_backfill_one_skips_previous_not_found_until_retry_requested(self):
        from pipeline.bot import BotConfig, CandidateStore, PipelineBotService
        from pipeline.config import category_to_msg_library_root

        class FakeSubtitleCache:
            def list_tracks(self, media_id):
                return []

        class FakeSubtitleMatcher:
            def __init__(self):
                self.cache = FakeSubtitleCache()
                self.calls = []

            def match_task(self, category, title, task, force=False):
                self.calls.append((category, title, task.get("msg_media_id"), task.get("openlist_adult_code"), force))
                return {
                    "subtitle_match_status": "success",
                    "subtitle_match_count": 1,
                    "subtitle_match_source": "subtitlecat",
                    "subtitle_match_filename": "SSIS-218.srt",
                }

        root = category_to_msg_library_root("adult")
        fake_msg = FakeMediaStationClient(
            get_response={
                "data": {
                    "id": "media-target",
                    "library_id": root["library_id"],
                    "title": "SSIS-218",
                    "path": "cloud://openlist/115/成人/SSIS-218/SSIS-218.mp4",
                }
            }
        )
        matcher = FakeSubtitleMatcher()

        with tempfile.TemporaryDirectory() as tmp, patch("pipeline.bot.MediaStationClient", return_value=fake_msg), patch(
            "pipeline.bot.build_subtitle_matcher_from_config", return_value=matcher
        ):
            state_db = str(Path(tmp) / "state.db")
            store = CandidateStore(state_db)
            store.save_subtitle_backfill_record(
                "media-target",
                "SSIS-218",
                "SSIS-218",
                {"subtitle_match_status": "skipped", "subtitle_match_reason": "not_found"},
            )
            service = PipelineBotService(
                BotConfig(
                    "token",
                    {700656624},
                    state_db,
                    msg_admin_user="admin",
                    msg_admin_password="secret",
                    msg_enabled=True,
                    subtitle_auto_match_enabled=True,
                )
            )
            skipped = service.subtitle_backfill_one_adult("media-target")
            retried = service.subtitle_backfill_one_adult("media-target", retry_attempted=True)

        self.assertEqual(skipped["attempted"], 0)
        self.assertEqual(skipped["previous"], 1)
        self.assertEqual(skipped["pending"], 1)
        self.assertEqual(retried["attempted"], 1)
        self.assertEqual(retried["matched"], 1)
        self.assertEqual(matcher.calls, [("adult", "SSIS-218", "media-target", "SSIS-218", False)])

    def test_subtitle_backfill_fails_when_cache_read_fails(self):
        from pipeline.bot import BotConfig, PipelineBotService
        from pipeline.config import category_to_msg_library_root

        class BrokenSubtitleCache:
            def list_tracks(self, media_id):
                raise RuntimeError("cache index unreadable")

        class FakeSubtitleMatcher:
            def __init__(self):
                self.cache = BrokenSubtitleCache()
                self.calls = []

            def match_task(self, category, title, task, force=False):
                self.calls.append((category, title, task))
                return {"subtitle_match_status": "success"}

        root = category_to_msg_library_root("adult")
        fake_msg = FakeMediaStationClient(
            list_response={
                "data": {
                    "items": [
                        {
                            "id": "media-target",
                            "library_id": root["library_id"],
                            "title": "MIDE-882",
                            "path": "cloud://openlist/115/成人/MIDE-882/MIDE-882.mp4",
                        }
                    ]
                }
            }
        )
        matcher = FakeSubtitleMatcher()

        with tempfile.TemporaryDirectory() as tmp, patch("pipeline.bot.MediaStationClient", return_value=fake_msg), patch(
            "pipeline.bot.build_subtitle_matcher_from_config", return_value=matcher
        ):
            service = PipelineBotService(
                BotConfig(
                    "token",
                    {700656624},
                    str(Path(tmp) / "state.db"),
                    msg_admin_user="admin",
                    msg_admin_password="secret",
                    msg_enabled=True,
                    subtitle_auto_match_enabled=True,
                )
            )
            with self.assertRaisesRegex(RuntimeError, "cache index unreadable"):
                service.subtitle_backfill_adult(limit=1)

        self.assertEqual(matcher.calls, [])

    def test_subtitle_backfill_skips_previous_not_found_until_retry_requested(self):
        from pipeline.bot import BotConfig, CandidateStore, PipelineBotService
        from pipeline.config import category_to_msg_library_root

        class FakeSubtitleCache:
            def list_tracks(self, media_id):
                return []

        class FakeSubtitleMatcher:
            def __init__(self):
                self.cache = FakeSubtitleCache()
                self.calls = []

            def match_task(self, category, title, task, force=False):
                self.calls.append((category, title, task.get("msg_media_id"), task.get("openlist_adult_code"), force))
                return {
                    "subtitle_match_status": "success",
                    "subtitle_match_count": 1,
                    "subtitle_match_source": "subtitlecat",
                    "subtitle_match_filename": "MIDE-882.srt",
                }

        root = category_to_msg_library_root("adult")
        fake_msg = FakeMediaStationClient(
            list_response={
                "data": {
                    "items": [
                        {
                            "id": "media-target",
                            "library_id": root["library_id"],
                            "title": "MIDE-882",
                            "path": "cloud://openlist/115/成人/MIDE-882/MIDE-882.mp4",
                        }
                    ]
                }
            }
        )
        matcher = FakeSubtitleMatcher()

        with tempfile.TemporaryDirectory() as tmp, patch("pipeline.bot.MediaStationClient", return_value=fake_msg), patch(
            "pipeline.bot.build_subtitle_matcher_from_config", return_value=matcher
        ):
            state_db = str(Path(tmp) / "state.db")
            store = CandidateStore(state_db)
            store.save_subtitle_backfill_record(
                "media-target",
                "MIDE-882",
                "MIDE-882",
                {"subtitle_match_status": "skipped", "subtitle_match_reason": "not_found"},
            )
            service = PipelineBotService(
                BotConfig(
                    "token",
                    {700656624},
                    state_db,
                    msg_admin_user="admin",
                    msg_admin_password="secret",
                    msg_enabled=True,
                    subtitle_auto_match_enabled=True,
                )
            )
            skipped = service.subtitle_backfill_adult(limit=1)
            retried = service.subtitle_backfill_adult(limit=1, retry_attempted=True)

        self.assertEqual(skipped["attempted"], 0)
        self.assertEqual(skipped["previous"], 1)
        self.assertEqual(skipped["pending"], 1)
        self.assertEqual(retried["attempted"], 1)
        self.assertEqual(retried["matched"], 1)
        self.assertEqual(retried["with_subtitles"], 1)
        self.assertEqual(matcher.calls, [("adult", "MIDE-882", "media-target", "MIDE-882", False)])

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

    def test_sync_completed_task_prefers_msg_media_path_over_search_title_match(self):
        from pipeline.bot import BotConfig, PipelineBotService

        movie_root = category_to_openlist_path("movie")
        target_path = movie_root + "/秋色之空"
        fake_openlist = CleaningOpenList(
            {
                movie_root: [{"name": "秋色之空", "is_dir": True, "size": 0}],
                target_path: [{"name": "01.mkv", "is_dir": False, "size": 900 * 1024 * 1024}],
            }
        )
        fake_msg = FakeMediaStationClient(
            search_response={
                "data": {
                    "items": [
                        {
                            "id": "media-wrong",
                            "library_id": "d150a96c-b467-4c60-82f1-207ae5949045",
                            "title": "秋色之空",
                            "path": "cloud://openlist/115/动漫/成龙历险记/01.mp4",
                        }
                    ]
                }
            },
            list_response={
                "data": {
                    "items": [
                        {
                            "id": "media-correct",
                            "library_id": "d150a96c-b467-4c60-82f1-207ae5949045",
                            "title": "秋色之空",
                            "path": "cloud://openlist/115/电影/秋色之空/01.mkv",
                            "size_bytes": 900 * 1024 * 1024,
                        }
                    ]
                }
            },
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
                "秋色之空",
                {"info_hash": "ABC", "status_name": "success", "name": "秋色之空"},
            )

        self.assertEqual(task["msg_media_id"], "media-correct")
        self.assertEqual(task["msg_match_mode"], "path")
        self.assertEqual(task["msg_match_path"], "cloud://openlist/115/电影/秋色之空/01.mkv")
        self.assertEqual(fake_msg.search_calls, [])

    def test_find_media_by_openlist_paths_matches_encoded_child_path(self):
        from pipeline.bot import find_media_by_openlist_paths

        media = find_media_by_openlist_paths(
            [
                {
                    "id": "wrong",
                    "library_id": "library-1",
                    "path": "cloud://openlist/115/电影/其他/01.mkv",
                },
                {
                    "id": "right",
                    "library_id": "library-1",
                    "path": "cloud://openlist/115/%E7%94%B5%E5%BD%B1/Sintel/main.mkv",
                    "size_bytes": 800 * 1024 * 1024,
                },
            ],
            ["/115/电影/Sintel"],
            library_id="library-1",
        )

        self.assertEqual(media["id"], "right")
        self.assertEqual(media["_pipeline_match_mode"], "path")
        self.assertEqual(media["_pipeline_match_path"], "cloud://openlist/115/电影/Sintel/main.mkv")

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
        self.assertEqual(duplicate["identity_type"], "adult_code")
        self.assertEqual(duplicate["identity_value"], "SSIS-450")

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
        self.assertEqual(duplicate["identity_type"], "title_query")
        self.assertEqual(duplicate["identity_value"], "Sintel")

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

        with patch("pipeline.bot.OpenListPasswordTokenProvider", FakeOpenListPasswordTokenProvider), patch(
            "pipeline.bot.OpenListClient", return_value=fake_openlist
        ):
            service = PipelineBotService(
                BotConfig(
                    "token",
                    {700656624},
                    "/tmp/state.db",
                    openlist_scan_username="media_scan",
                    openlist_scan_password="scan-secret",
                )
            )
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

        with patch("pipeline.bot.OpenListPasswordTokenProvider", FakeOpenListPasswordTokenProvider), patch(
            "pipeline.bot.OpenListClient", return_value=fake_openlist
        ):
            service = PipelineBotService(
                BotConfig(
                    "token",
                    {700656624},
                    "/tmp/state.db",
                    openlist_scan_username="media_scan",
                    openlist_scan_password="scan-secret",
                )
            )
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

    def test_collect_openlist_dedupe_entries_requires_media_scan_credentials(self):
        from pipeline.bot import BotConfig, PipelineBotService

        service = PipelineBotService(BotConfig("token", {700656624}, "/tmp/state.db"))

        with self.assertRaisesRegex(RuntimeError, "media scan credentials missing"):
            service.collect_openlist_dedupe_entries(refresh=True)

    def test_collect_openlist_dedupe_entries_uses_media_scan_view(self):
        from pipeline.bot import BotConfig, PipelineBotService

        movie_root = category_to_openlist_path("movie")
        hidden_name = "Hidden Extra"
        visible_name = "Visible Movie"
        scan_openlist = CleaningOpenList(
            {
                movie_root: [{"name": visible_name, "is_dir": True, "size": 0}],
                movie_root + "/" + visible_name: [{"name": "main.mkv", "is_dir": False, "size": 900 * 1024 * 1024}],
                category_to_openlist_path("tv"): [],
                category_to_openlist_path("anime"): [],
                category_to_openlist_path("adult"): [],
                category_to_openlist_path("other"): [],
            }
        )
        admin_openlist = CleaningOpenList(
            {
                movie_root: [
                    {"name": visible_name, "is_dir": True, "size": 0},
                    {"name": hidden_name, "is_dir": True, "size": 0},
                ],
                movie_root + "/" + visible_name: [{"name": "main.mkv", "is_dir": False, "size": 900 * 1024 * 1024}],
                movie_root + "/" + hidden_name: [{"name": "sample.mp4", "is_dir": False, "size": 20 * 1024 * 1024}],
                category_to_openlist_path("tv"): [],
                category_to_openlist_path("anime"): [],
                category_to_openlist_path("adult"): [],
                category_to_openlist_path("other"): [],
            }
        )

        def build_client(_url, token):
            return scan_openlist if token == "media-scan-token-value" else admin_openlist

        with patch("pipeline.bot.OpenListPasswordTokenProvider", FakeOpenListPasswordTokenProvider), patch(
            "pipeline.bot.OpenListClient", side_effect=build_client
        ):
            service = PipelineBotService(
                BotConfig(
                    "token",
                    {700656624},
                    "/tmp/state.db",
                    openlist_scan_username="media_scan",
                    openlist_scan_password="scan-secret",
                )
            )
            entries = service.collect_openlist_dedupe_entries(refresh=True)

        keys = {(entry["category"], entry["identity_type"], entry["identity_value"]) for entry in entries}
        self.assertIn(("movie", "normalized_title", "visiblemovie"), keys)
        self.assertNotIn(("movie", "normalized_title", "hiddenextra"), keys)

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
