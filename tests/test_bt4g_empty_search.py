import unittest
from unittest.mock import patch

from tests.test_pipeline_core import FakeProwlarr

from pipeline.bot import (
    BotConfig,
    PipelineBotService,
    SEARCH_PROFILE_ADULT,
    SEARCH_PROFILE_GENERAL,
)
from pipeline.search_stats import exception_search_metadata, search_result_metadata


class EmptySearchResultTest(unittest.TestCase):
    def test_profile_search_returns_empty_result_after_successful_empty_response(self):
        fake_prowlarr = FakeProwlarr(
            [],
            indexers=[
                {
                    "id": 8,
                    "name": "sukebei.nyaa.si",
                    "enable": True,
                    "capabilities": {"categories": [{"id": 6000}]},
                },
            ],
        )

        with patch("pipeline.bot.ProwlarrConfig") as config_cls, patch(
            "pipeline.bot.ProwlarrClient", return_value=fake_prowlarr
        ):
            config_cls.return_value.load_api_key.return_value = "prowlarr-key"
            service = PipelineBotService(
                BotConfig(
                    "token",
                    {700656624},
                    "/tmp/state.db",
                    search_profile_max_workers={SEARCH_PROFILE_ADULT: 1},
                )
            )
            results = service.search_adult("MISSING-001", limit=5)

        metadata = search_result_metadata(results)
        self.assertEqual(results, [])
        self.assertEqual(metadata["success_count"], 1)
        self.assertEqual(metadata["failed_count"], 0)
        self.assertEqual(metadata["raw_count"], 0)
        self.assertEqual(metadata["selected_count"], 0)

    def test_profile_search_keeps_failure_when_all_indexers_fail(self):
        fake_prowlarr = FakeProwlarr(
            [],
            indexers=[
                {
                    "id": 8,
                    "name": "sukebei.nyaa.si",
                    "enable": True,
                    "capabilities": {"categories": [{"id": 6000}]},
                },
            ],
            indexer_errors={(8,): TimeoutError("timed out")},
        )

        with patch("pipeline.bot.ProwlarrConfig") as config_cls, patch(
            "pipeline.bot.ProwlarrClient", return_value=fake_prowlarr
        ):
            config_cls.return_value.load_api_key.return_value = "prowlarr-key"
            service = PipelineBotService(
                BotConfig(
                    "token",
                    {700656624},
                    "/tmp/state.db",
                    search_profile_max_workers={SEARCH_PROFILE_ADULT: 1},
                )
            )
            with self.assertRaisesRegex(RuntimeError, "no acceptable resource") as raised:
                service.search_adult("MISSING-001", limit=5)

        metadata = exception_search_metadata(raised.exception)
        self.assertEqual(metadata["success_count"], 0)
        self.assertEqual(metadata["failed_count"], 1)

    def test_bt4g_search_returns_empty_result_after_successful_empty_response(self):
        fake_prowlarr = FakeProwlarr(
            [],
            indexers=[
                {
                    "id": 2,
                    "name": "BT4G",
                    "enable": True,
                    "priority": 25,
                    "capabilities": {"categories": [{"id": 2000}]},
                },
            ],
        )

        with patch("pipeline.bot.ProwlarrConfig") as config_cls, patch(
            "pipeline.bot.ProwlarrClient", return_value=fake_prowlarr
        ) as client_cls:
            config_cls.return_value.load_api_key.return_value = "prowlarr-key"
            service = PipelineBotService(
                BotConfig(
                    "token",
                    {700656624},
                    "/tmp/state.db",
                    prowlarr_bt4g_search_timeout_seconds=9,
                    search_profile_upstream_limits={SEARCH_PROFILE_GENERAL: 40},
                )
            )
            results = service.search_bt4g("MISSING-001", limit=5)

        metadata = search_result_metadata(results)
        self.assertEqual(results, [])
        self.assertEqual(metadata["profile"], "bt4g")
        self.assertEqual(metadata["success_count"], 1)
        self.assertEqual(metadata["raw_count"], 0)
        self.assertEqual(metadata["selected_count"], 0)
        self.assertEqual(metadata["settings"]["timeout_seconds"], 9)
        self.assertEqual(client_cls.call_args.kwargs["timeout"], 9)


if __name__ == "__main__":
    unittest.main()
