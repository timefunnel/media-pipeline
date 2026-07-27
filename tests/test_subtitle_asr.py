import json
import os
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "app"))

for category in ("movie", "tv", "anime", "adult", "other"):
    prefix = "MEDIA_PIPELINE_%s" % category.upper()
    os.environ.setdefault(prefix + "_FOLDER_ID", "test-%s-folder" % category)
    os.environ.setdefault(prefix + "_MSG_LIBRARY_ID", "test-%s-library" % category)
    os.environ.setdefault(prefix + "_MSG_ROOT_ID", "test-%s-root" % category)

from pipeline.bot import BotConfig
from pipeline.internal_api import InternalApiApplication, InternalApiStore, SubtitleAsrTaskManager
from pipeline.subtitle_asr import (
    SenseVoiceClient,
    SubtitleTranslationClient,
    build_srt,
    validate_asr_segments,
)


class FakeHttpResponse:
    def __init__(self, payload):
        self.body = json.dumps(payload).encode("utf-8")

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def read(self, _limit):
        return self.body


class SenseVoiceHealthTest(unittest.TestCase):
    @patch("pipeline.subtitle_asr.urllib.request.urlopen")
    def test_health_rejects_explicitly_unavailable_translation_service(self, urlopen):
        urlopen.return_value = FakeHttpResponse(
            {"status": "ok", "cuda_available": True, "llm_available": False}
        )
        client = SenseVoiceClient("http://10.77.0.5:17860", "secret")
        with self.assertRaisesRegex(RuntimeError, "translation service is unavailable"):
            client.health()


class FakeTranslationTransport:
    def __init__(self, response):
        self.response = response
        self.requests = []

    def request(self, url, payload, headers=None, timeout=None):
        self.requests.append((url, payload, headers, timeout))
        return self.response


class SubtitleTranslationTest(unittest.TestCase):
    def test_translation_keeps_exact_ids_and_timestamps(self):
        transport = FakeTranslationTransport(
            {
                "choices": [
                    {
                        "message": {
                            "content": json.dumps(
                                {
                                    "translations": [
                                        {"id": 0, "text": "第一句"},
                                        {"id": 1, "text": "第二句"},
                                    ]
                                },
                                ensure_ascii=False,
                            )
                        }
                    }
                ]
            }
        )
        client = SubtitleTranslationClient(
            "https://api.deepseek.invalid/v1",
            "secret",
            "deepseek-test",
            30,
            transport=transport,
        )
        result = client.translate(
            [
                {"id": 0, "start": 0.21, "end": 1.5, "text": "hello"},
                {"id": 1, "start": 1.5, "end": 3.125, "text": "world"},
            ]
        )
        self.assertEqual([item["text"] for item in result], ["第一句", "第二句"])
        self.assertEqual(result[1]["end"], 3.125)
        self.assertIn("00:00:00,210 --> 00:00:01,500", build_srt(result))

    def test_translation_rejects_missing_segment_id(self):
        transport = FakeTranslationTransport(
            {"choices": [{"message": {"content": '{"translations":[{"id":0,"text":"第一句"}]}'}}]}
        )
        client = SubtitleTranslationClient(
            "https://api.deepseek.invalid/v1",
            "secret",
            "deepseek-test",
            30,
            transport=transport,
        )
        with self.assertRaisesRegex(RuntimeError, "segment IDs"):
            client.translate(
                [
                    {"id": 0, "start": 0, "end": 1, "text": "one"},
                    {"id": 1, "start": 1, "end": 2, "text": "two"},
                ]
            )

    def test_asr_segments_require_real_ordered_timeline(self):
        with self.assertRaisesRegex(RuntimeError, "timeline"):
            validate_asr_segments([{"id": 0, "start": 1, "end": 1, "text": "invalid"}])


class FakeSubtitleAsrProcessor:
    def __init__(self):
        self.ensure_calls = 0
        self.run_calls = []

    def ensure_available(self):
        self.ensure_calls += 1

    def run(self, media_id, source_language, progress_callback=None):
        self.run_calls.append((media_id, source_language))
        progress_callback("extracting_audio", 0, 0)
        progress_callback("translating", 1, 1)
        return {
            "filename": "sensevoice-deepseek-zh-cn.srt",
            "source": "sensevoice-deepseek",
            "language": "zh-CN",
            "segment_count": 2,
            "duration": 3.125,
        }


class SubtitleAsrTaskTest(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.store = InternalApiStore(str(Path(self.tempdir.name) / "state.db"))
        self.processor = FakeSubtitleAsrProcessor()
        self.manager = SubtitleAsrTaskManager(self.processor, self.store, poll_seconds=0.01)
        self.application = InternalApiApplication(None, self.store, None, subtitle_asr_manager=self.manager)

    def tearDown(self):
        self.manager.stop()
        self.tempdir.cleanup()

    def test_task_is_persisted_and_completed_by_single_worker(self):
        task, created = self.application.create_subtitle_asr(
            {"owner_id": "admin", "media_id": "media-1", "source_language": "ja"}
        )
        duplicate, duplicate_created = self.application.create_subtitle_asr(
            {"owner_id": "admin", "media_id": "media-1", "source_language": "en"}
        )
        self.assertTrue(created)
        self.assertFalse(duplicate_created)
        self.assertEqual(duplicate["id"], task["id"])
        self.assertEqual(duplicate["source_language"], "ja")

        self.manager.start()
        deadline = time.time() + 2
        while time.time() < deadline:
            completed = self.application.get_subtitle_asr("admin", task["id"])
            if completed["status"] in {"completed", "failed"}:
                break
            time.sleep(0.01)
        self.assertEqual(completed["status"], "completed")
        self.assertEqual(completed["result"]["source"], "sensevoice-deepseek")
        self.assertEqual(self.processor.run_calls, [("media-1", "ja")])

    def test_running_task_is_requeued_after_restart(self):
        task, _ = self.store.create_subtitle_asr_task("admin", "media-2", "auto")
        claimed = self.store.claim_next_subtitle_asr_task()
        self.assertEqual(claimed["status"], "running")
        self.assertEqual(self.store.recover_running_subtitle_asr_tasks(), 1)
        recovered = self.store.get_subtitle_asr_task("admin", task["id"])
        self.assertEqual(recovered["status"], "queued")
        self.assertEqual(recovered["stage"], "queued")


class SubtitleAsrConfigTest(unittest.TestCase):
    def base_env(self):
        return {
            "TG_BOT_TOKEN": "123:token",
            "TG_ALLOWED_USER_IDS": "1",
            "ASR_ENABLED": "1",
            "ASR_BASE_URL": "http://10.77.0.5:17860",
            "ASR_API_TOKEN": "asr-secret",
            "LLM_API_KEY": "llm-secret",
        }

    def test_config_reads_asr_settings(self):
        env = self.base_env()
        env["ASR_TIMEOUT_SECONDS"] = "1200"
        env["ASR_TRANSLATION_TIMEOUT_SECONDS"] = "75"
        config = BotConfig.from_env(env)
        self.assertTrue(config.asr_enabled)
        self.assertEqual(config.asr_base_url, "http://10.77.0.5:17860")
        self.assertEqual(config.asr_timeout_seconds, 1200)
        self.assertEqual(config.asr_translation_timeout_seconds, 75)

    def test_enabled_asr_requires_llm_key(self):
        env = self.base_env()
        env.pop("LLM_API_KEY")
        with self.assertRaisesRegex(RuntimeError, "LLM_API_KEY missing"):
            BotConfig.from_env(env)


if __name__ == "__main__":
    unittest.main()
