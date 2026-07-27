import json
import os
import sys
import tempfile
import time
import unittest
from types import SimpleNamespace
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
    MediaStationTranslationClient,
    SenseVoiceClient,
    SubtitleAsrProcessor,
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

    def test_cloud_translation_uses_mediastation_proxy(self):
        msg_client = FakeCloudTranslationClient()
        client = MediaStationTranslationClient(msg_client, "deepseek", "deepseek-chat")
        result = client.translate(
            [{"id": 0, "start": 0.0, "end": 1.0, "text": "hello"}]
        )
        self.assertEqual(result[0]["text"], "你好")
        self.assertEqual(msg_client.calls[0][0:2], ("deepseek", "deepseek-chat"))

    def test_asr_segments_require_real_ordered_timeline(self):
        with self.assertRaisesRegex(RuntimeError, "timeline"):
            validate_asr_segments([{"id": 0, "start": 1, "end": 1, "text": "invalid"}])


class FakeCloudTranslationClient:
    def __init__(self):
        self.calls = []

    def pipeline_translate_subtitles(self, provider, model, segments):
        self.calls.append((provider, model, segments))
        return {"translations": [{"id": item["id"], "text": "你好"} for item in segments]}


class FakeSubtitleAsrProcessor:
    def __init__(self):
        self.ensure_calls = 0
        self.run_calls = []
        self.deleted_cache_ids = []
        self.cache = (True, True)
        self.config = SimpleNamespace(asr_translation_model="fake-model")

    def ensure_available(self, translation_provider="local", translation_model=""):
        self.ensure_calls += 1

    def translation_models(self):
        return ["fake-model", "other-model"]

    def cache_state(self, _task_id):
        return self.cache

    def delete_cache(self, task_id):
        self.deleted_cache_ids.append(task_id)

    def run(
        self,
        task_id,
        media_id,
        source_language,
        translation_provider="local",
        translation_model="",
        progress_callback=None,
        cache_callback=None,
    ):
        self.run_calls.append((media_id, source_language))
        progress_callback("extracting_audio", 30, 120)
        cache_callback(True, True)
        progress_callback("translating", 1, 1)
        return {
            "filename": "sensevoice-qwen-zh-cn.srt",
            "source": "sensevoice-qwen",
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
        self.assertEqual(completed["result"]["source"], "sensevoice-qwen")
        self.assertEqual(self.processor.run_calls, [("media-1", "ja")])

    def test_running_task_is_requeued_after_restart(self):
        task, _ = self.store.create_subtitle_asr_task("admin", "media-2", "auto")
        claimed = self.store.claim_next_subtitle_asr_task()
        self.assertEqual(claimed["status"], "running")
        self.assertEqual(self.store.recover_running_subtitle_asr_tasks(), 1)
        recovered = self.store.get_subtitle_asr_task("admin", task["id"])
        self.assertEqual(recovered["status"], "queued")
        self.assertEqual(recovered["stage"], "queued")

    def test_task_list_keeps_active_tasks_visible_before_recent_results(self):
        completed, _ = self.store.create_subtitle_asr_task("admin", "media-completed", "ja")
        self.store.finish_subtitle_asr_task(
            completed["id"],
            "completed",
            "completed",
            result={"filename": "completed.zh-CN.srt", "segment_count": 8},
        )
        queued, _ = self.store.create_subtitle_asr_task("admin", "media-queued", "en")

        listed = self.application.list_subtitle_asr(50)["items"]

        self.assertEqual([task["id"] for task in listed], [queued["id"], completed["id"]])
        self.assertEqual(listed[1]["result"]["filename"], "completed.zh-CN.srt")

    def test_failed_task_retry_keeps_id_and_cached_artifacts(self):
        task, _ = self.store.create_subtitle_asr_task(
            "admin", "media-retry", "ja", "local", "fake-model"
        )
        self.store.finish_subtitle_asr_task(task["id"], "failed", "failed", error="translation failed")

        retried = self.manager.retry_task(
            "admin",
            task["id"],
            {"translation_provider": "local", "translation_model": "other-model"},
        )

        self.assertEqual(retried["id"], task["id"])
        self.assertEqual(retried["status"], "queued")
        self.assertEqual(retried["translation_model"], "other-model")
        self.assertTrue(retried["cached_audio"])
        self.assertTrue(retried["cached_transcript"])

    def test_finished_task_delete_removes_cache_and_row(self):
        task, _ = self.store.create_subtitle_asr_task("admin", "media-delete", "auto")
        self.store.finish_subtitle_asr_task(task["id"], "failed", "failed", error="failed")

        self.manager.delete_task("admin", task["id"])

        self.assertEqual(self.processor.deleted_cache_ids, [task["id"]])
        with self.assertRaisesRegex(Exception, "not found"):
            self.store.get_subtitle_asr_task("admin", task["id"])

    def test_active_task_delete_is_rejected(self):
        task, _ = self.store.create_subtitle_asr_task("admin", "media-active", "auto")
        with self.assertRaisesRegex(Exception, "active"):
            self.manager.delete_task("admin", task["id"])

    def test_local_model_list_is_exposed(self):
        self.assertEqual(self.application.list_subtitle_asr_models(), {"models": ["fake-model", "other-model"]})


class SubtitleAsrCacheReuseTest(unittest.TestCase):
    def test_cached_audio_and_transcript_skip_download_and_transcription(self):
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            cache_dir = root / "cache" / ("a" * 32)
            cache_dir.mkdir(parents=True)
            (cache_dir / "audio.mp3").write_bytes(b"audio")
            (cache_dir / "transcript.json").write_text(
                json.dumps(
                    {
                        "segments": [
                            {"id": 0, "start": 0.0, "end": 1.5, "text": "hello"}
                        ]
                    }
                ),
                encoding="utf-8",
            )
            config = SimpleNamespace(
                asr_cache_dir=str(root / "cache"),
                subtitle_cache_dir=str(root / "subtitles"),
            )
            processor = SubtitleAsrProcessor(config)
            processor._msg_client = lambda: self.fail("cached audio must skip MediaStationGo download")
            processor._asr_client = lambda: self.fail("cached transcript must skip SenseVoice")
            processor._translation_client = lambda _provider, _model: FakeCachedTranslationClient()
            progress = []

            result = processor.run(
                "a" * 32,
                "media-1",
                "en",
                translation_provider="local",
                translation_model="fake-model",
                progress_callback=lambda stage, current, total: progress.append((stage, current, total)),
            )

            self.assertEqual(result["translation_model"], "fake-model")
            self.assertIn(("using_cached_audio", 1, 1), progress)
            self.assertIn(("using_cached_transcript", 1, 1), progress)


class FakeCachedTranslationClient:
    def translate(self, segments, progress_callback=None):
        if progress_callback is not None:
            progress_callback(1, 1)
        return [{**segment, "text": "你好"} for segment in segments]


class SubtitleAsrConfigTest(unittest.TestCase):
    def base_env(self):
        return {
            "TG_BOT_TOKEN": "123:token",
            "TG_ALLOWED_USER_IDS": "1",
            "ASR_ENABLED": "1",
            "ASR_BASE_URL": "http://10.77.0.5:17860",
            "ASR_API_TOKEN": "asr-secret",
            "ASR_TRANSLATION_BASE_URL": "http://10.77.0.5:17860/v1",
            "ASR_TRANSLATION_MODEL": "qwen-test",
            "ASR_TRANSLATION_API_KEY": "translation-secret",
        }

    def test_config_reads_asr_settings(self):
        env = self.base_env()
        env["ASR_TIMEOUT_SECONDS"] = "1200"
        env["ASR_TRANSLATION_TIMEOUT_SECONDS"] = "75"
        config = BotConfig.from_env(env)
        self.assertTrue(config.asr_enabled)
        self.assertEqual(config.asr_base_url, "http://10.77.0.5:17860")
        self.assertEqual(config.asr_timeout_seconds, 1200)
        self.assertEqual(config.asr_translation_base_url, "http://10.77.0.5:17860/v1")
        self.assertEqual(config.asr_translation_model, "qwen-test")
        self.assertEqual(config.asr_translation_api_key, "translation-secret")
        self.assertEqual(config.asr_translation_timeout_seconds, 75)

    def test_enabled_asr_requires_translation_key(self):
        env = self.base_env()
        env.pop("ASR_TRANSLATION_API_KEY")
        with self.assertRaisesRegex(RuntimeError, "ASR_TRANSLATION_API_KEY missing"):
            BotConfig.from_env(env)

    def test_asr_translation_config_does_not_replace_search_llm_config(self):
        env = self.base_env()
        env.update(
            {
                "LLM_BASE_URL": "https://search-llm.invalid/v1",
                "LLM_MODEL": "search-model",
                "LLM_API_KEY": "search-secret",
            }
        )
        config = BotConfig.from_env(env)
        self.assertEqual(config.llm_base_url, "https://search-llm.invalid/v1")
        self.assertEqual(config.llm_model, "search-model")
        self.assertEqual(config.llm_api_key, "search-secret")
        self.assertEqual(config.asr_translation_model, "qwen-test")

    def test_asr_translation_config_keeps_legacy_llm_fallback(self):
        env = self.base_env()
        env.pop("ASR_TRANSLATION_BASE_URL")
        env.pop("ASR_TRANSLATION_MODEL")
        env.pop("ASR_TRANSLATION_API_KEY")
        env.update(
            {
                "LLM_BASE_URL": "https://legacy-llm.invalid/v1",
                "LLM_MODEL": "legacy-model",
                "LLM_API_KEY": "legacy-secret",
            }
        )
        config = BotConfig.from_env(env)
        self.assertEqual(config.asr_translation_base_url, "https://legacy-llm.invalid/v1")
        self.assertEqual(config.asr_translation_model, "legacy-model")
        self.assertEqual(config.asr_translation_api_key, "legacy-secret")


if __name__ == "__main__":
    unittest.main()
