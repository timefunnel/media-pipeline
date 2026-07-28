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
    DEFAULT_ASR_MODEL,
    MediaStationTranslationClient,
    SenseVoiceClient,
    SubtitleAsrProcessor,
    SubtitleTranslationClient,
    build_translation_glossary,
    build_srt,
    load_translation_history,
    select_translatable_asr_segments,
    subtitle_translation_prompt,
    translate_sequentially,
    validate_asr_segments,
    validate_translation_text,
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
    def __init__(self, translations):
        self.translations = translations
        self.requests = []

    def request(self, url, payload, headers=None, timeout=None):
        self.requests.append((url, payload, headers, timeout))
        if url.endswith("/models/unload"):
            return {"unloaded": payload["model"]}
        prompt = payload["messages"][0]["content"]
        target = prompt.split("只输出译文，不要解释：\n\n", 1)[1]
        return {"choices": [{"message": {"content": self.translations[target]}}]}


class SubtitleTranslationTest(unittest.TestCase):
    def test_local_retry_prompt_contains_only_the_current_text(self):
        prompt = subtitle_translation_prompt(
            "こんにちは。",
            ["前の文。"],
            "人名：テスト",
            "上次译文仍含日文假名。",
        )

        self.assertEqual(
            prompt,
            "将下面的日文文本翻译成自然、准确的简体中文。\n"
            "只输出译文，不要解释：\n\n"
            "こんにちは。",
        )
        self.assertNotIn("参考上下文", prompt)
        self.assertNotIn("术语参考", prompt)
        self.assertNotIn("重试要求", prompt)

    def test_translation_uses_only_previous_context_and_server_owned_timeline(self):
        transport = FakeTranslationTransport(
            {"今日は遅かった。": "今天来晚了。", "電車が止まりました。": "电车停运了。"}
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
                {"id": 0, "start": 0.21, "end": 1.5, "text": "今日は遅かった。"},
                {"id": 1, "start": 1.5, "end": 3.125, "text": "電車が止まりました。"},
            ],
            glossary="東京 -> 东京",
        )
        self.assertEqual([item["text"] for item in result], ["今天来晚了。", "电车停运了。"])
        self.assertEqual(result[1]["end"], 3.125)
        self.assertIn("00:00:00,210 --> 00:00:01,500", build_srt(result))
        self.assertEqual(len(transport.requests), 2)
        first_payload = transport.requests[0][1]
        second_payload = transport.requests[1][1]
        self.assertNotIn("response_format", first_payload)
        self.assertEqual(first_payload["temperature"], 0.1)
        self.assertEqual(first_payload["top_p"], 0.9)
        self.assertEqual(first_payload["max_tokens"], 1024)
        self.assertEqual(len(first_payload["messages"]), 1)
        self.assertIn("参考上下文：\n（无）", first_payload["messages"][0]["content"])
        self.assertIn("术语参考：\n東京 -> 东京", first_payload["messages"][0]["content"])
        self.assertIn("参考上下文：\n今日は遅かった。", second_payload["messages"][0]["content"])
        self.assertNotIn("電車が止まりました。\n\n术语参考", second_payload["messages"][0]["content"])
        client.unload_model()
        self.assertTrue(transport.requests[-1][0].endswith("/models/unload"))

    @patch("pipeline.subtitle_asr.time.sleep")
    def test_translation_rejects_empty_plain_text_after_current_segment_retry(self, sleep):
        transport = FakeTranslationTransport({"こんにちは": ""})
        client = SubtitleTranslationClient(
            "https://api.deepseek.invalid/v1",
            "secret",
            "deepseek-test",
            30,
            transport=transport,
        )
        with self.assertRaisesRegex(RuntimeError, "segment 0 after 3 attempts"):
            client.translate([{"id": 0, "start": 0, "end": 1, "text": "こんにちは"}])
        chat_requests = [item for item in transport.requests if item[0].endswith("/chat/completions")]
        unload_requests = [item for item in transport.requests if item[0].endswith("/models/unload")]
        self.assertEqual(len(chat_requests), 3)
        self.assertEqual(len(unload_requests), 2)
        self.assertEqual(sleep.call_count, 2)

    def test_translation_reuses_completed_segments_and_checkpoints_new_results(self):
        transport = FakeTranslationTransport({"元気ですか。": "你好吗？"})
        client = SubtitleTranslationClient(
            "https://api.deepseek.invalid/v1",
            "secret",
            "deepseek-test",
            30,
            transport=transport,
        )
        checkpoints = []
        result = client.translate(
            [
                {"id": 0, "start": 0, "end": 1, "text": "こんにちは。"},
                {"id": 1, "start": 1, "end": 2, "text": "元気ですか。"},
            ],
            cached_translations=[{"id": 0, "text": "你好。"}],
            checkpoint_callback=lambda values: checkpoints.append(values),
        )
        self.assertEqual([item["text"] for item in result], ["你好。", "你好吗？"])
        self.assertEqual(len(transport.requests), 1)
        self.assertEqual(
            checkpoints[-1],
            [{"id": 0, "text": "你好。"}, {"id": 1, "text": "你好吗？"}],
        )

    def test_translation_rejects_bad_cached_prompt_echo_and_retranslates_only_that_segment(self):
        calls = []
        events = []

        result = translate_sequentially(
            [
                {"id": 0, "start": 0, "end": 1, "text": "こんにちは。"},
                {"id": 1, "start": 1, "end": 2, "text": "元気ですか。"},
            ],
            lambda text, _context, _glossary, _retry: calls.append(text) or "你好。",
            provider="local",
            model="test-model",
            cached_translations=[
                {"id": 0, "text": "术语参考：（无） 只输出译文，不要解释"},
                {"id": 1, "text": "你好吗？"},
            ],
            event_callback=lambda event: events.append(event),
            retry_delay_seconds=0,
        )

        self.assertEqual(calls, ["こんにちは。"])
        self.assertEqual([item["text"] for item in result], ["你好。", "你好吗？"])
        self.assertEqual(events[0]["attempt"], 0)
        self.assertIn("cached translation rejected", events[0]["error"])

    def test_translation_program_handles_target_interjection_and_standalone_particle(self):
        calls = []
        checkpoints = []

        def translate_one(text, _context, _glossary, _retry_instruction):
            calls.append(text)
            return "你好。"

        result = translate_sequentially(
            [
                {"id": 0, "start": 0, "end": 1, "text": "て。"},
                {"id": 1, "start": 1, "end": 2, "text": "丈。"},
                {"id": 2, "start": 2, "end": 3, "text": "啊。"},
                {"id": 3, "start": 3, "end": 4, "text": "嗯嗯。"},
                {"id": 4, "start": 4, "end": 5, "text": "こんにちは。"},
            ],
            translate_one,
            provider="openai",
            model="test-model",
            checkpoint_callback=lambda values: checkpoints.append(values),
            retry_delay_seconds=0,
        )

        self.assertEqual(calls, ["こんにちは。"])
        self.assertEqual(
            result,
            [
                {"id": 2, "start": 2.0, "end": 3.0, "text": "啊。"},
                {"id": 3, "start": 3.0, "end": 4.0, "text": "嗯嗯。"},
                {"id": 4, "start": 4.0, "end": 5.0, "text": "你好。"},
            ],
        )
        self.assertEqual(
            checkpoints[-1],
            [
                {"id": 0, "mode": "skipped_nonsemantic"},
                {"id": 1, "mode": "skipped_nonsemantic"},
                {"id": 2, "text": "啊。", "mode": "target_language"},
                {"id": 3, "text": "嗯嗯。", "mode": "target_language"},
                {"id": 4, "text": "你好。"},
            ],
        )

    def test_translation_reuses_cached_program_handled_segments(self):
        calls = []
        result = translate_sequentially(
            [
                {"id": 0, "start": 0, "end": 1, "text": "て。"},
                {"id": 1, "start": 1, "end": 2, "text": "啊。"},
            ],
            lambda *args: calls.append(args),
            provider="openai",
            model="test-model",
            cached_translations=[
                {"id": 0, "mode": "skipped_nonsemantic"},
                {"id": 1, "text": "啊。", "mode": "target_language"},
            ],
            retry_delay_seconds=0,
        )

        self.assertEqual(calls, [])
        self.assertEqual(result, [{"id": 1, "start": 1.0, "end": 2.0, "text": "啊。"}])

    def test_translation_skips_short_emoji_sound_fragment_only(self):
        calls = []

        result = translate_sequentially(
            [
                {"id": 0, "start": 0, "end": 1, "text": "🤧った。"},
                {"id": 1, "start": 1, "end": 2, "text": "🤧大丈夫ですか。"},
            ],
            lambda text, _context, _glossary, _retry: calls.append(text) or "没事吧？",
            provider="local",
            model="test-model",
            retry_delay_seconds=0,
        )

        self.assertEqual(calls, ["🤧大丈夫ですか。"])
        self.assertEqual(
            result,
            [{"id": 1, "start": 1.0, "end": 2.0, "text": "没事吧？"}],
        )

    def test_translation_failure_retries_only_current_segment_and_stops(self):
        calls = []
        checkpoints = []
        events = []

        def translate_one(text, _context, _glossary, _retry_instruction):
            calls.append(text)
            raise RuntimeError("translation failed")

        with self.assertRaisesRegex(RuntimeError, "segment 0 after 3 attempts"):
            translate_sequentially(
                [
                    {"id": 0, "start": 0, "end": 1, "text": "最初。"},
                    {"id": 1, "start": 1, "end": 2, "text": "次。"},
                ],
                translate_one,
                provider="local",
                model="test-model",
                checkpoint_callback=lambda values: checkpoints.append(values),
                event_callback=lambda event: events.append(event),
                retry_delay_seconds=0,
            )
        self.assertEqual(calls, ["最初。", "最初。", "最初。"])
        self.assertEqual(checkpoints, [])
        self.assertEqual([event["status"] for event in events], ["failed", "failed", "failed"])
        self.assertEqual([event["attempt"] for event in events], [1, 2, 3])

    def test_cloud_translation_uses_mediastation_proxy(self):
        msg_client = FakeCloudTranslationClient()
        client = MediaStationTranslationClient(msg_client, "deepseek", "deepseek-chat")
        result = client.translate(
            [
                {"id": 0, "start": 0.0, "end": 1.0, "text": "こんにちは。"},
                {"id": 1, "start": 1.0, "end": 2.0, "text": "元気ですか。"},
            ],
            glossary="東京 -> 东京",
        )
        self.assertEqual(
            [item["text"] for item in result],
            ["你好。", "你好吗？"],
        )
        self.assertEqual(msg_client.calls[0][0:2], ("deepseek", "deepseek-chat"))
        hello_call = next(call for call in msg_client.calls if call[2] == "こんにちは。")
        self.assertEqual(hello_call[3], [])
        self.assertEqual(hello_call[4], "東京 -> 东京")
        self.assertEqual(msg_client.calls[1][3], ["こんにちは。"])

    def test_translation_rejects_abnormal_repetition(self):
        with self.assertRaisesRegex(RuntimeError, "same text for three different segments"):
            translate_sequentially(
                [
                    {"id": 0, "start": 0, "end": 1, "text": "一つ目。"},
                    {"id": 1, "start": 1, "end": 2, "text": "二つ目。"},
                    {"id": 2, "start": 2, "end": 3, "text": "三つ目。"},
                ],
                lambda _text, _context, _glossary, _retry_instruction: "完全相同的异常译文",
                provider="local",
                model="test-model",
                retry_delay_seconds=0,
            )

    def test_translation_retry_includes_quality_correction(self):
        retry_instructions = []
        contexts = []
        glossaries = []

        def translate_one(_text, context, glossary, retry_instruction):
            retry_instructions.append(retry_instruction)
            contexts.append(context)
            glossaries.append(glossary)
            return "こんにちは" if not retry_instruction else "你好"

        result = translate_sequentially(
            [
                {"id": 0, "start": 0, "end": 1, "text": "前の文。"},
                {"id": 1, "start": 1, "end": 2, "text": "こんにちは"},
            ],
            translate_one,
            provider="local",
            model="test-model",
            glossary="人名：テスト",
            cached_translations=[{"id": 0, "text": "前一句。"}],
            retry_delay_seconds=0,
        )

        self.assertEqual(result[1]["text"], "你好")
        self.assertEqual(retry_instructions[0], "")
        self.assertIn("未完成翻译", retry_instructions[1])
        self.assertEqual(contexts, [["前の文。"], []])
        self.assertEqual(glossaries, ["人名：テスト", ""])

    def test_translation_retry_corrects_empty_cloud_response(self):
        retry_instructions = []
        contexts = []
        glossaries = []

        def translate_one(_text, context, glossary, retry_instruction):
            retry_instructions.append(retry_instruction)
            contexts.append(context)
            glossaries.append(glossary)
            if not retry_instruction:
                raise RuntimeError("MediaStationGo API failed: HTTP 502 cloud translation returned empty content")
            return "这是空气净化器吗？"

        result = translate_sequentially(
            [
                {"id": 0, "start": 0, "end": 1, "text": "前の文。"},
                {"id": 1, "start": 1, "end": 2, "text": "それ空気清浄機？"},
            ],
            translate_one,
            provider="openai",
            model="deepseek-v4-flash",
            glossary="作品名：成人作品",
            cached_translations=[{"id": 0, "text": "前一句。"}],
            retry_delay_seconds=0,
        )

        self.assertEqual(result[1]["text"], "这是空气净化器吗？")
        self.assertEqual(retry_instructions[0], "")
        self.assertIn("非空", retry_instructions[1])
        self.assertEqual(contexts, [["前の文。"], []])
        self.assertEqual(glossaries, ["作品名：成人作品", ""])

    def test_translation_retry_corrects_prompt_echo(self):
        retry_instructions = []

        def translate_one(_text, _context, _glossary, retry_instruction):
            retry_instructions.append(retry_instruction)
            if not retry_instruction:
                return "术语参考：（无） 只输出译文，不要解释"
            return "你好。"

        result = translate_sequentially(
            [{"id": 0, "start": 0, "end": 1, "text": "こんにちは。"}],
            translate_one,
            provider="local",
            model="test-model",
            retry_delay_seconds=0,
        )

        self.assertEqual(result[0]["text"], "你好。")
        self.assertEqual(retry_instructions[0], "")
        self.assertIn("纯中文译文", retry_instructions[1])

    def test_non_speech_segments_are_not_sent_for_translation(self):
        selected = select_translatable_asr_segments(
            [
                {"id": 0, "start": 0, "end": 1, "text": "🎼."},
                {"id": 1, "start": 1, "end": 2, "text": "こんにちは"},
                {"id": 2, "start": 2, "end": 3, "text": "."},
            ]
        )

        self.assertEqual(
            selected,
            [{"id": 0, "start": 1.0, "end": 2.0, "text": "こんにちは"}],
        )

    def test_asr_segments_require_real_ordered_timeline(self):
        with self.assertRaisesRegex(RuntimeError, "timeline"):
            validate_asr_segments([{"id": 0, "start": 1, "end": 1, "text": "invalid"}])

    def test_translation_rejects_reasoning_tags(self):
        with self.assertRaisesRegex(RuntimeError, "reasoning content"):
            validate_translation_text("こんにちは", "<think>分析</think>你好")

    def test_translation_rejects_prompt_echo(self):
        with self.assertRaisesRegex(RuntimeError, "echoed the translation prompt"):
            validate_translation_text("こんにちは", "术语参考：（无） 只输出译文，不要解释")

    def test_srt_layout_collapses_translation_blank_lines(self):
        rendered = build_srt(
            [{"id": 0, "start": 0, "end": 1, "text": "第一行\n\n第二行"}]
        )
        self.assertIn("第一行 第二行", rendered)
        self.assertEqual(len([block for block in rendered.split("\n\n") if block.strip()]), 1)

    def test_translation_allows_small_readable_kana_residue(self):
        translated = "才没有这回事呢，のし一先生如果真的好吃的话，就原谅你吧。"
        self.assertEqual(validate_translation_text("そんなことないでしょ。", translated), translated)

    def test_translation_rejects_kana_dominated_output(self):
        with self.assertRaisesRegex(RuntimeError, "Japanese kana"):
            validate_translation_text("これはテストです。", "这是翻译，但まだほとんど日本語です。")


class FakeCloudTranslationClient:
    def __init__(self):
        self.calls = []

    def pipeline_translate_subtitle(self, provider, model, text, context, glossary, retry_instruction=""):
        self.calls.append((provider, model, text, context, glossary, retry_instruction))
        translations = {"こんにちは。": "你好。", "元気ですか。": "你好吗？"}
        return {"translation": translations[text]}


class FakeSubtitleAsrProcessor:
    def __init__(self):
        self.ensure_calls = 0
        self.run_calls = []
        self.deleted_cache_ids = []
        self.cache = (True, True)
        self.config = SimpleNamespace(
            asr_model=DEFAULT_ASR_MODEL,
            asr_translation_model="fake-model",
        )

    def ensure_available(
        self,
        translation_provider="local",
        translation_model="",
        asr_model=DEFAULT_ASR_MODEL,
    ):
        self.ensure_calls += 1

    def ensure_translation_available(self, translation_provider="local", translation_model=""):
        self.ensure_calls += 1

    def translation_models(self):
        return ["fake-model", "other-model"]

    def asr_models(self):
        return [DEFAULT_ASR_MODEL, "faster-whisper/large-v3"]

    def cache_state(self, _task_id, _asr_model=DEFAULT_ASR_MODEL):
        return self.cache

    def delete_cache(self, task_id):
        self.deleted_cache_ids.append(task_id)

    def run(
        self,
        task_id,
        media_id,
        source_language,
        asr_model=DEFAULT_ASR_MODEL,
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
            "asr_model": asr_model,
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

    def test_queued_task_model_can_change_before_worker_claim(self):
        task, _ = self.store.create_subtitle_asr_task(
            "admin", "media-edit-model", "ja", "local", "fake-model"
        )

        updated = self.manager.update_task_model(
            "admin",
            task["id"],
            {"translation_provider": "local", "translation_model": "other-model"},
        )
        claimed = self.store.claim_next_subtitle_asr_task()

        self.assertEqual(updated["translation_model"], "other-model")
        self.assertEqual(claimed["translation_model"], "other-model")

    def test_running_task_model_change_and_cancel_are_rejected(self):
        task, _ = self.store.create_subtitle_asr_task(
            "admin", "media-running", "ja", "local", "fake-model"
        )
        self.store.claim_next_subtitle_asr_task()

        with self.assertRaisesRegex(Exception, "only queued"):
            self.manager.update_task_model(
                "admin",
                task["id"],
                {"translation_provider": "local", "translation_model": "other-model"},
            )
        with self.assertRaisesRegex(Exception, "only queued"):
            self.manager.cancel_task("admin", task["id"])

    def test_queued_task_cancel_clears_cache_and_keeps_audit_row(self):
        task, _ = self.store.create_subtitle_asr_task("admin", "media-cancel", "ja")
        self.store.save_subtitle_asr_cache(task["id"], True, True)

        canceled = self.manager.cancel_task("admin", task["id"])

        self.assertEqual(canceled["status"], "canceled")
        self.assertEqual(canceled["stage"], "canceled")
        self.assertFalse(canceled["cached_audio"])
        self.assertFalse(canceled["cached_transcript"])
        self.assertEqual(self.processor.deleted_cache_ids, [task["id"]])
        self.assertEqual(
            self.store.get_subtitle_asr_task("admin", task["id"])["status"],
            "canceled",
        )

    def test_completed_task_retranslation_requires_and_reuses_asr_cache(self):
        task, _ = self.store.create_subtitle_asr_task(
            "admin", "media-retranslate", "ja", "local", "fake-model"
        )
        self.store.finish_subtitle_asr_task(
            task["id"], "completed", "completed", result={"filename": "old.srt"}
        )

        requeued = self.manager.retranslate_task(
            "admin",
            task["id"],
            {"translation_provider": "local", "translation_model": "other-model"},
        )

        self.assertEqual(requeued["status"], "queued")
        self.assertEqual(requeued["translation_model"], "other-model")
        self.assertIsNone(requeued["result"])
        self.assertTrue(requeued["cached_audio"])
        self.assertTrue(requeued["cached_transcript"])

    def test_completed_task_retranslation_rejects_incomplete_cache(self):
        task, _ = self.store.create_subtitle_asr_task(
            "admin", "media-no-transcript", "ja", "local", "fake-model"
        )
        self.store.finish_subtitle_asr_task(task["id"], "completed", "completed")
        self.processor.cache = (True, False)

        with self.assertRaisesRegex(Exception, "both required"):
            self.manager.retranslate_task(
                "admin",
                task["id"],
                {"translation_provider": "local", "translation_model": "other-model"},
            )

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

    def test_asr_model_list_is_exposed(self):
        self.assertEqual(
            self.application.list_subtitle_asr_engines(),
            {"models": [DEFAULT_ASR_MODEL, "faster-whisper/large-v3"]},
        )


class SubtitleAsrCacheReuseTest(unittest.TestCase):
    def test_switching_asr_model_reuses_audio_but_replaces_transcript(self):
        class FakeASRClient:
            def __init__(self):
                self.transcribe_calls = 0
                self.unload_calls = 0

            def transcribe(self, *_args, **_kwargs):
                self.transcribe_calls += 1
                return {
                    "segments": [
                        {"id": 0, "start": 0.0, "end": 2.0, "text": "こんにちは。"}
                    ]
                }

            def unload_asr_model(self):
                self.unload_calls += 1
                return "faster-whisper/large-v3"

        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            task_id = "c" * 32
            cache_dir = root / "cache" / task_id
            cache_dir.mkdir(parents=True)
            (cache_dir / "audio.mp3").write_bytes(b"audio")
            (cache_dir / "transcript.json").write_text(
                json.dumps(
                    {
                        "model": DEFAULT_ASR_MODEL,
                        "segments": [
                            {"id": 0, "start": 0.0, "end": 1.0, "text": "旧结果"}
                        ],
                    }
                ),
                encoding="utf-8",
            )
            config = SimpleNamespace(
                asr_cache_dir=str(root / "cache"),
                subtitle_cache_dir=str(root / "subtitles"),
            )
            processor = SubtitleAsrProcessor(config)
            self.assertEqual(
                processor.cache_state(task_id, "faster-whisper/large-v3"),
                (True, False),
            )
            asr_client = FakeASRClient()
            processor._asr_client = lambda _model=None: asr_client
            processor._msg_client = lambda: FakeMediaMetadataClient()
            processor._translation_client = (
                lambda _provider, _model, _msg=None: FakeCachedTranslationClient()
            )

            result = processor.run(
                task_id,
                "media-whisper",
                "ja",
                asr_model="faster-whisper/large-v3",
                translation_provider="local",
                translation_model="fake-model",
            )

            transcript = json.loads((cache_dir / "transcript.json").read_text(encoding="utf-8"))
            self.assertEqual(transcript["model"], "faster-whisper/large-v3")
            self.assertEqual(transcript["segments"][0]["text"], "こんにちは。")
            self.assertEqual(result["asr_model"], "faster-whisper/large-v3")
            self.assertEqual(asr_client.transcribe_calls, 1)
            self.assertEqual(asr_client.unload_calls, 1)

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
                            {"id": 0, "start": 0.0, "end": 1.5, "text": "こんにちは。"}
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
            msg_client = FakeMediaMetadataClient()
            processor._msg_client = lambda: msg_client
            processor._asr_client = lambda: self.fail("cached transcript must skip SenseVoice")
            translation_client = FakeCachedTranslationClient()
            processor._translation_client = lambda _provider, _model, _msg=None: translation_client
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
            self.assertEqual(translation_client.calls, ["こんにちは。"])
            self.assertEqual(translation_client.unload_calls, 1)
            self.assertTrue((cache_dir / "translations.json").is_file())
            self.assertTrue((cache_dir / "glossary.json").is_file())
            history = load_translation_history(cache_dir / "translation-history.json")
            self.assertEqual(len(history), 1)
            self.assertEqual(history[0]["status"], "completed")
            self.assertEqual(history[0]["model"], "fake-model")
            self.assertEqual(msg_client.get_media_calls, ["media-1"])

            retry_client = FakeCachedTranslationClient()
            processor._translation_client = lambda _provider, _model, _msg=None: retry_client
            retry_progress = []
            processor.run(
                "a" * 32,
                "media-1",
                "en",
                translation_provider="local",
                translation_model="fake-model",
                progress_callback=lambda stage, current, total: retry_progress.append(
                    (stage, current, total)
                ),
            )
            self.assertEqual(retry_client.calls, [])
            self.assertEqual(retry_client.unload_calls, 0)
            self.assertIn(("translating", 1, 1), retry_progress)
            self.assertEqual(msg_client.get_media_calls, ["media-1"])

    def test_failed_translation_unloads_local_model_once_and_keeps_attempt_history(self):
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            cache_dir = root / "cache" / ("b" * 32)
            cache_dir.mkdir(parents=True)
            (cache_dir / "audio.mp3").write_bytes(b"audio")
            (cache_dir / "transcript.json").write_text(
                json.dumps(
                    {
                        "segments": [
                            {"id": 0, "start": 0.0, "end": 1.5, "text": "こんにちは。"}
                        ]
                    }
                ),
                encoding="utf-8",
            )
            (cache_dir / "glossary.json").write_text(
                json.dumps({"media_id": "media-2", "glossary": ""}),
                encoding="utf-8",
            )
            config = SimpleNamespace(
                asr_cache_dir=str(root / "cache"),
                subtitle_cache_dir=str(root / "subtitles"),
            )
            processor = SubtitleAsrProcessor(config)
            processor._msg_client = lambda: self.fail("cached inputs must not call MediaStationGo")
            processor._asr_client = lambda: self.fail("cached transcript must skip SenseVoice")
            translation_client = FakeCachedTranslationClient(fail=True)
            processor._translation_client = lambda _provider, _model, _msg=None: translation_client

            with self.assertRaisesRegex(RuntimeError, "segment 0 after 3 attempts"):
                processor.run(
                    "b" * 32,
                    "media-2",
                    "ja",
                    translation_provider="local",
                    translation_model="fake-model",
                )

            self.assertEqual(translation_client.unload_calls, 1)
            history = load_translation_history(cache_dir / "translation-history.json")
            self.assertEqual(
                [event["status"] for event in history], ["failed", "failed", "failed"]
            )

    def test_corrupt_translation_history_fails_explicitly(self):
        with tempfile.TemporaryDirectory() as tempdir:
            path = Path(tempdir) / "translation-history.json"
            path.write_text("{broken", encoding="utf-8")
            with self.assertRaisesRegex(RuntimeError, "history is invalid"):
                load_translation_history(path)

    def test_glossary_uses_media_titles_and_actor_names(self):
        glossary = build_translation_glossary(
            {
                "display_title": "恶魔阴谋",
                "title": "恶魔阴谋",
                "original_name": "The Devil Conspiracy",
                "actors": "Alice, Bob，Alice",
            }
        )
        self.assertEqual(
            glossary,
            "作品名：恶魔阴谋\n原名：The Devil Conspiracy\n人名：Alice、Bob",
        )


class FakeMediaMetadataClient:
    def __init__(self):
        self.get_media_calls = []

    def get_media(self, media_id):
        self.get_media_calls.append(media_id)
        return {
            "display_title": "测试作品",
            "original_name": "テスト作品",
            "actors": "山田太郎, 佐藤花子",
        }


class FakeCachedTranslationClient:
    def __init__(self, fail=False):
        self.calls = []
        self.unload_calls = 0
        self.fail = fail

    def translate(
        self,
        segments,
        glossary="",
        progress_callback=None,
        cached_translations=None,
        checkpoint_callback=None,
        event_callback=None,
    ):
        def translate_one(text, _context, received_glossary, _retry_instruction):
            self.calls.append(text)
            if self.fail:
                raise RuntimeError("translation failed")
            if "山田太郎" not in received_glossary:
                raise AssertionError("translation glossary was not passed")
            return "你好。"

        return translate_sequentially(
            segments,
            translate_one,
            provider="local",
            model="fake-model",
            glossary=glossary,
            progress_callback=progress_callback,
            cached_translations=cached_translations,
            checkpoint_callback=checkpoint_callback,
            event_callback=event_callback,
            retry_delay_seconds=0,
        )

    def unload_model(self):
        self.unload_calls += 1


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
