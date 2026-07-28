import hashlib
import http.client
import json
import math
import os
import re
import shutil
import time
import unicodedata
import urllib.error
import urllib.parse
import urllib.request
import uuid
from datetime import datetime, timezone
from pathlib import Path

from pipeline.external_subtitles import SubtitleCache, SubtitleDownload
from pipeline.llm import LlmTransport
from pipeline.mediastation import MediaStationClient


DEFAULT_ASR_MODEL = "FunAudioLLM/SenseVoiceSmall"
DEFAULT_ASR_TIMEOUT_SECONDS = 1800
DEFAULT_ASR_TRANSLATION_TIMEOUT_SECONDS = 90
DEFAULT_ASR_TRANSLATION_CONTEXT_SEGMENTS = 4
DEFAULT_ASR_TRANSLATION_ATTEMPTS = 2
DEFAULT_ASR_TRANSLATION_RETRY_DELAY_SECONDS = 1
DEFAULT_ASR_MAX_AUDIO_BYTES = 250 * 1024 * 1024
ASR_SOURCE_LANGUAGES = {"auto", "ja", "en", "zh", "ko"}
ASR_SUBTITLE_SOURCE = "sensevoice-qwen"
ASR_SUBTITLE_PROVIDER_ID = "sensevoice-qwen:zh-CN"
ASR_TRANSLATION_PROVIDERS = {"local", "openai", "deepseek", "siliconflow"}
JAPANESE_KANA_RE = re.compile(r"[\u3040-\u30ff\u31f0-\u31ff]")
JAPANESE_STANDALONE_PARTICLES = frozenset({"て", "を", "に", "へ", "と", "が", "の", "も"})
TARGET_LANGUAGE_INTERJECTIONS = frozenset({"啊", "嗯", "哦", "呀", "哇", "哈", "诶", "唉", "喂"})
TRANSLATION_MODE_TARGET_LANGUAGE = "target_language"
TRANSLATION_MODE_SKIPPED_NONSEMANTIC = "skipped_nonsemantic"


class SenseVoiceClient:
    def __init__(self, base_url, api_token, model=DEFAULT_ASR_MODEL, timeout=DEFAULT_ASR_TIMEOUT_SECONDS):
        parsed = urllib.parse.urlsplit(str(base_url or "").rstrip("/"))
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise RuntimeError("ASR_BASE_URL must be an absolute HTTP URL")
        self.base_url = urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, parsed.path.rstrip("/"), "", ""))
        self.api_token = str(api_token or "").strip()
        if not self.api_token:
            raise RuntimeError("ASR_API_TOKEN missing")
        self.model = str(model or DEFAULT_ASR_MODEL).strip()
        self.timeout = max(1, int(timeout or DEFAULT_ASR_TIMEOUT_SECONDS))

    def health(self):
        request = urllib.request.Request(self.base_url + "/health", method="GET")
        try:
            with urllib.request.urlopen(request, timeout=min(5, self.timeout)) as response:
                raw = response.read(1024 * 1024)
        except (OSError, TimeoutError, urllib.error.URLError) as exc:
            raise RuntimeError("SenseVoice ASR is not running or unreachable: %s" % exc) from exc
        try:
            payload = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, ValueError) as exc:
            raise RuntimeError("SenseVoice ASR health response is invalid") from exc
        if not isinstance(payload, dict) or payload.get("status") != "ok":
            raise RuntimeError("SenseVoice ASR health check failed")
        if payload.get("cuda_available") is not True:
            raise RuntimeError("SenseVoice ASR reports CUDA unavailable")
        if payload.get("llm_available") is False:
            raise RuntimeError("AI subtitle translation service is unavailable")
        return payload

    def models(self):
        request = urllib.request.Request(
            self.base_url + "/v1/models",
            headers={"Authorization": "Bearer " + self.api_token, "Accept": "application/json"},
            method="GET",
        )
        try:
            with urllib.request.urlopen(request, timeout=min(10, self.timeout)) as response:
                raw = response.read(4 * 1024 * 1024 + 1)
        except (OSError, TimeoutError, urllib.error.URLError) as exc:
            raise RuntimeError("SenseVoice model list is unreachable: %s" % exc) from exc
        if len(raw) > 4 * 1024 * 1024:
            raise RuntimeError("SenseVoice model list is too large")
        try:
            payload = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, ValueError) as exc:
            raise RuntimeError("SenseVoice model list is invalid") from exc
        values = payload.get("data") if isinstance(payload, dict) else None
        models = []
        seen = set()
        for item in values if isinstance(values, list) else []:
            model_id = str(item.get("id") or "").strip() if isinstance(item, dict) else ""
            if model_id and model_id not in seen:
                seen.add(model_id)
                models.append(model_id)
        if not models:
            raise RuntimeError("SenseVoice service returned no installed translation models")
        return models

    def transcribe(
        self,
        audio_path,
        language="auto",
        upload_progress_callback=None,
        inference_callback=None,
    ):
        language = str(language or "auto").strip().lower()
        if language not in ASR_SOURCE_LANGUAGES:
            raise RuntimeError("unsupported ASR source language: %s" % language)
        audio_path = Path(audio_path)
        if not audio_path.is_file() or audio_path.stat().st_size <= 0:
            raise RuntimeError("ASR audio file is missing or empty")
        boundary = "----MediaStationASR%s" % uuid.uuid4().hex
        preamble = multipart_preamble(boundary, audio_path.name, self.model, language)
        epilogue = ("\r\n--%s--\r\n" % boundary).encode("ascii")
        content_length = len(preamble) + audio_path.stat().st_size + len(epilogue)
        parsed = urllib.parse.urlsplit(self.base_url + "/v1/audio/transcriptions")
        connection_class = http.client.HTTPSConnection if parsed.scheme == "https" else http.client.HTTPConnection
        connection = connection_class(parsed.hostname, parsed.port, timeout=self.timeout)
        path = urllib.parse.urlunsplit(("", "", parsed.path, parsed.query, ""))
        try:
            connection.putrequest("POST", path)
            connection.putheader("Authorization", "Bearer " + self.api_token)
            connection.putheader("Accept", "application/json")
            connection.putheader("Content-Type", "multipart/form-data; boundary=%s" % boundary)
            connection.putheader("Content-Length", str(content_length))
            connection.endheaders()
            connection.send(preamble)
            sent = 0
            audio_size = audio_path.stat().st_size
            if upload_progress_callback is not None:
                upload_progress_callback(0, audio_size)
            with audio_path.open("rb") as audio:
                while True:
                    chunk = audio.read(256 * 1024)
                    if not chunk:
                        break
                    connection.send(chunk)
                    sent += len(chunk)
                    if upload_progress_callback is not None:
                        upload_progress_callback(sent, audio_size)
            connection.send(epilogue)
            if inference_callback is not None:
                inference_callback()
            response = connection.getresponse()
            raw = response.read(32 * 1024 * 1024 + 1)
            if len(raw) > 32 * 1024 * 1024:
                raise RuntimeError("SenseVoice ASR response is too large")
            if response.status < 200 or response.status >= 300:
                raise RuntimeError("SenseVoice ASR failed: HTTP %s %s" % (response.status, asr_error_message(raw)))
        except (OSError, TimeoutError, http.client.HTTPException) as exc:
            raise RuntimeError("SenseVoice ASR request failed: %s" % exc) from exc
        finally:
            connection.close()
        try:
            return json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, ValueError) as exc:
            raise RuntimeError("SenseVoice ASR returned invalid JSON") from exc


class SubtitleTranslationClient:
    def __init__(self, base_url, api_key, model, timeout, thinking_disabled=True, transport=None):
        self.base_url = str(base_url or "").rstrip("/")
        self.api_key = str(api_key or "").strip()
        self.model = str(model or "").strip()
        self.timeout = max(1, int(timeout or DEFAULT_ASR_TRANSLATION_TIMEOUT_SECONDS))
        self.thinking_disabled = bool(thinking_disabled)
        self.transport = transport or LlmTransport()
        if not self.base_url:
            raise RuntimeError("AI translation base URL missing")
        if not self.api_key:
            raise RuntimeError("AI translation API key missing")
        if not self.model:
            raise RuntimeError("AI translation model missing")

    def translate(
        self,
        segments,
        glossary="",
        progress_callback=None,
        cached_translations=None,
        checkpoint_callback=None,
        event_callback=None,
    ):
        return translate_sequentially(
            segments,
            self._translate_one,
            provider="local",
            model=self.model,
            glossary=glossary,
            progress_callback=progress_callback,
            cached_translations=cached_translations,
            checkpoint_callback=checkpoint_callback,
            event_callback=event_callback,
        )

    def _translate_one(self, text, context, glossary, retry_instruction=""):
        payload = {
            "model": self.model,
            "max_tokens": 1024,
            "temperature": 0.1,
            "top_p": 0.9,
            "stream": False,
            "messages": [
                {
                    "role": "user",
                    "content": subtitle_translation_prompt(
                        text, context, glossary, retry_instruction
                    ),
                },
            ],
        }
        if self.thinking_disabled:
            payload["thinking"] = {"type": "disabled"}
        response = self.transport.request(
            self.base_url + "/chat/completions",
            payload,
            headers={
                "Authorization": "Bearer " + self.api_key,
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
            timeout=self.timeout,
        )
        return llm_message_content(response)

    def unload_model(self):
        response = self.transport.request(
            self.base_url + "/models/unload",
            {"model": self.model},
            headers={
                "Authorization": "Bearer " + self.api_key,
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
            timeout=min(30, self.timeout),
        )
        if not isinstance(response, dict) or response.get("unloaded") != self.model:
            raise RuntimeError("AI translation service did not confirm model unload")


class MediaStationTranslationClient:
    def __init__(self, msg_client, provider, model):
        self.msg_client = msg_client
        self.provider = require_translation_provider(provider)
        self.model = str(model or "").strip()
        if self.provider == "local":
            raise RuntimeError("local translation must use the configured local client")
        if not self.model:
            raise RuntimeError("AI translation model missing")

    def translate(
        self,
        segments,
        glossary="",
        progress_callback=None,
        cached_translations=None,
        checkpoint_callback=None,
        event_callback=None,
    ):
        return translate_sequentially(
            segments,
            self._translate_one,
            provider=self.provider,
            model=self.model,
            glossary=glossary,
            progress_callback=progress_callback,
            cached_translations=cached_translations,
            checkpoint_callback=checkpoint_callback,
            event_callback=event_callback,
        )

    def _translate_one(self, text, context, glossary, retry_instruction=""):
        response = self.msg_client.pipeline_translate_subtitle(
            self.provider,
            self.model,
            text,
            context,
            glossary,
            retry_instruction,
        )
        translation = response.get("translation") if isinstance(response, dict) else None
        return translation


class SubtitleAsrProcessor:
    def __init__(self, config):
        self.config = config

    def ensure_available(self, translation_provider="local", translation_model=""):
        self.ensure_asr_available()
        self.ensure_translation_available(translation_provider, translation_model)

    def ensure_translation_available(self, translation_provider="local", translation_model=""):
        if self.config is None or not bool(getattr(self.config, "asr_enabled", False)):
            raise RuntimeError("AI subtitle generation is disabled")
        if not bool(getattr(self.config, "msg_enabled", False)):
            raise RuntimeError("MediaStationGo is disabled in media-pipeline")
        provider = require_translation_provider(translation_provider)
        model = str(
            translation_model or getattr(self.config, "asr_translation_model", "")
        ).strip()
        if not model:
            raise RuntimeError("AI translation model missing")
        if provider == "local":
            client = self._asr_client()
            client.health()
            if model not in client.models():
                raise RuntimeError("local translation model is not installed: %s" % model)
        self._translation_client(
            provider, model, self._msg_client() if provider != "local" else None
        )

    def translation_models(self):
        self.ensure_asr_available()
        return self._asr_client().models()

    def ensure_asr_available(self):
        if self.config is None or not bool(getattr(self.config, "asr_enabled", False)):
            raise RuntimeError("AI subtitle generation is disabled")
        if not bool(getattr(self.config, "msg_enabled", False)):
            raise RuntimeError("MediaStationGo is disabled in media-pipeline")
        self._asr_client().health()

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
        source_language = str(source_language or "auto").strip().lower()
        if source_language not in ASR_SOURCE_LANGUAGES:
            raise RuntimeError("unsupported ASR source language: %s" % source_language)
        provider = require_translation_provider(translation_provider)
        model = str(translation_model or "").strip()
        if not model:
            raise RuntimeError("AI translation model missing")
        cache_dir = self._task_cache_dir(task_id)
        cache_dir.mkdir(parents=True, exist_ok=True)
        audio_path = cache_dir / "audio.mp3"
        transcript_path = cache_dir / "transcript.json"
        translations_path = cache_dir / "translations.json"
        translation_history_path = cache_dir / "translation-history.json"
        glossary_path = cache_dir / "glossary.json"
        msg_client = None

        def task_msg_client():
            nonlocal msg_client
            if msg_client is None:
                msg_client = self._msg_client()
            return msg_client

        if not audio_path.is_file() or audio_path.stat().st_size <= 0:
            partial_audio = cache_dir / "audio.mp3.partial"
            partial_audio.unlink(missing_ok=True)
            emit_progress(progress_callback, "extracting_audio", 0, 0)
            task_msg_client().download_pipeline_asr_audio(
                media_id,
                partial_audio,
                timeout=self._timeout(),
                max_bytes=DEFAULT_ASR_MAX_AUDIO_BYTES,
                progress_callback=lambda current, total: emit_progress(
                    progress_callback, "extracting_audio", current, total
                ),
            )
            os.replace(partial_audio, audio_path)
            emit_cache(cache_callback, True, False)
        else:
            emit_progress(progress_callback, "using_cached_audio", 1, 1)
            emit_cache(cache_callback, True, False)

        if transcript_path.is_file():
            transcript = load_cached_transcript(transcript_path)
            emit_progress(progress_callback, "using_cached_transcript", 1, 1)
            emit_cache(cache_callback, True, True)
        else:
            emit_progress(progress_callback, "uploading_audio", 0, audio_path.stat().st_size)

            def uploaded_audio(current, total):
                emit_progress(progress_callback, "uploading_audio", current, total)

            transcript = self._asr_client().transcribe(
                audio_path,
                source_language,
                upload_progress_callback=uploaded_audio,
                inference_callback=lambda: emit_progress(
                    progress_callback, "transcribing", 0, 0
                ),
            )
            if not isinstance(transcript, dict):
                raise RuntimeError("SenseVoice ASR returned an invalid response")
            validate_asr_segments(transcript.get("segments"))
            atomic_write_json(transcript_path, transcript)
            emit_cache(cache_callback, True, True)

        try:
            segments = select_translatable_asr_segments(
                validate_asr_segments(transcript.get("segments"))
            )
            glossary = load_or_create_translation_glossary(
                glossary_path, media_id, task_msg_client
            )
            cached_translations = load_translation_cache(
                translations_path, provider, model, segments
            )
            translation_history = load_translation_history(translation_history_path)
            save_translation_cache(
                translations_path, provider, model, segments, cached_translations
            )
            translation_total = len(segments)
            emit_progress(
                progress_callback, "translating", len(cached_translations), translation_total
            )

            def translated_segment(current, total):
                emit_progress(progress_callback, "translating", current, total)

            def checkpoint_translations(values):
                save_translation_cache(
                    translations_path, provider, model, segments, values
                )

            def record_translation_event(event):
                translation_history.append(event)
                save_translation_history(translation_history_path, translation_history)

            translation_client = self._translation_client(
                provider,
                model,
                task_msg_client() if provider != "local" else None,
            )
            translation_requested = len(cached_translations) < translation_total
            try:
                translated = translation_client.translate(
                    segments,
                    glossary=glossary,
                    progress_callback=translated_segment,
                    cached_translations=cached_translations,
                    checkpoint_callback=checkpoint_translations,
                    event_callback=record_translation_event,
                )
            except Exception as translation_error:
                if provider == "local" and translation_requested:
                    try:
                        translation_client.unload_model()
                    except Exception as unload_error:
                        raise RuntimeError(
                            "AI translation failed and local model unload failed: %s"
                            % unload_error
                        ) from translation_error
                raise
            if provider == "local" and translation_requested:
                translation_client.unload_model()
            emit_progress(progress_callback, "saving", translation_total, translation_total)
            subtitle = build_srt(translated)
            track = SubtitleCache(self.config.subtitle_cache_dir).save_download(
                media_id,
                SubtitleDownload(
                    source=ASR_SUBTITLE_SOURCE,
                    provider_id=ASR_SUBTITLE_PROVIDER_ID,
                    filename="sensevoice-qwen.zh-CN.srt",
                    body=subtitle.encode("utf-8"),
                    lang="zh-CN",
                    label="AI 简体中文",
                    query=source_language,
                ),
            )
            return {
                "filename": str(track.get("filename") or ""),
                "source": ASR_SUBTITLE_SOURCE,
                "language": "zh-CN",
                "segment_count": len(translated),
                "duration": float(translated[-1]["end"]),
                "translation_provider": provider,
                "translation_model": model,
            }
        except Exception:
            emit_cache(cache_callback, audio_path.is_file(), transcript_path.is_file())
            raise

    def cache_state(self, task_id):
        cache_dir = self._task_cache_dir(task_id)
        audio_path = cache_dir / "audio.mp3"
        transcript_path = cache_dir / "transcript.json"
        audio_cached = audio_path.is_file() and audio_path.stat().st_size > 0
        transcript_cached = False
        if transcript_path.is_file():
            load_cached_transcript(transcript_path)
            transcript_cached = True
        return audio_cached, transcript_cached

    def delete_cache(self, task_id):
        cache_dir = self._task_cache_dir(task_id)
        if cache_dir.exists():
            shutil.rmtree(cache_dir)

    def _task_cache_dir(self, task_id):
        task_id = str(task_id or "").strip()
        if not task_id or any(char not in "0123456789abcdef" for char in task_id.lower()):
            raise RuntimeError("invalid AI subtitle task ID")
        root = Path(getattr(self.config, "asr_cache_dir", "/bot-data/subtitle-asr-cache"))
        return root / task_id

    def _timeout(self):
        return max(1, int(getattr(self.config, "asr_timeout_seconds", DEFAULT_ASR_TIMEOUT_SECONDS)))

    def _asr_client(self):
        return SenseVoiceClient(
            getattr(self.config, "asr_base_url", ""),
            getattr(self.config, "asr_api_token", ""),
            model=getattr(self.config, "asr_model", DEFAULT_ASR_MODEL),
            timeout=self._timeout(),
        )

    def _translation_client(self, provider, model, msg_client=None):
        if provider == "local":
            return SubtitleTranslationClient(
                getattr(self.config, "asr_translation_base_url", ""),
                getattr(self.config, "asr_translation_api_key", ""),
                model,
                getattr(self.config, "asr_translation_timeout_seconds", DEFAULT_ASR_TRANSLATION_TIMEOUT_SECONDS),
                thinking_disabled=getattr(self.config, "asr_translation_thinking_disabled", True),
            )
        if msg_client is None:
            msg_client = self._msg_client()
        return MediaStationTranslationClient(msg_client, provider, model)

    def _msg_client(self):
        username = str(getattr(self.config, "msg_admin_user", "") or "").strip()
        password = str(getattr(self.config, "msg_admin_password", "") or "")
        if not username or not password:
            raise RuntimeError("MediaStationGo credentials missing")
        return MediaStationClient(getattr(self.config, "msg_base_url", ""), username, password)


def multipart_preamble(boundary, filename, model, language):
    safe_filename = str(filename or "audio.mp3").replace('"', "")
    fields = []
    for name, value in (("model", model), ("language", language)):
        fields.append(
            "--%s\r\nContent-Disposition: form-data; name=\"%s\"\r\n\r\n%s\r\n"
            % (boundary, name, value)
        )
    fields.append(
        "--%s\r\nContent-Disposition: form-data; name=\"file\"; filename=\"%s\"\r\n"
        "Content-Type: audio/mpeg\r\n\r\n" % (boundary, safe_filename)
    )
    return "".join(fields).encode("utf-8")


def asr_error_message(raw):
    try:
        payload = json.loads(raw.decode("utf-8", "replace"))
    except ValueError:
        return raw.decode("utf-8", "replace")[:500]
    if isinstance(payload, dict):
        return str(payload.get("detail") or payload.get("error") or payload)[:500]
    return str(payload)[:500]


def validate_asr_segments(values):
    if not isinstance(values, list) or not values:
        raise RuntimeError("SenseVoice ASR returned no timestamped segments")
    out = []
    for index, item in enumerate(values):
        if not isinstance(item, dict):
            raise RuntimeError("SenseVoice ASR returned an invalid segment")
        segment_id = item.get("id")
        start = item.get("start")
        end = item.get("end")
        text = str(item.get("text") or "").strip()
        if segment_id != index or not isinstance(start, (int, float)) or not isinstance(end, (int, float)):
            raise RuntimeError("SenseVoice ASR returned an invalid segment timeline")
        if not math.isfinite(float(start)) or not math.isfinite(float(end)) or float(end) <= float(start) or not text:
            raise RuntimeError("SenseVoice ASR returned an invalid segment timeline")
        out.append({"id": segment_id, "start": float(start), "end": float(end), "text": text})
    return out


def select_translatable_asr_segments(segments):
    selected = []
    for item in validate_asr_segments(segments):
        if not any(unicodedata.category(char)[0] in {"L", "N"} for char in item["text"]):
            continue
        selected.append({**item, "id": len(selected)})
    if not selected:
        raise RuntimeError("SenseVoice ASR returned no translatable speech segments")
    return selected


def validate_translation_text(source, value):
    if not isinstance(value, str) or not value.strip():
        raise RuntimeError("AI translation returned empty content")
    translated = value.strip()
    if "```" in translated or translated.startswith(("{", "[")):
        raise RuntimeError("AI translation returned structured content instead of plain text")
    if "<think" in translated.lower() or "</think>" in translated.lower():
        raise RuntimeError("AI translation returned reasoning content instead of plain text")
    if translated.startswith(("译文：", "翻译：", "翻译结果：", "Translation:")):
        raise RuntimeError("AI translation returned an explanation instead of plain text")
    if normalize_translation_text(source) == normalize_translation_text(translated):
        raise RuntimeError("AI translation returned the untranslated source text")
    if JAPANESE_KANA_RE.search(translated):
        raise RuntimeError("AI translation still contains Japanese kana")

    source_length = len(str(source))
    translated_length = len(translated)
    maximum_length = max(80, source_length * 4 + 20)
    if translated_length > maximum_length:
        raise RuntimeError("AI translation length is abnormally large")
    if source_length >= 20 and translated_length < source_length // 10:
        raise RuntimeError("AI translation length is abnormally small")
    return translated


def normalize_translation_text(value):
    return "".join(str(value or "").split()).casefold()


def subtitle_segment_program_mode(value):
    text = "".join(
        char
        for char in str(value or "").strip()
        if unicodedata.category(char)[0] in {"L", "N"}
    )
    if text in JAPANESE_STANDALONE_PARTICLES:
        return TRANSLATION_MODE_SKIPPED_NONSEMANTIC
    if text in TARGET_LANGUAGE_INTERJECTIONS:
        return TRANSLATION_MODE_TARGET_LANGUAGE
    return ""


def validate_cached_translations(values, segments):
    if values is None:
        return []
    if not isinstance(values, list):
        raise RuntimeError("cached AI translations are invalid")
    segment_by_id = {item["id"]: item for item in segments}
    expected_ids = set(segment_by_id)
    translated = []
    seen = set()
    for item in values:
        if not isinstance(item, dict) or not isinstance(item.get("id"), int):
            raise RuntimeError("cached AI translations contain an invalid segment")
        segment_id = item["id"]
        if segment_id not in expected_ids or segment_id in seen:
            raise RuntimeError("cached AI translations contain an unexpected segment")
        seen.add(segment_id)
        mode = str(item.get("mode") or "").strip()
        if not mode:
            translated.append(
                {"id": segment_id, "text": validate_translation_text_for_cache(item.get("text"))}
            )
            continue
        expected_mode = subtitle_segment_program_mode(segment_by_id[segment_id]["text"])
        if mode != expected_mode:
            raise RuntimeError("cached AI translations contain an invalid program-handled segment")
        if mode == TRANSLATION_MODE_TARGET_LANGUAGE:
            text = validate_translation_text_for_cache(item.get("text"))
            if normalize_translation_text(text) != normalize_translation_text(
                segment_by_id[segment_id]["text"]
            ):
                raise RuntimeError("cached AI translations contain an invalid target-language segment")
            translated.append({"id": segment_id, "text": text, "mode": mode})
            continue
        if mode == TRANSLATION_MODE_SKIPPED_NONSEMANTIC:
            skipped_text = item.get("text")
            if skipped_text is not None and skipped_text != "":
                raise RuntimeError("cached AI translations contain an invalid skipped segment")
            translated.append({"id": segment_id, "mode": mode})
            continue
        raise RuntimeError("cached AI translations contain an unsupported program mode")
    return sorted(translated, key=lambda item: item["id"])


def validate_translation_text_for_cache(value):
    if not isinstance(value, str) or not value.strip():
        raise RuntimeError("cached AI translations contain empty content")
    return value.strip()


def translate_sequentially(
    segments,
    translate_one,
    provider,
    model,
    glossary="",
    progress_callback=None,
    cached_translations=None,
    checkpoint_callback=None,
    event_callback=None,
    retry_delay_seconds=DEFAULT_ASR_TRANSLATION_RETRY_DELAY_SECONDS,
):
    segments = validate_asr_segments(segments)
    cached = validate_cached_translations(cached_translations, segments)
    handled_ids = {item["id"] for item in cached}
    by_id = {
        item["id"]: item["text"]
        for item in cached
        if item.get("mode") != TRANSLATION_MODE_SKIPPED_NONSEMANTIC
    }
    mode_by_id = {item["id"]: item.get("mode", "") for item in cached}
    total = len(segments)
    if progress_callback is not None:
        progress_callback(len(handled_ids), total)

    def checkpoint_values():
        values = []
        for item in segments:
            segment_id = item["id"]
            if segment_id not in handled_ids:
                continue
            mode = mode_by_id.get(segment_id, "")
            if mode == TRANSLATION_MODE_SKIPPED_NONSEMANTIC:
                values.append({"id": segment_id, "mode": mode})
                continue
            value = {"id": segment_id, "text": by_id[segment_id]}
            if mode:
                value["mode"] = mode
            values.append(value)
        return values

    for index, segment in enumerate(segments):
        segment_id = segment["id"]
        if segment_id in handled_ids:
            continue
        program_mode = subtitle_segment_program_mode(segment["text"])
        if program_mode:
            handled_ids.add(segment_id)
            mode_by_id[segment_id] = program_mode
            if program_mode == TRANSLATION_MODE_TARGET_LANGUAGE:
                by_id[segment_id] = segment["text"]
            if checkpoint_callback is not None:
                checkpoint_callback(checkpoint_values())
            if progress_callback is not None:
                progress_callback(len(handled_ids), total)
            continue
        context = [
            item["text"]
            for item in segments[
                max(0, index - DEFAULT_ASR_TRANSLATION_CONTEXT_SEGMENTS) : index
            ]
        ]
        retry_instruction = ""
        for attempt in range(1, DEFAULT_ASR_TRANSLATION_ATTEMPTS + 1):
            started = time.monotonic()
            try:
                raw_translation = translate_one(
                    segment["text"], context, glossary, retry_instruction
                )
                translation = validate_translation_text(segment["text"], raw_translation)
                validate_repeated_translation(segments, by_id, segment, translation)
            except Exception as exc:
                emit_translation_event(
                    event_callback,
                    segment_id,
                    provider,
                    model,
                    attempt,
                    started,
                    "failed",
                    str(exc),
                )
                if attempt >= DEFAULT_ASR_TRANSLATION_ATTEMPTS:
                    raise RuntimeError(
                        "AI translation failed for segment %s after %s attempts: %s"
                        % (segment_id, attempt, exc)
                    ) from exc
                retry_instruction = translation_retry_instruction(exc)
                time.sleep(max(0, float(retry_delay_seconds)))
                continue

            emit_translation_event(
                event_callback,
                segment_id,
                provider,
                model,
                attempt,
                started,
                "completed",
                "",
            )
            by_id[segment_id] = translation
            handled_ids.add(segment_id)
            if checkpoint_callback is not None:
                checkpoint_callback(checkpoint_values())
            if progress_callback is not None:
                progress_callback(len(handled_ids), total)
            break

    if len(handled_ids) != total:
        raise RuntimeError("AI translation did not cover the ASR timeline")
    translated = [
        {**item, "text": by_id[item["id"]]}
        for item in segments
        if mode_by_id.get(item["id"]) != TRANSLATION_MODE_SKIPPED_NONSEMANTIC
    ]
    if not translated:
        raise RuntimeError("AI translation produced no subtitle content after filtering")
    return translated


def translation_retry_instruction(error):
    message = str(error or "")
    if "Japanese kana" in message:
        return "上次译文仍含日文假名，请将全部内容译成自然的简体中文。"
    if "length is abnormally" in message:
        return "上次译文长度异常，请只翻译当前字幕并保持简洁。"
    if "untranslated source text" in message:
        return "上次输出未完成翻译，请输出对应的简体中文译文。"
    if "structured content" in message or "reasoning content" in message or "explanation" in message:
        return "上次输出包含格式或解释，请只输出纯中文译文。"
    if "same text for three different segments" in message:
        return "上次译文与前文异常重复，请根据当前原文重新翻译。"
    return ""


def validate_repeated_translation(segments, translated_by_id, current_segment, translation):
    current_id = current_segment["id"]
    if current_id < 2 or len(translation) < 6:
        return
    previous_ids = (current_id - 2, current_id - 1)
    if any(segment_id not in translated_by_id for segment_id in previous_ids):
        return
    normalized = normalize_translation_text(translation)
    if any(
        normalize_translation_text(translated_by_id[segment_id]) != normalized
        for segment_id in previous_ids
    ):
        return
    source_by_id = {item["id"]: normalize_translation_text(item["text"]) for item in segments}
    sources = {source_by_id[current_id], *(source_by_id[segment_id] for segment_id in previous_ids)}
    if len(sources) == 3:
        raise RuntimeError("AI translation returned the same text for three different segments")


def emit_translation_event(callback, segment_id, provider, model, attempt, started, status, error):
    if callback is None:
        return
    event = {
        "segment_id": segment_id,
        "provider": provider,
        "model": model,
        "parameters": {"temperature": 0.1, "top_p": 0.9, "max_tokens": 1024},
        "attempt": attempt,
        "duration_ms": max(0, round((time.monotonic() - started) * 1000)),
        "status": status,
        "recorded_at": datetime.now(timezone.utc).isoformat(),
    }
    if error:
        event["error"] = error
    callback(event)


def require_translation_provider(provider):
    provider = str(provider or "local").strip().lower()
    if provider not in ASR_TRANSLATION_PROVIDERS:
        raise RuntimeError("unsupported AI translation provider: %s" % provider)
    return provider


def load_cached_transcript(path):
    try:
        with Path(path).open("r", encoding="utf-8") as source:
            payload = json.load(source)
    except (OSError, ValueError) as exc:
        raise RuntimeError("cached SenseVoice transcript is invalid: %s" % exc) from exc
    if not isinstance(payload, dict):
        raise RuntimeError("cached SenseVoice transcript is invalid")
    validate_asr_segments(payload.get("segments"))
    return payload


def translation_source_sha256(segments):
    source = json.dumps(
        [{"id": item["id"], "text": item["text"]} for item in segments],
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(source).hexdigest()


def load_translation_cache(path, provider, model, segments):
    path = Path(path)
    if not path.is_file():
        return []
    try:
        with path.open("r", encoding="utf-8") as source:
            payload = json.load(source)
    except (OSError, ValueError) as exc:
        raise RuntimeError("cached AI translations are invalid: %s" % exc) from exc
    if not isinstance(payload, dict):
        raise RuntimeError("cached AI translations are invalid")
    if (
        payload.get("provider") != provider
        or payload.get("model") != model
        or payload.get("source_sha256") != translation_source_sha256(segments)
    ):
        return []
    return validate_cached_translations(payload.get("translations"), segments)


def save_translation_cache(path, provider, model, segments, translations):
    validated = validate_cached_translations(translations, segments)
    atomic_write_json(
        path,
        {
            "provider": provider,
            "model": model,
            "source_sha256": translation_source_sha256(segments),
            "translations": validated,
        },
    )


def load_or_create_translation_glossary(path, media_id, msg_client_factory):
    path = Path(path)
    if path.is_file():
        try:
            with path.open("r", encoding="utf-8") as source:
                payload = json.load(source)
        except (OSError, ValueError) as exc:
            raise RuntimeError("cached AI translation glossary is invalid: %s" % exc) from exc
        if not isinstance(payload, dict) or payload.get("media_id") != media_id:
            raise RuntimeError("cached AI translation glossary is invalid")
        glossary = payload.get("glossary")
        if not isinstance(glossary, str):
            raise RuntimeError("cached AI translation glossary is invalid")
        return glossary

    media = msg_client_factory().get_media(media_id)
    if not isinstance(media, dict):
        raise RuntimeError("MediaStationGo returned invalid media metadata for translation glossary")
    glossary = build_translation_glossary(media)
    atomic_write_json(path, {"media_id": media_id, "glossary": glossary})
    return glossary


def build_translation_glossary(media):
    title = first_nonempty_media_string(media, "display_title", "title")
    original_name = first_nonempty_media_string(media, "original_name")
    actors = media.get("actors")
    if actors is None:
        actor_names = []
    elif isinstance(actors, str):
        actor_names = [item.strip() for item in re.split(r"[,，、;；]", actors) if item.strip()]
    elif isinstance(actors, list):
        actor_names = [str(item).strip() for item in actors if str(item).strip()]
    else:
        raise RuntimeError("MediaStationGo media actors are invalid for translation glossary")

    lines = []
    if title:
        lines.append("作品名：" + title)
    if original_name and normalize_translation_text(original_name) != normalize_translation_text(title):
        lines.append("原名：" + original_name)
    if actor_names:
        lines.append("人名：" + "、".join(dict.fromkeys(actor_names)))
    return "\n".join(lines)


def first_nonempty_media_string(media, *keys):
    for key in keys:
        value = media.get(key)
        if value is None:
            continue
        if not isinstance(value, str):
            raise RuntimeError("MediaStationGo media %s is invalid for translation glossary" % key)
        if value.strip():
            return value.strip()
    return ""


def load_translation_history(path):
    path = Path(path)
    if not path.is_file():
        return []
    try:
        with path.open("r", encoding="utf-8") as source:
            payload = json.load(source)
    except (OSError, ValueError) as exc:
        raise RuntimeError("AI translation history is invalid: %s" % exc) from exc
    if not isinstance(payload, dict) or payload.get("version") != 1:
        raise RuntimeError("AI translation history is invalid")
    return validate_translation_history(payload.get("events"))


def save_translation_history(path, events):
    atomic_write_json(path, {"version": 1, "events": validate_translation_history(events)})


def validate_translation_history(events):
    if not isinstance(events, list):
        raise RuntimeError("AI translation history is invalid")
    validated = []
    required = {
        "segment_id",
        "provider",
        "model",
        "parameters",
        "attempt",
        "duration_ms",
        "status",
        "recorded_at",
    }
    for event in events:
        if not isinstance(event, dict) or not required.issubset(event):
            raise RuntimeError("AI translation history contains an invalid event")
        if event.get("status") not in {"completed", "failed"}:
            raise RuntimeError("AI translation history contains an invalid event")
        if event.get("status") == "failed" and not str(event.get("error") or "").strip():
            raise RuntimeError("AI translation history contains an invalid failure event")
        validated.append(dict(event))
    return validated


def atomic_write_json(path, payload):
    path = Path(path)
    partial = path.with_name(path.name + ".partial")
    with partial.open("w", encoding="utf-8", newline="\n") as output:
        json.dump(payload, output, ensure_ascii=False, separators=(",", ":"))
        output.flush()
        os.fsync(output.fileno())
    os.replace(partial, path)


def emit_cache(callback, audio_cached, transcript_cached):
    if callback is not None:
        callback(bool(audio_cached), bool(transcript_cached))


def subtitle_translation_prompt(text, context, glossary, retry_instruction=""):
    context_text = "\n".join(context) if context else "（无）"
    glossary_text = str(glossary or "").strip() or "（无）"
    retry_text = str(retry_instruction or "").strip()
    return (
        "参考上下文：\n"
        + context_text
        + "\n\n术语参考：\n"
        + glossary_text
        + ("\n\n重试要求：\n" + retry_text if retry_text else "")
        + "\n\n将下面的日文翻译成自然、准确的简体中文。\n"
        + "只输出译文，不要解释：\n\n"
        + text
    )


def llm_message_content(response):
    try:
        content = response["choices"][0]["message"]["content"]
    except (KeyError, IndexError, TypeError) as exc:
        raise RuntimeError("AI translation returned an invalid response") from exc
    if not isinstance(content, str) or not content.strip():
        raise RuntimeError("AI translation returned empty content")
    return content.strip()


def build_srt(segments):
    lines = []
    for index, item in enumerate(segments, start=1):
        lines.extend(
            [
                str(index),
                "%s --> %s" % (format_srt_timestamp(item["start"]), format_srt_timestamp(item["end"])),
                str(item["text"]).strip(),
                "",
            ]
        )
    return "\n".join(lines)


def format_srt_timestamp(seconds):
    total_ms = max(0, int(round(float(seconds) * 1000)))
    hours, remainder = divmod(total_ms, 3600000)
    minutes, remainder = divmod(remainder, 60000)
    whole_seconds, milliseconds = divmod(remainder, 1000)
    return "%02d:%02d:%02d,%03d" % (hours, minutes, whole_seconds, milliseconds)


def emit_progress(callback, stage, current, total):
    if callback is not None:
        callback(stage, int(current), int(total))
