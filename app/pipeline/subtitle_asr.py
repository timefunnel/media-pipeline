import http.client
import json
import math
import os
import shutil
import urllib.error
import urllib.parse
import urllib.request
import uuid
from pathlib import Path

from pipeline.external_subtitles import SubtitleCache, SubtitleDownload
from pipeline.llm import LlmTransport
from pipeline.mediastation import MediaStationClient


DEFAULT_ASR_MODEL = "FunAudioLLM/SenseVoiceSmall"
DEFAULT_ASR_TIMEOUT_SECONDS = 1800
DEFAULT_ASR_TRANSLATION_TIMEOUT_SECONDS = 90
DEFAULT_ASR_TRANSLATION_BATCH_SEGMENTS = 25
DEFAULT_ASR_TRANSLATION_BATCH_CHARS = 3500
DEFAULT_ASR_MAX_AUDIO_BYTES = 250 * 1024 * 1024
ASR_SOURCE_LANGUAGES = {"auto", "ja", "en", "zh", "ko"}
ASR_SUBTITLE_SOURCE = "sensevoice-qwen"
ASR_SUBTITLE_PROVIDER_ID = "sensevoice-qwen:zh-CN"
ASR_TRANSLATION_PROVIDERS = {"local", "openai", "deepseek", "siliconflow"}


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

    def translate(self, segments, progress_callback=None):
        segments = validate_asr_segments(segments)
        batches = translation_batches(segments)
        translated = []
        for index, batch in enumerate(batches, start=1):
            translated.extend(self._translate_batch(batch))
            if progress_callback is not None:
                progress_callback(index, len(batches))
        if [item["id"] for item in translated] != [item["id"] for item in segments]:
            raise RuntimeError("AI translation segment IDs do not match the ASR timeline")
        by_id = {item["id"]: item["text"] for item in translated}
        return [{**item, "text": by_id[item["id"]]} for item in segments]

    def _translate_batch(self, segments):
        payload = {
            "model": self.model,
            "max_tokens": 4096,
            "stream": False,
            "response_format": {"type": "json_object"},
            "messages": [
                {
                    "role": "system",
                    "content": (
                        "Translate spoken subtitle dialogue into natural Simplified Chinese (zh-CN). "
                        "Return strict JSON only with schema "
                        '{"translations":[{"id":0,"text":"translated text"}]}. '
                        "Keep every input id exactly once and in the same order. Do not add, omit, merge, "
                        "or split segments. Preserve names, tone, and meaning. Translation text must not be empty."
                    ),
                },
                {
                    "role": "user",
                    "content": json.dumps(
                        {"segments": [{"id": item["id"], "text": item["text"]} for item in segments]},
                        ensure_ascii=False,
                        separators=(",", ":"),
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
        content = llm_message_content(response)
        try:
            result = json.loads(content)
        except ValueError as exc:
            raise RuntimeError("AI translation returned invalid JSON") from exc
        values = result.get("translations") if isinstance(result, dict) else None
        if not isinstance(values, list):
            raise RuntimeError("AI translation response is missing translations")
        return validate_translations(values, [item["id"] for item in segments])


class MediaStationTranslationClient:
    def __init__(self, msg_client, provider, model):
        self.msg_client = msg_client
        self.provider = require_translation_provider(provider)
        self.model = str(model or "").strip()
        if self.provider == "local":
            raise RuntimeError("local translation must use the configured local client")
        if not self.model:
            raise RuntimeError("AI translation model missing")

    def translate(self, segments, progress_callback=None):
        segments = validate_asr_segments(segments)
        batches = translation_batches(segments)
        translated = []
        for index, batch in enumerate(batches, start=1):
            response = self.msg_client.pipeline_translate_subtitles(self.provider, self.model, batch)
            values = response.get("translations") if isinstance(response, dict) else None
            translated.extend(validate_translations(values, [item["id"] for item in batch]))
            if progress_callback is not None:
                progress_callback(index, len(batches))
        if [item["id"] for item in translated] != [item["id"] for item in segments]:
            raise RuntimeError("AI translation segment IDs do not match the ASR timeline")
        by_id = {item["id"]: item["text"] for item in translated}
        return [{**item, "text": by_id[item["id"]]} for item in segments]


class SubtitleAsrProcessor:
    def __init__(self, config):
        self.config = config

    def ensure_available(self, translation_provider="local", translation_model=""):
        if self.config is None or not bool(getattr(self.config, "asr_enabled", False)):
            raise RuntimeError("AI subtitle generation is disabled")
        if not bool(getattr(self.config, "msg_enabled", False)):
            raise RuntimeError("MediaStationGo is disabled in media-pipeline")
        client = self._asr_client()
        client.health()
        provider = require_translation_provider(translation_provider)
        if provider == "local":
            model = str(translation_model or getattr(self.config, "asr_translation_model", "")).strip()
            if model not in client.models():
                raise RuntimeError("local translation model is not installed: %s" % model)
            self._translation_client(provider, model)

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
        if not audio_path.is_file() or audio_path.stat().st_size <= 0:
            partial_audio = cache_dir / "audio.mp3.partial"
            partial_audio.unlink(missing_ok=True)
            emit_progress(progress_callback, "extracting_audio", 0, 0)
            self._msg_client().download_pipeline_asr_audio(
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
            segments = validate_asr_segments(transcript.get("segments"))
            batch_total = len(translation_batches(segments))
            emit_progress(progress_callback, "translating", 0, batch_total)

            def translated_batch(current, total):
                emit_progress(progress_callback, "translating", current, total)

            translated = self._translation_client(provider, model).translate(
                segments, progress_callback=translated_batch
            )
            emit_progress(progress_callback, "saving", batch_total, batch_total)
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

    def _translation_client(self, provider, model):
        if provider == "local":
            return SubtitleTranslationClient(
                getattr(self.config, "asr_translation_base_url", ""),
                getattr(self.config, "asr_translation_api_key", ""),
                model,
                getattr(self.config, "asr_translation_timeout_seconds", DEFAULT_ASR_TRANSLATION_TIMEOUT_SECONDS),
                thinking_disabled=getattr(self.config, "asr_translation_thinking_disabled", True),
            )
        return MediaStationTranslationClient(self._msg_client(), provider, model)

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


def validate_translations(values, expected_ids):
    if not isinstance(values, list):
        raise RuntimeError("AI translation response is missing translations")
    translated = []
    seen = set()
    for item in values:
        if not isinstance(item, dict) or not isinstance(item.get("id"), int):
            raise RuntimeError("AI translation returned an invalid segment")
        segment_id = item["id"]
        text = str(item.get("text") or "").strip()
        if segment_id in seen or not text:
            raise RuntimeError("AI translation returned duplicate or empty segments")
        seen.add(segment_id)
        translated.append({"id": segment_id, "text": text})
    if [item["id"] for item in translated] != list(expected_ids):
        raise RuntimeError("AI translation segment IDs do not match the requested batch")
    return translated


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


def translation_batches(segments):
    batches = []
    current = []
    current_chars = 0
    for item in segments:
        text_length = len(item["text"])
        if current and (
            len(current) >= DEFAULT_ASR_TRANSLATION_BATCH_SEGMENTS
            or current_chars + text_length > DEFAULT_ASR_TRANSLATION_BATCH_CHARS
        ):
            batches.append(current)
            current = []
            current_chars = 0
        current.append(item)
        current_chars += text_length
    if current:
        batches.append(current)
    return batches


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
