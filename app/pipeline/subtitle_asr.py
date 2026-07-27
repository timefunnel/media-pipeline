import http.client
import json
import math
import os
import shutil
import tempfile
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
ASR_SUBTITLE_SOURCE = "sensevoice-deepseek"
ASR_SUBTITLE_PROVIDER_ID = "sensevoice-deepseek:zh-CN"


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
        return payload

    def transcribe(self, audio_path, language="auto"):
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
            with audio_path.open("rb") as audio:
                while True:
                    chunk = audio.read(1024 * 1024)
                    if not chunk:
                        break
                    connection.send(chunk)
            connection.send(epilogue)
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
            raise RuntimeError("LLM_BASE_URL missing")
        if not self.api_key:
            raise RuntimeError("LLM_API_KEY missing for AI subtitle translation")
        if not self.model:
            raise RuntimeError("LLM_MODEL missing for AI subtitle translation")

    def translate(self, segments, progress_callback=None):
        segments = validate_asr_segments(segments)
        batches = translation_batches(segments)
        translated = []
        for index, batch in enumerate(batches, start=1):
            translated.extend(self._translate_batch(batch))
            if progress_callback is not None:
                progress_callback(index, len(batches))
        if [item["id"] for item in translated] != [item["id"] for item in segments]:
            raise RuntimeError("DeepSeek translation segment IDs do not match the ASR timeline")
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
            raise RuntimeError("DeepSeek translation returned invalid JSON") from exc
        values = result.get("translations") if isinstance(result, dict) else None
        if not isinstance(values, list):
            raise RuntimeError("DeepSeek translation response is missing translations")
        expected_ids = [item["id"] for item in segments]
        translated = []
        seen = set()
        for item in values:
            if not isinstance(item, dict) or not isinstance(item.get("id"), int):
                raise RuntimeError("DeepSeek translation returned an invalid segment")
            segment_id = item["id"]
            text = str(item.get("text") or "").strip()
            if segment_id in seen or not text:
                raise RuntimeError("DeepSeek translation returned duplicate or empty segments")
            seen.add(segment_id)
            translated.append({"id": segment_id, "text": text})
        if [item["id"] for item in translated] != expected_ids:
            raise RuntimeError("DeepSeek translation segment IDs do not match the requested batch")
        return translated


class SubtitleAsrProcessor:
    def __init__(self, config):
        self.config = config

    def ensure_available(self):
        if self.config is None or not bool(getattr(self.config, "asr_enabled", False)):
            raise RuntimeError("AI subtitle generation is disabled")
        if not bool(getattr(self.config, "msg_enabled", False)):
            raise RuntimeError("MediaStationGo is disabled in media-pipeline")
        client = self._asr_client()
        client.health()
        self._translation_client()

    def run(self, media_id, source_language, progress_callback=None):
        source_language = str(source_language or "auto").strip().lower()
        if source_language not in ASR_SOURCE_LANGUAGES:
            raise RuntimeError("unsupported ASR source language: %s" % source_language)
        temp_root = tempfile.mkdtemp(prefix="media-pipeline-asr-")
        audio_path = os.path.join(temp_root, "audio.mp3")
        try:
            emit_progress(progress_callback, "extracting_audio", 0, 0)
            self._msg_client().download_pipeline_asr_audio(
                media_id,
                audio_path,
                timeout=self._timeout(),
                max_bytes=DEFAULT_ASR_MAX_AUDIO_BYTES,
            )
            emit_progress(progress_callback, "transcribing", 0, 0)
            transcript = self._asr_client().transcribe(audio_path, source_language)
            if not isinstance(transcript, dict):
                raise RuntimeError("SenseVoice ASR returned an invalid response")
            segments = validate_asr_segments(transcript.get("segments"))
            batch_total = len(translation_batches(segments))
            emit_progress(progress_callback, "translating", 0, batch_total)

            def translated_batch(current, total):
                emit_progress(progress_callback, "translating", current, total)

            translated = self._translation_client().translate(segments, progress_callback=translated_batch)
            emit_progress(progress_callback, "saving", batch_total, batch_total)
            subtitle = build_srt(translated)
            track = SubtitleCache(self.config.subtitle_cache_dir).save_download(
                media_id,
                SubtitleDownload(
                    source=ASR_SUBTITLE_SOURCE,
                    provider_id=ASR_SUBTITLE_PROVIDER_ID,
                    filename="sensevoice-deepseek.zh-CN.srt",
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
            }
        finally:
            shutil.rmtree(temp_root, ignore_errors=True)

    def _timeout(self):
        return max(1, int(getattr(self.config, "asr_timeout_seconds", DEFAULT_ASR_TIMEOUT_SECONDS)))

    def _asr_client(self):
        return SenseVoiceClient(
            getattr(self.config, "asr_base_url", ""),
            getattr(self.config, "asr_api_token", ""),
            model=getattr(self.config, "asr_model", DEFAULT_ASR_MODEL),
            timeout=self._timeout(),
        )

    def _translation_client(self):
        return SubtitleTranslationClient(
            getattr(self.config, "llm_base_url", ""),
            getattr(self.config, "llm_api_key", ""),
            getattr(self.config, "llm_model", ""),
            getattr(self.config, "asr_translation_timeout_seconds", DEFAULT_ASR_TRANSLATION_TIMEOUT_SECONDS),
            thinking_disabled=getattr(self.config, "llm_thinking_disabled", True),
        )

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
        raise RuntimeError("DeepSeek translation returned an invalid response") from exc
    if not isinstance(content, str) or not content.strip():
        raise RuntimeError("DeepSeek translation returned empty content")
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
