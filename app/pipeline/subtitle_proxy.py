import base64
import hashlib
import http.server
import io
import json
import os
import posixpath
import re
import socketserver
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

try:
    from PIL import Image, ImageOps
except ImportError:
    Image = None
    ImageOps = None

from pipeline.openlist import DEFAULT_OPENLIST_URL, OpenListClient, OpenListPasswordTokenProvider
from pipeline.external_subtitles import DEFAULT_SUBTITLE_CACHE_DIR, LocalSubtitleProvider, local_subtitle_uri_valid


DEFAULT_SUBTITLE_PROXY_HOST = "127.0.0.1"
DEFAULT_SUBTITLE_PROXY_PORT = 18081
DEFAULT_SUBTITLE_PROXY_UPSTREAM = "http://127.0.0.1:18080"
DEFAULT_MSG_API_BASE_URL = "http://127.0.0.1:18080/api"
EMBY_TICKS_PER_SECOND = 10_000_000
MIN_SYNTHETIC_RUNTIME_TICKS = 10 * 60 * EMBY_TICKS_PER_SECOND
SYNTHETIC_RUNTIME_PADDING_TICKS = 60 * 60 * EMBY_TICKS_PER_SECOND

HOP_BY_HOP_HEADERS = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
}

WEBVTT_TIMESTAMP_RE = re.compile(r"(?P<time>\d{2}:\d{2}:\d{2})\.(?P<fraction>\d{1,2})(?=\s|$)")
SRT_TIMESTAMP_RE = re.compile(r"(?P<time>\d{2}:\d{2}:\d{2}),(?P<fraction>\d{1,3})(?=\s|$)")
ASS_EVENT_TIME_RE = re.compile(r"^(?P<hour>\d+):(?P<minute>\d{2}):(?P<second>\d{2})\.(?P<fraction>\d{1,2})$")
ASS_OVERRIDE_RE = re.compile(r"\{[^}]*\}")
SENSITIVE_QUERY_RE = re.compile(r"([?&](?:api_?key|access_token|token)=)[^&\s\"]+", re.IGNORECASE)
EMBY_MEDIA_ID_PATTERN = r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
EMBY_PATH_PREFIX_PATTERN = r"(?:/emby)?"
EMBY_ITEM_ID_RE = re.compile(
    r"^%s/(?:Users/[^/]+/)?Items/(?P<media_id>%s)(?:/PlaybackInfo)?/?$" % (EMBY_PATH_PREFIX_PATTERN, EMBY_MEDIA_ID_PATTERN),
    re.IGNORECASE,
)
EMBY_SUBTITLE_STREAM_RE = re.compile(
    r"^%s/Videos/(?P<media_id>[^/]+)/(?P<source_id>[^/]+)/Subtitles/(?P<stream_index>\d+)/Stream\.(?P<extension>vtt|srt|ass|ssa)$"
    % EMBY_PATH_PREFIX_PATTERN,
    re.IGNORECASE,
)
EMBY_ITEM_IMAGE_RE = re.compile(
    r"^%s/Items/(?P<item_id>[^/]+)/Images/(?P<image_type>Primary|Backdrop|Thumb)(?:/\d+)?/?$" % EMBY_PATH_PREFIX_PATTERN,
    re.IGNORECASE,
)
EMBY_USER_ITEMS_RE = re.compile(r"^%s/Users/(?P<user_id>[^/]+)/Items/?$" % EMBY_PATH_PREFIX_PATTERN, re.IGNORECASE)
EMBY_FOLDER_COVER_CACHE_TTL_SECONDS = 300
EMBY_FOLDER_COVER_GRID_LIMIT = 4
EMBY_FOLDER_COVER_ASPECT_RATIO = 16 / 9
EMBY_FOLDER_COVER_DEFAULT_WIDTH = 960
EMBY_FOLDER_COVER_MIN_WIDTH = 160
EMBY_FOLDER_COVER_MAX_WIDTH = 1200
EMBY_FOLDER_COVER_TAG_LENGTH = 32
EMBY_FOLDER_COVER_TAG_VERSION = "folder-cover-grid-v6-jellyfin-shape"
SUBTITLE_EXTENSIONS = {".ass", ".srt", ".ssa", ".vtt"}
VIDEO_EXTENSIONS = {
    ".avi",
    ".flv",
    ".m2ts",
    ".m4v",
    ".mkv",
    ".mov",
    ".mp4",
    ".mpeg",
    ".mpg",
    ".rmvb",
    ".ts",
    ".vob",
    ".webm",
    ".wmv",
}
SUBTITLE_TRACK_BOOTSTRAP = """
<script>
(function () {
  window.__subtitleProxyDebug = window.__subtitleProxyDebug || { loads: [] };
  function timeToSeconds(value) {
    var parts = value.trim().split(":");
    if (parts.length !== 3) return 0;
    var seconds = parts[2].replace(",", ".").split(".");
    return Number(parts[0]) * 3600 + Number(parts[1]) * 60 + Number(seconds[0]) + Number(seconds[1] || 0) / 1000;
  }
  function cleanCueText(lines) {
    return lines.join("\\n").replace(/\\\\N/g, "\\n").replace(/\\{[^}]*\\}/g, "").trim();
  }
  function parseWebVtt(text) {
    var lines = String(text || "").replace(/\\r/g, "").split("\\n");
    var cues = [];
    for (var index = 0; index < lines.length; index += 1) {
      var line = lines[index];
      if (line.indexOf("-->") < 0) continue;
      var parts = line.split("-->");
      var start = timeToSeconds(parts[0].trim());
      var end = timeToSeconds(parts[1].trim().split(/\\s+/)[0]);
      var body = [];
      index += 1;
      while (index < lines.length && lines[index].trim() !== "") {
        body.push(lines[index]);
        index += 1;
      }
      var textValue = cleanCueText(body);
      if (end > start && textValue) cues.push({ start: start, end: end, text: textValue });
    }
    return cues;
  }
  function requestText(url, callback) {
    if (typeof window.fetch === "function") {
      window.fetch(url, { credentials: "same-origin" }).then(function (response) {
        if (!response.ok) throw new Error("subtitle status " + response.status);
        return response.text();
      }).then(function (text) {
        callback(null, text);
      }).catch(function (error) {
        callback(error);
      });
      return;
    }
    if (typeof window.XMLHttpRequest !== "function") {
      callback(new Error("no fetch or XMLHttpRequest"));
      return;
    }
    var xhr = new XMLHttpRequest();
    xhr.open("GET", url, true);
    xhr.onreadystatechange = function () {
      if (xhr.readyState !== 4) return;
      if (xhr.status >= 200 && xhr.status < 300) callback(null, xhr.responseText || "");
      else callback(new Error("subtitle status " + xhr.status));
    };
    xhr.onerror = function () { callback(new Error("subtitle request failed")); };
    xhr.send();
  }
  function selectedTrack(video) {
    var tracks = Array.prototype.slice.call(video.querySelectorAll("track"));
    if (!tracks.length) return null;
    return tracks.find(function (track) { return track.default; }) || tracks[0];
  }
  function ensureOverlay(video) {
    var overlay = document.createElement("div");
    overlay.dataset.subtitleOverlay = "1";
    overlay.style.cssText = [
      "position:fixed",
      "left:0",
      "top:0",
      "width:0",
      "box-sizing:border-box",
      "padding:0 4vw",
      "text-align:center",
      "white-space:pre-line",
      "font:600 clamp(18px,2.4vw,34px) sans-serif",
      "line-height:1.35",
      "color:#fff",
      "text-shadow:0 2px 4px #000,0 0 8px #000",
      "pointer-events:none",
      "z-index:2147483647"
    ].join(";");
    document.body.appendChild(overlay);
    function position() {
      if (!document.body.contains(video)) {
        overlay.remove();
        return;
      }
      var rect = video.getBoundingClientRect();
      overlay.style.left = rect.left + "px";
      overlay.style.width = rect.width + "px";
      overlay.style.top = (rect.bottom - Math.max(54, rect.height * 0.08)) + "px";
      overlay.style.transform = "translateY(-100%)";
    }
    window.addEventListener("resize", position);
    window.addEventListener("scroll", position, true);
    position();
    return { element: overlay, position: position };
  }
  function enableDefaultSubtitle(video) {
    if (!video || video.dataset.subtitleAutoEnabled === "1") return;
    video.dataset.subtitleAutoEnabled = "1";
    var overlay = ensureOverlay(video);
    overlay.element.dataset.subtitleStatus = "initializing";
    var cues = [];
    var loadedTrackSrc = "";
    var retryTimer = null;
    function render() {
      overlay.position();
      var now = video.currentTime || 0;
      var active = cues.filter(function (cue) { return now >= cue.start && now <= cue.end; }).map(function (cue) { return cue.text; });
      overlay.element.dataset.subtitleCurrentTime = String(now);
      overlay.element.dataset.subtitleActiveCount = String(active.length);
      overlay.element.textContent = active.join("\\n");
    }
    function load() {
      var track = selectedTrack(video);
      if (!track || !track.src) return;
      if (track.src === loadedTrackSrc) return;
      loadedTrackSrc = track.src;
      overlay.element.dataset.subtitleStatus = "loading";
      window.__subtitleProxyDebug.loads.push({ src: track.src, at: Date.now(), status: "loading" });
      Array.prototype.forEach.call(video.textTracks || [], function (textTrack) {
        textTrack.mode = "disabled";
      });
      requestText(track.src, function (error, text) {
        var current = window.__subtitleProxyDebug.loads[window.__subtitleProxyDebug.loads.length - 1];
        if (error) {
          loadedTrackSrc = "";
          if (current) {
            current.status = "error";
            current.error = String(error && error.message || error);
          }
          overlay.element.dataset.subtitleStatus = "error";
          overlay.element.dataset.subtitleError = String(error && error.message || error);
          return;
        }
        cues = parseWebVtt(text);
        overlay.element.dataset.subtitleStatus = "loaded";
        overlay.element.dataset.subtitleCueCount = String(cues.length);
        if (current) {
          current.status = "loaded";
          current.cueCount = cues.length;
        }
        if (retryTimer) {
          window.clearInterval(retryTimer);
          retryTimer = null;
        }
        render();
      });
    }
    video.addEventListener("timeupdate", render);
    video.addEventListener("seeked", render);
    video.addEventListener("loadedmetadata", load);
    video.addEventListener("loadeddata", render);
    load();
    window.setTimeout(load, 500);
    window.setTimeout(load, 1000);
    window.setTimeout(load, 2000);
    window.setTimeout(load, 4000);
    window.setTimeout(render, 1500);
    retryTimer = window.setInterval(load, 1000);
    try {
      new MutationObserver(load).observe(video, { childList: true, subtree: true, attributes: true, attributeFilter: ["src"] });
    } catch (error) {
      window.__subtitleProxyDebug.observerError = String(error && error.message || error);
    }
  }
  function scan() {
    Array.prototype.forEach.call(document.querySelectorAll("video"), enableDefaultSubtitle);
  }
  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", scan);
  }
  scan();
  new MutationObserver(scan).observe(document.documentElement, { childList: true, subtree: true });
})();
</script>
"""


def normalize_webvtt_timestamps(text):
    lines = []
    for line in text.splitlines(keepends=True):
        if "-->" not in line:
            lines.append(line)
            continue
        lines.append(WEBVTT_TIMESTAMP_RE.sub(_pad_webvtt_fraction, line))
    return "".join(lines)


def _pad_webvtt_fraction(match):
    fraction = match.group("fraction")
    return "%s.%s" % (match.group("time"), fraction.ljust(3, "0"))


def should_normalize_subtitle(content_type, body):
    if "text/vtt" in (content_type or "").lower():
        return True
    return body.lstrip().startswith(b"WEBVTT")


def subtitle_body_to_vtt(body, path="", content_type=""):
    text = (body or b"").decode("utf-8-sig", "replace")
    extension = subtitle_extension(path)
    if should_normalize_subtitle(content_type, body or b""):
        normalized = normalize_webvtt_timestamps(text)
        return normalized.encode("utf-8"), "text/vtt; charset=utf-8"
    if extension == ".srt":
        converted = "WEBVTT\n\n" + SRT_TIMESTAMP_RE.sub(_pad_srt_fraction, text.replace("\r", ""))
        return converted.encode("utf-8"), "text/vtt; charset=utf-8"
    if extension in (".ass", ".ssa"):
        converted = convert_ass_to_vtt(text)
        if converted:
            return converted.encode("utf-8"), "text/vtt; charset=utf-8"
    return body or b"", content_type or "text/plain; charset=utf-8"


def _pad_srt_fraction(match):
    return "%s.%s" % (match.group("time"), match.group("fraction").ljust(3, "0")[:3])


def convert_ass_to_vtt(text):
    cues = []
    format_fields = []
    for raw_line in str(text or "").replace("\r", "").split("\n"):
        line = raw_line.strip()
        if line.lower().startswith("format:"):
            format_fields = [field.strip().lower() for field in line.split(":", 1)[1].split(",")]
            continue
        if not line.lower().startswith("dialogue:"):
            continue
        payload = line.split(":", 1)[1].lstrip()
        if format_fields:
            parts = payload.split(",", max(0, len(format_fields) - 1))
            values = {field: parts[index] for index, field in enumerate(format_fields) if index < len(parts)}
            start = values.get("start", "")
            end = values.get("end", "")
            body = values.get("text", "")
        else:
            parts = payload.split(",", 9)
            if len(parts) < 10:
                continue
            start, end, body = parts[1], parts[2], parts[9]
        start_vtt = ass_time_to_vtt(start)
        end_vtt = ass_time_to_vtt(end)
        text_value = ASS_OVERRIDE_RE.sub("", body).replace("\\N", "\n").replace("\\n", "\n").strip()
        if start_vtt and end_vtt and text_value:
            cues.append("%s --> %s\n%s" % (start_vtt, end_vtt, text_value))
    if not cues:
        return ""
    return "WEBVTT\n\n" + "\n\n".join(cues) + "\n"


def ass_time_to_vtt(value):
    match = ASS_EVENT_TIME_RE.match(str(value or "").strip())
    if not match:
        return ""
    return "%02d:%s:%s.%s" % (
        int(match.group("hour")),
        match.group("minute"),
        match.group("second"),
        match.group("fraction").ljust(3, "0")[:3],
    )


def subtitle_extension(path):
    return posixpath.splitext(str(path or "").split("?", 1)[0])[1].lower()


def subtitle_delivery_extension(track):
    extension = subtitle_extension((track or {}).get("path"))
    if extension not in SUBTITLE_EXTENSIONS:
        return ".vtt"
    return extension


def subtitle_track_codec(extension):
    extension = str(extension or "").lower()
    if extension == ".vtt":
        return "webvtt"
    if extension == ".srt":
        return "srt"
    if extension == ".ssa":
        return "ssa"
    if extension == ".ass":
        return "ass"
    return "webvtt"


def subtitle_display_title(label, codec):
    label = str(label or "Subtitle").strip() or "Subtitle"
    codec_label = str(codec or "").upper() or "SUBTITLE"
    if "external" in label.lower() or codec_label in label.upper():
        return label
    return "%s - %s - External" % (label, codec_label)


def subtitle_content_type(path, fallback=""):
    extension = subtitle_extension(path)
    if extension == ".vtt":
        return "text/vtt; charset=utf-8"
    if extension == ".srt":
        return "application/x-subrip; charset=utf-8"
    if extension == ".ass":
        return "text/x-ass; charset=utf-8"
    if extension == ".ssa":
        return "text/x-ssa; charset=utf-8"
    return fallback or "text/plain; charset=utf-8"


def cloud_path_to_openlist_path(path):
    parsed = urllib.parse.urlparse(str(path or ""))
    if parsed.scheme != "cloud" or parsed.netloc != "openlist":
        return ""
    return urllib.parse.unquote(parsed.path or "")


def openlist_path_to_cloud_path(path):
    path = str(path or "")
    if not path.startswith("/"):
        return ""
    return "cloud://openlist" + path


def openlist_item_is_dir(item):
    if not isinstance(item, dict):
        return False
    value = item.get("is_dir")
    if value is None:
        value = item.get("isDir")
    if value is not None:
        return bool(value)
    return str(item.get("type") or "").lower() in ("folder", "dir", "directory")


def openlist_item_name(item):
    if not isinstance(item, dict):
        return ""
    return str(item.get("name") or item.get("Name") or "").strip()


def subtitle_matches_video(video_name, subtitle_name):
    video_stem = posixpath.splitext(str(video_name or ""))[0].lower()
    subtitle_stem = posixpath.splitext(str(subtitle_name or ""))[0].lower()
    if not video_stem or not subtitle_stem:
        return False
    return subtitle_stem == video_stem or subtitle_stem.startswith(video_stem + ".")


def subtitle_lang_label(name):
    stem = posixpath.splitext(str(name or ""))[0]
    token = stem.rsplit(".", 1)[-1].lower() if "." in stem else ""
    labels = {
        "sc": ("zh-Hans", "简体中文"),
        "chs": ("zh-Hans", "简体中文"),
        "zh-cn": ("zh-Hans", "简体中文"),
        "tc": ("zh-Hant", "繁体中文"),
        "cht": ("zh-Hant", "繁体中文"),
        "zh-tw": ("zh-Hant", "繁体中文"),
        "jp": ("ja", "日语"),
        "jpn": ("ja", "日语"),
        "ja": ("ja", "日语"),
        "en": ("en", "English"),
        "eng": ("en", "English"),
    }
    return labels.get(token, ("und", posixpath.basename(str(name or "")) or "Subtitle"))


def redact_sensitive_query_values(text):
    return SENSITIVE_QUERY_RE.sub(r"\1REDACTED", text)


def inject_subtitle_track_bootstrap(text):
    if "subtitleAutoEnabled" in text:
        return text
    if "</body>" in text:
        return text.replace("</body>", SUBTITLE_TRACK_BOOTSTRAP + "\n</body>", 1)
    return text + SUBTITLE_TRACK_BOOTSTRAP


def parse_emby_subtitle_stream_path(path):
    parsed = urllib.parse.urlparse(path)
    match = EMBY_SUBTITLE_STREAM_RE.match(parsed.path)
    if not match:
        return None
    query = urllib.parse.parse_qs(parsed.query)
    track_values = query.get("mp_track") or []
    track_index = int(track_values[0]) if track_values and str(track_values[0]).isdigit() else None
    stream_index = int(match.group("stream_index"))
    return {
        "media_id": urllib.parse.unquote(match.group("media_id")),
        "source_id": urllib.parse.unquote(match.group("source_id")),
        "stream_index": stream_index,
        "track_index": track_index if track_index is not None else max(0, stream_index - 1),
        "extension": "." + str(match.group("extension") or "vtt").lower(),
    }


def parse_emby_item_media_id(path):
    parsed = urllib.parse.urlparse(path)
    match = EMBY_ITEM_ID_RE.match(parsed.path)
    if not match:
        return ""
    return urllib.parse.unquote(match.group("media_id"))


def parse_emby_item_image_request(path):
    parsed = urllib.parse.urlparse(path)
    match = EMBY_ITEM_IMAGE_RE.match(parsed.path)
    if not match:
        return None
    return {
        "item_id": urllib.parse.unquote(match.group("item_id")),
        "image_type": image_type_value(match.group("image_type")),
        "query": parsed.query,
    }


def image_type_value(value):
    normalized = str(value or "Primary").strip().lower()
    if normalized == "backdrop":
        return "Backdrop"
    if normalized == "thumb":
        return "Thumb"
    return "Primary"


def inject_emby_subtitle_streams(payload, media_id, tracks):
    if not isinstance(payload, dict) or not media_id or not tracks:
        return False
    media_sources = payload.get("MediaSources")
    if not isinstance(media_sources, list):
        return False
    changed = False
    for source in media_sources:
        if not isinstance(source, dict):
            continue
        streams = source.get("MediaStreams")
        if not isinstance(streams, list):
            continue
        source_id = str(source.get("Id") or media_id)
        if source_id != str(media_id):
            continue
        existing_delivery_urls = {
            str(stream.get("DeliveryUrl") or "")
            for stream in streams
            if isinstance(stream, dict) and stream.get("Type") == "Subtitle"
        }
        existing_paths = {
            str(stream.get("Path") or "")
            for stream in streams
            if isinstance(stream, dict) and stream.get("Type") == "Subtitle" and stream.get("Path")
        }
        next_index = max([int(stream.get("Index") or 0) for stream in streams if isinstance(stream, dict)] + [-1]) + 1
        first_added_index = None
        for track_index, track in enumerate(tracks):
            if not isinstance(track, dict):
                continue
            label = str(track.get("label") or track.get("lang") or "Subtitle")
            language = str(track.get("lang") or "und")
            extension = subtitle_delivery_extension(track)
            codec = subtitle_track_codec(extension)
            stream_index = next_index + track_index
            path = str(track.get("path") or "")
            delivery_url = "/emby/Videos/%s/%s/Subtitles/%d/Stream%s?mp_track=%d" % (
                urllib.parse.quote(str(media_id), safe=""),
                urllib.parse.quote(source_id, safe=""),
                stream_index,
                extension,
                track_index,
            )
            if delivery_url in existing_delivery_urls or (path and path in existing_paths):
                continue
            streams.append(
                {
                    "Index": stream_index,
                    "Type": "Subtitle",
                    "Codec": codec,
                    "Language": language,
                    "DisplayTitle": subtitle_display_title(label, codec),
                    "Title": label,
                    "IsExternal": True,
                    "IsExternalUrl": False,
                    "IsInterlaced": False,
                    "IsForced": False,
                    "IsDefault": first_added_index is None,
                    "IsTextSubtitleStream": True,
                    "SupportsExternalStream": True,
                    "DeliveryMethod": "External",
                    "DeliveryUrl": delivery_url,
                    "Path": path,
                    "Protocol": "File",
                    "LocalizedDefault": "Default" if first_added_index is None else "",
                    "LocalizedForced": "",
                    "LocalizedExternal": "External",
                }
            )
            existing_delivery_urls.add(delivery_url)
            if path:
                existing_paths.add(path)
            if first_added_index is None:
                first_added_index = stream_index
            changed = True
        if first_added_index is not None and source.get("DefaultSubtitleStreamIndex") in (None, -1):
            source["DefaultSubtitleStreamIndex"] = first_added_index
    return changed


def emby_auth_token(path="", headers=None):
    parsed = urllib.parse.urlparse(path or "")
    query = urllib.parse.parse_qs(parsed.query)
    for key in ("X-Emby-Token", "api_key", "ApiKey", "access_token"):
        values = query.get(key) or []
        if values and values[0]:
            return str(values[0])
    for key, value in (headers or {}).items():
        if key.lower() in ("x-emby-token", "x-mediabrowser-token"):
            return str(value or "")
        if key.lower() == "authorization":
            prefix, _, token = str(value or "").partition(" ")
            if prefix.lower() == "bearer" and token:
                return token
    return ""


def emby_user_id_from_token(token):
    parts = str(token or "").split(".")
    if len(parts) < 2:
        return ""
    payload = parts[1] + "=" * (-len(parts[1]) % 4)
    try:
        data = json.loads(base64.urlsafe_b64decode(payload.encode("ascii")).decode("utf-8"))
    except (TypeError, ValueError, OSError):
        return ""
    return str(data.get("uid") or data.get("sub") or "").strip()


def emby_request_user_id_from_auth(path="", headers=None):
    return emby_request_user_id(path) or emby_user_id_from_token(emby_auth_token(path, headers))


def iter_emby_items(payload):
    if isinstance(payload, dict):
        yield payload
        items = payload.get("Items") or payload.get("items")
        if isinstance(items, list):
            for item in items:
                if isinstance(item, dict):
                    yield item
    elif isinstance(payload, list):
        for item in payload:
            if isinstance(item, dict):
                yield item


def emby_item_is_collection_folder(item):
    if not isinstance(item, dict):
        return False
    if str(item.get("Type") or item.get("type") or "").lower() == "collectionfolder":
        return True
    return emby_item_is_virtual_folder(item)


def emby_item_is_virtual_folder(item):
    return isinstance(item, dict) and bool(item.get("CollectionType") and item.get("ItemId") and item.get("Locations") is not None)


def emby_collection_folder_id(item):
    if not isinstance(item, dict):
        return ""
    return str(item.get("Id") or item.get("ItemId") or "").strip()


def emby_item_needs_folder_cover(item):
    if not emby_item_is_collection_folder(item):
        return False
    image_tags = item.get("ImageTags")
    has_primary_image = isinstance(image_tags, dict) and bool(image_tags.get("Primary"))
    if emby_item_is_virtual_folder(item):
        return bool(emby_collection_folder_id(item) and not has_primary_image)
    if has_primary_image:
        return False
    return bool(emby_collection_folder_id(item))


def select_emby_folder_cover_items(items, preferred_image_type="Primary", limit=EMBY_FOLDER_COVER_GRID_LIMIT):
    preferred_image_type = image_type_value(preferred_image_type)
    selected = []
    selected_ids = set()
    for image_type in unique_values([preferred_image_type, "Primary", "Backdrop", "Thumb"]):
        for item in items or []:
            cover = emby_item_cover(item, image_type)
            if not cover or cover["item_id"] in selected_ids:
                continue
            selected.append(cover)
            selected_ids.add(cover["item_id"])
            if len(selected) >= max(1, int_value(limit)):
                return selected
    return selected


def emby_item_cover(item, image_type="Primary"):
    if not isinstance(item, dict):
        return None
    item_id = str(item.get("Id") or item.get("id") or "").strip()
    if not item_id:
        return None
    image_type = image_type_value(image_type)
    image_tags = item.get("ImageTags")
    image_tags = image_tags if isinstance(image_tags, dict) else {}
    if image_type == "Primary":
        tag = image_tags.get("Primary")
    elif image_type == "Backdrop":
        tags = item.get("BackdropImageTags")
        tag = tags[0] if isinstance(tags, list) and tags else image_tags.get("Backdrop")
    else:
        tag = image_tags.get("Thumb")
    if not tag and image_type != "Primary":
        return emby_item_cover(item, "Primary")
    if not tag:
        return None
    cover = {
        "item_id": item_id,
        "image_type": image_type,
        "tag": str(tag),
    }
    if item.get("PrimaryImageAspectRatio") is not None:
        cover["primary_image_aspect_ratio"] = item.get("PrimaryImageAspectRatio")
    return cover


def patch_emby_collection_folder_item_cover(item, cover):
    covers = cover if isinstance(cover, list) else ([cover] if cover else [])
    if not emby_item_is_collection_folder(item):
        return False
    if not covers:
        return clear_emby_collection_folder_item_cover(item) if emby_item_needs_folder_cover(item) else False
    if not emby_item_needs_folder_cover(item):
        return False
    folder_id = emby_collection_folder_id(item)
    tag = emby_folder_cover_grid_tag(folder_id, covers)
    image_tags = dict(item.get("ImageTags") if isinstance(item.get("ImageTags"), dict) else {})
    image_tags["Primary"] = tag
    item["ImageTags"] = image_tags
    item.pop("PrimaryImageItemId", None)
    item.pop("PrimaryImageTag", None)
    item["PrimaryImageAspectRatio"] = EMBY_FOLDER_COVER_ASPECT_RATIO
    return True


def clear_emby_collection_folder_item_cover(item):
    if not emby_item_is_collection_folder(item):
        return False
    changed = False
    image_tags = item.get("ImageTags")
    if isinstance(image_tags, dict) and image_tags.get("Primary"):
        image_tags = dict(image_tags)
        image_tags.pop("Primary", None)
        item["ImageTags"] = image_tags
        changed = True
    for key in ("PrimaryImageItemId", "PrimaryImageTag", "PrimaryImageAspectRatio"):
        if key in item:
            item.pop(key, None)
            changed = True
    return changed


def is_emby_placeholder_image_body(body):
    if not body or len(body) > 128:
        return False
    png_signature = b"\x89PNG\r\n\x1a\n"
    if not body.startswith(png_signature) or len(body) < 24:
        return False
    width = int.from_bytes(body[16:20], "big")
    height = int.from_bytes(body[20:24], "big")
    return width == 1 and height == 1


def emby_folder_cover_grid_tag(folder_id, covers):
    digest = hashlib.sha1(
        json.dumps(
            [
                EMBY_FOLDER_COVER_TAG_VERSION,
                str(folder_id or ""),
                [
                    [
                        str(cover.get("item_id") or ""),
                        str(cover.get("image_type") or ""),
                        str(cover.get("tag") or ""),
                    ]
                    for cover in covers or []
                    if isinstance(cover, dict)
                ],
            ],
            ensure_ascii=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()[:EMBY_FOLDER_COVER_TAG_LENGTH]
    return digest


def emby_folder_cover_response_headers(tag, now=None):
    now = time.time() if now is None else now
    expires_at = now + 31536000
    return {
        "Content-Type": "image/png",
        "Cache-Control": "public, max-age=31536000",
        "ETag": '"%s"' % tag,
        "Last-Modified": http.server.BaseHTTPRequestHandler.date_time_string(None, now),
        "Expires": http.server.BaseHTTPRequestHandler.date_time_string(None, expires_at),
        "Accept-Ranges": "bytes",
        "Cross-Origin-Resource-Policy": "cross-origin",
        "Access-Control-Allow-Origin": "*",
    }


def emby_image_proxy_path(cover, original_query=""):
    query_items = []
    for key, value in urllib.parse.parse_qsl(original_query or "", keep_blank_values=True):
        if key.lower() == "tag":
            continue
        query_items.append((key, value))
    query_items.append(("tag", cover["tag"]))
    return "/emby/Items/%s/Images/%s?%s" % (
        urllib.parse.quote(str(cover["item_id"]), safe=""),
        urllib.parse.quote(image_type_value(cover.get("image_type")), safe=""),
        urllib.parse.urlencode(query_items),
    )


def emby_image_request_tag(query=""):
    for key, value in urllib.parse.parse_qsl(query or "", keep_blank_values=True):
        if key.lower() == "tag":
            return str(value or "")
    return ""


def emby_folder_cover_grid_dimensions(query=""):
    max_width = 0
    max_height = 0
    for key, value in urllib.parse.parse_qsl(query or "", keep_blank_values=True):
        normalized_key = key.lower()
        if normalized_key in ("maxwidth", "width"):
            parsed = int_value(value)
            if parsed > 0:
                max_width = parsed if not max_width else min(max_width, parsed)
        elif normalized_key in ("maxheight", "height"):
            parsed = int_value(value)
            if parsed > 0:
                max_height = parsed if not max_height else min(max_height, parsed)
    width = max_width or EMBY_FOLDER_COVER_DEFAULT_WIDTH
    if max_height:
        width = min(width, int(max_height * EMBY_FOLDER_COVER_ASPECT_RATIO))
    width = max(EMBY_FOLDER_COVER_MIN_WIDTH, min(EMBY_FOLDER_COVER_MAX_WIDTH, width))
    height = max(1, int(round(width / EMBY_FOLDER_COVER_ASPECT_RATIO)))
    return width, height


def build_emby_folder_cover_grid(image_bodies, dimensions=None):
    if Image is None or ImageOps is None:
        raise RuntimeError("Pillow is required to build Emby folder cover grids")
    decoded = []
    for body in image_bodies or []:
        if not body:
            continue
        try:
            with Image.open(io.BytesIO(body)) as image:
                decoded.append(image.convert("RGBA"))
        except (OSError, ValueError):
            continue
        if len(decoded) >= EMBY_FOLDER_COVER_GRID_LIMIT:
            break
    if not decoded:
        return b""
    if dimensions:
        width, height = dimensions
    else:
        width, height = emby_folder_cover_grid_dimensions("")
    width = max(EMBY_FOLDER_COVER_MIN_WIDTH, min(EMBY_FOLDER_COVER_MAX_WIDTH, int_value(width) or EMBY_FOLDER_COVER_DEFAULT_WIDTH))
    height = max(1, int_value(height) or int(round(width / EMBY_FOLDER_COVER_ASPECT_RATIO)))
    canvas = Image.new("RGBA", (width, height), (18, 18, 18, 255))
    resample = getattr(getattr(Image, "Resampling", Image), "LANCZOS")
    column_count = min(len(decoded), EMBY_FOLDER_COVER_GRID_LIMIT)
    x = 0
    for index, image in enumerate(decoded[:column_count]):
        next_x = width if index == column_count - 1 else int(round(width * (index + 1) / column_count))
        tile_size = (max(1, next_x - x), height)
        canvas.paste(ImageOps.fit(image, tile_size, method=resample), (x, 0))
        x = next_x
    out = io.BytesIO()
    canvas.save(out, format="PNG", optimize=True)
    return out.getvalue()


def unique_values(values):
    seen = set()
    out = []
    for value in values or []:
        normalized = str(value or "")
        if normalized and normalized not in seen:
            seen.add(normalized)
            out.append(normalized)
    return out


class OpenListSubtitleProvider:
    def __init__(self, base_url=DEFAULT_OPENLIST_URL, username="", password="", timeout=30):
        self.base_url = str(base_url or DEFAULT_OPENLIST_URL).rstrip("/")
        self.username = str(username or "").strip()
        self.password = str(password or "")
        self.timeout = timeout
        self._client = None
        self._lock = threading.Lock()

    def enabled(self):
        return bool(self.username and self.password)

    def tracks_for_media_path(self, media_path):
        if not self.enabled():
            return []
        openlist_media_path = cloud_path_to_openlist_path(media_path)
        if not openlist_media_path:
            return []
        media_name = posixpath.basename(openlist_media_path)
        media_extension = subtitle_extension(media_name)
        if media_extension not in VIDEO_EXTENSIONS:
            return []
        dir_path = posixpath.dirname(openlist_media_path) or "/"
        tracks = []
        for item in self._client_instance().list_all(dir_path, refresh=False):
            if openlist_item_is_dir(item):
                continue
            name = openlist_item_name(item)
            if subtitle_extension(name) not in SUBTITLE_EXTENSIONS:
                continue
            if not subtitle_matches_video(media_name, name):
                continue
            lang, label = subtitle_lang_label(name)
            subtitle_path = posix_join(dir_path, name)
            tracks.append(
                {
                    "lang": lang,
                    "label": label,
                    "path": openlist_path_to_cloud_path(subtitle_path),
                    "source": "openlist",
                }
            )
        return tracks

    def read_subtitle(self, cloud_path, target_extension=""):
        openlist_path = cloud_path_to_openlist_path(cloud_path)
        if not openlist_path:
            raise RuntimeError("OpenList subtitle path invalid")
        response = self._client_instance().get_path(openlist_path)
        raw_url = extract_openlist_raw_url(response)
        if not raw_url:
            raise RuntimeError("OpenList subtitle raw_url missing")
        request = urllib.request.Request(raw_url, headers={"Accept-Encoding": "identity"}, method="GET")
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                body = response.read()
                content_type = response.headers.get("Content-Type", "")
        except (OSError, TimeoutError) as exc:
            raise RuntimeError("OpenList subtitle read failed: %s" % exc) from exc
        if str(target_extension or "").lower() == ".vtt":
            return subtitle_body_to_vtt(body, openlist_path, content_type)
        return body, subtitle_content_type(openlist_path, content_type)

    def _client_instance(self):
        if self._client is not None:
            return self._client
        with self._lock:
            if self._client is None:
                token = OpenListPasswordTokenProvider(
                    self.base_url,
                    self.username,
                    self.password,
                    timeout=self.timeout,
                ).load_token()
                self._client = OpenListClient(self.base_url, token, timeout=self.timeout)
        return self._client


def posix_join(parent, name):
    return str(parent or "/").rstrip("/") + "/" + str(name or "").lstrip("/")


def extract_openlist_raw_url(response):
    data = response.get("data") if isinstance(response, dict) else None
    candidates = [data, response]
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        for key in ("raw_url", "rawUrl", "url", "download_url", "downloadUrl"):
            value = str(candidate.get(key) or "").strip()
            if value:
                return value
    return ""


def merge_subtitle_tracks(*track_lists):
    merged = []
    seen = set()
    for tracks in track_lists:
        for track in tracks or []:
            if not isinstance(track, dict) or not track.get("path"):
                continue
            key = str(track.get("path"))
            if key in seen:
                continue
            seen.add(key)
            merged.append(track)
    return merged


def msg_media_path(payload):
    candidates = [payload]
    if isinstance(payload, dict):
        for key in ("data", "media", "item"):
            value = payload.get(key)
            if isinstance(value, dict):
                candidates.append(value)
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        value = str(candidate.get("path") or "").strip()
        if value:
            return value
    return ""


def int_value(value, default=0):
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def synthetic_runtime_ticks(position_ticks):
    position_ticks = max(0, int_value(position_ticks))
    if position_ticks <= 0:
        return 0
    return max(
        MIN_SYNTHETIC_RUNTIME_TICKS,
        position_ticks * 2,
        position_ticks + SYNTHETIC_RUNTIME_PADDING_TICKS,
    )


def patch_emby_resume_runtime_fields(payload):
    changed = False
    if isinstance(payload, dict):
        changed = patch_emby_media_item_runtime(payload) or changed
        items = payload.get("Items") or payload.get("items")
        if isinstance(items, list):
            for item in items:
                if patch_emby_media_item_runtime(item):
                    changed = True
    return changed


def patch_emby_media_item_runtime(item):
    if not isinstance(item, dict):
        return False
    user_data = item.get("UserData")
    if not isinstance(user_data, dict):
        return False
    position_ticks = int_value(user_data.get("PlaybackPositionTicks"))
    if position_ticks <= 0:
        return False

    changed = False
    runtime_ticks = int_value(item.get("RunTimeTicks"))
    if runtime_ticks <= 0:
        runtime_ticks = synthetic_runtime_ticks(position_ticks)
        item["RunTimeTicks"] = runtime_ticks
        changed = True
    if runtime_ticks > 0:
        percentage = min(99.0, max(0.0, position_ticks * 100.0 / runtime_ticks))
        current_percentage = user_data.get("PlayedPercentage")
        if current_percentage in (None, 0, 0.0):
            user_data["PlayedPercentage"] = percentage
            changed = True

    media_sources = item.get("MediaSources")
    if isinstance(media_sources, list):
        for source in media_sources:
            if isinstance(source, dict) and int_value(source.get("RunTimeTicks")) <= 0 and runtime_ticks > 0:
                source["RunTimeTicks"] = runtime_ticks
                changed = True
    return changed


def patch_emby_playback_info_runtime(payload, runtime_ticks, media_id=""):
    runtime_ticks = int_value(runtime_ticks)
    if runtime_ticks <= 0 or not isinstance(payload, dict):
        return False
    media_id = str(media_id or "")
    changed = False
    media_sources = payload.get("MediaSources")
    if isinstance(media_sources, list):
        for source in media_sources:
            if not isinstance(source, dict):
                continue
            if media_id and str(source.get("Id") or "") != media_id:
                continue
            if int_value(source.get("RunTimeTicks")) <= 0:
                source["RunTimeTicks"] = runtime_ticks
                changed = True
    return changed


def emby_request_user_id(path):
    parsed = urllib.parse.urlparse(path)
    match = re.match(r"^(?:/emby)?/Users/(?P<user_id>[^/]+)/", parsed.path, re.IGNORECASE)
    if match:
        return urllib.parse.unquote(match.group("user_id"))
    query = urllib.parse.parse_qs(parsed.query)
    values = query.get("UserId") or query.get("userId") or query.get("user_id") or []
    return str(values[0]) if values else ""


def query_first_value(query, key):
    values = query.get(key) or []
    return str(values[0]) if values else ""


def int_query_value(query, key, default=0):
    value = query_first_value(query, key)
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def case_insensitive_query_variants(term):
    value = str(term or "").strip()
    if not value:
        return []
    variants = []
    for candidate in (value, value.upper(), value.lower(), value.title()):
        if candidate and candidate not in variants:
            variants.append(candidate)
    return variants


def parse_emby_user_items_search_request(path):
    parsed = urllib.parse.urlparse(path)
    match = EMBY_USER_ITEMS_RE.match(parsed.path)
    if not match:
        return None
    query = urllib.parse.parse_qs(parsed.query)
    term = query_first_value(query, "SearchTerm").strip()
    mode = "SearchTerm"
    if not term:
        term = query_first_value(query, "NameStartsWith").strip()
        mode = "NameStartsWith"
    if not term:
        return None
    limit = max(1, min(int_query_value(query, "Limit", 50), 100))
    start_index = max(0, int_query_value(query, "StartIndex", 0))
    return {
        "user_id": urllib.parse.unquote(match.group("user_id")),
        "term": term,
        "mode": mode,
        "limit": limit,
        "start_index": start_index,
    }


class MsgApiAuthenticator:
    def __init__(self, base_url=DEFAULT_MSG_API_BASE_URL, username="", password=""):
        self.base_url = str(base_url or DEFAULT_MSG_API_BASE_URL).rstrip("/")
        self.username = str(username or "")
        self.password = str(password or "")
        self._access_token = ""
        self._refresh_token = ""
        self._token_expires_at = 0
        self._lock = threading.Lock()

    def authorization_header(self):
        token = self.access_token()
        return "Bearer %s" % token

    def access_token(self):
        if self._access_token and time.time() < self._token_expires_at:
            return self._access_token
        with self._lock:
            if self._access_token and time.time() < self._token_expires_at:
                return self._access_token
            self._login()
            return self._access_token

    def clear(self):
        with self._lock:
            self._access_token = ""
            self._refresh_token = ""
            self._token_expires_at = 0

    def _login(self):
        if not self.username or not self.password:
            raise RuntimeError("subtitle proxy MSG credentials missing")
        payload = json.dumps({"username": self.username, "password": self.password}).encode("utf-8")
        request = urllib.request.Request(
            self.base_url + "/auth/login",
            data=payload,
            headers={"Content-Type": "application/json", "Accept-Encoding": "identity"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=20) as response:
                body = json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", "replace")[:200]
            raise RuntimeError("subtitle proxy MSG login failed: HTTP %s %s" % (exc.code, detail)) from exc
        except (OSError, TimeoutError, ValueError) as exc:
            raise RuntimeError("subtitle proxy MSG login failed: %s" % exc) from exc

        tokens = body.get("tokens") if isinstance(body, dict) else None
        access_token = (tokens or {}).get("access_token")
        refresh_token = (tokens or {}).get("refresh_token")
        if not access_token:
            raise RuntimeError("subtitle proxy MSG login response missing access token")
        self._access_token = str(access_token)
        self._refresh_token = str(refresh_token or "")
        self._token_expires_at = time.time() + 50 * 60


class SubtitleProxyHandler(http.server.BaseHTTPRequestHandler):
    upstream_base_url = DEFAULT_SUBTITLE_PROXY_UPSTREAM
    msg_api_auth = None
    openlist_subtitle_provider = None
    local_subtitle_provider = None
    folder_cover_cache = {}
    folder_image_cache = {}
    folder_id_cache = {}
    # Some Emby-compatible clients omit tokens on image requests after authenticated Views responses.
    # Only serve tokenless folder covers when the exact folder/tag pair was just published by this proxy.
    published_folder_cover_cache = {}
    folder_cover_cache_lock = threading.Lock()

    def do_GET(self):
        self._proxy()

    def do_HEAD(self):
        self._proxy(head_only=True)

    def do_POST(self):
        self._proxy(method="POST")

    def _proxy(self, head_only=False, method=None):
        method = method or ("HEAD" if head_only else "GET")
        request_body = None
        if method not in ("GET", "HEAD"):
            try:
                content_length = int(self.headers.get("Content-Length") or 0)
            except ValueError:
                content_length = 0
            request_body = self.rfile.read(content_length) if content_length > 0 else b""
        headers = {
            key: value
            for key, value in self.headers.items()
            if key.lower() not in HOP_BY_HOP_HEADERS and key.lower() not in ("host", "content-length")
        }
        if method == "GET" and not head_only and self._serve_emby_folder_image(headers):
            return
        if method == "GET" and not head_only and self._serve_emby_subtitle_stream(headers):
            return
        if method == "GET" and not head_only and self._serve_emby_items_search(headers):
            return
        upstream_url = urllib.parse.urljoin(self.upstream_base_url.rstrip("/") + "/", self.path.lstrip("/"))
        headers["Accept-Encoding"] = "identity"
        request = urllib.request.Request(upstream_url, data=request_body, headers=headers, method=method)
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                body = b"" if head_only else response.read()
                self._write_response(response.status, response.headers, body, head_only=head_only, request_headers=headers, request_path=self.path)
        except urllib.error.HTTPError as exc:
            body = b"" if head_only else exc.read()
            self._write_response(exc.code, exc.headers, body, head_only=head_only, request_headers=headers, request_path=self.path)
        except (OSError, TimeoutError) as exc:
            self.send_response(502)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.end_headers()
            if not head_only:
                self.wfile.write(("subtitle proxy upstream error: %s\n" % exc).encode("utf-8"))

    def _serve_emby_folder_image(self, request_headers):
        image_request = parse_emby_item_image_request(self.path)
        if not image_request:
            return False
        user_id = emby_request_user_id_from_auth(self.path, request_headers)
        if user_id:
            if not self._is_emby_collection_folder_id(user_id, image_request["item_id"], request_headers, self.path):
                return False
            covers = self._find_emby_folder_covers(
                user_id,
                image_request["item_id"],
                request_headers,
                self.path,
                preferred_image_type=image_request["image_type"],
            )
            cache_user_id = user_id
        else:
            covers = self._find_published_emby_folder_covers(
                image_request["item_id"],
                image_request["image_type"],
                emby_image_request_tag(image_request["query"]),
            )
            cache_user_id = "published"
            if not covers:
                return False
        if not covers:
            status, headers, body = self._read_upstream(self.path, request_headers)
            if status >= 200 and status < 300 and is_emby_placeholder_image_body(body):
                self._write_response(
                    404,
                    {"Content-Type": "text/plain; charset=utf-8", "Cache-Control": "no-store"},
                    b"",
                    request_headers=request_headers,
                    request_path=self.path,
                )
                return True
            return False
        body = self._build_emby_folder_cover_image(
            cache_user_id,
            image_request["item_id"],
            covers,
            request_headers,
            image_request["query"],
        )
        if not body:
            return False
        tag = emby_folder_cover_grid_tag(image_request["item_id"], covers)
        headers = emby_folder_cover_response_headers(tag)
        self._write_response(200, headers, body, request_headers=request_headers, request_path=self.path)
        return True

    def _serve_emby_subtitle_stream(self, request_headers):
        stream_request = parse_emby_subtitle_stream_path(self.path)
        if not stream_request:
            return False
        tracks = self._fetch_msg_subtitle_tracks(stream_request["media_id"])
        track_index = stream_request["track_index"]
        if track_index < 0 or track_index >= len(tracks):
            self.send_response(404)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.end_headers()
            self.wfile.write(b"subtitle track not found\n")
            return True
        track_path = tracks[track_index].get("path")
        if not track_path:
            self.send_response(404)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.end_headers()
            self.wfile.write(b"subtitle path missing\n")
            return True
        if tracks[track_index].get("source") == "openlist":
            self._serve_openlist_subtitle_track(tracks[track_index], request_headers, stream_request["extension"])
            return True
        if tracks[track_index].get("source") in ("assrt", "opensubtitles", "local") or local_subtitle_uri_valid(track_path):
            self._serve_local_subtitle_track(tracks[track_index], request_headers, stream_request["extension"])
            return True
        query = urllib.parse.urlencode({"path": track_path})
        upstream_path = "/api/subtitles/%s?%s" % (urllib.parse.quote(stream_request["media_id"], safe=""), query)
        try:
            status, headers, body = self._read_msg_api(upstream_path[len("/api") :])
        except RuntimeError as exc:
            self.send_response(502)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.end_headers()
            self.wfile.write(("subtitle proxy MSG subtitle stream error: %s\n" % exc).encode("utf-8"))
            return True
        self._write_response(status, headers, body, request_headers=request_headers, request_path=self.path)
        return True

    def _serve_emby_items_search(self, request_headers):
        search_request = parse_emby_user_items_search_request(self.path)
        if not search_request:
            return False
        try:
            item_ids = self._fetch_msg_search_media_ids(search_request["term"], search_request["limit"], search_request["start_index"])
            items = self._fetch_emby_items_by_ids(search_request["user_id"], item_ids, request_headers)
            if item_ids and not items:
                raise RuntimeError("Emby item detail lookup returned no usable items for %r" % search_request["term"])
        except RuntimeError as exc:
            print("subtitle proxy Emby search error: %s" % exc, flush=True)
            self._write_response(
                502,
                {"Content-Type": "text/plain; charset=utf-8", "Cache-Control": "no-store"},
                ("subtitle proxy Emby search error: %s\n" % exc).encode("utf-8"),
                request_headers=request_headers,
                request_path=self.path,
            )
            return True
        payload = {
            "Items": items,
            "TotalRecordCount": len(items),
        }
        self._write_response(
            200,
            {"Content-Type": "application/json; charset=utf-8", "Cache-Control": "no-store"},
            json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8"),
            request_headers=request_headers,
            request_path=self.path,
        )
        print(
            "subtitle proxy Emby search %s term=%r ids=%d items=%d"
            % (search_request["mode"], search_request["term"], len(item_ids), len(items)),
            flush=True,
        )
        return True

    def _serve_openlist_subtitle_track(self, track, request_headers, target_extension):
        if self.openlist_subtitle_provider is None:
            self.send_response(502)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.end_headers()
            self.wfile.write(b"OpenList subtitle provider is not configured\n")
            return
        try:
            body, content_type = self.openlist_subtitle_provider.read_subtitle(track.get("path"), target_extension=target_extension)
        except RuntimeError as exc:
            self.send_response(502)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.end_headers()
            self.wfile.write(("OpenList subtitle stream error: %s\n" % exc).encode("utf-8"))
            return
        self._write_response(
            200,
            {"Content-Type": content_type},
            body,
            request_headers=request_headers,
            request_path=self.path,
        )

    def _serve_local_subtitle_track(self, track, request_headers, target_extension):
        if self.local_subtitle_provider is None:
            self.send_response(502)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.end_headers()
            self.wfile.write(b"Local subtitle provider is not configured\n")
            return
        try:
            body, filename = self.local_subtitle_provider.read_subtitle(track.get("path"))
            content_type = subtitle_content_type(filename)
            if str(target_extension or "").lower() == ".vtt":
                body, content_type = subtitle_body_to_vtt(body, filename, content_type)
        except RuntimeError as exc:
            self.send_response(502)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.end_headers()
            self.wfile.write(("Local subtitle stream error: %s\n" % exc).encode("utf-8"))
            return
        self._write_response(
            200,
            {"Content-Type": content_type},
            body,
            request_headers=request_headers,
            request_path=self.path,
        )

    def _fetch_msg_subtitle_tracks(self, media_id):
        msg_tracks = []
        try:
            status, _headers, body = self._read_msg_api("/media/%s/subtitles" % urllib.parse.quote(str(media_id), safe=""))
        except RuntimeError as exc:
            print("subtitle proxy MSG subtitle list error: %s" % exc, flush=True)
            status = 0
            body = b""
        if status and (status < 200 or status >= 300 or not body):
            print("subtitle proxy MSG subtitle list HTTP %s for media %s" % (status, media_id), flush=True)
        elif body:
            try:
                payload = json.loads(body.decode("utf-8"))
            except (TypeError, ValueError):
                print("subtitle proxy MSG subtitle list invalid JSON for media %s" % media_id, flush=True)
                payload = None
            tracks = payload.get("tracks") if isinstance(payload, dict) else None
            if isinstance(tracks, list):
                msg_tracks = [track for track in tracks if isinstance(track, dict) and track.get("path")]
        openlist_tracks = self._fetch_openlist_subtitle_tracks(media_id)
        local_tracks = self._fetch_local_subtitle_tracks(media_id)
        return merge_subtitle_tracks(openlist_tracks, local_tracks, msg_tracks)

    def _fetch_local_subtitle_tracks(self, media_id):
        if self.local_subtitle_provider is None or not self.local_subtitle_provider.enabled():
            return []
        try:
            tracks = self.local_subtitle_provider.tracks_for_media_id(media_id)
        except RuntimeError as exc:
            print("subtitle proxy local subtitle discovery error: %s" % exc, flush=True)
            return []
        if tracks:
            print("subtitle proxy local subtitle tracks %s for media %s" % (len(tracks), media_id), flush=True)
        return tracks

    def _fetch_openlist_subtitle_tracks(self, media_id):
        if self.openlist_subtitle_provider is None or not self.openlist_subtitle_provider.enabled():
            return []
        try:
            status, _headers, body = self._read_msg_api("/media/%s" % urllib.parse.quote(str(media_id), safe=""))
        except RuntimeError as exc:
            print("subtitle proxy MSG media detail error: %s" % exc, flush=True)
            return []
        if status < 200 or status >= 300 or not body:
            print("subtitle proxy MSG media detail HTTP %s for media %s" % (status, media_id), flush=True)
            return []
        try:
            payload = json.loads(body.decode("utf-8"))
        except (TypeError, ValueError):
            print("subtitle proxy MSG media detail invalid JSON for media %s" % media_id, flush=True)
            return []
        media_path = msg_media_path(payload)
        if not media_path:
            return []
        try:
            tracks = self.openlist_subtitle_provider.tracks_for_media_path(media_path)
        except RuntimeError as exc:
            print("subtitle proxy OpenList subtitle discovery error: %s" % exc, flush=True)
            return []
        if tracks:
            print("subtitle proxy OpenList subtitle tracks %s for media %s" % (len(tracks), media_id), flush=True)
        return tracks

    def _fetch_msg_search_media_ids(self, term, limit, start_index=0):
        requested_limit = max(1, min(int(limit or 50), 100))
        offset = max(0, int(start_index or 0))
        api_limit = min(requested_limit + offset, 100)
        ids = []
        seen = set()
        for query_term in case_insensitive_query_variants(term):
            query = urllib.parse.urlencode(
                {
                    "q": query_term,
                    "limit": str(api_limit),
                }
            )
            status, _headers, body = self._read_msg_api("/media?%s" % query)
            if status < 200 or status >= 300 or not body:
                raise RuntimeError("MSG media search HTTP %s for %r" % (status, query_term))
            try:
                payload = json.loads(body.decode("utf-8"))
            except (TypeError, ValueError) as exc:
                raise RuntimeError("MSG media search invalid JSON for %r" % query_term) from exc
            rows = payload.get("items") if isinstance(payload, dict) else None
            if not isinstance(rows, list):
                raise RuntimeError("MSG media search response missing items for %r" % query_term)
            for row in rows:
                if not isinstance(row, dict):
                    continue
                media_id = str(row.get("id") or row.get("Id") or "").strip()
                if not media_id or media_id in seen:
                    continue
                seen.add(media_id)
                ids.append(media_id)
            if len(ids) >= offset + requested_limit:
                break
        return ids[offset : offset + requested_limit]

    def _fetch_emby_items_by_ids(self, user_id, media_ids, request_headers):
        items = []
        for media_id in media_ids:
            path = "/emby/Users/%s/Items/%s" % (
                urllib.parse.quote(str(user_id), safe=""),
                urllib.parse.quote(str(media_id), safe=""),
            )
            status, _headers, body = self._read_upstream(path, request_headers)
            if status < 200 or status >= 300 or not body:
                print("subtitle proxy Emby search item HTTP %s for media %s" % (status, media_id), flush=True)
                continue
            try:
                item = json.loads(body.decode("utf-8"))
            except (TypeError, ValueError):
                print("subtitle proxy Emby search item invalid JSON for media %s" % media_id, flush=True)
                continue
            if isinstance(item, dict) and item.get("Id"):
                items.append(item)
        return items

    def _fetch_emby_resume_runtime_ticks(self, media_id, request_headers, request_path):
        user_id = emby_request_user_id(request_path or "")
        if not user_id:
            return 0
        status, _headers, body = self._read_upstream(
            "/emby/Users/%s/Items/%s" % (
                urllib.parse.quote(str(user_id), safe=""),
                urllib.parse.quote(str(media_id), safe=""),
            ),
            request_headers,
        )
        if status < 200 or status >= 300 or not body:
            return 0
        try:
            payload = json.loads(body.decode("utf-8"))
        except (TypeError, ValueError):
            return 0
        patch_emby_media_item_runtime(payload)
        return int_value(payload.get("RunTimeTicks"))

    def _patch_emby_collection_folder_covers(self, payload, request_headers, request_path):
        user_id = emby_request_user_id_from_auth(request_path or "", request_headers)
        if not user_id:
            return False
        changed = False
        for item in iter_emby_items(payload):
            if not emby_item_needs_folder_cover(item):
                continue
            folder_id = emby_collection_folder_id(item)
            self._remember_emby_collection_folder_id(user_id, folder_id)
            covers = self._find_emby_folder_covers(user_id, folder_id, request_headers, request_path or "")
            if patch_emby_collection_folder_item_cover(item, covers):
                self._remember_published_emby_folder_covers(folder_id, "Primary", covers)
                changed = True
        return changed

    def _remember_published_emby_folder_covers(self, folder_id, preferred_image_type, covers):
        folder_id = str(folder_id or "").strip()
        covers = [cover for cover in (covers or []) if isinstance(cover, dict)]
        if not folder_id or not covers:
            return
        preferred_image_type = image_type_value(preferred_image_type)
        tag = emby_folder_cover_grid_tag(folder_id, covers)
        now = time.time()
        with self.folder_cover_cache_lock:
            self.published_folder_cover_cache[(folder_id, preferred_image_type)] = {
                "covers": covers,
                "tag": tag,
                "expires_at": now + EMBY_FOLDER_COVER_CACHE_TTL_SECONDS,
            }

    def _find_published_emby_folder_covers(self, folder_id, preferred_image_type, requested_tag=""):
        folder_id = str(folder_id or "").strip()
        if not folder_id:
            return []
        preferred_image_type = image_type_value(preferred_image_type)
        requested_tag = str(requested_tag or "").strip()
        if not requested_tag:
            return []
        now = time.time()
        with self.folder_cover_cache_lock:
            cached = self.published_folder_cover_cache.get((folder_id, preferred_image_type))
            if not cached or cached.get("expires_at", 0) <= now:
                return []
            if requested_tag != cached.get("tag"):
                return []
            return cached.get("covers") or []

    def _remember_emby_collection_folder_id(self, user_id, folder_id):
        user_id = str(user_id or "").strip()
        folder_id = str(folder_id or "").strip()
        if not user_id or not folder_id:
            return
        now = time.time()
        with self.folder_cover_cache_lock:
            cached = self.folder_id_cache.get(user_id)
            if not cached or cached.get("expires_at", 0) <= now:
                cached = {"ids": set(), "expires_at": now + EMBY_FOLDER_COVER_CACHE_TTL_SECONDS}
                self.folder_id_cache[user_id] = cached
            cached["ids"].add(folder_id)

    def _is_emby_collection_folder_id(self, user_id, item_id, request_headers, request_path):
        user_id = str(user_id or "").strip()
        item_id = str(item_id or "").strip()
        if not user_id or not item_id:
            return False
        now = time.time()
        with self.folder_cover_cache_lock:
            cached = self.folder_id_cache.get(user_id)
            if cached and cached.get("expires_at", 0) > now:
                return item_id in cached.get("ids", set())
        folder_ids = self._fetch_emby_collection_folder_ids(user_id, request_headers, request_path)
        with self.folder_cover_cache_lock:
            self.folder_id_cache[user_id] = {
                "ids": set(folder_ids),
                "expires_at": now + EMBY_FOLDER_COVER_CACHE_TTL_SECONDS,
            }
        return item_id in folder_ids

    def _fetch_emby_collection_folder_ids(self, user_id, request_headers, request_path):
        query_items = []
        token = emby_auth_token(request_path, request_headers)
        if token:
            query_items.append(("api_key", token))
        suffix = "?" + urllib.parse.urlencode(query_items) if query_items else ""
        path = "/emby/Users/%s/Views%s" % (urllib.parse.quote(str(user_id), safe=""), suffix)
        status, _headers, body = self._read_upstream(path, request_headers)
        if status < 200 or status >= 300 or not body:
            return set()
        try:
            payload = json.loads(body.decode("utf-8"))
        except (TypeError, ValueError):
            return set()
        ids = set()
        for item in iter_emby_items(payload):
            if emby_item_is_collection_folder(item) and item.get("Id"):
                ids.add(str(item.get("Id")))
        return ids

    def _find_emby_folder_cover(self, user_id, folder_id, request_headers, request_path, preferred_image_type="Primary"):
        covers = self._find_emby_folder_covers(user_id, folder_id, request_headers, request_path, preferred_image_type)
        return covers[0] if covers else None

    def _find_emby_folder_covers(self, user_id, folder_id, request_headers, request_path, preferred_image_type="Primary"):
        user_id = str(user_id or "").strip()
        folder_id = str(folder_id or "").strip()
        if not user_id or not folder_id:
            return []
        preferred_image_type = image_type_value(preferred_image_type)
        key = (user_id, folder_id, preferred_image_type)
        now = time.time()
        with self.folder_cover_cache_lock:
            cached = self.folder_cover_cache.get(key)
            if cached and cached.get("expires_at", 0) > now:
                return cached.get("covers") or []
        covers = self._fetch_emby_folder_covers(user_id, folder_id, request_headers, request_path, preferred_image_type)
        with self.folder_cover_cache_lock:
            self.folder_cover_cache[key] = {
                "covers": covers,
                "expires_at": now + EMBY_FOLDER_COVER_CACHE_TTL_SECONDS,
            }
        return covers

    def _build_emby_folder_cover_image(self, user_id, folder_id, covers, request_headers, original_query):
        dimensions = emby_folder_cover_grid_dimensions(original_query)
        tag = emby_folder_cover_grid_tag(folder_id, covers)
        key = (str(user_id), str(folder_id), tag, dimensions)
        now = time.time()
        with self.folder_cover_cache_lock:
            cached = self.folder_image_cache.get(key)
            if cached and cached.get("expires_at", 0) > now:
                return cached.get("body") or b""
        image_bodies = []
        for cover in covers[:EMBY_FOLDER_COVER_GRID_LIMIT]:
            upstream_path = emby_image_proxy_path(cover, original_query)
            status, _headers, body = self._read_upstream(upstream_path, request_headers)
            if status >= 200 and status < 300 and body:
                image_bodies.append(body)
        try:
            body = build_emby_folder_cover_grid(image_bodies, dimensions=dimensions)
        except RuntimeError as exc:
            print("subtitle proxy Emby folder cover grid failed: %s" % exc, flush=True)
            return b""
        if body:
            with self.folder_cover_cache_lock:
                self.folder_image_cache[key] = {
                    "body": body,
                    "expires_at": now + EMBY_FOLDER_COVER_CACHE_TTL_SECONDS,
                }
        return body

    def _fetch_emby_folder_cover(self, user_id, folder_id, request_headers, request_path, preferred_image_type="Primary"):
        covers = self._fetch_emby_folder_covers(user_id, folder_id, request_headers, request_path, preferred_image_type)
        return covers[0] if covers else None

    def _fetch_emby_folder_covers(self, user_id, folder_id, request_headers, request_path, preferred_image_type="Primary"):
        query_items = [
            ("ParentId", str(folder_id)),
            ("Limit", "30"),
            ("Fields", "PrimaryImageAspectRatio"),
            ("ImageTypeLimit", "1"),
            ("EnableImageTypes", "Primary,Backdrop,Thumb"),
        ]
        token = emby_auth_token(request_path, request_headers)
        if token:
            query_items.append(("api_key", token))
        path = "/emby/Users/%s/Items?%s" % (
            urllib.parse.quote(str(user_id), safe=""),
            urllib.parse.urlencode(query_items),
        )
        status, _headers, body = self._read_upstream(path, request_headers)
        if status < 200 or status >= 300 or not body:
            return None
        try:
            payload = json.loads(body.decode("utf-8"))
        except (TypeError, ValueError):
            return None
        items = payload.get("Items") if isinstance(payload, dict) else None
        if not isinstance(items, list) or not items:
            return []
        return select_emby_folder_cover_items(items, preferred_image_type)

    def _read_msg_api(self, path, retry_auth=True):
        if self.msg_api_auth is None:
            raise RuntimeError("subtitle proxy MSG auth is not configured")
        upstream_url = urllib.parse.urljoin(self.msg_api_auth.base_url.rstrip("/") + "/", str(path).lstrip("/"))
        headers = {
            "Accept-Encoding": "identity",
            "Authorization": self.msg_api_auth.authorization_header(),
        }
        request = urllib.request.Request(upstream_url, headers=headers, method="GET")
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                return response.status, response.headers, response.read()
        except urllib.error.HTTPError as exc:
            body = exc.read()
            if exc.code == 401 and retry_auth:
                self.msg_api_auth.clear()
                return self._read_msg_api(path, retry_auth=False)
            return exc.code, exc.headers, body

    def _read_upstream(self, path, request_headers):
        upstream_url = urllib.parse.urljoin(self.upstream_base_url.rstrip("/") + "/", str(path).lstrip("/"))
        headers = {
            key: value
            for key, value in (request_headers or {}).items()
            if key.lower() not in HOP_BY_HOP_HEADERS and key.lower() != "host"
        }
        headers["Accept-Encoding"] = "identity"
        request = urllib.request.Request(upstream_url, headers=headers, method="GET")
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                return response.status, response.headers, response.read()
        except urllib.error.HTTPError as exc:
            return exc.code, exc.headers, exc.read()

    def _write_response(self, status, headers, body, head_only=False, request_headers=None, request_path=None):
        content_type = headers.get("Content-Type", "")
        no_store = False
        if not head_only and body and should_normalize_subtitle(content_type, body):
            body = normalize_webvtt_timestamps(body.decode("utf-8", "replace")).encode("utf-8")
            content_type = "text/vtt; charset=utf-8"
        elif not head_only and body and "application/json" in (content_type or "").lower():
            media_id = parse_emby_item_media_id(request_path or "")
            try:
                payload = json.loads(body.decode("utf-8"))
            except (TypeError, ValueError):
                payload = None
            if isinstance(payload, (dict, list)):
                if isinstance(payload, dict) and patch_emby_resume_runtime_fields(payload):
                    no_store = True
                if self._patch_emby_collection_folder_covers(payload, request_headers, request_path or ""):
                    no_store = True
                if isinstance(payload, dict) and media_id:
                    runtime_ticks = self._fetch_emby_resume_runtime_ticks(media_id, request_headers, request_path or "")
                    if patch_emby_playback_info_runtime(payload, runtime_ticks, media_id=media_id):
                        no_store = True
                    tracks = self._fetch_msg_subtitle_tracks(media_id)
                    if inject_emby_subtitle_streams(payload, media_id, tracks):
                        no_store = True
                if no_store:
                    body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
                    content_type = "application/json; charset=utf-8"
        elif not head_only and body and "text/html" in (content_type or "").lower():
            body = inject_subtitle_track_bootstrap(body.decode("utf-8", "replace")).encode("utf-8")
            no_store = True

        self.send_response(status)
        for key, value in headers.items():
            lower_key = key.lower()
            if lower_key in HOP_BY_HOP_HEADERS or lower_key in ("content-length", "content-encoding"):
                continue
            if no_store and lower_key in ("cache-control", "etag", "expires", "last-modified"):
                continue
            if lower_key == "content-type" and content_type:
                continue
            self.send_header(key, value)
        if content_type:
            self.send_header("Content-Type", content_type)
        if no_store:
            self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if not head_only:
            self.wfile.write(body)

    def log_message(self, format, *args):
        message = redact_sensitive_query_values(format % args)
        print("%s - - [%s] %s" % (self.address_string(), self.log_date_time_string(), message), flush=True)


class ThreadingHTTPServer(socketserver.ThreadingMixIn, http.server.HTTPServer):
    daemon_threads = True


def run_subtitle_proxy(
    host=DEFAULT_SUBTITLE_PROXY_HOST,
    port=DEFAULT_SUBTITLE_PROXY_PORT,
    upstream=DEFAULT_SUBTITLE_PROXY_UPSTREAM,
    msg_api_base_url=None,
    msg_admin_user=None,
    msg_admin_password=None,
    openlist_base_url=None,
    openlist_username=None,
    openlist_password=None,
    subtitle_cache_dir=None,
):
    SubtitleProxyHandler.upstream_base_url = upstream
    SubtitleProxyHandler.msg_api_auth = MsgApiAuthenticator(
        msg_api_base_url or os.environ.get("MSG_BASE_URL") or DEFAULT_MSG_API_BASE_URL,
        msg_admin_user if msg_admin_user is not None else os.environ.get("MSG_ADMIN_USER", ""),
        msg_admin_password if msg_admin_password is not None else os.environ.get("MSG_ADMIN_PASSWORD", ""),
    )
    SubtitleProxyHandler.openlist_subtitle_provider = OpenListSubtitleProvider(
        openlist_base_url or os.environ.get("OPENLIST_URL") or DEFAULT_OPENLIST_URL,
        openlist_username if openlist_username is not None else os.environ.get("OPENLIST_MEDIA_SCAN_USERNAME", ""),
        openlist_password if openlist_password is not None else os.environ.get("OPENLIST_MEDIA_SCAN_PASSWORD", ""),
    )
    SubtitleProxyHandler.local_subtitle_provider = LocalSubtitleProvider(
        subtitle_cache_dir or os.environ.get("SUBTITLE_CACHE_DIR") or DEFAULT_SUBTITLE_CACHE_DIR
    )
    server = ThreadingHTTPServer((host, port), SubtitleProxyHandler)
    print("subtitle proxy listening on %s:%s upstream=%s" % (host, port, upstream), flush=True)
    try:
        server.serve_forever()
    finally:
        server.server_close()
