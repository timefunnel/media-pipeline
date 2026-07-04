import http.server
import json
import os
import re
import socketserver
import threading
import time
import urllib.error
import urllib.parse
import urllib.request


DEFAULT_SUBTITLE_PROXY_HOST = "127.0.0.1"
DEFAULT_SUBTITLE_PROXY_PORT = 18081
DEFAULT_SUBTITLE_PROXY_UPSTREAM = "http://127.0.0.1:18080"
DEFAULT_MSG_API_BASE_URL = "http://127.0.0.1:18080/api"

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
SENSITIVE_QUERY_RE = re.compile(r"([?&](?:api_?key|access_token|token)=)[^&\s\"]+", re.IGNORECASE)
EMBY_ITEM_ID_RE = re.compile(r"^/emby/(?:Users/[^/]+/)?Items/(?P<media_id>[^/?]+)(?:/PlaybackInfo)?/?$", re.IGNORECASE)
EMBY_SUBTITLE_STREAM_RE = re.compile(
    r"^/emby/Videos/(?P<media_id>[^/]+)/(?P<source_id>[^/]+)/Subtitles/(?P<stream_index>\d+)/Stream\.(?:vtt|srt|ass)$",
    re.IGNORECASE,
)
SUBTITLE_TRACK_BOOTSTRAP = """
<script>
(function () {
  window.__subtitleProxyDebug = window.__subtitleProxyDebug || { loads: [] };
  function timeToSeconds(value) {
    var parts = value.trim().split(":");
    if (parts.length !== 3) return 0;
    var seconds = parts[2].split(".");
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
    }


def parse_emby_item_media_id(path):
    parsed = urllib.parse.urlparse(path)
    match = EMBY_ITEM_ID_RE.match(parsed.path)
    if not match:
        return ""
    return urllib.parse.unquote(match.group("media_id"))


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
        if any(isinstance(stream, dict) and stream.get("Type") == "Subtitle" for stream in streams):
            continue
        source_id = str(source.get("Id") or media_id)
        next_index = max([int(stream.get("Index") or 0) for stream in streams if isinstance(stream, dict)] + [-1]) + 1
        for track_index, track in enumerate(tracks):
            if not isinstance(track, dict):
                continue
            label = str(track.get("label") or track.get("lang") or "Subtitle")
            language = str(track.get("lang") or "und")
            stream_index = next_index + track_index
            delivery_url = "/emby/Videos/%s/%s/Subtitles/%d/Stream.vtt?mp_track=%d" % (
                urllib.parse.quote(str(media_id), safe=""),
                urllib.parse.quote(source_id, safe=""),
                stream_index,
                track_index,
            )
            streams.append(
                {
                    "Index": stream_index,
                    "Type": "Subtitle",
                    "Codec": "webvtt",
                    "Language": language,
                    "DisplayTitle": label,
                    "Title": label,
                    "IsExternal": True,
                    "IsForced": False,
                    "IsDefault": track_index == 0,
                    "IsTextSubtitleStream": True,
                    "SupportsExternalStream": True,
                    "DeliveryMethod": "External",
                    "DeliveryUrl": delivery_url,
                }
            )
            changed = True
    return changed


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
        if method == "GET" and not head_only and self._serve_emby_subtitle_stream(headers):
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

    def _fetch_msg_subtitle_tracks(self, media_id):
        try:
            status, _headers, body = self._read_msg_api("/media/%s/subtitles" % urllib.parse.quote(str(media_id), safe=""))
        except RuntimeError as exc:
            print("subtitle proxy MSG subtitle list error: %s" % exc, flush=True)
            return []
        if status < 200 or status >= 300 or not body:
            print("subtitle proxy MSG subtitle list HTTP %s for media %s" % (status, media_id), flush=True)
            return []
        try:
            payload = json.loads(body.decode("utf-8"))
        except (TypeError, ValueError):
            print("subtitle proxy MSG subtitle list invalid JSON for media %s" % media_id, flush=True)
            return []
        tracks = payload.get("tracks") if isinstance(payload, dict) else None
        if not isinstance(tracks, list):
            return []
        return [track for track in tracks if isinstance(track, dict) and track.get("path")]

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
            if media_id:
                try:
                    payload = json.loads(body.decode("utf-8"))
                except (TypeError, ValueError):
                    payload = None
                if isinstance(payload, dict):
                    tracks = self._fetch_msg_subtitle_tracks(media_id)
                    if inject_emby_subtitle_streams(payload, media_id, tracks):
                        body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
                        content_type = "application/json; charset=utf-8"
                        no_store = True
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
):
    SubtitleProxyHandler.upstream_base_url = upstream
    SubtitleProxyHandler.msg_api_auth = MsgApiAuthenticator(
        msg_api_base_url or os.environ.get("MSG_BASE_URL") or DEFAULT_MSG_API_BASE_URL,
        msg_admin_user if msg_admin_user is not None else os.environ.get("MSG_ADMIN_USER", ""),
        msg_admin_password if msg_admin_password is not None else os.environ.get("MSG_ADMIN_PASSWORD", ""),
    )
    server = ThreadingHTTPServer((host, port), SubtitleProxyHandler)
    print("subtitle proxy listening on %s:%s upstream=%s" % (host, port, upstream), flush=True)
    try:
        server.serve_forever()
    finally:
        server.server_close()
