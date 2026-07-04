import http.server
import re
import socketserver
import urllib.error
import urllib.parse
import urllib.request


DEFAULT_SUBTITLE_PROXY_HOST = "127.0.0.1"
DEFAULT_SUBTITLE_PROXY_PORT = 18081
DEFAULT_SUBTITLE_PROXY_UPSTREAM = "http://127.0.0.1:18080"

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


class SubtitleProxyHandler(http.server.BaseHTTPRequestHandler):
    upstream_base_url = DEFAULT_SUBTITLE_PROXY_UPSTREAM

    def do_GET(self):
        self._proxy()

    def do_HEAD(self):
        self._proxy(head_only=True)

    def _proxy(self, head_only=False):
        upstream_url = urllib.parse.urljoin(self.upstream_base_url.rstrip("/") + "/", self.path.lstrip("/"))
        headers = {
            key: value
            for key, value in self.headers.items()
            if key.lower() not in HOP_BY_HOP_HEADERS and key.lower() != "host"
        }
        headers["Accept-Encoding"] = "identity"
        request = urllib.request.Request(upstream_url, headers=headers, method="HEAD" if head_only else "GET")
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                body = b"" if head_only else response.read()
                self._write_response(response.status, response.headers, body, head_only=head_only)
        except urllib.error.HTTPError as exc:
            body = b"" if head_only else exc.read()
            self._write_response(exc.code, exc.headers, body, head_only=head_only)
        except (OSError, TimeoutError) as exc:
            self.send_response(502)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.end_headers()
            if not head_only:
                self.wfile.write(("subtitle proxy upstream error: %s\n" % exc).encode("utf-8"))

    def _write_response(self, status, headers, body, head_only=False):
        content_type = headers.get("Content-Type", "")
        no_store = False
        if not head_only and body and should_normalize_subtitle(content_type, body):
            body = normalize_webvtt_timestamps(body.decode("utf-8", "replace")).encode("utf-8")
            content_type = "text/vtt; charset=utf-8"
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


def run_subtitle_proxy(host=DEFAULT_SUBTITLE_PROXY_HOST, port=DEFAULT_SUBTITLE_PROXY_PORT, upstream=DEFAULT_SUBTITLE_PROXY_UPSTREAM):
    SubtitleProxyHandler.upstream_base_url = upstream
    server = ThreadingHTTPServer((host, port), SubtitleProxyHandler)
    print("subtitle proxy listening on %s:%s upstream=%s" % (host, port, upstream), flush=True)
    try:
        server.serve_forever()
    finally:
        server.server_close()
