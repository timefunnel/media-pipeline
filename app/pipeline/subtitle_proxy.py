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
        if not head_only and body and should_normalize_subtitle(content_type, body):
            body = normalize_webvtt_timestamps(body.decode("utf-8", "replace")).encode("utf-8")
            content_type = "text/vtt; charset=utf-8"

        self.send_response(status)
        for key, value in headers.items():
            lower_key = key.lower()
            if lower_key in HOP_BY_HOP_HEADERS or lower_key in ("content-length", "content-encoding"):
                continue
            if lower_key == "content-type" and content_type:
                continue
            self.send_header(key, value)
        if content_type:
            self.send_header("Content-Type", content_type)
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
