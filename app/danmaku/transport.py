"""弹幕 HTTP 传输：限制响应体，不隐藏 HTTP/JSON 错误，不自动重试。"""

import json
import urllib.error
import urllib.parse
import urllib.request


class DanmakuHttpTransport:
    def __init__(self, proxy_url=""):
        handlers = []
        if proxy_url:
            parsed = urllib.parse.urlparse(proxy_url)
            if parsed.scheme not in ("http", "https") or not parsed.hostname:
                raise ValueError("DANMAKU_PROXY_URL must be an HTTP(S) URL")
            handlers.append(urllib.request.ProxyHandler({"http": proxy_url, "https": proxy_url}))
        self.opener = urllib.request.build_opener(*handlers)

    def json_request(self, method, url, headers=None, data=None, timeout=12):
        body = None if data is None else json.dumps(data, ensure_ascii=False).encode("utf-8")
        req_headers = dict(headers or {})
        if body is not None:
            req_headers["Content-Type"] = "application/json"
        request = urllib.request.Request(url, data=body, headers=req_headers, method=method)
        try:
            with self.opener.open(request, timeout=timeout) as response:
                raw = response.read(32 * 1024 * 1024 + 1)
        except urllib.error.HTTPError as exc:
            raise RuntimeError("danmaku upstream HTTP %s" % exc.code) from exc
        except (OSError, TimeoutError) as exc:
            raise RuntimeError("danmaku upstream request failed: %s" % exc) from exc
        if len(raw) > 32 * 1024 * 1024:
            raise RuntimeError("danmaku upstream response exceeds 32 MiB")
        try:
            return json.loads(raw.decode("utf-8"))
        except (ValueError, UnicodeError) as exc:
            raise RuntimeError("danmaku upstream returned invalid JSON") from exc
