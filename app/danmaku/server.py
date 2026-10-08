"""MSG 的独立弹幕后端。仅接收已识别的媒体字段，不读取媒体文件或云盘。"""

import hmac
import json
import logging
import math
import os
import signal
import shutil
import threading
from urllib.parse import urlsplit, parse_qs
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from .config import Config
from .core import DanmakuImportError, build_danmaku_local_import_from_config, build_danmaku_matcher_from_config
from .prewarm import DanmakuPrewarmManager


def validate_target(value):
    if not isinstance(value, dict):
        raise ValueError("target must be an object containing identified media metadata")
    target = dict(value)
    for field in ("title", "original_title"):
        if not isinstance(target.get(field, ""), str):
            raise ValueError(field + " must be a string")
        target[field] = target.get(field, "").strip()
    for field in ("season", "episode", "year"):
        raw = target.get(field, 0)
        if isinstance(raw, bool) or not str(raw).isdigit():
            raise ValueError(field + " must be a nonnegative integer")
        target[field] = int(raw)
    tmdb = str(target.get("tmdb_id") or "").strip()
    if not tmdb.isdigit() or int(tmdb) < 1 or not target["title"]:
        raise ValueError("identified title and positive tmdb_id are required")
    target["tmdb_id"] = tmdb
    if bool(target["season"]) != bool(target["episode"]):
        raise ValueError("both season and episode are required for a series")
    if len(target["title"]) > 255 or len(target["original_title"]) > 255:
        raise ValueError("identified title is too long")
    return target


class Application:
    def __init__(self, config, matcher=None):
        self.config = config
        if matcher is None and any(name in ('tencent', 'iqiyi') for name in config.danmaku_providers):
            bridge = Path(config.danmaku_bridge)
            if shutil.which('node') is None or not bridge.is_file() or not (bridge.parent / 'node_modules/danmu-api-server/package.json').is_file():
                raise RuntimeError('native source runtime is missing; install Node.js and run npm ci in danmaku-server')
        Path(config.danmaku_cache_dir).mkdir(parents=True, exist_ok=True)
        self.matcher = matcher or build_danmaku_matcher_from_config(config)
        self.local_import = build_danmaku_local_import_from_config(config)
        self.prewarm = DanmakuPrewarmManager(self, delay_seconds=config.danmaku_prewarm_delay_seconds,
                                            max_episodes=config.danmaku_prewarm_max_episodes)

    def match(self, payload):
        target = validate_target(payload.get("target"))
        result = self.matcher.match(tmdb_id=target["tmdb_id"], anime=target["title"], season=target["season"],
                                    episode=target["episode"] or None, original_title=target["original_title"], year=target["year"])
        result.setdefault("attempts", []).insert(0, {"source": "danmaku_server", "mode": "priority_v1", "outcome": "evaluated", "cached": result.get("cached", False)})
        if not result.get("matched") and any(item.get("outcome") == "error" for item in result.get("attempts", [])):
            raise RuntimeError("danmaku source matching failed: " + json.dumps(result["attempts"], ensure_ascii=False))
        logging.info("danmaku match media=%s source=%s matched=%s attempts=%s", payload.get("media_id", ""),
                     result.get("source"), result.get("matched"), json.dumps(result.get("attempts", []), ensure_ascii=False))
        return {"media_id": payload.get("media_id", ""), "target": target, "match": result}

    def danmaku_comments(self, media_id, target=None, **kwargs):
        if not kwargs.get("episode_id"):
            result = self.match({"media_id": media_id, "target": target})["match"]
            if not result.get("matched"):
                raise ValueError("no exact season/episode match: " + json.dumps(result.get("attempts", []), ensure_ascii=False))
            kwargs.update(episode_id=result["episode_id"], source=result["source"],
                          anime_title=result.get("anime_title", ""), episode_title=result.get("episode_title", ""),
                          match_mode=result.get("match_mode", ""), provider_shift_seconds=result.get("shift", 0))
        source = kwargs.pop("source", "")
        if not source:
            raise ValueError("source is required for an explicit episode reference")
        episode = kwargs.pop("episode_id")
        payload = self.matcher.comments(episode, source_name=source, **kwargs)
        payload["media_id"] = media_id
        return payload

    def comment(self, payload):
        validate_options(payload)
        fields = ("episode_id", "source", "ch_convert", "offset_seconds", "provider_shift_seconds", "anime_title",
                  "episode_title", "match_mode", "with_related")
        kwargs = {field: payload[field] for field in fields if field in payload}
        if payload.get("target") is not None:
            kwargs["target"] = validate_target(payload["target"])
        return self.danmaku_comments(str(payload.get("media_id") or ""), **kwargs)

    def dispatch(self, method, path, payload):
        parsed = urlsplit(path)
        path = parsed.path
        if method == 'GET' and path == '/v1/danmaku/season/prewarm':
            raw = parse_qs(parsed.query).get('limit', ['10'])[0]
            if not raw.isdigit() or not 1 <= int(raw) <= 50:
                raise ValueError('limit must be an integer between 1 and 50')
            return 200, {'items': self.prewarm.recent(limit=int(raw))}
        if method == "POST" and path == "/v1/danmaku/match":
            return 200, self.match(payload)
        if method == "POST" and path == "/v1/danmaku/comment":
            return 200, self.comment(payload)
        if method == "POST" and path == "/v1/danmaku/parse":
            validate_options(payload)
            return 200, self.local_import.payload(payload.get("content"), source_format=payload.get("format", "auto"),
                                                  offset_seconds=payload.get("offset_seconds", 0),
                                                  ch_convert=payload.get("ch_convert", 0), title=payload.get("title", ""))
        if method == "POST" and path == "/v1/danmaku/season/prewarm":
            episodes = payload.get("episodes")
            if not isinstance(episodes, list):
                raise ValueError("episodes must be a list")
            for episode in episodes:
                if not isinstance(episode, dict):
                    raise ValueError("episode must contain identified target metadata")
                episode["target"] = validate_target(episode.get("target"))
            return 202, self.prewarm.submit(payload.get("owner_id"), payload.get("media_id"), payload.get("season"), episodes)
        if method == "GET" and path.startswith("/v1/danmaku/season/prewarm/"):
            task = self.prewarm.get(path.rsplit("/", 1)[-1])
            return (200, task) if task is not None else (404, {"error": {"code": "danmaku_prewarm_not_found", "message": "task not found"}})
        return 404, {"error": {"code": "not_found", "message": "endpoint not found"}}


class Server:
    def __init__(self, config, application=None):
        self.config = config
        self.application = application or Application(config)
        server = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                self.handle_api()

            def do_POST(self):
                self.handle_api()

            def log_message(self, format, *args):
                logging.info("danmaku HTTP " + format, *args)

            def send_json(self, status, payload):
                raw = json.dumps(payload, ensure_ascii=False).encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(raw)))
                self.send_header("Cache-Control", "no-store")
                self.end_headers()
                self.wfile.write(raw)

            def handle_api(self):
                self.connection.settimeout(30)
                if self.command == "GET" and self.path == "/healthz":
                    self.send_json(200, {"status": "ok", "sources": list(config.danmaku_providers),
                                         "revision": os.getenv("DANMAKU_REVISION", "local"),
                                         "version": os.getenv("DANMAKU_VERSION", "local")})
                    return
                auth = self.headers.get("Authorization", "")
                if not hmac.compare_digest(auth.encode("utf-8"), ("Bearer " + config.token).encode("utf-8")):
                    # 小型请求先消费已发送的 body，避免关闭连接时未读数据导致 TCP reset 吞掉 401。
                    self.drain_body(256 * 1024)
                    self.send_json(401, {"error": {"code": "unauthorized", "message": "invalid token"}})
                    return
                try:
                    payload = {}
                    if self.command == "POST":
                        length = int(self.headers.get("Content-Length", "0"))
                        limit = config.danmaku_import_max_bytes * 2 + 65536 if self.path == "/v1/danmaku/parse" else 256 * 1024
                        if length <= 0:
                            raise ValueError("JSON request body is required")
                        if length > limit:
                            self.drain_body(limit + 65536)
                            self.send_json(413, {"error": {"code": "body_too_large", "message": "request body is too large"}})
                            return
                        payload = json.loads(self.rfile.read(length).decode("utf-8"))
                        if not isinstance(payload, dict):
                            raise ValueError("request body must be an object")
                    status, body = server.application.dispatch(self.command, self.path, payload)
                    self.send_json(status, body)
                except (ValueError, UnicodeError, DanmakuImportError) as exc:
                    self.send_json(400, {"error": {"code": getattr(exc, "code", "invalid_input"), "message": str(exc)}})
                except RuntimeError as exc:
                    logging.error("danmaku request failed: %s", exc)
                    self.send_json(502, {"error": {"code": "danmaku_upstream_failed", "message": str(exc)}})
                except Exception:
                    logging.exception("unexpected danmaku server failure")
                    self.send_json(500, {"error": {"code": "internal_error", "message": "internal danmaku error"}})

            def drain_body(self, maximum):
                length = self.headers.get('Content-Length', '0')
                if length.isdigit() and 0 < int(length) <= maximum:
                    self.rfile.read(int(length))

        self.httpd = ThreadingHTTPServer((config.host, config.port), Handler)
        self.httpd.daemon_threads = True
        self.thread = None

    def start(self):
        self.application.prewarm.start()
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        self.thread.start()

    def stop(self):
        self.httpd.shutdown()
        self.httpd.server_close()
        if self.thread is not None:
            self.thread.join(timeout=5)
        self.application.prewarm.stop()


def validate_options(payload):
    for field in ("offset_seconds", "provider_shift_seconds"):
        value = payload.get(field, 0)
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or abs(value) > 600:
            raise ValueError(field + " must be a finite number between -600 and 600")
    if payload.get("ch_convert", 0) not in (0, 1, 2) or isinstance(payload.get("ch_convert"), bool):
        raise ValueError("ch_convert must be 0, 1 or 2")
    if "with_related" in payload and not isinstance(payload["with_related"], bool):
        raise ValueError("with_related must be a boolean")


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    server = Server(Config.from_env())
    server.start()
    stopped = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stopped.set())
    signal.signal(signal.SIGINT, lambda *_: stopped.set())
    try:
        stopped.wait()
    finally:
        server.stop()


if __name__ == "__main__":
    main()
