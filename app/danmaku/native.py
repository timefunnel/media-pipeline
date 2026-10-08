"""固定版本 danmu_api 适配器桥接。进程是实现细节，不增加 HTTP 转发层。"""

import json
import subprocess
import threading
from pathlib import Path

from .core import normalize_danmaku_anime, normalize_danmaku_comments

_BRIDGE_SLOTS = threading.BoundedSemaphore(2)


class NativeSource:
    def __init__(self, name, cache, bridge, search_timeout=30, comment_timeout=90):
        self.name = name
        self.cache = cache
        self.bridge = str(bridge)
        self.timeout = search_timeout
        self.comment_timeout = comment_timeout
        self.base_url = "danmu_api/afc8b8119f981492a5caee52f1e1ebf756bd0d41/" + name

    def enabled(self):
        return True

    def _call(self, action, data, timeout):
        if not Path(self.bridge).is_file():
            raise RuntimeError("danmu_api source bridge is missing")
        try:
            if not _BRIDGE_SLOTS.acquire(timeout=timeout):
                raise RuntimeError("native source request queue timed out")
            try:
                completed = subprocess.run(
                    ["node", "--max-old-space-size=192", self.bridge], input=json.dumps({"action": action, "source": self.name, "data": data}),
                    capture_output=True, text=True, encoding="utf-8", timeout=timeout,
                )
            finally:
                _BRIDGE_SLOTS.release()
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise RuntimeError("%s source bridge failed: %s" % (self.name, exc)) from exc
        if completed.returncode:
            raise RuntimeError("%s source failed: %s" % (self.name, completed.stderr.strip()[-1800:]))
        try:
            return json.loads(completed.stdout)
        except ValueError as exc:
            raise RuntimeError("%s source returned invalid JSON" % self.name) from exc

    def search_target(self, target):
        response = self._call("search", target, self.timeout)
        if not isinstance(response, list):
            raise RuntimeError("%s source returned invalid search response" % self.name)
        for anime in response:
            if not isinstance(anime, dict) or not isinstance(anime.get("episodes"), list):
                raise RuntimeError("%s source returned invalid episode list" % self.name)
            for episode in anime.get("episodes", []):
                if not isinstance(episode, dict) or not str(episode.get("episodeId", "")).isdigit() or not episode.get("url"):
                    raise RuntimeError("%s source returned invalid episode reference" % self.name)
                key = "reference_%s_%s" % (self.name, episode["episodeId"])
                previous = self.cache.load(key, 0)
                if previous and previous.get("url") != episode["url"]:
                    raise RuntimeError("stable episode reference collision")
                self.cache.save(key, {"url": episode["url"]})
        return [normalize_danmaku_anime(item) for item in response]

    def comment(self, episode_id, with_related=True, ch_convert=0):
        if ch_convert:
            raise ValueError("native source character conversion is not supported")
        reference = self.cache.load("reference_%s_%s" % (self.name, episode_id), 0)
        if reference is None:
            raise ValueError("%s episode reference is unknown; match this episode first" % self.name)
        result = self._call("comment", reference, self.comment_timeout)
        if not isinstance(result, dict) or not isinstance(result.get("comments"), list):
            raise RuntimeError("%s source returned invalid comments" % self.name)
        comments, skipped = normalize_danmaku_comments(result["comments"])
        return {"comments": comments, "skipped": skipped}
