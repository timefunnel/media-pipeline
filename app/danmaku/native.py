"""固定版本 danmu_api 适配器桥接。进程是实现细节，不增加 HTTP 转发层。"""

from .bridge import BridgePool
from .core import normalize_danmaku_anime, normalize_danmaku_comments


class NativeSource:
    def __init__(self, name, cache, bridge, search_timeout=30, comment_timeout=90, pool=None):
        self.name = name
        self.cache = cache
        self.bridge = str(bridge)
        self.pool = pool if pool is not None else BridgePool(bridge)
        self.timeout = search_timeout
        self.comment_timeout = comment_timeout
        self.base_url = "danmu_api/afc8b8119f981492a5caee52f1e1ebf756bd0d41/" + name

    def enabled(self):
        return True

    def _call(self, action, data, timeout):
        return self.pool.call(action, self.name, data, timeout)

    def close(self):
        self.pool.close()

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
                if previous is not None and previous.get("url") != episode["url"]:
                    raise RuntimeError("stable episode reference collision")
                if previous is None:
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
