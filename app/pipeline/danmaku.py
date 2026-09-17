"""弹弹play 开放弹幕网络 / 兼容聚合服务的弹幕适配、归一化与缓存。

设计约束（依据 docs/调研-播放器内置弹幕模块-2026-09-11.md）：
- 服务端只做「归一化」，**不下发 ASS**：对客户端输出沿用弹弹play JSON 的字段
  （``cid`` / ``p`` / ``m``），并补充结构化字段供客户端直接渲染。
- 凭证只存在于服务端；签名算法 ``base64(sha256(AppId + Timestamp + Path + AppSecret))``，
  其中 Path 不含域名与查询参数。
- 弹幕接口没有分页能力（``from``/``to`` 官方文档存在但实测无效），因此**整集缓存**，
  下发前再做密度采样。
- 自动匹配先用 TMDB ID 反查并消歧；只有 TMDB 无法得到唯一节目编号时，才允许
  用作品标题与同一集号再查一次。关键词结果必须作品名完全一致且唯一，失败如实上报。

字段事实（实测，勿照抄 B 站 8 段解析）：
- 弹弹play 的 ``p`` 是 **4 段**：``时间(秒),模式,颜色(十进制RGB),用户/来源``。
- B 站 XML / 部分聚合服务是 **8 段**：``时间,模式,字号,颜色,时间戳,弹幕池,用户,行号``。
- ``cid`` 是超出 JS 安全整数范围的 64 位整数，对外一律按字符串下发。
"""

import base64
import hashlib
import json
import re
import threading
import time
import unicodedata
import urllib.parse
import xml.etree.ElementTree as ET
from pathlib import Path

from .external_subtitles import SubtitleHttpTransport


DEFAULT_DANDANPLAY_BASE_URL = "https://api.dandanplay.net"
DEFAULT_DANMAKU_CACHE_DIR = "/danmaku-cache"
DEFAULT_DANMAKU_PROVIDERS = ("dandanplay",)
DEFAULT_DANMAKU_CACHE_TTL_SECONDS = 7 * 24 * 3600
DEFAULT_DANMAKU_SEARCH_CACHE_TTL_SECONDS = 24 * 3600
DEFAULT_DANMAKU_SEARCH_TIMEOUT_SECONDS = 12
DEFAULT_DANMAKU_COMMENT_TIMEOUT_SECONDS = 30
DEFAULT_DANMAKU_MAX_COMMENTS = 6000
# 本地导入文件的体积上限：整集 B 站 XML 通常 100KB~1.5MB，8MB 足够容纳超大热番，
# 同时保证一次请求不会把内存吃满（HTTP 层对该路由单独放宽请求体上限）。
DEFAULT_DANMAKU_IMPORT_MAX_BYTES = 8 * 1024 * 1024
# 本地弹幕不是任何上游源，``source``/``match_mode`` 如实标注为本地导入。
DANMAKU_LOCAL_SOURCE_NAME = "local"
DANMAKU_LOCAL_MATCH_MODE = "import"

# 弹幕模式：与 B 站语义一致。社区实现只对 1/4/5 有稳定共识，
# 6 降级为普通滚动，7/8/9 不下发（避免客户端执行不可信脚本）。
DANMAKU_MODE_NAMES = {
    1: "scroll",
    4: "bottom",
    5: "top",
    6: "reverse",
    7: "advanced",
    8: "code",
    9: "bas",
}
DANMAKU_RENDERABLE_MODES = (1, 4, 5)
DANMAKU_DEGRADED_MODES = (6,)

DEFAULT_DANMAKU_COLOR = 0xFFFFFF


def normalize_danmaku_base_url(value):
    text = str(value or "").strip()
    if not text:
        return DEFAULT_DANDANPLAY_BASE_URL
    return text.rstrip("/")


def dandanplay_signature(app_id, timestamp, path, app_secret):
    """``base64(sha256(AppId + Timestamp + Path + AppSecret))``（Path 不含查询串）。"""
    payload = "%s%s%s%s" % (
        str(app_id or ""),
        str(int(timestamp)),
        str(path or ""),
        str(app_secret or ""),
    )
    digest = hashlib.sha256(payload.encode("utf-8")).digest()
    return base64.b64encode(digest).decode("ascii")


def danmaku_comment_path(episode_id):
    return "/api/v2/comment/%s" % urllib.parse.quote(str(episode_id), safe="")


def normalize_danmaku_episode_id(value):
    text = str(value or "").strip()
    if not text:
        return ""
    if not text.isdigit():
        raise ValueError("episode_id must be a numeric string")
    return text


def normalize_danmaku_cid(value):
    """``cid`` 统一按字符串承载：它已超出 JS 安全整数范围（2^53）。"""
    if value is None:
        return ""
    if isinstance(value, bool):
        return ""
    if isinstance(value, int):
        return str(value)
    text = str(value).strip()
    if not text:
        return ""
    return text


def parse_danmaku_color(value):
    try:
        color = int(value)
    except (TypeError, ValueError):
        return DEFAULT_DANMAKU_COLOR
    if color < 0 or color > 0xFFFFFF:
        return DEFAULT_DANMAKU_COLOR
    return color


def parse_danmaku_time(value):
    try:
        seconds = float(value)
    except (TypeError, ValueError):
        return None
    if seconds < 0:
        return None
    return seconds


def parse_comment_p(raw_p):
    """解析弹幕 ``p`` 字段。

    4 段按弹弹play 语义（时间,模式,颜色,用户），8 段按 B 站语义
    （时间,模式,字号,颜色,...）。返回 ``None`` 表示该条不可用（交由调用方丢弃并计数）。
    """
    text = str(raw_p or "").strip()
    if not text:
        return None
    parts = [segment.strip() for segment in text.split(",")]
    if len(parts) < 4:
        return None
    seconds = parse_danmaku_time(parts[0])
    if seconds is None:
        return None
    try:
        mode = int(parts[1])
    except (TypeError, ValueError):
        return None
    if len(parts) >= 8:
        try:
            size = int(parts[2])
        except (TypeError, ValueError):
            size = 25
        color = parse_danmaku_color(parts[3])
        user = parts[6]
        row_id = normalize_danmaku_cid(parts[7])
    else:
        size = 25
        color = parse_danmaku_color(parts[2])
        user = parts[3]
        row_id = ""
    return {
        "time": seconds,
        "mode": mode,
        "mode_name": DANMAKU_MODE_NAMES.get(mode, "unknown"),
        "color": color,
        "size": size,
        "user": user,
        "row_id": row_id,
    }


def normalize_danmaku_comment(entry):
    """把单条原始弹幕归一成对外结构；不可用时返回 ``None``。"""
    if not isinstance(entry, dict):
        return None
    text = entry.get("m")
    if text is None:
        return None
    text = str(text).replace("\r\n", "\n").replace("\r", "\n").strip()
    if not text:
        return None
    parsed = parse_comment_p(entry.get("p") if entry.get("p") is not None else entry.get("c"))
    if parsed is None:
        return None
    return {
        "cid": normalize_danmaku_cid(entry.get("cid")),
        "p": str(entry.get("p") if entry.get("p") is not None else entry.get("c") or ""),
        "m": text,
        "time": parsed["time"],
        "mode": parsed["mode"],
        "mode_name": parsed["mode_name"],
        "color": parsed["color"],
        "size": parsed["size"],
        "user": parsed["user"],
    }


def normalize_danmaku_comments(entries):
    """归一化整包弹幕，返回 ``(comments, skipped)``。跳过数量必须向上汇报，不得静默吞掉。"""
    comments = []
    skipped = 0
    for entry in entries or []:
        normalized = normalize_danmaku_comment(entry)
        if normalized is None:
            skipped += 1
            continue
        comments.append(normalized)
    comments.sort(key=lambda item: (item["time"], item["cid"]))
    return comments, skipped


def dedupe_danmaku(comments):
    """按 (时间, 模式, 文本) 去重，保留首次出现。"""
    seen = set()
    out = []
    for item in comments or []:
        key = (round(float(item.get("time") or 0.0), 2), int(item.get("mode") or 1), item.get("m") or "")
        if key in seen:
            continue
        seen.add(key)
        out.append(item)
    return out


def filter_danmaku_modes(comments):
    """丢掉客户端无法安全渲染的模式，返回 ``(comments, dropped)``。

    保留 1/4/5（滚动/底部/顶部）与 6（逆向滚动，可降级为普通滚动）；
    丢弃 7（高级弹幕，携带坐标脚本）、8（代码弹幕）、9（BAS）—— 这些需要执行
    不可信内容或复杂排版，三端都不实现。丢弃数量必须上报，不能静默吞掉。
    """
    kept = []
    dropped = 0
    for item in comments or []:
        try:
            mode = int(item.get("mode") or 0)
        except (TypeError, ValueError):
            mode = 0
        if mode in DANMAKU_RENDERABLE_MODES or mode in DANMAKU_DEGRADED_MODES:
            kept.append(item)
        else:
            dropped += 1
    return kept, dropped


def filter_danmaku_keywords(comments, keywords):
    """关键词/正则黑名单过滤；空规则时原样返回。"""
    patterns = []
    for raw in keywords or []:
        text = str(raw or "").strip()
        if not text:
            continue
        try:
            patterns.append(re.compile(text, re.IGNORECASE))
        except re.error:
            patterns.append(re.compile(re.escape(text), re.IGNORECASE))
    if not patterns:
        return list(comments or [])
    out = []
    for item in comments or []:
        text = item.get("m") or ""
        if any(pattern.search(text) for pattern in patterns):
            continue
        out.append(item)
    return out


def apply_danmaku_offset(comments, seconds):
    offset = float(seconds or 0.0)
    if not offset:
        return list(comments or [])
    out = []
    for item in comments or []:
        shifted = dict(item)
        value = float(item.get("time") or 0.0) + offset
        shifted["time"] = value if value > 0 else 0.0
        out.append(shifted)
    out.sort(key=lambda item: (item["time"], item.get("cid") or ""))
    return out


def sample_danmaku(comments, limit):
    """按时间**均匀采样**（而不是截断），保证整段视频都有弹幕覆盖。"""
    items = list(comments or [])
    try:
        maximum = int(limit)
    except (TypeError, ValueError):
        return items, False
    if maximum <= 0 or len(items) <= maximum:
        return items, False
    total = len(items)
    step = total / float(maximum)
    sampled = []
    index = 0.0
    for _ in range(maximum):
        position = int(index)
        if position >= total:
            position = total - 1
        sampled.append(items[position])
        index += step
    return sampled, True


def build_danmaku_payload(
    comments,
    source,
    episode_id,
    anime_title="",
    episode_title="",
    match_mode="",
    confidence=None,
    provider_shift_seconds=0.0,
    offset_seconds=0.0,
    max_comments=DEFAULT_DANMAKU_MAX_COMMENTS,
    blacklist=None,
    ch_convert=0,
    skipped=0,
    cached=False,
):
    """把归一化后的弹幕组装成对客户端下发的结构（弹弹play JSON 字段 + 结构化补充）。"""
    items = dedupe_danmaku(comments)
    total = len(items)
    items, dropped_modes = filter_danmaku_modes(items)
    items = filter_danmaku_keywords(items, blacklist)
    filtered = total - dropped_modes - len(items)
    items = apply_danmaku_offset(items, provider_shift_seconds)
    items = apply_danmaku_offset(items, offset_seconds)
    items, truncated = sample_danmaku(items, max_comments)
    return {
        "source": source,
        "episode_id": str(episode_id),
        "anime_title": anime_title or "",
        "episode_title": episode_title or "",
        "match_mode": match_mode or "",
        "confidence": confidence,
        "provider_shift_seconds": float(provider_shift_seconds or 0.0),
        "offset_seconds": float(offset_seconds or 0.0),
        "ch_convert": int(ch_convert or 0),
        "count": len(items),
        "total": total,
        "filtered": filtered,
        "dropped_modes": dropped_modes,
        "skipped": int(skipped or 0),
        "truncated": truncated,
        "cached": bool(cached),
        "comments": items,
    }


class DanmakuCache:
    """磁盘缓存：key -> JSON 包 + 抓取时间，按 TTL 判定过期。"""

    def __init__(self, root_dir=DEFAULT_DANMAKU_CACHE_DIR):
        self.root_dir = Path(root_dir or DEFAULT_DANMAKU_CACHE_DIR)

    def _path(self, key):
        safe = re.sub(r"[^0-9A-Za-z._-]+", "_", str(key or "").strip())
        if not safe:
            raise ValueError("danmaku cache key is required")
        return self.root_dir / ("%s.json" % safe)

    def load(self, key, ttl_seconds=DEFAULT_DANMAKU_CACHE_TTL_SECONDS):
        path = self._path(key)
        if not path.exists():
            return None
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return None
        if not isinstance(payload, dict):
            return None
        fetched_at = payload.get("fetched_at")
        try:
            age = time.time() - float(fetched_at)
        except (TypeError, ValueError):
            return None
        try:
            ttl = float(ttl_seconds)
        except (TypeError, ValueError):
            ttl = float(DEFAULT_DANMAKU_CACHE_TTL_SECONDS)
        if ttl > 0 and age > ttl:
            return None
        body = payload.get("body")
        return body if isinstance(body, dict) else None

    def save(self, key, body):
        path = self._path(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        record = {"fetched_at": time.time(), "body": body}
        tmp_path = path.with_suffix(".tmp")
        tmp_path.write_text(json.dumps(record, ensure_ascii=False), encoding="utf-8")
        tmp_path.replace(path)
        return path


class DanmakuSource:
    """弹幕源基类：只定义契约，具体实现见下。"""

    name = ""

    def enabled(self):
        raise NotImplementedError

    def search_episodes(self, tmdb_id=None, episode=None, anime=None):
        raise NotImplementedError

    def comment(self, episode_id, with_related=True, ch_convert=0):
        raise NotImplementedError


class DandanplayProtocolSource(DanmakuSource):
    """弹弹play 协议客户端；``AggregatorSource`` 复用同一套请求/解析逻辑。"""

    name = "dandanplay"
    requires_signature = True

    def __init__(
        self,
        app_id="",
        app_secret="",
        base_url=DEFAULT_DANDANPLAY_BASE_URL,
        timeout=DEFAULT_DANMAKU_SEARCH_TIMEOUT_SECONDS,
        comment_timeout=DEFAULT_DANMAKU_COMMENT_TIMEOUT_SECONDS,
        transport=None,
    ):
        self.app_id = str(app_id or "").strip()
        self.app_secret = str(app_secret or "").strip()
        self.base_url = normalize_danmaku_base_url(base_url)
        self.timeout = max(1, int(timeout or DEFAULT_DANMAKU_SEARCH_TIMEOUT_SECONDS))
        self.comment_timeout = max(1, int(comment_timeout or DEFAULT_DANMAKU_COMMENT_TIMEOUT_SECONDS))
        self.transport = transport or SubtitleHttpTransport()

    def enabled(self):
        if not self.app_id:
            return False
        if self.requires_signature and not self.app_secret:
            return False
        return True

    def _headers(self, path):
        headers = {
            "Accept": "application/json",
            "User-Agent": "MediaStationGo-Danmaku/1.0",
        }
        if not self.requires_signature:
            return headers
        timestamp = int(time.time())
        headers["X-AppId"] = self.app_id
        headers["X-Timestamp"] = str(timestamp)
        headers["X-Signature"] = dandanplay_signature(self.app_id, timestamp, path, self.app_secret)
        return headers

    def _get_json(self, path, params=None, timeout=None):
        url = self.base_url + path
        if params:
            filtered = {key: value for key, value in params.items() if value not in (None, "")}
            if filtered:
                url = url + "?" + urllib.parse.urlencode(filtered)
        payload = self.transport.json_request(
            "GET",
            url,
            headers=self._headers(path),
            timeout=timeout or self.timeout,
        )
        return self._unwrap(payload)

    def _unwrap(self, payload):
        if not isinstance(payload, dict):
            raise RuntimeError("%s returned a non-object response" % self.name)
        if payload.get("success") is False:
            code = payload.get("errorCode")
            message = payload.get("errorMessage") or "unknown error"
            raise RuntimeError("%s API error %s: %s" % (self.name, code, message))
        return payload

    def search_episodes(self, tmdb_id=None, episode=None, anime=None):
        normalized_tmdb_id = str(tmdb_id or "").strip()
        normalized_anime = str(anime or "").strip()
        if not normalized_tmdb_id and not normalized_anime:
            raise ValueError("anime or tmdbId is required for danmaku search")
        params = {"tmdbId": normalized_tmdb_id, "anime": normalized_anime, "v2": "true"}
        if episode:
            params["episode"] = str(episode)
        payload = self._get_json("/api/v2/search/episodes", params=params)
        animes = payload.get("animes")
        if not isinstance(animes, list):
            animes = []
        return [normalize_danmaku_anime(item) for item in animes if isinstance(item, dict)]

    def comment(self, episode_id, with_related=True, ch_convert=0):
        episode = normalize_danmaku_episode_id(episode_id)
        if not episode:
            raise ValueError("episode_id is required for danmaku comment")
        params = {
            "withRelated": "true" if with_related else "false",
            "chConvert": str(int(ch_convert or 0)),
        }
        payload = self._get_json(
            danmaku_comment_path(episode),
            params=params,
            timeout=self.comment_timeout,
        )
        raw_comments = payload.get("comments")
        if not isinstance(raw_comments, list):
            raw_comments = []
        comments, skipped = normalize_danmaku_comments(raw_comments)
        try:
            declared = int(payload.get("count"))
        except (TypeError, ValueError):
            declared = len(raw_comments)
        return {
            "episode_id": episode,
            "count": declared,
            "raw_count": len(raw_comments),
            "comments": comments,
            "skipped": skipped,
        }


class AggregatorSource(DandanplayProtocolSource):
    """自建「弹弹play 兼容」聚合服务（如 danmu_api / misaka_danmu_server）。

    与官方源的差别只有 base_url 与是否需要签名；协议保持一致，
    以便在配置里换源而不改代码。
    """

    name = "aggregator"
    requires_signature = False

    def __init__(self, base_url="", timeout=DEFAULT_DANMAKU_SEARCH_TIMEOUT_SECONDS, transport=None, **kwargs):
        if not str(base_url or "").strip():
            raise ValueError("aggregator danmaku source requires a base_url")
        super().__init__(
            app_id="aggregator",
            app_secret="",
            base_url=base_url,
            timeout=timeout,
            transport=transport,
            **kwargs,
        )

    def enabled(self):
        return bool(self.base_url)


def normalize_danmaku_anime(item):
    episodes = item.get("episodes")
    if not isinstance(episodes, list):
        episodes = []
    return {
        "anime_id": normalize_danmaku_cid(item.get("animeId")),
        "anime_title": str(item.get("animeTitle") or ""),
        "type": str(item.get("type") or ""),
        "type_description": str(item.get("typeDescription") or ""),
        "image_url": str(item.get("imageUrl") or ""),
        "episodes": [
            {
                "episode_id": normalize_danmaku_episode_id(entry.get("episodeId")),
                "episode_title": str(entry.get("episodeTitle") or ""),
                "episode_number": str(entry.get("episodeNumber") or ""),
            }
            for entry in episodes
            if isinstance(entry, dict)
        ],
    }


def normalize_danmaku_search_title(value):
    """只折叠 Unicode、大小写与空白；标点和正文必须仍然完全一致。"""
    text = unicodedata.normalize("NFKC", str(value or ""))
    return re.sub(r"\s+", " ", text).strip().casefold()


def normalize_danmaku_episode_number(value):
    text = unicodedata.normalize("NFKC", str(value or "")).strip()
    if re.fullmatch(r"\d+", text):
        return str(int(text))
    return text


def episode_number_from_exact_title(value):
    """只接受完整的“第 N 话/集/期”，避免从描述文字里猜集号。"""
    text = unicodedata.normalize("NFKC", str(value or "")).strip()
    matched = re.fullmatch(r"第\s*0*(\d+)\s*(?:话|集|期)", text)
    if not matched:
        return ""
    return str(int(matched.group(1)))


class DanmakuMatcher:
    """匹配 + 回源 + 缓存的编排层。

    自动匹配先使用 TMDB ID 反查并消歧；只有无法得到唯一节目编号时才使用作品标题
    和同一集号查询一次。每个源按配置顺序尝试，结果里必须如实标明来源与模式；
    缺少 TMDB ID 时直接返回未匹配，绝不直接从标题起步。
    """

    def __init__(
        self,
        sources,
        cache=None,
        cache_ttl_seconds=DEFAULT_DANMAKU_CACHE_TTL_SECONDS,
        search_cache_ttl_seconds=DEFAULT_DANMAKU_SEARCH_CACHE_TTL_SECONDS,
        max_comments=DEFAULT_DANMAKU_MAX_COMMENTS,
        blacklist=None,
    ):
        self.sources = [source for source in (sources or []) if source is not None]
        self.cache = cache if cache is not None else DanmakuCache()
        self.cache_ttl_seconds = max(0, int(cache_ttl_seconds or 0))
        self.search_cache_ttl_seconds = max(0, int(search_cache_ttl_seconds or 0))
        self.max_comments = max(1, int(max_comments or DEFAULT_DANMAKU_MAX_COMMENTS))
        self.blacklist = tuple(blacklist or ())
        # 固定数量的分片锁避免同一缓存键并发未命中时击穿上游，也避免按搜索词永久累积锁对象。
        self._cache_locks = tuple(threading.Lock() for _ in range(64))

    def enabled_sources(self):
        return [source for source in self.sources if source.enabled()]

    def _source_by_name(self, name):
        wanted = str(name or "").strip().lower()
        for source in self.enabled_sources():
            if source.name == wanted:
                return source
        return None

    def _source_cache_identity(self, source):
        return {
            "name": source.name,
            "base_url": str(getattr(source, "base_url", "") or "").rstrip("/"),
        }

    def _cache_key(self, prefix, value):
        serialized = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        digest = hashlib.sha256(serialized.encode("utf-8")).hexdigest()
        return "%s_%s" % (prefix, digest)

    def _cache_lock(self, key):
        digest = hashlib.sha256(str(key).encode("utf-8")).digest()
        return self._cache_locks[int.from_bytes(digest[:4], "big") % len(self._cache_locks)]

    def _cached_call(self, key, ttl_seconds, loader):
        cached = self.cache.load(key, ttl_seconds)
        if cached is not None:
            return cached, True
        with self._cache_lock(key):
            # 等锁期间另一个请求可能已经完成回源，必须二次检查。
            cached = self.cache.load(key, ttl_seconds)
            if cached is not None:
                return cached, True
            body = loader()
            self.cache.save(key, body)
            return body, False

    def _search_source(self, source, tmdb_id="", episode=None, anime=""):
        normalized_tmdb_id = str(tmdb_id or "").strip()
        normalized_anime = str(anime or "").strip()
        cache_key = self._cache_key(
            "source_search_v1",
            {
                "source": self._source_cache_identity(source),
                # TMDB 路线仍写空 anime，继续命中已发布版本的缓存键。
                "anime": normalized_anime,
                "tmdb_id": normalized_tmdb_id,
                "episode": str(episode or "").strip(),
            },
        )
        body, served_from_cache = self._cached_call(
            cache_key,
            self.search_cache_ttl_seconds,
            lambda: {
                "animes": source.search_episodes(
                    tmdb_id=normalized_tmdb_id,
                    episode=episode,
                    anime=normalized_anime,
                )
            },
        )
        return list(body.get("animes") or []), served_from_cache

    def match(self, tmdb_id=None, episode=None, anime=""):
        sources = self.enabled_sources()
        if not sources:
            raise RuntimeError("no danmaku source is configured and enabled")
        normalized_tmdb_id = str(tmdb_id or "").strip()
        normalized_anime = str(anime or "").strip()
        if not normalized_tmdb_id:
            return {
                "matched": False,
                "source": "",
                "match_mode": "",
                "episode_id": "",
                "anime_title": "",
                "episode_title": "",
                "shift": 0.0,
                "candidates": [],
                "attempts": [
                    {
                        "source": source.name,
                        "mode": "tmdb",
                        "outcome": "skipped",
                        "error": "tmdb_id is required",
                        "cached": False,
                    }
                    for source in sources
                ],
                "cached": False,
            }
        attempts = []
        for source in sources:
            animes, cached, error = self._attempt(
                lambda: self._search_source(source, tmdb_id=normalized_tmdb_id, episode=episode)
            )
            candidates = self._episode_candidates_from_animes(
                animes,
                episode=episode,
                allow_episode_title=True,
            )
            candidates = self._unique_episode_candidates(candidates)
            tmdb_ambiguous = False
            if len(candidates) > 1:
                exact_title_candidates = self._candidates_with_exact_anime_title(
                    candidates,
                    normalized_anime,
                )
                exact_title_candidates = self._unique_episode_candidates(exact_title_candidates)
                if len(exact_title_candidates) == 1:
                    candidates = exact_title_candidates
                else:
                    candidates = exact_title_candidates or candidates
                    tmdb_ambiguous = True
            self._record(attempts, source.name, "tmdb", candidates, error, cached=cached)
            if len(candidates) == 1:
                return self._match_result(source.name, "tmdb", candidates, attempts)
            if tmdb_ambiguous:
                attempts[-1]["outcome"] = "ambiguous"
                attempts[-1]["error"] = (
                    "TMDB lookup remained ambiguous after exact anime title and episode filtering"
                )
            if error:
                # 上游错误不是“未命中”，不能用另一种查询掩盖。
                continue
            if not normalized_anime:
                attempts.append(
                    {
                        "source": source.name,
                        "mode": "keyword",
                        "outcome": "skipped",
                        "error": "anime title is required for keyword fallback",
                        "cached": False,
                    }
                )
                if tmdb_ambiguous:
                    return self._ambiguous_match_result(
                        source.name,
                        "tmdb",
                        candidates,
                        attempts,
                        "ambiguous_candidates",
                    )
                continue
            if not normalize_danmaku_episode_number(episode).isdigit():
                attempts.append(
                    {
                        "source": source.name,
                        "mode": "keyword",
                        "outcome": "skipped",
                        "error": "numeric episode is required for keyword fallback",
                        "cached": False,
                    }
                )
                if tmdb_ambiguous:
                    return self._ambiguous_match_result(
                        source.name,
                        "tmdb",
                        candidates,
                        attempts,
                        "ambiguous_candidates",
                    )
                continue
            keyword_animes, keyword_cached, keyword_error = self._attempt(
                lambda: self._search_source(
                    source,
                    anime=normalized_anime,
                    episode=episode,
                )
            )
            keyword_candidates = self._episode_candidates_from_animes(
                keyword_animes,
                episode=episode,
                anime_title=normalized_anime,
                allow_episode_title=True,
                require_exact_anime_title=True,
            )
            keyword_candidates = self._unique_episode_candidates(keyword_candidates)
            self._record(
                attempts,
                source.name,
                "keyword",
                keyword_candidates,
                keyword_error,
                cached=keyword_cached,
            )
            if len(keyword_candidates) == 1:
                return self._match_result(source.name, "keyword", keyword_candidates, attempts)
            if len(keyword_candidates) > 1:
                attempts[-1]["outcome"] = "ambiguous"
                attempts[-1]["error"] = "keyword lookup returned multiple exact episode candidates"
                return self._ambiguous_match_result(
                    source.name,
                    "keyword",
                    keyword_candidates,
                    attempts,
                    "ambiguous_keyword_candidates",
                )
            if tmdb_ambiguous:
                return self._ambiguous_match_result(
                    source.name,
                    "tmdb",
                    candidates,
                    attempts,
                    "ambiguous_candidates",
                )
        return {
            "matched": False,
            "source": "",
            "match_mode": "",
            "episode_id": "",
            "anime_title": "",
            "episode_title": "",
            "shift": 0.0,
            "candidates": [],
            "attempts": attempts,
            "cached": bool(attempts) and all(item.get("cached") for item in attempts),
        }

    def _attempt(self, call):
        """执行一次缓存优先的匹配尝试，返回 ``(结果, 是否命中缓存, 错误文本)``。"""
        try:
            result, cached = call()
            return result, bool(cached), None
        except (RuntimeError, ValueError) as exc:
            return None, False, str(exc)

    def _record(self, attempts, source_name, mode, candidates, error, cached=False):
        """把每次尝试都记录进 attempts —— 未匹配时也必须能解释「试过什么、为什么没中」。"""
        entry = {"source": source_name, "mode": mode, "cached": bool(cached)}
        if error:
            entry["outcome"] = "error"
            entry["error"] = error
        elif candidates:
            entry["outcome"] = "matched"
            entry["candidate_count"] = len(candidates)
        else:
            entry["outcome"] = "no_candidates"
        attempts.append(entry)
        return entry

    def _candidates_with_exact_anime_title(self, candidates, anime_title):
        expected = normalize_danmaku_search_title(anime_title)
        if not expected:
            return []
        return [
            candidate
            for candidate in candidates or []
            if normalize_danmaku_search_title(candidate.get("anime_title")) == expected
        ]

    def _unique_episode_candidates(self, candidates):
        """同一 episodeId 的重复返回不是歧义；保留第一次出现的完整候选。"""
        unique = []
        seen_episode_ids = set()
        for candidate in candidates or []:
            episode_id = str(candidate.get("episode_id") or "").strip()
            if not episode_id or episode_id in seen_episode_ids:
                continue
            seen_episode_ids.add(episode_id)
            unique.append(candidate)
        return unique

    def _ambiguous_match_result(self, source_name, mode, candidates, attempts, reason):
        return {
            "matched": False,
            "source": source_name,
            "match_mode": mode,
            "episode_id": "",
            "anime_title": "",
            "episode_title": "",
            "shift": 0.0,
            "candidates": candidates,
            "ambiguous": True,
            "unmatched_reason": reason,
            "attempts": attempts,
            "cached": bool(attempts) and all(item.get("cached") for item in attempts),
        }

    def _episode_candidates_from_animes(
        self,
        animes,
        episode=None,
        anime_title="",
        allow_episode_title=False,
        require_exact_anime_title=False,
    ):
        candidates = []
        expected_episode = normalize_danmaku_episode_number(episode)
        expected_anime_title = normalize_danmaku_search_title(anime_title)
        for anime in animes or []:
            if require_exact_anime_title and (
                not expected_anime_title
                or normalize_danmaku_search_title(anime.get("anime_title")) != expected_anime_title
            ):
                continue
            for entry in anime.get("episodes") or []:
                if episode:
                    number = normalize_danmaku_episode_number(entry.get("episode_number"))
                    if not number and allow_episode_title:
                        number = episode_number_from_exact_title(entry.get("episode_title"))
                    if number != expected_episode:
                        continue
                candidates.append(
                    {
                        "episode_id": entry.get("episode_id") or "",
                        "anime_id": anime.get("anime_id") or "",
                        "anime_title": anime.get("anime_title") or "",
                        "episode_title": entry.get("episode_title") or "",
                        "episode_number": entry.get("episode_number") or "",
                        "type": anime.get("type") or "",
                        "type_description": anime.get("type_description") or "",
                        "image_url": anime.get("image_url") or "",
                        "shift": 0.0,
                    }
                )
        return [item for item in candidates if item["episode_id"]]

    def _match_result(self, source_name, mode, matches, attempts):
        if len(matches) != 1:
            raise ValueError("danmaku match requires exactly one candidate")
        primary = matches[0]
        return {
            "matched": True,
            "source": source_name,
            "match_mode": mode,
            "episode_id": primary.get("episode_id") or "",
            "anime_title": primary.get("anime_title") or "",
            "episode_title": primary.get("episode_title") or "",
            "shift": float(primary.get("shift") or 0.0),
            "candidates": matches,
            "ambiguous": False,
            "attempts": attempts,
            "cached": bool(attempts) and all(item.get("cached") for item in attempts),
        }

    def comments(
        self,
        episode_id,
        source_name="",
        with_related=True,
        ch_convert=0,
        offset_seconds=0.0,
        provider_shift_seconds=0.0,
        anime_title="",
        episode_title="",
        match_mode="",
        confidence=None,
    ):
        source = self._source_by_name(source_name) if source_name else None
        if source is None:
            enabled = self.enabled_sources()
            if not enabled:
                raise RuntimeError("no danmaku source is configured and enabled")
            source = enabled[0]
        episode = normalize_danmaku_episode_id(episode_id)
        if not episode:
            raise ValueError("episode_id is required for danmaku comments")
        cache_key = "comment_%s_%s_%s_%s" % (
            source.name,
            episode,
            "1" if with_related else "0",
            int(ch_convert or 0),
        )
        cached, served_from_cache = self._cached_call(
            cache_key,
            self.cache_ttl_seconds,
            lambda: source.comment(episode, with_related=with_related, ch_convert=ch_convert),
        )
        return build_danmaku_payload(
            cached.get("comments") or [],
            source=source.name,
            episode_id=episode,
            anime_title=anime_title,
            episode_title=episode_title,
            match_mode=match_mode,
            confidence=confidence,
            provider_shift_seconds=provider_shift_seconds,
            offset_seconds=offset_seconds,
            max_comments=self.max_comments,
            blacklist=self.blacklist,
            ch_convert=ch_convert,
            skipped=int(cached.get("skipped") or 0),
            cached=served_from_cache,
        )

def build_danmaku_matcher_from_config(config):
    """按配置构建 matcher；未启用或缺少凭证时返回 ``None``（由调用方如实上报）。"""
    if not getattr(config, "danmaku_enabled", False):
        return None
    timeout = getattr(config, "danmaku_search_timeout_seconds", DEFAULT_DANMAKU_SEARCH_TIMEOUT_SECONDS)
    comment_timeout = getattr(config, "danmaku_comment_timeout_seconds", DEFAULT_DANMAKU_COMMENT_TIMEOUT_SECONDS)
    transport = SubtitleHttpTransport(proxy_url=getattr(config, "danmaku_proxy_url", ""))
    sources = []
    names = tuple(getattr(config, "danmaku_providers", DEFAULT_DANMAKU_PROVIDERS) or ())
    for name in names:
        normalized = str(name or "").strip().lower()
        if normalized == "dandanplay":
            sources.append(
                DandanplayProtocolSource(
                    app_id=getattr(config, "dandanplay_app_id", ""),
                    app_secret=getattr(config, "dandanplay_app_secret", ""),
                    base_url=getattr(config, "dandanplay_base_url", DEFAULT_DANDANPLAY_BASE_URL),
                    timeout=timeout,
                    comment_timeout=comment_timeout,
                    transport=transport,
                )
            )
        elif normalized == "aggregator":
            base_url = str(getattr(config, "danmaku_aggregator_url", "") or "").strip()
            if not base_url:
                continue
            sources.append(
                AggregatorSource(
                    base_url=base_url,
                    timeout=timeout,
                    comment_timeout=comment_timeout,
                    transport=transport,
                )
            )
    if not sources:
        return None
    cache = DanmakuCache(getattr(config, "danmaku_cache_dir", DEFAULT_DANMAKU_CACHE_DIR))
    return DanmakuMatcher(
        sources,
        cache=cache,
        cache_ttl_seconds=getattr(config, "danmaku_cache_ttl_seconds", DEFAULT_DANMAKU_CACHE_TTL_SECONDS),
        search_cache_ttl_seconds=getattr(
            config,
            "danmaku_search_cache_ttl_seconds",
            DEFAULT_DANMAKU_SEARCH_CACHE_TTL_SECONDS,
        ),
        max_comments=getattr(config, "danmaku_max_comments", DEFAULT_DANMAKU_MAX_COMMENTS),
        blacklist=getattr(config, "danmaku_blacklist", ()),
    )


# ---------------------------------------------------------------------------
# 本地弹幕文件导入（B 站 XML / 弹弹play JSON）
#
# 这条路不访问任何外部数据源：用户在官方库里匹配不到的时候，可以把手里的弹幕文件
# 交给服务端解析。解析出来的仍是弹弹play JSON 结构，和上游弹幕走同一套归一化、
# 去重、模式过滤、黑名单、密度采样与偏移，客户端不需要区分来源。
# ---------------------------------------------------------------------------

DANMAKU_IMPORT_FORMATS = ("auto", "bilibili-xml", "dandanplay-json")
DANMAKU_IMPORT_FORMAT_ALIASES = {
    "auto": "auto",
    "bilibili-xml": "bilibili-xml",
    "bilibili": "bilibili-xml",
    "xml": "bilibili-xml",
    "dandanplay-json": "dandanplay-json",
    "dandanplay": "dandanplay-json",
    "json": "dandanplay-json",
}


class DanmakuImportError(ValueError):
    """本地弹幕文件不可用。``code`` 由 API 层原样回给调用方，避免笼统报错。"""

    def __init__(self, message, code="invalid_danmaku_file"):
        super().__init__(message)
        self.code = code


def decode_danmaku_file(content):
    """把上传内容统一成文本；非 UTF-8 如实报错，不做有损替换。

    BOM 一律去掉：Windows 上导出的 JSON/XML 经常带 UTF-8 BOM，而 ``json.loads``
    与 ``ElementTree`` 都不接受它。
    """
    if isinstance(content, (bytes, bytearray)):
        try:
            return bytes(content).decode("utf-8-sig").lstrip("\ufeff")
        except UnicodeDecodeError as exc:
            raise DanmakuImportError(
                "danmaku file must be UTF-8 text: %s" % exc,
                code="invalid_danmaku_file",
            )
    if content is None:
        raise DanmakuImportError("danmaku file is empty", code="empty_danmaku_file")
    return str(content).lstrip("\ufeff")


def detect_danmaku_import_format(content):
    """按内容判断格式（``auto`` 时使用）：JSON 以 ``{``/``[`` 开头，XML 以 ``<`` 开头。"""
    text = decode_danmaku_file(content).lstrip("\ufeff \t\r\n")
    if not text:
        raise DanmakuImportError("danmaku file is empty", code="empty_danmaku_file")
    head = text[0]
    if head in ("{", "["):
        return "dandanplay-json"
    if head == "<":
        return "bilibili-xml"
    raise DanmakuImportError(
        'unrecognised danmaku file: expected dandanplay JSON ({"comments": [...]}) or Bilibili XML (<i><d p="...">)',
        code="unsupported_danmaku_format",
    )


def parse_dandanplay_json(content):
    """解析弹弹play JSON 弹幕：``{"comments": [...]}``（官方 /comment 结构），也接受裸数组。"""
    try:
        payload = json.loads(decode_danmaku_file(content))
    except (TypeError, ValueError) as exc:
        raise DanmakuImportError("danmaku json is not valid JSON: %s" % exc, code="invalid_danmaku_file")
    entries = payload.get("comments") if isinstance(payload, dict) else payload
    if not isinstance(entries, list):
        raise DanmakuImportError(
            'danmaku json must be a comments array or {"comments": [...]}',
            code="invalid_danmaku_file",
        )
    return entries


def _xml_local_tag(tag):
    return str(tag or "").rsplit("}", 1)[-1]


def parse_bilibili_xml(content):
    """解析 B 站弹幕 XML。

    ``p`` 属性按 8 段原样保留（时间,模式,字号,颜色,时间戳,弹幕池,用户,行号），
    归一化交由 :func:`normalize_danmaku_comments`；行号作为 ``cid``，让导入的弹幕
    和上游弹幕一样有稳定标识。不可用的条目**不在这里丢弃**，由归一化如实计数。
    """
    try:
        root = ET.fromstring(decode_danmaku_file(content))
    except ET.ParseError as exc:
        raise DanmakuImportError("danmaku xml is not well formed: %s" % exc, code="invalid_danmaku_file")
    entries = []
    for node in root.iter():
        if _xml_local_tag(node.tag) != "d":
            continue
        raw_p = str(node.get("p") or "").strip()
        parts = [segment.strip() for segment in raw_p.split(",")]
        entries.append(
            {
                "cid": parts[7] if len(parts) >= 8 else "",
                "p": raw_p,
                "m": node.text or "",
            }
        )
    return entries


def parse_local_danmaku(content, source_format="auto"):
    """解析本地弹幕文件，返回 ``(format, entries)``。

    显式指定的格式必须受支持：写错了直接报 400，绝不悄悄按另一种格式重试。
    """
    text = decode_danmaku_file(content)
    requested = str(source_format or "auto").strip().lower() or "auto"
    resolved = DANMAKU_IMPORT_FORMAT_ALIASES.get(requested)
    if resolved is None:
        raise DanmakuImportError(
            "unsupported danmaku format %r: expected one of %s"
            % (source_format, ", ".join(DANMAKU_IMPORT_FORMATS)),
            code="unsupported_danmaku_format",
        )
    if resolved == "auto":
        resolved = detect_danmaku_import_format(text)
    if resolved == "bilibili-xml":
        return resolved, parse_bilibili_xml(text)
    return resolved, parse_dandanplay_json(text)


class DanmakuLocalImport:
    """本地弹幕文件的解析 + 归一化。

    - 不需要任何凭证或上游源，只受 ``DANMAKU_ENABLED`` 控制。
    - 简繁转换（``ch_convert``）依赖上游服务端的 ``chConvert``，本地文件无法在服务端
      完成转换，因此非 0 时如实报错，而不是假装转换过。
    """

    def __init__(
        self,
        max_comments=DEFAULT_DANMAKU_MAX_COMMENTS,
        blacklist=(),
        max_bytes=DEFAULT_DANMAKU_IMPORT_MAX_BYTES,
    ):
        self.max_comments = max_comments
        self.blacklist = tuple(blacklist or ())
        self.max_bytes = int(max_bytes or 0)

    def payload(self, content, source_format="auto", offset_seconds=0.0, ch_convert=0, title=""):
        if int(ch_convert or 0) != 0:
            raise DanmakuImportError(
                "ch_convert is not supported for imported danmaku files: "
                "dandanplay performs the conversion on its side",
                code="danmaku_ch_convert_unsupported",
            )
        text = decode_danmaku_file(content)
        size = len(text.encode("utf-8"))
        if self.max_bytes > 0 and size > self.max_bytes:
            raise DanmakuImportError(
                "danmaku file is %d bytes, the limit is %d bytes" % (size, self.max_bytes),
                code="danmaku_file_too_large",
            )
        resolved_format, entries = parse_local_danmaku(text, source_format)
        comments, skipped = normalize_danmaku_comments(entries)
        if not comments:
            raise DanmakuImportError(
                "danmaku file has no usable comment (%d entries were skipped)" % skipped,
                code="no_danmaku_comments",
            )
        payload = build_danmaku_payload(
            comments,
            source=DANMAKU_LOCAL_SOURCE_NAME,
            episode_id="",
            anime_title=str(title or ""),
            match_mode=DANMAKU_LOCAL_MATCH_MODE,
            offset_seconds=offset_seconds,
            max_comments=self.max_comments,
            blacklist=self.blacklist,
            skipped=skipped,
        )
        payload["format"] = resolved_format
        payload["size_bytes"] = size
        return payload


def build_danmaku_local_import_from_config(config):
    """按配置构建本地导入器；``DANMAKU_ENABLED`` 关闭时返回 ``None``。

    与 :func:`build_danmaku_matcher_from_config` 不同，这里**不要求任何源或凭证**。
    """
    if not getattr(config, "danmaku_enabled", False):
        return None
    return DanmakuLocalImport(
        max_comments=getattr(config, "danmaku_max_comments", DEFAULT_DANMAKU_MAX_COMMENTS),
        blacklist=getattr(config, "danmaku_blacklist", ()),
        max_bytes=getattr(config, "danmaku_import_max_bytes", DEFAULT_DANMAKU_IMPORT_MAX_BYTES),
    )
