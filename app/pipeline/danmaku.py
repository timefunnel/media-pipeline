"""弹弹play 开放弹幕网络 / 兼容聚合服务的弹幕适配、归一化与缓存。

设计约束（依据 docs/调研-播放器内置弹幕模块-2026-09-11.md）：
- 服务端只做「归一化」，**不下发 ASS**：对客户端输出沿用弹弹play JSON 的字段
  （``cid`` / ``p`` / ``m``），并补充结构化字段供客户端直接渲染。
- 凭证只存在于服务端；签名算法 ``base64(sha256(AppId + Timestamp + Path + AppSecret))``，
  其中 Path 不含域名与查询参数。
- 弹幕接口没有分页能力（``from``/``to`` 官方文档存在但实测无效），因此**整集缓存**，
  下发前再做密度采样。
- 匹配优先 TMDB ID 反查，其次文件名匹配；失败如实上报，不静默换源。

字段事实（实测，勿照抄 B 站 8 段解析）：
- 弹弹play 的 ``p`` 是 **4 段**：``时间(秒),模式,颜色(十进制RGB),用户/来源``。
- B 站 XML / 部分聚合服务是 **8 段**：``时间,模式,字号,颜色,时间戳,弹幕池,用户,行号``。
- ``cid`` 是超出 JS 安全整数范围的 64 位整数，对外一律按字符串下发。
"""

import base64
import hashlib
import json
import re
import time
import urllib.parse
from pathlib import Path

from .external_subtitles import SubtitleHttpTransport


DEFAULT_DANDANPLAY_BASE_URL = "https://api.dandanplay.net"
DEFAULT_DANMAKU_CACHE_DIR = "/danmaku-cache"
DEFAULT_DANMAKU_PROVIDERS = ("dandanplay",)
DEFAULT_DANMAKU_CACHE_TTL_SECONDS = 6 * 3600
DEFAULT_DANMAKU_MATCH_TTL_SECONDS = 24 * 3600
DEFAULT_DANMAKU_SEARCH_TIMEOUT_SECONDS = 12
DEFAULT_DANMAKU_COMMENT_TIMEOUT_SECONDS = 30
DEFAULT_DANMAKU_MAX_COMMENTS = 6000
DEFAULT_DANMAKU_MAX_BYTES = 16 * 1024 * 1024

# 官方与社区实践：拿不到真实 hash 时用占位 hash 配合 matchMode="hashAndFileName"，
# 这样「只有文件名」也能走 /api/v2/match。
PLACEHOLDER_FILE_HASH = "a1b2c3d4e5f67890abcd1234ef567890"
DANMAKU_MATCH_MODE = "hashAndFileName"

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

    def match(self, file_name, file_hash="", file_size=None, video_duration=None):
        raise NotImplementedError

    def search_episodes(self, anime="", tmdb_id=None, episode=None):
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

    def _post_json(self, path, body, timeout=None):
        url = self.base_url + path
        payload = self.transport.json_request(
            "POST",
            url,
            headers=self._headers(path),
            data=body,
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

    def match(self, file_name, file_hash="", file_size=None, video_duration=None):
        body = {
            "fileName": str(file_name or "").strip(),
            "fileHash": str(file_hash or "").strip() or PLACEHOLDER_FILE_HASH,
            "matchMode": DANMAKU_MATCH_MODE,
        }
        if file_size:
            body["fileSize"] = int(file_size)
        if video_duration:
            body["videoDuration"] = int(video_duration)
        if not body["fileName"]:
            raise ValueError("fileName is required for danmaku matching")
        payload = self._post_json("/api/v2/match", body)
        matches = payload.get("matches")
        if not isinstance(matches, list):
            matches = []
        return {
            "is_matched": bool(payload.get("isMatched")),
            "matches": [normalize_danmaku_match(item) for item in matches if isinstance(item, dict)],
        }

    def search_episodes(self, anime="", tmdb_id=None, episode=None):
        params = {"anime": str(anime or "").strip(), "v2": "true"}
        if tmdb_id:
            params["tmdbId"] = str(tmdb_id)
        if episode:
            params["episode"] = str(episode)
        if not params["anime"] and not params.get("tmdbId"):
            raise ValueError("anime or tmdbId is required for danmaku search")
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


def normalize_danmaku_match(item):
    return {
        "episode_id": normalize_danmaku_episode_id(item.get("episodeId")),
        "anime_id": normalize_danmaku_cid(item.get("animeId")),
        "anime_title": str(item.get("animeTitle") or ""),
        "episode_title": str(item.get("episodeTitle") or ""),
        "type": str(item.get("type") or ""),
        "type_description": str(item.get("typeDescription") or ""),
        "shift": float(item.get("shift") or 0.0),
        "image_url": str(item.get("imageUrl") or ""),
    }


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


class DanmakuMatcher:
    """匹配 + 回源 + 缓存的编排层。

    匹配优先级：TMDB ID 反查 > 文件名匹配（hash 缺失时用占位 hash）。
    每个源按配置顺序尝试，**结果里必须如实标明用的是哪个源与哪种匹配方式**。
    """

    def __init__(
        self,
        sources,
        cache=None,
        cache_ttl_seconds=DEFAULT_DANMAKU_CACHE_TTL_SECONDS,
        match_ttl_seconds=DEFAULT_DANMAKU_MATCH_TTL_SECONDS,
        max_comments=DEFAULT_DANMAKU_MAX_COMMENTS,
        blacklist=None,
    ):
        self.sources = [source for source in (sources or []) if source is not None]
        self.cache = cache if cache is not None else DanmakuCache()
        self.cache_ttl_seconds = max(0, int(cache_ttl_seconds or 0))
        self.match_ttl_seconds = max(0, int(match_ttl_seconds or 0))
        self.max_comments = max(1, int(max_comments or DEFAULT_DANMAKU_MAX_COMMENTS))
        self.blacklist = tuple(blacklist or ())

    def enabled_sources(self):
        return [source for source in self.sources if source.enabled()]

    def _source_by_name(self, name):
        wanted = str(name or "").strip().lower()
        for source in self.enabled_sources():
            if source.name == wanted:
                return source
        return None

    def match(
        self,
        title="",
        season=None,
        episode=None,
        file_name="",
        tmdb_id=None,
        file_hash="",
        file_size=None,
        video_duration=None,
    ):
        sources = self.enabled_sources()
        if not sources:
            raise RuntimeError("no danmaku source is configured and enabled")
        attempts = []
        for source in sources:
            if tmdb_id:
                animes, error = self._attempt(
                    lambda: source.search_episodes(tmdb_id=tmdb_id, episode=episode)
                )
                candidates = self._episode_candidates_from_animes(animes, episode=episode)
                self._record(attempts, source.name, "tmdb", candidates, error)
                if candidates:
                    return self._match_result(source.name, "tmdb", candidates, attempts)
            if file_name or title:
                result, error = self._attempt(
                    lambda: source.match(
                        file_name=file_name or title,
                        file_hash=file_hash,
                        file_size=file_size,
                        video_duration=video_duration,
                    )
                )
                matches = list((result or {}).get("matches") or [])
                if (result or {}).get("is_matched") and matches:
                    self._record(attempts, source.name, "filename", matches, None)
                    return self._match_result(source.name, "filename", matches, attempts)
                self._record(attempts, source.name, "filename", [], error)
            keyword = str(title or "").strip()
            if keyword:
                animes, error = self._attempt(lambda: source.search_episodes(anime=keyword))
                candidates = self._episode_candidates_from_animes(animes, episode=episode)
                self._record(attempts, source.name, "title", candidates, error)
                if candidates:
                    return self._match_result(source.name, "title", candidates, attempts)
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
        }

    def _attempt(self, call):
        """执行一次匹配尝试，返回 ``(结果, 错误文本)``；错误只记录不抛出。"""
        try:
            return call(), None
        except (RuntimeError, ValueError) as exc:
            return None, str(exc)

    def _record(self, attempts, source_name, mode, candidates, error):
        """把每次尝试都记录进 attempts —— 未匹配时也必须能解释「试过什么、为什么没中」。"""
        entry = {"source": source_name, "mode": mode}
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

    def _episode_candidates_from_animes(self, animes, episode=None):
        candidates = []
        for anime in animes or []:
            for entry in anime.get("episodes") or []:
                if episode:
                    number = str(entry.get("episode_number") or "").strip()
                    if number and number != str(episode).strip():
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
            "ambiguous": len(matches) > 1,
            "attempts": attempts,
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
        cached = self.cache.load(cache_key, self.cache_ttl_seconds)
        served_from_cache = cached is not None
        if cached is None:
            cached = source.comment(episode, with_related=with_related, ch_convert=ch_convert)
            self.cache.save(cache_key, cached)
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

    def search(self, keyword, episode=None):
        sources = self.enabled_sources()
        if not sources:
            raise RuntimeError("no danmaku source is configured and enabled")
        results = []
        errors = []
        for source in sources:
            try:
                animes = source.search_episodes(anime=keyword, episode=episode)
            except (RuntimeError, ValueError) as exc:
                errors.append({"source": source.name, "error": str(exc)})
                continue
            results.append({"source": source.name, "animes": animes})
        return {"keyword": str(keyword or ""), "results": results, "errors": errors}


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
        match_ttl_seconds=getattr(config, "danmaku_match_ttl_seconds", DEFAULT_DANMAKU_MATCH_TTL_SECONDS),
        max_comments=getattr(config, "danmaku_max_comments", DEFAULT_DANMAKU_MAX_COMMENTS),
        blacklist=getattr(config, "danmaku_blacklist", ()),
    )
