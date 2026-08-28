import json
import re
import urllib.parse
import urllib.request

from pipeline.client115 import parse_115_share_url


DEFAULT_PANSOU_URL = "http://127.0.0.1:8888"
DEFAULT_PANSOU_TIMEOUT_SECONDS = 5
DEFAULT_PANSOU_CLOUD_TYPES = ("115",)
PANSOU_CONTENT_FIELD_LABELS = {
    "title": ("名称", "片名", "标题", "电影", "电视剧", "剧集", "动漫", "资源名称"),
    "description": ("剧情简介", "描述", "简介", "介绍", "剧情"),
    "country": ("国家", "地区"),
    "link": ("115 云盘链接", "115云盘链接", "115 网盘链接", "115网盘链接", "链接"),
    "version": ("版本",),
    "audio": ("音频",),
    "subtitles": ("字幕",),
    "filename": ("文件名",),
    "file_count": ("文件",),
    "resource_type": ("资源类型", "质量", "画质", "规格"),
    "tmdb": ("TMDB ID", "TMDBID", "TMDB"),
    "douban": ("豆瓣 ID", "豆瓣ID", "豆瓣"),
    "size": ("大小", "容量"),
    "tags": ("标签",),
    "cast": ("主演",),
    "director": ("导演",),
    "submitter": ("投稿", "投稿人"),
    "genre": ("类型",),
    "category": ("分类",),
    "rating": ("评分",),
    "release_date": ("发行时间", "年份"),
}
_PANSOU_LABEL_TO_FIELD = {
    label.casefold(): key for key, labels in PANSOU_CONTENT_FIELD_LABELS.items() for label in labels
}
_PANSOU_FIELD_PATTERN = re.compile(
    r"(?P<label>%s)\s*[:：]"
    % "|".join(re.escape(label) for label in sorted(_PANSOU_LABEL_TO_FIELD.keys(), key=len, reverse=True)),
    re.IGNORECASE,
)


class PanSouTransport:
    def request(self, method, url, headers=None, data=None, timeout=None):
        body = None
        request_headers = dict(headers or {})
        if data is not None:
            body = json.dumps(data, ensure_ascii=False).encode("utf-8")
            request_headers.setdefault("Content-Type", "application/json")
        request = urllib.request.Request(url, data=body, headers=request_headers, method=method)
        with urllib.request.urlopen(request, timeout=timeout) as response:
            raw = response.read().decode("utf-8", "replace")
        return json.loads(raw)


class PanSouClient:
    def __init__(self, base_url=DEFAULT_PANSOU_URL, token="", transport=None, timeout=DEFAULT_PANSOU_TIMEOUT_SECONDS):
        self.base_url = str(base_url or "").rstrip("/")
        if not self.base_url:
            raise RuntimeError("PANSOU_URL missing")
        self.token = str(token or "").strip()
        self.transport = transport or PanSouTransport()
        self.timeout = timeout

    def search(self, query, limit=20, cloud_types=None, source_type="all", plugins=None):
        query = str(query or "").strip()
        if not query:
            raise ValueError("query must not be empty")
        payload = {
            "kw": query,
            "res": "all",
            "src": source_type or "all",
            "cloud_types": list(cloud_types or DEFAULT_PANSOU_CLOUD_TYPES),
        }
        if plugins:
            payload["plugins"] = list(plugins)
        response = self.transport.request(
            "POST",
            self.base_url + "/api/search",
            headers=self._headers(),
            data=payload,
            timeout=self.timeout,
        )
        return pansou_115_candidates(response, limit=limit, query=query)

    def _headers(self):
        headers = {"Accept": "application/json"}
        if self.token:
            headers["Authorization"] = "Bearer " + self.token
        return headers


def pansou_115_candidates(response, limit=20, query=""):
    if not isinstance(response, dict):
        raise RuntimeError("PanSou response is not an object")
    if response.get("code", 0) != 0:
        raise RuntimeError("PanSou search failed: %s" % (response.get("message") or response.get("code")))

    data = response.get("data") if "data" in response else response
    candidates = []
    seen = set()

    for item in iter_pansou_115_links(data):
        url = item.get("url")
        password = item.get("password") or ""
        share_url = pansou_115_url_with_password(url, password)
        parsed = parse_115_share_url(share_url)
        if parsed is None:
            continue
        key = (parsed.share_code, parsed.receive_code, parsed.pdir_fid)
        if key in seen:
            continue
        seen.add(key)
        title = pansou_candidate_title(item, parsed.share_code)
        fields = pansou_candidate_fields(item)
        size_text = normalize_pansou_size_text(fields.get("size"))
        if size_text:
            fields["size"] = size_text
        size = parse_pansou_size_bytes(size_text)
        candidates.append(
            {
                "title": title,
                "download_uri": parsed.url,
                "indexer": item.get("source") or "PanSou",
                "seeders": None,
                "size": size,
                "rank": 0,
                "source_kind": "115_share",
                "shareCode": parsed.share_code,
                "sharePassword": parsed.receive_code,
                "pansou_channel": item.get("channel") or "",
                "pansou_fields": fields,
                "pansou_size_text": size_text,
                "pansou_note": item.get("note") or "",
                "pansou_datetime": item.get("datetime") or "",
                "pansou_summary": pansou_candidate_summary(item),
                "_pansou_order": len(candidates),
                "_pansou_score_text": pansou_score_text(item, title),
            }
        )
    candidates = rank_pansou_candidates(candidates, query)
    selected = candidates[: int(limit)]
    for index, candidate in enumerate(selected, start=1):
        candidate["rank"] = index
        candidate.pop("_pansou_order", None)
        candidate.pop("_pansou_score_text", None)
    return selected


def iter_pansou_115_links(data):
    if not isinstance(data, dict):
        return

    # results carries the original Telegram content; merged_by_type often only
    # keeps the share link/title, so prefer results when the same share appears
    # in both structures.
    for result in data.get("results") or []:
        if not isinstance(result, dict):
            continue
        source = "tg:%s" % result.get("channel") if result.get("channel") else "PanSou"
        for link in result.get("links") or []:
            if not isinstance(link, dict):
                continue
            link_type = str(link.get("type") or "").casefold()
            if link_type not in {"115", "pan115"}:
                continue
            item = dict(link)
            item.setdefault("note", link.get("work_title") or result.get("title") or result.get("content") or "")
            item.setdefault("source", source)
            item.setdefault("channel", result.get("channel") or "")
            item.setdefault("datetime", link.get("datetime") or result.get("datetime") or "")
            item.setdefault("result_title", result.get("title") or "")
            if not item.get("content"):
                item["content"] = result.get("content") or ""
            yield item

    merged = data.get("merged_by_type") or {}
    for item in merged.get("115") or merged.get("pan115") or []:
        if isinstance(item, dict):
            yield item


def pansou_115_url_with_password(url, password):
    url = str(url or "").strip()
    password = str(password or "").strip()
    if not url or not password:
        return url
    parsed = urllib.parse.urlsplit(url)
    query = urllib.parse.parse_qs(parsed.query)
    if query.get("password") or query.get("pwd"):
        return url
    pairs = urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)
    pairs.append(("password", password))
    return urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, parsed.path, urllib.parse.urlencode(pairs), parsed.fragment))


def pansou_candidate_title(item, fallback):
    fields = pansou_candidate_fields(item)
    content_title = fields.get("title") or extract_pansou_content_title((item or {}).get("content"))
    note = str((item or {}).get("note") or (item or {}).get("work_title") or "").strip()
    if content_title:
        note_norm = normalize_pansou_text(note)
        title_norm = normalize_pansou_text(content_title)
        if note and note_norm != title_norm and title_norm not in note_norm:
            return "%s %s" % (content_title, note)
        return content_title
    for key in ("note", "work_title", "title", "result_title", "content"):
        value = str((item or {}).get(key) or "").strip()
        if value:
            return value
    return "PanSou 115分享 %s" % fallback


def pansou_candidate_summary(item):
    fields = pansou_candidate_fields(item)
    content = str((item or {}).get("content") or "").strip()
    description = fields.get("description") or extract_pansou_content_description(content)
    if description:
        return description
    note = str((item or {}).get("note") or "").strip()
    title = extract_pansou_content_title(content)
    if note and normalize_pansou_text(note) != normalize_pansou_text(title):
        return clean_pansou_text(note)
    return ""


def pansou_candidate_fields(item):
    fields = extract_pansou_content_fields((item or {}).get("content"))
    return enrich_pansou_fields(fields, item)


def extract_pansou_content_fields(content):
    content = str(content or "").strip()
    if not content:
        return {}
    matches = list(_PANSOU_FIELD_PATTERN.finditer(content))
    fields = {}
    for index, match in enumerate(matches):
        label = match.group("label").casefold()
        key = _PANSOU_LABEL_TO_FIELD.get(label)
        if not key:
            continue
        start = match.end()
        end = matches[index + 1].start() if index + 1 < len(matches) else len(content)
        value = clean_pansou_field_value(content[start:end])
        if value:
            fields[key] = value
    return fields


def extract_pansou_content_title(content):
    return extract_pansou_content_fields(content).get("title", "")


def extract_pansou_content_description(content):
    return extract_pansou_content_fields(content).get("description", "")


def clean_pansou_title(value):
    value = str(value or "").strip()
    value = re.sub(r"^[🎬📺🎞️🍿🎭📂📦💾👥⭐️\s]+", "", value)
    return clean_pansou_text(value).strip(" -_｜|")


def clean_pansou_field_value(value):
    value = str(value or "").strip()
    value = re.sub(r"❤️.*$", "", value)
    value = re.sub(r"捐助.*$", "", value)
    value = re.sub(r"投稿人?\s*[:：].*$", "", value)
    value = re.sub(r"资源搜索机器人bot.*$", "", value)
    value = re.sub(r"📢.*$", "", value)
    value = re.sub(r"^[🎬📺🎞️📝🌏🔗🧩🔊💬📄🆔📁🏷❤️🍿🎭📂📦💾👥⭐️\s]+", "", value)
    value = re.sub(r"[🎬📺🎞️📝🌏🔗🧩🔊💬📄🆔📁🏷❤️🍿🎭📂📦💾👥⭐️\s]+$", "", value)
    return clean_pansou_text(value).strip(" -_｜|")


def enrich_pansou_fields(fields, item):
    fields = dict(fields or {})
    text = pansou_candidate_detail_text(item)
    tokens = extract_pansou_feature_tokens(text)
    if not fields.get("resource_type"):
        resource_type = infer_pansou_resource_type(text, tokens)
        if resource_type:
            fields["resource_type"] = resource_type
    if not fields.get("audio"):
        audio = infer_pansou_audio(text, tokens)
        if audio:
            fields["audio"] = audio
    if not fields.get("subtitles"):
        subtitles = infer_pansou_subtitles(text, tokens)
        if subtitles:
            fields["subtitles"] = subtitles
    if not fields.get("size"):
        size_text = normalize_pansou_size_text(text)
        if size_text:
            fields["size"] = size_text
    return fields


def pansou_candidate_detail_text(item):
    values = [
        (item or {}).get("content"),
        (item or {}).get("note"),
        (item or {}).get("work_title"),
        (item or {}).get("title"),
        (item or {}).get("result_title"),
    ]
    return " ".join(str(value or "") for value in values if str(value or "").strip())


def extract_pansou_feature_tokens(text):
    text = clean_pansou_text(text)
    if not text:
        return []
    tokens = []
    tokens.extend(match.group(1).strip() for match in re.finditer(r"[【\[]([^【】\[\]]{2,80})[】\]]", text))
    tokens.extend(match.group(1).strip() for match in re.finditer(r"[（(]([^（）()]{2,80})[）)]", text))
    if not tokens:
        tokens = re.split(r"[\s,，。/|]+", text)
    cleaned = []
    for token in tokens:
        token = clean_pansou_text(token).strip(" -_｜|")
        if token:
            cleaned.append(token)
    return cleaned


def first_matching_token(tokens, pattern):
    regex = re.compile(pattern)
    for token in tokens or []:
        if regex.search(token):
            return token
    return ""


def infer_pansou_resource_type(text, tokens):
    patterns = [
        r"(?i)\bBD\s*(?:4K|2160p|1080p|720p)\b",
        r"(?i)\b(?:4K|2160p|1080p|720p)\b(?:[ ._-]*(?:REMUX|UHD|BluRay|Bluray|WEB[- .]?DL|BDRip|HDR10\+?|DV|HQ|60fps|原盘))*",
        r"(?i)(?:REMUX|UHD|BluRay|Bluray|WEB[- .]?DL|BDRip|BDISO)(?:[ ._-]*(?:4K|2160p|1080p|720p|REMUX|BluRay|Bluray|UHD|HDR10\+?|DV|HQ|60fps|原盘))*",
        r"(?:UHD|蓝光)?原盘(?:[ &/+]?(?:4K|2160p|1080p|HDR10\+?|杜比视界|高码率))*",
        r"(?:4K|2160p|1080p|720p)?\s*(?:高码率|杜比视界)",
    ]
    candidates = []
    regex_value = first_regex_match(text, patterns)
    if regex_value:
        candidates.append(regex_value)
    token = first_matching_token(tokens, r"(?i)(remux|blu[- .]?ray|bluray|web[- .]?dl|bdrip|uhd|原盘|蓝光|2160p|1080p|720p|4k|60fps|高码率)")
    if token:
        candidates.append(token)
    if not candidates:
        return ""
    return max(candidates, key=lambda value: (len(value), value))


def infer_pansou_audio(text, tokens):
    token = first_matching_token(tokens, r"(音轨|音频|国语|国英|粤语|台配|多语|多音轨)")
    if token:
        return token
    patterns = [
        r"(?:国英|英粤|国粤|中英)?(?:双语|多语|多音轨)",
        r"(?:国语|粤语|台配|国配|英音|日语|韩语)(?:[+/、，, ]+(?:国语|粤语|台配|国配|英语|日语|韩语))*",
    ]
    return first_regex_match(text, patterns)


def infer_pansou_subtitles(text, tokens):
    token = first_matching_token(tokens, r"(字幕|中字|内封|外挂)")
    if token:
        return token
    patterns = [
        r"(?:中英|简繁英|简繁|繁简|双语|中文|英语|特效|官方|内封|外挂|硬字幕|软字幕){1,8}字幕",
        r"(?:内封|外挂)(?:简繁|中英|中字|中文字幕|双语)",
        r"(?:中文字幕|中字)",
    ]
    return first_regex_match(text, patterns)


def first_regex_match(text, patterns):
    text = str(text or "")
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return clean_pansou_text(match.group(0)).strip(" -_｜|.。")
    return ""


def normalize_pansou_size_text(value):
    value = str(value or "").strip()
    if not value:
        return ""
    for match in re.finditer(r"(\d+(?:\.\d+)?)\s*([kmgt]i?b|[kmgt]|p(?:i?b))(?=$|[^a-z])", value, re.IGNORECASE):
        unit_raw = match.group(2)
        if unit_raw.casefold() == "p":
            continue
        unit = unit_raw.upper()
        unit = {"K": "KB", "M": "MB", "G": "GB", "T": "TB", "PIB": "PiB"}.get(unit, unit)
        return "%s %s" % (match.group(1), unit)
    return ""


def clean_pansou_text(value):
    value = str(value or "").strip()
    value = re.sub(r"\s+", " ", value)
    return value.strip()


def parse_pansou_size_bytes(value):
    value = str(value or "").strip()
    if not value:
        return None
    match = re.search(r"(\d+(?:\.\d+)?)\s*([kmgtp]?i?b|[kmgtp])?", value, re.IGNORECASE)
    if not match:
        return None
    number = float(match.group(1))
    unit = (match.group(2) or "B").casefold()
    unit = {"k": "kb", "m": "mb", "g": "gb", "t": "tb", "p": "pb"}.get(unit, unit)
    powers = {"b": 0, "kb": 1, "kib": 1, "mb": 2, "mib": 2, "gb": 3, "gib": 3, "tb": 4, "tib": 4, "pb": 5, "pib": 5}
    if unit not in powers:
        return None
    return int(number * (1024 ** powers[unit]))


def rank_pansou_candidates(candidates, query):
    # PanSou's API filter ignores result.content, where some channels keep the real title.
    query_norm = normalize_pansou_text(query)
    if not query_norm:
        return sorted(candidates, key=lambda item: item.get("_pansou_order", 0))
    query_terms = pansou_query_terms(query)
    scored = [(pansou_candidate_score(item, query_norm, query_terms), item) for item in candidates]
    return [
        item
        for score, item in sorted(scored, key=lambda pair: (-pair[0], pair[1].get("_pansou_order", 0)))
        if score > 0
    ]


def pansou_query_terms(query):
    terms = [normalize_pansou_text(part) for part in re.split(r"\s+", str(query or "").strip())]
    return [term for term in terms if term]


def pansou_candidate_score(candidate, query_norm, query_terms=None):
    title_norm = normalize_pansou_text(candidate.get("title"))
    text_norm = normalize_pansou_text(candidate.get("_pansou_score_text"))
    terms = list(query_terms or [query_norm])
    full_title_match = bool(query_norm and query_norm in title_norm)
    full_text_match = bool(query_norm and query_norm in text_norm)
    all_title_terms_match = bool(terms and all(term in title_norm for term in terms))
    all_text_terms_match = bool(terms and all(term in text_norm for term in terms))
    if not (full_title_match or full_text_match or all_title_terms_match or all_text_terms_match):
        return 0

    score = 0
    if title_norm == query_norm:
        score += 500
    elif title_norm.startswith(query_norm):
        score += 350
    elif full_title_match:
        score += 300
    if full_text_match:
        score += 150
    if len(terms) > 1:
        if all_title_terms_match:
            score += 120
        elif all_text_terms_match:
            score += 60
    return score


def pansou_score_text(item, title):
    values = [
        title,
        (item or {}).get("note"),
        (item or {}).get("work_title"),
        (item or {}).get("title"),
        (item or {}).get("result_title"),
        (item or {}).get("content"),
        (item or {}).get("source"),
    ]
    return " ".join(str(value or "") for value in values)


def normalize_pansou_text(value):
    return re.sub(r"[\W_]+", "", str(value or "").casefold())
