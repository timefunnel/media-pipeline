import hashlib
import html
import json
import os
import re
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path

from pipeline.mediastation import extract_codes


DEFAULT_SUBTITLE_CACHE_DIR = "/subtitle-cache"
DEFAULT_SUBTITLE_SEARCH_TIMEOUT_SECONDS = 12
DEFAULT_SUBTITLE_DOWNLOAD_MAX_BYTES = 2 * 1024 * 1024
DEFAULT_SUBTITLE_PROVIDERS = ("subtitlecat", "assrt", "opensubtitles")
SUBTITLE_EXTENSIONS = {".ass", ".srt", ".ssa", ".vtt"}
LOCAL_SUBTITLE_SCHEME = "local-subtitle"
CHINESE_LANGUAGE_CODES = ("zh-cn", "zh-tw", "ze")
SUBTITLECAT_BASE_URL = "https://www.subtitlecat.com/"
SUBTITLECAT_LANGUAGE_ORDER = ("zh-CN", "zh-TW")


@dataclass
class SubtitleDownload:
    source: str
    provider_id: str
    filename: str
    body: bytes
    lang: str = "zh"
    label: str = "中文字幕"
    query: str = ""
    score: int = 0


class SubtitleHttpTransport:
    def json_request(self, method, url, headers=None, data=None, timeout=DEFAULT_SUBTITLE_SEARCH_TIMEOUT_SECONDS):
        body = None
        req_headers = dict(headers or {})
        if data is not None:
            body = json.dumps(data, ensure_ascii=False).encode("utf-8")
            req_headers["Content-Type"] = "application/json"
        request = urllib.request.Request(url, data=body, headers=req_headers, method=method)
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                raw = response.read().decode("utf-8", "replace")
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", "replace")[:300]
            raise RuntimeError("subtitle API failed: HTTP %s %s" % (exc.code, detail)) from exc
        except (OSError, TimeoutError) as exc:
            raise RuntimeError("subtitle API failed: %s" % exc) from exc
        if not raw:
            return {}
        try:
            return json.loads(raw)
        except ValueError as exc:
            raise RuntimeError("subtitle API returned invalid JSON") from exc

    def text_request(self, url, headers=None, timeout=DEFAULT_SUBTITLE_SEARCH_TIMEOUT_SECONDS, max_bytes=DEFAULT_SUBTITLE_DOWNLOAD_MAX_BYTES):
        request = urllib.request.Request(url, headers=dict(headers or {}), method="GET")
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                body = response.read(int(max_bytes) + 1)
                charset = response.headers.get_content_charset() or "utf-8"
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", "replace")[:300]
            raise RuntimeError("subtitle page failed: HTTP %s %s" % (exc.code, detail)) from exc
        except (OSError, TimeoutError) as exc:
            raise RuntimeError("subtitle page failed: %s" % exc) from exc
        if len(body) > int(max_bytes):
            raise RuntimeError("subtitle page too large")
        return body.decode(charset, "replace")

    def download(self, url, headers=None, timeout=DEFAULT_SUBTITLE_SEARCH_TIMEOUT_SECONDS, max_bytes=DEFAULT_SUBTITLE_DOWNLOAD_MAX_BYTES):
        request = urllib.request.Request(url, headers=dict(headers or {}), method="GET")
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                body = response.read(int(max_bytes) + 1)
        except urllib.error.HTTPError as exc:
            raise RuntimeError("subtitle download failed: HTTP %s" % exc.code) from exc
        except (OSError, TimeoutError) as exc:
            raise RuntimeError("subtitle download failed: %s" % exc) from exc
        if len(body) > int(max_bytes):
            raise RuntimeError("subtitle download too large")
        return body


class SubtitleCache:
    def __init__(self, root_dir=DEFAULT_SUBTITLE_CACHE_DIR):
        self.root_dir = Path(root_dir or DEFAULT_SUBTITLE_CACHE_DIR)

    def list_tracks(self, media_id):
        media_dir = self._media_dir(media_id)
        index_path = media_dir / "tracks.json"
        if not index_path.exists():
            return []
        try:
            payload = json.loads(index_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return []
        tracks = payload.get("tracks") if isinstance(payload, dict) else None
        out = []
        for track in tracks or []:
            if not isinstance(track, dict):
                continue
            filename = safe_subtitle_filename(track.get("filename"))
            if not filename:
                continue
            if not (media_dir / filename).exists():
                continue
            item = dict(track)
            item["path"] = local_subtitle_uri(media_id, filename)
            out.append(item)
        return out

    def save_download(self, media_id, download):
        if not media_id:
            raise RuntimeError("subtitle cache media_id missing")
        if not isinstance(download, SubtitleDownload):
            raise RuntimeError("subtitle cache download missing")
        filename = safe_subtitle_filename(download.filename)
        extension = subtitle_extension(filename)
        if extension not in SUBTITLE_EXTENSIONS:
            raise RuntimeError("subtitle extension unsupported: %s" % extension)
        media_dir = self._media_dir(media_id)
        media_dir.mkdir(parents=True, exist_ok=True)
        stored_name = self._stored_filename(download)
        target = media_dir / stored_name
        tmp = media_dir / (stored_name + ".tmp")
        tmp.write_bytes(download.body)
        os.replace(str(tmp), str(target))
        track = {
            "media_id": str(media_id),
            "filename": stored_name,
            "lang": download.lang or "zh",
            "label": download.label or "中文字幕",
            "path": local_subtitle_uri(media_id, stored_name),
            "source": download.source,
            "provider_id": str(download.provider_id or ""),
            "query": download.query,
            "score": int(download.score or 0),
        }
        tracks = self.list_tracks(media_id)
        key = subtitle_track_key(track)
        tracks = [item for item in tracks if subtitle_track_key(item) != key]
        tracks.append(track)
        self._write_index(media_dir, media_id, tracks)
        return track

    def read_local_uri(self, uri):
        parsed = urllib.parse.urlparse(str(uri or ""))
        if parsed.scheme != LOCAL_SUBTITLE_SCHEME:
            raise RuntimeError("local subtitle path invalid")
        media_id = urllib.parse.unquote(parsed.netloc)
        filename = safe_subtitle_filename(urllib.parse.unquote(parsed.path.lstrip("/")))
        if not media_id or not filename:
            raise RuntimeError("local subtitle path invalid")
        target = self._media_dir(media_id) / filename
        if not target.exists() or not target.is_file():
            raise RuntimeError("local subtitle file missing")
        return target.read_bytes(), filename

    def _stored_filename(self, download):
        extension = subtitle_extension(download.filename)
        digest = hashlib.sha256(
            ("%s:%s:%s" % (download.source, download.provider_id, download.filename)).encode("utf-8", "replace")
        ).hexdigest()[:12]
        source = re.sub(r"[^0-9A-Za-z_-]+", "-", str(download.source or "subtitle")).strip("-") or "subtitle"
        return "%s-%s%s" % (source, digest, extension)

    def _write_index(self, media_dir, media_id, tracks):
        payload = {"media_id": str(media_id), "tracks": tracks}
        tmp = media_dir / "tracks.json.tmp"
        tmp.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2), encoding="utf-8")
        os.replace(str(tmp), str(media_dir / "tracks.json"))

    def _media_dir(self, media_id):
        return self.root_dir / safe_media_id(media_id)


class LocalSubtitleProvider:
    def __init__(self, cache_dir=DEFAULT_SUBTITLE_CACHE_DIR):
        self.cache = SubtitleCache(cache_dir)

    def enabled(self):
        return bool(self.cache.root_dir)

    def tracks_for_media_id(self, media_id):
        return self.cache.list_tracks(media_id)

    def read_subtitle(self, local_uri):
        return self.cache.read_local_uri(local_uri)


class SubtitleCatProvider:
    name = "subtitlecat"

    def __init__(self, base_url=SUBTITLECAT_BASE_URL, transport=None, timeout=DEFAULT_SUBTITLE_SEARCH_TIMEOUT_SECONDS, max_bytes=DEFAULT_SUBTITLE_DOWNLOAD_MAX_BYTES):
        self.base_url = str(base_url or SUBTITLECAT_BASE_URL).rstrip("/") + "/"
        self.transport = transport or SubtitleHttpTransport()
        self.timeout = timeout
        self.max_bytes = max_bytes

    def enabled(self):
        return bool(self.base_url)

    def search(self, query, code=""):
        search_text = str(code or query or "").strip()
        if not search_text:
            return []
        url = urllib.parse.urljoin(self.base_url, "index.php?%s" % urllib.parse.urlencode({"search": search_text}))
        text = self.transport.text_request(url, headers=self._headers(), timeout=self.timeout, max_bytes=self.max_bytes)
        candidates = []
        for item in extract_subtitlecat_search_results(text, self.base_url):
            score = candidate_code_score(item, code or query)
            if code and score <= 0:
                continue
            candidate = dict(item)
            candidate["_score"] = score + subtitlecat_candidate_bonus(item)
            candidates.append(candidate)
        return sorted(candidates, key=lambda item: int(item.get("_score") or 0), reverse=True)

    def download(self, candidate, query, code=""):
        detail_url = str((candidate or {}).get("url") or "").strip()
        if not detail_url:
            return None
        text = self.transport.text_request(detail_url, headers=self._headers(), timeout=self.timeout, max_bytes=self.max_bytes)
        for item in extract_subtitlecat_download_links(text, self.base_url):
            filename = safe_subtitle_filename(item.get("filename"))
            if not filename:
                continue
            if code and candidate_code_score({"filename": filename}, code) <= 0 and candidate_code_score(candidate, code) <= 0:
                continue
            body = self.transport.download(item["url"], headers=self._headers(), timeout=self.timeout, max_bytes=self.max_bytes)
            lang, label = subtitle_lang_label(filename, item.get("language"))
            return SubtitleDownload(
                source=self.name,
                provider_id=item.get("provider_id") or item.get("url"),
                filename=filename,
                body=body,
                lang=lang,
                label=label,
                query=query,
                score=int((candidate or {}).get("_score") or 0),
            )
        return None

    def _headers(self):
        return {"User-Agent": "MediaPipeline/0.1", "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"}


class AssrtSubtitleProvider:
    name = "assrt"

    def __init__(self, token="", transport=None, timeout=DEFAULT_SUBTITLE_SEARCH_TIMEOUT_SECONDS, max_bytes=DEFAULT_SUBTITLE_DOWNLOAD_MAX_BYTES):
        self.token = str(token or "").strip()
        self.transport = transport or SubtitleHttpTransport()
        self.timeout = timeout
        self.max_bytes = max_bytes

    def enabled(self):
        return bool(self.token)

    def search(self, query, code=""):
        params = urllib.parse.urlencode({"q": query, "cnt": 10, "pos": 0})
        payload = self.transport.json_request("GET", "https://api.assrt.net/v1/sub/search?%s" % params, headers=self._headers(), timeout=self.timeout)
        candidates = []
        for item in extract_assrt_subs(payload):
            score = candidate_code_score(item, code or query)
            if code and score <= 0:
                continue
            candidate = dict(item)
            candidate["_score"] = score + chinese_score(item) + int(item.get("vote_score") or 0)
            candidates.append(candidate)
        return sorted(candidates, key=lambda item: int(item.get("_score") or 0), reverse=True)

    def download(self, candidate, query, code=""):
        sub_id = str((candidate or {}).get("id") or "").strip()
        if not sub_id:
            return None
        params = urllib.parse.urlencode({"id": sub_id})
        detail = self.transport.json_request("GET", "https://api.assrt.net/v1/sub/detail?%s" % params, headers=self._headers(), timeout=self.timeout)
        files = sorted(extract_assrt_detail_files(detail), key=lambda item: assrt_file_score(item, code or query), reverse=True)
        for item in files:
            filename = safe_subtitle_filename(item.get("f") or item.get("filename") or "")
            extension = subtitle_extension(filename)
            if extension not in SUBTITLE_EXTENSIONS:
                continue
            if code and candidate_code_score({"filename": filename}, code) <= 0 and candidate_code_score(candidate, code) <= 0:
                continue
            url = str(item.get("url") or "").strip()
            if not url:
                continue
            body = self.transport.download(url, timeout=self.timeout, max_bytes=self.max_bytes)
            lang, label = subtitle_lang_label(filename, item.get("lang") or (candidate.get("lang") or {}).get("desc"))
            return SubtitleDownload(
                source=self.name,
                provider_id=sub_id,
                filename=filename,
                body=body,
                lang=lang,
                label=label,
                query=query,
                score=int(candidate.get("_score") or 0),
            )
        return None

    def _headers(self):
        return {"Authorization": "Bearer " + self.token, "User-Agent": "MediaPipeline/0.1"}


class OpenSubtitlesProvider:
    name = "opensubtitles"

    def __init__(
        self,
        api_key="",
        username="",
        password="",
        transport=None,
        timeout=DEFAULT_SUBTITLE_SEARCH_TIMEOUT_SECONDS,
        max_bytes=DEFAULT_SUBTITLE_DOWNLOAD_MAX_BYTES,
    ):
        self.api_key = str(api_key or "").strip()
        self.username = str(username or "").strip()
        self.password = str(password or "")
        self.transport = transport or SubtitleHttpTransport()
        self.timeout = timeout
        self.max_bytes = max_bytes
        self._access_token = ""

    def enabled(self):
        return bool(self.api_key)

    def search(self, query, code=""):
        params = urllib.parse.urlencode({"query": query, "languages": ",".join(CHINESE_LANGUAGE_CODES)})
        payload = self.transport.json_request(
            "GET",
            "https://api.opensubtitles.com/api/v1/subtitles?%s" % params,
            headers=self._headers(auth=False),
            timeout=self.timeout,
        )
        candidates = []
        for item in payload.get("data") or []:
            candidate = open_subtitles_candidate(item)
            if not candidate:
                continue
            score = candidate_code_score(candidate, code or query)
            if code and score <= 0:
                continue
            candidate["_score"] = score + chinese_score(candidate)
            candidates.append(candidate)
        return sorted(candidates, key=lambda item: int(item.get("_score") or 0), reverse=True)

    def download(self, candidate, query, code=""):
        file_id = str((candidate or {}).get("file_id") or "").strip()
        if not file_id:
            return None
        payload = self.transport.json_request(
            "POST",
            "https://api.opensubtitles.com/api/v1/download",
            headers=self._headers(auth=True),
            data={"file_id": file_id},
            timeout=self.timeout,
        )
        link = str(payload.get("link") or "").strip()
        if not link:
            raise RuntimeError("OpenSubtitles download link missing")
        filename = safe_subtitle_filename(payload.get("file_name") or candidate.get("filename") or ("opensubtitles-%s.srt" % file_id))
        if subtitle_extension(filename) not in SUBTITLE_EXTENSIONS:
            filename += ".srt"
        body = self.transport.download(link, headers={"User-Agent": "MediaPipeline/0.1"}, timeout=self.timeout, max_bytes=self.max_bytes)
        lang, label = subtitle_lang_label(filename, candidate.get("language"))
        return SubtitleDownload(
            source=self.name,
            provider_id=file_id,
            filename=filename,
            body=body,
            lang=lang,
            label=label,
            query=query,
            score=int(candidate.get("_score") or 0),
        )

    def _headers(self, auth=False):
        headers = {"Api-Key": self.api_key, "User-Agent": "MediaPipeline/0.1", "Accept": "application/json"}
        if auth:
            token = self._login_token()
            if token:
                headers["Authorization"] = "Bearer " + token
        return headers

    def _login_token(self):
        if self._access_token:
            return self._access_token
        if not self.username or not self.password:
            return ""
        payload = self.transport.json_request(
            "POST",
            "https://api.opensubtitles.com/api/v1/login",
            headers=self._headers(auth=False),
            data={"username": self.username, "password": self.password},
            timeout=self.timeout,
        )
        token = str(payload.get("token") or "").strip()
        self._access_token = token
        return token


class SubtitleMatcher:
    def __init__(
        self,
        cache,
        providers=None,
        enabled=False,
        adult_only=True,
    ):
        self.cache = cache
        self.providers = list(providers or [])
        self.enabled = bool(enabled)
        self.adult_only = bool(adult_only)

    def match_task(self, category, title, task, force=False):
        if not self.enabled:
            return subtitle_result("skipped", reason="disabled")
        if self.adult_only and category != "adult":
            return subtitle_result("skipped", reason="adult_only")
        media_id = str((task or {}).get("msg_media_id") or "").strip()
        if not media_id:
            return subtitle_result("skipped", reason="media_id_missing")
        existing = self.cache.list_tracks(media_id)
        if existing and not force:
            return subtitle_result("success", count=len(existing), source="cache", reason="already_cached")
        queries, code = subtitle_task_queries(category, title, task)
        if not queries:
            return subtitle_result("skipped", reason="query_missing")
        enabled_providers = [provider for provider in self.providers if provider.enabled()]
        if not enabled_providers:
            return subtitle_result("skipped", reason="provider_missing")
        errors = []
        for provider in enabled_providers:
            for query in queries:
                try:
                    candidates = provider.search(query, code=code)
                except Exception as exc:
                    errors.append("%s: %s" % (provider.name, exc))
                    continue
                for candidate in candidates:
                    try:
                        download = provider.download(candidate, query, code=code)
                    except Exception as exc:
                        errors.append("%s: %s" % (provider.name, exc))
                        continue
                    if not download:
                        continue
                    track = self.cache.save_download(media_id, download)
                    return subtitle_result(
                        "success",
                        count=1,
                        source=provider.name,
                        query=query,
                        filename=track.get("filename"),
                    )
        if errors:
            return subtitle_result("failed", error="; ".join(errors[:3]))
        return subtitle_result("skipped", reason="not_found", query=queries[0])


def build_subtitle_matcher_from_config(config):
    cache = SubtitleCache(getattr(config, "subtitle_cache_dir", DEFAULT_SUBTITLE_CACHE_DIR))
    providers = []
    names = tuple(getattr(config, "subtitle_providers", DEFAULT_SUBTITLE_PROVIDERS) or ())
    for name in names:
        normalized = str(name or "").strip().lower()
        if normalized == "subtitlecat":
            providers.append(
                SubtitleCatProvider(
                    timeout=getattr(config, "subtitle_search_timeout_seconds", DEFAULT_SUBTITLE_SEARCH_TIMEOUT_SECONDS),
                    max_bytes=getattr(config, "subtitle_download_max_bytes", DEFAULT_SUBTITLE_DOWNLOAD_MAX_BYTES),
                )
            )
        elif normalized == "assrt":
            providers.append(
                AssrtSubtitleProvider(
                    getattr(config, "assrt_api_token", ""),
                    timeout=getattr(config, "subtitle_search_timeout_seconds", DEFAULT_SUBTITLE_SEARCH_TIMEOUT_SECONDS),
                    max_bytes=getattr(config, "subtitle_download_max_bytes", DEFAULT_SUBTITLE_DOWNLOAD_MAX_BYTES),
                )
            )
        elif normalized == "opensubtitles":
            providers.append(
                OpenSubtitlesProvider(
                    getattr(config, "opensubtitles_api_key", ""),
                    username=getattr(config, "opensubtitles_username", ""),
                    password=getattr(config, "opensubtitles_password", ""),
                    timeout=getattr(config, "subtitle_search_timeout_seconds", DEFAULT_SUBTITLE_SEARCH_TIMEOUT_SECONDS),
                    max_bytes=getattr(config, "subtitle_download_max_bytes", DEFAULT_SUBTITLE_DOWNLOAD_MAX_BYTES),
                )
            )
    return SubtitleMatcher(
        cache,
        providers,
        enabled=getattr(config, "subtitle_auto_match_enabled", False),
        adult_only=getattr(config, "subtitle_auto_match_adult_only", True),
    )


def subtitle_result(status, count=0, source="", query="", filename="", reason="", error=""):
    result = {
        "subtitle_match_status": status,
        "subtitle_match_count": int(count or 0),
        "subtitle_match_source": source,
        "subtitle_match_query": query,
        "subtitle_match_filename": filename,
        "subtitle_match_reason": reason,
        "subtitle_match_error": error,
    }
    return {key: value for key, value in result.items() if value not in ("", None)}


def subtitle_task_queries(category, title, task):
    values = [
        title,
        (task or {}).get("name"),
        (task or {}).get("openlist_adult_code"),
        (task or {}).get("msg_media_title"),
        (task or {}).get("msg_match_path"),
    ]
    codes = []
    for value in values:
        codes.extend(sorted(extract_codes(value)))
    if category == "adult":
        if not codes:
            return [], ""
        code = codes[0]
        return [code], code
    queries = unique_values([title, (task or {}).get("msg_media_title")])
    return queries[:2], codes[0] if codes else ""


def extract_assrt_subs(payload):
    sub = payload.get("sub") if isinstance(payload, dict) else None
    subs = sub.get("subs") if isinstance(sub, dict) else None
    return [item for item in subs or [] if isinstance(item, dict)]


def extract_assrt_detail_files(payload):
    out = []
    for item in extract_assrt_subs(payload):
        filelist = item.get("filelist")
        if isinstance(filelist, list):
            out.extend([file_item for file_item in filelist if isinstance(file_item, dict)])
        elif item.get("url"):
            out.append(item)
    return out


def open_subtitles_candidate(item):
    if not isinstance(item, dict):
        return None
    attributes = item.get("attributes")
    if not isinstance(attributes, dict):
        return None
    files = attributes.get("files")
    if not isinstance(files, list):
        return None
    first_file = next((file_item for file_item in files if isinstance(file_item, dict) and file_item.get("file_id")), None)
    if not first_file:
        return None
    feature_details = attributes.get("feature_details") if isinstance(attributes.get("feature_details"), dict) else {}
    filename = first_file.get("file_name") or attributes.get("release") or feature_details.get("title")
    return {
        "id": item.get("id"),
        "file_id": first_file.get("file_id"),
        "filename": filename,
        "title": attributes.get("release") or feature_details.get("title"),
        "language": attributes.get("language"),
    }


def extract_subtitlecat_search_results(text, base_url=SUBTITLECAT_BASE_URL):
    out = []
    for row in re.findall(r"<tr\b.*?</tr>", str(text or ""), flags=re.IGNORECASE | re.DOTALL):
        for tag_match in re.finditer(r"<a\b[^>]*href=[\"'](?P<href>subs/[^\"']+\.html)[\"'][^>]*>(?P<title>.*?)</a>", row, flags=re.IGNORECASE | re.DOTALL):
            href = html.unescape(tag_match.group("href"))
            title = strip_html_text(tag_match.group("title"))
            if not href or not title:
                continue
            url = urllib.parse.urljoin(base_url, href)
            out.append(
                {
                    "id": href,
                    "url": url,
                    "title": title,
                    "filename": title,
                    "release": title,
                    "row_text": strip_html_text(row),
                }
            )
    return out


def extract_subtitlecat_download_links(text, base_url=SUBTITLECAT_BASE_URL):
    out = []
    for tag in re.findall(r"<a\b[^>]*>", str(text or ""), flags=re.IGNORECASE | re.DOTALL):
        language_match = re.search(r"\bid=[\"']download_([^\"']+)[\"']", tag, flags=re.IGNORECASE)
        href_match = re.search(r"\bhref=[\"']([^\"']+\.srt)[\"']", tag, flags=re.IGNORECASE)
        if not language_match or not href_match:
            continue
        language = html.unescape(language_match.group(1))
        href = html.unescape(href_match.group(1))
        url = urllib.parse.urljoin(base_url, href)
        filename = subtitlecat_filename_from_url(url)
        if not filename:
            continue
        out.append(
            {
                "language": language,
                "url": url,
                "filename": filename,
                "provider_id": "%s:%s" % (href, language),
            }
        )
    return sorted(out, key=lambda item: subtitlecat_language_rank(item.get("language")))


def subtitlecat_filename_from_url(url):
    path = urllib.parse.urlparse(str(url or "")).path
    return urllib.parse.unquote(posix_basename(path))


def subtitlecat_language_rank(language):
    normalized = str(language or "").strip()
    try:
        return SUBTITLECAT_LANGUAGE_ORDER.index(normalized)
    except ValueError:
        return len(SUBTITLECAT_LANGUAGE_ORDER)


def subtitlecat_candidate_bonus(candidate):
    text = str((candidate or {}).get("row_text") or "").casefold()
    score = 0
    if "translated from chinese" in text:
        score += 80
    match = re.search(r"(\d+)\s+downloads?", text)
    if match:
        score += min(int(match.group(1)), 50)
    return score


def strip_html_text(value):
    text = re.sub(r"<[^>]+>", " ", str(value or ""))
    return re.sub(r"\s+", " ", html.unescape(text)).strip()


def posix_basename(path):
    return str(path or "").replace("\\", "/").rsplit("/", 1)[-1]


def candidate_code_score(candidate, code):
    if not code:
        return 0
    haystack = " ".join(str((candidate or {}).get(key) or "") for key in ("native_name", "videoname", "filename", "title", "release"))
    compact_code = compact_text(code)
    if not compact_code:
        return 0
    if compact_code in compact_text(haystack):
        return 1000
    extracted = {compact_text(value) for value in extract_codes(haystack)}
    return 800 if compact_code in extracted else 0


def assrt_file_score(item, code):
    score = candidate_code_score({"filename": item.get("f") or item.get("filename") or ""}, code)
    extension = subtitle_extension(item.get("f") or item.get("filename") or "")
    if extension in (".ass", ".ssa", ".srt"):
        score += 100
    return score


def chinese_score(candidate):
    text = " ".join(str((candidate or {}).get(key) or "") for key in ("lang", "language", "filename", "title", "native_name"))
    lowered = text.casefold()
    score = 0
    for token in ("zh", "chi", "chs", "cht", "sc", "tc", "中文", "简体", "繁体", "双语"):
        if token in lowered:
            score += 20
    return score


def subtitle_lang_label(filename="", language=""):
    text = ("%s %s" % (filename or "", language or "")).casefold()
    if any(token in text for token in ("zh-tw", "zht", "cht", ".tc.", "traditional", "繁体", "繁體")):
        return "zh-Hant", "繁体中文"
    if any(token in text for token in ("zh-cn", "zhs", "chs", ".sc.", "simplified", "简体", "簡體")):
        return "zh-Hans", "简体中文"
    return "zh", "中文字幕"


def subtitle_extension(value):
    suffix = Path(str(value or "")).suffix.lower()
    return suffix


def safe_subtitle_filename(value):
    name = os.path.basename(str(value or "").replace("\\", "/")).strip()
    name = re.sub(r"[\r\n\t]+", " ", name)
    name = re.sub(r"[^0-9A-Za-z._()\\[\\] -]+", "_", name)
    name = name.strip(" .")
    if not name:
        return ""
    if subtitle_extension(name) not in SUBTITLE_EXTENSIONS:
        return ""
    return name[:180]


def safe_media_id(value):
    text = str(value or "").strip()
    if re.match(r"^[0-9A-Za-z_.-]{1,128}$", text):
        return text
    return hashlib.sha256(text.encode("utf-8", "replace")).hexdigest()


def local_subtitle_uri(media_id, filename):
    return "%s://%s/%s" % (
        LOCAL_SUBTITLE_SCHEME,
        urllib.parse.quote(safe_media_id(media_id), safe=""),
        urllib.parse.quote(safe_subtitle_filename(filename), safe=""),
    )


def local_subtitle_uri_valid(value):
    return urllib.parse.urlparse(str(value or "")).scheme == LOCAL_SUBTITLE_SCHEME


def subtitle_track_key(track):
    return "%s:%s:%s" % (track.get("source"), track.get("provider_id"), track.get("filename"))


def compact_text(value):
    return re.sub(r"[^0-9A-Za-z]+", "", str(value or "")).upper()


def unique_values(values):
    out = []
    seen = set()
    for value in values or []:
        text = str(value or "").strip()
        if not text:
            continue
        key = text.casefold()
        if key in seen:
            continue
        seen.add(key)
        out.append(text)
    return out
