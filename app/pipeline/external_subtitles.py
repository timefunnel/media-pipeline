import hashlib
import html
import http.cookiejar
import io
import json
import os
import re
import subprocess
import tempfile
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from dataclasses import dataclass
from pathlib import Path

from pipeline.mediastation import extract_codes


DEFAULT_SUBTITLE_CACHE_DIR = "/subtitle-cache"
DEFAULT_SUBTITLE_SEARCH_TIMEOUT_SECONDS = 12
DEFAULT_SUBTITLE_DOWNLOAD_MAX_BYTES = 2 * 1024 * 1024
DEFAULT_SUBTITLE_PROVIDERS = ("subhd", "subtitlecat", "assrt", "opensubtitles")
SUBTITLE_EXTENSIONS = {".ass", ".srt", ".ssa", ".vtt"}
LOCAL_SUBTITLE_SCHEME = "local-subtitle"
CHINESE_LANGUAGE_CODES = ("zh-cn", "zh-tw", "ze")
SUBHD_BASE_URL = "https://subhd.tv/"
SUBTITLECAT_BASE_URL = "https://www.subtitlecat.com/"
SUBTITLECAT_LANGUAGE_ORDER = ("zh-CN", "zh-TW")
SOURCE_DECLARED_CHINESE_SUBTITLE_REASON = "source_declares_chinese_subtitles"
EXPLICIT_CHINESE_SUBTITLE_PATTERN = re.compile(
    r"中文(?:字幕)?|中字|简中|簡中|繁中|简体中文|簡體中文|繁體中文|官中|中英(?:双语|雙語)?"
    r"|(?<![a-z0-9])(?:chs|cht|chinese)(?![a-z0-9])",
    re.IGNORECASE,
)
CHINESE_SUBTITLE_LANGUAGE_PATTERN = re.compile(
    r"中文|中字|简体|簡體|繁体|繁體|中英(?:双语|雙語)?"
    r"|(?<![a-z0-9])(?:zh(?:[-_](?:cn|tw|hans|hant))?|zho|chi|chs|cht|zhs|zht|ze|sc|tc|chinese)(?![a-z0-9])",
    re.IGNORECASE,
)
ADULT_CODE_CHINESE_SUBTITLE_SUFFIX_PATTERN = re.compile(
    r"(?:^|[^a-z0-9])[a-z]{2,10}[-_\s]?\d{2,8}[\s._-]*ch(?=$|[^a-z0-9])",
    re.IGNORECASE,
)


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
    def __init__(self, use_cookies=False):
        self.opener = None
        if use_cookies:
            cookie_jar = http.cookiejar.CookieJar()
            self.opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cookie_jar))

    def _open(self, request, timeout):
        if self.opener is not None:
            return self.opener.open(request, timeout=timeout)
        return urllib.request.urlopen(request, timeout=timeout)

    def json_request(self, method, url, headers=None, data=None, timeout=DEFAULT_SUBTITLE_SEARCH_TIMEOUT_SECONDS):
        body = None
        req_headers = dict(headers or {})
        if data is not None:
            body = json.dumps(data, ensure_ascii=False).encode("utf-8")
            req_headers["Content-Type"] = "application/json"
        request = urllib.request.Request(url, data=body, headers=req_headers, method=method)
        try:
            with self._open(request, timeout) as response:
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
            with self._open(request, timeout) as response:
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
            with self._open(request, timeout) as response:
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


class SubHDProvider:
    name = "subhd"

    def __init__(self, base_url=SUBHD_BASE_URL, transport=None, timeout=DEFAULT_SUBTITLE_SEARCH_TIMEOUT_SECONDS, max_bytes=DEFAULT_SUBTITLE_DOWNLOAD_MAX_BYTES):
        self.base_url = str(base_url or SUBHD_BASE_URL).rstrip("/") + "/"
        self.transport = transport
        self.timeout = timeout
        self.max_bytes = max_bytes

    def enabled(self):
        return bool(self.base_url)

    def search(self, query, code=""):
        search_text = str(query or "").strip()
        if not search_text:
            return []
        transport = self._transport()
        url = urllib.parse.urljoin(self.base_url, "search/" + urllib.parse.quote(search_text, safe=""))
        text = transport.text_request(url, headers=self._headers(), timeout=self.timeout, max_bytes=self.max_bytes)
        return extract_subhd_search_results(text, self.base_url)

    def download(self, candidate, query, code=""):
        sid = str((candidate or {}).get("id") or (candidate or {}).get("provider_id") or "").strip()
        if not re.fullmatch(r"[0-9A-Za-z_-]{2,32}", sid):
            raise RuntimeError("SubHD subtitle id invalid")
        if not subtitle_candidate_is_chinese(candidate):
            raise RuntimeError("SubHD candidate is not Chinese")

        transport = self._transport()
        detail_url = urllib.parse.urljoin(self.base_url, "a/" + sid)
        transport.text_request(detail_url, headers=self._headers(), timeout=self.timeout, max_bytes=self.max_bytes)
        prepare = transport.json_request(
            "POST",
            urllib.parse.urljoin(self.base_url, "api/sub/prepare-download"),
            headers=self._headers(referer=detail_url),
            data={"sid": sid},
            timeout=self.timeout,
        )
        down_url = subhd_down_page_url(self.base_url, prepare, sid)
        transport.text_request(
            down_url,
            headers=self._headers(referer=detail_url),
            timeout=self.timeout,
            max_bytes=self.max_bytes,
        )
        payload = transport.json_request(
            "POST",
            urllib.parse.urljoin(self.base_url, "api/sub/down"),
            headers=self._headers(referer=down_url),
            data={"sid": sid},
            timeout=self.timeout,
        )
        download_url = subhd_download_url(payload)
        extension = subtitle_extension(urllib.parse.urlparse(download_url).path)
        body = transport.download(download_url, headers=self._headers(), timeout=self.timeout, max_bytes=self.max_bytes)
        if extension in (".zip", ".7z", ".rar"):
            filename, body = extract_chinese_subtitle_from_archive(
                body,
                extension,
                candidate,
                self.max_bytes,
                query=query,
                timeout=self.timeout,
            )
        elif extension in SUBTITLE_EXTENSIONS:
            filename = subhd_download_filename(candidate, sid, extension)
            if not subtitle_body_is_chinese(body, filename):
                raise RuntimeError("SubHD downloaded subtitle is not Chinese")
        else:
            raise RuntimeError("SubHD subtitle archive unsupported: %s" % (extension or "unknown"))

        lang, label = subtitle_lang_label(filename, candidate.get("language"))
        return SubtitleDownload(
            source=self.name,
            provider_id=sid,
            filename=filename,
            body=body,
            lang=lang,
            label=label,
            query=query,
            score=int((candidate or {}).get("_score") or 0),
        )

    def _transport(self):
        return self.transport or SubtitleHttpTransport(use_cookies=True)

    def _headers(self, referer=""):
        headers = {
            "User-Agent": "Mozilla/5.0 (compatible; MediaPipeline/0.2; +https://subhd.tv/)",
            "Accept": "text/html,application/json;q=0.9,*/*;q=0.8",
        }
        if referer:
            headers["Referer"] = referer
        return headers


class SubtitleCatProvider:
    name = "subtitlecat"

    def __init__(self, base_url=SUBTITLECAT_BASE_URL, transport=None, timeout=DEFAULT_SUBTITLE_SEARCH_TIMEOUT_SECONDS, max_bytes=DEFAULT_SUBTITLE_DOWNLOAD_MAX_BYTES):
        self.base_url = str(base_url or SUBTITLECAT_BASE_URL).rstrip("/") + "/"
        self.transport = transport or SubtitleHttpTransport()
        self.timeout = timeout
        self.max_bytes = max_bytes
        self._download_links_cache = {}

    def enabled(self):
        return bool(self.base_url)

    def search(self, query, code=""):
        candidates = self.search_candidates(query, code=code, limit=10)
        verified = []
        for candidate in candidates:
            download_item = self._preferred_download(candidate, chinese_only=True)
            if not download_item:
                continue
            candidate["_subtitlecat_download"] = download_item
            candidate["filename"] = download_item.get("filename") or candidate.get("filename")
            candidate["language"] = download_item.get("language") or "中文"
            verified.append(candidate)
        return verified

    def search_candidates(self, query, code="", limit=10):
        search_text = str(code or query or "").strip()
        if not search_text:
            return []
        limit = max(1, int(limit or 10))
        url = urllib.parse.urljoin(self.base_url, "index.php?%s" % urllib.parse.urlencode({"search": search_text}))
        text = self.transport.text_request(url, headers=self._headers(), timeout=self.timeout, max_bytes=self.max_bytes)
        candidates = []
        for item in extract_subtitlecat_search_results(text, self.base_url):
            score = candidate_code_score(item, code) if code else candidate_title_score(item, query)
            if score <= 0:
                continue
            candidate = dict(item)
            candidate["_score"] = score + subtitlecat_candidate_bonus(item)
            candidate["language"] = "语言待预览确认"
            candidates.append(candidate)
        return sorted(candidates, key=lambda item: int(item.get("_score") or 0), reverse=True)[:limit]

    def download(self, candidate, query, code=""):
        detail_url = str((candidate or {}).get("url") or "").strip()
        if not detail_url:
            return None
        item = (candidate or {}).get("_subtitlecat_download")
        if not isinstance(item, dict):
            item = self._preferred_download(candidate)
        if not item:
            return None
        filename = safe_subtitle_filename(item.get("filename"))
        if not filename:
            return None
        if code and candidate_code_score({"filename": filename}, code) <= 0 and candidate_code_score(candidate, code) <= 0:
            return None
        body = self.transport.download(item["url"], headers=self._headers(), timeout=self.timeout, max_bytes=self.max_bytes)
        lang, label = subtitlecat_download_lang_label(filename, item.get("language"))
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

    def _preferred_download(self, candidate, chinese_only=False):
        detail_url = str((candidate or {}).get("url") or "").strip()
        if not detail_url:
            return None
        if detail_url not in self._download_links_cache:
            text = self.transport.text_request(detail_url, headers=self._headers(), timeout=self.timeout, max_bytes=self.max_bytes)
            self._download_links_cache[detail_url] = extract_subtitlecat_download_links(text, self.base_url)
        links = self._download_links_cache[detail_url]
        if chinese_only:
            return next((link for link in links if subtitle_language_value_is_chinese(link.get("language"))), None)
        return next(iter(links), None)

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
            if not subtitle_candidate_is_chinese(item):
                continue
            score = candidate_code_score(item, code or query)
            if code and score <= 0:
                continue
            candidate = dict(item)
            candidate["_score"] = score + chinese_score(item) + int(item.get("vote_score") or 0)
            candidates.append(candidate)
        return sorted(candidates, key=lambda item: int(item.get("_score") or 0), reverse=True)

    def download(self, candidate, query, code=""):
        if not subtitle_candidate_is_chinese(candidate):
            return None
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
            if not subtitle_candidate_is_chinese(candidate):
                continue
            score = candidate_code_score(candidate, code or query)
            if code and score <= 0:
                continue
            candidate["_score"] = score + chinese_score(candidate)
            candidates.append(candidate)
        return sorted(candidates, key=lambda item: int(item.get("_score") or 0), reverse=True)

    def download(self, candidate, query, code=""):
        if not subtitle_candidate_is_chinese(candidate):
            return None
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
        if category == "adult" and not force and adult_source_declares_chinese_subtitles(title, task):
            return subtitle_result("skipped", reason=SOURCE_DECLARED_CHINESE_SUBTITLE_REASON)
        existing = self.cache.list_tracks(media_id)
        if existing and not force:
            return subtitle_result("success", count=len(existing), source="cache", reason="already_cached")
        queries, code = subtitle_task_queries(category, title, task)
        if not queries:
            return subtitle_result("skipped", reason="query_missing")
        enabled_providers = self._providers_for_category(category)
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

    def search_task_candidates(self, category, title, task, limit=10, manual=False):
        if not self.enabled and not manual:
            return []
        if self.adult_only and category != "adult" and not manual:
            return []
        media_id = str((task or {}).get("msg_media_id") or "").strip()
        if not media_id:
            raise RuntimeError("subtitle media_id missing")
        queries, code = subtitle_task_queries(category, title, task)
        if not queries:
            return []
        enabled_providers = self._providers_for_category(category)
        if not enabled_providers:
            raise RuntimeError("subtitle provider missing")
        limit = max(1, int(limit or 10))
        records = []
        seen = set()
        errors = []
        for provider in enabled_providers:
            for query in queries:
                try:
                    search_candidates = getattr(provider, "search_candidates", None)
                    if callable(search_candidates):
                        candidates = search_candidates(query, code=code, limit=limit - len(records))
                    else:
                        candidates = provider.search(query, code=code)
                except Exception as exc:
                    errors.append("%s: %s" % (provider.name, exc))
                    continue
                for candidate in candidates:
                    record = subtitle_candidate_record(provider, query, code, candidate)
                    key = subtitle_candidate_record_key(record)
                    if key in seen:
                        continue
                    seen.add(key)
                    records.append(record)
                    if len(records) >= limit:
                        return ranked_subtitle_candidate_records(records)
        if errors and not records:
            raise RuntimeError("; ".join(errors[:3]))
        return ranked_subtitle_candidate_records(records)

    def apply_candidate(self, media_id, candidate_record):
        media_id = str(media_id or "").strip()
        if not media_id:
            raise RuntimeError("subtitle media_id missing")
        provider_name = str((candidate_record or {}).get("provider") or "").strip()
        if not provider_name:
            raise RuntimeError("subtitle provider missing")
        provider = next((item for item in self.providers if item.name == provider_name and item.enabled()), None)
        if provider is None:
            raise RuntimeError("subtitle provider not enabled: %s" % provider_name)
        query = str((candidate_record or {}).get("query") or "").strip()
        code = str((candidate_record or {}).get("code") or "").strip()
        candidate = (candidate_record or {}).get("candidate")
        if not isinstance(candidate, dict):
            raise RuntimeError("subtitle candidate payload missing")
        download = provider.download(candidate, query, code=code)
        if not download:
            return subtitle_result("skipped", source=provider.name, query=query, reason="download_missing")
        track = self.cache.save_download(media_id, download)
        return subtitle_result(
            "success",
            count=1,
            source=provider.name,
            query=query,
            filename=track.get("filename"),
        )

    def preview_candidate(self, candidate_record, max_chars=2000):
        provider_name = str((candidate_record or {}).get("provider") or "").strip()
        if not provider_name:
            raise RuntimeError("subtitle provider missing")
        provider = next((item for item in self.providers if item.name == provider_name and item.enabled()), None)
        if provider is None:
            raise RuntimeError("subtitle provider not enabled: %s" % provider_name)
        query = str((candidate_record or {}).get("query") or "").strip()
        code = str((candidate_record or {}).get("code") or "").strip()
        candidate = (candidate_record or {}).get("candidate")
        if not isinstance(candidate, dict):
            raise RuntimeError("subtitle candidate payload missing")
        download = provider.download(candidate, query, code=code)
        if not download:
            raise RuntimeError("subtitle preview download missing")
        preview = dict(candidate_record or {})
        preview.update(subtitle_download_preview(download, max_chars=max_chars))
        return preview

    def _providers_for_category(self, category):
        providers = [provider for provider in self.providers if provider.enabled()]
        if category == "adult":
            return [provider for provider in providers if provider.name != "subhd"]
        subhd = [provider for provider in providers if provider.name == "subhd"]
        return subhd + [provider for provider in providers if provider.name != "subhd"]


def build_subtitle_matcher_from_config(config):
    cache = SubtitleCache(getattr(config, "subtitle_cache_dir", DEFAULT_SUBTITLE_CACHE_DIR))
    providers = []
    names = tuple(getattr(config, "subtitle_providers", DEFAULT_SUBTITLE_PROVIDERS) or ())
    for name in names:
        normalized = str(name or "").strip().lower()
        if normalized == "subhd":
            providers.append(
                SubHDProvider(
                    timeout=getattr(config, "subtitle_search_timeout_seconds", DEFAULT_SUBTITLE_SEARCH_TIMEOUT_SECONDS),
                    max_bytes=getattr(config, "subtitle_download_max_bytes", DEFAULT_SUBTITLE_DOWNLOAD_MAX_BYTES),
                )
            )
        elif normalized == "subtitlecat":
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


def subtitle_candidate_record(provider, query, code, candidate):
    candidate = dict(candidate or {})
    return {
        "provider": str(getattr(provider, "name", "") or ""),
        "query": str(query or ""),
        "code": str(code or ""),
        "provider_id": subtitle_candidate_provider_id(candidate),
        "title": subtitle_candidate_title(candidate),
        "filename": subtitle_candidate_filename(candidate),
        "language": subtitle_candidate_language(candidate),
        "source_score": int(candidate.get("_score") or 0),
        "candidate": candidate,
    }


def ranked_subtitle_candidate_records(records):
    out = [dict(record) for record in records or []]
    for index, record in enumerate(out, start=1):
        record["rank"] = index
    return out


def subtitle_candidate_record_key(record):
    provider = str((record or {}).get("provider") or "")
    provider_id = str((record or {}).get("provider_id") or "")
    if provider_id:
        return provider, provider_id
    title = str((record or {}).get("title") or "")
    filename = str((record or {}).get("filename") or "")
    query = str((record or {}).get("query") or "")
    return provider, query, title, filename


def subtitle_candidate_provider_id(candidate):
    for key in ("provider_id", "id", "file_id", "url", "download_url", "link"):
        value = str((candidate or {}).get(key) or "").strip()
        if value:
            return value
    return ""


def subtitle_candidate_title(candidate):
    for key in ("title", "name", "filename", "file_name", "release_name"):
        value = str((candidate or {}).get(key) or "").strip()
        if value:
            return value
    return ""


def subtitle_candidate_filename(candidate):
    for key in ("filename", "file_name", "name", "title"):
        value = safe_subtitle_filename((candidate or {}).get(key) or "")
        if value:
            return value
    return ""


def subtitle_candidate_language(candidate):
    language = (candidate or {}).get("language")
    if isinstance(language, dict):
        return str(language.get("language_name") or language.get("desc") or language.get("code") or "").strip()
    for key in ("lang", "language", "language_name"):
        value = str((candidate or {}).get(key) or "").strip()
        if value:
            return value
    return ""


def subtitle_download_preview(download, max_chars=2000):
    body = (download or SubtitleDownload("", "", "", b"")).body or b""
    text = decode_subtitle_body(body)
    sample = subtitle_text_sample(text, max_chars=max_chars)
    return {
        "filename": safe_subtitle_filename((download or SubtitleDownload("", "", "", b"")).filename) or "",
        "language": (download or SubtitleDownload("", "", "", b"")).lang or "",
        "preview_char_count": len(text),
        "preview_line_count": len(text.splitlines()),
        "content_sample": sample,
    }


def decode_subtitle_body(body):
    data = bytes(body or b"")
    for encoding in ("utf-8-sig", "utf-16", "gb18030", "big5"):
        try:
            return data.decode(encoding)
        except UnicodeDecodeError:
            continue
    return data.decode("utf-8", "replace")


def subtitle_text_sample(text, max_chars=2000):
    max_chars = max(200, int(max_chars or 2000))
    cleaned = []
    for line in str(text or "").splitlines():
        value = line.strip()
        if not value:
            continue
        if re.match(r"^\d+$", value):
            continue
        if "-->" in value:
            continue
        if value.startswith(("[Script Info]", "[V4+", "[Events]", "Format:", "Style:")):
            continue
        if value.startswith("Dialogue:"):
            parts = value.split(",", 9)
            value = parts[-1].strip() if len(parts) >= 10 else value
        value = re.sub(r"\{\\[^}]+\}", "", value)
        value = re.sub(r"<[^>]+>", "", value)
        if value:
            cleaned.append(value)
        if sum(len(item) + 1 for item in cleaned) >= max_chars:
            break
    sample = "\n".join(cleaned)
    return sample[:max_chars]


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
    return queries[:2], ""


def adult_source_declares_chinese_subtitles(title, task):
    values = [
        title,
        (task or {}).get("name"),
        (task or {}).get("source_name"),
        (task or {}).get("download_name"),
        (task or {}).get("file_name"),
        (task or {}).get("openlist_adult_format_old_path"),
        (task or {}).get("openlist_adult_video_old_path"),
    ]
    for value in values:
        text = urllib.parse.unquote(str(value or "")).strip()
        if not text:
            continue
        if EXPLICIT_CHINESE_SUBTITLE_PATTERN.search(text):
            return True
        if ADULT_CODE_CHINESE_SUBTITLE_SUFFIX_PATTERN.search(text):
            return True
    return False


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


def extract_subhd_search_results(text, base_url=SUBHD_BASE_URL):
    source = str(text or "")
    pattern = re.compile(
        r"<a\b[^>]*class=[\"'][^\"']*\balign-middle\b[^\"']*[\"'][^>]*"
        r"href=[\"'](?P<href>/a/(?P<sid>[0-9A-Za-z_-]+))[\"'][^>]*>(?P<title>.*?)</a>",
        flags=re.IGNORECASE | re.DOTALL,
    )
    matches = list(pattern.finditer(source))
    out = []
    for index, match in enumerate(matches):
        end = matches[index + 1].start() if index + 1 < len(matches) else min(len(source), match.end() + 8000)
        segment = source[match.start():end]
        release_match = re.search(
            r"<div\b[^>]*class=[\"'][^\"']*\bview-text\b[^\"']*[\"'][^>]*>.*?"
            r"<a\b[^>]*href=[\"']%s[\"'][^>]*>(?P<release>.*?)</a>" % re.escape(match.group("href")),
            segment,
            flags=re.IGNORECASE | re.DOTALL,
        )
        metadata_match = re.search(
            r"<div\b[^>]*class=[\"'][^\"']*\btext-truncate\b[^\"']*\bpy-2\b[^\"']*\bf11\b[^\"']*[\"'][^>]*>"
            r"(?P<metadata>.*?)</div>",
            segment,
            flags=re.IGNORECASE | re.DOTALL,
        )
        metadata = strip_html_text(metadata_match.group("metadata") if metadata_match else "")
        if not subtitle_language_value_is_chinese(metadata):
            continue
        sid = match.group("sid")
        title = strip_html_text(match.group("title"))
        release = strip_html_text(release_match.group("release") if release_match else "") or title
        format_match = re.search(r"(?<![A-Za-z])(?:ASS|SRT|SSA|VTT)(?![A-Za-z])", metadata, flags=re.IGNORECASE)
        extension = "." + format_match.group(0).lower() if format_match else ".srt"
        filename = safe_subtitle_filename(release + extension) or ("subhd-%s%s" % (sid, extension))
        out.append(
            {
                "id": sid,
                "provider_id": sid,
                "url": urllib.parse.urljoin(base_url, match.group("href")),
                "title": release,
                "media_title": title,
                "release": release,
                "filename": filename,
                "language": metadata,
                "_score": 200 + max(0, 100 - index) + chinese_score({"language": metadata}),
            }
        )
    return out


def subhd_down_page_url(base_url, payload, sid):
    if not isinstance(payload, dict) or payload.get("success") is not True:
        message = str((payload or {}).get("msg") or "prepare download failed") if isinstance(payload, dict) else "prepare download failed"
        raise RuntimeError("SubHD %s" % message)
    relative = str(payload.get("url") or "").strip()
    url = urllib.parse.urljoin(base_url, relative)
    base = urllib.parse.urlparse(base_url)
    parsed = urllib.parse.urlparse(url)
    if parsed.scheme != base.scheme or parsed.netloc != base.netloc or parsed.path != "/down/" + sid:
        raise RuntimeError("SubHD download page URL invalid")
    return url


def subhd_download_url(payload):
    if not isinstance(payload, dict) or payload.get("success") is not True or payload.get("pass") is not True:
        message = str((payload or {}).get("msg") or "download validation failed") if isinstance(payload, dict) else "download validation failed"
        raise RuntimeError("SubHD %s" % message)
    url = str(payload.get("url") or "").strip()
    parsed = urllib.parse.urlparse(url)
    try:
        hostname = str(parsed.hostname or "").casefold()
        port = parsed.port
    except ValueError as exc:
        raise RuntimeError("SubHD subtitle download URL invalid") from exc
    if (
        parsed.scheme != "https"
        or parsed.username
        or parsed.password
        or port not in (None, 443)
        or not (hostname == "subhd.me" or hostname.endswith(".subhd.me"))
    ):
        raise RuntimeError("SubHD subtitle download URL invalid")
    return url


def subhd_download_filename(candidate, sid, extension):
    release = str((candidate or {}).get("release") or (candidate or {}).get("title") or "").strip()
    return safe_subtitle_filename(release + extension) or ("subhd-%s%s" % (sid, extension))


def extract_chinese_subtitle_from_archive(body, extension, candidate, max_bytes, query="", timeout=DEFAULT_SUBTITLE_SEARCH_TIMEOUT_SECONDS):
    extension = str(extension or "").casefold()
    if extension == ".zip":
        return extract_chinese_subtitle_from_zip(body, candidate, max_bytes, query=query)
    if extension not in (".7z", ".rar"):
        raise RuntimeError("SubHD subtitle archive unsupported: %s" % (extension or "unknown"))
    return extract_chinese_subtitle_with_7zip(body, extension, candidate, max_bytes, query=query, timeout=timeout)


def extract_chinese_subtitle_from_zip(body, candidate, max_bytes, query=""):
    try:
        archive = zipfile.ZipFile(io.BytesIO(bytes(body or b"")))
    except zipfile.BadZipFile as exc:
        raise RuntimeError("SubHD subtitle ZIP is invalid") from exc
    with archive:
        entries = [
            {"path": item.filename, "size": int(item.file_size or 0), "directory": item.is_dir()}
            for item in archive.infolist()
        ]
        subtitle_entries = validate_subtitle_archive_entries(entries, max_bytes, query=query)
        for entry in rank_subtitle_archive_entries(subtitle_entries, candidate, query=query):
            data = archive.read(entry["path"])
            filename = safe_subtitle_filename(posix_basename(entry["path"]))
            if filename and subtitle_body_is_chinese(data, filename):
                return filename, data
    raise RuntimeError("SubHD archive contains no Chinese subtitle")


def extract_chinese_subtitle_with_7zip(body, extension, candidate, max_bytes, query="", timeout=DEFAULT_SUBTITLE_SEARCH_TIMEOUT_SECONDS):
    with tempfile.TemporaryDirectory(prefix="subhd-subtitle-") as tmp:
        root = Path(tmp)
        archive_path = root / ("subtitle" + extension)
        archive_path.write_bytes(bytes(body or b""))
        try:
            listed = subprocess.run(
                ["7zz", "l", "-slt", "--", str(archive_path)],
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=max(1, int(timeout or DEFAULT_SUBTITLE_SEARCH_TIMEOUT_SECONDS)),
                check=False,
            )
        except FileNotFoundError as exc:
            raise RuntimeError("SubHD archive extractor missing: 7zz") from exc
        except subprocess.TimeoutExpired as exc:
            raise RuntimeError("SubHD archive listing timed out") from exc
        if listed.returncode != 0:
            raise RuntimeError("SubHD archive listing failed: %s" % listed.stderr.strip()[:200])
        entries = parse_7zip_archive_entries(listed.stdout)
        subtitle_entries = validate_subtitle_archive_entries(entries, max_bytes, query=query)

        extract_root = root / "extracted"
        extract_root.mkdir()
        try:
            extracted = subprocess.run(
                ["7zz", "x", "-y", "-bd", "-bso0", "-bsp0", "-o%s" % extract_root, "--", str(archive_path)],
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=max(1, int(timeout or DEFAULT_SUBTITLE_SEARCH_TIMEOUT_SECONDS)),
                check=False,
            )
        except subprocess.TimeoutExpired as exc:
            raise RuntimeError("SubHD archive extraction timed out") from exc
        if extracted.returncode != 0:
            raise RuntimeError("SubHD archive extraction failed: %s" % extracted.stderr.strip()[:200])
        verify_extracted_archive_tree(extract_root, max_bytes)
        for entry in rank_subtitle_archive_entries(subtitle_entries, candidate, query=query):
            target = extract_root.joinpath(*archive_member_parts(entry["path"]))
            if not target.is_file():
                continue
            data = target.read_bytes()
            filename = safe_subtitle_filename(posix_basename(entry["path"]))
            if filename and subtitle_body_is_chinese(data, filename):
                return filename, data
    raise RuntimeError("SubHD archive contains no Chinese subtitle")


def parse_7zip_archive_entries(output):
    text = str(output or "").replace("\r\n", "\n")
    payload = text.split("\n----------\n", 1)[-1]
    entries = []
    for block in re.split(r"\n\s*\n", payload):
        values = {}
        for line in block.splitlines():
            if " = " not in line:
                continue
            key, value = line.split(" = ", 1)
            values[key.strip()] = value.strip()
        path = values.get("Path")
        if not path:
            continue
        try:
            size = int(values.get("Size") or 0)
        except ValueError as exc:
            raise RuntimeError("SubHD archive entry size invalid") from exc
        entries.append({"path": path, "size": size, "directory": values.get("Folder") == "+"})
    return entries


def validate_subtitle_archive_entries(entries, max_bytes, query=""):
    entries = list(entries or [])
    if not entries:
        raise RuntimeError("SubHD subtitle archive is empty")
    if len(entries) > 100:
        raise RuntimeError("SubHD subtitle archive has too many entries")
    max_bytes = max(1024, int(max_bytes or DEFAULT_SUBTITLE_DOWNLOAD_MAX_BYTES))
    max_expanded_bytes = max_bytes * 8
    total = 0
    subtitles = []
    episode = subtitle_episode_key(query)
    for entry in entries:
        archive_member_parts(entry.get("path"))
        size = int(entry.get("size") or 0)
        if size < 0:
            raise RuntimeError("SubHD archive entry size invalid")
        total += size
        if total > max_expanded_bytes:
            raise RuntimeError("SubHD subtitle archive expands beyond limit")
        if entry.get("directory"):
            continue
        filename = posix_basename(entry.get("path"))
        if subtitle_extension(filename) not in SUBTITLE_EXTENSIONS:
            continue
        if size > max_bytes:
            continue
        if episode and subtitle_episode_key(filename) not in ("", episode):
            continue
        subtitles.append(dict(entry))
    if not subtitles:
        raise RuntimeError("SubHD archive contains no supported subtitle file")
    if episode and any(subtitle_episode_key(posix_basename(item["path"])) for item in subtitles):
        subtitles = [item for item in subtitles if subtitle_episode_key(posix_basename(item["path"])) == episode]
    if not subtitles:
        raise RuntimeError("SubHD archive does not contain the requested episode")
    return subtitles


def archive_member_parts(path):
    normalized = str(path or "").replace("\\", "/").rstrip("/")
    parts = normalized.split("/")
    if not normalized or normalized.startswith("/") or any(part in ("", ".", "..") for part in parts):
        raise RuntimeError("SubHD archive path invalid")
    if re.match(r"^[A-Za-z]:", parts[0]):
        raise RuntimeError("SubHD archive path invalid")
    return parts


def verify_extracted_archive_tree(root, max_bytes):
    root = Path(root).resolve()
    max_bytes = max(1024, int(max_bytes or DEFAULT_SUBTITLE_DOWNLOAD_MAX_BYTES))
    max_expanded_bytes = max_bytes * 8
    files = 0
    total = 0
    for target in root.rglob("*"):
        resolved = target.resolve()
        if os.path.commonpath((str(root), str(resolved))) != str(root):
            raise RuntimeError("SubHD archive extracted outside temporary directory")
        if not resolved.is_file():
            continue
        files += 1
        total += resolved.stat().st_size
        if files > 100 or total > max_expanded_bytes:
            raise RuntimeError("SubHD subtitle archive expands beyond limit")


def rank_subtitle_archive_entries(entries, candidate, query=""):
    def score(entry):
        filename = posix_basename(entry.get("path"))
        value = chinese_score({"filename": filename})
        if subtitle_language_value_is_chinese(filename):
            value += 1000
        episode = subtitle_episode_key(query)
        if episode and subtitle_episode_key(filename) == episode:
            value += 2000
        if compact_text(query) and compact_text(query) in compact_text(filename):
            value += 200
        release = str((candidate or {}).get("release") or (candidate or {}).get("title") or "")
        if compact_text(release) and compact_text(release) in compact_text(filename):
            value += 100
        if subtitle_extension(filename) in (".ass", ".ssa", ".srt"):
            value += 50
        return value

    return sorted(entries or [], key=score, reverse=True)


def subtitle_episode_key(value):
    match = re.search(r"(?<![A-Za-z0-9])S(\d{1,2})[ ._-]*E(\d{1,3})(?![A-Za-z0-9])", str(value or ""), flags=re.IGNORECASE)
    if not match:
        return ""
    return "S%02dE%02d" % (int(match.group(1)), int(match.group(2)))


def subtitle_body_is_chinese(body, filename):
    text = decode_subtitle_body(body)
    if len(re.findall(r"[\u3400-\u9fff\uf900-\ufaff]", text)) < 4:
        return False
    extension = subtitle_extension(filename)
    if extension in (".ass", ".ssa"):
        return "[Script Info]" in text or "Dialogue:" in text
    if extension == ".vtt":
        return "WEBVTT" in text or "-->" in text
    return "-->" in text


def extract_subtitlecat_search_results(text, base_url=SUBTITLECAT_BASE_URL):
    out = []
    for row in re.findall(r"<tr\b.*?</tr>", str(text or ""), flags=re.IGNORECASE | re.DOTALL):
        for tag_match in re.finditer(r"<a\b[^>]*href=[\"'](?P<href>subs/[^\"']+\.html)[\"'][^>]*>(?P<title>.*?)</a>", row, flags=re.IGNORECASE | re.DOTALL):
            href = html.unescape(tag_match.group("href"))
            title = strip_html_text(tag_match.group("title"))
            if not href or not title:
                continue
            url = subtitlecat_urljoin(base_url, href)
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
        url = subtitlecat_urljoin(base_url, href)
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


def subtitlecat_urljoin(base_url, href):
    url = urllib.parse.urljoin(str(base_url or ""), str(href or ""))
    parsed = urllib.parse.urlsplit(url)
    path = urllib.parse.quote(parsed.path, safe="/%")
    query = urllib.parse.quote(parsed.query, safe="=&%:+/?")
    fragment = urllib.parse.quote(parsed.fragment, safe="=&%:+/?")
    return urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, path, query, fragment))


def subtitlecat_language_rank(language):
    normalized = str(language or "").strip()
    try:
        return SUBTITLECAT_LANGUAGE_ORDER.index(normalized)
    except ValueError:
        return len(SUBTITLECAT_LANGUAGE_ORDER)


def subtitlecat_download_lang_label(filename, language):
    if subtitle_language_value_is_chinese((filename, language)):
        return subtitle_lang_label(filename, language)
    normalized = str(language or "").strip()
    return normalized, normalized or "未知语言"


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


def candidate_title_score(candidate, query):
    query_tokens = subtitle_title_tokens(query)
    if not query_tokens:
        return 0
    haystack = " ".join(str((candidate or {}).get(key) or "") for key in ("filename", "title", "release"))
    candidate_tokens = subtitle_title_tokens(haystack)
    if not candidate_tokens:
        return 0
    compact_query = "".join(query_tokens)
    compact_candidate = "".join(candidate_tokens)
    if compact_query in compact_candidate:
        return 600
    if set(query_tokens).issubset(set(candidate_tokens)):
        return 500
    return 0


def subtitle_title_tokens(value):
    stop_words = {"a", "an", "and", "of", "the"}
    tokens = re.findall(r"[^\W_]+", str(value or "").casefold(), flags=re.UNICODE)
    return [token for token in tokens if token not in stop_words and not re.fullmatch(r"(?:19|20)\d{2}", token)]


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


def subtitle_candidate_is_chinese(candidate):
    if not isinstance(candidate, dict):
        return False
    for key in ("lang", "language", "language_name"):
        if subtitle_language_value_is_chinese(candidate.get(key)):
            return True
    for key in ("filename", "file_name", "title", "native_name", "release"):
        if subtitle_language_value_is_chinese(candidate.get(key)):
            return True
    return False


def subtitle_language_value_is_chinese(value):
    if isinstance(value, dict):
        return any(subtitle_language_value_is_chinese(item) for item in value.values())
    if isinstance(value, (list, tuple, set)):
        return any(subtitle_language_value_is_chinese(item) for item in value)
    return bool(CHINESE_SUBTITLE_LANGUAGE_PATTERN.search(str(value or "")))


def subtitle_lang_label(filename="", language=""):
    filename_text = str(filename or "").casefold()
    language_text = str(language or "").casefold()
    simplified_tokens = ("zh-cn", "zh-hans", "zhs", "chs", ".sc.", "simplified", "简体", "簡體", "-简", "-簡")
    traditional_tokens = ("zh-tw", "zh-hant", "zht", "cht", ".tc.", "traditional", "繁体", "繁體", "-繁")
    if any(token in filename_text for token in simplified_tokens):
        return "zh-Hans", "简体中文"
    if any(token in filename_text for token in traditional_tokens):
        return "zh-Hant", "繁体中文"
    simplified = any(token in language_text for token in simplified_tokens)
    traditional = any(token in language_text for token in traditional_tokens)
    if simplified and not traditional:
        return "zh-Hans", "简体中文"
    if traditional and not simplified:
        return "zh-Hant", "繁体中文"
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
