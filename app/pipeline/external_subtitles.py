import hashlib
import html
import http.cookiejar
import io
import json
import locale
import os
import re
import shutil
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
DEFAULT_SUBHD_DOWNLOAD_MAX_BYTES = 32 * 1024 * 1024
DEFAULT_SUBTITLE_PROVIDERS = ("subhd", "subtitlecat", "assrt", "opensubtitles")
SUBTITLE_EXTENSIONS = {".ass", ".srt", ".ssa", ".sup", ".vtt"}
TEXT_SUBTITLE_EXTENSIONS = {".ass", ".srt", ".ssa", ".vtt"}
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

    def save_download(self, media_id, download, display_name=""):
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
        tracks = self.list_tracks(media_id)
        key = subtitle_track_key(
            {
                "source": download.source,
                "provider_id": str(download.provider_id or ""),
                "filename": download.filename,
            }
        )
        replaced_tracks = [item for item in tracks if subtitle_track_key(item) == key]
        retained_tracks = [item for item in tracks if subtitle_track_key(item) != key]
        stored_name = self._stored_filename(download, retained_tracks)
        target = media_dir / stored_name
        tmp = media_dir / (stored_name + ".tmp")
        tmp.write_bytes(download.body)
        os.replace(str(tmp), str(target))
        track = {
            "media_id": str(media_id),
            "filename": stored_name,
            "name": subtitle_display_name(display_name, stored_name),
            "lang": download.lang or "zh",
            "label": download.label or "中文字幕",
            "path": local_subtitle_uri(media_id, stored_name),
            "source": download.source,
            "provider_id": str(download.provider_id or ""),
            "query": download.query,
            "score": int(download.score or 0),
        }
        retained_tracks.append(track)
        self._write_index(media_dir, media_id, retained_tracks)
        retained_names = {safe_subtitle_filename(item.get("filename")) for item in retained_tracks}
        for item in replaced_tracks:
            old_name = safe_subtitle_filename(item.get("filename"))
            if old_name and old_name != stored_name and old_name not in retained_names:
                old_path = media_dir / old_name
                if old_path.exists():
                    old_path.unlink()
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

    def _stored_filename(self, download, tracks=None):
        extension = subtitle_extension(download.filename)
        source = subtitle_storage_token(download.source, "subtitle").casefold()
        language = subtitle_storage_language(download.lang, download.filename, download.label)
        stem = "%s-%s" % (source, language)
        reserved = {
            safe_subtitle_filename(item.get("filename"))
            for item in tracks or []
            if safe_subtitle_filename(item.get("filename"))
        }
        candidate = stem + extension
        suffix = 2
        while candidate in reserved:
            candidate = "%s-%d%s" % (stem, suffix, extension)
            suffix += 1
        return candidate

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

    def __init__(
        self,
        base_url=SUBHD_BASE_URL,
        transport=None,
        timeout=DEFAULT_SUBTITLE_SEARCH_TIMEOUT_SECONDS,
        max_bytes=DEFAULT_SUBTITLE_DOWNLOAD_MAX_BYTES,
        download_max_bytes=DEFAULT_SUBHD_DOWNLOAD_MAX_BYTES,
    ):
        self.base_url = str(base_url or SUBHD_BASE_URL).rstrip("/") + "/"
        self.transport = transport
        self.timeout = timeout
        self.max_bytes = max(1024, int(max_bytes or DEFAULT_SUBTITLE_DOWNLOAD_MAX_BYTES))
        self.download_max_bytes = max(
            self.max_bytes,
            int(download_max_bytes or DEFAULT_SUBHD_DOWNLOAD_MAX_BYTES),
        )

    def enabled(self):
        return bool(self.base_url)

    def search(self, query, code=""):
        search_text = str(query or "").strip()
        if not search_text:
            return []
        transport = self._transport()
        url = urllib.parse.urljoin(self.base_url, "search/" + urllib.parse.quote(search_text, safe=""))
        text = transport.text_request(url, headers=self._headers(), timeout=self.timeout, max_bytes=self.max_bytes)
        candidates = [
            item for item in extract_subhd_search_results(text, self.base_url)
            if not subtitle_candidate_is_sup(item)
        ]
        detail_pages = extract_subhd_search_detail_pages(text, self.base_url)
        selected = select_subhd_season_detail_page(detail_pages, search_text)
        if selected is None:
            return candidates
        detail_html = transport.text_request(
            str(selected.get("url") or ""),
            headers=self._headers(referer=url),
            timeout=self.timeout,
            max_bytes=self.max_bytes,
        )
        details = {
            str(item.get("id") or ""): item
            for item in extract_subhd_movie_detail_results(detail_html, self.base_url)
        }
        enriched = []
        for candidate in candidates:
            detail = details.get(str(candidate.get("id") or ""))
            if detail is None:
                enriched.append(candidate)
                continue
            source_score = int(candidate.get("_score") or 0)
            media_title = str(candidate.get("media_title") or "")
            enriched.append({**candidate, **detail, "_score": source_score, "media_title": media_title})
        return [item for item in enriched if not subtitle_candidate_is_sup(item)]

    def search_season(self, query, season):
        search_text = str(query or "").strip()
        if not search_text:
            raise RuntimeError("SubHD season query missing")
        try:
            season = int(season)
        except (TypeError, ValueError) as exc:
            raise RuntimeError("SubHD season must be an integer") from exc
        if season < 1 or season > 99:
            raise RuntimeError("SubHD season must be between 1 and 99")

        transport = self._transport()
        search_url = urllib.parse.urljoin(self.base_url, "search/" + urllib.parse.quote(search_text, safe=""))
        search_html = transport.text_request(
            search_url,
            headers=self._headers(),
            timeout=self.timeout,
            max_bytes=self.max_bytes,
        )
        detail_pages = extract_subhd_search_detail_pages(search_html, self.base_url)
        selected = select_subhd_season_detail_page(detail_pages, search_text)
        if selected is None:
            raise RuntimeError("SubHD season detail page match missing")
        detail_url = str(selected.get("url") or "").strip()
        detail_html = transport.text_request(
            detail_url,
            headers=self._headers(referer=search_url),
            timeout=self.timeout,
            max_bytes=self.max_bytes,
        )
        return {
            "detail_url": detail_url,
            "detail_title": str(selected.get("title") or ""),
            "candidates": [
                item
                for item in extract_subhd_detail_results(detail_html, season, self.base_url)
                if not subtitle_candidate_is_sup(item)
            ],
        }

    def download(self, candidate, query, code=""):
        return self._download(candidate, query, code=code, require_chinese_body=True)

    def download_for_review(self, candidate, query, code=""):
        return self._download(candidate, query, code=code, require_chinese_body=False)

    def _download(self, candidate, query, code="", require_chinese_body=True):
        sid = str((candidate or {}).get("id") or (candidate or {}).get("provider_id") or "").strip()
        if not re.fullmatch(r"[0-9A-Za-z_-]{2,32}", sid):
            raise RuntimeError("SubHD subtitle id invalid")
        if require_chinese_body and not subtitle_candidate_is_chinese(candidate):
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
        body = transport.download(
            download_url,
            headers=self._headers(),
            timeout=self.timeout,
            max_bytes=self.download_max_bytes,
        )
        if extension in (".zip", ".7z", ".rar"):
            filename, body = extract_chinese_subtitle_from_archive(
                body,
                extension,
                candidate,
                self.download_max_bytes,
                query=query,
                timeout=self.timeout,
                require_chinese_body=require_chinese_body,
            )
        elif extension in SUBTITLE_EXTENSIONS:
            filename = subhd_download_filename(candidate, sid, extension)
            if not subtitle_body_matches_candidate(body, filename, require_chinese_body):
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

    def search_task_candidates(self, category, title, task, limit=10, manual=False, provider_names=None):
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
        enabled_providers = self._providers_for_category(category, provider_names=provider_names)
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
                    if subtitle_candidate_is_sup(candidate):
                        continue
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

    def search_season_candidates(self, query, season, targets, limit=20):
        provider = next((item for item in self.providers if item.name == "subhd" and item.enabled()), None)
        if provider is None:
            raise RuntimeError("subtitle provider missing: subhd")
        search_season = getattr(provider, "search_season", None)
        if not callable(search_season):
            raise RuntimeError("SubHD provider does not support season detail search")
        result = search_season(query, season)
        if not isinstance(result, dict) or not isinstance(result.get("candidates"), list):
            raise RuntimeError("SubHD season detail search returned invalid response")

        limit = max(1, min(50, int(limit or 20)))
        by_episode = {}
        for candidate in result.get("candidates") or []:
            episode_key = str((candidate or {}).get("episode_key") or "").strip().upper()
            if episode_key:
                by_episode.setdefault(episode_key, []).append(candidate)
        records = []
        for target in targets or []:
            media_id = str((target or {}).get("media_id") or "").strip()
            episode_key = str((target or {}).get("episode_key") or "").strip().upper()
            if not media_id or not episode_key:
                raise RuntimeError("season subtitle target is invalid")
            candidates = sorted(
                by_episode.get(episode_key) or [],
                key=subhd_detail_candidate_sort_key,
            )[:limit]
            for episode_rank, candidate in enumerate(candidates, start=1):
                record = subtitle_candidate_record(provider, episode_key, "", candidate)
                record.update(
                    {
                        "media_id": media_id,
                        "season": int(season),
                        "episode_key": episode_key,
                        "rank": episode_rank,
                    }
                )
                records.append(record)
        return {
            "detail_url": str(result.get("detail_url") or ""),
            "detail_title": str(result.get("detail_title") or ""),
            "candidates": records,
        }

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
        supported, reason = subtitle_candidate_application_status(candidate)
        if not supported:
            raise RuntimeError(reason)
        download = download_subtitle_candidate_for_review(provider, candidate, query, code=code)
        if not download:
            return subtitle_result("skipped", source=provider.name, query=query, reason="download_missing")
        track = self.cache.save_download(
            media_id,
            download,
            display_name=subtitle_candidate_title(candidate),
        )
        return subtitle_result(
            "success",
            count=1,
            source=provider.name,
            query=query,
            filename=track.get("filename"),
        )

    def save_download(self, media_id, download):
        return self.cache.save_download(media_id, download)

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
        previewable, reason = subtitle_candidate_preview_status(candidate)
        if not previewable:
            raise RuntimeError(reason)
        download = download_subtitle_candidate_for_review(provider, candidate, query, code=code)
        if not download:
            raise RuntimeError("subtitle preview download missing")
        preview = dict(candidate_record or {})
        preview.update(subtitle_download_preview(download, max_chars=max_chars))
        return preview

    def _providers_for_category(self, category, provider_names=None):
        providers = [provider for provider in self.providers if provider.enabled()]
        if provider_names is not None:
            requested = {str(value or "").strip().lower() for value in provider_names}
            providers = [provider for provider in providers if provider.name in requested]
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
                    download_max_bytes=getattr(
                        config,
                        "subhd_download_max_bytes",
                        DEFAULT_SUBHD_DOWNLOAD_MAX_BYTES,
                    ),
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
        adult_only=getattr(config, "subtitle_auto_match_adult_only", False),
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
    record = {
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
    for key in (
        "url",
        "subtitle_group",
        "source_type",
        "language_tags",
        "formats",
        "like_count",
        "download_count",
        "uploader",
        "uploaded_at",
        "uploaded_date",
        "episode_key",
    ):
        if key in candidate:
            record[key] = candidate[key]
    return record


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


def subtitle_candidate_application_status(candidate):
    if subtitle_candidate_is_sup(candidate):
        return False, "SUP 图形字幕暂不支持应用"
    raw_formats = (candidate or {}).get("formats")
    if not isinstance(raw_formats, (list, tuple)):
        raw_formats = [raw_formats] if raw_formats else []
    formats = unique_values(
        [str(value).strip().upper() for value in raw_formats if str(value).strip()]
    )
    if not formats:
        return True, ""
    if any("." + value.lower() in SUBTITLE_EXTENSIONS for value in formats):
        return True, ""
    return False, "%s 不是当前服务端可保存的字幕格式" % "、".join(formats)


def subtitle_candidate_is_sup(candidate):
    raw_formats = (candidate or {}).get("formats")
    if not isinstance(raw_formats, (list, tuple)):
        raw_formats = [raw_formats] if raw_formats else []
    formats = {str(value).strip().casefold().lstrip(".") for value in raw_formats if str(value).strip()}
    text_formats = {"ass", "srt", "ssa", "vtt"}
    if "sup" in formats and not formats.intersection(text_formats):
        return True
    for key in ("filename", "file_name", "name", "title"):
        if subtitle_extension(str((candidate or {}).get(key) or "")) == ".sup":
            return True
    return False


def subtitle_candidate_preview_status(candidate):
    raw_formats = (candidate or {}).get("formats")
    if not isinstance(raw_formats, (list, tuple)):
        raw_formats = [raw_formats] if raw_formats else []
    formats = unique_values(
        [str(value).strip().upper() for value in raw_formats if str(value).strip()]
    )
    if not formats or any("." + value.lower() in TEXT_SUBTITLE_EXTENSIONS for value in formats):
        return True, ""
    return False, "%s 是图形字幕，当前没有可展示的文本预览" % "、".join(formats)


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


def download_subtitle_candidate_for_review(provider, candidate, query, code=""):
    review_download = getattr(provider, "download_for_review", None)
    if callable(review_download):
        return review_download(candidate, query, code=code)
    return provider.download(candidate, query, code=code)


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
        metadata_html = metadata_match.group("metadata") if metadata_match else ""
        metadata = strip_html_text(metadata_html)
        if not subtitle_language_value_is_chinese(metadata):
            continue
        sid = match.group("sid")
        title = strip_html_text(match.group("title"))
        release = strip_html_text(release_match.group("release") if release_match else "") or title
        format_match = re.search(
            r"(?<![A-Za-z])(?:ASS|SRT|SSA|VTT|SUP|PGS|VOBSUB|IDX|SUB)(?![A-Za-z])",
            metadata,
            flags=re.IGNORECASE,
        )
        extension = "." + format_match.group(0).lower() if format_match else ".srt"
        filename = safe_subtitle_filename(release + extension) or ("subhd-%s%s" % (sid, extension))
        source_type = ""
        language_tags = []
        formats = []
        for span in re.finditer(r"<span\b(?P<attrs>[^>]*)>(?P<body>.*?)</span>", metadata_html, flags=re.IGNORECASE | re.DOTALL):
            attrs = span.group("attrs")
            class_match = re.search(r"\bclass=[\"'](?P<class>[^\"']*)[\"']", attrs, flags=re.IGNORECASE)
            classes = set((class_match.group("class") if class_match else "").split())
            value = strip_html_text(span.group("body"))
            if not value:
                continue
            if "text-white" in classes and not source_type:
                source_type = value
            elif "fw-bold" in classes:
                language_tags.append(value)
            elif "text-secondary" in classes:
                formats.extend(re.findall(r"(?<![A-Za-z])(?:ASS|SRT|SSA|VTT|SUP|PGS|VOBSUB|IDX|SUB)(?![A-Za-z])", value, flags=re.IGNORECASE))
        formats = unique_values([value.upper() for value in formats])
        if not formats:
            formats = unique_values(
                re.findall(
                    r"(?<![A-Za-z])(?:ASS|SRT|SSA|VTT|SUP|PGS|VOBSUB|IDX|SUB)(?![A-Za-z])",
                    metadata,
                    flags=re.IGNORECASE,
                )
            )
        stats_match = re.search(
            r"<div\b[^>]*class=[\"'][^\"']*\bpt-2\b[^\"']*\btext-secondary\b[^\"']*\bf12\b[^\"']*[\"'][^>]*>(?P<body>.*?)</div>",
            segment,
            flags=re.IGNORECASE | re.DOTALL,
        )
        stats_spans = re.findall(
            r"<span\b[^>]*class=[\"'][^\"']*\balign-text-top\b[^\"']*[\"'][^>]*>(?P<body>.*?)</span>",
            stats_match.group("body") if stats_match else "",
            flags=re.IGNORECASE | re.DOTALL,
        )
        download_count = subhd_integer(strip_html_text(stats_spans[1])) if len(stats_spans) > 1 else 0
        group_match = re.search(r"<a\b[^>]*href=[\"']/zu/[^\"']+[\"'][^>]*>(?P<group>.*?)</a>", segment, flags=re.IGNORECASE | re.DOTALL)
        uploader_match = re.search(r"<a\b[^>]*href=[\"']/u/[^\"']+[\"'][^>]*>(?P<uploader>.*?)</a>", segment, flags=re.IGNORECASE | re.DOTALL)
        date_match = re.search(
            r"<time\b[^>]*datetime=[\"'](?P<datetime>[^\"']+)[\"'][^>]*>(?P<date>.*?)</time>",
            segment,
            flags=re.IGNORECASE | re.DOTALL,
        )
        out.append(
            {
                "id": sid,
                "provider_id": sid,
                "url": urllib.parse.urljoin(base_url, match.group("href")),
                "title": release,
                "media_title": title,
                "release": release,
                "filename": filename,
                "language": " ".join(language_tags) or metadata,
                "language_tags": language_tags,
                "formats": formats,
                "subtitle_group": strip_html_text(group_match.group("group") if group_match else ""),
                "source_type": source_type,
                "like_count": 0,
                "download_count": download_count,
                "uploader": strip_html_text(uploader_match.group("uploader") if uploader_match else ""),
                "uploaded_at": str(date_match.group("datetime") if date_match else ""),
                "uploaded_date": strip_html_text(date_match.group("date") if date_match else ""),
                "_score": 200 + max(0, 100 - index) + chinese_score({"language": metadata}),
            }
        )
    return out


def extract_subhd_search_detail_pages(text, base_url=SUBHD_BASE_URL):
    source = str(text or "")
    marker = re.compile(
        r"<div\b[^>]*class=[\"'][^\"']*\bbg-white\b[^\"']*\bshadow-sm\b[^\"']*\bmb-4\b[^\"']*[\"'][^>]*>",
        flags=re.IGNORECASE,
    )
    matches = list(marker.finditer(source))
    pages = []
    seen = set()
    for index, match in enumerate(matches):
        end = matches[index + 1].start() if index + 1 < len(matches) else len(source)
        block = source[match.start():end]
        detail_match = re.search(r"href\s*=\s*[\"']/d/(?P<id>\d+)[\"']", block, flags=re.IGNORECASE)
        if detail_match is None or detail_match.group("id") in seen:
            continue
        title_match = re.search(r"<img\b[^>]*\balt=[\"'](?P<title>.*?)[\"']", block, flags=re.IGNORECASE | re.DOTALL)
        if title_match is None:
            title_match = re.search(
                r"<a\b[^>]*class=[\"'][^\"']*\balign-middle\b[^\"']*[\"'][^>]*>(?P<title>.*?)</a>",
                block,
                flags=re.IGNORECASE | re.DOTALL,
            )
        title = strip_html_text(title_match.group("title") if title_match else "")
        if not title:
            continue
        seen.add(detail_match.group("id"))
        pages.append(
            {
                "id": detail_match.group("id"),
                "title": title,
                "url": urllib.parse.urljoin(base_url, "/d/" + detail_match.group("id")),
            }
        )
    return pages


def select_subhd_season_detail_page(pages, query):
    ranked = []
    for index, page in enumerate(pages or []):
        score = subhd_season_title_match_score(query, (page or {}).get("title"))
        if score > 0:
            ranked.append((score, -index, page))
    if not ranked:
        return None
    ranked.sort(key=lambda item: (item[0], item[1]), reverse=True)
    return ranked[0][2]


def subhd_season_title_match_score(query, title):
    expected = normalize_subhd_series_title(query)
    actual = normalize_subhd_series_title(title)
    if not expected or not actual:
        return 0
    if expected == actual:
        return 10000
    if expected in actual or actual in expected:
        return 5000 + min(len(expected), len(actual))
    expected_tokens = {token for token in expected.split() if len(token) > 1}
    actual_tokens = {token for token in actual.split() if len(token) > 1}
    overlap = expected_tokens & actual_tokens
    return len(overlap) * 100 if overlap else 0


def normalize_subhd_series_title(value):
    text = html.unescape(str(value or "")).casefold()
    text = re.sub(r"(?<![a-z0-9])s\d{1,2}(?![a-z0-9])", " ", text)
    text = re.sub(r"\bseason\s*\d{1,2}\b", " ", text)
    text = re.sub(r"第\s*[0-9一二三四五六七八九十百零两]+\s*季", " ", text)
    text = re.sub(r"\((?:19|20)\d{2}\)", " ", text)
    text = re.sub(r"[^\w\u3400-\u9fff]+", " ", text, flags=re.UNICODE)
    return re.sub(r"\s+", " ", text).strip()


def extract_subhd_detail_results(text, season, base_url=SUBHD_BASE_URL):
    source = str(text or "")
    header = re.search(r">\s*字幕信息\s*<", source)
    if header is None:
        return []
    end = source.find("同系列作品", header.end())
    section = source[header.start():end if end >= 0 else len(source)]
    token_pattern = re.compile(
        r"(?P<header><div\b[^>]*class=[\"'][^\"']*\btext-danger\b[^\"']*\bfw-bold\b[^\"']*[\"'][^>]*>.*?</div>)"
        r"|(?P<row><div\b[^>]*class=[\"'][^\"']*\brow\b[^\"']*\bpt-2\b[^\"']*\bmb-2\b[^\"']*[\"'][^>]*>.*?<hr\b[^>]*class=[\"'][^\"']*\bmy-0\b[^\"']*[\"'][^>]*>)",
        flags=re.IGNORECASE | re.DOTALL,
    )
    current_episode_key = ""
    out = []
    for token in token_pattern.finditer(section):
        if token.group("header") is not None:
            label = strip_html_text(token.group("header"))
            episode_match = re.fullmatch(r"第\s*(\d{1,3})\s*集", label)
            current_episode_key = (
                "S%02dE%02d" % (int(season), int(episode_match.group(1)))
                if episode_match is not None
                else ""
            )
            continue
        if not current_episode_key:
            continue
        candidate = extract_subhd_detail_candidate(token.group("row"), current_episode_key, base_url)
        if candidate is not None:
            out.append(candidate)
    return out


def extract_subhd_movie_detail_results(text, base_url=SUBHD_BASE_URL):
    source = str(text or "")
    header = re.search(r">\s*字幕信息\s*<", source)
    if header is None:
        return []
    end = source.find("同系列作品", header.end())
    section = source[header.start():end if end >= 0 else len(source)]
    row_pattern = re.compile(
        r"<div\b[^>]*class=[\"'][^\"']*\brow\b[^\"']*\bpt-2\b[^\"']*\bmb-2\b[^\"']*[\"'][^>]*>.*?<hr\b[^>]*class=[\"'][^\"']*\bmy-0\b[^\"']*[\"'][^>]*>",
        flags=re.IGNORECASE | re.DOTALL,
    )
    return [
        candidate
        for candidate in (
            extract_subhd_detail_candidate(match.group(0), "", base_url)
            for match in row_pattern.finditer(section)
        )
        if candidate is not None
    ]


def extract_subhd_detail_candidate(row, episode_key, base_url=SUBHD_BASE_URL):
    title_match = re.search(
        r"<a\b[^>]*class=[\"'][^\"']*\blink-dark\b[^\"']*[\"'][^>]*href=[\"'](?P<href>/a/(?P<sid>[0-9A-Za-z_-]+))[\"'][^>]*>(?P<title>.*?)</a>",
        row,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if title_match is None:
        return None
    title = strip_html_text(title_match.group("title"))
    if not title:
        return None

    view_match = re.search(
        r"<div\b[^>]*class=[\"'][^\"']*\bview-text\b[^\"']*[\"'][^>]*>(?P<body>.*?)</div>",
        row,
        flags=re.IGNORECASE | re.DOTALL,
    )
    view = view_match.group("body") if view_match else ""
    group_match = re.search(r"<a\b[^>]*href=[\"']/zu/[^\"']+[\"'][^>]*>(?P<group>.*?)</a>", view, flags=re.IGNORECASE | re.DOTALL)
    subtitle_group = strip_html_text(group_match.group("group") if group_match else "")

    metadata_match = re.search(
        r"<div\b[^>]*class=[\"'][^\"']*\bpt-1\b[^\"']*\bf11\b[^\"']*[\"'][^>]*>(?P<body>.*?)</div>",
        row,
        flags=re.IGNORECASE | re.DOTALL,
    )
    metadata = metadata_match.group("body") if metadata_match else ""
    source_type = ""
    language_tags = []
    formats = []
    like_count = 0
    for span in re.finditer(r"<span\b(?P<attrs>[^>]*)>(?P<body>.*?)</span>", metadata, flags=re.IGNORECASE | re.DOTALL):
        attrs = span.group("attrs")
        class_match = re.search(r"\bclass=[\"'](?P<class>[^\"']*)[\"']", attrs, flags=re.IGNORECASE)
        classes = set((class_match.group("class") if class_match else "").split())
        value = strip_html_text(span.group("body"))
        if not value:
            continue
        if "text-white" in classes and not source_type:
            source_type = value
        elif "fw-bold" in classes:
            language_tags.append(value)
        elif "text-secondary" in classes:
            formats.extend(re.findall(r"(?<![A-Za-z])(?:ASS|SRT|SSA|VTT|SUP|PGS|VOBSUB|IDX|SUB)(?![A-Za-z])", value, flags=re.IGNORECASE))
        elif "text-danger" in classes:
            like_count = subhd_integer(value)
    formats = unique_values([value.upper() for value in formats])
    if not formats:
        formats = unique_values(
            re.findall(
                r"(?<![A-Za-z])(?:ASS|SRT|SSA|VTT|SUP|PGS|VOBSUB|IDX|SUB)(?![A-Za-z])",
                metadata,
                flags=re.IGNORECASE,
            )
        )
    if not subtitle_language_value_is_chinese(" ".join(language_tags)):
        return None

    download_match = re.search(
        r"<div\b[^>]*class=[\"'][^\"']*\bpx-3\b[^\"']*\bpy-2\b[^\"']*\btext-end\b[^\"']*\btext-secondary\b[^\"']*[\"'][^>]*>\s*(?P<count>[0-9,]+)\s*</div>",
        row,
        flags=re.IGNORECASE,
    )
    uploader_match = re.search(r"<a\b[^>]*href=[\"']/u/[^\"']+[\"'][^>]*>(?P<uploader>.*?)</a>", row, flags=re.IGNORECASE | re.DOTALL)
    date_match = re.search(
        r"<time\b[^>]*datetime=[\"'](?P<datetime>[^\"']+)[\"'][^>]*>(?P<date>.*?)</time>",
        row,
        flags=re.IGNORECASE | re.DOTALL,
    )
    sid = title_match.group("sid")
    preferred_format = next(
        (value for value in formats if "." + value.lower() in SUBTITLE_EXTENSIONS),
        formats[0] if formats else "SRT",
    )
    extension = "." + preferred_format.lower()
    filename = safe_subtitle_filename(title + extension) or ("subhd-%s%s" % (sid, extension))
    download_count = subhd_integer(download_match.group("count") if download_match else "")
    return {
        "id": sid,
        "provider_id": sid,
        "url": urllib.parse.urljoin(base_url, title_match.group("href")),
        "title": title,
        "release": title,
        "filename": filename,
        "language": " ".join(language_tags),
        "language_tags": language_tags,
        "formats": formats,
        "subtitle_group": subtitle_group,
        "source_type": source_type,
        "like_count": like_count,
        "download_count": download_count,
        "uploader": strip_html_text(uploader_match.group("uploader") if uploader_match else ""),
        "uploaded_at": str(date_match.group("datetime") if date_match else ""),
        "uploaded_date": strip_html_text(date_match.group("date") if date_match else ""),
        "episode_key": str(episode_key or "").strip().upper(),
        "_score": 200 + min(download_count, 1000000),
    }


def subhd_integer(value):
    digits = re.sub(r"[^0-9]", "", str(value or ""))
    return int(digits) if digits else 0


def subhd_detail_candidate_sort_key(candidate):
    candidate = candidate or {}
    return (
        -int(candidate.get("download_count") or 0),
        -int(candidate.get("like_count") or 0),
        str(candidate.get("uploader") or "").casefold(),
        str(candidate.get("uploaded_at") or ""),
        str(candidate.get("provider_id") or candidate.get("id") or ""),
    )


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


def extract_chinese_subtitle_from_archive(
    body,
    extension,
    candidate,
    max_bytes,
    query="",
    timeout=DEFAULT_SUBTITLE_SEARCH_TIMEOUT_SECONDS,
    require_chinese_body=True,
):
    extension = str(extension or "").casefold()
    if extension == ".zip":
        return extract_chinese_subtitle_from_zip(
            body,
            candidate,
            max_bytes,
            query=query,
            require_chinese_body=require_chinese_body,
        )
    if extension == ".rar":
        return extract_chinese_subtitle_with_bsdtar(
            body,
            candidate,
            max_bytes,
            extension=extension,
            query=query,
            timeout=timeout,
            require_chinese_body=require_chinese_body,
        )
    if extension != ".7z":
        raise RuntimeError("SubHD subtitle archive unsupported: %s" % (extension or "unknown"))
    if subhd_7z_extractor() == "bsdtar":
        return extract_chinese_subtitle_with_bsdtar(
            body,
            candidate,
            max_bytes,
            extension=extension,
            query=query,
            timeout=timeout,
            require_chinese_body=require_chinese_body,
        )
    return extract_chinese_subtitle_with_7zip(
        body,
        extension,
        candidate,
        max_bytes,
        query=query,
        timeout=timeout,
        require_chinese_body=require_chinese_body,
    )


def subhd_archive_episode_keys(body, extension, max_bytes, timeout):
    extension = str(extension or "").casefold()
    if extension == ".zip":
        try:
            archive = zipfile.ZipFile(io.BytesIO(bytes(body or b"")))
        except zipfile.BadZipFile as exc:
            raise RuntimeError("SubHD subtitle ZIP is invalid") from exc
        with archive:
            entries = [
                {"path": item.filename, "size": int(item.file_size or 0), "directory": item.is_dir()}
                for item in archive.infolist()
            ]
    elif extension in (".7z", ".rar"):
        extractor = subhd_7z_extractor() if extension == ".7z" else "bsdtar"
        command = ["7zz", "l", "-slt"] if extractor == "7zz" else ["bsdtar", "-tvf"]
        with tempfile.TemporaryDirectory(prefix="subhd-season-list-") as tmp:
            root = Path(tmp)
            archive_path = root / ("subtitle" + extension)
            archive_path.write_bytes(bytes(body or b""))
            if extractor == "7zz":
                command = command + ["--", str(archive_path)]
            else:
                command = command + [str(archive_path)]
            try:
                listed = subprocess.run(
                    command,
                    capture_output=True,
                    text=True,
                    encoding="utf-8",
                    errors="replace",
                    timeout=max(1, int(timeout or DEFAULT_SUBTITLE_SEARCH_TIMEOUT_SECONDS)),
                    check=False,
                )
            except FileNotFoundError as exc:
                raise RuntimeError("SubHD archive extractor missing: %s" % extractor) from exc
            except subprocess.TimeoutExpired as exc:
                raise RuntimeError("SubHD archive listing timed out") from exc
            if listed.returncode != 0:
                raise RuntimeError("SubHD archive listing failed: %s" % listed.stderr.strip()[:200])
            entries = parse_7zip_archive_entries(listed.stdout) if extractor == "7zz" else parse_bsdtar_archive_entries(listed.stdout)
    else:
        raise RuntimeError("SubHD subtitle archive unsupported: %s" % (extension or "unknown"))
    subtitle_entries = validate_subtitle_archive_entries(entries, max_bytes)
    keys = {
        subtitle_episode_key(posix_basename(item.get("path")))
        for item in subtitle_entries
    }
    keys.discard("")
    return sorted(keys)


def extract_chinese_subtitle_from_zip(body, candidate, max_bytes, query="", require_chinese_body=True):
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
            if filename and subtitle_body_matches_candidate(data, filename, require_chinese_body):
                return filename, data
    if require_chinese_body:
        raise RuntimeError("SubHD archive contains no Chinese subtitle")
    raise RuntimeError("SubHD archive contains no readable subtitle")


def extract_chinese_subtitle_with_7zip(
    body,
    extension,
    candidate,
    max_bytes,
    query="",
    timeout=DEFAULT_SUBTITLE_SEARCH_TIMEOUT_SECONDS,
    require_chinese_body=True,
):
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
            if filename and subtitle_body_matches_candidate(data, filename, require_chinese_body):
                return filename, data
    if require_chinese_body:
        raise RuntimeError("SubHD archive contains no Chinese subtitle")
    raise RuntimeError("SubHD archive contains no readable subtitle")


def extract_chinese_subtitle_with_bsdtar(
    body,
    candidate,
    max_bytes,
    extension=".rar",
    query="",
    timeout=DEFAULT_SUBTITLE_SEARCH_TIMEOUT_SECONDS,
    require_chinese_body=True,
):
    with tempfile.TemporaryDirectory(prefix="subhd-subtitle-") as tmp:
        root = Path(tmp)
        archive_path = root / ("subtitle" + extension)
        archive_path.write_bytes(bytes(body or b""))
        try:
            listed = subprocess.run(
                ["bsdtar", "-tvf", str(archive_path)],
                capture_output=True,
                text=True,
                encoding=locale.getpreferredencoding(False),
                errors="replace",
                timeout=max(1, int(timeout or DEFAULT_SUBTITLE_SEARCH_TIMEOUT_SECONDS)),
                check=False,
            )
        except FileNotFoundError as exc:
            raise RuntimeError("SubHD archive extractor missing: bsdtar") from exc
        except subprocess.TimeoutExpired as exc:
            raise RuntimeError("SubHD archive listing timed out") from exc
        if listed.returncode != 0:
            raise RuntimeError("SubHD archive listing failed: %s" % listed.stderr.strip()[:200])
        entries = parse_bsdtar_archive_entries(listed.stdout)
        subtitle_entries = validate_subtitle_archive_entries(entries, max_bytes, query=query)

        extract_root = root / "extracted"
        extract_root.mkdir()
        try:
            extracted = subprocess.run(
                [
                    "bsdtar",
                    "-xf",
                    str(archive_path),
                    "-C",
                    str(extract_root),
                    "--no-same-owner",
                    "--no-same-permissions",
                ],
                capture_output=True,
                text=True,
                encoding=locale.getpreferredencoding(False),
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
            if filename and subtitle_body_matches_candidate(data, filename, require_chinese_body):
                return filename, data
    if require_chinese_body:
        raise RuntimeError("SubHD archive contains no Chinese subtitle")
    raise RuntimeError("SubHD archive contains no readable subtitle")


def subhd_7z_extractor():
    if shutil.which("7zz"):
        return "7zz"
    if shutil.which("bsdtar"):
        return "bsdtar"
    raise RuntimeError("SubHD archive extractor missing: 7zz or bsdtar")


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


def parse_bsdtar_archive_entries(output):
    entries = []
    for line in str(output or "").replace("\r\n", "\n").splitlines():
        line = line.strip()
        if not line:
            continue
        fields = line.split(None, 8)
        if len(fields) != 9:
            raise RuntimeError("SubHD archive listing output invalid")
        mode, size_text, path = fields[0], fields[4], fields[8]
        if not mode or mode[0] not in ("-", "d"):
            raise RuntimeError("SubHD archive contains unsupported entry type")
        try:
            size = int(size_text)
        except ValueError as exc:
            raise RuntimeError("SubHD archive entry size invalid") from exc
        directory = mode[0] == "d"
        entries.append({"path": path.rstrip("/"), "size": size, "directory": directory})
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
        if target.is_symlink():
            raise RuntimeError("SubHD archive contains symbolic links")
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


def subtitle_body_matches_candidate(body, filename, require_chinese_body):
    if not require_chinese_body:
        return True
    if subtitle_extension(filename) == ".sup":
        return True
    return subtitle_body_is_chinese(body, filename)


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
    extracted = {compact_text(value) for value in extract_codes(haystack)}
    if compact_code in extracted:
        return 1000
    if compact_code_boundary_match(haystack, compact_code):
        return 900
    return 0


def compact_code_boundary_match(value, compact_code):
    if not compact_code:
        return False
    tokens = [compact_text(token) for token in re.findall(r"[0-9A-Za-z]+", str(value or ""))]
    tokens = [token for token in tokens if token]
    for start in range(len(tokens)):
        combined = ""
        for token in tokens[start:]:
            combined += token
            if combined == compact_code:
                return True
            if combined.startswith(compact_code):
                suffix = combined[len(compact_code) :]
                if suffix and not suffix[0].isdigit():
                    return True
            if len(combined) > len(compact_code) + 8:
                break
        if tokens[start].startswith(compact_code):
            suffix = tokens[start][len(compact_code) :]
            if suffix and not suffix[0].isdigit():
                return True
    return False


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


def subtitle_display_name(value, fallback=""):
    name = re.sub(r"[\r\n\t]+", " ", str(value or "")).strip()
    if not name:
        name = str(fallback or "").strip()
    return name[:500]


def subtitle_storage_token(value, fallback):
    token = re.sub(r"[^0-9A-Za-z_-]+", "-", str(value or "").strip()).strip("-_")
    return token or fallback


def subtitle_storage_language(language, filename="", label=""):
    value = str(language or "").strip().replace("_", "-")
    normalized = value.casefold()
    aliases = {
        "chs": "zh-Hans",
        "sc": "zh-Hans",
        "zh-cn": "zh-Hans",
        "zh-hans": "zh-Hans",
        "cht": "zh-Hant",
        "tc": "zh-Hant",
        "zh-tw": "zh-Hant",
        "zh-hant": "zh-Hant",
        "chi": "zh",
        "zho": "zh",
        "chinese": "zh",
        "eng": "en",
        "jpn": "ja",
    }
    if normalized in aliases:
        return aliases[normalized]
    if value:
        return subtitle_storage_token(value, "und")
    detected, _ = subtitle_lang_label(filename, label)
    return subtitle_storage_token(detected, "und")


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
    source = str(track.get("source") or "")
    provider_id = str(track.get("provider_id") or "")
    if provider_id:
        return "%s:%s" % (source, provider_id)
    return "%s:%s" % (source, track.get("filename"))


def subtitle_application_key(source, provider_id):
    source = str(source or "").strip().casefold()
    provider_id = str(provider_id or "").strip()
    if not source or not provider_id:
        return ""
    return hashlib.sha256((source + "\x00" + provider_id).encode("utf-8")).hexdigest()


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
