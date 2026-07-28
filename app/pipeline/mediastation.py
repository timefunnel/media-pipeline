import json
import os
import re
import urllib.error
import urllib.parse
import urllib.request

DEFAULT_MSG_BASE_URL = "http://127.0.0.1:18080/api"
CODE_PATTERN = re.compile(r"(?<![A-Za-z0-9])([A-Za-z]{2,10})[\s._-]+(\d{3,4})(?![\d-])", re.IGNORECASE)
FC2_PPV_PATTERN = re.compile(r"(?<![A-Za-z0-9])FC2[\s._-]*PPV[\s._-]*(\d{5,10})(?!\d)", re.IGNORECASE)
FC2_LEGACY_PATTERN = re.compile(r"(?<![A-Za-z0-9])FC2[\s._-]+(\d{5,10})(?:[\s._-]*[A-Za-z])?(?!\d)", re.IGNORECASE)
CODE_PREFIX_DENYLIST = {
    "AAC",
    "BD",
    "BDREMUX",
    "BDRIP",
    "BLURAY",
    "DTS",
    "DL",
    "FLAC",
    "FULLHD",
    "HDR",
    "HEVC",
    "H264",
    "H265",
    "IMAX",
    "MP4",
    "REMUX",
    "TRUEHD",
    "UHD",
    "UHDBD",
    "WEB",
    "WEBDL",
    "X264",
    "X265",
}


def header_positive_int(headers, name):
    try:
        value = int(float(str(headers.get(name) or "0")))
    except (TypeError, ValueError):
        return 0
    return value if value > 0 else 0


class MediaStationTransport:
    def request(self, method, url, headers=None, data=None, timeout=None):
        req_headers = dict(headers or {})
        body = None
        if data is not None:
            body = json.dumps(data, ensure_ascii=False).encode("utf-8")
            req_headers["Content-Type"] = "application/json"

        request = urllib.request.Request(url, data=body, headers=req_headers, method=method)
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                raw = response.read().decode("utf-8", "replace")
        except urllib.error.HTTPError as exc:
            raw = exc.read().decode("utf-8", "replace")
            message = raw
            try:
                payload = json.loads(raw)
                message = payload.get("error") or payload.get("message") or payload.get("msg") or raw
            except ValueError:
                pass
            raise MediaStationApiError(exc.code, message)
        if not raw:
            return {}
        return json.loads(raw)

    def open(self, method, url, headers=None, data=None, timeout=None):
        req_headers = dict(headers or {})
        body = None
        if data is not None:
            body = json.dumps(data, ensure_ascii=False).encode("utf-8")
            req_headers["Content-Type"] = "application/json"
        request = urllib.request.Request(url, data=body, headers=req_headers, method=method)
        try:
            return urllib.request.urlopen(request, timeout=timeout)
        except urllib.error.HTTPError as exc:
            raw = exc.read().decode("utf-8", "replace")
            message = raw
            try:
                payload = json.loads(raw)
                message = payload.get("error") or payload.get("message") or payload.get("msg") or raw
            except ValueError:
                pass
            raise MediaStationApiError(exc.code, message) from exc


class MediaStationApiError(RuntimeError):
    def __init__(self, status_code, message):
        self.status_code = int(status_code)
        self.response_message = str(message or "")
        super().__init__("MediaStationGo API failed: HTTP %s %s" % (self.status_code, self.response_message))


class MediaStationClient:
    def __init__(self, base_url, username, password, transport=None, timeout=30):
        self.base_url = str(base_url or DEFAULT_MSG_BASE_URL).rstrip("/")
        self.username = username
        self.password = password
        self.transport = transport or MediaStationTransport()
        self.timeout = timeout
        self.access_token = None

    def login(self):
        response = self.transport.request(
            "POST",
            self._url("/auth/login"),
            data={"username": self.username, "password": self.password},
            timeout=self.timeout,
        )
        token = extract_access_token(response)
        if not token:
            raise RuntimeError("MediaStationGo login failed: access_token missing")
        self.access_token = token
        return response

    def list_library_media(self, library_id, page=1, page_size=200, group_versions=0):
        query = urllib.parse.urlencode(
            {
                "page": int(page),
                "page_size": int(page_size),
                "group_versions": int(group_versions),
            }
        )
        return self._request("GET", "/libraries/%s/media?%s" % (quote_path(library_id), query))

    def search_media(self, query, limit=20):
        params = urllib.parse.urlencode({"q": query, "limit": int(limit)})
        return self._request("GET", "/media?%s" % params)

    def pipeline_scrape_media(self, media_id, category, title, queries, provider, media_type):
        return self._pipeline_request(
            "POST",
            "/pipeline/media/%s/scrape" % quote_path(media_id),
            data={
                "category": category,
                "title": title,
                "queries": list(queries or []),
                "provider": provider,
                "media_type": media_type,
            },
        )

    def pipeline_repair_movie_extras(self, media_id, target):
        return self._pipeline_request(
            "POST",
            "/pipeline/media/%s/repair-movie-extras" % quote_path(media_id),
            data=dict(target or {}),
        )

    def pipeline_repair_episode_visibility(self, media_id, target):
        return self._pipeline_request(
            "POST",
            "/pipeline/media/%s/repair-episode-visibility" % quote_path(media_id),
            data=dict(target or {}),
        )

    def pipeline_prune_deleted_media(self, target, openlist_paths):
        payload = dict(target or {})
        payload["openlist_paths"] = list(openlist_paths or [])
        return self._pipeline_request("POST", "/pipeline/deleted-media/prune", data=payload)

    def pipeline_list_deleted_media_hide_candidates(self, limit=100):
        return self._pipeline_request(
            "POST",
            "/pipeline/deleted-media/hide-candidates",
            data={"limit": max(1, int(limit))},
        )

    def pipeline_search_migration_candidates(self, query, limit=20):
        return self._pipeline_request(
            "POST",
            "/pipeline/migrations/search",
            data={"query": str(query or "").strip(), "limit": max(1, int(limit))},
        )

    def pipeline_validate_migration(self, source, target):
        return self._pipeline_request(
            "POST",
            "/pipeline/migrations/validate",
            data={"source": dict(source or {}), "target": dict(target or {})},
        )

    def pipeline_apply_migration(self, source, target):
        return self._pipeline_request(
            "POST",
            "/pipeline/migrations/apply",
            data={"source": dict(source or {}), "target": dict(target or {})},
        )

    def pipeline_start_ingest(self, request):
        return self._pipeline_request("POST", "/pipeline/ingest", data=dict(request or {}))

    def pipeline_get_ingest(self, job_id):
        return self._pipeline_request("GET", "/pipeline/ingest/%s" % quote_path(job_id))

    def get_media(self, media_id):
        return self._request("GET", "/media/%s" % quote_path(media_id))

    def soft_delete_media(self, media_id):
        return self._request("DELETE", "/media/%s" % quote_path(media_id))

    def soft_delete_media_version(self, anchor_media_id, version_media_id):
        return self._request(
            "DELETE",
            "/media/%s/versions/%s" % (quote_path(anchor_media_id), quote_path(version_media_id)),
        )

    def pipeline_subtitle_status(self, media_id):
        return self._pipeline_request(
            "GET",
            "/pipeline/media/%s/subtitle-status" % quote_path(media_id),
        )

    def pipeline_translate_subtitle(
        self, provider, model, text, context, glossary, retry_instruction=""
    ):
        response = self._request(
            "POST",
            "/pipeline/subtitles/translate",
            data={
                "provider": str(provider or "").strip(),
                "model": str(model or "").strip(),
                "text": str(text or "").strip(),
                "context": list(context or []),
                "glossary": str(glossary or "").strip(),
                "retry_instruction": str(retry_instruction or "").strip(),
            },
            timeout=120,
        )
        if not isinstance(response, dict) or not isinstance(response.get("translation"), str):
            raise RuntimeError("MediaStationGo subtitle translation API returned invalid response")
        return response

    def download_pipeline_asr_audio(
        self,
        media_id,
        target_path,
        timeout=1800,
        max_bytes=250 * 1024 * 1024,
        retry=True,
        progress_callback=None,
    ):
        if not self.access_token:
            self.login()
        headers = {"Authorization": "Bearer " + self.access_token, "Accept": "audio/mpeg"}
        try:
            response = self.transport.open(
                "GET",
                self._url("/pipeline/media/%s/asr-audio" % quote_path(media_id)),
                headers=headers,
                timeout=timeout,
            )
        except MediaStationApiError as exc:
            if retry and exc.status_code == 401:
                self.access_token = None
                self.login()
                return self.download_pipeline_asr_audio(
                    media_id,
                    target_path,
                    timeout=timeout,
                    max_bytes=max_bytes,
                    retry=False,
                    progress_callback=progress_callback,
                )
            raise
        duration_seconds = header_positive_int(response.headers, "X-Media-Duration-Seconds")
        bitrate_bps = header_positive_int(response.headers, "X-ASR-Audio-Bitrate") or 48000
        written = 0
        try:
            with response, open(target_path, "wb") as output:
                if progress_callback is not None:
                    progress_callback(0, duration_seconds)
                while True:
                    chunk = response.read(64 * 1024)
                    if not chunk:
                        break
                    written += len(chunk)
                    if written > int(max_bytes):
                        raise RuntimeError("MediaStationGo ASR audio exceeds the size limit")
                    output.write(chunk)
                    if progress_callback is not None:
                        extracted_seconds = int(written * 8 / bitrate_bps)
                        progress_callback(
                            min(extracted_seconds, duration_seconds) if duration_seconds > 0 else extracted_seconds,
                            duration_seconds,
                        )
        except Exception:
            try:
                os.unlink(target_path)
            except FileNotFoundError:
                pass
            raise
        if written == 0:
            try:
                os.unlink(target_path)
            except FileNotFoundError:
                pass
            raise RuntimeError("MediaStationGo ASR audio response was empty")
        if progress_callback is not None and duration_seconds > 0:
            progress_callback(duration_seconds, duration_seconds)
        return written

    def pipeline_replace_work_source(self, old_media_id, new_media_id, target, new_openlist_paths):
        payload = dict(target or {})
        payload["new_media_id"] = str(new_media_id or "").strip()
        payload["new_openlist_paths"] = list(new_openlist_paths or [])
        return self._pipeline_request(
            "POST",
            "/pipeline/media/%s/replace-work-source" % quote_path(old_media_id),
            data=payload,
        )

    def _request(self, method, path, data=None, retry=True, timeout=None):
        if not self.access_token:
            self.login()
        headers = {"Authorization": "Bearer " + self.access_token}
        try:
            return self.transport.request(
                method,
                self._url(path),
                headers=headers,
                data=data,
                timeout=self.timeout if timeout is None else timeout,
            )
        except MediaStationApiError as exc:
            if retry and exc.status_code == 401:
                self.access_token = None
                self.login()
                return self._request(method, path, data=data, retry=False, timeout=timeout)
            raise

    def _pipeline_request(self, method, path, data=None, timeout=None):
        response = self._request(method, path, data=data, timeout=timeout)
        if not isinstance(response, dict) or response.get("code") != 0 or not isinstance(response.get("data"), dict):
            raise RuntimeError("MediaStationGo pipeline API returned invalid response")
        return response["data"]

    def _url(self, path):
        return self.base_url + "/" + str(path).lstrip("/")


def extract_access_token(response):
    if not isinstance(response, dict):
        return None
    tokens = response.get("tokens")
    if isinstance(tokens, dict) and tokens.get("access_token"):
        return tokens["access_token"]
    data = response.get("data")
    if isinstance(data, dict):
        tokens = data.get("tokens")
        if isinstance(tokens, dict) and tokens.get("access_token"):
            return tokens["access_token"]
        if data.get("access_token"):
            return data["access_token"]
    return response.get("access_token")


def quote_path(value):
    return urllib.parse.quote(str(value), safe="")


def extract_media_items(response):
    return extract_items(response, ("items", "media", "medias", "results", "list", "content", "records"))


def extract_items(response, keys):
    if isinstance(response, list):
        return response
    if not isinstance(response, dict):
        return []
    for value in response.values():
        if isinstance(value, list):
            return value
    data = response.get("data")
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for key in keys:
            value = data.get(key)
            if isinstance(value, list):
                return value
    return []


def extract_media_id(item):
    if not isinstance(item, dict):
        return None
    for key in ("id", "media_id", "uuid", "item_id"):
        value = item.get(key)
        if value:
            return str(value)
    nested = item.get("media")
    if isinstance(nested, dict):
        return extract_media_id(nested)
    return None


def find_matching_media(items, queries, library_id=None):
    filtered = [item for item in items if media_belongs_to_library(item, library_id)]

    codes = set()
    normalized_queries = []
    for query in queries:
        if not query:
            continue
        codes.update(extract_codes(query))
        normalized = normalize_text(query)
        if normalized:
            normalized_queries.append(normalized)

    best = None
    best_score = 0
    for item in filtered:
        score = media_match_score(item, codes, normalized_queries)
        if score > best_score:
            best = item
            best_score = score
    return best


def media_match_score(item, codes, normalized_queries):
    haystack = media_haystack(item)
    normalized_haystack = normalize_text(haystack)
    score = 0
    if codes and codes.intersection(extract_codes(haystack)):
        score += 1000
    for query in normalized_queries:
        if query and query in normalized_haystack:
            score += 100
    if score <= 0:
        return 0

    score += min(extract_size_bytes(item) // (100 * 1024 * 1024), 100)
    if media_looks_like_extra(item):
        score -= 500
    return max(score, 0)


def extract_size_bytes(item):
    if not isinstance(item, dict):
        return 0
    for key in ("size_bytes", "sizeBytes", "size", "file_size", "fileSize"):
        value = item.get(key)
        if value is None:
            continue
        try:
            return int(value)
        except (TypeError, ValueError):
            continue
    return 0


def media_looks_like_extra(item):
    if not isinstance(item, dict):
        return False
    title = normalize_text(item.get("title") or item.get("name") or "")
    if title in ("menu", "pv", "花絮", "特典", "特辑"):
        return True
    path = str(item.get("path") or item.get("file_path") or item.get("source_path") or "").lower()
    return any(token in path for token in ("/menu/", "/pv/", "/花絮/", "/特典/", "/特辑/"))


def media_belongs_to_library(item, library_id):
    if not library_id or not isinstance(item, dict):
        return True
    item_library_id = item.get("library_id") or item.get("libraryId")
    library = item.get("library")
    if not item_library_id and isinstance(library, dict):
        item_library_id = library.get("id") or library.get("library_id")
    return not item_library_id or str(item_library_id) == str(library_id)


def media_haystack(value):
    chunks = []
    collect_text(value, chunks)
    return " ".join(chunks)


def collect_text(value, chunks):
    if isinstance(value, dict):
        for child in value.values():
            collect_text(child, chunks)
    elif isinstance(value, list):
        for child in value:
            collect_text(child, chunks)
    elif isinstance(value, str):
        chunks.append(value)
    elif isinstance(value, (int, float)):
        chunks.append(str(value))


def iter_code_matches(value):
    text = str(value or "")
    out = []
    occupied = []
    for match in FC2_PPV_PATTERN.finditer(text):
        out.append("FC2-PPV-%s" % match.group(1))
        occupied.append(match.span())
    for match in FC2_LEGACY_PATTERN.finditer(text):
        out.append("FC2-PPV-%s" % match.group(1))
        occupied.append(match.span())
    for match in CODE_PATTERN.finditer(text):
        if any(spans_overlap(match.span(), span) for span in occupied):
            continue
        prefix = match.group(1).upper()
        number = match.group(2)
        if code_like_token_is_noise(prefix, number):
            continue
        out.append("%s-%s" % (prefix, number))
    seen = set()
    unique = []
    for code in out:
        key = code.lower()
        if key in seen:
            continue
        seen.add(key)
        unique.append(code)
    return unique


def spans_overlap(left, right):
    return left[0] < right[1] and right[0] < left[1]


def extract_codes(value):
    out = set()
    for code in iter_code_matches(value):
        out.add(code)
    return out


def code_like_token_is_noise(prefix, number):
    if prefix in CODE_PREFIX_DENYLIST:
        return True
    try:
        numeric = int(number)
    except ValueError:
        return False
    return 1900 <= numeric <= 2099


def normalize_text(value):
    return re.sub(r"[^0-9A-Za-z\u4e00-\u9fff]+", "", str(value or "")).lower()
