import json
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
    "BDRIP",
    "BLURAY",
    "DTS",
    "FLAC",
    "FULLHD",
    "HDR",
    "HEVC",
    "H264",
    "H265",
    "IMAX",
    "MP4",
    "TRUEHD",
    "UHD",
    "UHDBD",
    "WEB",
    "WEBDL",
    "X264",
    "X265",
}
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

    def scan_root(self, library_id, root_id):
        return self._request("POST", "/libraries/%s/roots/%s/scan" % (quote_path(library_id), quote_path(root_id)))

    def list_libraries(self, include_hidden=False):
        path = "/libraries"
        if include_hidden:
            path += "?" + urllib.parse.urlencode({"include_hidden": 1})
        return self._request("GET", path)

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

    def pipeline_scrape_media(self, media_id, payload):
        if not isinstance(payload, dict):
            raise ValueError("MediaStationGo pipeline scrape payload must be an object")
        return extract_response_data(
            self._request("POST", "/pipeline/media/%s/scrape" % quote_path(media_id), data=payload)
        )

    def scrape_media(self, media_id):
        return self._request(
            "POST",
            "/media/%s/scrape" % quote_path(media_id),
            data={
                "episode_images": False,
                "refresh_matched": True,
                "include_matched": True,
            },
        )

    def search_scrape_matches(self, media_id, query, provider, media_type):
        params = urllib.parse.urlencode({"query": query, "provider": provider, "media_type": media_type})
        return self._request("GET", "/media/%s/scrape/search?%s" % (quote_path(media_id), params))

    def apply_scrape_match(self, media_id, match):
        if not isinstance(match, dict):
            raise ValueError("MediaStationGo scrape match must be an object")
        data = dict(match)
        data["episode_images"] = False
        return self._request("POST", "/media/%s/scrape/apply" % quote_path(media_id), data=data)

    def get_media(self, media_id):
        return self._request("GET", "/media/%s" % quote_path(media_id))

    def update_media_metadata(self, media_id, fields):
        allowed = {
            "title",
            "original_name",
            "overview",
            "poster_url",
            "backdrop_url",
            "year",
            "release_date",
            "rating",
            "genres",
            "nsfw",
        }
        data = {key: value for key, value in (fields or {}).items() if key in allowed}
        if not data:
            raise ValueError("MediaStationGo metadata update fields missing")
        return self._request("PATCH", "/media/%s/metadata" % quote_path(media_id), data=data)

    def repair_movie_extras(self, media_id, category, library_id=None, root_id=None, root_openlist_path=None):
        return extract_response_data(
            self._request(
                "POST",
                "/pipeline/media/%s/repair-movie-extras" % quote_path(media_id),
                data=build_pipeline_maintenance_payload(category, library_id, root_id, root_openlist_path),
            )
        )

    def repair_episode_visibility(self, media_id, category, library_id=None, root_id=None, root_openlist_path=None):
        return extract_response_data(
            self._request(
                "POST",
                "/pipeline/media/%s/repair-episode-visibility" % quote_path(media_id),
                data=build_pipeline_maintenance_payload(category, library_id, root_id, root_openlist_path),
            )
        )

    def prune_deleted_media(self, category, openlist_paths, library_id=None, root_id=None, root_openlist_path=None):
        data = build_pipeline_maintenance_payload(category, library_id, root_id, root_openlist_path)
        data["openlist_paths"] = list(openlist_paths or [])
        return extract_response_data(self._request("POST", "/pipeline/deleted-media/prune", data=data))

    def start_pipeline_ingest(self, payload):
        if not isinstance(payload, dict):
            raise ValueError("MediaStationGo pipeline ingest payload must be an object")
        return extract_response_data(self._request("POST", "/pipeline/ingest", data=payload))

    def get_pipeline_ingest(self, job_id):
        return extract_response_data(self._request("GET", "/pipeline/ingest/%s" % quote_path(job_id)))

    def _request(self, method, path, data=None, retry=True):
        if not self.access_token:
            self.login()
        headers = {"Authorization": "Bearer " + self.access_token}
        try:
            return self.transport.request(method, self._url(path), headers=headers, data=data, timeout=self.timeout)
        except MediaStationApiError as exc:
            if retry and exc.status_code == 401:
                self.access_token = None
                self.login()
                return self._request(method, path, data=data, retry=False)
            raise

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


def build_pipeline_maintenance_payload(category, library_id=None, root_id=None, root_openlist_path=None):
    data = {"category": str(category or "").strip()}
    for key, value in (
        ("library_id", library_id),
        ("root_id", root_id),
        ("root_openlist_path", root_openlist_path),
    ):
        value = str(value or "").strip()
        if value:
            data[key] = value
    return data


def extract_response_data(response):
    if isinstance(response, dict) and isinstance(response.get("data"), dict):
        return response["data"]
    return response


def extract_media_items(response):
    return extract_items(response, ("items", "media", "medias", "results", "list", "content", "records"))


def extract_scrape_matches(response):
    return extract_items(response, ("items", "matches", "results", "list", "content", "records"))


def extract_library_items(response):
    return extract_items(response, ("items", "libraries", "results", "list", "content", "records"))


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
