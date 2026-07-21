import copy
import hashlib
import json
import re
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET


DEFAULT_PROWLARR_URL = "http://127.0.0.1:9696"
DEFAULT_PROWLARR_CONFIG = "/prowlarr-config/config.xml"
PROWLARR_DOWNLOAD_SCHEME = "prowlarr-download"
PROWLARR_DOWNLOAD_PATH_PATTERN = re.compile(r"^/(\d+)/download$")
DEFAULT_PROWLARR_SEARCH_CACHE_SECONDS = 60
DEFAULT_PROWLARR_SEARCH_CACHE_ENTRIES = 256


class ProwlarrApiError(RuntimeError):
    def __init__(self, status_code, message):
        self.status_code = int(status_code)
        self.message = str(message or "request failed")
        super().__init__("Prowlarr HTTP %s: %s" % (self.status_code, self.message))


class ProwlarrSearchCache:
    def __init__(self, ttl_seconds=DEFAULT_PROWLARR_SEARCH_CACHE_SECONDS, max_entries=DEFAULT_PROWLARR_SEARCH_CACHE_ENTRIES, clock=None):
        self.ttl_seconds = max(0, float(ttl_seconds))
        self.max_entries = max(1, int(max_entries))
        self.clock = clock or time.monotonic
        self._entries = {}
        self._lock = threading.Lock()

    def get(self, key):
        now = self.clock()
        with self._lock:
            entry = self._entries.get(key)
            if entry is None:
                return False, None
            expires_at, value = entry
            if expires_at <= now:
                self._entries.pop(key, None)
                return False, None
            return True, copy.deepcopy(value)

    def put(self, key, value):
        if self.ttl_seconds <= 0:
            return
        now = self.clock()
        with self._lock:
            expired = [entry_key for entry_key, (expires_at, _) in self._entries.items() if expires_at <= now]
            for entry_key in expired:
                self._entries.pop(entry_key, None)
            if key not in self._entries and len(self._entries) >= self.max_entries:
                oldest_key = min(self._entries, key=lambda entry_key: self._entries[entry_key][0])
                self._entries.pop(oldest_key, None)
            self._entries[key] = (now + self.ttl_seconds, copy.deepcopy(value))


def safe_prowlarr_download_uri(value):
    if not value:
        return None
    parsed = urllib.parse.urlsplit(str(value))
    if parsed.scheme.lower() not in ("http", "https"):
        return None
    match = PROWLARR_DOWNLOAD_PATH_PATTERN.match(parsed.path)
    if not match:
        return None
    params = [(key, value) for key, value in urllib.parse.parse_qsl(parsed.query, keep_blank_values=True) if key != "apikey"]
    if not any(key == "link" and value for key, value in params):
        return None
    return urllib.parse.urlunsplit((PROWLARR_DOWNLOAD_SCHEME, match.group(1), "", urllib.parse.urlencode(params), ""))


def parse_prowlarr_download_uri(value):
    parsed = urllib.parse.urlsplit(str(value or ""))
    if parsed.scheme.lower() != PROWLARR_DOWNLOAD_SCHEME:
        return None
    if not parsed.netloc.isdigit() or parsed.path not in ("", "/"):
        return None
    params = [(key, value) for key, value in urllib.parse.parse_qsl(parsed.query, keep_blank_values=True) if key != "apikey"]
    if not any(key == "link" and value for key, value in params):
        return None
    return parsed.netloc, params


def is_prowlarr_download_uri(value):
    return parse_prowlarr_download_uri(value) is not None


class ProwlarrConfig:
    def __init__(self, config_path=DEFAULT_PROWLARR_CONFIG):
        self.config_path = str(config_path)

    def load_api_key(self):
        root = ET.parse(self.config_path).getroot()
        api_key = root.findtext("ApiKey")
        if not api_key:
            raise RuntimeError("Prowlarr ApiKey missing in %s" % self.config_path)
        return api_key.strip()


class ProwlarrTransport:
    def request(self, method, url, headers=None, data=None, timeout=None):
        if data is not None:
            raise ValueError("ProwlarrTransport only supports GET")
        request = urllib.request.Request(url, headers=headers or {}, method=method)
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                raw = response.read().decode("utf-8", "replace")
        except urllib.error.HTTPError as error:
            raw = error.read().decode("utf-8", "replace")
            message = error.reason
            try:
                payload = json.loads(raw)
                if isinstance(payload, dict) and payload.get("message"):
                    message = payload["message"]
            except (TypeError, ValueError):
                pass
            raise ProwlarrApiError(error.code, message) from error
        return json.loads(raw)

    def resolve_magnet_redirect(self, url, timeout=None):
        class NoRedirect(urllib.request.HTTPRedirectHandler):
            def redirect_request(self, req, fp, code, msg, headers, newurl):
                return None

        opener = urllib.request.build_opener(NoRedirect)
        request = urllib.request.Request(url, headers={"User-Agent": "media-pipeline"})
        try:
            with opener.open(request, timeout=timeout) as response:
                body = response.read()
                content_type = (response.headers.get("Content-Type") or "").lower()
                if "application/x-bittorrent" in content_type or body.startswith(b"d"):
                    return torrent_bytes_to_magnet(body)
        except urllib.error.HTTPError as error:
            location = error.headers.get("Location") or ""
            if error.code in (301, 302, 303, 307, 308) and urllib.parse.urlsplit(location).scheme.lower() == "magnet":
                return location
            raise RuntimeError("Prowlarr download did not resolve to magnet: HTTP %s" % error.code)
        except urllib.error.URLError as error:
            raise RuntimeError("Prowlarr download resolution failed: %s" % error.reason)
        raise RuntimeError("Prowlarr download did not resolve to magnet")


class ProwlarrClient:
    def __init__(self, base_url, api_key, transport=None, timeout=30, search_cache=None):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.transport = transport or ProwlarrTransport()
        self.timeout = timeout
        self.search_cache = search_cache

    def search(self, query, limit=20, indexer_ids=None, categories=None):
        if not query:
            raise ValueError("query must not be empty")
        params = [("query", query), ("limit", limit)]
        for category in categories or []:
            params.append(("categories", str(category)))
        for indexer_id in indexer_ids or []:
            params.append(("indexerIds", str(indexer_id)))
        params = urllib.parse.urlencode(params)
        url = self.base_url + "/api/v1/search?" + params
        cache_key = url
        if self.search_cache is not None:
            hit, cached = self.search_cache.get(cache_key)
            if hit:
                return cached
        results = self.transport.request(
            "GET",
            url,
            headers={"X-Api-Key": self.api_key},
            timeout=self.timeout,
        )
        if self.search_cache is not None:
            self.search_cache.put(cache_key, results)
        return results

    def tags(self):
        url = self.base_url + "/api/v1/tag"
        return self.transport.request(
            "GET",
            url,
            headers={"X-Api-Key": self.api_key},
            timeout=self.timeout,
        )

    def indexers(self):
        url = self.base_url + "/api/v1/indexer"
        return self.transport.request(
            "GET",
            url,
            headers={"X-Api-Key": self.api_key},
            timeout=self.timeout,
        )

    def resolve_download_uri(self, download_uri):
        parsed = parse_prowlarr_download_uri(download_uri)
        if not parsed:
            return download_uri
        indexer_id, params = parsed
        query = urllib.parse.urlencode([("apikey", self.api_key)] + params)
        url = "%s/%s/download?%s" % (self.base_url, indexer_id, query)
        return self.transport.resolve_magnet_redirect(url, timeout=self.timeout)


def torrent_bytes_to_magnet(data):
    if not isinstance(data, (bytes, bytearray)):
        raise RuntimeError("Prowlarr download did not return torrent bytes")
    torrent, info_bytes = parse_torrent_bytes(bytes(data))
    info = torrent.get(b"info")
    if not isinstance(info, dict) or not info_bytes:
        raise RuntimeError("Prowlarr torrent missing info dictionary")

    info_hash = hashlib.sha1(info_bytes).hexdigest()
    params = [("xt", "urn:btih:%s" % info_hash)]
    name = bdecode_text(info.get(b"name.utf-8") or info.get(b"name"))
    if name:
        params.append(("dn", name))
    for tracker in torrent_trackers(torrent):
        params.append(("tr", tracker))
    return "magnet:?" + urllib.parse.urlencode(params, safe=":")


def parse_torrent_bytes(data):
    if not data:
        raise RuntimeError("empty torrent response")
    if data[0:1] != b"d":
        raise RuntimeError("torrent response is not bencoded")
    value, position, info_bytes = parse_bencode_value(data, 0, capture_info=True)
    if position != len(data):
        raise RuntimeError("torrent response has trailing data")
    if not isinstance(value, dict):
        raise RuntimeError("torrent root is not a dictionary")
    return value, info_bytes


def parse_bencode_value(data, position, capture_info=False):
    if position >= len(data):
        raise RuntimeError("invalid bencode: unexpected end")
    token = data[position : position + 1]
    if token == b"i":
        return parse_bencode_int(data, position)
    if token == b"l":
        return parse_bencode_list(data, position)
    if token == b"d":
        return parse_bencode_dict(data, position, capture_info=capture_info)
    if b"0" <= token <= b"9":
        return parse_bencode_bytes(data, position)
    raise RuntimeError("invalid bencode token at %s" % position)


def parse_bencode_int(data, position):
    end = data.find(b"e", position)
    if end < 0:
        raise RuntimeError("invalid bencode integer")
    raw = data[position + 1 : end]
    try:
        value = int(raw)
    except ValueError as exc:
        raise RuntimeError("invalid bencode integer") from exc
    return value, end + 1, None


def parse_bencode_bytes(data, position):
    colon = data.find(b":", position)
    if colon < 0:
        raise RuntimeError("invalid bencode byte string")
    try:
        length = int(data[position:colon])
    except ValueError as exc:
        raise RuntimeError("invalid bencode byte string length") from exc
    start = colon + 1
    end = start + length
    if end > len(data):
        raise RuntimeError("invalid bencode byte string length")
    return data[start:end], end, None


def parse_bencode_list(data, position):
    position += 1
    values = []
    while True:
        if position >= len(data):
            raise RuntimeError("invalid bencode list")
        if data[position : position + 1] == b"e":
            return values, position + 1, None
        value, position, _ = parse_bencode_value(data, position)
        values.append(value)


def parse_bencode_dict(data, position, capture_info=False):
    position += 1
    values = {}
    info_bytes = None
    while True:
        if position >= len(data):
            raise RuntimeError("invalid bencode dictionary")
        if data[position : position + 1] == b"e":
            return values, position + 1, info_bytes
        key, position, _ = parse_bencode_bytes(data, position)
        value_start = position
        value, position, child_info_bytes = parse_bencode_value(data, position)
        values[key] = value
        if capture_info and key == b"info":
            info_bytes = data[value_start:position]
        elif child_info_bytes and info_bytes is None:
            info_bytes = child_info_bytes


def bdecode_text(value):
    if not isinstance(value, bytes):
        return ""
    for encoding in ("utf-8", "gb18030", "latin-1"):
        try:
            return value.decode(encoding).strip()
        except UnicodeDecodeError:
            continue
    return ""


def torrent_trackers(torrent):
    trackers = []
    announce = bdecode_text(torrent.get(b"announce"))
    if announce:
        trackers.append(announce)
    announce_list = torrent.get(b"announce-list")
    if isinstance(announce_list, list):
        for tier in announce_list:
            if isinstance(tier, list):
                for item in tier:
                    tracker = bdecode_text(item)
                    if tracker:
                        trackers.append(tracker)
            else:
                tracker = bdecode_text(tier)
                if tracker:
                    trackers.append(tracker)
    out = []
    seen = set()
    for tracker in trackers:
        if tracker not in seen:
            seen.add(tracker)
            out.append(tracker)
    return out
