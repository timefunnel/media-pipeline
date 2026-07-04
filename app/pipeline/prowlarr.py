import json
import re
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET


DEFAULT_PROWLARR_URL = "http://127.0.0.1:9696"
DEFAULT_PROWLARR_CONFIG = "/prowlarr-config/config.xml"
PROWLARR_DOWNLOAD_SCHEME = "prowlarr-download"
PROWLARR_DOWNLOAD_PATH_PATTERN = re.compile(r"^/(\d+)/download$")


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
        with urllib.request.urlopen(request, timeout=timeout) as response:
            raw = response.read().decode("utf-8", "replace")
        return json.loads(raw)

    def resolve_magnet_redirect(self, url, timeout=None):
        class NoRedirect(urllib.request.HTTPRedirectHandler):
            def redirect_request(self, req, fp, code, msg, headers, newurl):
                return None

        opener = urllib.request.build_opener(NoRedirect)
        request = urllib.request.Request(url, headers={"User-Agent": "media-pipeline"})
        try:
            opener.open(request, timeout=timeout)
        except urllib.error.HTTPError as error:
            location = error.headers.get("Location") or ""
            if error.code in (301, 302, 303, 307, 308) and urllib.parse.urlsplit(location).scheme.lower() == "magnet":
                return location
            raise RuntimeError("Prowlarr download did not resolve to magnet: HTTP %s" % error.code)
        except urllib.error.URLError as error:
            raise RuntimeError("Prowlarr download resolution failed: %s" % error.reason)
        raise RuntimeError("Prowlarr download did not resolve to magnet")


class ProwlarrClient:
    def __init__(self, base_url, api_key, transport=None, timeout=30):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.transport = transport or ProwlarrTransport()
        self.timeout = timeout

    def search(self, query, limit=20, indexer_ids=None):
        if not query:
            raise ValueError("query must not be empty")
        params = [("query", query), ("limit", limit)]
        for indexer_id in indexer_ids or []:
            params.append(("indexerIds", str(indexer_id)))
        params = urllib.parse.urlencode(params)
        url = self.base_url + "/api/v1/search?" + params
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
