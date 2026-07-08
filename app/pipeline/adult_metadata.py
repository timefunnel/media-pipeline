import hashlib
import html
import io
import json
import os
import re
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass

try:
    from PIL import Image, ImageFilter, ImageOps
except ImportError:
    Image = None
    ImageFilter = None
    ImageOps = None


ADULT_ARTWORK_PUBLIC_PATH = "/pipeline-artwork/adult"
DEFAULT_ADULT_ARTWORK_CACHE_DIR = "/artwork-cache/adult"
DEFAULT_ADULT_ARTWORK_FETCH_TIMEOUT_SECONDS = 8
DEFAULT_ADULT_ARTWORK_DOWNLOAD_MAX_BYTES = 6 * 1024 * 1024
DEFAULT_ADULT_METADATA_FETCH_TIMEOUT_SECONDS = 6
DEFAULT_ADULT_METADATA_FLARESOLVERR_URL = ""
DEFAULT_ADULT_METADATA_FLARESOLVERR_TIMEOUT_SECONDS = 20
DEFAULT_ADULT_METADATA_BASE_URLS = (
    "https://javdb.com",
    "https://javbus.sbs",
    "https://www.javbus.com",
    "https://www.cdnbus.cyou",
    "https://www.javsee.cyou",
    "https://www.busjav.cyou",
)
DEFAULT_GENERATED_POSTER_SIZE = (600, 900)
GENERATED_POSTER_STRATEGY = "right-half-v1"
CACHED_ARTWORK_STRATEGY = "cache-v1"
CODE_PATTERN = re.compile(r"(?<![A-Za-z0-9])([A-Za-z]{2,10})[\s._-]?(\d{2,8})(?![\d-])", re.IGNORECASE)
FC2_PPV_PATTERN = re.compile(r"(?<![A-Za-z0-9])FC2[\s._-]*PPV[\s._-]*(\d{5,10})(?!\d)", re.IGNORECASE)
HEYZO_PATTERN = re.compile(r"(?<![A-Za-z0-9])HEYZO[\s._-]*(\d{3,6})(?!\d)", re.IGNORECASE)
UNCENSORED_PATTERN = re.compile(r"(?<!\d)(\d{6})[\s._-](\d{3,5})(?!\d)", re.IGNORECASE)
TITLE_TAG_RE = re.compile(r"(?is)<h[123][^>]*>(.*?)</h[123]>")
HTML_TAG_RE = re.compile(r"(?is)<[^>]+>")
ANCHOR_RE = re.compile(r"(?is)<a\b([^>]*)>(.*?)</a>")
IMAGE_RE = re.compile(r"(?is)<img\b([^>]*)>")
ATTR_RE = re.compile(r"""(?is)([a-zA-Z_:][-a-zA-Z0-9_:.]*)\s*=\s*["']([^"']*)["']""")
JAVBUS_COVER_RE = re.compile(r"""(?is)class=["']bigImage["'][^>]*href=["']([^"']+)["']""")
SAMPLE_RE = re.compile(r"""(?is)<a[^>]+class=["'][^"']*\bsample-box\b[^"']*["'][^>]+href=["']([^"']+)["']""")
DATE_RE = re.compile(r"(?:19|20)\d{2}[-/.]\d{1,2}[-/.]\d{1,2}")
RATING_RE = re.compile(r"(?i)(?:score|rating|評分|评分)[^0-9]{0,20}([0-9](?:\.[0-9])?)")
CODE_PREFIX_DENYLIST = {
    "AC",
    "AAC",
    "AVC",
    "BD",
    "BDRIP",
    "BLURAY",
    "CD",
    "DDP",
    "DTS",
    "FHD",
    "FLAC",
    "FC2",
    "FULLHD",
    "HD",
    "HDR",
    "HEVC",
    "H264",
    "H265",
    "IMAX",
    "MP4",
    "MP",
    "PPV",
    "SD",
    "TRUEHD",
    "UHD",
    "UHDBD",
    "WEB",
    "WEBDL",
    "X264",
    "X265",
}
ADULT_TEXT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "zh-CN,zh;q=0.9,ja;q=0.8,en;q=0.7",
}
ADULT_IMAGE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0 Safari/537.36",
    "Accept": "image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8",
}
FLARESOLVERR_FALLBACK_HTTP_STATUSES = {403, 429, 503, 521, 522, 523, 524, 525, 526}
CF_CHALLENGE_NEEDLES = (
    "cf-browser-verification",
    "cf-chl-widget",
    "cf_chl_opt",
    "cf-chl-bypass",
    "checking if the site connection is secure",
)


class AdultSourceFetchError(RuntimeError):
    def __init__(self, message, status=0, retryable=False):
        super().__init__(message)
        self.status = int(status or 0)
        self.retryable = bool(retryable)


@dataclass(frozen=True)
class AdultArtworkCandidate:
    url: str
    source: str
    role: str
    priority: int = 100


@dataclass(frozen=True)
class AdultMetadataMatch:
    code: str
    source: str
    title: str = ""
    detail_url: str = ""
    poster_url: str = ""
    backdrop_url: str = ""
    year: int = 0
    release_date: str = ""
    rating: float = 0.0
    genres: tuple = ()
    nsfw: bool = True


@dataclass(frozen=True)
class AdultImageProbe:
    url: str
    source: str
    role: str
    priority: int
    width: int
    height: int
    content_type: str
    body: bytes

    @property
    def area(self):
        return int(self.width or 0) * int(self.height or 0)

    @property
    def orientation(self):
        return image_orientation(self.width, self.height)


class AdultMetadataProvider:
    name = "provider"

    def candidates(self, media, codes):
        return []

    def search(self, media, codes):
        return None


class ExistingArtworkProvider(AdultMetadataProvider):
    name = "existing"

    def candidates(self, media, codes):
        poster_url = str((media or {}).get("poster_url") or "").strip()
        backdrop_url = str((media or {}).get("backdrop_url") or "").strip()
        out = []
        if poster_url:
            out.append(AdultArtworkCandidate(poster_url, self.name, "poster", 10))
        if backdrop_url:
            out.append(AdultArtworkCandidate(backdrop_url, self.name, "backdrop", 5))
        return out


class MgstageArtworkProvider(AdultMetadataProvider):
    name = "mgstage"

    def candidates(self, media, codes):
        out = []
        for value in ((media or {}).get("poster_url"), (media or {}).get("backdrop_url")):
            out.extend(AdultArtworkCandidate(url, self.name, "poster", 20) for url in iter_mgstage_poster_candidates(value))
        return out


class DmmArtworkProvider(AdultMetadataProvider):
    name = "dmm"

    def candidates(self, media, codes):
        out = []
        for code in codes:
            for cid in iter_dmm_cids(code):
                base = "https://pics.dmm.co.jp/digital/video/%s/" % cid
                out.append(AdultArtworkCandidate(base + "%spl.jpg" % cid, self.name, "poster", 40))
                out.append(AdultArtworkCandidate(base + "%sjp-1.jpg" % cid, self.name, "backdrop", 45))
                out.append(AdultArtworkCandidate(base + "%sjp.jpg" % cid, self.name, "backdrop", 50))
        return out


class AdultHTMLMetadataProvider(AdultMetadataProvider):
    name = "adult_html"

    def __init__(
        self,
        bases=None,
        timeout=DEFAULT_ADULT_METADATA_FETCH_TIMEOUT_SECONDS,
        max_bytes=4 * 1024 * 1024,
        flaresolverr_url=DEFAULT_ADULT_METADATA_FLARESOLVERR_URL,
        flaresolverr_timeout=DEFAULT_ADULT_METADATA_FLARESOLVERR_TIMEOUT_SECONDS,
    ):
        self.bases = tuple(normalize_adult_base_urls(bases or DEFAULT_ADULT_METADATA_BASE_URLS))
        self.timeout = max(1, int(timeout))
        self.max_bytes = max(1024, int(max_bytes))
        self.flaresolverr_url = str(flaresolverr_url or "").strip().rstrip("/")
        self.flaresolverr_timeout = max(1, int(flaresolverr_timeout))

    def search(self, media, codes):
        for code in codes or []:
            normalized_code = normalize_adult_code(code)
            if not normalized_code:
                continue
            last_error = None
            for base in self.bases:
                try:
                    if adult_source_kind(base) == "javbus":
                        match = self._search_javbus(base, normalized_code)
                    else:
                        match = self._search_javdb(base, normalized_code)
                except RuntimeError as exc:
                    last_error = exc
                    continue
                if match:
                    return match
            if last_error:
                continue
        return None

    def _search_javdb(self, base, code):
        search_url = base.rstrip("/") + "/search?" + urllib.parse.urlencode({"q": code, "f": "all"})
        body = self._fetch_text(search_url, referer=base, allow_flaresolverr=True)
        detail_url = ""
        for raw_attrs, raw_text in ANCHOR_RE.findall(body):
            attrs = html_attrs(raw_attrs)
            class_name = " " + attrs.get("class", "") + " "
            href = attrs.get("href", "")
            if " box " not in class_name or not href:
                continue
            if code in strip_html(raw_text).upper():
                detail_url = absolutize_url(base, href)
                break
        if not detail_url:
            return None
        return parse_adult_detail_html(
            self._fetch_text(detail_url, referer=base, allow_flaresolverr=True), code, "javdb", detail_url
        )

    def _search_javbus(self, base, code):
        detail_url = base.rstrip("/") + "/" + urllib.parse.quote(code, safe="")
        body = self._fetch_text(detail_url, referer=base)
        return parse_adult_detail_html(body, code, "javbus", detail_url)

    def _fetch_text(self, url, referer="", allow_flaresolverr=False):
        try:
            return self._fetch_text_direct(url, referer=referer)
        except AdultSourceFetchError as exc:
            if allow_flaresolverr and self.flaresolverr_url and exc.retryable:
                return self._fetch_text_flaresolverr(url, referer=referer, direct_error=exc)
            raise

    def _fetch_text_direct(self, url, referer=""):
        headers = dict(ADULT_TEXT_HEADERS)
        if referer:
            headers["Referer"] = referer
        request = urllib.request.Request(url, headers=headers, method="GET")
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                status = getattr(response, "status", 200)
                if status == 404:
                    return ""
                if status >= 400:
                    raise AdultSourceFetchError(
                        "adult source returned HTTP %s" % status,
                        status=status,
                        retryable=status in FLARESOLVERR_FALLBACK_HTTP_STATUSES,
                    )
                body = read_limited(response, self.max_bytes).decode("utf-8", "replace")
                if looks_like_cloudflare_challenge(body):
                    raise AdultSourceFetchError("adult source returned Cloudflare challenge", retryable=True)
                return body
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                return ""
            raise AdultSourceFetchError(
                "adult source returned HTTP %s" % exc.code,
                status=exc.code,
                retryable=exc.code in FLARESOLVERR_FALLBACK_HTTP_STATUSES,
            ) from exc
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            raise AdultSourceFetchError("adult source fetch failed: %s" % exc, retryable=True) from exc

    def _fetch_text_flaresolverr(self, url, referer="", direct_error=None):
        api_url = self.flaresolverr_url.rstrip("/")
        if not api_url.endswith("/v1"):
            api_url += "/v1"
        payload = {
            "cmd": "request.get",
            "url": url,
            "maxTimeout": int(self.flaresolverr_timeout * 1000),
        }
        request = urllib.request.Request(
            api_url,
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json", "Accept": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=self.flaresolverr_timeout) as response:
                status = getattr(response, "status", 200)
                if status < 200 or status >= 300:
                    raise RuntimeError("FlareSolverr returned HTTP %s" % status)
                raw = read_limited(response, self.max_bytes + 1024 * 1024).decode("utf-8", "replace")
        except urllib.error.HTTPError as exc:
            raise RuntimeError("FlareSolverr returned HTTP %s after direct error: %s" % (exc.code, direct_error)) from exc
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            raise RuntimeError("FlareSolverr request failed after direct error: %s" % exc) from exc

        try:
            data = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise RuntimeError("FlareSolverr returned invalid JSON after direct error: %s" % direct_error) from exc
        if not isinstance(data, dict):
            raise RuntimeError("FlareSolverr returned invalid response type after direct error: %s" % direct_error)
        if data.get("status") != "ok":
            message = str(data.get("message") or data.get("status") or "unknown")
            raise RuntimeError("FlareSolverr returned status %s after direct error: %s" % (message, direct_error))
        solution = data.get("solution")
        if not isinstance(solution, dict):
            raise RuntimeError("FlareSolverr returned invalid solution after direct error: %s" % direct_error)
        try:
            solution_status = int(solution.get("status") or 0)
        except (TypeError, ValueError) as exc:
            raise RuntimeError("FlareSolverr solution returned invalid HTTP status after direct error: %s" % direct_error) from exc
        if solution_status < 200 or solution_status >= 300:
            raise RuntimeError(
                "FlareSolverr solution returned HTTP %s after direct error: %s" % (solution_status, direct_error)
            )
        body = str(solution.get("response") or "")
        if not body.strip():
            raise RuntimeError("FlareSolverr returned empty response after direct error: %s" % direct_error)
        if len(body.encode("utf-8")) > self.max_bytes:
            raise RuntimeError("FlareSolverr response exceeds max bytes")
        if looks_like_cloudflare_challenge(body):
            raise RuntimeError("FlareSolverr returned Cloudflare challenge page after direct error: %s" % direct_error)
        return body


DEFAULT_ADULT_METADATA_PROVIDERS = (
    ExistingArtworkProvider(),
    MgstageArtworkProvider(),
    DmmArtworkProvider(),
)


class AdultImageFetcher:
    def __init__(self, timeout=DEFAULT_ADULT_ARTWORK_FETCH_TIMEOUT_SECONDS, max_bytes=DEFAULT_ADULT_ARTWORK_DOWNLOAD_MAX_BYTES):
        self.timeout = max(1, int(timeout))
        self.max_bytes = max(1024, int(max_bytes))

    def fetch(self, candidate):
        url = str(candidate.url or "").strip()
        if not url.startswith(("http://", "https://")):
            raise RuntimeError("image url must be http(s)")
        request = urllib.request.Request(url, headers=ADULT_IMAGE_HEADERS, method="GET")
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                final_url = response.geturl() if hasattr(response, "geturl") else url
                if "now_printing" in str(final_url or "").lower():
                    raise RuntimeError("image is DMM now_printing placeholder")
                status = getattr(response, "status", 200)
                if status < 200 or status >= 300:
                    raise RuntimeError("image HTTP status %s" % status)
                content_type = response.headers.get("Content-Type", "")
                body = read_limited(response, self.max_bytes)
        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError, OSError) as exc:
            raise RuntimeError("image fetch failed: %s" % exc) from exc
        width, height = image_size(body)
        if width <= 0 or height <= 0:
            raise RuntimeError("image decode failed")
        return AdultImageProbe(
            url=url,
            source=candidate.source,
            role=candidate.role,
            priority=candidate.priority,
            width=width,
            height=height,
            content_type=content_type,
            body=body,
        )


def read_limited(response, max_bytes):
    chunks = []
    total = 0
    while True:
        chunk = response.read(min(64 * 1024, max_bytes + 1 - total))
        if not chunk:
            break
        total += len(chunk)
        if total > max_bytes:
            raise RuntimeError("response exceeds max bytes")
        chunks.append(chunk)
    return b"".join(chunks)


def image_size(body):
    if Image is None:
        raise RuntimeError("Pillow is required to read artwork dimensions")
    try:
        with Image.open(io.BytesIO(body or b"")) as image:
            return int(image.width), int(image.height)
    except (OSError, ValueError) as exc:
        raise RuntimeError("image decode failed") from exc


def image_orientation(width, height):
    width = int(width or 0)
    height = int(height or 0)
    if width <= 0 or height <= 0:
        return "unknown"
    if height >= width * 1.12:
        return "portrait"
    if width >= height * 1.12:
        return "landscape"
    return "square"


def build_adult_artwork_repair(
    media,
    cache_dir="",
    public_base_url="",
    generate_portrait=True,
    fetcher=None,
    providers=None,
    metadata_provider=None,
):
    if not isinstance(media, dict):
        return {"status": "skipped", "updated": 0, "reason": "invalid_media"}
    codes = adult_codes_from_media(media)
    metadata_match = None
    if metadata_provider is not False:
        metadata_provider = metadata_provider or AdultHTMLMetadataProvider()
        metadata_match = metadata_provider.search(media, codes)
    patch = metadata_patch(media, metadata_match)
    candidates = collect_adult_artwork_candidates(media, providers=providers, metadata_match=metadata_match)
    if not candidates:
        if patch:
            return {
                "status": "success",
                "updated": len(patch),
                "reason": "metadata_repaired",
                "fields": sorted(patch.keys()),
                "patch": patch,
                "metadata_source": metadata_match.source if metadata_match else None,
            }
        return {"status": "skipped", "updated": 0, "reason": "candidate_not_found"}

    fetch = fetcher or AdultImageFetcher()
    probes = []
    errors = []
    for candidate in candidates:
        try:
            probe = fetch.fetch(candidate)
        except RuntimeError as exc:
            errors.append("%s:%s" % (candidate.source, str(exc)))
            continue
        if probe.orientation in ("portrait", "landscape"):
            probes.append(probe)

    if not probes:
        if patch:
            return {
                "status": "success",
                "updated": len(patch),
                "reason": "metadata_repaired",
                "fields": sorted(patch.keys()),
                "patch": patch,
                "metadata_source": metadata_match.source if metadata_match else None,
                "errors": errors[:5],
            }
        return {
            "status": "skipped",
            "updated": 0,
            "reason": "usable_image_not_found",
            "errors": errors[:5],
        }

    portrait = select_probe(probes, "portrait")
    landscape = select_probe(probes, "landscape")
    generated = None
    generated_reason = None
    if not portrait and landscape and generate_portrait:
        if not str(public_base_url or "").strip():
            generated_reason = "public_base_url_missing"
        else:
            generated = generate_portrait_artwork(landscape, cache_dir=cache_dir, public_base_url=public_base_url)
            portrait = AdultImageProbe(
                url=generated["url"],
                source="generated",
                role="poster",
                priority=15,
                width=generated["width"],
                height=generated["height"],
                content_type="image/jpeg",
                body=b"",
            )
    if portrait:
        portrait = cache_artwork_probe(portrait, cache_dir=cache_dir, public_base_url=public_base_url, role="poster")
    if landscape:
        landscape = cache_artwork_probe(landscape, cache_dir=cache_dir, public_base_url=public_base_url, role="backdrop")

    current_poster = str(media.get("poster_url") or "").strip()
    current_backdrop = str(media.get("backdrop_url") or "").strip()
    if portrait and normalize_url(current_poster) != normalize_url(portrait.url):
        patch["poster_url"] = portrait.url
    if landscape and normalize_url(current_backdrop) != normalize_url(landscape.url):
        patch["backdrop_url"] = landscape.url
    if not patch:
        if generated_reason:
            return {
                "status": "skipped",
                "updated": 0,
                "reason": generated_reason,
                "poster_source": None,
                "backdrop_source": getattr(landscape, "source", None) if landscape else None,
            }
        return {"status": "skipped", "updated": 0, "reason": "not_needed"}

    reason = "semantic_artwork_repaired"
    if generated:
        reason = "portrait_generated"
    elif generated_reason:
        reason = generated_reason

    return {
        "status": "success",
        "updated": len(patch),
        "reason": reason,
        "fields": sorted(patch.keys()),
        "patch": patch,
        "poster_source": getattr(portrait, "source", None) if portrait else None,
        "backdrop_source": getattr(landscape, "source", None) if landscape else None,
        "generated": generated,
        "metadata_source": metadata_match.source if metadata_match else None,
    }


def collect_adult_artwork_candidates(media, providers=None, metadata_match=None):
    providers = providers or DEFAULT_ADULT_METADATA_PROVIDERS
    codes = adult_codes_from_media(media)
    out = []
    seen = set()
    if metadata_match:
        for candidate in metadata_artwork_candidates(metadata_match):
            key = normalize_url(candidate.url)
            if key and key not in seen:
                seen.add(key)
                out.append(candidate)
    for provider in providers:
        for candidate in provider.candidates(media, codes):
            url = str(candidate.url or "").strip()
            key = normalize_url(url)
            if not key or key in seen:
                continue
            seen.add(key)
            out.append(candidate)
    return out


def metadata_artwork_candidates(match):
    out = []
    if match.poster_url:
        out.append(AdultArtworkCandidate(match.poster_url, match.source, "poster", 1))
    if match.backdrop_url:
        out.append(AdultArtworkCandidate(match.backdrop_url, match.source, "backdrop", 2))
    return out


def metadata_patch(media, match):
    if not match:
        return {}
    patch = {}
    for key, value in (
        ("title", match.title),
        ("original_name", match.code),
        ("release_date", match.release_date),
    ):
        value = str(value or "").strip()
        if value and str((media or {}).get(key) or "").strip() != value:
            patch[key] = value
    if match.year and int_value((media or {}).get("year")) != match.year:
        patch["year"] = match.year
    if match.rating and float_value((media or {}).get("rating")) != match.rating:
        patch["rating"] = match.rating
    if match.genres:
        genres = ",".join(unique_strings(match.genres))
        if genres and str((media or {}).get("genres") or "").strip() != genres:
            patch["genres"] = genres
    if match.nsfw and (media or {}).get("nsfw") is not True:
        patch["nsfw"] = True
    return patch


def select_probe(probes, orientation):
    matches = [probe for probe in probes or [] if probe.orientation == orientation]
    if not matches:
        return None
    return sorted(matches, key=lambda probe: (probe.priority, -probe.area, probe.url))[0]


def generate_portrait_artwork(probe, cache_dir="", public_base_url="", size=DEFAULT_GENERATED_POSTER_SIZE):
    if Image is None or ImageOps is None or ImageFilter is None:
        raise RuntimeError("Pillow is required to generate portrait artwork")
    cache_dir = str(cache_dir or DEFAULT_ADULT_ARTWORK_CACHE_DIR)
    public_base_url = str(public_base_url or "").strip().rstrip("/")
    if not public_base_url:
        raise RuntimeError("adult artwork public base url missing")
    os.makedirs(cache_dir, exist_ok=True)
    digest = hashlib.sha1(
        ("%s\n%s\n%s" % (GENERATED_POSTER_STRATEGY, probe.url, hashlib.sha1(probe.body).hexdigest())).encode("utf-8")
    ).hexdigest()[:20]
    filename = "%s.jpg" % digest
    path = os.path.join(cache_dir, filename)
    width, height = size
    if not os.path.exists(path):
        with Image.open(io.BytesIO(probe.body)) as image:
            source = image.convert("RGB")
            resample = getattr(getattr(Image, "Resampling", Image), "LANCZOS")
            right_half = source.crop((source.width // 2, 0, source.width, source.height))
            poster = ImageOps.fit(right_half, (width, height), method=resample)
            poster.save(path, format="JPEG", quality=90, optimize=True)
    return {
        "path": path,
        "filename": filename,
        "url": public_base_url + ADULT_ARTWORK_PUBLIC_PATH + "/" + urllib.parse.quote(filename, safe=""),
        "width": width,
        "height": height,
    }


def cache_artwork_probe(probe, cache_dir="", public_base_url="", role="artwork"):
    public_base_url = str(public_base_url or "").strip().rstrip("/")
    if not public_base_url or artwork_url_is_local(probe.url, public_base_url):
        return probe
    if Image is None:
        raise RuntimeError("Pillow is required to cache artwork")
    cache_dir = str(cache_dir or DEFAULT_ADULT_ARTWORK_CACHE_DIR)
    os.makedirs(cache_dir, exist_ok=True)
    body_hash = hashlib.sha1(probe.body).hexdigest()
    digest = hashlib.sha1(
        ("%s\n%s\n%s\n%s" % (CACHED_ARTWORK_STRATEGY, role, probe.url, body_hash)).encode("utf-8")
    ).hexdigest()[:20]
    filename = "%s.jpg" % digest
    path = os.path.join(cache_dir, filename)
    width = int(probe.width or 0)
    height = int(probe.height or 0)
    if not os.path.exists(path):
        with Image.open(io.BytesIO(probe.body)) as image:
            source = image.convert("RGB")
            width, height = int(source.width), int(source.height)
            source.save(path, format="JPEG", quality=90, optimize=True)
    return AdultImageProbe(
        url=public_base_url + ADULT_ARTWORK_PUBLIC_PATH + "/" + urllib.parse.quote(filename, safe=""),
        source="cached:%s" % probe.source,
        role=probe.role,
        priority=probe.priority,
        width=width,
        height=height,
        content_type="image/jpeg",
        body=b"",
    )


def artwork_url_is_local(url, public_base_url):
    public_base_url = str(public_base_url or "").strip().rstrip("/")
    if not public_base_url:
        return False
    return str(url or "").startswith(public_base_url + ADULT_ARTWORK_PUBLIC_PATH + "/")


def adult_codes_from_media(media):
    values = []
    if isinstance(media, dict):
        for key in ("original_name", "title", "name", "file_name", "filename", "path", "file_path", "source_path"):
            value = media.get(key)
            if value:
                values.append(value)
        nested = media.get("media")
        if isinstance(nested, dict):
            values.extend(adult_codes_from_media(nested))
    return unique_codes(values)


def unique_codes(values):
    out = []
    seen = set()
    for value in values or []:
        for code in iter_code_matches(value):
            key = code.lower()
            if key in seen:
                continue
            seen.add(key)
            out.append(code)
    return out


def iter_code_matches(value):
    text = str(value or "")
    for match in FC2_PPV_PATTERN.finditer(text):
        yield "FC2-PPV-%s" % match.group(1)
    for match in HEYZO_PATTERN.finditer(text):
        yield "HEYZO-%s" % match.group(1)
    for match in UNCENSORED_PATTERN.finditer(text):
        yield "%s-%s" % (match.group(1), match.group(2))
    for match in CODE_PATTERN.finditer(text):
        prefix = match.group(1).upper()
        if prefix in CODE_PREFIX_DENYLIST:
            continue
        yield "%s-%s" % (prefix, match.group(2))


def normalize_adult_code(value):
    codes = list(iter_code_matches(str(value or "").replace("_", "-").upper()))
    return codes[0] if codes else ""


def iter_dmm_cids(code):
    match = re.match(r"^([A-Za-z]{2,10})-(\d{3,5})$", str(code or ""))
    if not match:
        return
    prefix = match.group(1).lower()
    raw = match.group(2)
    number = int(raw)
    variants = [str(number), "%03d" % number, "%04d" % number, "%05d" % number]
    seen = set()
    for prefix_value in (prefix, "1" + prefix):
        for value in variants:
            cid = prefix_value + value
            if cid in seen:
                continue
            seen.add(cid)
            yield cid


def iter_mgstage_poster_candidates(value):
    parsed = urllib.parse.urlparse(str(value or ""))
    if parsed.netloc.lower() != "image.mgstage.com":
        return
    filename = parsed.path.rsplit("/", 1)[-1]
    if "cap_e_0_" not in filename:
        return
    for prefix in ("pf_o1_", "pb_e_", "pb_e_0_"):
        candidate_name = filename.replace("cap_e_0_", prefix, 1)
        yield urllib.parse.urlunparse(parsed._replace(path=parsed.path.rsplit("/", 1)[0] + "/" + candidate_name))


def normalize_url(value):
    return str(value or "").strip()


def looks_like_cloudflare_challenge(value):
    lower = str(value or "").lower()
    if not lower:
        return False
    if "cloudflare" in lower and ("just a moment" in lower or "attention required" in lower):
        return True
    return any(needle in lower for needle in CF_CHALLENGE_NEEDLES)


def normalize_adult_base_urls(value):
    if isinstance(value, str):
        raw_values = re.split(r"[,;\s]+", value)
    else:
        raw_values = list(value or [])
    out = []
    seen = set()
    for raw in raw_values:
        base = str(raw or "").strip()
        if not base:
            continue
        if not base.startswith(("http://", "https://")):
            base = "https://" + base
        base = base.rstrip("/")
        key = base.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(base)
    return out


def adult_source_kind(base):
    host = urllib.parse.urlparse(str(base or "")).hostname or str(base or "")
    host = host.lower()
    if "javdb" in host:
        return "javdb"
    for needle in ("javbus", "cdnbus", "javsee", "busjav"):
        if needle in host:
            return "javbus"
    return "javdb"


def parse_adult_detail_html(body, code, source, detail_url):
    title = first_adult_title(body, code)
    if not title:
        return None
    poster_url = ""
    if source == "javbus":
        match = JAVBUS_COVER_RE.search(body or "")
        if match:
            poster_url = absolutize_url(detail_url, match.group(1))
    else:
        poster_url = first_adult_image(body, "video-cover", "cover", "column-video-cover")
        if poster_url:
            poster_url = absolutize_url(detail_url, poster_url)
    backdrop_url = ""
    match = SAMPLE_RE.search(body or "")
    if match:
        backdrop_url = absolutize_url(detail_url, match.group(1))
    dmm_poster = dmm_poster_from_sample_url(backdrop_url)
    if dmm_poster:
        poster_url = dmm_poster
    release_date = first_date(body)
    year = int(release_date[:4]) if release_date else 0
    return AdultMetadataMatch(
        code=code,
        source=source,
        title=title,
        detail_url=detail_url,
        poster_url=poster_url,
        backdrop_url=backdrop_url,
        year=year,
        release_date=release_date,
        rating=first_rating(body),
        genres=("Adult", source),
        nsfw=True,
    )


def first_adult_title(body, code):
    for raw in TITLE_TAG_RE.findall(body or ""):
        title = strip_html(raw).strip()
        if not title:
            continue
        for prefix in (str(code or ""), str(code or "").upper()):
            if title.startswith(prefix):
                title = title[len(prefix) :].strip()
        if title:
            return title
    return ""


def strip_html(value):
    return " ".join(html.unescape(HTML_TAG_RE.sub(" ", str(value or ""))).split())


def first_adult_image(body, *class_needles):
    for raw_attrs in IMAGE_RE.findall(body or ""):
        attrs = html_attrs(raw_attrs)
        class_name = attrs.get("class", "").lower()
        for needle in class_needles:
            if needle.lower() in class_name:
                return attrs.get("src") or attrs.get("data-src") or ""
    return ""


def html_attrs(raw):
    return {key.lower(): html.unescape(value) for key, value in ATTR_RE.findall(raw or "")}


def absolutize_url(base, raw):
    raw = html.unescape(str(raw or "").strip())
    if not raw:
        return ""
    parsed = urllib.parse.urlparse(raw)
    if parsed.scheme and parsed.netloc:
        return raw
    return urllib.parse.urljoin(str(base or ""), raw)


def dmm_poster_from_sample_url(raw):
    parsed = urllib.parse.urlparse(str(raw or "").strip())
    if not parsed.netloc or "dmm.co.jp" not in parsed.netloc.lower():
        return ""
    lower_path = parsed.path.lower()
    for suffix in ("jp-1.jpg", "jp.jpg"):
        if lower_path.endswith(suffix):
            path = parsed.path[: -len(suffix)] + "pl.jpg"
            return urllib.parse.urlunparse(parsed._replace(path=path))
    return ""


def first_date(body):
    match = DATE_RE.search(body or "")
    if not match:
        return ""
    return match.group(0).replace("/", "-").replace(".", "-")


def first_rating(body):
    match = RATING_RE.search(body or "")
    if not match:
        return 0.0
    try:
        return float(match.group(1))
    except ValueError:
        return 0.0


def int_value(value):
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def float_value(value):
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def unique_strings(values):
    out = []
    seen = set()
    for value in values or []:
        text = str(value or "").strip()
        key = text.lower()
        if text and key not in seen:
            seen.add(key)
            out.append(text)
    return out
