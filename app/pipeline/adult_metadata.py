import hashlib
import io
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
DEFAULT_GENERATED_POSTER_SIZE = (600, 900)
CODE_PATTERN = re.compile(r"(?<![A-Za-z0-9])([A-Za-z]{2,10})[\s._-]+(\d{3,5})(?![\d-])", re.IGNORECASE)
FC2_PPV_PATTERN = re.compile(r"(?<![A-Za-z0-9])FC2[\s._-]*PPV[\s._-]*(\d{5,10})(?!\d)", re.IGNORECASE)
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
ADULT_IMAGE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0 Safari/537.36",
    "Accept": "image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8",
}


@dataclass(frozen=True)
class AdultArtworkCandidate:
    url: str
    source: str
    role: str
    priority: int = 100


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
            raise RuntimeError("image exceeds max bytes")
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


def build_adult_artwork_repair(media, cache_dir="", public_base_url="", generate_portrait=True, fetcher=None, providers=None):
    if not isinstance(media, dict):
        return {"status": "skipped", "updated": 0, "reason": "invalid_media"}
    candidates = collect_adult_artwork_candidates(media, providers=providers)
    if not candidates:
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

    patch = {}
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
    }


def collect_adult_artwork_candidates(media, providers=None):
    providers = providers or DEFAULT_ADULT_METADATA_PROVIDERS
    codes = adult_codes_from_media(media)
    out = []
    seen = set()
    for provider in providers:
        for candidate in provider.candidates(media, codes):
            url = str(candidate.url or "").strip()
            key = normalize_url(url)
            if not key or key in seen:
                continue
            seen.add(key)
            out.append(candidate)
    return out


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
    digest = hashlib.sha1(("%s\n%s" % (probe.url, hashlib.sha1(probe.body).hexdigest())).encode("utf-8")).hexdigest()[:20]
    filename = "%s.jpg" % digest
    path = os.path.join(cache_dir, filename)
    width, height = size
    if not os.path.exists(path):
        with Image.open(io.BytesIO(probe.body)) as image:
            source = image.convert("RGB")
            resample = getattr(getattr(Image, "Resampling", Image), "LANCZOS")
            background = ImageOps.fit(source, (width, height), method=resample)
            background = background.filter(ImageFilter.GaussianBlur(radius=18))
            overlay_width = int(width * 0.92)
            overlay_height = int(round(overlay_width * source.height / max(1, source.width)))
            if overlay_height > int(height * 0.78):
                overlay_height = int(height * 0.78)
                overlay_width = int(round(overlay_height * source.width / max(1, source.height)))
            overlay = source.resize((max(1, overlay_width), max(1, overlay_height)), resample=resample)
            x = (width - overlay.width) // 2
            y = (height - overlay.height) // 2
            background.paste(overlay, (x, y))
            background.save(path, format="JPEG", quality=90, optimize=True)
    return {
        "path": path,
        "filename": filename,
        "url": public_base_url + ADULT_ARTWORK_PUBLIC_PATH + "/" + urllib.parse.quote(filename, safe=""),
        "width": width,
        "height": height,
    }


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
    for match in CODE_PATTERN.finditer(text):
        prefix = match.group(1).upper()
        if prefix in CODE_PREFIX_DENYLIST:
            continue
        yield "%s-%03d" % (prefix, int(match.group(2)))


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
