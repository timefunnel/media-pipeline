import re
import sys
import time
import urllib.parse
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait

from pipeline.client115 import parse_115_share_url
from pipeline.mediastation import extract_codes
from pipeline.resource_selector import ResourceSelector


DEFAULT_UPSTREAM_SEARCH_LIMIT = 100
DEFAULT_REQUIRED_INDEXER_SEARCH_LIMIT = 1000
DEFAULT_ANIME_INDEXER_SEARCH_LIMIT = 100
DEFAULT_PRIMARY_INDEXER_TIMEOUT_SECONDS = 12
DEFAULT_OPTIONAL_INDEXER_TIMEOUT_SECONDS = 12
DEFAULT_PROWLARR_MAX_WORKERS = 8
DEFAULT_PROWLARR_SEARCH_TIMEOUT_SECONDS = 4
DEFAULT_PROWLARR_EARLY_RETURN_AFTER_SECONDS = 1.0
DEFAULT_PROWLARR_EARLY_RETURN_MIN_RESULTS = 50
DEFAULT_PROWLARR_EARLY_RETURN_REQUIRED_PRIORITY = 10
ANIME_QUERY_HINT_PATTERN = re.compile(
    r"(anime|bangumi|mikan|nyaa|acg|动漫|動畫|动画|番剧|番劇|新番|日漫|"
    r"鬼灭|鬼滅|葬送|芙莉莲|芙莉蓮|海贼|海賊|火影|柯南|进击|進擊|咒术|咒術|"
    r"电锯|電鋸|间谍过家家|間諜家家酒|孤独摇滚|孤獨搖滾|我推|药屋|藥屋|"
    r"高达|高達|宝可梦|寶可夢|名侦探|名偵探|灌篮|灌籃|排球|无职|無職|"
    r"刀剑神域|刀劍神域|fate|re0|re:0|[ぁ-んァ-ン])",
    re.IGNORECASE,
)
SEARCH_PROFILE_GENERAL = "general"
SEARCH_PROFILE_ADULT = "adult"
SEARCH_PROFILE_ANIME = "anime"
SEARCH_PROFILES = (SEARCH_PROFILE_GENERAL, SEARCH_PROFILE_ADULT, SEARCH_PROFILE_ANIME)
SEARCH_PROFILE_CATEGORIES = {
    SEARCH_PROFILE_GENERAL: (2000, 5000),
    SEARCH_PROFILE_ADULT: (6000,),
    SEARCH_PROFILE_ANIME: (2000, 5000),
}
SEARCH_PROFILE_TAG_LABELS = {
    SEARCH_PROFILE_GENERAL: ("media-general", "general"),
    SEARCH_PROFILE_ADULT: ("media-adult", "adult"),
    SEARCH_PROFILE_ANIME: ("media-anime", "anime"),
}
def search_profile_for_query(category, query):
    if category == "adult" or is_strong_adult_code_query(query):
        return SEARCH_PROFILE_ADULT
    if should_search_anime(category, query):
        return SEARCH_PROFILE_ANIME
    return SEARCH_PROFILE_GENERAL


def is_strong_adult_code_query(query):
    codes = sorted(extract_codes(query))
    if len(codes) != 1:
        return False
    code = codes[0]
    compact_query = re.sub(r"[^0-9A-Za-z]+", "", str(query or "")).upper()
    compact_code = re.sub(r"[^0-9A-Za-z]+", "", code).upper()
    return bool(compact_query) and compact_query == compact_code


def safe_prowlarr_tags(prowlarr):
    if not hasattr(prowlarr, "tags"):
        return []
    try:
        return prowlarr.tags()
    except Exception as error:
        print("prowlarr tag load failed: %s" % error, file=sys.stderr)
        return []


def search_profile_indexer_results(
    prowlarr,
    query,
    profile,
    limit,
    indexers=None,
    tags=None,
    timeout_seconds=None,
    stats=None,
    categories_by_profile=None,
    tag_labels_by_profile=None,
    max_workers=DEFAULT_PROWLARR_MAX_WORKERS,
    early_return_after_seconds=DEFAULT_PROWLARR_EARLY_RETURN_AFTER_SECONDS,
    early_return_min_results=DEFAULT_PROWLARR_EARLY_RETURN_MIN_RESULTS,
    early_return_required_priority=DEFAULT_PROWLARR_EARLY_RETURN_REQUIRED_PRIORITY,
):
    if indexers is None:
        indexers = prowlarr.indexers()
    selected = search_profile_indexers(
        indexers,
        tags or [],
        profile,
        categories_by_profile=categories_by_profile,
        tag_labels_by_profile=tag_labels_by_profile,
    )
    categories = search_profile_categories(profile, categories_by_profile)
    if not selected:
        if stats is not None:
            return stats.measure(
                "Prowlarr aggregate",
                lambda: prowlarr.search(query, limit=limit, categories=categories),
                phase="profile_aggregate",
            )
        return prowlarr.search(query, limit=limit, categories=categories)
    return search_indexers_concurrently(
        prowlarr,
        query,
        limit,
        selected,
        categories=categories,
        timeout_seconds=timeout_seconds or DEFAULT_PROWLARR_SEARCH_TIMEOUT_SECONDS,
        stats=stats,
        max_workers=max_workers,
        early_return_after_seconds=early_return_after_seconds,
        early_return_min_results=early_return_min_results,
        early_return_required_priority=early_return_required_priority,
    )


def search_profile_indexers(indexers, tags, profile, categories_by_profile=None, tag_labels_by_profile=None):
    enabled = [indexer for indexer in indexers if indexer_enabled(indexer) and indexer.get("id") is not None]
    tag_ids = search_profile_tag_ids(tags, profile, tag_labels_by_profile=tag_labels_by_profile)
    if tag_ids:
        tagged = [indexer for indexer in enabled if tag_ids.intersection(set(indexer.get("tags") or []))]
        if tagged:
            return tagged
    categories = search_profile_categories(profile, categories_by_profile, default=())
    return [indexer for indexer in enabled if indexer_supports_any_category(indexer, categories)]


def search_profile_categories(profile, categories_by_profile=None, default=None):
    categories_by_profile = categories_by_profile or SEARCH_PROFILE_CATEGORIES
    if default is None:
        default = categories_by_profile.get(SEARCH_PROFILE_GENERAL, SEARCH_PROFILE_CATEGORIES[SEARCH_PROFILE_GENERAL])
    return tuple(categories_by_profile.get(profile, default))


def search_profile_tag_ids(tags, profile, tag_labels_by_profile=None):
    tag_labels_by_profile = tag_labels_by_profile or SEARCH_PROFILE_TAG_LABELS
    labels = {label.casefold() for label in tag_labels_by_profile.get(profile, ())}
    ids = set()
    for tag in tags or []:
        label = str(tag.get("label") or tag.get("name") or "").casefold()
        if label in labels and tag.get("id") is not None:
            ids.add(tag.get("id"))
    return ids


def indexer_supports_any_category(indexer, categories):
    supported = indexer_category_ids(indexer)
    return any(int(category) in supported for category in categories)


def indexer_category_ids(indexer):
    ids = set()

    def visit(category):
        if not isinstance(category, dict):
            return
        category_id = category.get("id")
        try:
            ids.add(int(category_id))
        except (TypeError, ValueError):
            pass
        for child in category.get("subCategories") or []:
            visit(child)

    capabilities = (indexer or {}).get("capabilities") or {}
    for category in capabilities.get("categories") or []:
        visit(category)
    return ids


def indexer_priority_map(indexers):
    priorities = {}
    for indexer in indexers or []:
        indexer_id = indexer.get("id")
        priority = indexer.get("priority")
        try:
            priorities[int(indexer_id)] = int(priority)
        except (TypeError, ValueError):
            pass
    return priorities


def search_indexers_concurrently(
    prowlarr,
    query,
    limit,
    indexers,
    categories=None,
    timeout_seconds=DEFAULT_PROWLARR_SEARCH_TIMEOUT_SECONDS,
    stats=None,
    max_workers=DEFAULT_PROWLARR_MAX_WORKERS,
    early_return_after_seconds=DEFAULT_PROWLARR_EARLY_RETURN_AFTER_SECONDS,
    early_return_min_results=DEFAULT_PROWLARR_EARLY_RETURN_MIN_RESULTS,
    early_return_required_priority=DEFAULT_PROWLARR_EARLY_RETURN_REQUIRED_PRIORITY,
):
    results = []
    if not indexers:
        return results
    max_workers = max(1, min(int(max_workers), len(indexers)))
    executor = ThreadPoolExecutor(max_workers=max_workers)
    future_to_indexer = {
        executor.submit(prowlarr.search, query, limit, [indexer.get("id")], categories): indexer for indexer in indexers
    }
    future_started = {future: time.monotonic() for future in future_to_indexer}
    pending = set(future_to_indexer)
    required_futures = required_priority_futures(future_to_indexer, early_return_required_priority)
    started = time.monotonic()
    deadline = started + max(0, float(timeout_seconds or 0))
    early_after = float(early_return_after_seconds or 0)
    early_deadline = started + early_after if early_after > 0 else None
    early_returned = False

    while pending:
        now = time.monotonic()
        if now >= deadline:
            break
        if should_early_return_indexer_search(
            now,
            early_deadline,
            results,
            pending,
            required_futures,
            early_return_min_results,
        ):
            early_returned = True
            break
        wait_timeout = deadline - now
        if early_deadline and now < early_deadline:
            wait_timeout = min(wait_timeout, early_deadline - now)
        done, pending = wait(pending, timeout=wait_timeout, return_when=FIRST_COMPLETED)
        for future in done:
            collect_indexer_future_result(future, future_to_indexer, future_started, results, stats)
    for future in pending:
        indexer = future_to_indexer[future]
        source = indexer.get("name") or indexer.get("id")
        duration = time.monotonic() - future_started[future]
        if stats is not None:
            if early_returned:
                stats.record(
                    source,
                    status="skipped",
                    duration_seconds=duration,
                    error="early return after enough prioritized results",
                    phase="profile_indexer",
                    indexer_id=indexer.get("id"),
                )
            else:
                stats.record_timeout(source, phase="profile_indexer", indexer_id=indexer.get("id"), duration_seconds=duration)
        if early_returned:
            print("profile indexer search skipped after early return: %s" % source, file=sys.stderr)
        else:
            print("profile indexer search timed out: %s" % source, file=sys.stderr)
        future.cancel()
    executor.shutdown(wait=False, cancel_futures=True)
    return results


def collect_indexer_future_result(future, future_to_indexer, future_started, results, stats=None):
    indexer = future_to_indexer[future]
    source = indexer.get("name") or indexer.get("id")
    duration = time.monotonic() - future_started[future]
    try:
        found = future.result()
        if stats is not None:
            stats.record(source, result_count=len(found or []), duration_seconds=duration, phase="profile_indexer", indexer_id=indexer.get("id"))
        results.extend(found)
    except Exception as error:
        if stats is not None:
            stats.record(source, status="failed", duration_seconds=duration, error=error, phase="profile_indexer", indexer_id=indexer.get("id"))
        print("profile indexer search failed: %s: %s" % (source, error), file=sys.stderr)


def required_priority_futures(future_to_indexer, required_priority):
    try:
        threshold = int(required_priority)
    except (TypeError, ValueError):
        return set()
    if threshold <= 0:
        return set()
    required = set()
    for future, indexer in future_to_indexer.items():
        try:
            priority = int(indexer.get("priority"))
        except (TypeError, ValueError):
            continue
        if priority <= threshold:
            required.add(future)
    return required


def should_early_return_indexer_search(now, early_deadline, results, pending, required_futures, min_results):
    if not early_deadline or now < early_deadline:
        return False
    try:
        required_count = int(min_results)
    except (TypeError, ValueError):
        return False
    if required_count <= 0 or len(results) < required_count:
        return False
    return not required_futures.intersection(pending)


def search_primary_indexer_results(prowlarr, query, limit, indexers=None, stats=None):
    if indexers is None:
        indexers = prowlarr.indexers()
    primary_indexers = [
        indexer
        for indexer in indexers
        if indexer_enabled(indexer)
        and not ResourceSelector.is_anime_specialized_item(indexer)
        and not ResourceSelector.is_sukebei_item(indexer)
        and indexer.get("id") is not None
    ]
    indexer_ids = [indexer.get("id") for indexer in primary_indexers]
    if not indexers:
        return prowlarr.search(query, limit=limit)
    if not indexer_ids:
        return []
    try:
        if stats is not None:
            return stats.measure(
                "primary aggregate",
                lambda: prowlarr.search(query, limit=limit, indexer_ids=indexer_ids),
                phase="primary_aggregate",
            )
        return prowlarr.search(query, limit=limit, indexer_ids=indexer_ids)
    except Exception as error:
        print("primary aggregate indexer search failed: %s" % error, file=sys.stderr)
        return search_primary_indexers_individually(prowlarr, query, limit, primary_indexers, error, stats=stats)


def search_primary_indexers_individually(prowlarr, query, limit, indexers, aggregate_error, stats=None):
    results = []
    attempted = 0
    failures = []
    for indexer in indexers:
        indexer_id = indexer.get("id")
        if indexer_id is None:
            continue
        attempted += 1
        try:
            results.extend(
                search_indexer_with_timeout(
                    prowlarr,
                    query,
                    limit,
                    indexer_id,
                    timeout=DEFAULT_PRIMARY_INDEXER_TIMEOUT_SECONDS,
                    stats=stats,
                    source=indexer.get("name") or indexer_id,
                    phase="primary_indexer",
                )
            )
        except Exception as error:
            failures.append((indexer.get("name") or indexer_id, error))
            print("primary indexer search failed: %s: %s" % (indexer.get("name") or indexer_id, error), file=sys.stderr)
    if attempted and len(failures) == attempted:
        raise RuntimeError("primary aggregate search failed and all primary indexers failed: %s" % aggregate_error)
    return results


def should_search_sukebei(category, query):
    return category == "adult" or is_strong_adult_code_query(query)


def should_search_anime(category, query):
    if category == "adult" or is_strong_adult_code_query(query):
        return False
    if category == "tv" and ANIME_QUERY_HINT_PATTERN.search(str(query or "")):
        return True
    return bool(ANIME_QUERY_HINT_PATTERN.search(str(query or "")))


def indexer_enabled(indexer):
    return (indexer or {}).get("enable", (indexer or {}).get("enabled", True)) is not False


def search_sukebei_indexer_results(prowlarr, query, indexers=None, stats=None):
    return search_required_indexer_results(
        prowlarr,
        query,
        ResourceSelector.is_sukebei_item,
        DEFAULT_REQUIRED_INDEXER_SEARCH_LIMIT,
        indexers=indexers,
        stats=stats,
        phase="sukebei_indexer",
    )


def search_anime_indexer_results(prowlarr, query, indexers=None, stats=None):
    return search_required_indexer_results(
        prowlarr,
        query,
        ResourceSelector.is_anime_specialized_item,
        DEFAULT_ANIME_INDEXER_SEARCH_LIMIT,
        indexers=indexers,
        optional=True,
        timeout=DEFAULT_OPTIONAL_INDEXER_TIMEOUT_SECONDS,
        stats=stats,
        phase="anime_indexer",
    )


def search_required_indexer_results(prowlarr, query, predicate, limit, indexers=None, optional=False, timeout=None, stats=None, phase="required_indexer"):
    results = []
    if indexers is None:
        indexers = prowlarr.indexers()
    for indexer in indexers:
        if not indexer_enabled(indexer):
            continue
        if not predicate(indexer):
            continue
        indexer_id = indexer.get("id")
        if indexer_id is None:
            continue
        try:
            results.extend(
                search_indexer_with_timeout(
                    prowlarr,
                    query,
                    limit,
                    indexer_id,
                    timeout=timeout,
                    stats=stats,
                    source=indexer.get("name") or indexer_id,
                    phase=phase,
                )
            )
        except Exception as error:
            if not optional:
                raise
            print("optional indexer search failed: %s: %s" % (indexer.get("name"), error), file=sys.stderr)
    return results


def search_indexer_with_timeout(prowlarr, query, limit, indexer_id, timeout=None, stats=None, source=None, phase=None):
    def run():
        if timeout is None or not hasattr(prowlarr, "timeout"):
            return prowlarr.search(query, limit=limit, indexer_ids=[indexer_id])
        original_timeout = prowlarr.timeout
        prowlarr.timeout = min(original_timeout, timeout)
        try:
            return prowlarr.search(query, limit=limit, indexer_ids=[indexer_id])
        finally:
            prowlarr.timeout = original_timeout

    if stats is not None:
        return stats.measure(source or indexer_id, run, phase=phase, indexer_id=indexer_id)
    return run()


def share115_candidate_from_text(text):
    parsed = parse_115_share_url(text)
    if parsed is None:
        return None
    download_uri = parsed.url
    if parsed.receive_code:
        parts = urllib.parse.urlsplit(download_uri)
        query = urllib.parse.parse_qsl(parts.query, keep_blank_values=True)
        if not any(key.lower() in {"password", "pwd"} for key, _value in query):
            query.append(("password", parsed.receive_code))
            download_uri = urllib.parse.urlunsplit(
                (parts.scheme, parts.netloc, parts.path, urllib.parse.urlencode(query), parts.fragment)
            )
    return {
        "title": "115分享 %s" % parsed.share_code,
        "download_uri": download_uri,
        "indexer": "115分享",
        "seeders": None,
        "size": None,
        "rank": 1,
        "source_kind": "115_share",
        "resource_type": "115_share",
        "shareCode": parsed.share_code,
    }


def magnet_candidate_from_text(text):
    uri = extract_magnet_uri(text)
    if not uri:
        return None
    info_hash = magnet_info_hash(uri)
    return {
        "title": magnet_title(uri, info_hash),
        "download_uri": uri,
        "indexer": "磁链",
        "seeders": None,
        "size": None,
        "rank": 1,
        "infoHash": info_hash,
        "source_kind": "magnet",
        "resource_type": "magnet",
    }


def extract_magnet_uri(text):
    match = re.search(r"magnet:\?[^\s]+", str(text or ""), re.IGNORECASE)
    if not match:
        return None
    uri = match.group(0).rstrip(".,;，。；)")
    if urllib.parse.urlsplit(uri).scheme.lower() != "magnet":
        return None
    return uri


def magnet_title(uri, info_hash=None):
    title = magnet_display_name(uri)
    if title:
        return title
    if info_hash:
        return "磁链 %s" % info_hash[:12]
    return "磁链"


def magnet_display_name(uri):
    params = urllib.parse.parse_qs(urllib.parse.urlsplit(uri).query)
    display_names = params.get("dn") or []
    for value in display_names:
        title = str(value or "").strip()
        if title:
            return title
    return ""


def magnet_info_hash(uri):
    params = urllib.parse.parse_qs(urllib.parse.urlsplit(uri).query)
    for value in params.get("xt") or []:
        prefix = "urn:btih:"
        if value.lower().startswith(prefix):
            return value[len(prefix) :].strip()
    return None


def valid_btih_info_hash(value):
    return re.fullmatch(r"(?:[0-9A-Fa-f]{40}|[A-Za-z2-7]{32})", str(value or "").strip()) is not None


def parse_csv_ints(value, default):
    if value is None or str(value).strip() == "":
        return tuple(default)
    out = []
    for item in str(value).split(","):
        item = item.strip()
        if item:
            out.append(int(item))
    return tuple(out)


def parse_csv_strings(value, default):
    if value is None or str(value).strip() == "":
        return tuple(default)
    return tuple(item.strip() for item in str(value).split(",") if item.strip())


def parse_int(value, default):
    if value is None or str(value).strip() == "":
        return int(default)
    return int(value)


def search_profile_value(values_by_profile, profile, default):
    values_by_profile = values_by_profile or {}
    if profile in values_by_profile:
        return values_by_profile[profile]
    if SEARCH_PROFILE_GENERAL in values_by_profile:
        return values_by_profile[SEARCH_PROFILE_GENERAL]
    return default


def search_profile_categories_from_env(env):
    return {
        SEARCH_PROFILE_GENERAL: parse_csv_ints(
            env.get("PROWLARR_PROFILE_GENERAL_CATEGORIES"),
            SEARCH_PROFILE_CATEGORIES[SEARCH_PROFILE_GENERAL],
        ),
        SEARCH_PROFILE_ADULT: parse_csv_ints(
            env.get("PROWLARR_PROFILE_ADULT_CATEGORIES"),
            SEARCH_PROFILE_CATEGORIES[SEARCH_PROFILE_ADULT],
        ),
        SEARCH_PROFILE_ANIME: parse_csv_ints(
            env.get("PROWLARR_PROFILE_ANIME_CATEGORIES"),
            SEARCH_PROFILE_CATEGORIES[SEARCH_PROFILE_ANIME],
        ),
    }


def search_profile_tag_labels_from_env(env):
    return {
        SEARCH_PROFILE_GENERAL: parse_csv_strings(
            env.get("PROWLARR_PROFILE_GENERAL_TAG_LABELS"),
            SEARCH_PROFILE_TAG_LABELS[SEARCH_PROFILE_GENERAL],
        ),
        SEARCH_PROFILE_ADULT: parse_csv_strings(
            env.get("PROWLARR_PROFILE_ADULT_TAG_LABELS"),
            SEARCH_PROFILE_TAG_LABELS[SEARCH_PROFILE_ADULT],
        ),
        SEARCH_PROFILE_ANIME: parse_csv_strings(
            env.get("PROWLARR_PROFILE_ANIME_TAG_LABELS"),
            SEARCH_PROFILE_TAG_LABELS[SEARCH_PROFILE_ANIME],
        ),
    }


def search_profile_upstream_limits_from_env(env):
    default_limit = parse_int(env.get("PROWLARR_UPSTREAM_SEARCH_LIMIT"), DEFAULT_UPSTREAM_SEARCH_LIMIT)
    return {
        SEARCH_PROFILE_GENERAL: parse_int(env.get("PROWLARR_PROFILE_GENERAL_UPSTREAM_LIMIT"), default_limit),
        SEARCH_PROFILE_ADULT: parse_int(env.get("PROWLARR_PROFILE_ADULT_UPSTREAM_LIMIT"), default_limit),
        SEARCH_PROFILE_ANIME: parse_int(env.get("PROWLARR_PROFILE_ANIME_UPSTREAM_LIMIT"), default_limit),
    }


def search_profile_timeout_seconds_from_env(env):
    default_timeout = parse_int(env.get("PROWLARR_SEARCH_TIMEOUT_SECONDS"), DEFAULT_PROWLARR_SEARCH_TIMEOUT_SECONDS)
    return {
        SEARCH_PROFILE_GENERAL: parse_int(env.get("PROWLARR_PROFILE_GENERAL_TIMEOUT_SECONDS"), default_timeout),
        SEARCH_PROFILE_ADULT: parse_int(env.get("PROWLARR_PROFILE_ADULT_TIMEOUT_SECONDS"), default_timeout),
        SEARCH_PROFILE_ANIME: parse_int(env.get("PROWLARR_PROFILE_ANIME_TIMEOUT_SECONDS"), default_timeout),
    }


def search_profile_max_workers_from_env(env):
    default_workers = parse_int(env.get("PROWLARR_MAX_WORKERS"), DEFAULT_PROWLARR_MAX_WORKERS)
    return {
        SEARCH_PROFILE_GENERAL: parse_int(env.get("PROWLARR_PROFILE_GENERAL_MAX_WORKERS"), default_workers),
        SEARCH_PROFILE_ADULT: parse_int(env.get("PROWLARR_PROFILE_ADULT_MAX_WORKERS"), default_workers),
        SEARCH_PROFILE_ANIME: parse_int(env.get("PROWLARR_PROFILE_ANIME_MAX_WORKERS"), default_workers),
    }

