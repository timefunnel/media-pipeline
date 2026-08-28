import json
import posixpath
import re

from pipeline.mediastation import extract_codes, extract_years, iter_code_matches
from pipeline.openlist_utils import (
    is_openlist_video_file,
    normalize_openlist_path,
    normalize_openlist_text,
    openlist_item_is_dir,
    openlist_item_name,
    openlist_item_path,
    openlist_item_size,
    replace_openlist_path_prefix,
)
from pipeline.search import ed2k_info_hash, magnet_info_hash
from pipeline.task_state import TASK_STATE


DEDUPE_CATEGORIES = {"movie", "tv", "anime", "adult", "other"}

# Bracketed release attributes are useful for display but are not title
# aliases. Keeping them out of the index prevents a later candidate from
# matching on generic values such as ``1080p`` or ``REMUX``.
DEDUPE_ALIAS_NOISE = {
    "4k",
    "4khdr",
    "8bit",
    "10bit",
    "2160p",
    "1080p",
    "720p",
    "480p",
    "hdr",
    "sdr",
    "dv",
    "uhd",
    "bluray",
    "brrip",
    "webrip",
    "webdl",
    "web",
    "hdtv",
    "remux",
    "h264",
    "h265",
    "hevc",
    "x264",
    "x265",
    "avc",
    "mkv",
    "mp4",
    "avi",
    "mov",
    "aac",
    "ac3",
    "dts",
    "flac",
    "truehd",
    "atmos",
    "chs",
    "cht",
    "eng",
    "sub",
    "dub",
}


def find_local_duplicate(records, category, candidate_record, candidate):
    info_hash = candidate_info_hash(candidate)
    if info_hash:
        for record in records:
            if str(record.get("info_hash") or "").lower() == info_hash.lower():
                if not local_task_blocks_duplicate(record):
                    continue
                duplicate = duplicate_from_task("strong", "same_info_hash", "Bot状态库", record, can_force=False)
                duplicate["identity_type"] = "info_hash"
                duplicate["identity_value"] = info_hash
                return duplicate

    if category == "adult":
        code = first_adult_code([candidate_record.get("query"), candidate.get("title"), candidate.get("download_uri")])
        if code:
            for record in records:
                if record.get("category") != "adult":
                    continue
                if code in task_duplicate_codes(record):
                    if not local_task_blocks_duplicate(record):
                        continue
                    duplicate = duplicate_from_task("strong", "adult_code", "Bot状态库", record, can_force=False)
                    duplicate["code"] = code
                    duplicate["identity_type"] = "adult_code"
                    duplicate["identity_value"] = code
                    return duplicate
    return None


def find_index_duplicate(store, category, candidate_record, candidate):
    identities = candidate_dedupe_identities(category, candidate_record, candidate)
    has_external_ids = bool(candidate_external_ids(candidate))
    for identity in identities:
        if has_external_ids and identity.get("identity_type") == "normalized_title":
            continue
        matches = store.find_dedupe_entries(category, [identity], limit=20)
        for match in matches:
            if identity.get("identity_type") == "normalized_title" and not dedupe_years_compatible(
                candidate_record,
                candidate,
                match,
            ):
                continue
            return duplicate_from_dedupe_entry(identity, match)
    return None


def candidate_dedupe_identities(category, candidate_record, candidate):
    identities = []
    info_hash = candidate_info_hash(candidate)
    if info_hash:
        identities.append({"identity_type": "info_hash", "identity_value": info_hash})

    candidate = candidate or {}
    values = [candidate_record.get("query"), candidate.get("title"), candidate.get("name"), candidate.get("file_name"), candidate.get("download_uri")]
    fields = candidate.get("pansou_fields")
    if isinstance(fields, dict):
        values.extend(fields.get(key) for key in ("title", "filename", "name"))
    if category == "adult":
        code = first_adult_code(values)
        if code:
            identities.append({"identity_type": "adult_code", "identity_value": code})

    alias_values = [candidate_record.get("query"), candidate.get("title"), candidate.get("name"), candidate.get("file_name")]
    if isinstance(fields, dict):
        alias_values.extend(fields.get(key) for key in ("title", "filename", "name"))
    for value in alias_values:
        normalized_title = dedupe_title_identity(value)
        if normalized_title:
            identities.append({"identity_type": "normalized_title", "identity_value": normalized_title})
        for alias in dedupe_title_aliases(value):
            normalized_alias = dedupe_title_identity(alias)
            if normalized_alias:
                identities.append({"identity_type": "normalized_title", "identity_value": normalized_alias})
    return unique_dedupe_identities(identities)


def duplicate_from_dedupe_entry(identity, entry):
    identity_type = identity.get("identity_type")
    level = "weak"
    reason = "openlist_title"
    can_force = True
    if identity_type == "info_hash":
        level = "strong"
        reason = "same_info_hash"
        can_force = False
    elif identity_type == "adult_code":
        level = "strong"
        reason = "adult_code"
        can_force = False
    if entry.get("source") == "openlist":
        level = "weak"
        can_force = True
    duplicate = {
        "level": level,
        "reason": reason,
        "source": dedupe_source_label(entry.get("source")),
        "title": entry.get("title"),
        "path": entry.get("path"),
        "identity_type": identity_type,
        "identity_value": identity.get("identity_value"),
        "can_force": can_force,
    }
    if identity_type == "info_hash":
        duplicate["info_hash"] = identity.get("identity_value")
    if identity_type == "adult_code":
        duplicate["code"] = identity.get("identity_value")
    return duplicate


def local_task_blocks_duplicate(record):
    task = (record or {}).get("task") or {}
    return TASK_STATE.is_offline_active(task) or TASK_STATE.msg_synced(task)


def duplicate_from_task(level, reason, source, record, can_force=False):
    task = record.get("task") or {}
    return {
        "level": level,
        "reason": reason,
        "source": source,
        "title": record.get("title") or task.get("name") or task.get("file_name") or task.get("info_hash"),
        "path": task.get("openlist_adult_format_new_path") or task.get("openlist_adult_format_path") or task.get("openlist_clean_target"),
        "info_hash": task.get("info_hash") or record.get("info_hash"),
        "status_name": task.get("status_name"),
        "msg_sync_status": task.get("msg_sync_status"),
        "msg_media_id": task.get("msg_media_id"),
        "can_force": can_force,
    }


def task_duplicate_codes(record):
    task = record.get("task") or {}
    values = [
        record.get("title"),
        task.get("name"),
        task.get("file_name"),
        task.get("openlist_adult_code"),
        task.get("openlist_adult_format_old_path"),
        task.get("openlist_adult_format_new_path"),
    ]
    codes = set()
    for value in values:
        codes.update(extract_codes(value))
    return codes


def candidate_info_hash(candidate):
    for key in ("infoHash", "info_hash"):
        value = str((candidate or {}).get(key) or "").strip()
        if value:
            return value
    uri = (candidate or {}).get("download_uri") or ""
    return ed2k_info_hash(uri) or magnet_info_hash(uri)


def openlist_dedupe_entries(client, category, root_path):
    entries = []
    for item in openlist_work_items(client, root_path):
        entries.extend(dedupe_entries_from_openlist_work_item(client, category, root_path, item))
    return unique_dedupe_entries(entries)


def openlist_work_items(client, root_path):
    items = []
    for item in client.list_all(root_path, refresh=False):
        if openlist_item_is_dir(item):
            items.append(item)
            continue
        if is_openlist_video_file(item):
            items.append(item)
    return items


def dedupe_entries_from_openlist_work_item(client, category, root_path, item):
    name = openlist_item_name(item)
    path = openlist_item_path(root_path, item)
    if not name:
        return []
    title = name if openlist_item_is_dir(item) else posixpath.splitext(name)[0]
    child_names = openlist_descendant_names(client, root_path, item) if openlist_item_is_dir(item) else []
    base = {
        "category": category,
        "source": "openlist",
        "title": title,
        "path": path,
        "metadata": {
            "is_dir": openlist_item_is_dir(item),
            "size": openlist_item_size(item),
        },
    }
    entries = []

    normalized_title = dedupe_title_identity(name)
    if normalized_title:
        entries.append(
            {
                **base,
                "identity_type": "normalized_title",
                "identity_value": normalized_title,
            }
        )
    for alias in dedupe_title_aliases(name):
        normalized_alias = dedupe_title_identity(alias)
        if normalized_alias and normalized_alias != normalized_title:
            entries.append(
                {
                    **base,
                    "identity_type": "normalized_title",
                    "identity_value": normalized_alias,
                }
            )

    info_hash = str((item or {}).get("info_hash") or (item or {}).get("infoHash") or "").strip()
    if info_hash:
        entries.append(
            {
                **base,
                "identity_type": "info_hash",
                "identity_value": info_hash,
            }
        )

    if category == "adult":
        code_text = " ".join([name, path] + child_names)
        for code in sorted(extract_codes(code_text)):
            entries.append(
                {
                    **base,
                    "identity_type": "adult_code",
                    "identity_value": code,
                }
            )
    return entries


def openlist_descendant_names(client, dir_path, item):
    if not openlist_item_is_dir(item):
        return []
    names = []
    child_dir = openlist_item_path(dir_path, item)
    for child in client.list_all(child_dir, refresh=False):
        child_name = openlist_item_name(child)
        if child_name:
            names.append(child_name)
        if openlist_item_is_dir(child):
            names.extend(openlist_descendant_names(client, child_dir, child))
    return names


def unique_dedupe_entries(entries):
    out = []
    seen = set()
    for entry in entries or []:
        normalized = normalize_dedupe_entry(entry)
        key = (
            normalized["category"],
            normalized["source"],
            normalized["identity_type"],
            normalized["identity_value"],
        )
        if key in seen:
            continue
        seen.add(key)
        out.append(normalized)
    return out


def unique_dedupe_identities(identities):
    out = []
    seen = set()
    for identity in identities or []:
        identity_type = str((identity or {}).get("identity_type") or "").strip()
        identity_value = normalize_dedupe_identity_value(identity_type, (identity or {}).get("identity_value"))
        key = (identity_type, identity_value)
        if not identity_type or not identity_value or key in seen:
            continue
        seen.add(key)
        out.append({"identity_type": identity_type, "identity_value": identity_value})
    return out


def normalize_dedupe_entry(entry, default_source=None):
    entry = entry or {}
    category = str(entry.get("category") or "").strip()
    if category not in DEDUPE_CATEGORIES:
        raise ValueError("invalid dedupe category: %s" % (category or "-"))
    source = str(entry.get("source") or default_source or "").strip()
    if not source:
        raise ValueError("dedupe source must not be empty")
    if default_source and source != default_source:
        raise ValueError("dedupe source mismatch: %s" % source)
    identity_type = str(entry.get("identity_type") or "").strip()
    identity_value = normalize_dedupe_identity_value(identity_type, entry.get("identity_value"))
    if not identity_type or not identity_value:
        raise ValueError("dedupe identity must not be empty")
    return {
        "category": category,
        "source": source,
        "identity_type": identity_type,
        "identity_value": identity_value,
        "title": str(entry.get("title") or "").strip(),
        "path": str(entry.get("path") or "").strip(),
        "metadata": entry.get("metadata") or {},
    }


def normalize_dedupe_identity_value(identity_type, value):
    raw = str(value or "").strip()
    if not raw:
        return ""
    if identity_type == "info_hash":
        return raw.lower()
    if identity_type == "adult_code":
        return first_adult_code([raw]) or raw.upper()
    if identity_type == "normalized_title":
        return dedupe_title_identity(raw)
    if identity_type == "openlist_path":
        return raw
    return raw


def dedupe_title_identity(value):
    text = str(value or "").strip()
    if not text:
        return ""
    stem = posixpath.splitext(text)[0]
    normalized = normalize_openlist_text(stem)
    if len(normalized) < 4:
        return ""
    return normalized


def dedupe_title_aliases(value):
    text = str(value or "")
    aliases = []
    for match in re.finditer(r"[\[\(（【]([^\]\)）】]{2,120})[\]\)）】]", text):
        alias = match.group(1).strip()
        if (
            alias
            and not is_dedupe_alias_noise(alias)
            and not extract_codes(alias)
        ):
            aliases.append(alias)
    return unique_text_values(aliases)


def candidate_external_ids(candidate):
    candidate = candidate or {}
    fields = candidate.get("pansou_fields")
    fields = fields if isinstance(fields, dict) else {}
    values = {
        "tmdb": candidate.get("tmdb_id") or candidate.get("tmdbId") or candidate.get("tm_db_id") or fields.get("tmdb"),
        "douban": candidate.get("douban_id") or candidate.get("doubanId") or fields.get("douban"),
        "bangumi": candidate.get("bangumi_id") or candidate.get("bangumiId") or fields.get("bangumi"),
        "thetvdb": candidate.get("thetvdb_id") or candidate.get("thetvdbId") or fields.get("thetvdb"),
    }
    return {key: str(value).strip() for key, value in values.items() if str(value or "").strip()}


def is_dedupe_alias_noise(value):
    normalized = normalize_openlist_text(value)
    if not normalized:
        return True
    if normalized in DEDUPE_ALIAS_NOISE or re.fullmatch(r"(?:19|20)\d{2}", normalized):
        return True

    # A single bracket can contain several release attributes, for example
    # ``[1080p REMUX x265]``. Treat it as noise only when every token is known
    # release metadata, preserving real aliases that happen to include a tag.
    tokens = re.findall(r"[a-z0-9]+", str(value or "").casefold())
    return bool(tokens) and all(
        token in DEDUPE_ALIAS_NOISE or re.fullmatch(r"(?:19|20)\d{2}", token)
        for token in tokens
    )


def unique_text_values(values):
    seen = set()
    out = []
    for value in values:
        text = str(value or "").strip()
        key = text.casefold()
        if text and key not in seen:
            seen.add(key)
            out.append(text)
    return out


def dedupe_years_compatible(candidate_record, candidate, entry):
    candidate_year = first_title_year(
        (candidate or {}).get("title"),
        (candidate or {}).get("name"),
        (candidate or {}).get("file_name"),
        (candidate_record or {}).get("query"),
    )
    entry_year = first_title_year((entry or {}).get("title"), (entry or {}).get("path"))
    return not candidate_year or not entry_year or candidate_year == entry_year


def first_title_year(*values):
    for value in values:
        years = sorted(extract_years(value))
        if years:
            return years[0]
    return None


def dedupe_source_label(source):
    if source == "openlist":
        return "OpenList基线"
    if source == "bot":
        return "Bot状态库"
    if source == "msg":
        return "MediaStationGo"
    return source or "重复索引"


def first_adult_code(values):
    seen = set()
    for value in values:
        for code in iter_code_matches(value):
            key = code.lower()
            if key not in seen:
                seen.add(key)
                return code
    return None


def dedupe_entry_from_row(row):
    metadata = {}
    if row[6]:
        metadata = json.loads(row[6])
    return {
        "category": row[0],
        "source": row[1],
        "identity_type": row[2],
        "identity_value": row[3],
        "title": row[4],
        "path": row[5],
        "metadata": metadata,
        "created_at": row[7],
        "updated_at": row[8],
    }
