import copy
import json
import os
import posixpath


PLACEHOLDER_PREFIX = "REPLACE_WITH_"


DEFAULT_CATEGORY_CONFIG = {
    "movie": {
        "folder_id": "REPLACE_WITH_115_MOVIE_CID",
        "openlist_path": "/115/\u7535\u5f71",
        "msg": {
            "library_id": "REPLACE_WITH_MSG_MOVIE_LIBRARY_ID",
            "root_id": "REPLACE_WITH_MSG_MOVIE_ROOT_ID",
            "provider": "tmdb",
            "media_type": "movie",
            "scrape_enabled": True,
        },
    },
    "tv": {
        "folder_id": "REPLACE_WITH_115_TV_CID",
        "openlist_path": "/115/\u5267\u96c6",
        "msg": {
            "library_id": "REPLACE_WITH_MSG_TV_LIBRARY_ID",
            "root_id": "REPLACE_WITH_MSG_TV_ROOT_ID",
            "provider": "tmdb",
            "media_type": "tv",
            "scrape_enabled": True,
        },
    },
    "anime": {
        "folder_id": "REPLACE_WITH_115_ANIME_CID",
        "openlist_path": "/115/\u52a8\u6f2b",
        "msg": {
            "library_id": "REPLACE_WITH_MSG_ANIME_LIBRARY_ID",
            "root_id": "REPLACE_WITH_MSG_ANIME_ROOT_ID",
            "provider": "tmdb",
            "media_type": "anime",
            "scrape_enabled": True,
        },
    },
    "adult": {
        "folder_id": "REPLACE_WITH_115_ADULT_CID",
        "openlist_path": "/115/\u6210\u4eba",
        "msg": {
            "library_id": "REPLACE_WITH_MSG_ADULT_LIBRARY_ID",
            "root_id": "REPLACE_WITH_MSG_ADULT_ROOT_ID",
            "provider": "adult",
            "media_type": "adult",
            "scrape_enabled": True,
        },
    },
    "other": {
        "folder_id": "REPLACE_WITH_115_OTHER_CID",
        "openlist_path": "/115/\u5176\u4ed6",
        "msg": {
            "library_id": "REPLACE_WITH_MSG_OTHER_LIBRARY_ID",
            "root_id": "REPLACE_WITH_MSG_OTHER_ROOT_ID",
            "provider": "tmdb",
            "media_type": "movie",
            "scrape_enabled": False,
        },
    },
}

CATEGORY_ENV_PREFIXES = {
    "movie": "MEDIA_PIPELINE_MOVIE",
    "tv": "MEDIA_PIPELINE_TV",
    "anime": "MEDIA_PIPELINE_ANIME",
    "adult": "MEDIA_PIPELINE_ADULT",
    "other": "MEDIA_PIPELINE_OTHER",
}


def load_category_config(env=None):
    env = env if env is not None else os.environ
    config = copy.deepcopy(DEFAULT_CATEGORY_CONFIG)

    config_path = str(env.get("MEDIA_PIPELINE_LIBRARY_CONFIG") or "").strip()
    inline_json = str(env.get("MEDIA_PIPELINE_LIBRARY_CONFIG_JSON") or "").strip()
    if config_path and inline_json:
        raise RuntimeError("set either MEDIA_PIPELINE_LIBRARY_CONFIG or MEDIA_PIPELINE_LIBRARY_CONFIG_JSON, not both")
    if config_path:
        _apply_category_config(config, _read_category_config_file(config_path))
    if inline_json:
        _apply_category_config(config, _decode_category_config(inline_json, "MEDIA_PIPELINE_LIBRARY_CONFIG_JSON"))

    _apply_category_env_overrides(config, env)
    _validate_category_config(config)
    return config


def category_maps(category_config=None):
    config = category_config or load_category_config()
    folder_ids = {}
    openlist_paths = {}
    msg_roots = {}
    for category, values in config.items():
        folder_ids[category] = values["folder_id"]
        openlist_paths[category] = values["openlist_path"]
        msg_roots[category] = dict(values["msg"])
    return folder_ids, openlist_paths, msg_roots


def _read_category_config_file(path):
    if not os.path.exists(path):
        raise RuntimeError("MEDIA_PIPELINE_LIBRARY_CONFIG not found: %s" % path)
    with open(path, "r", encoding="utf-8") as handle:
        return _decode_category_config(handle.read(), path)


def _decode_category_config(raw, source):
    try:
        config = json.loads(raw)
    except ValueError as exc:
        raise RuntimeError("invalid library config JSON in %s" % source) from exc
    if not isinstance(config, dict):
        raise RuntimeError("library config must be a JSON object: %s" % source)
    return config


def _apply_category_config(config, overrides):
    for category, values in (overrides or {}).items():
        category = str(category or "").strip()
        if not category:
            raise RuntimeError("library config category must not be empty")
        if not isinstance(values, dict):
            raise RuntimeError("library config for %s must be an object" % category)
        target = config.setdefault(category, {"msg": {}})
        msg = target.setdefault("msg", {})

        for source_key, target_key in (
            ("folder_id", "folder_id"),
            ("folderId", "folder_id"),
            ("openlist_path", "openlist_path"),
            ("openlistPath", "openlist_path"),
        ):
            if values.get(source_key) not in (None, ""):
                target[target_key] = str(values[source_key]).strip()

        nested_msg = values.get("msg") if isinstance(values.get("msg"), dict) else {}
        flat_msg = {
            "library_id": values.get("msg_library_id") or values.get("library_id") or values.get("libraryId"),
            "root_id": values.get("msg_root_id") or values.get("root_id") or values.get("rootId"),
            "provider": values.get("provider"),
            "media_type": values.get("media_type") or values.get("mediaType"),
            "scrape_enabled": values["scrape_enabled"]
            if "scrape_enabled" in values
            else values.get("scrapeEnabled"),
        }
        for key, value in nested_msg.items():
            if key in ("library_id", "libraryId"):
                flat_msg["library_id"] = value
            elif key in ("root_id", "rootId"):
                flat_msg["root_id"] = value
            elif key in ("provider", "media_type", "mediaType"):
                flat_msg["media_type" if key == "mediaType" else key] = value
            elif key in ("scrape_enabled", "scrapeEnabled"):
                flat_msg["scrape_enabled"] = value

        for key, value in flat_msg.items():
            if key == "scrape_enabled" and value not in (None, ""):
                msg[key] = _category_config_bool(value, "%s.msg.scrape_enabled" % category)
            elif value not in (None, ""):
                msg[key] = str(value).strip()


def _apply_category_env_overrides(config, env):
    for category, prefix in CATEGORY_ENV_PREFIXES.items():
        values = {}
        for env_key, config_key in (
            (prefix + "_FOLDER_ID", "folder_id"),
            (prefix + "_OPENLIST_PATH", "openlist_path"),
            (prefix + "_MSG_LIBRARY_ID", "msg_library_id"),
            (prefix + "_MSG_ROOT_ID", "msg_root_id"),
            (prefix + "_MSG_PROVIDER", "provider"),
            (prefix + "_MSG_MEDIA_TYPE", "media_type"),
            (prefix + "_MSG_SCRAPE_ENABLED", "scrape_enabled"),
        ):
            value = str(env.get(env_key) or "").strip()
            if value:
                values[config_key] = value
        if values:
            _apply_category_config(config, {category: values})


def _validate_category_config(config):
    for category, values in sorted((config or {}).items()):
        if not isinstance(values, dict):
            raise RuntimeError("library config for %s must be an object" % category)
        msg = values.get("msg")
        if not isinstance(msg, dict):
            raise RuntimeError("library config for %s must include msg object" % category)
        for key in ("folder_id", "openlist_path"):
            if not str(values.get(key) or "").strip():
                raise RuntimeError("library config for %s missing %s" % (category, key))
        for key in ("library_id", "root_id", "provider", "media_type"):
            if not str(msg.get(key) or "").strip():
                raise RuntimeError("library config for %s missing msg.%s" % (category, key))
        msg["scrape_enabled"] = _category_config_bool(
            msg.get("scrape_enabled", True), "%s.msg.scrape_enabled" % category
        )


def _category_config_bool(value, label):
    if isinstance(value, bool):
        return value
    normalized = str(value or "").strip().lower()
    if normalized in ("1", "true", "yes", "on"):
        return True
    if normalized in ("0", "false", "no", "off"):
        return False
    raise RuntimeError("invalid boolean for %s: %s" % (label, value))


FOLDER_IDS, OPENLIST_PATHS, MSG_LIBRARY_ROOTS = category_maps()


def category_to_folder_id(category):
    try:
        return FOLDER_IDS[category]
    except KeyError:
        raise ValueError("unsupported category: %s" % category)


def category_to_openlist_path(category):
    try:
        return OPENLIST_PATHS[category]
    except KeyError:
        raise ValueError("unsupported category: %s" % category)


def category_to_msg_library_root(category):
    try:
        root = dict(MSG_LIBRARY_ROOTS[category])
    except KeyError:
        raise ValueError("unsupported category: %s" % category)
    if msg_library_root_needs_discovery(root):
        raise RuntimeError(
            "MediaStationGo root ids missing for %s; configure MEDIA_PIPELINE_%s_MSG_LIBRARY_ID and MEDIA_PIPELINE_%s_MSG_ROOT_ID"
            % (category, category.upper(), category.upper())
        )
    return root


def msg_library_roots():
    roots = {}
    for category in sorted(MSG_LIBRARY_ROOTS.keys()):
        roots[category] = category_to_msg_library_root(category)
    return roots


def msg_library_root_needs_discovery(root):
    return is_placeholder_or_empty(root.get("library_id")) or is_placeholder_or_empty(root.get("root_id"))


def is_placeholder_or_empty(value):
    value = str(value or "").strip()
    return not value or value.startswith(PLACEHOLDER_PREFIX)


def normalize_openlist_path(path):
    raw = str(path or "").strip().replace("\\", "/")
    if not raw:
        return ""
    if not raw.startswith("/"):
        raw = "/" + raw
    normalized = posixpath.normpath(raw)
    return "" if normalized == "." else normalized
