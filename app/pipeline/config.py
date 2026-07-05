import copy
import json
import os


DEFAULT_CATEGORY_CONFIG = {
    "movie": {
        "folder_id": "3464134653584082023",
        "openlist_path": "/115/\u7535\u5f71",
        "msg": {
            "library_id": "d150a96c-b467-4c60-82f1-207ae5949045",
            "root_id": "0c1dda42-29ef-4069-b051-c9549a8d4440",
            "provider": "tmdb",
            "media_type": "movie",
        },
    },
    "tv": {
        "folder_id": "3465137076394001831",
        "openlist_path": "/115/\u5267\u96c6",
        "msg": {
            "library_id": "b6c58f40-76dc-46b5-8f27-9e74d22e5e3d",
            "root_id": "3d2e0cb4-3537-4f7d-8d79-9d4d5f1800df",
            "provider": "tmdb",
            "media_type": "tv",
        },
    },
    "anime": {
        "folder_id": "3465784028030830531",
        "openlist_path": "/115/\u52a8\u6f2b",
        "msg": {
            "library_id": "e1333358-17ff-4b90-82f0-663cec26c0df",
            "root_id": "fc7058d6-0b32-4536-bb92-4755c488be55",
            "provider": "tmdb",
            "media_type": "anime",
        },
    },
    "adult": {
        "folder_id": "3464134590896014943",
        "openlist_path": "/115/\u6210\u4eba",
        "msg": {
            "library_id": "26768071-73bb-4b5c-85f3-ad0dd84f9fd9",
            "root_id": "3fe479e8-4a96-4e61-9f69-fa802e448446",
            "provider": "adult",
            "media_type": "adult",
        },
    },
    "other": {
        "folder_id": "3465205291639899794",
        "openlist_path": "/115/\u5176\u4ed6",
        "msg": {
            "library_id": "60067bc7-eb34-466c-8bf9-5654297a609f",
            "root_id": "1f889ec1-b34d-40b6-b3ca-f4372170a42b",
            "provider": "tmdb",
            "media_type": "movie",
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
        }
        for key, value in nested_msg.items():
            if key in ("library_id", "libraryId"):
                flat_msg["library_id"] = value
            elif key in ("root_id", "rootId"):
                flat_msg["root_id"] = value
            elif key in ("provider", "media_type", "mediaType"):
                flat_msg["media_type" if key == "mediaType" else key] = value

        for key, value in flat_msg.items():
            if value not in (None, ""):
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
        return dict(MSG_LIBRARY_ROOTS[category])
    except KeyError:
        raise ValueError("unsupported category: %s" % category)
