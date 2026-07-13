import posixpath
import re


DEFAULT_VIDEO_EXTENSIONS = {
    ".avi",
    ".flv",
    ".m2ts",
    ".m4v",
    ".mkv",
    ".mov",
    ".mp4",
    ".mpeg",
    ".mpg",
    ".rmvb",
    ".ts",
    ".vob",
    ".webm",
    ".wmv",
}
MSG_CLOUD_PREFIX = "cloud://openlist"


def is_openlist_video_file(item):
    if openlist_item_is_dir(item):
        return False
    return posixpath.splitext(openlist_item_name(item))[1].lower() in DEFAULT_VIDEO_EXTENSIONS


def openlist_item_path(dir_path, item):
    name = openlist_item_name(item)
    if not name:
        return dir_path
    return posixpath.join(str(dir_path).rstrip("/") or "/", name)


def openlist_item_name(item):
    if not isinstance(item, dict):
        return ""
    return str(item.get("name") or item.get("file_name") or item.get("filename") or "").strip()


def openlist_item_is_dir(item):
    if not isinstance(item, dict):
        return False
    if item.get("is_dir") is not None:
        return bool(item.get("is_dir"))
    return item.get("type") == 1


def openlist_item_size(item):
    if not isinstance(item, dict):
        return 0
    try:
        return int(item.get("size") or item.get("size_bytes") or item.get("sizeBytes") or 0)
    except (TypeError, ValueError):
        return 0


def normalize_openlist_path(path):
    raw = str(path or "").strip().replace("\\", "/")
    if not raw:
        return ""
    if not raw.startswith("/"):
        raw = "/" + raw
    normalized = posixpath.normpath(raw)
    if normalized == ".":
        return ""
    return normalized


def openlist_path_to_cloud_path(path):
    return MSG_CLOUD_PREFIX + normalize_openlist_path(path)


def replace_openlist_path_prefix(path, old_prefix, new_prefix):
    path = normalize_openlist_path(path)
    old_prefix = normalize_openlist_path(old_prefix).rstrip("/")
    new_prefix = normalize_openlist_path(new_prefix).rstrip("/")
    if path == old_prefix:
        return new_prefix
    if path.startswith(old_prefix + "/"):
        return new_prefix + path[len(old_prefix) :]
    raise ValueError("path does not start with source prefix: %s" % path)


def normalize_openlist_text(value):
    return re.sub(r"[^0-9A-Za-z\u4e00-\u9fff]+", "", str(value or "")).lower()
