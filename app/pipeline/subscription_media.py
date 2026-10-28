import posixpath


def is_subscription_format_pair(names):
    """Keep an MKV/MP4 pair of the same release as two files for one episode."""
    if len(names) != 2:
        return False
    parts = [posixpath.splitext(str(name or "").casefold()) for name in names]
    return bool(parts[0][0]) and parts[0][0] == parts[1][0] and {
        extension for _stem, extension in parts
    } == {".mkv", ".mp4"}
