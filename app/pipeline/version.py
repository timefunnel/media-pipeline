import os


APP_NAME = "media-pipeline"
DEFAULT_APP_VERSION = "0.1.0"
DEFAULT_APP_REVISION = "unknown"


def get_version_info(env=None):
    env = env if env is not None else os.environ
    version = str(env.get("MEDIA_PIPELINE_VERSION") or DEFAULT_APP_VERSION).strip() or DEFAULT_APP_VERSION
    revision = str(env.get("MEDIA_PIPELINE_REVISION") or DEFAULT_APP_REVISION).strip() or DEFAULT_APP_REVISION
    return {
        "name": APP_NAME,
        "version": version,
        "revision": revision,
    }


def format_version_info(info=None):
    info = info or get_version_info()
    return "%s %s\nrevision: %s" % (
        info.get("name") or APP_NAME,
        info.get("version") or DEFAULT_APP_VERSION,
        info.get("revision") or DEFAULT_APP_REVISION,
    )
