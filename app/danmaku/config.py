"""独立服务配置。DANMAKU_PROVIDERS 的顺序就是实际查询优先级。"""

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass
class Config:
    token: str = ""
    host: str = "127.0.0.1"
    port: int = 9322
    danmaku_enabled: bool = True
    danmaku_providers: tuple = ("tencent", "iqiyi", "youku")
    danmaku_dandanplay_enabled: bool = False
    danmaku_cache_dir: str = "/danmaku-cache"
    danmaku_cache_ttl_seconds: int = 604800
    danmaku_search_cache_ttl_seconds: int = 86400
    danmaku_negative_cache_ttl_seconds: int = 3600
    danmaku_search_timeout_seconds: int = 30
    danmaku_comment_timeout_seconds: int = 90
    danmaku_segment_concurrency: int = 6
    danmaku_max_comments: int = 6000
    danmaku_import_max_bytes: int = 8388608
    danmaku_blacklist: tuple = ()
    danmaku_proxy_url: str = ""
    danmaku_bridge: str = str(Path(__file__).resolve().parents[2] / "danmaku-server" / "bridge.mjs")
    dandanplay_base_url: str = "https://api.dandanplay.net"
    dandanplay_app_id: str = ""
    dandanplay_app_secret: str = ""
    danmaku_aggregator_url: str = ""
    danmaku_prewarm_delay_seconds: float = 2.0
    danmaku_prewarm_max_episodes: int = 50

    @classmethod
    def from_env(cls, env=None):
        env = os.environ if env is None else env
        config = cls()
        for name, raw in env.items():
            attribute = {"DANMAKU_SERVER_TOKEN": "token", "DANMAKU_SERVER_HOST": "host", "DANMAKU_SERVER_PORT": "port"}.get(name, name.lower())
            if not hasattr(config, attribute) or attribute == "danmaku_enabled":
                continue
            current = getattr(config, attribute)
            if isinstance(current, tuple):
                value = tuple(part.strip() for part in str(raw).split(",") if part.strip())
            elif isinstance(current, bool):
                if str(raw).strip().lower() not in ('true', 'false', '1', '0'):
                    raise ValueError(name + ' must be true or false')
                value = str(raw).strip().lower() in ('true', '1')
            elif isinstance(current, int):
                value = int(raw)
            elif isinstance(current, float):
                value = float(raw)
            else:
                value = str(raw).strip()
            setattr(config, attribute, value)
        if not config.token or len(config.token) < 16:
            raise ValueError("DANMAKU_SERVER_TOKEN must contain at least 16 characters")
        if not config.danmaku_cache_dir:
            raise ValueError('DANMAKU_CACHE_DIR must not be empty')
        if not config.danmaku_providers or len(set(config.danmaku_providers)) != len(config.danmaku_providers):
            raise ValueError("DANMAKU_PROVIDERS must be a nonempty unique ordered list")
        if any(name not in ("tencent", "iqiyi", "youku", "dandanplay", "aggregator") for name in config.danmaku_providers):
            raise ValueError("unsupported DANMAKU_PROVIDERS entry")
        if "dandanplay" in config.danmaku_providers:
            if not config.danmaku_dandanplay_enabled:
                raise ValueError("dandanplay is disabled; remove it from DANMAKU_PROVIDERS")
            if not (config.dandanplay_app_id and config.dandanplay_app_secret):
                raise ValueError("dandanplay requires DANDANPLAY_APP_ID and DANDANPLAY_APP_SECRET")
        if "aggregator" in config.danmaku_providers and not config.danmaku_aggregator_url:
            raise ValueError("aggregator requires DANMAKU_AGGREGATOR_URL")
        for name in ("danmaku_cache_ttl_seconds", "danmaku_search_cache_ttl_seconds", "danmaku_negative_cache_ttl_seconds", "danmaku_search_timeout_seconds",
                     "danmaku_comment_timeout_seconds", "danmaku_max_comments", "danmaku_import_max_bytes", "danmaku_prewarm_max_episodes"):
            if getattr(config, name) <= 0:
                raise ValueError(name + " must be positive")
        if not 1 <= config.port <= 65535 or config.danmaku_prewarm_delay_seconds < 0:
            raise ValueError("invalid server port or prewarm delay")
        if not 1 <= config.danmaku_segment_concurrency <= 8:
            raise ValueError("DANMAKU_SEGMENT_CONCURRENCY must be between 1 and 8")
        return config
