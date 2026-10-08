"""按源共享的风控熔断；缓存优先，冷却持久化，恢复时只放行一个业务请求。"""

import hashlib
import json
import logging
import math
import threading
import time

SOURCE_COOLDOWN_SECONDS = 30 * 60
logger = logging.getLogger(__name__)


class SourceRiskControlError(RuntimeError):
    code = "danmaku_source_risk_control"

    def __init__(self, reason="upstream_risk_control"):
        self.reason = reason if reason in ("http_403", "http_429", "iqiyi_search_risk_control", "explicit_risk_control") else "upstream_risk_control"
        super().__init__("%s: %s" % (self.code, self.reason))


class SourceCircuitOpenError(RuntimeError):
    code = "danmaku_source_circuit_open"

    def __init__(self, source, remaining, probing=False):
        self.retry_after_seconds = max(1, math.ceil(remaining))
        self.state = "half_open" if probing else "open"
        super().__init__("%s: source=%s state=%s retry_after_seconds=%s" %
                         (self.code, source, self.state, self.retry_after_seconds))


class SourceCircuitBreaker:
    def __init__(self, cache, clock=None):
        self.cache = cache
        self.clock = clock or time.time
        self._lock = threading.Lock()
        self._states = {}

    def _state(self, identity):
        key = "source_circuit_v1_" + hashlib.sha256(json.dumps(identity, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        if key not in self._states:
            body = self.cache.load(key, 0)
            if body is None:
                if self.cache._path(key).exists():
                    raise RuntimeError("invalid persisted danmaku source circuit state")
                body = {"opened_until": 0, "reason": ""}
            if not isinstance(body, dict):
                raise RuntimeError("invalid persisted danmaku source circuit state")
            until = body.get("opened_until")
            if isinstance(until, bool) or not isinstance(until, (int, float)) or not math.isfinite(until) or until < 0:
                raise RuntimeError("invalid persisted danmaku source circuit deadline")
            self._states[key] = {"opened_until": until, "reason": body.get("reason", ""), "generation": 0, "probing": False}
        return key, self._states[key]

    def _save(self, key, state):
        self.cache.save(key, {"opened_until": state["opened_until"], "reason": state["reason"]})

    def check(self, identity):
        """桥接排队后再次检查，已排队但尚未发出的请求也不能穿过新熔断。"""
        with self._lock:
            _, state = self._state(identity)
            remaining = state["opened_until"] - self.clock()
            if remaining > 0:
                raise SourceCircuitOpenError(identity["name"], remaining)

    def call(self, identity, loader):
        with self._lock:
            key, state = self._state(identity)
            remaining = state["opened_until"] - self.clock()
            if remaining > 0 or state["probing"]:
                raise SourceCircuitOpenError(identity["name"], remaining, state["probing"])
            probe = state["opened_until"] > 0
            state["probing"] = probe
            generation = state["generation"]
        try:
            result = loader()
        except Exception as exc:
            with self._lock:
                if isinstance(exc, SourceRiskControlError) or (probe and generation == state["generation"] and not isinstance(exc, ValueError)):
                    state.update(opened_until=self.clock() + SOURCE_COOLDOWN_SECONDS,
                                 reason=exc.reason if isinstance(exc, SourceRiskControlError) else "probe_failed",
                                 generation=state["generation"] + 1, probing=False)
                    self._save(key, state)
                    logger.warning("danmaku source circuit opened: source=%s reason=%s cooldown_seconds=%s",
                                   identity["name"], state["reason"], SOURCE_COOLDOWN_SECONDS)
                elif probe and generation == state["generation"]:
                    # 无效客户端参数/未知节目引用没有发出源请求，不能据此延长源风控。
                    state["probing"] = False
            raise
        with self._lock:
            # 旧的在途成功不得清除另一个请求刚触发的熔断。
            if probe and generation == state["generation"]:
                state.update(opened_until=0, reason="", generation=generation + 1, probing=False)
                self._save(key, state)
                logger.info("danmaku source circuit recovered: source=%s", identity["name"])
        return result
