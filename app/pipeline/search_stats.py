import time


class SearchResultList(list):
    def __init__(self, values=None, metadata=None):
        super().__init__(values or [])
        self.metadata = metadata or {}


class SearchStats:
    def __init__(self, clock=None):
        self.clock = clock or time.monotonic
        self.started_at = self.clock()
        self.sources = []

    def record(self, source, result_count=0, status="success", duration_seconds=0, error=None, phase=None, indexer_id=None):
        entry = {
            "source": str(source or "-"),
            "status": str(status or "success"),
            "result_count": int(result_count or 0),
            "duration_ms": int(round(float(duration_seconds or 0) * 1000)),
        }
        if phase:
            entry["phase"] = str(phase)
        if indexer_id is not None:
            entry["indexer_id"] = indexer_id
        if error:
            entry["error"] = str(error)
        self.sources.append(entry)

    def measure(self, source, callback, phase=None, indexer_id=None):
        started = self.clock()
        try:
            results = callback()
        except Exception as exc:
            self.record(
                source,
                status="failed",
                duration_seconds=self.clock() - started,
                error=exc,
                phase=phase,
                indexer_id=indexer_id,
            )
            raise
        self.record(
            source,
            result_count=len(results or []),
            duration_seconds=self.clock() - started,
            phase=phase,
            indexer_id=indexer_id,
        )
        return results

    def record_timeout(self, source, phase=None, indexer_id=None, duration_seconds=0):
        self.record(source, status="timeout", duration_seconds=duration_seconds, phase=phase, indexer_id=indexer_id)

    def to_metadata(self, profile=None, raw_count=0, selected_count=0, settings=None):
        total_ms = int(round((self.clock() - self.started_at) * 1000))
        success_count = sum(1 for source in self.sources if source.get("status") == "success")
        failed_count = sum(1 for source in self.sources if source.get("status") == "failed")
        timeout_count = sum(1 for source in self.sources if source.get("status") == "timeout")
        result_count = sum(int(source.get("result_count") or 0) for source in self.sources)
        metadata = {
            "profile": profile or "",
            "total_ms": total_ms,
            "source_count": len(self.sources),
            "success_count": success_count,
            "failed_count": failed_count,
            "timeout_count": timeout_count,
            "raw_count": int(raw_count or result_count),
            "selected_count": int(selected_count or 0),
            "sources": list(self.sources),
        }
        if settings:
            metadata["settings"] = dict(settings)
        return metadata


def search_result_metadata(results):
    return getattr(results, "metadata", {}) or {}


def attach_search_metadata(exc, metadata):
    try:
        setattr(exc, "search_metadata", metadata or {})
    except Exception:
        pass
    return exc


def exception_search_metadata(exc):
    return getattr(exc, "search_metadata", {}) or {}


def format_search_stats(metadata):
    metadata = metadata or {}
    if not metadata:
        return ""
    source_count = int(metadata.get("source_count") or 0)
    total_ms = int(metadata.get("total_ms") or 0)
    raw_count = int(metadata.get("raw_count") or 0)
    selected_count = int(metadata.get("selected_count") or 0)
    failed_count = int(metadata.get("failed_count") or 0)
    timeout_count = int(metadata.get("timeout_count") or 0)
    parts = ["来源%s个" % source_count, "耗时%.1fs" % (total_ms / 1000.0), "返回%s条" % raw_count]
    if selected_count:
        parts.append("展示%s条" % selected_count)
    if failed_count:
        parts.append("失败%s个" % failed_count)
    if timeout_count:
        parts.append("超时%s个" % timeout_count)
    return "搜索统计：" + "，".join(parts)
