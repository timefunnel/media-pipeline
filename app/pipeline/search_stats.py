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
        skipped_count = sum(1 for source in self.sources if source.get("status") == "skipped")
        result_count = sum(int(source.get("result_count") or 0) for source in self.sources)
        metadata = {
            "profile": profile or "",
            "total_ms": total_ms,
            "source_count": len(self.sources),
            "success_count": success_count,
            "failed_count": failed_count,
            "timeout_count": timeout_count,
            "skipped_count": skipped_count,
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
    skipped_count = int(metadata.get("skipped_count") or 0)
    parts = ["来源%s个" % source_count, "耗时%.1fs" % (total_ms / 1000.0), "返回%s条" % raw_count]
    if selected_count:
        parts.append("展示%s条" % selected_count)
    if failed_count:
        parts.append("失败%s个" % failed_count)
    if timeout_count:
        parts.append("超时%s个" % timeout_count)
    if skipped_count:
        parts.append("跳过慢源%s个" % skipped_count)
    llm_part = format_llm_rerank_stats(metadata)
    if llm_part:
        parts.append(llm_part)
    return "搜索统计：" + "，".join(parts)


def format_llm_rerank_stats(metadata):
    settings = (metadata or {}).get("settings") or {}
    sources = (metadata or {}).get("sources") or []
    llm_entry = None
    for source in sources:
        if source.get("phase") == "llm_rerank" or source.get("source") == "LLM rerank":
            llm_entry = source
            break
    if not llm_entry:
        if settings.get("llm_rerank_enabled"):
            return "LLM重排未执行"
        return ""

    duration_ms = int(llm_entry.get("duration_ms") or 0)
    duration = "%.1fs" % (duration_ms / 1000.0)
    if llm_entry.get("status") == "success":
        return "LLM重排成功%s" % duration
    if llm_entry.get("status") == "timeout":
        return "LLM重排超时%s" % duration
    if llm_entry.get("status") == "skipped":
        return "LLM重排跳过%s" % duration
    return "LLM重排失败%s" % duration
