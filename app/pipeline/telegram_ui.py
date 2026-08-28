from collections import defaultdict

from pipeline.dedupe import candidate_info_hash
from pipeline.migration import format_size
from pipeline.prowlarr import is_prowlarr_download_uri
from pipeline.search_stats import format_search_stats
from pipeline.task_state import TASK_STATE


SEARCH_PAGE_SIZE = 5

DEFAULT_TASK_LIST_PAGE_SIZE = 5

CATEGORY_LABELS = {"movie": "电影库", "tv": "剧集库", "anime": "动漫库", "adult": "成人库", "other": "其他库"}

CONTENT_PROFILE_LABELS = {
    "adult": "成人",
    "movie": "电影",
    "tv": "剧集",
    "anime": "动漫",
    "other": "其他",
}

TASK_STATUS_LABELS = {
    "submitted": "已提交",
    "pending": "排队中",
    "waiting": "等待中",
    "running": "处理中",
    "downloading": "下载中",
    "success": "已完成",
    "completed": "已完成",
    "failed": "失败",
    "cancelled": "已取消",
    "canceled": "已取消",
    "skipped": "已跳过",
}

def search_page_count(total, page_size=SEARCH_PAGE_SIZE):
    if total <= 0:
        return 1
    return (total + int(page_size) - 1) // int(page_size)

def normalize_page(page, page_count):
    if page < 0:
        return 0
    if page >= page_count:
        return page_count - 1
    return page

def task_from_submit_result(result, info_hash):
    info_hash = str(info_hash or "").strip()
    task_status = result.get("task_status") or {}
    status_hash = str(task_status.get("info_hash") or "").strip()
    if status_hash.lower() == info_hash.lower() or (task_status and not status_hash and len(result.get("tasks") or []) == 1):
        out = dict(task_status)
        out["info_hash"] = info_hash
        return out
    for task in result.get("tasks") or []:
        if str(task.get("info_hash") or "").lower() == info_hash.lower():
            out = dict(task)
            out.setdefault("status_name", "submitted")
            return out
    return {"info_hash": info_hash, "status_name": "submitted"}

def download_uri_label(value):
    value = str(value or "")
    if value.lower().startswith("magnet:"):
        return "磁链"
    if "115.com/s/" in value or "115cdn.com/s/" in value:
        return "115分享"
    if is_prowlarr_download_uri(value):
        return "Prowlarr下载项"
    if value:
        return "下载链接"
    return "-"

def msg_sync_status_label(value):
    return {
        "success": "已完成",
        "running": "进行中",
        "failed": "失败",
        "skipped": "已跳过",
    }.get(value, value or "-")

def msg_match_mode_label(value):
    return {
        "path": "路径命中",
        "query": "标题/番号命中",
    }.get(value, value or "-")


def task_status_label(value):
    value = str(value or "").strip()
    return TASK_STATUS_LABELS.get(value.lower(), value or "-")


def compact_task_id(value, length=12):
    value = str(value or "").strip()
    if len(value) <= int(length):
        return value
    return value[: int(length)] + "…"

def format_search_page_message(query, candidates, page, page_count, total, title="搜索结果", metadata=None):
    lines = [title, "关键词：%s" % query, "第 %s/%s 页 · 共 %s 条" % (page + 1, page_count, total)]
    stats_line = format_search_stats(metadata)
    if stats_line:
        lines.append(stats_line)
    for _candidate_id, candidate in candidates:
        rank = candidate.get("rank")
        if lines and lines[-1] != "":
            lines.append("")
        lines.append("#%s %s" % (rank, candidate.get("title")))
        append_candidate_detail_lines(lines, candidate)
    if not candidates:
        lines.extend(["", "可使用下方按钮补查其他来源。"])
    return "\n".join(lines)

def format_library_choice_message(candidate):
    lines = ["已选择：%s" % candidate.get("title")]
    if candidate.get("rank"):
        lines.append("候选：#%s" % candidate.get("rank"))
    append_candidate_detail_lines(lines, candidate)
    lines.append("链接类型：%s" % download_uri_label(candidate.get("download_uri")))
    info_hash = candidate_info_hash(candidate)
    if info_hash:
        lines.append("info_hash：%s" % info_hash)
    return "\n".join(lines)

def append_candidate_detail_lines(lines, candidate):
    if (candidate or {}).get("source_kind") == "115_share":
        source = candidate.get("indexer") or candidate.get("pansou_channel") or "-"
        append_pansou_field_lines(lines, candidate)
        lines.append("来源：%s  类型：115分享" % source)
        share_parts = []
        if candidate.get("shareCode"):
            share_parts.append("分享：%s" % candidate.get("shareCode"))
        if candidate.get("sharePassword"):
            share_parts.append("提取：%s" % candidate.get("sharePassword"))
        if share_parts:
            lines.append("  ".join(share_parts))
        summary = truncate_candidate_text(candidate.get("pansou_summary"), 110)
        if summary and not pansou_has_detail_fields(candidate):
            lines.append("摘要：%s" % summary)
        return
    lines.append("站点：%s  做种：%s  大小：%s" % (candidate.get("indexer"), candidate.get("seeders"), format_size(candidate.get("size"))))

def append_pansou_field_lines(lines, candidate):
    fields = (candidate or {}).get("pansou_fields") or {}
    version = fields.get("version")
    resource_type = fields.get("resource_type")
    primary_parts = []
    size_label = share_candidate_size_label(candidate)
    if size_label and size_label != "未知":
        primary_parts.append("大小：%s" % size_label)
    if resource_type:
        primary_parts.append("规格：%s" % truncate_candidate_text(resource_type, 42))
    if version:
        primary_parts.append("版本：%s" % truncate_candidate_text(version, 30))
    if primary_parts:
        lines.append("  ".join(primary_parts))
    elif size_label:
        lines.append("大小：%s" % size_label)
    if fields.get("subtitles"):
        lines.append("字幕：%s" % truncate_candidate_text(fields.get("subtitles"), 150))
    if fields.get("audio"):
        lines.append("音频：%s" % truncate_candidate_text(fields.get("audio"), 150))
    if fields.get("filename"):
        lines.append("文件：%s" % truncate_candidate_text(fields.get("filename"), 150))
    tail_parts = []
    if fields.get("country"):
        tail_parts.append("国家：%s" % truncate_candidate_text(fields.get("country"), 24))
    if fields.get("tmdb"):
        tail_parts.append("TMDB：%s" % truncate_candidate_text(fields.get("tmdb"), 24))
    if fields.get("tags"):
        tail_parts.append("标签：%s" % truncate_candidate_text(fields.get("tags"), 70))
    if tail_parts:
        lines.append("  ".join(tail_parts))

def pansou_has_detail_fields(candidate):
    fields = (candidate or {}).get("pansou_fields") or {}
    return any(fields.get(key) for key in ("version", "audio", "subtitles", "filename", "resource_type", "tmdb", "size", "tags"))

def share_candidate_size_label(candidate):
    size_text = str((candidate or {}).get("pansou_size_text") or "").strip()
    if size_text:
        return size_text
    size = (candidate or {}).get("size")
    if size in (None, "", 0):
        return "未知"
    return format_size(size)

def truncate_candidate_text(value, limit):
    value = " ".join(str(value or "").split())
    if not value:
        return ""
    limit = int(limit)
    if len(value) <= limit:
        return value
    return value[: max(0, limit - 3)].rstrip() + "..."

def format_duplicate_message(candidate, duplicate):
    if duplicate.get("level") == "strong":
        lines = ["重复入库拦截：%s" % candidate.get("title")]
    else:
        lines = ["可能重复入库：%s" % candidate.get("title")]
    lines.append("判定：%s" % duplicate_level_label(duplicate))
    lines.append("原因：%s" % duplicate_reason_label(duplicate))
    if duplicate.get("identity_type"):
        lines.append("命中规则：%s" % duplicate_identity_label(duplicate.get("identity_type")))
    if duplicate.get("identity_value"):
        lines.append("命中值：%s" % duplicate.get("identity_value"))
    candidate_hash = candidate_info_hash(candidate)
    if candidate_hash:
        lines.append("当前info_hash：%s" % candidate_hash)
    if candidate.get("indexer"):
        lines.append("当前来源：%s" % candidate.get("indexer"))
    if duplicate.get("source"):
        lines.append("来源：%s" % duplicate.get("source"))
    if duplicate.get("title"):
        lines.append("已有作品：%s" % duplicate.get("title"))
    if duplicate.get("path"):
        lines.append("已有路径：%s" % duplicate.get("path"))
    if duplicate.get("status_name"):
        lines.append("已有状态：%s" % duplicate.get("status_name"))
    if duplicate.get("msg_sync_status"):
        lines.append("已有MSG同步：%s" % duplicate.get("msg_sync_status"))
    if duplicate.get("info_hash"):
        lines.append("已有info_hash：%s" % duplicate.get("info_hash"))
    if duplicate.get("media_id"):
        lines.append("已有媒体ID：%s" % duplicate.get("media_id"))
    if duplicate.get("msg_media_id"):
        lines.append("已有MSG媒体ID：%s" % duplicate.get("msg_media_id"))
    if duplicate_can_force(duplicate):
        lines.append("如确认这是更高质量或不同版本，可点击“仍然入库”。")
    else:
        lines.append("该重复项不建议再次离线。")
    return "\n".join(lines)

def duplicate_reason_label(duplicate):
    reason = duplicate.get("reason")
    if reason == "same_info_hash":
        return "相同info_hash"
    if reason == "adult_code":
        return "成人番号重复（%s）" % duplicate.get("code")
    if reason == "mediastation_code":
        return "MediaStationGo 已有相同番号"
    if reason == "mediastation_title":
        return "MediaStationGo 标题相似"
    if reason == "openlist_title":
        return "OpenList 标题相似"
    return reason or "重复作品"

def duplicate_level_label(duplicate):
    if duplicate.get("level") == "strong":
        return "强重复（禁止重复提交）"
    return "弱重复（可手动确认）"

def duplicate_identity_label(identity_type):
    labels = {
        "info_hash": "info_hash",
        "adult_code": "成人番号",
        "normalized_title": "规范化标题",
        "title_query": "标题查询",
    }
    return labels.get(identity_type, identity_type or "-")

def duplicate_can_force(duplicate):
    if "can_force" in duplicate:
        return bool(duplicate.get("can_force"))
    return duplicate.get("level") == "weak"

def format_dedupe_refresh_message(entries, count):
    category_counts = defaultdict(int)
    identity_counts = defaultdict(int)
    for entry in entries or []:
        category_counts[entry.get("category")] += 1
        identity_counts[entry.get("identity_type")] += 1
    lines = ["OpenList已入库记录刷新完成", "写入：%s" % count]
    lines.append("电影库：%s" % category_counts.get("movie", 0))
    lines.append("剧集库：%s" % category_counts.get("tv", 0))
    lines.append("成人库：%s" % category_counts.get("adult", 0))
    lines.append("其他库：%s" % category_counts.get("other", 0))
    lines.append("成人番号：%s" % identity_counts.get("adult_code", 0))
    lines.append("标题索引：%s" % identity_counts.get("normalized_title", 0))
    return "\n".join(lines)

def format_submit_message(candidate, result, category=None, content_profile=None):
    lines = ["已提交：%s" % candidate.get("title")]
    if result.get("submit_kind") == "115_share_receive":
        lines[0] = "已转存115分享：%s" % candidate.get("title")
    if category:
        lines.append("入库目录：%s" % CATEGORY_LABELS.get(category, category))
    if content_profile and content_profile != category:
        lines.append("内容分类：%s" % CONTENT_PROFILE_LABELS.get(content_profile, content_profile))
    for task in result.get("tasks") or []:
        if task_is_115_share_receive(task):
            lines.append("任务ID：%s" % task.get("info_hash"))
            if task.get("share_code"):
                lines.append("分享码：%s" % task.get("share_code"))
            continue
        if task.get("info_hash"):
            lines.append("info_hash：%s" % task["info_hash"])
    task_status = result.get("task_status")
    if task_status:
        lines.append("当前状态：%s" % task_status_label(task_status.get("status_name")))
        if task_status.get("percent_done") is not None:
            lines.append("完成进度：%s" % format_percent(task_status.get("percent_done")))
        if task_status.get("file_id"):
            lines.append("file_id：%s" % task_status.get("file_id"))
    if result.get("message"):
        lines.append("结果：%s" % result["message"])
    return "\n".join(lines)

def format_task_status_message(title, task, category=None):
    lines = ["任务详情", str(title or "-")]
    append_task_lines(lines, task, category=category)
    return "\n".join(lines)

def task_diagnostic_stage_values(task):
    out = []
    for key, label in (
        ("openlist_adult_format_status", "番号格式化"),
        ("openlist_trash_hide_status", "回收站隐藏"),
        ("openlist_clean_status", "OpenList隐藏"),
        ("openlist_adult_extra_hide_status", "成人附加隐藏"),
        ("msg_scan_status", "MSG扫描"),
        ("msg_scrape_status", "MSG刮削"),
        ("msg_extra_cleanup_status", "特典隐藏"),
        ("msg_visibility_repair_status", "可见性修复"),
        ("subtitle_match_status", "字幕匹配"),
    ):
        value = (task or {}).get(key)
        if value:
            out.append((label, value))
    return out

def format_cancel_result_message(title, result, category=None):
    task = result.get("task") or {}
    if result.get("cancelled"):
        lines = ["已取消任务：%s" % title]
    else:
        lines = ["任务未取消：%s" % title]
        if result.get("reason"):
            lines.append("原因：%s" % result["reason"])
    append_task_lines(lines, task, category=category)
    return "\n".join(lines)

def format_auto_cancel_result_message(title, result, task, category=None):
    if result.get("cancelled"):
        lines = ["任务超时已自动取消：%s" % title]
    else:
        lines = ["任务超时自动取消结果：%s" % title]
        if result.get("reason"):
            lines.append("原因：%s" % result["reason"])
    if (task or {}).get("auto_cancel_reason"):
        lines.append("超时规则：%s" % task.get("auto_cancel_reason"))
    append_task_lines(lines, task or {}, category=category)
    return "\n".join(lines)

def format_task_list_message(records, page=0, page_count=1, total=None, page_size=DEFAULT_TASK_LIST_PAGE_SIZE):
    if total is None:
        total = len(records)
    lines = ["最近任务", "第 %s/%s 页 · 共 %s 条" % (page + 1, page_count, total)]
    start_index = page * int(page_size) + 1
    for idx, record in enumerate(records, 1):
        task = record["task"]
        title = record["title"] or task.get("name") or task.get("info_hash")
        display_index = start_index + idx - 1
        lines.append("")
        lines.append("#%s %s" % (display_index, title))
        summary = [
            CATEGORY_LABELS.get(record.get("category"), record.get("category") or "-"),
            task_status_label(task.get("status_name")),
        ]
        progress = format_percent(task.get("percent_done"))
        if progress != "-":
            summary.append(progress)
        lines.append(" · ".join(summary))
        if task.get("content_profile") and task.get("content_profile") != record.get("category"):
            lines.append("内容：%s" % CONTENT_PROFILE_LABELS.get(task.get("content_profile"), task.get("content_profile")))
        if task.get("msg_sync_status"):
            lines.append("MSG：%s" % msg_sync_status_label(task.get("msg_sync_status")))
        if task.get("info_hash"):
            lines.append("任务ID：%s" % compact_task_id(task["info_hash"]))
    return "\n".join(lines)

def append_task_lines(lines, task, category=None):
    if task_is_115_share_receive(task):
        lines.append("任务类型：115分享转存")
        lines.append("任务ID：%s" % task.get("info_hash"))
        if task.get("share_code"):
            lines.append("分享码：%s" % task.get("share_code"))
    elif task.get("info_hash"):
        lines.append("info_hash：%s" % task["info_hash"])
    status_summary = task_status_label(task.get("status_name"))
    progress = format_percent(task.get("percent_done"))
    if progress != "-":
        status_summary += " · " + progress
    lines.append("当前状态：%s" % status_summary)
    if category:
        lines.append("入库目录：%s" % CATEGORY_LABELS.get(category, category))
    if task.get("file_id"):
        lines.append("file_id：%s" % task.get("file_id"))
    if task.get("wp_path_id"):
        lines.append("wp_path_id：%s" % task.get("wp_path_id"))
    if task.get("content_profile"):
        lines.append("内容分类：%s" % CONTENT_PROFILE_LABELS.get(task.get("content_profile"), task.get("content_profile")))
    if task.get("transfer_verify_status"):
        verification = task.get("transfer_verification") or {}
        direct_count = ((verification.get("direct_manifest") or {}).get("entry_count"))
        openlist_count = ((verification.get("openlist_manifest") or {}).get("entry_count"))
        if task.get("transfer_verify_status") == "success":
            lines.append("文件完整性：已校验（115 %s / OpenList %s）" % (direct_count or 0, openlist_count or 0))
        elif task.get("transfer_verify_status") == "running":
            lines.append("文件完整性：等待115与OpenList一致（115 %s / OpenList %s）" % (direct_count or 0, openlist_count or 0))
        else:
            lines.append("文件完整性：失败")
            if task.get("transfer_verify_error"):
                lines.append("完整性错误：%s" % task.get("transfer_verify_error"))
    if task.get("msg_sync_status"):
        if task.get("msg_sync_status") == "success":
            lines.append("MSG同步：已完成")
            if task.get("msg_media_id"):
                lines.append("MSG媒体ID：%s" % task.get("msg_media_id"))
            if task.get("msg_match_mode"):
                lines.append("MSG匹配：%s" % msg_match_mode_label(task.get("msg_match_mode")))
            if task.get("msg_match_path"):
                lines.append("MSG路径：%s" % task.get("msg_match_path"))
        elif task.get("msg_sync_status") == "running":
            lines.append("MSG同步：进行中")
        else:
            lines.append("MSG同步：失败")
            if task.get("msg_error"):
                lines.append("MSG错误：%s" % task.get("msg_error"))
    if task.get("openlist_clean_status") and task.get("openlist_clean_status") != "skipped":
        if task.get("openlist_clean_status") == "success":
            lines.append("OpenList隐藏：已完成（%s 个）" % (task.get("openlist_hidden_count") or task.get("openlist_cleaned_count") or 0))
        elif task.get("openlist_clean_status") == "running":
            lines.append("OpenList隐藏：进行中")
        else:
            lines.append("OpenList隐藏：失败")
            if task.get("openlist_clean_error"):
                lines.append("OpenList错误：%s" % task.get("openlist_clean_error"))
            lines.append("OpenList处理：请手动为目标目录添加 Meta Hide，隐藏广告/样片等无效小文件，然后点击重试MSG同步")
    if category == "adult" and task.get("openlist_adult_format_status") and task.get("openlist_adult_format_status") != "skipped":
        if task.get("openlist_adult_format_status") == "success":
            lines.append("番号格式化：已完成（%s）" % task.get("openlist_adult_code"))
        elif task.get("openlist_adult_format_status") == "running":
            lines.append("番号格式化：进行中")
        else:
            lines.append("番号格式化：失败")
            if task.get("openlist_adult_format_error"):
                lines.append("番号错误：%s" % task.get("openlist_adult_format_error"))
            lines.append("番号处理：请手动将目录重命名为“标准番号 - 原名称”，然后点击重试MSG同步")
    if task.get("openlist_trash_hide_status") and task.get("openlist_trash_hide_status") != "skipped":
        if task.get("openlist_trash_hide_status") == "success":
            lines.append(
                "回收站隐藏：已完成（隐藏 %s 个，跳过 %s 个）"
                % (task.get("openlist_trash_hide_hidden_count") or 0, task.get("openlist_trash_hide_skipped_count") or 0)
            )
        elif task.get("openlist_trash_hide_status") == "running":
            lines.append("回收站隐藏：进行中")
        else:
            lines.append("回收站隐藏：失败")
            if task.get("openlist_trash_hide_error"):
                lines.append("回收站隐藏错误：%s" % task.get("openlist_trash_hide_error"))
    if task.get("msg_scan_status"):
        if task.get("msg_scan_status") == "success":
            lines.append("MSG扫描：已完成")
        elif task.get("msg_scan_status") == "running":
            lines.append("MSG扫描：进行中")
        else:
            lines.append("MSG扫描：失败")
    if task.get("msg_scrape_status"):
        if task.get("msg_scrape_status") == "success":
            lines.append("MSG刮削：已完成")
        elif task.get("msg_scrape_status") == "skipped":
            lines.append("MSG刮削：已跳过")
        elif task.get("msg_scrape_status") == "running":
            lines.append("MSG刮削：进行中")
        else:
            lines.append("MSG刮削：失败")
    if category in ("tv", "anime") and task.get("msg_visibility_repair_status"):
        if task.get("msg_visibility_repair_status") == "success":
            lines.append("MSG可见性修复：已完成（%s 项）" % (task.get("msg_visibility_repair_updated") or 0))
        elif task.get("msg_visibility_repair_status") == "running":
            lines.append("MSG可见性修复：进行中")
        elif task.get("msg_visibility_repair_status") != "skipped":
            lines.append("MSG可见性修复：失败")
            if task.get("msg_visibility_repair_error"):
                lines.append("可见性修复错误：%s" % task.get("msg_visibility_repair_error"))
    if task.get("subtitle_match_status"):
        if task.get("subtitle_match_status") == "success":
            lines.append(
                "字幕匹配：已完成（%s 条，%s）"
                % (task.get("subtitle_match_count") or 0, task.get("subtitle_match_source") or "-")
            )
            if task.get("subtitle_match_filename"):
                lines.append("字幕文件：%s" % task.get("subtitle_match_filename"))
        elif task.get("subtitle_match_status") == "running":
            lines.append("字幕匹配：进行中")
        elif task.get("subtitle_match_status") == "skipped":
            reason = task.get("subtitle_match_reason")
            if reason == "not_found":
                lines.append("字幕匹配：未找到")
            elif reason in ("provider_missing", "query_missing", "media_id_missing"):
                lines.append("字幕匹配：已跳过（%s）" % reason)
        else:
            lines.append("字幕匹配：失败")
            if task.get("subtitle_match_error"):
                lines.append("字幕错误：%s" % task.get("subtitle_match_error"))

def format_percent(value):
    if value is None:
        return "-"
    text = str(value).strip()
    if not text:
        return "-"
    if text.endswith("%"):
        return text
    try:
        number = float(text)
    except (TypeError, ValueError):
        return text
    if number.is_integer():
        return "%s%%" % int(number)
    return ("%.1f" % number).rstrip("0").rstrip(".") + "%"


def home_reply_markup():
    return {
        "inline_keyboard": [
            [
                {"text": "最近任务", "callback_data": "home:tasks"},
                {"text": "字幕管理", "callback_data": "home:subtitles"},
            ],
            [
                {"text": "媒体迁移", "callback_data": "home:migrate"},
                {"text": "115 Cookie（MSG 管理）", "callback_data": "home:p115"},
            ],
        ]
    }


def home_back_reply_markup():
    return {"inline_keyboard": [[{"text": "⌂ 功能菜单", "callback_data": "home:menu"}]]}

def submit_reply_markup(result):
    for task in result.get("tasks") or []:
        if task.get("info_hash"):
            return task_reply_markup(task_from_submit_result(result, task["info_hash"]))
    task_status = result.get("task_status")
    if task_status:
        return task_reply_markup(task_status)
    return None

def search_page_reply_markup(
    session_id,
    candidates,
    page,
    page_count,
    allow_adult_retry=False,
    allow_anime_retry=False,
    allow_bt4g_retry=False,
    allow_llm_rerank=False,
    allow_pansou_search=False,
):
    rows = []
    for candidate_id, candidate in candidates:
        rows.append([{"text": "#%s 入库" % candidate.get("rank"), "callback_data": "choose:%s" % candidate_id}])
    nav = []
    if page > 0:
        nav.append({"text": "‹ 上一页", "callback_data": "page:%s:%s" % (session_id, page - 1)})
    if page + 1 < page_count:
        nav.append({"text": "下一页 ›", "callback_data": "page:%s:%s" % (session_id, page + 1)})
    if nav:
        rows.append(nav)
    page_jump = search_page_jump_buttons(session_id, page, page_count)
    if page_jump:
        rows.append(page_jump)
    retry = []
    if allow_adult_retry:
        retry.append({"text": "补查成人", "callback_data": "adult_search:%s" % session_id})
    if allow_anime_retry:
        retry.append({"text": "补查动漫", "callback_data": "anime_search:%s" % session_id})
    if allow_bt4g_retry:
        retry.append({"text": "补查 BT4G", "callback_data": "bt4g_search:%s" % session_id})
    if allow_pansou_search:
        retry.append({"text": "搜索115网盘", "callback_data": "pansou_search:%s" % session_id})
    if retry:
        rows.append(retry)
    if allow_llm_rerank:
        rows.append([{"text": "LLM 优选", "callback_data": "llm_rerank:%s" % session_id}])
    rows.append([{"text": "关闭结果", "callback_data": "close_search:%s" % session_id}])
    return {"inline_keyboard": rows}

def task_is_115_share_receive(task):
    return (task or {}).get("source_kind") == "115_share"

def submit_callback_text(result):
    if (result or {}).get("submit_kind") == "115_share_receive":
        return "已转存115分享"
    return "已提交 115 离线"

def search_page_jump_buttons(session_id, page, page_count):
    if page_count <= 1:
        return []

    candidates = {0, page_count - 1}
    for offset in (-1, 0, 1):
        target = page + offset
        if 0 <= target < page_count:
            candidates.add(target)
    if page <= 2:
        candidates.update(range(0, min(5, page_count)))
    if page >= page_count - 3:
        candidates.update(range(max(0, page_count - 5), page_count))

    row = []
    previous = None
    for target in sorted(candidates):
        if previous is not None and target - previous > 1:
            row.append({"text": "...", "callback_data": "page:%s:%s" % (session_id, page)})
        text = str(target + 1)
        if target == page:
            text = "·%s·" % text
        row.append({"text": text, "callback_data": "page:%s:%s" % (session_id, target)})
        previous = target
    return row

def library_choice_reply_markup(candidate_id, include_back=False):
    rows = [
        [
            {"text": "电影", "callback_data": "profile:movie:%s" % candidate_id},
            {"text": "剧集", "callback_data": "profile:tv:%s" % candidate_id},
            {"text": "动漫", "callback_data": "profile:anime:%s" % candidate_id},
        ],
        [
            {"text": "成人", "callback_data": "profile:adult:%s" % candidate_id},
            {"text": "其他", "callback_data": "profile:other:%s" % candidate_id},
        ],
    ]
    if include_back:
        rows.append([{"text": "返回结果", "callback_data": "close_choice:%s" % candidate_id}])
    return {"inline_keyboard": rows}

def dedupe_refresh_confirm_reply_markup():
    return {
        "inline_keyboard": [
            [
                {"text": "确认刷新", "callback_data": "dedupe_refresh_confirm:1"},
                {"text": "取消", "callback_data": "dedupe_refresh_cancel:1"},
            ]
        ]
    }

def duplicate_reply_markup(duplicate, category, candidate_id, content_profile=None):
    rows = []
    if duplicate.get("info_hash"):
        rows.append([{"text": "查看已有任务", "callback_data": "status:%s" % duplicate.get("info_hash")}])
    if duplicate_can_force(duplicate):
        callback_data = "force_submit:%s:%s" % (category, candidate_id)
        if content_profile:
            callback_data = "%s:%s" % (callback_data, content_profile)
        rows.append([{"text": "仍然入库", "callback_data": callback_data}])
    return {"inline_keyboard": rows}

def task_reply_markup(task):
    info_hash = (task or {}).get("info_hash")
    if task_can_retry_msg_sync(task):
        return {"inline_keyboard": [[{"text": "重试 MSG 同步", "callback_data": "retry_msg:%s" % info_hash}]]}
    rows = []
    if TASK_STATE.can_refresh_offline_status(task):
        row = [{"text": "↻ 刷新进度", "callback_data": "status:%s" % info_hash}]
        if TASK_STATE.can_cancel_offline_task(task):
            row.append({"text": "✕ 取消任务", "callback_data": "cancel:%s" % info_hash})
        rows.append(row)
    if task_can_match_subtitles(task):
        rows.append([{"text": "查找字幕", "callback_data": "subtitle:%s" % info_hash}])
    return {"inline_keyboard": rows} if rows else None

def callback_task_reply_markup(task):
    return task_reply_markup(task) or {"inline_keyboard": []}

def task_is_final(task):
    return TASK_STATE.is_offline_final(task)


def task_can_retry_msg_sync(task):
    return TASK_STATE.can_retry_msg_sync(task)


def task_can_match_subtitles(task):
    task = task or {}
    return bool(
        task.get("info_hash")
        and task.get("msg_media_id")
        and task.get("msg_sync_status") == "success"
        and task.get("subtitle_match_status") != "running"
    )


def task_page_count(total, page_size=DEFAULT_TASK_LIST_PAGE_SIZE):
    if total <= 0:
        return 1
    return (total + int(page_size) - 1) // int(page_size)

def task_list_priority(record):
    task = (record or {}).get("task") or {}
    return TASK_STATE.task_list_priority(task)

def task_list_reply_markup(records, page=0, page_count=1, page_size=DEFAULT_TASK_LIST_PAGE_SIZE):
    rows = []
    for idx, record in enumerate(records, 1):
        task = record["task"]
        info_hash = task.get("info_hash") or record["info_hash"]
        display_index = page * int(page_size) + idx
        if task_can_retry_msg_sync(task):
            rows.append([{"text": "#%s 重试 MSG" % display_index, "callback_data": "retry_msg:%s" % info_hash}])
            continue
        if not TASK_STATE.can_refresh_offline_status(task):
            continue
        row = [{"text": "#%s ↻ 刷新" % display_index, "callback_data": "status:%s" % info_hash}]
        if TASK_STATE.can_cancel_offline_task(task):
            row.append({"text": "#%s ✕ 取消" % display_index, "callback_data": "cancel:%s" % info_hash})
        rows.append(row)
    nav = []
    if page > 0:
        nav.append({"text": "‹ 上一页", "callback_data": "tasks_page:%s" % (page - 1)})
    if page + 1 < page_count:
        nav.append({"text": "下一页 ›", "callback_data": "tasks_page:%s" % (page + 1)})
    if nav:
        rows.append(nav)
    rows.append([{"text": "⌂ 功能菜单", "callback_data": "home:menu"}])
    return {"inline_keyboard": rows}
