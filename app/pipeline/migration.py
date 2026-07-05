MIGRATION_CATEGORY_LABELS = {
    "movie": "电影库",
    "tv": "剧集库",
    "anime": "动漫库",
    "adult": "成人库",
    "other": "其他库",
}


def format_size(value):
    if value is None:
        return "-"
    size = float(value)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if size < 1024 or unit == "TB":
            return "%.1f%s" % (size, unit)
        size = size / 1024


def migration_source_kind_label(candidate):
    if (candidate or {}).get("source_kind") == "file":
        return "文件"
    return "目录"


def format_migration_search_message(query, candidates):
    lines = ["媒体迁移搜索：%s" % query, "请选择要迁移的媒体。"]
    for index, (_candidate_id, candidate) in enumerate(candidates, 1):
        lines.append("%s. %s" % (index, candidate.get("title") or "-"))
        lines.append(
            "当前：%s  类型：%s  数量：%s  大小：%s"
            % (
                MIGRATION_CATEGORY_LABELS.get(candidate.get("category"), candidate.get("library_name") or "-"),
                migration_source_kind_label(candidate),
                candidate.get("media_count") or 0,
                format_size(candidate.get("total_size")),
            )
        )
        lines.append("路径：%s" % candidate.get("source_openlist_path"))
    return "\n".join(lines)


def format_migration_target_choice_message(candidate):
    lines = ["迁移媒体：%s" % (candidate.get("title") or "-")]
    lines.append("当前库：%s" % MIGRATION_CATEGORY_LABELS.get(candidate.get("category"), candidate.get("library_name") or "-"))
    lines.append("媒体数量：%s" % (candidate.get("media_count") or 0))
    lines.append("源路径：%s" % candidate.get("source_openlist_path"))
    lines.append("请选择目标库。")
    return "\n".join(lines)


def format_migration_confirm_message(candidate, target_category, target):
    lines = ["确认迁移？"]
    lines.append("媒体：%s" % (candidate.get("title") or "-"))
    lines.append("源库：%s" % MIGRATION_CATEGORY_LABELS.get(candidate.get("category"), candidate.get("library_name") or "-"))
    lines.append("目标库：%s" % MIGRATION_CATEGORY_LABELS.get(target_category, target_category))
    lines.append("媒体数量：%s" % (candidate.get("media_count") or 0))
    lines.append("源路径：%s" % candidate.get("source_openlist_path"))
    lines.append("目标路径：%s" % target.get("target_openlist_path"))
    lines.append("将移动 OpenList/115 路径并更新 MSG 数据库；不会重新扫描或重新刮削。")
    return "\n".join(lines)


def format_migration_running_message(candidate, target_category):
    return "正在迁移：%s -> %s" % (
        candidate.get("source_openlist_path"),
        MIGRATION_CATEGORY_LABELS.get(target_category, target_category),
    )


def format_migration_result_message(candidate, result):
    lines = ["迁移完成：%s" % (candidate.get("title") or "-")]
    lines.append("源路径：%s" % result.get("source_openlist_path"))
    lines.append("目标路径：%s" % result.get("target_openlist_path"))
    lines.append("目标库：%s" % MIGRATION_CATEGORY_LABELS.get(result.get("target_category"), result.get("target_category")))
    lines.append("MSG媒体记录：%s" % (result.get("media_count") or 0))
    if result.get("series_count"):
        lines.append("剧集记录：%s" % result.get("series_count"))
    return "\n".join(lines)


def migration_search_reply_markup(candidates):
    rows = []
    for index, (candidate_id, _candidate) in enumerate(candidates, 1):
        rows.append([{"text": "迁移 %s" % index, "callback_data": "migrate_select:%s" % candidate_id}])
    return {"inline_keyboard": rows}


def migration_target_choice_reply_markup(candidate_id, candidate):
    rows = []
    current = candidate.get("category")
    first = []
    for category in ("movie", "tv", "anime"):
        if category != current:
            first.append({"text": MIGRATION_CATEGORY_LABELS[category].replace("库", ""), "callback_data": "migrate_to:%s:%s" % (category, candidate_id)})
    if first:
        rows.append(first)
    second = []
    for category in ("adult", "other"):
        if category != current:
            second.append({"text": MIGRATION_CATEGORY_LABELS[category].replace("库", ""), "callback_data": "migrate_to:%s:%s" % (category, candidate_id)})
    if second:
        rows.append(second)
    rows.append([{"text": "取消", "callback_data": "migrate_cancel:%s" % candidate_id}])
    return {"inline_keyboard": rows}


def migration_confirm_reply_markup(candidate_id, target_category):
    return {
        "inline_keyboard": [
            [
                {"text": "确认迁移", "callback_data": "migrate_confirm:%s:%s" % (target_category, candidate_id)},
                {"text": "取消", "callback_data": "migrate_cancel:%s" % candidate_id},
            ]
        ]
    }
