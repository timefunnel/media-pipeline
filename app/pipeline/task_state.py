from dataclasses import dataclass


STATUS_RUNNING = "running"
STATUS_SUCCESS = "success"
STATUS_FAILED = "failed"
STATUS_SKIPPED = "skipped"
STATUS_CANCELLED = "cancelled"
STATUS_SUBMITTED = "submitted"
STATUS_ALLOCATING = "allocating"
STATUS_DOWNLOADING = "downloading"
STATUS_UNKNOWN = "unknown"

OFFLINE_STATUS_NAMES = {
    -1: STATUS_FAILED,
    0: STATUS_ALLOCATING,
    1: STATUS_DOWNLOADING,
    2: STATUS_SUCCESS,
}
OFFLINE_ACTIVE_STATUS_NAMES = {STATUS_SUBMITTED, STATUS_ALLOCATING, STATUS_DOWNLOADING}
OFFLINE_FINAL_STATUS_NAMES = {STATUS_SUCCESS, STATUS_FAILED, STATUS_CANCELLED}
SYNC_COMPLETE_STATUS_NAMES = {STATUS_SUCCESS, STATUS_SKIPPED}


@dataclass(frozen=True)
class StageDefinition:
    key: str
    error_key: str = ""


SYNC_STAGE_DEFINITIONS = (
    StageDefinition("openlist_adult_format_status", "openlist_adult_format_error"),
    StageDefinition("openlist_trash_hide_status", "openlist_trash_hide_error"),
    StageDefinition("openlist_clean_status", "openlist_clean_error"),
    StageDefinition("openlist_adult_extra_hide_status", "openlist_adult_extra_hide_error"),
    StageDefinition("msg_scan_status"),
    StageDefinition("msg_scrape_status"),
    StageDefinition("msg_extra_cleanup_status", "msg_extra_cleanup_error"),
    StageDefinition("msg_visibility_repair_status", "msg_visibility_repair_error"),
    StageDefinition("subtitle_match_status", "subtitle_match_error"),
)


class TaskStateMachine:
    def __init__(self, sync_stages=SYNC_STAGE_DEFINITIONS):
        self.sync_stages = tuple(sync_stages)

    def normalize_offline_status_name(self, status):
        try:
            status = int(status)
        except (TypeError, ValueError):
            return STATUS_UNKNOWN
        return OFFLINE_STATUS_NAMES.get(status, STATUS_UNKNOWN)

    def is_offline_active(self, task):
        return self.status_name(task) in OFFLINE_ACTIVE_STATUS_NAMES

    def is_offline_final(self, task):
        return self.status_name(task) in OFFLINE_FINAL_STATUS_NAMES

    def is_offline_success(self, task):
        return self.status_name(task) == STATUS_SUCCESS

    def can_refresh_offline_status(self, task):
        task = task or {}
        return bool(task.get("info_hash") and not self.is_offline_final(task))

    def can_cancel_offline_task(self, task):
        return self.is_offline_active(task)

    def status_name(self, task):
        return (task or {}).get("status_name")

    def msg_synced(self, task):
        task = task or {}
        post_enhancement = str(task.get("post_enhancement_status") or "").strip().lower()
        return task.get("msg_sync_status") == STATUS_SUCCESS and (
            self.stage_is_complete(task.get("msg_scrape_status")) or post_enhancement in {"pending", "running"}
        )

    def sync_is_running(self, task):
        task = task or {}
        return any(task.get(stage.key) == STATUS_RUNNING for stage in self.sync_stages) or task.get("msg_sync_status") == STATUS_RUNNING

    def stage_is_complete(self, status):
        return status in SYNC_COMPLETE_STATUS_NAMES

    def mark_running_sync_stage_failed(self, task, error):
        task = task or {}
        for stage in self.sync_stages:
            if task.get(stage.key) == STATUS_RUNNING:
                task[stage.key] = STATUS_FAILED
                if stage.error_key:
                    task[stage.error_key] = error
                return stage.key
        return ""

    def can_retry_msg_sync(self, task):
        task = task or {}
        return bool(
            task.get("info_hash")
            and self.is_offline_success(task)
            and task.get("msg_sync_status") == STATUS_FAILED
            and not self.msg_synced(task)
        )

    def should_show_syncing_status(self, task, msg_enabled):
        return bool(
            msg_enabled
            and self.is_offline_success(task)
            and not self.msg_synced(task)
            and not self.sync_is_running(task)
        )

    def task_list_priority(self, task):
        task = task or {}
        if self.can_retry_msg_sync(task):
            return 0
        if not self.is_offline_final(task):
            return 1
        if self.status_name(task) in {STATUS_FAILED, STATUS_CANCELLED}:
            return 2
        return 3


TASK_STATE = TaskStateMachine()
