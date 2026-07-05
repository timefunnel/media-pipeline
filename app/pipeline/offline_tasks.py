import time

from pipeline.task_state import STATUS_CANCELLED, STATUS_FAILED, STATUS_SUCCESS, TASK_STATE


def normalize_task(task):
    status = int(task.get("status") or 0)
    return {
        "info_hash": task.get("info_hash"),
        "name": task.get("name"),
        "status": status,
        "status_name": TASK_STATE.normalize_offline_status_name(status),
        "percent_done": task.get("percentDone"),
        "size": task.get("size"),
        "file_id": task.get("file_id"),
        "wp_path_id": task.get("wp_path_id"),
        "url": task.get("url"),
    }


def find_task_by_info_hash(client, info_hash, max_pages=10):
    expected = str(info_hash).lower()
    page = 1
    while page <= max_pages:
        response = client.get_offline_tasks(page=page)
        if response.get("state") is not True:
            raise RuntimeError("115 offline task list failed: %s" % (response.get("message") or response.get("msg") or response.get("code")))

        data = response.get("data") or {}
        for task in data.get("tasks") or []:
            if str(task.get("info_hash") or "").lower() == expected:
                return normalize_task(task)

        page_count = int(data.get("page_count") or page)
        if page >= page_count:
            break
        page += 1

    raise RuntimeError("offline task not found: %s" % info_hash)


def find_tasks_by_info_hashes(client, info_hashes, max_pages=10):
    expected = {str(info_hash or "").strip().lower() for info_hash in info_hashes or [] if str(info_hash or "").strip()}
    if not expected:
        return {}

    found = {}
    page = 1
    while page <= max_pages:
        response = client.get_offline_tasks(page=page)
        if response.get("state") is not True:
            raise RuntimeError("115 offline task list failed: %s" % (response.get("message") or response.get("msg") or response.get("code")))

        data = response.get("data") or {}
        for task in data.get("tasks") or []:
            info_hash = str(task.get("info_hash") or "").strip().lower()
            if info_hash in expected and info_hash not in found:
                found[info_hash] = normalize_task(task)
        if len(found) == len(expected):
            break

        page_count = int(data.get("page_count") or page)
        if page >= page_count:
            break
        page += 1

    return found


def wait_for_task(client, info_hash, timeout_seconds=600, interval_seconds=15, max_pages=10, sleep=time.sleep, now=time.monotonic):
    deadline = now() + timeout_seconds
    while True:
        task = find_task_by_info_hash(client, info_hash, max_pages=max_pages)
        if task["status_name"] == STATUS_SUCCESS:
            return task
        if task["status_name"] == STATUS_FAILED:
            raise RuntimeError("offline task failed: %s" % info_hash)
        if now() >= deadline:
            raise TimeoutError("offline task wait timeout: %s" % info_hash)
        sleep(interval_seconds)


def task_can_cancel(task):
    return task is not None and TASK_STATE.is_offline_active(task)


def cancel_task_if_active(client, info_hash, max_pages=10):
    task = find_task_by_info_hash(client, info_hash, max_pages=max_pages)
    if not task_can_cancel(task):
        return {
            "cancelled": False,
            "task": task,
            "response": None,
            "reason": "task is not cancellable: %s" % task.get("status_name"),
        }

    response = client.delete_offline_task(info_hash, delete_files=False)
    if response.get("state") is not True:
        raise RuntimeError("115 offline task cancel failed: %s" % (response.get("message") or response.get("msg") or response.get("code")))
    cancelled = dict(task)
    cancelled["status_name"] = STATUS_CANCELLED
    return {
        "cancelled": True,
        "task": cancelled,
        "response": response,
        "reason": "",
    }
