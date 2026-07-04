import argparse
import json
import os
import sys
import urllib.parse

from pipeline.client115 import Client115
from pipeline.bot import (
    DEFAULT_UPSTREAM_SEARCH_LIMIT,
    build_bot,
    search_anime_indexer_results,
    search_primary_indexer_results,
    should_search_sukebei,
    search_sukebei_indexer_results,
)
from pipeline.config import FOLDER_IDS, category_to_folder_id, category_to_msg_library_root, category_to_openlist_path
from pipeline.mediastation import DEFAULT_MSG_BASE_URL, MediaStationClient
from pipeline.offline_tasks import find_task_by_info_hash, wait_for_task
from pipeline.openlist import DEFAULT_OPENLIST_URL, OpenListClient, OpenListTokenProvider
from pipeline.openlist_tokens import OpenListTokenStore
from pipeline.prowlarr import DEFAULT_PROWLARR_CONFIG, DEFAULT_PROWLARR_URL, ProwlarrClient, ProwlarrConfig, is_prowlarr_download_uri
from pipeline.resource_selector import ResourceSelector


DEFAULT_OPENLIST_DB = "/openlist-data/data.db"


def build_parser():
    parser = argparse.ArgumentParser(prog="media-pipeline")
    parser.add_argument("--openlist-db", default=os.environ.get("OPENLIST_DB", DEFAULT_OPENLIST_DB))
    parser.add_argument("--openlist-url", default=os.environ.get("OPENLIST_URL", DEFAULT_OPENLIST_URL))
    parser.add_argument("--prowlarr-url", default=os.environ.get("PROWLARR_URL", DEFAULT_PROWLARR_URL))
    parser.add_argument("--prowlarr-config", default=os.environ.get("PROWLARR_CONFIG", DEFAULT_PROWLARR_CONFIG))
    parser.add_argument("--msg-base-url", default=os.environ.get("MSG_BASE_URL", DEFAULT_MSG_BASE_URL))
    parser.add_argument("--msg-admin-user", default=os.environ.get("MSG_ADMIN_USER", ""))
    parser.add_argument("--msg-admin-password", default=os.environ.get("MSG_ADMIN_PASSWORD", ""))
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("folders")
    subparsers.add_parser("verify-folders")
    subparsers.add_parser("probe")
    subparsers.add_parser("bot")
    subparsers.add_parser("msg-login")

    msg_scan_parser = subparsers.add_parser("msg-scan")
    msg_scan_parser.add_argument("--category", choices=sorted(FOLDER_IDS.keys()), required=True)

    status_parser = subparsers.add_parser("task-status")
    status_parser.add_argument("--info-hash", required=True)
    status_parser.add_argument("--max-pages", type=int, default=10)

    wait_parser = subparsers.add_parser("wait-task")
    wait_parser.add_argument("--info-hash", required=True)
    wait_parser.add_argument("--timeout-seconds", type=int, default=600)
    wait_parser.add_argument("--interval-seconds", type=int, default=15)
    wait_parser.add_argument("--max-pages", type=int, default=10)

    search_parser = subparsers.add_parser("search")
    search_parser.add_argument("--query", required=True)
    search_parser.add_argument("--limit", type=int, default=20)

    submit_search_parser = subparsers.add_parser("submit-search")
    submit_search_parser.add_argument("--query", required=True)
    submit_search_parser.add_argument("--category", choices=sorted(FOLDER_IDS.keys()), required=True)
    submit_search_parser.add_argument("--limit", type=int, default=20)
    submit_search_parser.add_argument("--rank", type=int, default=1)
    submit_search_parser.add_argument("--commit", action="store_true")

    add_parser = subparsers.add_parser("add-offline")
    add_parser.add_argument("--category", choices=sorted(FOLDER_IDS.keys()), required=True)
    add_parser.add_argument("--url", action="append", required=True)
    add_parser.add_argument("--commit", action="store_true")

    return parser


def main(argv=None):
    args = build_parser().parse_args(argv)

    if args.command == "folders":
        print(json.dumps(FOLDER_IDS, ensure_ascii=False, sort_keys=True))
        return 0

    if args.command == "bot":
        build_bot().run_forever()
        return 0

    if args.command == "msg-login":
        build_msg_client(args).login()
        print(json.dumps({"authenticated": True}, ensure_ascii=False, sort_keys=True))
        return 0

    if args.command == "msg-scan":
        root = category_to_msg_library_root(args.category)
        response = build_msg_client(args).scan_root(root["library_id"], root["root_id"])
        print(
            json.dumps(
                {
                    "category": args.category,
                    "library_id": root["library_id"],
                    "root_id": root["root_id"],
                    "response": summarize_response(response),
                },
                ensure_ascii=False,
                sort_keys=True,
            )
        )
        return 0

    if args.command in ("search", "submit-search"):
        prowlarr = build_prowlarr_client(args)
        indexers = prowlarr.indexers()
        raw_candidates = search_primary_indexer_results(prowlarr, args.query, max(args.limit, DEFAULT_UPSTREAM_SEARCH_LIMIT), indexers=indexers)
        if should_search_sukebei(getattr(args, "category", "movie"), args.query):
            raw_candidates.extend(search_sukebei_indexer_results(prowlarr, args.query, indexers=indexers))
        raw_candidates.extend(search_anime_indexer_results(prowlarr, args.query, indexers=indexers))
        candidates = ResourceSelector().select_ranked_limited(raw_candidates, query=args.query, limit=args.limit)
        if args.command == "search":
            print(
                json.dumps(
                    {
                        "query": args.query,
                        "count": len(candidates),
                        "results": [public_resource_summary(candidate) for candidate in candidates],
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                )
            )
            return 0

        selected = select_candidate_by_rank(candidates, args.rank)
        folder_id = category_to_folder_id(args.category)
        if not args.commit:
            print(
                json.dumps(
                    {
                        "dry_run": True,
                        "category": args.category,
                        "folder_id": folder_id,
                        "query": args.query,
                        "selected_rank": args.rank,
                        "selected": public_resource_summary(selected),
                        "results": [public_resource_summary(candidate) for candidate in candidates],
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                )
            )
            return 0

        client, _token = build_115_client_after_openlist_warm(args, args.category)
        download_uri = selected["download_uri"]
        if is_prowlarr_download_uri(download_uri):
            download_uri = prowlarr.resolve_download_uri(download_uri)
        result = client.add_offline_urls([download_uri], folder_id)
        print(json.dumps({"selected": public_resource_summary(selected), "submit": summarize_offline_submit(result)}, ensure_ascii=False, sort_keys=True))
        return 0 if result.get("state") is True else 2

    if args.command == "verify-folders":
        client, _token = build_115_client_after_openlist_warm(args, "movie")
        result = {}
        for category, folder_id in sorted(FOLDER_IDS.items()):
            result[category] = {
                "folder_id": folder_id,
                "response": summarize_response(client.get_folder_info(folder_id)),
            }
        print(json.dumps(result, ensure_ascii=False, sort_keys=True))
        return 0

    if args.command == "probe":
        client, token = build_115_client_after_openlist_warm(args, "movie")
        quota = client.get_quota_info()
        tasks = client.get_offline_tasks(page=1)
        print(
            json.dumps(
                {
                    "storage_id": token.storage_id,
                    "mount_path": token.mount_path,
                    "quota": summarize_response(quota),
                    "tasks": summarize_response(tasks),
                },
                ensure_ascii=False,
                sort_keys=True,
            )
        )
        return 0

    if args.command == "task-status":
        client, _token = build_115_client_after_openlist_warm(args, "movie")
        task = find_task_by_info_hash(client, args.info_hash, max_pages=args.max_pages)
        print(json.dumps(public_task_summary(task), ensure_ascii=False, sort_keys=True))
        return 0

    if args.command == "wait-task":
        client, _token = build_115_client_after_openlist_warm(args, "movie")
        task = wait_for_task(
            client,
            args.info_hash,
            timeout_seconds=args.timeout_seconds,
            interval_seconds=args.interval_seconds,
            max_pages=args.max_pages,
        )
        print(json.dumps(public_task_summary(task), ensure_ascii=False, sort_keys=True))
        return 0

    if args.command == "add-offline":
        folder_id = category_to_folder_id(args.category)
        if not args.commit:
            print(
                json.dumps(
                    {
                        "dry_run": True,
                        "category": args.category,
                        "folder_id": folder_id,
                        "url_count": len(args.url),
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                )
            )
            return 0

        client, _token = build_115_client_after_openlist_warm(args, args.category)
        result = client.add_offline_urls(args.url, folder_id)
        print(json.dumps(summarize_offline_submit(result), ensure_ascii=False, sort_keys=True))
        return 0 if result.get("state") is True else 2

    raise RuntimeError("unreachable command: %s" % args.command)


def run(argv=None):
    try:
        return main(argv)
    except (RuntimeError, TimeoutError, ValueError) as exc:
        print("error: %s" % exc, file=sys.stderr)
        return 1


def summarize_response(response):
    data = response.get("data")
    summary = {
        "state": response.get("state"),
        "code": response.get("code"),
        "message": response.get("message") or response.get("msg"),
    }
    if isinstance(data, list):
        summary["data_len"] = len(data)
    elif isinstance(data, dict):
        summary["data_keys"] = sorted(data.keys())[:30]
        for key in ("count", "quota", "total", "used", "page_count", "task_count"):
            if key in data:
                summary[key] = data[key]
    return summary


def summarize_offline_submit(response):
    summary = {
        "state": response.get("state"),
        "code": response.get("code"),
        "message": response.get("message") or response.get("msg"),
        "tasks": [],
    }
    data = response.get("data") or []
    if isinstance(data, list):
        for item in data:
            summary["tasks"].append(
                {
                    "info_hash": item.get("info_hash"),
                    "state": item.get("state"),
                    "code": item.get("code"),
                    "message": item.get("message") or item.get("msg"),
                }
            )
    return summary


def build_prowlarr_client(args):
    api_key = ProwlarrConfig(args.prowlarr_config).load_api_key()
    return ProwlarrClient(args.prowlarr_url, api_key)


def build_msg_client(args):
    if not args.msg_admin_user or not args.msg_admin_password:
        raise RuntimeError("MediaStationGo credentials missing")
    return MediaStationClient(args.msg_base_url, args.msg_admin_user, args.msg_admin_password)


def build_openlist_client(args):
    token = OpenListTokenProvider().load_token()
    return OpenListClient(args.openlist_url, token)


def warm_openlist(args, category):
    build_openlist_client(args).list_path(category_to_openlist_path(category))


def build_115_client_after_openlist_warm(args, category):
    warm_openlist(args, category)
    token = OpenListTokenStore(args.openlist_db).load_access_token()
    return Client115(token.access_token), token


def public_resource_summary(resource):
    summary = {
        "title": resource.get("title"),
        "indexer": resource.get("indexer"),
        "seeders": resource.get("seeders"),
        "size": resource.get("size"),
        "score": resource.get("score"),
        "download_uri": redact_sensitive_url(resource.get("download_uri")),
    }
    if resource.get("rank") is not None:
        summary["rank"] = resource.get("rank")
    return summary


def select_candidate_by_rank(candidates, rank):
    if rank < 1 or rank > len(candidates):
        raise RuntimeError("resource rank out of range: %s" % rank)
    return candidates[rank - 1]


def public_task_summary(task):
    return {
        "info_hash": task.get("info_hash"),
        "name": task.get("name"),
        "status": task.get("status"),
        "status_name": task.get("status_name"),
        "percent_done": task.get("percent_done"),
        "size": task.get("size"),
        "file_id": task.get("file_id"),
        "wp_path_id": task.get("wp_path_id"),
    }


def redact_sensitive_url(value):
    if not value:
        return value
    parsed = urllib.parse.urlsplit(value)
    if not parsed.query:
        return value
    params = urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)
    redacted = []
    changed = False
    for key, val in params:
        if key.lower() in ("apikey", "api_key", "token", "access_token"):
            redacted.append((key, "REDACTED"))
            changed = True
        else:
            redacted.append((key, val))
    if not changed:
        return value
    return urllib.parse.urlunsplit(
        (
            parsed.scheme,
            parsed.netloc,
            parsed.path,
            urllib.parse.urlencode(redacted),
            parsed.fragment,
        )
    )


if __name__ == "__main__":
    sys.exit(run())
