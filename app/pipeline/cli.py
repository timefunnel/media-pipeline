import argparse
import json
import os
import sys

from pipeline.client115 import Client115
from pipeline.bot import build_bot
from pipeline.config import FOLDER_IDS, category_to_openlist_path
from pipeline.mediastation import DEFAULT_MSG_BASE_URL, MediaStationClient
from pipeline.openlist import DEFAULT_OPENLIST_URL, OpenListClient, OpenListTokenProvider
from pipeline.openlist_tokens import OpenListTokenStore


DEFAULT_OPENLIST_DB = "/openlist-data/data.db"


def build_parser():
    parser = argparse.ArgumentParser(prog="media-pipeline")
    parser.add_argument("--openlist-db", default=os.environ.get("OPENLIST_DB", DEFAULT_OPENLIST_DB))
    parser.add_argument("--openlist-url", default=os.environ.get("OPENLIST_URL", DEFAULT_OPENLIST_URL))
    parser.add_argument("--msg-base-url", default=os.environ.get("MSG_BASE_URL", DEFAULT_MSG_BASE_URL))
    parser.add_argument("--msg-admin-user", default=os.environ.get("MSG_ADMIN_USER", ""))
    parser.add_argument("--msg-admin-password", default=os.environ.get("MSG_ADMIN_PASSWORD", ""))
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("folders")
    subparsers.add_parser("verify-folders")
    subparsers.add_parser("bot")
    subparsers.add_parser("msg-login")

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


if __name__ == "__main__":
    sys.exit(run())
