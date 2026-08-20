import json
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from http.cookiejar import CookieJar


API_BASE = "https://proapi.115.com"
ADD_OFFLINE_URLS = API_BASE + "/open/offline/add_task_urls"
DELETE_OFFLINE_TASK = API_BASE + "/open/offline/del_task"
OFFLINE_TASK_LIST = API_BASE + "/open/offline/get_task_list"
OFFLINE_QUOTA_INFO = API_BASE + "/open/offline/get_quota_info"
FOLDER_INFO = API_BASE + "/open/folder/get_info"
FOLDER_ADD = API_BASE + "/open/folder/add"
FILE_LIST = API_BASE + "/open/ufile/files"
FILE_MOVE = API_BASE + "/open/ufile/move"
FILE_DELETE = API_BASE + "/open/ufile/delete"
QRCODE_TOKEN = "https://qrcodeapi.115.com/api/1.0/web/1.0/token"
QRCODE_STATUS = "https://qrcodeapi.115.com/get/status/"
QRCODE_LOGIN_WITH_APP = "https://passportapi.115.com/app/1.0/%s/1.0/login/qrcode"
QRCODE_IMAGE = "https://qrcodeapi.115.com/api/1.0/mac/1.0/qrcode?uid=%s"
SHARE_WEB_BASE = "https://115cdn.com"
SHARE_PAGE_URL = SHARE_WEB_BASE + "/s/%s"
SHARE_SNAP = SHARE_WEB_BASE + "/webapi/share/snap"
SHARE_RECEIVE = SHARE_WEB_BASE + "/webapi/share/receive"
SHARE_DOMAIN_PATTERN = re.compile(
    r"((?:https?://)?(?:www\.)?(?:115|anxia|115cdn)\.com/s/[^\s<>\"']+)",
    re.IGNORECASE,
)


class UrllibTransport:
    def request(self, method, url, headers=None, data=None, timeout=None):
        req_headers = dict(headers or {})
        body = None
        if data is not None:
            body = urllib.parse.urlencode(data).encode("utf-8")
            req_headers["Content-Type"] = "application/x-www-form-urlencoded"

        request = urllib.request.Request(url, data=body, headers=req_headers, method=method)
        with urllib.request.urlopen(request, timeout=timeout) as response:
            raw = response.read().decode("utf-8", "replace")
        return json.loads(raw)


class UrllibShareTransport:
    def new_cookie_jar(self):
        return CookieJar()

    def request(self, method, url, headers=None, data=None, timeout=None, cookie_jar=None, parse_json=True):
        req_headers = dict(headers or {})
        body = None
        if data is not None:
            body = urllib.parse.urlencode(data).encode("utf-8")
            req_headers.setdefault("Content-Type", "application/x-www-form-urlencoded;charset=UTF-8")

        request = urllib.request.Request(url, data=body, headers=req_headers, method=method)
        opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cookie_jar)) if cookie_jar is not None else urllib.request.build_opener()
        try:
            with opener.open(request, timeout=timeout) as response:
                raw = response.read().decode("utf-8", "replace")
        except urllib.error.HTTPError as exc:
            raw = exc.read().decode("utf-8", "replace")
            raise RuntimeError("115 share request failed: HTTP %s %s" % (exc.code, raw[:300])) from exc
        if not parse_json:
            return raw
        try:
            return json.loads(raw)
        except ValueError as exc:
            raise RuntimeError("115 share response is not JSON: %s" % raw[:300]) from exc


@dataclass(frozen=True)
class Share115Url:
    url: str
    share_code: str
    receive_code: str = ""
    pdir_fid: str = "0"


class Client115:
    def __init__(self, access_token, transport=None, timeout=30):
        self.access_token = access_token
        self.transport = transport or UrllibTransport()
        self.timeout = timeout

    def add_offline_urls(self, urls, folder_id):
        if not urls:
            raise ValueError("urls must not be empty")
        return self._post(
            ADD_OFFLINE_URLS,
            {
                "urls": "\n".join(urls),
                "wp_path_id": str(folder_id),
            },
        )

    def delete_offline_task(self, info_hash, delete_files=False):
        info_hash = str(info_hash or "").strip()
        if not info_hash:
            raise ValueError("info_hash must not be empty")
        return self._post(
            DELETE_OFFLINE_TASK,
            {
                "info_hash": info_hash,
                "del_source_file": "1" if delete_files else "0",
            },
        )

    def get_offline_tasks(self, page=1):
        query = urllib.parse.urlencode({"page": page})
        return self._get(OFFLINE_TASK_LIST + "?" + query)

    def get_quota_info(self):
        return self._get(OFFLINE_QUOTA_INFO)

    def get_folder_info(self, folder_id):
        query = urllib.parse.urlencode({"file_id": folder_id})
        return self._get(FOLDER_INFO + "?" + query)

    def create_folder(self, name, parent_id):
        name = str(name or "").strip()
        parent_id = str(parent_id or "").strip()
        if not name:
            raise ValueError("115 folder name must not be empty")
        if not parent_id:
            raise ValueError("115 parent folder id must not be empty")
        response = self._post(FOLDER_ADD, {"file_name": name, "pid": parent_id})
        ensure_115_open_success(response, "create folder")
        return response

    def list_files(self, folder_id, limit=1000, offset=0):
        folder_id = str(folder_id or "").strip()
        if not folder_id:
            raise ValueError("115 folder id must not be empty")
        query = urllib.parse.urlencode(
            {
                "cid": folder_id,
                "limit": max(1, min(int(limit), 7000)),
                "offset": max(0, int(offset)),
                "show_dir": 1,
                "count_folders": 1,
            }
        )
        response = self._get(FILE_LIST + "?" + query)
        ensure_115_open_success(response, "list folder")
        return response

    def list_all_files(self, folder_id, page_size=1000):
        return self.list_all_files_with_request_count(folder_id, page_size=page_size)[0]

    def list_all_files_with_request_count(self, folder_id, page_size=1000):
        offset = 0
        out = []
        request_count = 0
        while True:
            response = self.list_files(folder_id, limit=page_size, offset=offset)
            request_count += 1
            raw_data = response.get("data") or []
            data = raw_data
            if isinstance(raw_data, dict):
                data = raw_data.get("data") or raw_data.get("list") or raw_data.get("content") or []
            if not isinstance(data, list):
                raise RuntimeError("115 list folder returned invalid data")
            out.extend(data)
            count = int(response.get("count") or (raw_data.get("count") if isinstance(raw_data, dict) else 0) or len(out))
            if not data or len(out) >= count:
                return out, request_count
            offset += len(data)

    def move_files(self, file_ids, target_folder_id):
        ids = [str(value or "").strip() for value in file_ids]
        ids = [value for value in ids if value]
        target_folder_id = str(target_folder_id or "").strip()
        if not ids:
            raise ValueError("115 move file ids must not be empty")
        if not target_folder_id:
            raise ValueError("115 move target folder id must not be empty")
        response = self._post(FILE_MOVE, {"file_ids": ",".join(ids), "to_cid": target_folder_id})
        ensure_115_open_success(response, "move files")
        return response

    def delete_files(self, file_ids):
        ids = [str(value or "").strip() for value in file_ids]
        ids = [value for value in ids if value]
        if not ids:
            raise ValueError("115 delete file ids must not be empty")
        response = self._post(FILE_DELETE, {"file_ids": ",".join(ids)})
        ensure_115_open_success(response, "delete files")
        return response

    def _get(self, url):
        return self.transport.request("GET", url, headers=self._headers(), timeout=self.timeout)

    def _post(self, url, data):
        return self.transport.request("POST", url, headers=self._headers(), data=data, timeout=self.timeout)

    def _headers(self):
        return {
            "Authorization": "Bearer " + self.access_token,
            "User-Agent": "media-pipeline/0.1",
        }


class Share115Client:
    USER_AGENT = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36"
    )

    def __init__(self, cookie, transport=None, timeout=30):
        self.cookie = str(cookie or "").strip()
        if not self.cookie:
            raise RuntimeError("P115_COOKIE missing")
        self.transport = transport or UrllibShareTransport()
        self.timeout = timeout

    def get_share_info(self, share_code, receive_code="", cid="0", limit=50):
        share_code = str(share_code or "").strip()
        if not share_code:
            raise ValueError("share_code must not be empty")
        cookie_jar = self._create_share_session(share_code, receive_code)
        return self._snap(cookie_jar, share_code, receive_code, cid=cid, limit=limit)

    def receive_share_url(self, url, folder_id):
        parsed = parse_115_share_url(url)
        if parsed is None:
            raise ValueError("not a 115 share url")
        return self.receive_share(
            parsed.share_code,
            parsed.receive_code,
            folder_id,
            cid=parsed.pdir_fid,
            source_url=parsed.url,
        )

    def receive_share(self, share_code, receive_code, folder_id, cid="0", source_url=""):
        share_code = str(share_code or "").strip()
        receive_code = str(receive_code or "").strip()
        folder_id = str(folder_id or "").strip()
        cid = str(cid or "0").strip() or "0"
        if not share_code:
            raise ValueError("share_code must not be empty")
        if not folder_id:
            raise ValueError("folder_id must not be empty")

        cookie_jar = self._create_share_session(share_code, receive_code)
        items, manifest_items, manifest_request_count = self._inspect_share_tree(
            cookie_jar,
            share_code,
            receive_code,
            cid=cid,
        )
        if not items:
            raise RuntimeError("115 share has no receivable items")

        file_ids = [item["file_id"] for item in items if item.get("file_id")]
        if not file_ids:
            raise RuntimeError("115 share items missing file_id")

        response = self.transport.request(
            "POST",
            SHARE_RECEIVE,
            headers=self._receive_headers(share_code, receive_code, cookie_jar),
            data={
                "cid": folder_id,
                "share_code": share_code,
                "receive_code": receive_code,
                "file_id": ",".join(file_ids),
            },
            timeout=self.timeout,
            cookie_jar=cookie_jar,
        )
        if response.get("state") is not True:
            raise RuntimeError(
                "115 share receive failed: %s"
                % (response.get("error") or response.get("message") or response.get("msg") or response.get("code"))
            )

        data = response.get("data") or {}
        return {
            "state": True,
            "code": response.get("code", 0),
            "message": response.get("message") or response.get("msg") or response.get("error") or "115 share receive success",
            "data": {
                "share_code": share_code,
                "source_url": source_url,
                "cid": folder_id,
                "source_cid": cid,
                "items": items,
                "manifest_items": manifest_items,
                "manifest_request_count": manifest_request_count,
                "file_ids": file_ids,
                "save_as_top_fids": data.get("save_as_top_fids") or data.get("file_id") or [],
                "raw": data,
            },
        }

    def _create_share_session(self, share_code, receive_code=""):
        cookie_jar = self.transport.new_cookie_jar() if hasattr(self.transport, "new_cookie_jar") else None
        page_url = SHARE_PAGE_URL % urllib.parse.quote(str(share_code), safe="")
        if receive_code:
            page_url = page_url + "?password=" + urllib.parse.quote(str(receive_code), safe="")
        try:
            self.transport.request(
                "GET",
                page_url,
                headers=self._share_headers(share_code, receive_code),
                timeout=self.timeout,
                cookie_jar=cookie_jar,
                parse_json=False,
            )
        except RuntimeError:
            # 访问分享页只是为了获取会话 cookie；真正的失败以后续 API 响应为准。
            pass
        return cookie_jar

    def _inspect_share_tree(
        self,
        cookie_jar,
        share_code,
        receive_code="",
        cid="0",
        page_size=1000,
        max_entries=20000,
    ):
        request_count = 0

        def list_items(folder_id):
            nonlocal request_count
            offset = 0
            out = []
            while True:
                request_count += 1
                response = self._snap(
                    cookie_jar,
                    share_code,
                    receive_code,
                    cid=folder_id,
                    limit=page_size,
                    offset=offset,
                )
                page = extract_115_share_items(response)
                out.extend(page)
                if len(out) > int(max_entries):
                    raise RuntimeError("115 share entry limit exceeded")
                data = response.get("data") or {}
                total = int(data.get("count") or data.get("total") or 0)
                if not page or len(page) < int(page_size) or (total and len(out) >= total):
                    return out
                offset += len(page)

        top_items = list_items(cid)
        manifest = list(top_items)
        stack = [item["file_id"] for item in top_items if item.get("is_dir")]
        while stack:
            children = list_items(stack.pop())
            manifest.extend(children)
            if len(manifest) > int(max_entries):
                raise RuntimeError("115 share entry limit exceeded")
            stack.extend(item["file_id"] for item in children if item.get("is_dir"))
        return top_items, manifest, request_count

    def _snap(self, cookie_jar, share_code, receive_code="", cid="0", limit=50, offset=0):
        query = urllib.parse.urlencode(
            {
                "share_code": share_code,
                "offset": max(0, int(offset)),
                "limit": int(limit),
                "asc": 0,
                "cid": "" if str(cid or "0") == "0" else str(cid),
                "receive_code": receive_code or "",
                "format": "json",
            }
        )
        response = self.transport.request(
            "GET",
            SHARE_SNAP + "?" + query,
            headers=self._share_headers(share_code, receive_code),
            timeout=self.timeout,
            cookie_jar=cookie_jar,
        )
        if response.get("state") is not True:
            shareinfo = (response.get("data") or {}).get("shareinfo") or {}
            raise RuntimeError(
                "115 share snap failed: %s"
                % (shareinfo.get("forbid_reason") or response.get("error") or response.get("message") or response.get("code"))
            )
        return response

    def _share_headers(self, share_code, receive_code=""):
        return {
            "User-Agent": self.USER_AGENT,
            "Accept": "*/*",
            "Referer": self._referer(share_code, receive_code),
        }

    def _receive_headers(self, share_code, receive_code, cookie_jar):
        return {
            "User-Agent": self.USER_AGENT,
            "Accept": "*/*",
            "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8",
            "Origin": SHARE_WEB_BASE,
            "Referer": self._referer(share_code, receive_code),
            "Cookie": merge_cookie_header(cookie_jar, self.cookie),
        }

    def _referer(self, share_code, receive_code=""):
        ref = SHARE_PAGE_URL % urllib.parse.quote(str(share_code), safe="")
        if receive_code:
            ref = ref + "?password=" + urllib.parse.quote(str(receive_code), safe="")
        return ref + "&"


def is_115_share_url(value):
    return parse_115_share_url(value) is not None


def parse_115_share_url(value):
    text = str(value or "").strip()
    match = SHARE_DOMAIN_PATTERN.search(text)
    if not match:
        return None

    raw_url = match.group(1).rstrip(".,;，。；)）]】>\"'")
    if not re.match(r"^https?://", raw_url, re.IGNORECASE):
        raw_url = "https://" + raw_url
    parsed = urllib.parse.urlsplit(raw_url)
    share_match = re.search(r"/s/([^/?#&]+)", parsed.path)
    if not share_match:
        return None

    share_code = urllib.parse.unquote(share_match.group(1)).strip()
    if not share_code:
        return None

    query = urllib.parse.parse_qs(parsed.query)
    receive_code = first_query_value(query, "password") or first_query_value(query, "pwd") or ""
    pdir_fid = "0"
    fragment = urllib.parse.unquote(parsed.fragment or "")
    pdir_match = re.search(r"(?:^|/)list/share/([0-9A-Za-z_]+)", fragment)
    if pdir_match:
        pdir_fid = pdir_match.group(1)
    elif fragment and re.match(r"^[0-9A-Za-z]+$", fragment):
        receive_code = receive_code or fragment

    if not receive_code:
        code_match = re.search(r"(?:提取码|访问码|密码|password|pwd)\s*[：: ]\s*([0-9A-Za-z]+)", text, re.IGNORECASE)
        if code_match:
            receive_code = code_match.group(1)

    return Share115Url(url=raw_url, share_code=share_code, receive_code=receive_code, pdir_fid=pdir_fid)


def first_query_value(query, key):
    values = query.get(key) or []
    for value in values:
        value = str(value or "").strip()
        if value:
            return value
    return ""


def extract_115_share_items(response):
    out = []
    data = response.get("data") or {}
    for item in data.get("list") or []:
        file_id = str(item.get("fid") or item.get("cid") or "").strip()
        if not file_id:
            continue
        is_dir = not item.get("fid")
        out.append(
            {
                "file_id": file_id,
                "name": item.get("n") or "",
                "size": int(item.get("s") or 0),
                "is_dir": bool(is_dir),
            }
        )
    return out


def ensure_115_open_success(response, operation):
    if not isinstance(response, dict):
        raise RuntimeError("115 %s returned invalid response" % operation)
    if response.get("state") is True and int(response.get("code") or 0) == 0:
        return response
    raise RuntimeError(
        "115 %s failed: %s"
        % (operation, response.get("message") or response.get("error") or response.get("code"))
    )


def merge_cookie_header(cookie_jar, user_cookie):
    parts = []
    if cookie_jar is not None:
        for cookie in cookie_jar:
            if cookie.name and cookie.value:
                parts.append("%s=%s" % (cookie.name, cookie.value))
    for item in str(user_cookie or "").split(";"):
        item = item.strip()
        if "=" in item:
            parts.append(item)
    return "; ".join(unique_cookie_parts(parts))


def unique_cookie_parts(parts):
    seen = set()
    out = []
    for part in parts:
        key = part.split("=", 1)[0].strip()
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(part)
    return out


def share_receive_task_id(share_code, now=None):
    timestamp = int(time.time() if now is None else now)
    safe_code = re.sub(r"[^0-9A-Za-z_-]+", "", str(share_code or "")) or "unknown"
    return "share:%s:%s" % (safe_code[:24], timestamp)


class P115QRCodeLoginClient:
    USER_AGENT = Share115Client.USER_AGENT

    def __init__(self, transport=None, timeout=30, app="web"):
        self.transport = transport or UrllibShareTransport()
        self.timeout = timeout
        self.app = str(app or "web").strip() or "web"

    def start(self):
        response = self.transport.request(
            "GET",
            QRCODE_TOKEN,
            headers=self._headers(),
            timeout=self.timeout,
        )
        if int(response.get("state") or 0) != 1:
            raise RuntimeError("115 QR token failed: %s" % (response.get("error") or response.get("message") or response.get("code")))
        data = response.get("data") or {}
        uid = str(data.get("uid") or "").strip()
        sign = str(data.get("sign") or "").strip()
        qrcode = str(data.get("qrcode") or "").strip()
        qrcode_time = data.get("time")
        if not uid or not sign or not qrcode_time:
            raise RuntimeError("115 QR token response missing uid/sign/time")
        return {
            "uid": uid,
            "sign": sign,
            "time": int(qrcode_time),
            "qrcode": qrcode,
            "qrcode_url": QRCODE_IMAGE % urllib.parse.quote(uid, safe=""),
            "app": self.app,
        }

    def status(self, session):
        uid = str((session or {}).get("uid") or "").strip()
        sign = str((session or {}).get("sign") or "").strip()
        qrcode_time = str((session or {}).get("time") or "").strip()
        if not uid or not sign or not qrcode_time:
            raise ValueError("115 QR session missing uid/sign/time")
        query = urllib.parse.urlencode(
            {
                "uid": uid,
                "time": qrcode_time,
                "sign": sign,
                "_": int(time.time() * 1000),
            }
        )
        response = self.transport.request(
            "GET",
            QRCODE_STATUS + "?" + query,
            headers=self._headers(),
            timeout=self.timeout,
        )
        if int(response.get("state") or 0) != 1:
            raise RuntimeError("115 QR status failed: %s" % (response.get("error") or response.get("message") or response.get("code")))
        data = response.get("data") or {}
        return {
            "status": int(data.get("status") or 0),
            "message": data.get("msg") or "",
            "version": data.get("version") or "",
        }

    def login(self, session, require_confirmed=True):
        if require_confirmed:
            status = self.status(session)
            if status.get("status") != 2:
                raise RuntimeError("115 QR login is not confirmed: %s" % qrcode_status_label(status.get("status")))
        uid = str((session or {}).get("uid") or "").strip()
        app = str((session or {}).get("app") or self.app or "web").strip() or "web"
        response = self.transport.request(
            "POST",
            QRCODE_LOGIN_WITH_APP % urllib.parse.quote(app, safe=""),
            headers=self._headers(),
            data={"account": uid, "app": app},
            timeout=self.timeout,
        )
        if int(response.get("state") or 0) != 1:
            raise RuntimeError("115 QR login failed: %s" % (response.get("error") or response.get("message") or response.get("code")))
        data = response.get("data") or {}
        credential = data.get("cookie") or data
        cookie = p115_cookie_from_credential(credential)
        if not p115_cookie_is_valid(cookie):
            raise RuntimeError("115 QR login returned invalid cookie")
        return cookie

    def _headers(self):
        return {
            "User-Agent": self.USER_AGENT,
            "Accept": "*/*",
        }


def p115_cookie_from_credential(credential):
    if isinstance(credential, str):
        return credential.strip()
    values = {}
    for key, value in (credential or {}).items():
        values[str(key).upper()] = str(value or "").strip()
    return "UID=%s;CID=%s;SEID=%s;KID=%s" % (
        values.get("UID", ""),
        values.get("CID", ""),
        values.get("SEID", ""),
        values.get("KID", ""),
    )


def p115_cookie_is_valid(cookie):
    values = {}
    for item in str(cookie or "").split(";"):
        item = item.strip()
        if "=" not in item:
            continue
        key, value = item.split("=", 1)
        values[key.strip().upper()] = value.strip()
    return bool(values.get("UID") and values.get("CID") and values.get("SEID"))


def mask_p115_cookie(cookie):
    values = {}
    for item in str(cookie or "").split(";"):
        item = item.strip()
        if "=" not in item:
            continue
        key, value = item.split("=", 1)
        values[key.strip().upper()] = value.strip()
    uid = values.get("UID") or ""
    if len(uid) <= 6:
        masked_uid = "***" if uid else "-"
    else:
        masked_uid = uid[:3] + "***" + uid[-3:]
    return "UID=%s; CID=%s; SEID=%s; KID=%s" % (
        masked_uid,
        "已保存" if values.get("CID") else "缺失",
        "已保存" if values.get("SEID") else "缺失",
        "已保存" if values.get("KID") else "缺失",
    )


def qrcode_status_label(status):
    return {
        0: "等待扫码",
        1: "已扫码，等待手机确认",
        2: "已确认",
        -1: "二维码已过期",
        -2: "已取消",
    }.get(int(status or 0), "未知状态")
