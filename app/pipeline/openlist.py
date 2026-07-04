import json
import os
import urllib.error
import urllib.request


DEFAULT_OPENLIST_URL = "http://127.0.0.1:5244"
DEFAULT_OPENLIST_TOKEN_FILE = "/run/secrets/openlist_token"
DEFAULT_LIST_PAGE_SIZE = 200
DEFAULT_REMOVE_BATCH_SIZE = 50
DEFAULT_META_LIST_PAGE_SIZE = 200


class OpenListTokenProvider:
    def __init__(self, env=None):
        self.env = env if env is not None else os.environ

    def load_token(self):
        token = (self.env.get("OPENLIST_TOKEN") or "").strip()
        if token:
            return token

        token_file = (self.env.get("OPENLIST_TOKEN_FILE") or DEFAULT_OPENLIST_TOKEN_FILE).strip()
        if token_file and os.path.exists(token_file):
            with open(token_file, "r", encoding="utf-8") as handle:
                token = handle.read().strip()
            if token:
                return token

        raise RuntimeError("OpenList token missing; set OPENLIST_TOKEN or OPENLIST_TOKEN_FILE")


class OpenListTransport:
    def request(self, method, url, headers=None, data=None, timeout=None):
        body = None
        req_headers = dict(headers or {})
        if data is not None:
            body = json.dumps(data, ensure_ascii=False).encode("utf-8")
            req_headers["Content-Type"] = "application/json"
        request = urllib.request.Request(url, data=body, headers=req_headers, method=method)
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                raw = response.read().decode("utf-8", "replace")
        except urllib.error.HTTPError as exc:
            raw = exc.read().decode("utf-8", "replace")
            try:
                data = json.loads(raw)
                message = data.get("message") or data.get("description") or raw.strip()
            except (TypeError, ValueError):
                message = raw.strip() or str(exc)
            raise RuntimeError("OpenList request failed: %s" % message) from exc
        except (TimeoutError, urllib.error.URLError) as exc:
            raise RuntimeError("OpenList request failed: %s" % exc) from exc
        return json.loads(raw)


class OpenListClient:
    def __init__(self, base_url, token, transport=None, timeout=30):
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.transport = transport or OpenListTransport()
        self.timeout = timeout

    def list_path(self, path, refresh=False, page=1, per_page=1):
        response = self.transport.request(
            "POST",
            self.base_url + "/api/fs/list",
            headers={"Authorization": self.token},
            data={"path": path, "password": "", "page": int(page), "per_page": int(per_page), "refresh": bool(refresh)},
            timeout=self.timeout,
        )
        if response.get("code") != 200:
            raise RuntimeError("OpenList list failed: %s" % (response.get("message") or response.get("code")))
        return response

    def list_all(self, path, refresh=False, per_page=DEFAULT_LIST_PAGE_SIZE):
        page = 1
        items = []
        total = None
        while True:
            response = self.list_path(path, refresh=refresh and page == 1, page=page, per_page=per_page)
            data = response.get("data") or {}
            content = data.get("content") or []
            items.extend(content)
            total = data.get("total", total)
            if not content:
                break
            if total is not None and len(items) >= int(total):
                break
            page_count = int(data.get("page_count") or 0)
            if page_count and page >= page_count:
                break
            if len(content) < int(per_page):
                break
            page += 1
        return items

    def get_path(self, path):
        response = self.transport.request(
            "POST",
            self.base_url + "/api/fs/get",
            headers={"Authorization": self.token},
            data={"path": path, "password": ""},
            timeout=self.timeout,
        )
        if response.get("code") != 200:
            raise RuntimeError("OpenList get failed: %s" % (response.get("message") or response.get("code")))
        return response

    def remove_names(self, dir_path, names, batch_size=DEFAULT_REMOVE_BATCH_SIZE):
        names = [str(name) for name in names if str(name or "").strip()]
        responses = []
        for index in range(0, len(names), int(batch_size)):
            batch = names[index : index + int(batch_size)]
            response = self.transport.request(
                "POST",
                self.base_url + "/api/fs/remove",
                headers={"Authorization": self.token},
                data={"dir": dir_path, "names": batch},
                timeout=self.timeout,
            )
            if response.get("code") != 200:
                raise RuntimeError("OpenList remove failed: %s" % (response.get("message") or response.get("code")))
            responses.append(response)
        return responses

    def rename_path(self, path, name):
        response = self.transport.request(
            "POST",
            self.base_url + "/api/fs/rename",
            headers={"Authorization": self.token},
            data={"path": path, "name": name},
            timeout=self.timeout,
        )
        if response.get("code") != 200:
            raise RuntimeError("OpenList rename failed: %s" % (response.get("message") or response.get("code")))
        return response

    def list_metas(self, page=1, per_page=DEFAULT_META_LIST_PAGE_SIZE):
        response = self.transport.request(
            "GET",
            self.base_url + "/api/admin/meta/list?page=%d&per_page=%d" % (int(page), int(per_page)),
            headers={"Authorization": self.token},
            timeout=self.timeout,
        )
        if response.get("code") != 200:
            raise RuntimeError("OpenList meta list failed: %s" % (response.get("message") or response.get("code")))
        return response

    def all_metas(self, per_page=DEFAULT_META_LIST_PAGE_SIZE):
        page = 1
        metas = []
        total = None
        while True:
            response = self.list_metas(page=page, per_page=per_page)
            data = response.get("data") or {}
            content = data.get("content") or []
            metas.extend(content)
            total = data.get("total", total)
            if not content:
                break
            if total is not None and len(metas) >= int(total):
                break
            if len(content) < int(per_page):
                break
            page += 1
        return metas

    def upsert_meta_hide(self, path, hide_patterns, h_sub=True):
        path = str(path or "").strip()
        if not path:
            raise ValueError("OpenList meta path missing")
        merged_hide = merge_meta_hide_patterns("", hide_patterns)
        if not merged_hide:
            return {"code": 200, "message": "skipped", "data": None}

        existing = None
        for meta in self.all_metas():
            if str(meta.get("path") or "") == path:
                existing = dict(meta)
                break

        if existing is not None:
            existing["hide"] = merge_meta_hide_patterns(existing.get("hide"), hide_patterns)
            existing["h_sub"] = bool(h_sub or existing.get("h_sub"))
            endpoint = "/api/admin/meta/update"
            payload = existing
        else:
            endpoint = "/api/admin/meta/create"
            payload = {
                "path": path,
                "read_users": [],
                "read_users_sub": False,
                "write_users": [],
                "write_users_sub": False,
                "password": "",
                "p_sub": False,
                "write": False,
                "w_sub": False,
                "hide": merged_hide,
                "h_sub": bool(h_sub),
                "readme": "",
                "r_sub": False,
                "header": "",
                "header_sub": False,
            }

        response = self.transport.request(
            "POST",
            self.base_url + endpoint,
            headers={"Authorization": self.token},
            data=payload,
            timeout=self.timeout,
        )
        if response.get("code") != 200:
            raise RuntimeError("OpenList meta hide update failed: %s" % (response.get("message") or response.get("code")))
        return response

    def move_names(self, src_dir, dst_dir, names):
        names = [str(name) for name in names if str(name or "").strip()]
        if not names:
            raise ValueError("OpenList move names missing")
        response = self.transport.request(
            "POST",
            self.base_url + "/api/fs/move",
            headers={"Authorization": self.token},
            data={"src_dir": src_dir, "dst_dir": dst_dir, "names": names},
            timeout=self.timeout,
        )
        if response.get("code") != 200:
            raise RuntimeError("OpenList move failed: %s" % (response.get("message") or response.get("code")))
        return response


def merge_meta_hide_patterns(existing_hide, patterns):
    lines = []
    seen = set()
    for value in str(existing_hide or "").splitlines():
        pattern = value.strip()
        if pattern and pattern not in seen:
            seen.add(pattern)
            lines.append(pattern)
    for value in patterns or []:
        pattern = str(value or "").strip()
        if pattern and pattern not in seen:
            seen.add(pattern)
            lines.append(pattern)
    return "\n".join(lines)
