import json
import urllib.parse
import urllib.request


API_BASE = "https://proapi.115.com"
ADD_OFFLINE_URLS = API_BASE + "/open/offline/add_task_urls"
DELETE_OFFLINE_TASK = API_BASE + "/open/offline/del_task"
OFFLINE_TASK_LIST = API_BASE + "/open/offline/get_task_list"
OFFLINE_QUOTA_INFO = API_BASE + "/open/offline/get_quota_info"
FOLDER_INFO = API_BASE + "/open/folder/get_info"


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

    def _get(self, url):
        return self.transport.request("GET", url, headers=self._headers(), timeout=self.timeout)

    def _post(self, url, data):
        return self.transport.request("POST", url, headers=self._headers(), data=data, timeout=self.timeout)

    def _headers(self):
        return {
            "Authorization": "Bearer " + self.access_token,
            "User-Agent": "media-pipeline/0.1",
        }
