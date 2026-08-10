import json
import sqlite3
from dataclasses import dataclass

from pipeline.openlist import OpenListTransport


@dataclass(frozen=True)
class OpenListAccessToken:
    storage_id: int
    mount_path: str
    access_token: str


def load_access_token_from_api(base_url, admin_token, transport=None, timeout=30):
    base_url = str(base_url or "").rstrip("/")
    admin_token = str(admin_token or "").strip()
    if not base_url:
        raise RuntimeError("OpenList API URL missing")
    if not admin_token:
        raise RuntimeError("OpenList admin token missing")

    client = transport or OpenListTransport()
    response = client.request(
        "GET",
        base_url + "/api/admin/storage/list",
        headers={"Authorization": admin_token},
        timeout=timeout,
    )
    if not isinstance(response, dict) or response.get("code") != 200:
        raise RuntimeError("OpenList storage list failed: %s" % ((response or {}).get("message") or (response or {}).get("code")))

    rows = ((response.get("data") or {}).get("content") or [])
    for row in rows:
        if not isinstance(row, dict) or row.get("driver") != "115 Open" or row.get("disabled"):
            continue
        addition = row.get("addition") or {}
        if isinstance(addition, str):
            try:
                addition = json.loads(addition)
            except (TypeError, ValueError) as exc:
                raise RuntimeError("115 Open storage addition is invalid") from exc
        if not isinstance(addition, dict):
            raise RuntimeError("115 Open storage addition is invalid")
        access_token = str(addition.get("access_token") or "").strip()
        if not access_token:
            raise RuntimeError("access_token missing in OpenList 115 Open storage")
        return OpenListAccessToken(
            storage_id=row.get("id"),
            mount_path=row.get("mount_path") or "",
            access_token=access_token,
        )

    raise RuntimeError("enabled 115 Open storage not found")

class OpenListTokenStore:
    def __init__(self, db_path):
        self.db_path = str(db_path)

    def load_access_token(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            row = conn.execute(
                """
                select id, mount_path, addition
                from x_storages
                where driver = ? and disabled = 0
                order by id
                limit 1
                """,
                ("115 Open",),
            ).fetchone()
        finally:
            conn.close()

        if row is None:
            raise RuntimeError("enabled 115 Open storage not found")

        addition = json.loads(row["addition"] or "{}")
        access_token = addition.get("access_token")
        if not access_token:
            raise RuntimeError("access_token missing in OpenList 115 Open storage")

        return OpenListAccessToken(
            storage_id=row["id"],
            mount_path=row["mount_path"],
            access_token=access_token,
        )
