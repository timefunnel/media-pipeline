import json
import sqlite3
from dataclasses import dataclass


@dataclass(frozen=True)
class OpenListAccessToken:
    storage_id: int
    mount_path: str
    access_token: str


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
