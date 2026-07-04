import posixpath
import urllib.parse
from collections import OrderedDict

from pipeline.config import MSG_LIBRARY_ROOTS, category_to_msg_library_root, category_to_openlist_path


DEFAULT_MSG_DATABASE_DSN = "postgresql://mediastation:mediastation@127.0.0.1:15432/mediastation"
MSG_CLOUD_PREFIX = "cloud://openlist"


class MediaStationDbClient:
    def __init__(self, dsn=DEFAULT_MSG_DATABASE_DSN, connect=None):
        self.dsn = str(dsn or DEFAULT_MSG_DATABASE_DSN)
        self._connect_override = connect

    def search_migration_candidates(self, query, limit=20):
        query = str(query or "").strip()
        if not query:
            raise ValueError("migration query must not be empty")
        pattern = "%" + query + "%"
        with self._connect() as conn:
            rows = conn.execute(
                """
                select
                    m.id,
                    m.library_id,
                    m.library_root_id,
                    m.series_id,
                    m.title,
                    m.original_name,
                    m.path,
                    m.relative_path,
                    m.size_bytes,
                    m.file_id,
                    l.name as library_name,
                    l.type as library_type,
                    r.path as root_path
                from media m
                left join libraries l on l.id = m.library_id
                left join library_roots r on r.id = m.library_root_id
                where m.deleted_at is null
                  and (
                    coalesce(m.title, '') ilike %s
                    or coalesce(m.original_name, '') ilike %s
                    or coalesce(m.path, '') ilike %s
                  )
                order by m.updated_at desc nulls last, m.created_at desc nulls last
                limit %s
                """,
                (pattern, pattern, pattern, max(1, int(limit)) * 20),
            ).fetchall()
        return build_migration_candidates(rows, limit=limit)

    def validate_migration_target_available(self, candidate, target_category):
        target = build_migration_target(candidate, target_category)
        target_cloud_path = openlist_path_to_cloud_path(target["target_openlist_path"])
        with self._connect() as conn:
            count = conn.execute(
                """
                select count(*)
                from media
                where deleted_at is null
                  and (path = %s or path like %s)
                """,
                (target_cloud_path, target_cloud_path + "/%"),
            ).fetchone()["count"]
        if int(count or 0) > 0:
            raise RuntimeError("MediaStationGo target already exists: %s" % target["target_openlist_path"])
        return target

    def migrate_media_group(self, candidate, target_category):
        target = build_migration_target(candidate, target_category)
        source_cloud_path = openlist_path_to_cloud_path(candidate["source_openlist_path"])
        target_cloud_path = openlist_path_to_cloud_path(target["target_openlist_path"])
        target_root = category_to_msg_library_root(target_category)
        target_root_cloud_path = openlist_path_to_cloud_path(target["target_root_openlist_path"])

        with self._connect() as conn:
            with conn.transaction():
                self._ensure_cloud_media_guard(conn)
                conn.execute("set local media_pipeline.allow_cloud_media_migration = 'on'")
                self._assert_target_available_in_tx(conn, target_cloud_path)
                rows = self._load_source_rows_for_update(conn, candidate, source_cloud_path)
                if not rows:
                    raise RuntimeError("MediaStationGo source media not found: %s" % candidate["source_openlist_path"])

                media_ids = [row["id"] for row in rows]
                series_ids = sorted({row.get("series_id") for row in rows if row.get("series_id")})
                self._assert_series_not_partial(conn, series_ids, media_ids)

                for row in rows:
                    new_path = replace_path_prefix(row["path"], source_cloud_path, target_cloud_path)
                    new_relative_path = cloud_relative_path(new_path, target_root_cloud_path)
                    conn.execute(
                        """
                        update media
                        set library_id = %s,
                            library_root_id = %s,
                            path = %s,
                            relative_path = %s,
                            updated_at = now()
                        where id = %s
                        """,
                        (target_root["library_id"], target_root["root_id"], new_path, new_relative_path, row["id"]),
                    )

                if series_ids:
                    conn.execute(
                        "update series set library_id = %s, updated_at = now() where id = any(%s)",
                        (target_root["library_id"], series_ids),
                    )

                self._update_strm_records(conn, media_ids, source_cloud_path, target_cloud_path)
                self._assert_media_rows_migrated(conn, media_ids, target_root, target_cloud_path)

        return {
            "source_openlist_path": candidate["source_openlist_path"],
            "target_openlist_path": target["target_openlist_path"],
            "target_category": target_category,
            "media_count": len(media_ids),
            "series_count": len(series_ids),
        }

    def _connect(self):
        if self._connect_override is not None:
            return self._connect_override()
        try:
            import psycopg
            from psycopg.rows import dict_row
        except ImportError as exc:
            raise RuntimeError("psycopg missing; rebuild media-pipeline image with Postgres support") from exc
        return psycopg.connect(self.dsn, row_factory=dict_row)

    def _load_source_rows_for_update(self, conn, candidate, source_cloud_path):
        if candidate.get("source_kind") == "file":
            condition = "path = %s"
            params = (source_cloud_path,)
        else:
            condition = "(path = %s or path like %s)"
            params = (source_cloud_path, source_cloud_path + "/%")
        return conn.execute(
            """
            select id, library_id, library_root_id, series_id, path, relative_path, file_id
            from media
            where deleted_at is null and """ + condition + """
            for update
            """,
            params,
        ).fetchall()

    def _assert_target_available_in_tx(self, conn, target_cloud_path):
        count = conn.execute(
            """
            select count(*)
            from media
            where deleted_at is null
              and (path = %s or path like %s)
            """,
            (target_cloud_path, target_cloud_path + "/%"),
        ).fetchone()["count"]
        if int(count or 0) > 0:
            raise RuntimeError("MediaStationGo target already exists: %s" % target_cloud_path)

    def _assert_series_not_partial(self, conn, series_ids, media_ids):
        if not series_ids:
            return
        rows = conn.execute(
            """
            select series_id, count(*) as outside_count
            from media
            where deleted_at is null
              and series_id = any(%s)
              and not (id = any(%s))
            group by series_id
            having count(*) > 0
            """,
            (series_ids, media_ids),
        ).fetchall()
        if rows:
            raise RuntimeError("refuse partial series migration: %s" % rows[0]["series_id"])

    def _update_strm_records(self, conn, media_ids, source_cloud_path, target_cloud_path):
        conn.execute(
            """
            update strm_records
            set file_path = case
                    when file_path = %s or file_path like %s then replace(file_path, %s, %s)
                    else file_path
                end,
                url = case
                    when url = %s or url like %s then replace(url, %s, %s)
                    else url
                end,
                updated_at = now()
            where media_id = any(%s)
            """,
            (
                source_cloud_path,
                source_cloud_path + "/%",
                source_cloud_path,
                target_cloud_path,
                source_cloud_path,
                source_cloud_path + "/%",
                source_cloud_path,
                target_cloud_path,
                media_ids,
            ),
        )

    def _assert_media_rows_migrated(self, conn, media_ids, target_root, target_cloud_path):
        row = conn.execute(
            """
            select count(*) as bad_count
            from media
            where id = any(%s)
              and (
                library_id <> %s
                or library_root_id <> %s
                or not (path = %s or path like %s)
              )
            """,
            (media_ids, target_root["library_id"], target_root["root_id"], target_cloud_path, target_cloud_path + "/%"),
        ).fetchone()
        if int(row["bad_count"] or 0) != 0:
            raise RuntimeError("MediaStationGo migration read-back validation failed")

    def _ensure_cloud_media_guard(self, conn):
        library_ids = [root["library_id"] for root in MSG_LIBRARY_ROOTS.values()]
        root_ids = [root["root_id"] for root in MSG_LIBRARY_ROOTS.values()]
        conn.execute(
            """
            create or replace function public.pipeline_guard_msg_cloud_media()
            returns trigger
            language plpgsql
            as $$
            begin
              if current_setting('media_pipeline.allow_cloud_media_migration', true) = 'on' then
                return new;
              end if;
              if old.library_id = any (%s::varchar[])
                 and old.library_root_id = any (%s::varchar[])
                 and coalesce(old.path, '') like 'cloud://openlist%%' then
                new.library_id := old.library_id;
                new.library_root_id := old.library_root_id;
                new.path := old.path;
                new.relative_path := old.relative_path;
                new.file_id := old.file_id;
              end if;
              return new;
            end;
            $$;
            """,
            (library_ids, root_ids),
        )
        conn.execute(
            """
            do $$
            begin
              if not exists (
                select 1
                from pg_trigger
                where tgname = 'pipeline_guard_msg_cloud_media'
                  and tgrelid = 'public.media'::regclass
              ) then
                create trigger pipeline_guard_msg_cloud_media
                before update on public.media
                for each row execute function public.pipeline_guard_msg_cloud_media();
              end if;
            end;
            $$;
            """
        )


def build_migration_candidates(rows, limit=20):
    grouped = OrderedDict()
    for row in rows or []:
        path = cloud_path_to_openlist_path(row.get("path"))
        root_path = cloud_path_to_openlist_path(row.get("root_path"))
        if not path or not root_path:
            continue
        try:
            source_path, source_kind = media_work_item_path(path, root_path)
        except ValueError:
            continue
        key = (row.get("library_id"), row.get("library_root_id"), source_path)
        if key not in grouped:
            grouped[key] = {
                "title": row.get("title") or row.get("original_name") or posixpath.basename(source_path),
                "library_id": row.get("library_id"),
                "library_root_id": row.get("library_root_id"),
                "library_name": row.get("library_name") or "-",
                "library_type": row.get("library_type") or "-",
                "category": library_id_to_category(row.get("library_id")),
                "source_openlist_path": source_path,
                "source_kind": source_kind,
                "media_count": 0,
                "total_size": 0,
                "sample_path": path,
            }
        grouped[key]["media_count"] += 1
        try:
            grouped[key]["total_size"] += int(row.get("size_bytes") or 0)
        except (TypeError, ValueError):
            pass
    return list(grouped.values())[: int(limit)]


def build_migration_target(candidate, target_category):
    target_category = str(target_category or "").strip()
    source_category = candidate.get("category")
    if source_category and source_category == target_category:
        raise ValueError("target category must be different from source category")
    target_root_path = category_to_openlist_path(target_category)
    source_name = posixpath.basename(str(candidate.get("source_openlist_path") or "").rstrip("/"))
    if not source_name:
        raise ValueError("source path name missing")
    target_path = posixpath.join(target_root_path, source_name)
    return {
        "target_category": target_category,
        "target_root_openlist_path": target_root_path,
        "target_openlist_path": target_path,
    }


def media_work_item_path(media_path, root_path):
    media_path = normalize_openlist_path(media_path)
    root_path = normalize_openlist_path(root_path)
    if media_path == root_path:
        raise ValueError("media path equals root path")
    if not path_is_same_or_child(media_path, root_path):
        raise ValueError("media path is outside library root")
    relative = media_path[len(root_path) :].lstrip("/")
    parts = [part for part in relative.split("/") if part]
    if not parts:
        raise ValueError("media relative path missing")
    source_path = posixpath.join(root_path, parts[0])
    source_kind = "folder" if len(parts) > 1 else "file"
    return source_path, source_kind


def cloud_path_to_openlist_path(value):
    raw = str(value or "").strip()
    if not raw:
        return ""
    if raw.startswith(MSG_CLOUD_PREFIX + "/"):
        raw = raw[len(MSG_CLOUD_PREFIX) :]
    elif raw.startswith(MSG_CLOUD_PREFIX):
        raw = raw[len(MSG_CLOUD_PREFIX) :]
    raw = urllib.parse.unquote(raw)
    return normalize_openlist_path(raw)


def openlist_path_to_cloud_path(path):
    return MSG_CLOUD_PREFIX + normalize_openlist_path(path)


def normalize_openlist_path(path):
    raw = str(path or "").strip()
    if not raw:
        return ""
    raw = raw.replace("\\", "/")
    if not raw.startswith("/"):
        raw = "/" + raw
    normalized = posixpath.normpath(raw)
    if normalized == ".":
        return ""
    return normalized


def path_is_same_or_child(path, parent):
    path = normalize_openlist_path(path)
    parent = normalize_openlist_path(parent)
    return path == parent or path.startswith(parent.rstrip("/") + "/")


def replace_path_prefix(path, old_prefix, new_prefix):
    if path == old_prefix:
        return new_prefix
    if str(path or "").startswith(old_prefix.rstrip("/") + "/"):
        return new_prefix.rstrip("/") + str(path)[len(old_prefix.rstrip("/")) :]
    raise ValueError("path does not start with source prefix: %s" % path)


def cloud_relative_path(path, root_cloud_path):
    root_cloud_path = root_cloud_path.rstrip("/")
    if path == root_cloud_path:
        return ""
    if str(path or "").startswith(root_cloud_path + "/"):
        return str(path)[len(root_cloud_path) + 1 :]
    return ""


def library_id_to_category(library_id):
    for category, root in MSG_LIBRARY_ROOTS.items():
        if root.get("library_id") == library_id:
            return category
    return ""
