import posixpath
import re
import urllib.parse
import uuid
from collections import OrderedDict

from pipeline.config import category_to_msg_library_root, category_to_openlist_path, msg_library_roots


DEFAULT_MSG_DATABASE_DSN = "postgresql://mediastation:mediastation@127.0.0.1:15432/mediastation"
MSG_CLOUD_PREFIX = "cloud://openlist"


def new_uuid():
    return str(uuid.uuid4())

SXX_EXX_RE = re.compile(r"(?i)(?:^|[^a-z0-9])S(?P<season>\d{1,2})\s*E(?P<episode>\d{1,4})(?:[^a-z0-9]|$)")
CHINESE_EPISODE_RE = re.compile(r"第\s*(?P<episode>\d{1,4})\s*[集話话]")
EPISODE_TOKEN_RE = re.compile(r"(?i)(?:^|[^a-z0-9])(?:EP|E)\s*(?P<episode>\d{1,4})(?:[^a-z0-9]|$)")
BRACKET_EPISODE_RE = re.compile(r"(?:^|[\s._\-\[\(])(?P<episode>\d{1,3})(?:v\d+)?(?:[\s._\-\]\)]|$)", re.IGNORECASE)
IGNORED_EPISODE_NUMBERS = {720, 1080, 2160, 264, 265}


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

    def validate_migration_source_ready(self, candidate):
        source_cloud_path = openlist_path_to_cloud_path(candidate["source_openlist_path"])
        with self._connect() as conn:
            with conn.transaction():
                rows = self._load_source_rows_for_update(conn, candidate, source_cloud_path)
                if not rows:
                    raise RuntimeError("MediaStationGo source media not found: %s" % candidate["source_openlist_path"])

                media_ids = [row["id"] for row in rows]
                series_ids = sorted({row.get("series_id") for row in rows if row.get("series_id")})
                self._assert_series_not_partial(conn, series_ids, media_ids)

        return {
            "source_openlist_path": candidate["source_openlist_path"],
            "media_count": len(media_ids),
            "series_count": len(series_ids),
        }

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
                    new_strm_url = replace_strm_url_prefix(
                        row.get("strm_url"),
                        candidate["source_openlist_path"],
                        target["target_openlist_path"],
                    )
                    conn.execute(
                        """
                        update media
                        set library_id = %s,
                            library_root_id = %s,
                            path = %s,
                            relative_path = %s,
                            strm_url = %s,
                            updated_at = now()
                        where id = %s
                        """,
                        (
                            target_root["library_id"],
                            target_root["root_id"],
                            new_path,
                            new_relative_path,
                            new_strm_url,
                            row["id"],
                        ),
                    )

                if series_ids:
                    conn.execute(
                        "update series set library_id = %s, updated_at = now() where id = any(%s)",
                        (target_root["library_id"], series_ids),
                    )

                self._update_strm_records(
                    conn,
                    media_ids,
                    source_cloud_path,
                    target_cloud_path,
                    candidate["source_openlist_path"],
                    target["target_openlist_path"],
                )
                self._assert_media_rows_migrated(
                    conn,
                    media_ids,
                    target_root,
                    target_cloud_path,
                    candidate["source_openlist_path"],
                )

        return {
            "source_openlist_path": candidate["source_openlist_path"],
            "target_openlist_path": target["target_openlist_path"],
            "target_category": target_category,
            "media_count": len(media_ids),
            "series_count": len(series_ids),
        }

    def repair_episode_visibility(self, category, media_id=None):
        if category not in ("tv", "anime"):
            return {"status": "skipped", "updated": 0, "reason": "not_episode_library"}

        root = category_to_msg_library_root(category)
        root_openlist_path = category_to_openlist_path(category)
        root_cloud_path = openlist_path_to_cloud_path(root_openlist_path)

        with self._connect() as conn:
            with conn.transaction():
                self._ensure_cloud_media_guard(conn)
                conn.execute("set local media_pipeline.allow_cloud_media_migration = 'on'")
                rows = self._load_episode_visibility_rows_for_update(conn, root, root_openlist_path, media_id)
                if not rows:
                    raise RuntimeError("MediaStationGo episode visibility target not found")

                updates = build_episode_visibility_updates(rows, root_cloud_path)
                for update in updates:
                    conn.execute(
                        """
                        update media
                        set relative_path = %s,
                            season_num = %s,
                            episode_num = %s,
                            episode_title = %s,
                            updated_at = now()
                        where id = %s
                        """,
                        (
                            update["relative_path"],
                            update["season_num"],
                            update["episode_num"],
                            update["episode_title"],
                            update["id"],
                        ),
                    )

                self._assert_episode_visibility_repaired(conn, [row["id"] for row in rows])

        return {
            "status": "success",
            "updated": len(updates),
            "media_count": len(rows),
            "reason": "repaired" if updates else "already_valid",
        }

    def repair_movie_extras(self, category, media_id=None):
        if category != "movie":
            return {"status": "skipped", "updated": 0, "reason": "not_movie_library"}
        if not media_id:
            return {"status": "skipped", "updated": 0, "reason": "media_id_missing"}

        root = category_to_msg_library_root(category)
        root_openlist_path = category_to_openlist_path(category)
        with self._connect() as conn:
            with conn.transaction():
                row = conn.execute(
                    """
                    select id, path
                    from media
                    where deleted_at is null
                      and library_id = %s
                      and library_root_id = %s
                      and id = %s
                    for update
                    """,
                    (root["library_id"], root["root_id"], media_id),
                ).fetchone()
                if not row:
                    raise RuntimeError("MediaStationGo movie extra cleanup target not found")
                source_path, source_kind = media_work_item_path(cloud_path_to_openlist_path(row.get("path")), root_openlist_path)
                source_cloud_path = openlist_path_to_cloud_path(source_path)
                if source_kind == "file":
                    condition = "path = %s"
                    params = (source_cloud_path,)
                else:
                    condition = "(path = %s or path like %s)"
                    params = (source_cloud_path, source_cloud_path + "/%")
                rows = conn.execute(
                    """
                    select id, path, title, deleted_at
                    from media
                    where library_id = %s
                      and library_root_id = %s
                      and """ + condition + """
                    for update
                    """,
                    (root["library_id"], root["root_id"], *params),
                ).fetchall()
                extra_rows = [
                    item
                    for item in rows
                    if item["id"] != media_id and movie_media_row_looks_like_extra(item, source_path)
                ]
                extra_ids = [item["id"] for item in extra_rows if item.get("deleted_at") is None]
                hide_patterns = movie_extra_hide_patterns(extra_rows, source_path)
                if extra_ids:
                    conn.execute(
                        """
                        update media
                        set deleted_at = now(),
                            updated_at = now()
                        where id = any(%s)
                        """,
                        (extra_ids,),
                    )

        return {
            "status": "success",
            "updated": len(extra_ids),
            "media_count": len(rows),
            "openlist_hide_path": source_path if hide_patterns else "",
            "openlist_hide_patterns": hide_patterns,
            "openlist_hidden_count": len(hide_patterns),
            "reason": "extras_hidden" if extra_ids else ("extras_already_hidden" if hide_patterns else "already_clean"),
        }

    def list_deleted_openlist_media_for_hide(self, limit=100):
        with self._connect() as conn:
            rows = conn.execute(
                """
                select
                    m.id,
                    m.library_id,
                    m.library_root_id,
                    m.path,
                    m.deleted_at,
                    l.type as library_type,
                    r.path as root_path
                from media m
                left join libraries l on l.id = m.library_id
                left join library_roots r on r.id = m.library_root_id
                where m.deleted_at is not null
                  and coalesce(m.path, '') like %s
                order by m.deleted_at asc nulls last, m.updated_at asc nulls last
                limit %s
                """,
                (MSG_CLOUD_PREFIX + "/%", max(1, int(limit))),
            ).fetchall()
        return [candidate for candidate in (deleted_openlist_media_hide_candidate(row) for row in rows) if candidate]

    def purge_deleted_media_under_openlist_paths(self, category, openlist_paths):
        root = category_to_msg_library_root(category)
        seen = set()
        cloud_paths = []
        for path in (openlist_path_to_cloud_path(path) for path in openlist_paths or []):
            if path and path not in seen:
                seen.add(path)
                cloud_paths.append(path)
        if not cloud_paths:
            return {"status": "skipped", "deleted": 0, "reason": "target_missing", "media_ids": []}
        conditions = []
        params = [root["library_id"]]
        for path in cloud_paths:
            conditions.append("(path = %s or path like %s)")
            params.extend([path, path.rstrip("/") + "/%"])
        with self._connect() as conn:
            with conn.transaction():
                rows = conn.execute(
                    """
                    select id, path
                    from media
                    where deleted_at is not null
                      and library_id = %s
                      and (""" + " or ".join(conditions) + """)
                    order by deleted_at asc nulls last, updated_at asc nulls last
                    """,
                    tuple(params),
                ).fetchall()
                media_ids = [row["id"] for row in rows]
                if media_ids:
                    conn.execute("delete from media where id = any(%s)", (media_ids,))
        return {
            "status": "success" if media_ids else "skipped",
            "deleted": len(media_ids),
            "reason": "deleted_media_pruned" if media_ids else "no_deleted_media",
            "media_ids": media_ids,
        }

    def create_temporary_library_root(self, category, openlist_path):
        root = category_to_msg_library_root(category)
        openlist_path = normalize_openlist_path(openlist_path)
        if not openlist_path:
            raise ValueError("temporary library root path must not be empty")
        root_id = new_uuid()
        cloud_path = openlist_path_to_cloud_path(openlist_path)
        with self._connect() as conn:
            with conn.transaction():
                conn.execute(
                    """
                    insert into library_roots (
                        id, created_at, updated_at, deleted_at, library_id, name, path, enabled, sort_order
                    ) values (%s, now(), now(), null, %s, %s, %s, true, 9999)
                    """,
                    (root_id, root["library_id"], posixpath.basename(openlist_path.rstrip("/")) or "target", cloud_path),
                )
        return {"root_id": root_id, "path": cloud_path}

    def reassign_media_root(self, media_id, root_id):
        with self._connect() as conn:
            with conn.transaction():
                conn.execute("update media set library_root_id = %s, updated_at = now() where id = %s", (root_id, media_id))

    def delete_library_root(self, root_id):
        with self._connect() as conn:
            with conn.transaction():
                conn.execute("delete from library_roots where id = %s", (root_id,))

    def _connect(self):
        if self._connect_override is not None:
            return self._connect_override()
        try:
            import psycopg
            from psycopg.rows import dict_row
        except ImportError as exc:
            raise RuntimeError("psycopg missing; rebuild media-pipeline image with Postgres support") from exc
        return psycopg.connect(self.dsn, row_factory=dict_row)

    def _load_episode_visibility_rows_for_update(self, conn, root, root_openlist_path, media_id):
        if media_id:
            row = conn.execute(
                """
                select id, path, relative_path, season_num, episode_num, episode_title
                from media
                where deleted_at is null
                  and library_id = %s
                  and library_root_id = %s
                  and id = %s
                for update
                """,
                (root["library_id"], root["root_id"], media_id),
            ).fetchone()
            if not row:
                return []
            media_path = cloud_path_to_openlist_path(row.get("path"))
            source_path, source_kind = media_work_item_path(media_path, root_openlist_path)
            source_cloud_path = openlist_path_to_cloud_path(source_path)
            if source_kind == "file":
                condition = "path = %s"
                params = (source_cloud_path,)
            else:
                condition = "(path = %s or path like %s)"
                params = (source_cloud_path, source_cloud_path + "/%")
            return conn.execute(
                """
                select id, path, relative_path, season_num, episode_num, episode_title
                from media
                where deleted_at is null
                  and library_id = %s
                  and library_root_id = %s
                  and """ + condition + """
                order by path
                for update
                """,
                (root["library_id"], root["root_id"], *params),
            ).fetchall()

        return conn.execute(
            """
            select id, path, relative_path, season_num, episode_num, episode_title
            from media
            where deleted_at is null
              and library_id = %s
              and library_root_id = %s
            order by path
            for update
            """,
            (root["library_id"], root["root_id"]),
        ).fetchall()

    def _assert_episode_visibility_repaired(self, conn, media_ids):
        row = conn.execute(
            """
            select count(*) as bad_count
            from media
            where id = any(%s)
              and (
                coalesce(relative_path, '') = ''
                or coalesce(season_num, 0) <= 0
                or coalesce(episode_num, 0) <= 0
              )
            """,
            (media_ids,),
        ).fetchone()
        if int(row["bad_count"] or 0) != 0:
            raise RuntimeError("MediaStationGo episode visibility validation failed")

    def _load_source_rows_for_update(self, conn, candidate, source_cloud_path):
        if candidate.get("source_kind") == "file":
            condition = "path = %s"
            params = (source_cloud_path,)
        else:
            condition = "(path = %s or path like %s)"
            params = (source_cloud_path, source_cloud_path + "/%")
        return conn.execute(
            """
            select id, library_id, library_root_id, series_id, path, relative_path, file_id, strm_url
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

    def _update_strm_records(
        self,
        conn,
        media_ids,
        source_cloud_path,
        target_cloud_path,
        source_openlist_path,
        target_openlist_path,
    ):
        source_ref_prefix = encode_openlist_ref_prefix(source_openlist_path)
        target_ref_prefix = encode_openlist_ref_prefix(target_openlist_path)
        conn.execute(
            """
            update strm_records
            set file_path = case
                    when file_path is not null then replace(replace(file_path, %s, %s), %s, %s)
                    else file_path
                end,
                url = case
                    when url is not null then replace(replace(url, %s, %s), %s, %s)
                    else url
                end,
                updated_at = now()
            where media_id = any(%s)
            """,
            (
                source_cloud_path,
                target_cloud_path,
                source_ref_prefix,
                target_ref_prefix,
                source_cloud_path,
                target_cloud_path,
                source_ref_prefix,
                target_ref_prefix,
                media_ids,
            ),
        )

    def _assert_media_rows_migrated(self, conn, media_ids, target_root, target_cloud_path, source_openlist_path=None):
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
        if source_openlist_path:
            source_ref_prefix = encode_openlist_ref_prefix(source_openlist_path)
            row = conn.execute(
                """
                select count(*) as stale_count
                from media
                where id = any(%s)
                  and coalesce(strm_url, '') like %s
                """,
                (media_ids, "%" + source_ref_prefix + "%"),
            ).fetchone()
            if int(row["stale_count"] or 0) != 0:
                raise RuntimeError("MediaStationGo migration strm_url validation failed")

    def _ensure_cloud_media_guard(self, conn):
        roots = msg_library_roots()
        library_ids = [root["library_id"] for root in roots.values()]
        root_ids = [root["root_id"] for root in roots.values()]
        library_ids_sql = sql_varchar_array(library_ids)
        root_ids_sql = sql_varchar_array(root_ids)
        conn.execute(
            ("""
            create or replace function public.pipeline_guard_msg_cloud_media()
            returns trigger
            language plpgsql
            as $$
            begin
              if current_setting('media_pipeline.allow_cloud_media_migration', true) = 'on' then
                return new;
              end if;
              if old.library_id = any (""" + library_ids_sql + """)
                 and old.library_root_id = any (""" + root_ids_sql + """)
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
            """)
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


def build_episode_visibility_updates(rows, root_cloud_path):
    rows = list(rows or [])
    used_episode_numbers = {
        int(row.get("episode_num"))
        for row in rows
        if positive_int(row.get("episode_num")) is not None
    }
    next_episode = 1
    updates = []
    for row in rows:
        relative_path = str(row.get("relative_path") or "")
        inferred_relative_path = cloud_relative_path(row.get("path"), root_cloud_path)
        if not relative_path:
            relative_path = inferred_relative_path
        season_num = positive_int(row.get("season_num"))
        episode_num = positive_int(row.get("episode_num"))
        parsed_season, parsed_episode = season_episode_from_path(relative_path or row.get("path") or "")
        if season_num is None:
            season_num = parsed_season or 1
        if episode_num is None:
            candidate_episode = parsed_episode if parsed_episode and parsed_episode not in used_episode_numbers else None
            if candidate_episode is None:
                while next_episode in used_episode_numbers:
                    next_episode += 1
                candidate_episode = next_episode
            episode_num = candidate_episode
            used_episode_numbers.add(episode_num)
        episode_title = str(row.get("episode_title") or "").strip()
        if not episode_title:
            episode_title = episode_title_from_relative_path(relative_path, episode_num)

        update = {
            "id": row["id"],
            "relative_path": relative_path,
            "season_num": season_num,
            "episode_num": episode_num,
            "episode_title": episode_title,
        }
        if episode_visibility_update_needed(row, update):
            updates.append(update)
    return updates


def episode_visibility_update_needed(row, update):
    return (
        str(row.get("relative_path") or "") != update["relative_path"]
        or positive_int(row.get("season_num")) != update["season_num"]
        or positive_int(row.get("episode_num")) != update["episode_num"]
        or str(row.get("episode_title") or "").strip() != update["episode_title"]
    )


def season_episode_from_path(path):
    value = str(path or "")
    match = SXX_EXX_RE.search(value)
    if match:
        return int(match.group("season")), int(match.group("episode"))
    for pattern in (CHINESE_EPISODE_RE, EPISODE_TOKEN_RE, BRACKET_EPISODE_RE):
        match = pattern.search(value)
        if not match:
            continue
        episode = int(match.group("episode"))
        if episode in IGNORED_EPISODE_NUMBERS:
            continue
        return None, episode
    return None, None


def episode_title_from_relative_path(relative_path, episode_num):
    basename = posixpath.basename(str(relative_path or "").rstrip("/"))
    title = posixpath.splitext(basename)[0].strip()
    return title or "Episode %s" % int(episode_num or 1)


def positive_int(value):
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def movie_media_row_looks_like_extra(row, source_openlist_path):
    path = cloud_path_to_openlist_path(row.get("path"))
    if not path_is_same_or_child(path, source_openlist_path):
        return False
    relative = path[len(normalize_openlist_path(source_openlist_path)) :].strip("/")
    parts = [part for part in relative.split("/") if part]
    if len(parts) <= 1:
        return False
    text = "/".join(parts[:-1] + [posixpath.splitext(parts[-1])[0]])
    normalized = re.sub(r"[^0-9A-Za-z\u4e00-\u9fff]+", "", text).lower()
    tokens = {
        "bonus",
        "extra",
        "extras",
        "gallery",
        "images",
        "menu",
        "pv",
        "specials",
        "tokuten",
        "予告",
        "图集",
        "映像特典",
        "特典",
        "特典映像",
        "特报",
        "花絮",
        "菜单",
        "预告",
    }
    return any(re.sub(r"[^0-9A-Za-z\u4e00-\u9fff]+", "", token).lower() in normalized for token in tokens)


def movie_extra_hide_patterns(rows, source_openlist_path):
    source_path = normalize_openlist_path(source_openlist_path)
    patterns = []
    seen = set()
    for row in rows or []:
        path = cloud_path_to_openlist_path(row.get("path"))
        if not path_is_same_or_child(path, source_path) or path == source_path:
            continue
        relative = path[len(source_path) :].strip("/")
        first_part = relative.split("/", 1)[0].strip()
        if not first_part:
            continue
        pattern = "^%s$" % re.escape(first_part)
        if pattern in seen:
            continue
        seen.add(pattern)
        patterns.append(pattern)
    return patterns


def deleted_openlist_media_hide_candidate(row):
    media_id = str((row or {}).get("id") or "").strip()
    media_path = cloud_path_to_openlist_path((row or {}).get("path"))
    if not media_id or not media_path:
        return None

    category = library_id_to_category((row or {}).get("library_id"))
    if category:
        root_path = category_to_openlist_path(category)
    else:
        root_path = cloud_path_to_openlist_path((row or {}).get("root_path"))
    if not root_path or not path_is_same_or_child(media_path, root_path) or media_path == root_path:
        return None

    if category in ("tv", "anime"):
        target_path = media_path
        target_kind = "file"
    else:
        try:
            target_path, target_kind = media_work_item_path(media_path, root_path)
        except ValueError:
            return None

    hide_path = posixpath.dirname(target_path.rstrip("/")) or "/"
    hide_name = posixpath.basename(target_path.rstrip("/"))
    if not hide_name:
        return None

    return {
        "media_id": media_id,
        "library_id": (row or {}).get("library_id"),
        "library_root_id": (row or {}).get("library_root_id"),
        "category": category,
        "media_path": media_path,
        "target_openlist_path": target_path,
        "target_kind": target_kind,
        "hide_path": normalize_openlist_path(hide_path),
        "hide_pattern": "^%s$" % re.escape(hide_name),
        "deleted_at": str((row or {}).get("deleted_at") or ""),
    }


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


def replace_strm_url_prefix(strm_url, old_openlist_path, new_openlist_path):
    if strm_url is None:
        return None
    value = str(strm_url)
    for old_prefix, new_prefix in openlist_ref_prefix_pairs(old_openlist_path, new_openlist_path):
        value = value.replace(old_prefix, new_prefix)
    old_raw = normalize_openlist_path(old_openlist_path)
    new_raw = normalize_openlist_path(new_openlist_path)
    return value.replace(old_raw, new_raw)


def openlist_ref_prefix_pairs(old_openlist_path, new_openlist_path):
    old_path = normalize_openlist_path(old_openlist_path)
    new_path = normalize_openlist_path(new_openlist_path)
    pairs = [
        (urllib.parse.quote_plus(old_path, safe=""), urllib.parse.quote_plus(new_path, safe="")),
        (urllib.parse.quote(old_path, safe=""), urllib.parse.quote(new_path, safe="")),
    ]
    unique = []
    seen = set()
    for old_prefix, new_prefix in pairs:
        key = (old_prefix, new_prefix)
        if key not in seen:
            seen.add(key)
            unique.append(key)
    return unique


def encode_openlist_ref_prefix(path):
    return urllib.parse.quote_plus(normalize_openlist_path(path), safe="")


def cloud_relative_path(path, root_cloud_path):
    root_cloud_path = root_cloud_path.rstrip("/")
    if path == root_cloud_path:
        return ""
    if str(path or "").startswith(root_cloud_path + "/"):
        return str(path)[len(root_cloud_path) + 1 :]
    return ""


def library_id_to_category(library_id):
    for category, root in msg_library_roots().items():
        if root.get("library_id") == library_id:
            return category
    return ""


def sql_varchar_array(values):
    quoted = []
    for value in values:
        quoted.append("'" + str(value).replace("'", "''") + "'")
    return "array[%s]::varchar[]" % ",".join(quoted)
