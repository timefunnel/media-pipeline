from tests.test_pipeline_core import *


class SubtitleProxyTest(unittest.TestCase):
    def test_normalize_webvtt_timestamps_pads_centiseconds(self):
        text = "WEBVTT\n\n1\n00:00:06.06 --> 00:00:08.16\nhello\n"

        normalized = normalize_webvtt_timestamps(text)

        self.assertIn("00:00:06.060 --> 00:00:08.160", normalized)

    def test_normalize_webvtt_timestamps_keeps_valid_milliseconds(self):
        text = "WEBVTT\n\n1\n00:00:06.123 --> 00:00:08.456\nhello\n"

        normalized = normalize_webvtt_timestamps(text)

        self.assertEqual(normalized, text)

    def test_normalize_webvtt_timestamps_does_not_change_caption_text(self):
        text = "WEBVTT\n\n1\n00:00:06.06 --> 00:00:08.16\nprinted 00:00:01.2 as text\n"

        normalized = normalize_webvtt_timestamps(text)

        self.assertIn("printed 00:00:01.2 as text", normalized)

    def test_should_normalize_subtitle_accepts_vtt_content_type_or_magic(self):
        self.assertTrue(should_normalize_subtitle("text/vtt; charset=utf-8", b"bad"))
        self.assertTrue(should_normalize_subtitle("text/plain", b"\nWEBVTT\n"))
        self.assertFalse(should_normalize_subtitle("text/plain", b"not vtt"))

    def test_redact_sensitive_query_values_hides_tokens(self):
        message = 'GET /api/subtitles/id?path=x&token=secret-value&name=y HTTP/1.1'

        redacted = redact_sensitive_query_values(message)

        self.assertNotIn("secret-value", redacted)
        self.assertIn("token=REDACTED", redacted)

    def test_msg_api_authenticator_rejects_missing_credentials(self):
        auth = MsgApiAuthenticator("http://127.0.0.1:18080/api", "", "")

        with self.assertRaisesRegex(RuntimeError, "credentials missing"):
            auth.authorization_header()

    def test_inject_subtitle_track_bootstrap_adds_script_before_body_close(self):
        html = "<html><body><div id=\"root\"></div></body></html>"

        injected = inject_subtitle_track_bootstrap(html)

        self.assertIn("subtitleAutoEnabled", injected)
        self.assertLess(injected.index("subtitleAutoEnabled"), injected.index("</body>"))

    def test_injected_subtitle_parser_trims_webvtt_end_time(self):
        injected = inject_subtitle_track_bootstrap("<html><body></body></html>")

        self.assertIn("parts[1].trim().split", injected)
        self.assertIn('replace(",", ".")', injected)

    def test_subtitle_body_to_vtt_converts_srt_when_requested(self):
        body = b"1\n00:00:01,2 --> 00:00:03,45\nhello\n"

        converted, content_type = subtitle_body_to_vtt(body, path="subtitle.srt")

        self.assertEqual(content_type, "text/vtt; charset=utf-8")
        self.assertIn(b"WEBVTT", converted)
        self.assertIn(b"00:00:01.200 --> 00:00:03.450", converted)

    def test_patch_emby_resume_runtime_fields_adds_synthetic_duration(self):
        payload = {
            "Items": [
                {
                    "Id": "media-1",
                    "RunTimeTicks": 0,
                    "UserData": {
                        "PlaybackPositionTicks": 52_620_0000,
                        "PlayedPercentage": 0,
                    },
                }
            ]
        }

        changed = patch_emby_resume_runtime_fields(payload)

        item = payload["Items"][0]
        self.assertTrue(changed)
        self.assertGreater(item["RunTimeTicks"], item["UserData"]["PlaybackPositionTicks"])
        self.assertGreater(item["UserData"]["PlayedPercentage"], 0)

    def test_patch_emby_playback_info_runtime_updates_media_sources(self):
        payload = {"MediaSources": [{"Id": "media-1", "RunTimeTicks": 0}, {"Id": "media-2", "RunTimeTicks": 10}]}

        changed = patch_emby_playback_info_runtime(payload, 600_000_0000, media_id="media-1")

        self.assertTrue(changed)
        self.assertEqual(payload["MediaSources"][0]["RunTimeTicks"], 600_000_0000)
        self.assertEqual(payload["MediaSources"][1]["RunTimeTicks"], 10)

    def test_patch_emby_playback_info_runtime_skips_other_media_sources(self):
        payload = {"MediaSources": [{"Id": "media-1", "RunTimeTicks": 0}, {"Id": "media-2", "RunTimeTicks": 0}]}

        changed = patch_emby_playback_info_runtime(payload, 600_000_0000, media_id="media-1")

        self.assertTrue(changed)
        self.assertEqual(payload["MediaSources"][0]["RunTimeTicks"], 600_000_0000)
        self.assertEqual(payload["MediaSources"][1]["RunTimeTicks"], 0)

    def test_inject_subtitle_track_bootstrap_is_idempotent(self):
        html = inject_subtitle_track_bootstrap("<html><body></body></html>")

        injected = inject_subtitle_track_bootstrap(html)

        self.assertEqual(injected.count("subtitleAutoEnabled"), html.count("subtitleAutoEnabled"))

    def test_inject_emby_subtitle_streams_preserves_source_subtitle_format(self):
        payload = {"MediaSources": [{"Id": "media-1", "MediaStreams": [{"Index": 0, "Type": "Video"}]}]}

        changed = inject_emby_subtitle_streams(
            payload,
            "media-1",
            [
                {"lang": "tc", "label": "tc", "path": "cloud://subtitle.tc.ass"},
                {"lang": "sc", "label": "sc", "path": "cloud://subtitle.sc.srt"},
            ],
        )

        streams = payload["MediaSources"][0]["MediaStreams"]
        self.assertTrue(changed)
        self.assertEqual([stream["Type"] for stream in streams], ["Video", "Subtitle", "Subtitle"])
        self.assertEqual(streams[1]["Index"], 1)
        self.assertEqual(streams[1]["Codec"], "ass")
        self.assertEqual(streams[2]["Codec"], "srt")
        self.assertEqual(streams[1]["DeliveryMethod"], "External")
        self.assertEqual(streams[1]["DisplayTitle"], "tc - ASS - External")
        self.assertEqual(streams[1]["Path"], "cloud://subtitle.tc.ass")
        self.assertEqual(payload["MediaSources"][0]["DefaultSubtitleStreamIndex"], 1)
        self.assertEqual(streams[1]["DeliveryUrl"], "/emby/Videos/media-1/media-1/Subtitles/1/Stream.ass?mp_track=0")
        self.assertEqual(streams[2]["DeliveryUrl"], "/emby/Videos/media-1/media-1/Subtitles/2/Stream.srt?mp_track=1")

    def test_inject_emby_subtitle_streams_appends_external_tracks_when_native_subtitles_exist(self):
        payload = {"MediaSources": [{"Id": "media-1", "MediaStreams": [{"Index": 0, "Type": "Video"}, {"Index": 1, "Type": "Subtitle"}]}]}

        changed = inject_emby_subtitle_streams(payload, "media-1", [{"lang": "sc", "label": "sc", "path": "cloud://subtitle.sc.ass"}])

        streams = payload["MediaSources"][0]["MediaStreams"]
        self.assertTrue(changed)
        self.assertEqual(len(streams), 3)
        self.assertEqual(streams[2]["Index"], 2)
        self.assertEqual(streams[2]["Codec"], "ass")

    def test_inject_emby_subtitle_streams_only_updates_current_media_source(self):
        payload = {
            "MediaSources": [
                {"Id": "media-1", "MediaStreams": [{"Index": 0, "Type": "Video"}]},
                {"Id": "media-2", "MediaStreams": [{"Index": 0, "Type": "Video"}]},
            ]
        }

        changed = inject_emby_subtitle_streams(payload, "media-1", [{"lang": "sc", "label": "sc", "path": "cloud://subtitle.sc.srt"}])

        self.assertTrue(changed)
        self.assertEqual(len(payload["MediaSources"][0]["MediaStreams"]), 2)
        self.assertEqual(len(payload["MediaSources"][1]["MediaStreams"]), 1)

    def test_parse_emby_subtitle_stream_path_reads_track_mapping(self):
        parsed = parse_emby_subtitle_stream_path("/emby/Videos/media-1/source-1/Subtitles/2/Stream.vtt?mp_track=1&api_key=secret")

        self.assertEqual(parsed["media_id"], "media-1")
        self.assertEqual(parsed["source_id"], "source-1")
        self.assertEqual(parsed["stream_index"], 2)
        self.assertEqual(parsed["track_index"], 1)
        self.assertEqual(parsed["extension"], ".vtt")

    def test_parse_emby_item_media_id_ignores_resume_collection(self):
        media_id = parse_emby_item_media_id(
            "/emby/Users/user-1/Items/Resume?Recursive=true&MediaTypes=Video&Limit=20"
        )

        self.assertEqual(media_id, "")

    def test_parse_emby_item_media_id_accepts_uuid_items(self):
        media_id = parse_emby_item_media_id(
            "/emby/Users/user-1/Items/7303b838-dab8-4eb7-a8b6-4dc761f69c18/PlaybackInfo?api_key=secret"
        )

        self.assertEqual(media_id, "7303b838-dab8-4eb7-a8b6-4dc761f69c18")


class CategoryConfigTest(unittest.TestCase):
    def test_msgdb_groups_episode_rows_into_one_migration_candidate(self):
        from pipeline.msgdb import build_migration_candidates, build_migration_target, cloud_path_to_openlist_path

        rows = [
            {
                "id": "m1",
                "library_id": "b6c58f40-76dc-46b5-8f27-9e74d22e5e3d",
                "library_root_id": "3d2e0cb4-3537-4f7d-8d79-9d4d5f1800df",
                "title": "成龙历险记",
                "path": "cloud://openlist/115/剧集/成龙历险记/成龙历险记 第01集.mp4",
                "root_path": "cloud://openlist/115%2F%E5%89%A7%E9%9B%86",
                "size_bytes": 100,
                "library_name": "剧集",
                "library_type": "tv",
            },
            {
                "id": "m2",
                "library_id": "b6c58f40-76dc-46b5-8f27-9e74d22e5e3d",
                "library_root_id": "3d2e0cb4-3537-4f7d-8d79-9d4d5f1800df",
                "title": "成龙历险记",
                "path": "cloud://openlist/115/剧集/成龙历险记/成龙历险记 第02集.mp4",
                "root_path": "cloud://openlist/115%2F%E5%89%A7%E9%9B%86",
                "size_bytes": 200,
                "library_name": "剧集",
                "library_type": "tv",
            },
        ]

        candidates = build_migration_candidates(rows, limit=20)
        target = build_migration_target(candidates[0], "anime")

        self.assertEqual(cloud_path_to_openlist_path("cloud://openlist/115%2F%E5%89%A7%E9%9B%86"), "/115/剧集")
        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0]["source_openlist_path"], "/115/剧集/成龙历险记")
        self.assertEqual(candidates[0]["source_kind"], "folder")
        self.assertEqual(candidates[0]["category"], "tv")
        self.assertEqual(candidates[0]["media_count"], 2)
        self.assertEqual(candidates[0]["total_size"], 300)
        self.assertEqual(target["target_openlist_path"], "/115/动漫/成龙历险记")

    def test_msgdb_rewrites_cloud_play_strm_url_when_migrating_paths(self):
        from pipeline.msgdb import replace_strm_url_prefix

        old_path = "/115/\u5267\u96c6/\u6210\u9f99\u5386\u9669\u8bb0"
        new_path = "/115/\u52a8\u6f2b/\u6210\u9f99\u5386\u9669\u8bb0"
        url = (
            "/api/cloud/play/openlist?ref="
            "%2F115%2F%E5%89%A7%E9%9B%86%2F%E6%88%90%E9%BE%99%E5%8E%86%E9%99%A9%E8%AE%B0"
            "%2F%E6%88%90%E9%BE%99%E5%8E%86%E9%99%A9%E8%AE%B0+%E7%AC%AC01%E9%9B%86.mp4"
        )

        rewritten = replace_strm_url_prefix(url, old_path, new_path)

        self.assertIn("%2F115%2F%E5%8A%A8%E6%BC%AB%2F%E6%88%90%E9%BE%99%E5%8E%86%E9%99%A9%E8%AE%B0", rewritten)
        self.assertNotIn("%2F115%2F%E5%89%A7%E9%9B%86%2F%E6%88%90%E9%BE%99%E5%8E%86%E9%99%A9%E8%AE%B0", rewritten)

    def test_msgdb_builds_episode_visibility_updates_for_bad_anime_rows(self):
        from pipeline.msgdb import build_episode_visibility_updates

        rows = [
            {
                "id": "m1",
                "path": "cloud://openlist/115/Anime/Show/Show S01E01.mkv",
                "relative_path": "",
                "season_num": 0,
                "episode_num": 0,
                "episode_title": "",
            },
            {
                "id": "m2",
                "path": "cloud://openlist/115/Anime/Show/Show 1080p.mkv",
                "relative_path": "",
                "season_num": None,
                "episode_num": None,
                "episode_title": "",
            },
        ]

        updates = build_episode_visibility_updates(rows, "cloud://openlist/115/Anime")

        self.assertEqual([item["id"] for item in updates], ["m1", "m2"])
        self.assertEqual(updates[0]["relative_path"], "Show/Show S01E01.mkv")
        self.assertEqual(updates[0]["season_num"], 1)
        self.assertEqual(updates[0]["episode_num"], 1)
        self.assertEqual(updates[1]["relative_path"], "Show/Show 1080p.mkv")
        self.assertEqual(updates[1]["season_num"], 1)
        self.assertEqual(updates[1]["episode_num"], 2)

    def test_msgdb_detects_movie_extra_rows_under_pack_subfolders(self):
        from pipeline.msgdb import movie_media_row_looks_like_extra

        self.assertTrue(
            movie_media_row_looks_like_extra(
                {
                    "path": "cloud://openlist/115/电影/Godzilla Pack/PV/[DBD-Raws][Godzilla Final Wars][PV][01].mkv",
                    "title": "pv",
                },
                "/115/电影/Godzilla Pack",
            )
        )
        self.assertFalse(
            movie_media_row_looks_like_extra(
                {
                    "path": "cloud://openlist/115/电影/Godzilla Pack/[DBD-Raws][Godzilla Final Wars][Ver.A].mkv",
                    "title": "Godzilla Pack",
                },
                "/115/电影/Godzilla Pack",
            )
        )

    def test_msgdb_movie_extra_repair_reason_uses_hidden_terms(self):
        from pipeline.msgdb import MediaStationDbClient

        class FakeCursor:
            def __init__(self, rows):
                self.rows = rows

            def fetchone(self):
                return self.rows[0] if self.rows else None

            def fetchall(self):
                return list(self.rows)

        class FakeConn:
            def __init__(self):
                self.step = 0

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def transaction(self):
                return self

            def execute(self, sql, params=()):
                self.step += 1
                if self.step == 1:
                    return FakeCursor(
                        [
                            {
                                "id": "main-1",
                                "path": "cloud://openlist/115/电影/Godzilla Pack/[DBD-Raws][Godzilla Final Wars][Ver.A].mkv",
                            }
                        ]
                    )
                if self.step == 2:
                    return FakeCursor(
                        [
                            {
                                "id": "main-1",
                                "path": "cloud://openlist/115/电影/Godzilla Pack/[DBD-Raws][Godzilla Final Wars][Ver.A].mkv",
                                "title": "Godzilla Final Wars",
                                "deleted_at": None,
                            },
                            {
                                "id": "extra-1",
                                "path": "cloud://openlist/115/电影/Godzilla Pack/PV/sample.mkv",
                                "title": "PV",
                                "deleted_at": None,
                            },
                        ]
                    )
                return FakeCursor([])

        client = MediaStationDbClient("postgres://unused", connect=lambda: FakeConn())
        result = client.repair_movie_extras("movie", "main-1")

        self.assertEqual(result["reason"], "extras_hidden")
        self.assertEqual(result["openlist_hide_patterns"], ["^PV$"])

    def test_routes_movie_tv_anime_adult_and_other_to_separate_115_folders(self):
        self.assertEqual(category_to_folder_id("movie"), "3464134653584082023")
        self.assertEqual(category_to_folder_id("tv"), "3465137076394001831")
        self.assertEqual(category_to_folder_id("anime"), "3465784028030830531")
        self.assertEqual(category_to_folder_id("adult"), "3464134590896014943")
        self.assertEqual(category_to_folder_id("other"), "3465205291639899794")

    def test_routes_movie_tv_anime_adult_and_other_to_openlist_paths(self):
        self.assertEqual(category_to_openlist_path("movie"), "/115/电影")
        self.assertEqual(category_to_openlist_path("tv"), "/115/剧集")
        self.assertEqual(category_to_openlist_path("anime"), "/115/动漫")
        self.assertEqual(category_to_openlist_path("adult"), "/115/成人")
        self.assertEqual(category_to_openlist_path("other"), "/115/其他")

    def test_routes_movie_tv_anime_adult_and_other_to_mediastation_roots(self):
        movie = category_to_msg_library_root("movie")
        tv = category_to_msg_library_root("tv")
        anime = category_to_msg_library_root("anime")
        adult = category_to_msg_library_root("adult")
        other = category_to_msg_library_root("other")

        self.assertEqual(movie["library_id"], "d150a96c-b467-4c60-82f1-207ae5949045")
        self.assertEqual(movie["root_id"], "0c1dda42-29ef-4069-b051-c9549a8d4440")
        self.assertEqual(tv["library_id"], "b6c58f40-76dc-46b5-8f27-9e74d22e5e3d")
        self.assertEqual(tv["root_id"], "3d2e0cb4-3537-4f7d-8d79-9d4d5f1800df")
        self.assertEqual(tv["media_type"], "tv")
        self.assertEqual(anime["library_id"], "e1333358-17ff-4b90-82f0-663cec26c0df")
        self.assertEqual(anime["root_id"], "fc7058d6-0b32-4536-bb92-4755c488be55")
        self.assertEqual(anime["provider"], "tmdb")
        self.assertEqual(anime["media_type"], "anime")
        self.assertEqual(adult["library_id"], "26768071-73bb-4b5c-85f3-ad0dd84f9fd9")
        self.assertEqual(adult["root_id"], "3fe479e8-4a96-4e61-9f69-fa802e448446")
        self.assertEqual(other["library_id"], "60067bc7-eb34-466c-8bf9-5654297a609f")
        self.assertEqual(other["root_id"], "1f889ec1-b34d-40b6-b3ca-f4372170a42b")
        self.assertEqual(other["provider"], "tmdb")
        self.assertEqual(other["media_type"], "movie")

    def test_load_category_config_allows_env_overrides(self):
        from pipeline.config import category_maps, load_category_config

        config = load_category_config(
            {
                "MEDIA_PIPELINE_MOVIE_FOLDER_ID": "folder-override",
                "MEDIA_PIPELINE_MOVIE_OPENLIST_PATH": "/115/电影新",
                "MEDIA_PIPELINE_MOVIE_MSG_LIBRARY_ID": "library-override",
                "MEDIA_PIPELINE_MOVIE_MSG_ROOT_ID": "root-override",
                "MEDIA_PIPELINE_MOVIE_MSG_PROVIDER": "tmdb",
                "MEDIA_PIPELINE_MOVIE_MSG_MEDIA_TYPE": "movie",
            }
        )
        folder_ids, openlist_paths, msg_roots = category_maps(config)

        self.assertEqual(folder_ids["movie"], "folder-override")
        self.assertEqual(openlist_paths["movie"], "/115/电影新")
        self.assertEqual(msg_roots["movie"]["library_id"], "library-override")
        self.assertEqual(msg_roots["movie"]["root_id"], "root-override")
        self.assertEqual(folder_ids["adult"], "3464134590896014943")

    def test_load_category_config_allows_inline_json_overrides(self):
        from pipeline.config import category_maps, load_category_config

        payload = json.dumps(
            {
                "anime": {
                    "folder_id": "anime-folder-json",
                    "openlist_path": "/115/动画",
                    "msg": {
                        "library_id": "anime-library-json",
                        "root_id": "anime-root-json",
                        "provider": "tmdb",
                        "media_type": "anime",
                    },
                }
            },
            ensure_ascii=False,
        )

        config = load_category_config({"MEDIA_PIPELINE_LIBRARY_CONFIG_JSON": payload})
        folder_ids, openlist_paths, msg_roots = category_maps(config)

        self.assertEqual(folder_ids["anime"], "anime-folder-json")
        self.assertEqual(openlist_paths["anime"], "/115/动画")
        self.assertEqual(msg_roots["anime"]["library_id"], "anime-library-json")
        self.assertEqual(msg_roots["anime"]["root_id"], "anime-root-json")

    def test_load_category_config_rejects_conflicting_external_sources(self):
        from pipeline.config import load_category_config

        with self.assertRaisesRegex(RuntimeError, "set either MEDIA_PIPELINE_LIBRARY_CONFIG"):
            load_category_config(
                {
                    "MEDIA_PIPELINE_LIBRARY_CONFIG": "/tmp/library.json",
                    "MEDIA_PIPELINE_LIBRARY_CONFIG_JSON": "{}",
                }
            )

    def test_rejects_unknown_category_without_fallback(self):
        with self.assertRaisesRegex(ValueError, "unsupported category"):
            category_to_folder_id("unknown")


class OpenListTokenStoreTest(unittest.TestCase):
    def test_reads_access_token_from_enabled_115_open_storage(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "data.db"
            conn = sqlite3.connect(db_path)
            conn.execute(
                "create table x_storages (id integer, mount_path text, driver text, disabled integer, addition text)"
            )
            conn.execute(
                "insert into x_storages values (?, ?, ?, ?, ?)",
                (
                    1,
                    "/115_audit_movie",
                    "115 Open",
                    0,
                    json.dumps(
                        {
                            "access_token": "access-token-value",
                            "refresh_token": "refresh-token-value",
                            "root_folder_id": "3462843402402399378",
                        }
                    ),
                ),
            )
            conn.commit()
            conn.close()

            token = OpenListTokenStore(db_path).load_access_token()

        self.assertEqual(token.storage_id, 1)
        self.assertEqual(token.mount_path, "/115_audit_movie")
        self.assertEqual(token.access_token, "access-token-value")

    def test_rejects_missing_access_token_without_refreshing(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "data.db"
            conn = sqlite3.connect(db_path)
            conn.execute(
                "create table x_storages (id integer, mount_path text, driver text, disabled integer, addition text)"
            )
            conn.execute(
                "insert into x_storages values (?, ?, ?, ?, ?)",
                (1, "/115_audit_movie", "115 Open", 0, json.dumps({"refresh_token": "refresh"})),
            )
            conn.commit()
            conn.close()

            with self.assertRaisesRegex(RuntimeError, "access_token missing"):
                OpenListTokenStore(db_path).load_access_token()


class ProwlarrConfigTest(unittest.TestCase):
    def test_reads_api_key_from_config_xml(self):
        with tempfile.TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "config.xml"
            config_path.write_text("<Config><ApiKey>prowlarr-key-value</ApiKey></Config>", encoding="utf-8")

            self.assertEqual(ProwlarrConfig(config_path).load_api_key(), "prowlarr-key-value")

    def test_rejects_missing_api_key_without_default(self):
        with tempfile.TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "config.xml"
            config_path.write_text("<Config></Config>", encoding="utf-8")

            with self.assertRaisesRegex(RuntimeError, "Prowlarr ApiKey missing"):
                ProwlarrConfig(config_path).load_api_key()


class OpenListTokenProviderTest(unittest.TestCase):
    def test_reads_openlist_token_from_environment(self):
        provider = OpenListTokenProvider(env={"OPENLIST_TOKEN": "openlist-token-value"})

        self.assertEqual(provider.load_token(), "openlist-token-value")

    def test_reads_openlist_token_from_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            token_path = Path(tmp) / "openlist_token"
            token_path.write_text("openlist-token-value\n", encoding="utf-8")
            provider = OpenListTokenProvider(env={"OPENLIST_TOKEN_FILE": str(token_path)})

            self.assertEqual(provider.load_token(), "openlist-token-value")

    def test_rejects_missing_openlist_token_without_default(self):
        provider = OpenListTokenProvider(env={"OPENLIST_TOKEN_FILE": ""})

        with self.assertRaisesRegex(RuntimeError, "OpenList token missing"):
            provider.load_token()

    def test_password_provider_logs_in_and_returns_user_token(self):
        transport = FakeTransport({"code": 200, "message": "success", "data": {"token": "scan-token"}})
        provider = OpenListPasswordTokenProvider("http://127.0.0.1:5244", "media_scan", "secret", transport=transport)

        token = provider.load_token()

        self.assertEqual(token, "scan-token")
        self.assertEqual(transport.calls[0]["method"], "POST")
        self.assertEqual(transport.calls[0]["url"], "http://127.0.0.1:5244/api/auth/login")
        self.assertEqual(transport.calls[0]["data"], {"username": "media_scan", "password": "secret"})

    def test_password_provider_rejects_missing_scan_credentials(self):
        provider = OpenListPasswordTokenProvider("http://127.0.0.1:5244", "", "")

        with self.assertRaisesRegex(RuntimeError, "OpenList media scan credentials missing"):
            provider.load_token()

    def test_extract_openlist_login_token_accepts_known_shapes(self):
        self.assertEqual(extract_openlist_login_token({"data": {"token": "token-1"}}), "token-1")
        self.assertEqual(extract_openlist_login_token({"data": {"access_token": "token-2"}}), "token-2")
        self.assertEqual(extract_openlist_login_token({"token": "token-3"}), "token-3")


class OpenListClientTest(unittest.TestCase):
    def test_list_path_uses_plain_authorization_header(self):
        transport = FakeTransport({"code": 200, "message": "success", "data": {"content": []}})
        client = OpenListClient("http://127.0.0.1:5244", "openlist-token-value", transport=transport)

        result = client.list_path("/115/电影")

        self.assertEqual(result["code"], 200)
        call = transport.calls[0]
        self.assertEqual(call["method"], "POST")
        self.assertEqual(call["url"], "http://127.0.0.1:5244/api/fs/list")
        self.assertEqual(call["headers"]["Authorization"], "openlist-token-value")
        self.assertEqual(call["data"]["path"], "/115/电影")
        self.assertNotIn("Bearer", call["headers"]["Authorization"])
        self.assertFalse(call["data"]["refresh"])

    def test_list_path_can_force_refresh(self):
        transport = FakeTransport({"code": 200, "message": "success", "data": {"content": []}})
        client = OpenListClient("http://127.0.0.1:5244", "openlist-token-value", transport=transport)

        client.list_path("/115/电影", refresh=True)

        self.assertTrue(transport.calls[0]["data"]["refresh"])

    def test_list_path_rejects_non_success_code(self):
        transport = FakeTransport({"code": 401, "message": "token is invalidated", "data": None})
        client = OpenListClient("http://127.0.0.1:5244", "openlist-token-value", transport=transport)

        with self.assertRaisesRegex(RuntimeError, "OpenList list failed"):
            client.list_path("/115/电影")

    def test_transport_converts_timeout_to_runtime_error(self):
        from pipeline.openlist import OpenListTransport

        with patch("pipeline.openlist.urllib.request.urlopen", side_effect=TimeoutError("timed out")):
            with self.assertRaisesRegex(RuntimeError, "OpenList request failed: timed out"):
                OpenListTransport().request("POST", "http://127.0.0.1:5244/api/fs/list", data={"path": "/root"}, timeout=1)

    def test_rename_path_uses_openlist_rename_endpoint(self):
        transport = FakeTransport({"code": 200, "message": "success", "data": None})
        client = OpenListClient("http://127.0.0.1:5244", "openlist-token-value", transport=transport)

        client.rename_path("/115/成人/old", "MIDA-304 - old")

        call = transport.calls[0]
        self.assertEqual(call["method"], "POST")
        self.assertEqual(call["url"], "http://127.0.0.1:5244/api/fs/rename")
        self.assertEqual(call["headers"]["Authorization"], "openlist-token-value")
        self.assertEqual(call["data"], {"path": "/115/成人/old", "name": "MIDA-304 - old"})

    def test_move_names_uses_openlist_move_endpoint(self):
        transport = FakeTransport({"code": 200, "message": "success", "data": None})
        client = OpenListClient("http://127.0.0.1:5244", "openlist-token-value", transport=transport)

        client.move_names("/115/剧集", "/115/动漫", ["成龙历险记"])

        call = transport.calls[0]
        self.assertEqual(call["method"], "POST")
        self.assertEqual(call["url"], "http://127.0.0.1:5244/api/fs/move")
        self.assertEqual(call["headers"]["Authorization"], "openlist-token-value")
        self.assertEqual(call["data"], {"src_dir": "/115/剧集", "dst_dir": "/115/动漫", "names": ["成龙历险记"]})


class MediaStationClientTest(unittest.TestCase):
    def test_list_libraries_uses_libraries_endpoint(self):
        transport = SequenceTransport(
            [
                {"tokens": {"access_token": "msg-token"}},
                {"data": {"items": [{"id": "lib-1", "name": "电影", "type": "movie"}]}},
            ]
        )
        client = MediaStationClient("http://127.0.0.1:18080/api", "admin", "secret", transport=transport)

        response = client.list_libraries(include_hidden=True)

        self.assertEqual(extract_library_items(response), [{"id": "lib-1", "name": "电影", "type": "movie"}])
        self.assertEqual(transport.calls[1]["method"], "GET")
        self.assertEqual(transport.calls[1]["url"], "http://127.0.0.1:18080/api/libraries?include_hidden=1")
        self.assertEqual(transport.calls[1]["headers"]["Authorization"], "Bearer msg-token")

    def test_scan_root_logs_in_and_uses_bearer_token(self):
        transport = SequenceTransport([{"tokens": {"access_token": "msg-token"}}, {"ok": True}])
        client = MediaStationClient("http://127.0.0.1:18080/api", "admin", "secret", transport=transport)

        result = client.scan_root("library-1", "root-1")

        self.assertEqual(result, {"ok": True})
        self.assertEqual(transport.calls[0]["method"], "POST")
        self.assertEqual(transport.calls[0]["url"], "http://127.0.0.1:18080/api/auth/login")
        self.assertEqual(transport.calls[0]["data"], {"username": "admin", "password": "secret"})
        self.assertEqual(transport.calls[1]["method"], "POST")
        self.assertEqual(transport.calls[1]["url"], "http://127.0.0.1:18080/api/libraries/library-1/roots/root-1/scan")
        self.assertEqual(transport.calls[1]["headers"]["Authorization"], "Bearer msg-token")

    def test_scrape_media_uses_single_item_scrape_body(self):
        transport = SequenceTransport([{"tokens": {"access_token": "msg-token"}}, {"ok": True}])
        client = MediaStationClient("http://127.0.0.1:18080/api", "admin", "secret", transport=transport)

        client.scrape_media("media-1")

        call = transport.calls[1]
        self.assertEqual(call["method"], "POST")
        self.assertEqual(call["url"], "http://127.0.0.1:18080/api/media/media-1/scrape")
        self.assertEqual(
            call["data"],
            {
                "episode_images": False,
                "refresh_matched": True,
                "include_matched": True,
            },
        )

    def test_update_media_metadata_patches_artwork_fields(self):
        transport = SequenceTransport([{"tokens": {"access_token": "msg-token"}}, {"id": "media-1"}])
        client = MediaStationClient("http://127.0.0.1:18080/api", "admin", "secret", transport=transport)

        client.update_media_metadata("media-1", {"poster_url": "https://img/poster.jpg", "title": "ignored"})

        call = transport.calls[1]
        self.assertEqual(call["method"], "PATCH")
        self.assertEqual(call["url"], "http://127.0.0.1:18080/api/media/media-1/metadata")
        self.assertEqual(call["data"], {"poster_url": "https://img/poster.jpg"})

    def test_adult_artwork_repair_patch_prefers_mgstage_poster(self):
        media = {
            "title": "ABF-159",
            "poster_url": "https://www.javbus.com/pics/cover/avrb_b.jpg",
            "backdrop_url": "https://image.mgstage.com/images/prestige/abf/159/cap_e_0_abf-159.jpg",
        }

        patch = adult_artwork_repair_patch(
            media,
            verifier=lambda url: url == "https://image.mgstage.com/images/prestige/abf/159/pf_o1_abf-159.jpg",
        )

        self.assertEqual(
            patch,
            {"poster_url": "https://image.mgstage.com/images/prestige/abf/159/pf_o1_abf-159.jpg"},
        )

    def test_adult_artwork_repair_patch_uses_dmm_candidates(self):
        media = {
            "title": "STARS-590",
            "poster_url": "https://www.javbus.com/pics/cover/8xio_b.jpg",
            "backdrop_url": "https://www.javbus.com/pics/sample/8xio_1.jpg",
        }

        patch = adult_artwork_repair_patch(
            media,
            verifier=lambda url: url
            in {
                "https://pics.dmm.co.jp/digital/video/1stars00590/1stars00590pl.jpg",
                "https://pics.dmm.co.jp/digital/video/1stars00590/1stars00590jp-1.jpg",
            },
        )

        self.assertEqual(patch["poster_url"], "https://pics.dmm.co.jp/digital/video/1stars00590/1stars00590pl.jpg")
        self.assertEqual(patch["backdrop_url"], "https://pics.dmm.co.jp/digital/video/1stars00590/1stars00590jp-1.jpg")
        self.assertTrue(is_bad_adult_artwork_url("https://www.javbus.com/pics/cover/8xio_b.jpg"))
        self.assertIn("stars00450", list(iter_dmm_cids("STARS-450")))
        self.assertEqual(
            list(
                iter_mgstage_poster_candidates(
                    "https://image.mgstage.com/images/prestige/abf/159/cap_e_0_abf-159.jpg"
                )
            )[0],
            "https://image.mgstage.com/images/prestige/abf/159/pf_o1_abf-159.jpg",
        )

    def test_adult_artwork_repair_skips_dmm_now_printing_placeholder(self):
        media = {
            "title": "STARS-590",
            "poster_url": "https://pics.dmm.co.jp/digital/video/stars590/stars590pl.jpg",
            "backdrop_url": "https://pics.dmm.co.jp/digital/video/stars590/stars590jp-1.jpg",
        }

        patch = adult_artwork_repair_patch(
            media,
            verifier=lambda url: url
            in {
                "https://pics.dmm.co.jp/digital/video/1stars00590/1stars00590pl.jpg",
                "https://pics.dmm.co.jp/digital/video/1stars00590/1stars00590jp-1.jpg",
            },
        )

        self.assertEqual(
            patch,
            {
                "poster_url": "https://pics.dmm.co.jp/digital/video/1stars00590/1stars00590pl.jpg",
                "backdrop_url": "https://pics.dmm.co.jp/digital/video/1stars00590/1stars00590jp-1.jpg",
            },
        )

    def test_adult_artwork_repair_clears_placeholder_when_replacement_is_missing(self):
        media = {
            "title": "STCV-017",
            "poster_url": "https://pics.dmm.co.jp/digital/video/stcv017/stcv017pl.jpg",
            "backdrop_url": "https://pics.dmm.co.jp/digital/video/stcv017/stcv017jp-1.jpg",
        }

        patch = adult_artwork_repair_patch(media, verifier=lambda url: False)

        self.assertEqual(patch, {"poster_url": "", "backdrop_url": ""})

    def test_reachable_image_url_rejects_dmm_now_printing_redirect(self):
        class PlaceholderImageResponse:
            status = 200
            headers = {"Content-Type": "image/jpeg"}

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def geturl(self):
                return "https://imgsrc.dmm.com/pics/mono/movie/n/now_printing/now_printing.jpg?w=800"

            def read(self, size=-1):
                return b"\xff\xd8\xff"

        with patch("pipeline.mediastation.urllib.request.urlopen", return_value=PlaceholderImageResponse()):
            self.assertFalse(reachable_image_url("https://pics.dmm.co.jp/digital/video/stcv017/stcv017pl.jpg"))

    def test_extracts_and_matches_media_items_flexibly(self):
        response = {"data": {"items": [{"id": "media-1", "library_id": "library-1", "title": "GANA-2525"}]}}

        items = extract_media_items(response)
        media = find_matching_media(items, ["GANA-2525"], library_id="library-1")

        self.assertEqual(extract_media_id(media), "media-1")

    def test_matching_prefers_main_feature_over_extras(self):
        items = [
            {
                "id": "menu-1",
                "library_id": "library-1",
                "title": "menu",
                "path": "cloud://openlist/115/电影/流浪地球2/menu/menu.mkv",
                "size_bytes": 175 * 1024 * 1024,
            },
            {
                "id": "extra-1",
                "library_id": "library-1",
                "title": "花絮",
                "path": "cloud://openlist/115/电影/流浪地球2/花絮/extra.mkv",
                "size_bytes": 5900 * 1024 * 1024,
            },
            {
                "id": "main-1",
                "library_id": "library-1",
                "title": "[DBD-Raws][4K_HDR][流浪地球2]",
                "path": "cloud://openlist/115/电影/流浪地球2/main.mkv",
                "size_bytes": 10122 * 1024 * 1024,
            },
        ]

        media = find_matching_media(items, ["流浪地球2"], library_id="library-1")

        self.assertEqual(extract_media_id(media), "main-1")

    def test_extract_codes_ignores_codec_tags_and_years(self):
        codes = extract_codes("[HEVC-10bit][H264-1080][Sintel.2010][ABF-363][GANA-2525]")

        self.assertEqual(codes, {"ABF-363", "GANA-2525"})

    def test_extract_codes_recognizes_standard_and_fc2_codes_only(self):
        codes = extract_codes(
            "SSIS-450 IPX_789 FSDSS.567 FC2-PPV-12345678 fc2ppv1234567 XXX-123ch "
            "n0680 k1234 Carib-020913-001 HEYDOUGA-1234-56 10musume 061234_01 10mu-022525_01"
        )

        self.assertEqual(
            codes,
            {
                "SSIS-450",
                "IPX-789",
                "FSDSS-567",
                "FC2-PPV-12345678",
                "FC2-PPV-1234567",
                "XXX-123",
            },
        )

    def test_strong_adult_code_query_accepts_exact_codes_and_excludes_long_titles(self):
        from pipeline.bot import is_strong_adult_code_query

        self.assertTrue(is_strong_adult_code_query("MIDE-882"))
        self.assertTrue(is_strong_adult_code_query("FC2-PPV-1234567"))
        self.assertTrue(is_strong_adult_code_query("BDMV-001"))
        self.assertFalse(is_strong_adult_code_query("电影名 MIDE-882 1080p"))


class ProwlarrClientTest(unittest.TestCase):
    def test_search_calls_prowlarr_api_with_query_limit_and_api_key(self):
        transport = FakeTransport([{"title": "Sintel 1080p", "seeders": 10, "magnetUrl": "magnet:?xt=urn:btih:abc"}])
        client = ProwlarrClient("http://127.0.0.1:9696", "prowlarr-key-value", transport=transport)

        results = client.search("sintel", limit=20)

        self.assertEqual(len(results), 1)
        call = transport.calls[0]
        self.assertEqual(call["method"], "GET")
        self.assertEqual(call["headers"]["X-Api-Key"], "prowlarr-key-value")
        self.assertEqual(call["url"], "http://127.0.0.1:9696/api/v1/search?query=sintel&limit=20")

    def test_search_can_limit_to_indexer_ids(self):
        transport = FakeTransport([])
        client = ProwlarrClient("http://127.0.0.1:9696", "prowlarr-key-value", transport=transport)

        client.search("ATFB-309", limit=1000, indexer_ids=[8])

        call = transport.calls[0]
        self.assertEqual(call["url"], "http://127.0.0.1:9696/api/v1/search?query=ATFB-309&limit=1000&indexerIds=8")

    def test_search_can_limit_to_categories(self):
        transport = FakeTransport([])
        client = ProwlarrClient("http://127.0.0.1:9696", "prowlarr-key-value", transport=transport)

        client.search("MIDE-882", limit=30, indexer_ids=[8], categories=[6000])

        call = transport.calls[0]
        self.assertEqual(call["url"], "http://127.0.0.1:9696/api/v1/search?query=MIDE-882&limit=30&categories=6000&indexerIds=8")

    def test_indexers_calls_prowlarr_indexer_api(self):
        transport = FakeTransport([{"id": 8, "name": "sukebei.nyaa.si"}])
        client = ProwlarrClient("http://127.0.0.1:9696", "prowlarr-key-value", transport=transport)

        results = client.indexers()

        self.assertEqual(results, [{"id": 8, "name": "sukebei.nyaa.si"}])
        self.assertEqual(transport.calls[0]["url"], "http://127.0.0.1:9696/api/v1/indexer")

    def test_resolve_download_uri_rebuilds_prowlarr_download_url_with_api_key(self):
        transport = DownloadRedirectTransport("magnet:?xt=urn:btih:ABC")
        client = ProwlarrClient("http://127.0.0.1:9696", "prowlarr-key-value", transport=transport)

        resolved = client.resolve_download_uri("prowlarr-download://9?link=ABC%2BDEF&file=SSIS-450")

        self.assertEqual(resolved, "magnet:?xt=urn:btih:ABC")
        self.assertEqual(
            transport.resolve_calls,
            [
                {
                    "url": "http://127.0.0.1:9696/9/download?apikey=prowlarr-key-value&link=ABC%2BDEF&file=SSIS-450",
                    "timeout": 30,
                }
            ],
        )

    def test_torrent_bytes_to_magnet_uses_bencoded_info_hash(self):
        info = (
            b"d"
            b"6:lengthi123e"
            b"4:name8:test.mkv"
            b"12:piece lengthi16384e"
            b"6:pieces20:aaaaaaaaaaaaaaaaaaaa"
            b"e"
        )
        torrent = b"d8:announce14:http://tracker4:info" + info + b"e"

        magnet = torrent_bytes_to_magnet(torrent)

        self.assertIn("xt=urn:btih:%s" % hashlib.sha1(info).hexdigest(), magnet)
        self.assertIn("dn=test.mkv", magnet)
        self.assertIn("tr=http:%2F%2Ftracker", magnet)


class ResourceSelectorTest(unittest.TestCase):
    def test_select_ranked_orders_candidates_and_assigns_one_based_rank(self):
        selector = ResourceSelector()

        ranked = selector.select_ranked(
            [
                {"title": "Sintel 720p", "seeders": 90, "infoHash": "BBB"},
                {"title": "Sintel 1080p BluRay", "seeders": 60, "infoHash": "AAA"},
                {"title": "Sintel CAM", "seeders": 500, "infoHash": "BAD"},
                {"title": "Sintel Dead", "seeders": 0, "infoHash": "DDD"},
            ]
        )

        self.assertEqual([item["rank"] for item in ranked], [1, 2, 3])
        self.assertEqual([item["title"] for item in ranked], ["Sintel 1080p BluRay", "Sintel 720p", "Sintel CAM"])
        self.assertEqual(ranked[0]["download_uri"], "magnet:?xt=urn:btih:AAA")

    def test_select_ranked_filters_unrelated_high_seed_results_by_query(self):
        selector = ResourceSelector()

        ranked = selector.select_ranked(
            [
                {"title": "流浪地球 2019 电影 国语中字 高清", "seeders": 3, "infoHash": "GOOD"},
                {"title": "Obsession.2026.1080p.AMZN.WEB-DL", "seeders": 9582, "infoHash": "BAD"},
            ],
            query="流浪地球",
        )

        self.assertEqual([item["title"] for item in ranked], ["流浪地球 2019 电影 国语中字 高清"])
        self.assertEqual(ranked[0]["download_uri"], "magnet:?xt=urn:btih:GOOD")

    def test_select_ranked_accepts_prowlarr_download_proxy_without_storing_api_key(self):
        selector = ResourceSelector()

        ranked = selector.select_ranked(
            [
                {
                    "title": "SSIS-450",
                    "seeders": 1,
                    "indexer": "0Magnet",
                    "downloadUrl": "http://127.0.0.1:9696/9/download?apikey=secret-key&link=ABC%2BDEF&file=SSIS-450",
                }
            ],
            query="SSIS-450",
        )

        self.assertEqual(ranked[0]["download_uri"], "prowlarr-download://9?link=ABC%2BDEF&file=SSIS-450")
        self.assertNotIn("secret-key", ranked[0]["download_uri"])

    def test_select_ranked_matches_code_queries_across_punctuation(self):
        selector = ResourceSelector()

        ranked = selector.select_ranked(
            [
                {"title": "MIDE-882 1080p", "seeders": 10, "infoHash": "GOOD"},
                {"title": "MIDE 777 1080p", "seeders": 100, "infoHash": "BAD"},
            ],
            query="MIDE-882",
        )

        self.assertEqual([item["title"] for item in ranked], ["MIDE-882 1080p"])

    def test_select_ranked_adds_uncensored_bonus_on_existing_score(self):
        selector = ResourceSelector()

        ranked = selector.select_ranked(
            [
                {"title": "STAR-646 1080p", "seeders": 10, "infoHash": "CEN"},
                {"title": "STAR-646 720p UC", "seeders": 10, "infoHash": "UNC"},
            ],
            query="STAR-646",
        )

        self.assertEqual([item["title"] for item in ranked], ["STAR-646 720p UC", "STAR-646 1080p"])

    def test_select_ranked_adds_chinese_subtitle_bonus_on_existing_score(self):
        selector = ResourceSelector()

        ranked = selector.select_ranked(
            [
                {"title": "MIDE-882 1080p", "seeders": 10, "infoHash": "NOSUB"},
                {"title": "MIDE-882 1080p 中字", "seeders": 10, "infoHash": "CHS"},
            ],
            query="MIDE-882",
        )

        self.assertEqual([item["title"] for item in ranked], ["MIDE-882 1080p 中字", "MIDE-882 1080p"])

    def test_select_ranked_prefers_exact_code_match_over_high_seed_suffix_title(self):
        selector = ResourceSelector()

        ranked = selector.select_ranked(
            [
                {"title": "SSIS-450", "seeders": 1, "infoHash": "EXACT"},
                {"title": "SSIS-450-C", "seeders": 100, "infoHash": "SUFFIX"},
            ],
            query="SSIS-450",
        )

        self.assertEqual([item["title"] for item in ranked], ["SSIS-450", "SSIS-450-C"])

    def test_select_ranked_uses_seeders_as_tiebreaker_not_primary_weight(self):
        selector = ResourceSelector()

        ranked = selector.select_ranked(
            [
                {"title": "MIDE-882 1080p 中字", "seeders": 10, "infoHash": "CHS"},
                {"title": "MIDE-882 1080p", "seeders": 11, "infoHash": "MORE"},
            ],
            query="MIDE-882",
        )

        self.assertEqual([item["title"] for item in ranked], ["MIDE-882 1080p 中字", "MIDE-882 1080p"])

    def test_select_ranked_adds_sukebei_indexer_bonus(self):
        selector = ResourceSelector()

        ranked = selector.select_ranked(
            [
                {"title": "ATFB-309 1080p", "indexer": "TorrentKitty", "seeders": 10, "infoHash": "OTHER"},
                {"title": "ATFB-309 720p", "indexer": "sukebei.nyaa.si", "seeders": 10, "infoHash": "SUKEBEI"},
            ],
            query="ATFB-309",
        )

        self.assertEqual([item["indexer"] for item in ranked], ["sukebei.nyaa.si", "TorrentKitty"])

    def test_select_ranked_uses_prowlarr_indexer_priority_when_available(self):
        selector = ResourceSelector(indexer_priorities={1: 25, 8: 1})

        ranked = selector.select_ranked(
            [
                {"title": "MIDE-882 1080p", "indexer": "Knaben", "indexerId": 1, "seeders": 30, "infoHash": "K1"},
                {"title": "MIDE-882 720p", "indexer": "sukebei.nyaa.si", "indexerId": 8, "seeders": 0, "infoHash": "S1"},
            ],
            query="MIDE-882",
        )

        self.assertEqual([item["indexer"] for item in ranked], ["sukebei.nyaa.si", "Knaben"])

    def test_select_ranked_does_not_let_one_extra_seeder_override_sukebei_bonus(self):
        selector = ResourceSelector()

        ranked = selector.select_ranked(
            [
                {"title": "ATFB-309 1080p", "indexer": "sukebei.nyaa.si", "seeders": 10, "infoHash": "SUKEBEI"},
                {"title": "ATFB-309 1080p", "indexer": "TorrentKitty", "seeders": 11, "infoHash": "OTHER"},
            ],
            query="ATFB-309",
        )

        self.assertEqual([item["indexer"] for item in ranked], ["sukebei.nyaa.si", "TorrentKitty"])

    def test_select_ranked_keeps_zero_seed_sukebei_candidates(self):
        selector = ResourceSelector()

        ranked = selector.select_ranked(
            [
                {"title": "ATFB-309 1080p", "indexer": "sukebei.nyaa.si", "seeders": 0, "infoHash": "SUKEBEI"},
            ],
            query="ATFB-309",
        )

        self.assertEqual([item["indexer"] for item in ranked], ["sukebei.nyaa.si"])

    def test_select_ranked_keeps_zero_seed_dht_candidates_but_penalizes_them(self):
        selector = ResourceSelector()

        ranked = selector.select_ranked(
            [
                {"title": "Sintel 1080p", "indexer": "Knaben", "seeders": 0, "infoHash": "DHT"},
                {"title": "Sintel 1080p", "indexer": "LimeTorrents", "seeders": 1, "infoHash": "SEEDED"},
                {"title": "Sintel 1080p", "indexer": "Unknown", "seeders": 0, "infoHash": "DROP"},
            ],
            query="Sintel",
        )

        self.assertEqual([item["infoHash"] for item in ranked], ["SEEDED", "DHT"])

    def test_select_ranked_limited_preserves_all_sukebei_candidates(self):
        selector = ResourceSelector()

        ranked = selector.select_ranked_limited(
            [
                {"title": "ATFB-309 other 1", "indexer": "TorrentKitty", "seeders": 20, "infoHash": "O1"},
                {"title": "ATFB-309 other 2", "indexer": "TorrentKitty", "seeders": 19, "infoHash": "O2"},
                {"title": "ATFB-309 sukebei 1", "indexer": "sukebei.nyaa.si", "seeders": 0, "infoHash": "S1"},
                {"title": "ATFB-309 sukebei 2", "indexer": "sukebei.nyaa.si", "seeders": 0, "infoHash": "S2"},
            ],
            query="ATFB-309",
            limit=2,
        )

        self.assertEqual([item["infoHash"] for item in ranked], ["S1", "S2"])
        self.assertEqual([item["rank"] for item in ranked], [1, 2])

    def test_select_ranked_deduplicates_same_info_hash(self):
        selector = ResourceSelector()

        ranked = selector.select_ranked(
            [
                {"title": "流浪地球 1080p", "indexer": "Knaben", "seeders": 7, "infoHash": "ABC"},
                {"title": "流浪地球 1080p", "indexer": "Nyaa.si", "seeders": 8, "magnetUrl": "magnet:?xt=urn:btih:abc"},
            ],
            query="流浪地球",
        )

        self.assertEqual(len(ranked), 1)
        self.assertEqual(ranked[0]["indexer"], "Nyaa.si")

    def test_select_ranked_prefers_sukebei_when_deduplicating_same_info_hash(self):
        selector = ResourceSelector()

        ranked = selector.select_ranked(
            [
                {"title": "ATFB-309", "indexer": "TorrentKitty", "seeders": 10, "infoHash": "ABC"},
                {"title": "ATFB-309", "indexer": "sukebei.nyaa.si", "seeders": 0, "infoHash": "ABC"},
            ],
            query="ATFB-309",
        )

        self.assertEqual(len(ranked), 1)
        self.assertEqual(ranked[0]["indexer"], "sukebei.nyaa.si")

    def test_select_ranked_does_not_filter_by_reported_size(self):
        selector = ResourceSelector()

        ranked = selector.select_ranked(
            [
                {"title": "ATFB-309 trailer", "seeders": 100, "size": 50 * 1024 * 1024, "infoHash": "SMALL"},
                {"title": "ATFB-309 full", "seeders": 2, "size": 1024 * 1024 * 1024, "infoHash": "GOOD"},
                {"title": "ATFB-309 1080p", "seeders": 1, "size": 0, "infoHash": "ZERO"},
            ],
            query="ATFB-309",
        )

        self.assertEqual([item["title"] for item in ranked], ["ATFB-309 1080p", "ATFB-309 full", "ATFB-309 trailer"])

    def test_select_rank_rejects_out_of_range_without_fallback(self):
        selector = ResourceSelector()

        with self.assertRaisesRegex(RuntimeError, "resource rank out of range"):
            selector.select_rank([{"title": "Sintel 1080p", "seeders": 10, "infoHash": "ABC"}], rank=2)

    def test_prefers_high_seeded_1080p_candidate_with_usable_uri(self):
        selector = ResourceSelector()
        candidates = [
            {"title": "Sintel CAM", "seeders": 500, "size": 1000000000, "magnetUrl": "magnet:?xt=urn:btih:bad"},
            {"title": "Sintel 1080p BluRay", "seeders": 60, "size": 3000000000, "magnetUrl": "magnet:?xt=urn:btih:good"},
            {"title": "Sintel 720p", "seeders": 90, "size": 1200000000, "magnetUrl": ""},
        ]

        selected = selector.select_best(candidates)

        self.assertEqual(selected["title"], "Sintel 1080p BluRay")
        self.assertEqual(selected["download_uri"], "magnet:?xt=urn:btih:good")

    def test_builds_magnet_from_info_hash_when_url_is_missing(self):
        selector = ResourceSelector()

        selected = selector.select_best([{"title": "Sintel 1080p", "seeders": 10, "infoHash": "ABCDEF"}])

        self.assertEqual(selected["download_uri"], "magnet:?xt=urn:btih:ABCDEF")

    def test_uses_info_hash_when_magnet_url_is_local_http_proxy(self):
        selector = ResourceSelector()

        selected = selector.select_best(
            [
                {
                    "title": "Sintel 1080p",
                    "seeders": 10,
                    "magnetUrl": "http://127.0.0.1:9696/1/download?apikey=secret",
                    "infoHash": "ABCDEF",
                }
            ]
        )

        self.assertEqual(selected["download_uri"], "magnet:?xt=urn:btih:ABCDEF")

    def test_rejects_local_download_url_without_magnet_or_info_hash(self):
        selector = ResourceSelector()

        with self.assertRaisesRegex(RuntimeError, "no acceptable resource"):
            selector.select_best(
                [
                    {
                        "title": "Sintel 1080p",
                        "seeders": 10,
                        "downloadUrl": "http://127.0.0.1:9696/1/download?apikey=secret",
                    }
                ]
            )

    def test_rejects_candidates_without_positive_seeders(self):
        selector = ResourceSelector()

        with self.assertRaisesRegex(RuntimeError, "no acceptable resource"):
            selector.select_best([{"title": "Sintel 1080p", "seeders": 0, "magnetUrl": "magnet:?xt=urn:btih:abc"}])

    def test_public_summary_redacts_sensitive_query_parameters(self):
        summary = public_resource_summary(
            {
                "title": "Sintel",
                "download_uri": "http://127.0.0.1:9696/1/download?apikey=secret&link=value",
            }
        )

        self.assertEqual(summary["download_uri"], "http://127.0.0.1:9696/1/download?apikey=REDACTED&link=value")


class Client115Test(unittest.TestCase):
    def test_add_offline_task_uses_official_endpoint_and_target_folder(self):
        transport = FakeTransport({"state": True, "data": [{"info_hash": "abc", "url": "magnet:?xt=urn:btih:abc"}]})
        client = Client115("access-token-value", transport=transport)

        result = client.add_offline_urls(["magnet:?xt=urn:btih:abc"], "3464134653584082023")

        self.assertEqual(result["state"], True)
        self.assertEqual(len(transport.calls), 1)
        call = transport.calls[0]
        self.assertEqual(call["method"], "POST")
        self.assertEqual(call["url"], "https://proapi.115.com/open/offline/add_task_urls")
        self.assertEqual(call["headers"]["Authorization"], "Bearer access-token-value")
        self.assertEqual(call["data"], {"urls": "magnet:?xt=urn:btih:abc", "wp_path_id": "3464134653584082023"})

    def test_delete_offline_task_uses_official_endpoint_without_deleting_files(self):
        transport = FakeTransport({"state": True, "data": []})
        client = Client115("access-token-value", transport=transport)

        result = client.delete_offline_task("ABC", delete_files=False)

        self.assertEqual(result["state"], True)
        call = transport.calls[0]
        self.assertEqual(call["method"], "POST")
        self.assertEqual(call["url"], "https://proapi.115.com/open/offline/del_task")
        self.assertEqual(call["headers"]["Authorization"], "Bearer access-token-value")
        self.assertEqual(call["data"], {"info_hash": "ABC", "del_source_file": "0"})

    def test_client_never_calls_refresh_token_endpoint(self):
        transport = FakeTransport({"state": False, "code": 40140125, "message": "access_token invalid"})
        client = Client115("expired-token", transport=transport)

        result = client.get_offline_tasks(page=1)

        self.assertEqual(result["code"], 40140125)
        self.assertNotIn("refreshToken", transport.calls[0]["url"])

    def test_get_folder_info_uses_official_folder_info_endpoint(self):
        transport = FakeTransport({"state": True, "data": {"file_id": "3464134653584082023", "file_name": "影视库-电影"}})
        client = Client115("access-token-value", transport=transport)

        result = client.get_folder_info("3464134653584082023")

        self.assertEqual(result["state"], True)
        call = transport.calls[0]
        self.assertEqual(call["method"], "GET")
        self.assertEqual(
            call["url"],
            "https://proapi.115.com/open/folder/get_info?file_id=3464134653584082023",
        )


class OfflineTaskTest(unittest.TestCase):
    def test_normalizes_115_status_code(self):
        task = normalize_task({"info_hash": "ABC", "status": 2, "percentDone": 100, "name": "Movie"})

        self.assertEqual(task["info_hash"], "ABC")
        self.assertEqual(task["status_name"], "success")
        self.assertEqual(task["percent_done"], 100)

    def test_finds_task_by_info_hash_case_insensitively(self):
        client = Fake115TaskClient(
            [
                {"state": True, "data": {"page_count": 2, "tasks": [{"info_hash": "AAA", "status": 1}]}},
                {"state": True, "data": {"page_count": 2, "tasks": [{"info_hash": "BbB", "status": 2}]}},
            ]
        )

        task = find_task_by_info_hash(client, "bbb")

        self.assertEqual(task["info_hash"], "BbB")
        self.assertEqual(task["status_name"], "success")
        self.assertEqual(client.calls, [1, 2])

    def test_finds_multiple_tasks_by_info_hashes_with_one_page_scan(self):
        client = Fake115TaskClient(
            [
                {
                    "state": True,
                    "data": {
                        "page_count": 1,
                        "tasks": [
                            {"info_hash": "AAA", "status": 1},
                            {"info_hash": "BbB", "status": 2},
                            {"info_hash": "CCC", "status": -1},
                        ],
                    },
                }
            ]
        )

        tasks = find_tasks_by_info_hashes(client, ["bbb", "ccc", "missing"])

        self.assertEqual(set(tasks.keys()), {"bbb", "ccc"})
        self.assertEqual(tasks["bbb"]["info_hash"], "BbB")
        self.assertEqual(tasks["bbb"]["status_name"], "success")
        self.assertEqual(tasks["ccc"]["status_name"], "failed")
        self.assertEqual(client.calls, [1])

    def test_rejects_missing_task_without_fallback(self):
        client = Fake115TaskClient([{"state": True, "data": {"page_count": 1, "tasks": []}}])

        with self.assertRaisesRegex(RuntimeError, "offline task not found"):
            find_task_by_info_hash(client, "missing")

    def test_wait_for_task_returns_when_success(self):
        client = Fake115TaskClient(
            [
                {"state": True, "data": {"page_count": 1, "tasks": [{"info_hash": "ABC", "status": 1}]}},
                {"state": True, "data": {"page_count": 1, "tasks": [{"info_hash": "ABC", "status": 2}]}},
            ]
        )
        sleeps = []

        task = wait_for_task(client, "ABC", timeout_seconds=30, interval_seconds=5, sleep=sleeps.append, now=StepClock())

        self.assertEqual(task["status_name"], "success")
        self.assertEqual(sleeps, [5])

    def test_wait_for_task_fails_fast_on_failed_status(self):
        client = Fake115TaskClient([{"state": True, "data": {"page_count": 1, "tasks": [{"info_hash": "ABC", "status": -1}]}}])

        with self.assertRaisesRegex(RuntimeError, "offline task failed"):
            wait_for_task(client, "ABC", timeout_seconds=30, interval_seconds=5, sleep=lambda seconds: None, now=StepClock())

    def test_wait_for_task_times_out_without_fallback(self):
        client = Fake115TaskClient([{"state": True, "data": {"page_count": 1, "tasks": [{"info_hash": "ABC", "status": 1}]}}])

        with self.assertRaisesRegex(TimeoutError, "offline task wait timeout"):
            wait_for_task(client, "ABC", timeout_seconds=1, interval_seconds=5, sleep=lambda seconds: None, now=StepClock(step=2))

    def test_task_can_cancel_only_active_statuses(self):
        self.assertTrue(task_can_cancel({"status_name": "downloading"}))
        self.assertTrue(task_can_cancel({"status_name": "allocating"}))
        self.assertFalse(task_can_cancel({"status_name": "success"}))
        self.assertFalse(task_can_cancel({"status_name": "failed"}))

    def test_cancel_task_if_active_deletes_offline_task_without_files(self):
        client = Fake115CancelClient(
            {"state": True, "data": {"page_count": 1, "tasks": [{"info_hash": "ABC", "status": 1, "percentDone": 10}]}},
            {"state": True, "data": []},
        )

        result = cancel_task_if_active(client, "ABC")

        self.assertTrue(result["cancelled"])
        self.assertEqual(result["task"]["status_name"], "cancelled")
        self.assertEqual(client.deleted, [("ABC", False)])

    def test_cancel_task_if_active_skips_finished_task(self):
        client = Fake115CancelClient(
            {"state": True, "data": {"page_count": 1, "tasks": [{"info_hash": "ABC", "status": 2, "percentDone": 100}]}},
            {"state": True, "data": []},
        )

        result = cancel_task_if_active(client, "ABC")

        self.assertFalse(result["cancelled"])
        self.assertEqual(result["task"]["status_name"], "success")
        self.assertEqual(client.deleted, [])

    def test_offline_submit_summary_keeps_info_hash_and_drops_url(self):
        summary = summarize_offline_submit(
            {
                "state": True,
                "data": [
                    {
                        "info_hash": "ABC",
                        "state": True,
                        "code": 0,
                        "url": "magnet:?xt=urn:btih:ABC",
                    }
                ],
            }
        )

        self.assertEqual(summary["tasks"][0]["info_hash"], "ABC")
        self.assertNotIn("url", summary["tasks"][0])


class CliSubmitSearchTest(unittest.TestCase):
    def test_search_prints_ranked_candidate_list(self):
        fake_prowlarr = FakeProwlarr(
            [
                {"title": "Sintel 720p", "seeders": 90, "size": 1200000000, "infoHash": "BBB"},
                {"title": "Sintel 1080p", "seeders": 60, "size": 3000000000, "infoHash": "AAA"},
            ]
        )
        stdout = io.StringIO()

        with patch("pipeline.cli.build_prowlarr_client", return_value=fake_prowlarr), patch("sys.stdout", stdout):
            code = cli_main(["search", "--query", "sintel", "--limit", "10"])

        payload = json.loads(stdout.getvalue())
        self.assertEqual(code, 0)
        self.assertEqual(payload["query"], "sintel")
        self.assertEqual(payload["count"], 2)
        self.assertEqual([item["rank"] for item in payload["results"]], [1, 2])
        self.assertEqual(payload["results"][0]["download_uri"], "magnet:?xt=urn:btih:AAA")

    def test_search_limits_ranked_candidate_list_locally(self):
        fake_prowlarr = FakeProwlarr(
            [
                {"title": "Sintel 720p", "seeders": 90, "infoHash": "BBB"},
                {"title": "Sintel 1080p", "seeders": 60, "infoHash": "AAA"},
            ]
        )
        stdout = io.StringIO()

        with patch("pipeline.cli.build_prowlarr_client", return_value=fake_prowlarr), patch("sys.stdout", stdout):
            code = cli_main(["search", "--query", "sintel", "--limit", "1"])

        payload = json.loads(stdout.getvalue())
        self.assertEqual(code, 0)
        self.assertEqual(payload["count"], 1)
        self.assertEqual([item["rank"] for item in payload["results"]], [1])

    def test_search_includes_all_sukebei_results_even_beyond_limit(self):
        fake_prowlarr = FakeProwlarr(
            [{"title": "ATFB-309 other", "indexer": "TorrentKitty", "seeders": 20, "infoHash": "OTHER"}],
            indexers=[{"id": 8, "name": "sukebei.nyaa.si"}],
            indexer_results={
                (8,): [
                    {"title": "ATFB-309 sukebei 1", "indexer": "sukebei.nyaa.si", "seeders": 0, "infoHash": "S1"},
                    {"title": "ATFB-309 sukebei 2", "indexer": "sukebei.nyaa.si", "seeders": 0, "infoHash": "S2"},
                ]
            },
        )
        stdout = io.StringIO()

        with patch("pipeline.cli.build_prowlarr_client", return_value=fake_prowlarr), patch("sys.stdout", stdout):
            code = cli_main(["search", "--query", "ATFB-309", "--limit", "1"])

        payload = json.loads(stdout.getvalue())
        self.assertEqual(code, 0)
        self.assertEqual(payload["count"], 2)
        self.assertEqual([item["indexer"] for item in payload["results"]], ["sukebei.nyaa.si", "sukebei.nyaa.si"])
        self.assertEqual(fake_prowlarr.search_calls, [("ATFB-309", 1000, (8,))])

    def test_search_skips_sukebei_supplement_for_non_adult_non_code_query(self):
        fake_prowlarr = FakeProwlarr(
            [],
            indexers=[
                {"id": 1, "name": "Knaben", "enable": True},
                {"id": 8, "name": "sukebei.nyaa.si", "enable": True},
                {"id": 10, "name": "Mikan", "enable": True},
            ],
            indexer_results={
                (1,): [{"title": "鬼灭之刃 1080p", "indexer": "Knaben", "seeders": 20, "infoHash": "K1"}],
                (8,): [{"title": "鬼灭之刃 sukebei", "indexer": "sukebei.nyaa.si", "seeders": 20, "infoHash": "S1"}],
                (10,): [{"title": "鬼灭之刃 Mikan", "indexer": "Mikan", "seeders": 1, "infoHash": "M1"}],
            },
        )
        stdout = io.StringIO()

        with patch("pipeline.cli.build_prowlarr_client", return_value=fake_prowlarr), patch("sys.stdout", stdout):
            code = cli_main(["search", "--query", "鬼灭之刃", "--limit", "10"])

        payload = json.loads(stdout.getvalue())
        self.assertEqual(code, 0)
        self.assertEqual({item["indexer"] for item in payload["results"]}, {"Knaben", "Mikan"})
        self.assertEqual(fake_prowlarr.search_calls, [("鬼灭之刃", 100, (1,)), ("鬼灭之刃", 100, (10,))])

    def test_search_skips_anime_supplements_for_plain_movie_query(self):
        fake_prowlarr = FakeProwlarr(
            [],
            indexers=[
                {"id": 1, "name": "Knaben", "enable": True},
                {"id": 5, "name": "Nyaa.si", "enable": True},
                {"id": 6, "name": "ACG.RIP", "enable": True},
                {"id": 8, "name": "sukebei.nyaa.si", "enable": True},
                {"id": 10, "name": "Mikan", "enable": True},
                {"id": 11, "name": "Bangumi Moe", "enable": True},
            ],
            indexer_results={
                (1,): [{"title": "Sintel 1080p", "indexer": "Knaben", "seeders": 20, "infoHash": "K1"}],
                (5,): [{"title": "Sintel Nyaa", "indexer": "Nyaa.si", "seeders": 20, "infoHash": "N1"}],
                (6,): [{"title": "Sintel ACG", "indexer": "ACG.RIP", "seeders": 20, "infoHash": "A1"}],
                (10,): [{"title": "Sintel Mikan", "indexer": "Mikan", "seeders": 20, "infoHash": "M1"}],
                (11,): [{"title": "Sintel Bangumi", "indexer": "Bangumi Moe", "seeders": 20, "infoHash": "B1"}],
            },
        )
        stdout = io.StringIO()

        with patch("pipeline.cli.build_prowlarr_client", return_value=fake_prowlarr), patch("sys.stdout", stdout):
            code = cli_main(["search", "--query", "Sintel", "--limit", "10"])

        payload = json.loads(stdout.getvalue())
        self.assertEqual(code, 0)
        self.assertEqual([item["indexer"] for item in payload["results"]], ["Knaben"])
        self.assertEqual(fake_prowlarr.search_calls, [("Sintel", 100, (1,))])

    def test_search_excludes_anime_specialized_indexers_from_primary_call_and_adds_them_as_supplements(self):
        fake_prowlarr = FakeProwlarr(
            [],
            indexers=[
                {"id": 1, "name": "Knaben", "enable": True},
                {"id": 5, "name": "Nyaa.si", "enable": True},
                {"id": 6, "name": "ACG.RIP", "enable": True},
                {"id": 10, "name": "Mikan", "enable": True},
                {"id": 11, "name": "Bangumi Moe", "enable": True},
            ],
            indexer_results={
                (1,): [{"title": "葬送的芙莉莲 1080p", "indexer": "Knaben", "seeders": 20, "infoHash": "K1"}],
                (5,): [{"title": "葬送的芙莉莲 Nyaa", "indexer": "Nyaa.si", "seeders": 2, "infoHash": "N1"}],
                (6,): [{"title": "葬送的芙莉莲 ACG", "indexer": "ACG.RIP", "seeders": 3, "infoHash": "A1"}],
                (10,): [{"title": "葬送的芙莉莲 Mikan", "indexer": "Mikan", "seeders": 1, "infoHash": "M1"}],
                (11,): [{"title": "葬送的芙莉莲 Bangumi", "indexer": "Bangumi Moe", "seeders": 1, "infoHash": "B1"}],
            },
        )
        stdout = io.StringIO()

        with patch("pipeline.cli.build_prowlarr_client", return_value=fake_prowlarr), patch("sys.stdout", stdout):
            code = cli_main(["search", "--query", "葬送的芙莉莲", "--limit", "10"])

        payload = json.loads(stdout.getvalue())
        self.assertEqual(code, 0)
        self.assertEqual(payload["count"], 5)
        self.assertEqual({item["indexer"] for item in payload["results"]}, {"Knaben", "Nyaa.si", "ACG.RIP", "Mikan", "Bangumi Moe"})
        self.assertEqual(
            fake_prowlarr.search_calls,
            [
                ("葬送的芙莉莲", 100, (1,)),
                ("葬送的芙莉莲", 100, (5,)),
                ("葬送的芙莉莲", 100, (6,)),
                ("葬送的芙莉莲", 100, (10,)),
                ("葬送的芙莉莲", 100, (11,)),
            ],
        )

    def test_search_skips_disabled_supplement_indexers(self):
        fake_prowlarr = FakeProwlarr(
            [],
            indexers=[
                {"id": 1, "name": "Knaben", "enable": True},
                {"id": 8, "name": "sukebei.nyaa.si", "enable": False},
                {"id": 10, "name": "Mikan", "enable": False},
            ],
            indexer_results={
                (1,): [{"title": "ATFB-309 1080p", "indexer": "Knaben", "seeders": 20, "infoHash": "K1"}],
                (8,): [{"title": "ATFB-309 sukebei", "indexer": "sukebei.nyaa.si", "seeders": 0, "infoHash": "S1"}],
                (10,): [{"title": "ATFB-309 Mikan", "indexer": "Mikan", "seeders": 1, "infoHash": "M1"}],
            },
        )
        stdout = io.StringIO()

        with patch("pipeline.cli.build_prowlarr_client", return_value=fake_prowlarr), patch("sys.stdout", stdout):
            code = cli_main(["search", "--query", "ATFB-309", "--limit", "10"])

        payload = json.loads(stdout.getvalue())
        self.assertEqual(code, 0)
        self.assertEqual([item["indexer"] for item in payload["results"]], ["Knaben"])
        self.assertEqual(fake_prowlarr.search_calls, [("ATFB-309", 100, (1,))])

    def test_profile_search_uses_externalized_categories(self):
        from pipeline.bot import SEARCH_PROFILE_GENERAL, search_profile_indexer_results

        fake_prowlarr = FakeProwlarr(
            [],
            indexers=[
                {
                    "id": 1,
                    "name": "MovieOnly",
                    "enable": True,
                    "capabilities": {"categories": [{"id": 2000}]},
                },
                {
                    "id": 2,
                    "name": "BookOnly",
                    "enable": True,
                    "capabilities": {"categories": [{"id": 7000}]},
                },
            ],
            indexer_results={
                (2,): [{"title": "Sintel Book", "indexer": "BookOnly", "seeders": 1, "infoHash": "B1"}],
            },
        )

        results = search_profile_indexer_results(
            fake_prowlarr,
            "sintel",
            SEARCH_PROFILE_GENERAL,
            100,
            indexers=fake_prowlarr.indexers(),
            categories_by_profile={SEARCH_PROFILE_GENERAL: (7000,)},
            max_workers=1,
        )

        self.assertEqual([item["indexer"] for item in results], ["BookOnly"])
        self.assertEqual(fake_prowlarr.search_calls, [("sintel", 100, (2,), (7000,))])

    def test_primary_search_falls_back_to_single_indexers_when_aggregate_times_out(self):
        from pipeline.bot import search_primary_indexer_results

        fake_prowlarr = FakeProwlarr(
            [],
            indexers=[
                {"id": 1, "name": "Knaben", "enable": True},
                {"id": 2, "name": "SlowIndexer", "enable": True},
            ],
            indexer_results={
                (1,): [{"title": "Sintel 1080p", "indexer": "Knaben", "seeders": 20, "infoHash": "K1"}],
            },
            indexer_errors={
                (1, 2): TimeoutError("timed out"),
                (2,): TimeoutError("timed out"),
            },
        )

        results = search_primary_indexer_results(fake_prowlarr, "sintel", 100, indexers=fake_prowlarr.indexers())

        self.assertEqual([item["infoHash"] for item in results], ["K1"])
        self.assertEqual(
            fake_prowlarr.search_calls,
            [("sintel", 100, (1, 2)), ("sintel", 100, (1,)), ("sintel", 100, (2,))],
        )

    def test_primary_search_records_source_timing_stats(self):
        from pipeline.bot import search_primary_indexer_results
        from pipeline.search_stats import SearchStats

        fake_prowlarr = FakeProwlarr(
            [],
            indexers=[
                {"id": 1, "name": "Knaben", "enable": True},
                {"id": 2, "name": "SlowIndexer", "enable": True},
            ],
            indexer_results={
                (1,): [{"title": "Sintel 1080p", "indexer": "Knaben", "seeders": 20, "infoHash": "K1"}],
            },
            indexer_errors={
                (1, 2): TimeoutError("timed out"),
                (2,): TimeoutError("timed out"),
            },
        )
        stats = SearchStats()

        results = search_primary_indexer_results(fake_prowlarr, "sintel", 100, indexers=fake_prowlarr.indexers(), stats=stats)
        metadata = stats.to_metadata(raw_count=len(results), selected_count=len(results))

        self.assertEqual([item["infoHash"] for item in results], ["K1"])
        self.assertEqual(metadata["failed_count"], 2)
        self.assertEqual(metadata["success_count"], 1)
        self.assertEqual([source["source"] for source in metadata["sources"]], ["primary aggregate", "Knaben", "SlowIndexer"])

    def test_submit_search_commit_uses_requested_rank(self):
        fake_prowlarr = FakeProwlarr(
            [
                {"title": "Sintel 720p", "seeders": 90, "infoHash": "BBB"},
                {"title": "Sintel 1080p", "seeders": 60, "infoHash": "AAA"},
            ]
        )
        fake_115 = Fake115SubmitClient({"state": True, "data": [{"info_hash": "AAA", "state": True, "code": 0}]})
        stdout = io.StringIO()

        with patch("pipeline.cli.build_prowlarr_client", return_value=fake_prowlarr), patch(
            "pipeline.cli.build_openlist_client", return_value=FakeOpenList()
        ), patch("pipeline.cli.OpenListTokenStore", return_value=FakeTokenStore("unused.db")), patch(
            "pipeline.cli.Client115", return_value=fake_115
        ), patch("sys.stdout", stdout):
            code = cli_main(
                [
                    "--openlist-db",
                    "unused.db",
                    "submit-search",
                    "--query",
                    "sintel",
                    "--category",
                    "movie",
                    "--rank",
                    "2",
                    "--commit",
                ]
            )

        payload = json.loads(stdout.getvalue())
        self.assertEqual(code, 0)
        self.assertEqual(fake_115.urls, ["magnet:?xt=urn:btih:BBB"])
        self.assertEqual(payload["selected"]["rank"], 2)

    def test_submit_search_rejects_rank_out_of_range_without_fallback(self):
        fake_prowlarr = FakeProwlarr([{"title": "Sintel 720p", "seeders": 90, "infoHash": "BBB"}])
        stderr = io.StringIO()

        with patch("pipeline.cli.build_prowlarr_client", return_value=fake_prowlarr), patch("sys.stderr", stderr):
            code = cli_run(["submit-search", "--query", "sintel", "--category", "movie", "--rank", "2"])

        self.assertEqual(code, 1)
        self.assertEqual(stderr.getvalue().strip(), "error: resource rank out of range: 2")

    def test_submit_search_commit_prints_info_hash_from_115_response(self):
        fake_prowlarr = FakeProwlarr([{"title": "Sintel 1080p", "seeders": 10, "infoHash": "ABC"}])
        fake_115 = Fake115SubmitClient(
            {
                "state": True,
                "data": [{"info_hash": "ABC", "state": True, "code": 0, "url": "magnet:?xt=urn:btih:ABC"}],
            }
        )
        fake_openlist = FakeOpenList()
        fake_token_store = FakeTokenStore("unused.db")
        stdout = io.StringIO()

        with patch("pipeline.cli.build_prowlarr_client", return_value=fake_prowlarr), patch(
            "pipeline.cli.build_openlist_client", return_value=fake_openlist
        ), patch(
            "pipeline.cli.OpenListTokenStore", return_value=fake_token_store
        ), patch("pipeline.cli.Client115", return_value=fake_115), patch("sys.stdout", stdout):
            code = cli_main(["--openlist-db", "unused.db", "submit-search", "--query", "sintel", "--category", "movie", "--commit"])

        payload = json.loads(stdout.getvalue())
        self.assertEqual(code, 0)
        self.assertEqual(fake_openlist.paths, ["/115/电影"])
        self.assertEqual(fake_token_store.load_count, 1)
        self.assertEqual(fake_115.urls, ["magnet:?xt=urn:btih:ABC"])
        self.assertEqual(fake_115.folder_id, "3464134653584082023")
        self.assertEqual(payload["submit"]["tasks"][0]["info_hash"], "ABC")
        self.assertNotIn("url", payload["submit"]["tasks"][0])

    def test_submit_search_adult_commit_warms_adult_openlist_path(self):
        fake_prowlarr = FakeProwlarr([{"title": "Adult 1080p", "seeders": 10, "infoHash": "DEF"}])
        fake_openlist = FakeOpenList()

        with patch("pipeline.cli.build_prowlarr_client", return_value=fake_prowlarr), patch(
            "pipeline.cli.build_openlist_client", return_value=fake_openlist
        ), patch("pipeline.cli.OpenListTokenStore", return_value=FakeTokenStore("unused.db")), patch(
            "pipeline.cli.Client115", return_value=Fake115SubmitClient({"state": True, "data": [{"info_hash": "DEF"}]})
        ), patch("sys.stdout", io.StringIO()):
            cli_main(["--openlist-db", "unused.db", "submit-search", "--query", "adult", "--category", "adult", "--commit"])

        self.assertEqual(fake_openlist.paths, ["/115/成人"])

    def test_cli_run_prints_business_error_without_traceback(self):
        stderr = io.StringIO()

        with patch("pipeline.cli.main", side_effect=RuntimeError("115 offline task list failed: access_token 无效")), patch(
            "sys.stderr", stderr
        ):
            code = cli_run(["task-status"])

        self.assertEqual(code, 1)
        self.assertEqual(stderr.getvalue().strip(), "error: 115 offline task list failed: access_token 无效")
        self.assertNotIn("Traceback", stderr.getvalue())

    def test_task_status_warms_openlist_before_loading_115_token(self):
        events = []
        fake_openlist = EventOpenList(events)
        fake_token_store = EventTokenStore("unused.db", events)
        stdout = io.StringIO()

        with patch("pipeline.cli.build_openlist_client", return_value=fake_openlist), patch(
            "pipeline.cli.OpenListTokenStore", return_value=fake_token_store
        ), patch("pipeline.cli.Client115", return_value=Fake115StatusClient()), patch("sys.stdout", stdout):
            code = cli_main(["--openlist-db", "unused.db", "task-status", "--info-hash", "ABC"])

        self.assertEqual(code, 0)
        self.assertEqual(events, ["warm:/115/电影", "load_token"])

    def test_add_offline_adult_commit_warms_adult_before_loading_115_token(self):
        events = []
        fake_openlist = EventOpenList(events)
        fake_token_store = EventTokenStore("unused.db", events)

        with patch("pipeline.cli.build_openlist_client", return_value=fake_openlist), patch(
            "pipeline.cli.OpenListTokenStore", return_value=fake_token_store
        ), patch("pipeline.cli.Client115", return_value=Fake115SubmitClient({"state": True, "data": [{"info_hash": "DEF"}]})), patch(
            "sys.stdout", io.StringIO()
        ):
            cli_main(
                [
                    "--openlist-db",
                    "unused.db",
                    "add-offline",
                    "--category",
                    "adult",
                    "--url",
                    "magnet:?xt=urn:btih:DEF",
                    "--commit",
                ]
            )

        self.assertEqual(events, ["warm:/115/成人", "load_token"])
