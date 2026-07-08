import copy
import json
import threading
import urllib.error

from tests.test_pipeline_core import *
from pipeline.subtitle_proxy import (
    emby_image_request_tag,
    emby_folder_cover_grid_tag,
    is_emby_placeholder_image_body,
)


class SubtitleProxyTest(unittest.TestCase):
    def test_adult_msg_library_id_resolves_lazily_and_caches(self):
        from pipeline import subtitle_proxy

        original_id = subtitle_proxy._ADULT_MSG_LIBRARY_ID
        original_resolver = subtitle_proxy.category_to_msg_library_root
        calls = []

        def fake_resolver(category):
            calls.append(category)
            return {"library_id": "adult-library"}

        try:
            subtitle_proxy._ADULT_MSG_LIBRARY_ID = None
            subtitle_proxy.category_to_msg_library_root = fake_resolver

            self.assertTrue(subtitle_proxy.emby_item_is_adult_media({"LibraryId": "adult-library"}))
            self.assertTrue(subtitle_proxy.emby_item_is_adult_media({"LibraryId": "adult-library"}))
        finally:
            subtitle_proxy._ADULT_MSG_LIBRARY_ID = original_id
            subtitle_proxy.category_to_msg_library_root = original_resolver

        self.assertEqual(calls, ["adult"])

    def test_subtitle_cache_saves_and_lists_local_tracks(self):
        from pipeline.external_subtitles import SubtitleCache, SubtitleDownload

        with tempfile.TemporaryDirectory() as tmp:
            cache = SubtitleCache(tmp)
            track = cache.save_download(
                "media-1",
                SubtitleDownload(
                    source="assrt",
                    provider_id="123",
                    filename="SSIS-218.chs.srt",
                    body=b"1\n00:00:01,000 --> 00:00:02,000\nhello\n",
                    lang="zh-Hans",
                    label="简体中文",
                    query="SSIS-218",
                ),
            )
            tracks = cache.list_tracks("media-1")
            body, filename = cache.read_local_uri(track["path"])

        self.assertEqual(len(tracks), 1)
        self.assertEqual(tracks[0]["source"], "assrt")
        self.assertEqual(tracks[0]["lang"], "zh-Hans")
        self.assertEqual(body, b"1\n00:00:01,000 --> 00:00:02,000\nhello\n")
        self.assertTrue(filename.endswith(".srt"))

    def test_assrt_provider_downloads_exact_adult_code_subtitle(self):
        from pipeline.external_subtitles import AssrtSubtitleProvider

        class FakeTransport:
            def __init__(self):
                self.json_urls = []
                self.download_urls = []

            def json_request(self, method, url, headers=None, data=None, timeout=None):
                self.json_urls.append(url)
                self.headers = headers
                if "/sub/search" in url:
                    return {
                        "status": 0,
                        "sub": {
                            "subs": [
                                {
                                    "id": 1,
                                    "native_name": "Other",
                                    "videoname": "OTHER-001",
                                    "lang": {"desc": "简体"},
                                },
                                {
                                    "id": 2,
                                    "native_name": "SSIS-218",
                                    "videoname": "SSIS-218 1080p",
                                    "lang": {"desc": "简体"},
                                },
                            ]
                        },
                    }
                return {
                    "status": 0,
                    "sub": {
                        "subs": [
                            {
                                "id": 2,
                                "filelist": [
                                    {"f": "SSIS-218.chs.srt", "url": "https://file.example/SSIS-218.chs.srt"},
                                    {"f": "cover.jpg", "url": "https://file.example/cover.jpg"},
                                ],
                            }
                        ]
                    },
                }

            def download(self, url, headers=None, timeout=None, max_bytes=None):
                self.download_urls.append(url)
                return b"subtitle-body"

        transport = FakeTransport()
        provider = AssrtSubtitleProvider("secret-token", transport=transport)
        candidates = provider.search("SSIS-218", code="SSIS-218")
        download = provider.download(candidates[0], "SSIS-218", code="SSIS-218")

        self.assertEqual(candidates[0]["id"], 2)
        self.assertEqual(download.filename, "SSIS-218.chs.srt")
        self.assertEqual(download.body, b"subtitle-body")
        self.assertEqual(transport.download_urls, ["https://file.example/SSIS-218.chs.srt"])
        self.assertNotIn("secret-token", transport.json_urls[0])
        self.assertEqual(transport.headers["Authorization"], "Bearer secret-token")

    def test_subtitlecat_provider_downloads_exact_adult_code_subtitle(self):
        from pipeline.external_subtitles import SubtitleCatProvider

        class FakeTransport:
            def __init__(self):
                self.text_urls = []
                self.download_urls = []

            def text_request(self, url, headers=None, timeout=None, max_bytes=None):
                self.text_urls.append(url)
                if "index.php" in url:
                    return """
                    <table><tbody>
                      <tr>
                        <td><a href="subs/1475/mimk-267-kor.html">mimk-267-kor</a> (translated from Korean)</td>
                        <td>9 downloads</td>
                      </tr>
                      <tr>
                        <td><a href="subs/1470/MIMK-267-C.html">MIMK-267-C</a> (translated from Chinese)</td>
                        <td>16 downloads</td>
                      </tr>
                    </tbody></table>
                    """
                return """
                <a id="download_zh-TW" href="/subs/1470/MIMK-267-C-zh-TW.srt" class="green-link">Download</a>
                <a id="download_zh-CN" href="/subs/1470/MIMK-267-C-zh-CN.srt" class="green-link">Download</a>
                """

            def download(self, url, headers=None, timeout=None, max_bytes=None):
                self.download_urls.append(url)
                return b"subtitlecat-body"

        transport = FakeTransport()
        provider = SubtitleCatProvider(transport=transport)
        candidates = provider.search("MIMK-267", code="MIMK-267")
        download = provider.download(candidates[0], "MIMK-267", code="MIMK-267")

        self.assertEqual(candidates[0]["title"], "MIMK-267-C")
        self.assertEqual(download.source, "subtitlecat")
        self.assertEqual(download.filename, "MIMK-267-C-zh-CN.srt")
        self.assertEqual(download.body, b"subtitlecat-body")
        self.assertEqual(transport.download_urls, ["https://www.subtitlecat.com/subs/1470/MIMK-267-C-zh-CN.srt"])

    def test_subtitlecat_provider_quotes_download_url_path(self):
        from pipeline.external_subtitles import SubtitleCatProvider

        class FakeTransport:
            def __init__(self):
                self.download_urls = []

            def text_request(self, url, headers=None, timeout=None, max_bytes=None):
                return """
                <a id="download_zh-CN" href="/subs/494/MIDV-373 [zh-TW] (2023)-zh-CN.srt" class="green-link">Download</a>
                <a id="download_zh-TW" href="/subs/281/KAWD-709 さくらゆら-zh-CN.srt" class="green-link">Download</a>
                """

            def download(self, url, headers=None, timeout=None, max_bytes=None):
                self.download_urls.append(url)
                return b"subtitlecat-body"

        transport = FakeTransport()
        provider = SubtitleCatProvider(transport=transport)
        download = provider.download(
            {"url": "https://www.subtitlecat.com/subs/494/MIDV-373.html", "filename": "MIDV-373", "_score": 10},
            "MIDV-373",
            code="MIDV-373",
        )

        self.assertEqual(download.filename, "MIDV-373 [zh-TW] (2023)-zh-CN.srt")
        self.assertEqual(
            transport.download_urls,
            ["https://www.subtitlecat.com/subs/494/MIDV-373%20%5Bzh-TW%5D%20%282023%29-zh-CN.srt"],
        )

    def test_build_subtitle_matcher_includes_subtitlecat_by_default(self):
        from pipeline.external_subtitles import build_subtitle_matcher_from_config

        class Config:
            subtitle_auto_match_enabled = True

        matcher = build_subtitle_matcher_from_config(Config())

        self.assertEqual([provider.name for provider in matcher.providers], ["subtitlecat", "assrt", "opensubtitles"])

    def test_subtitle_matcher_caches_first_matching_subtitle(self):
        from pipeline.external_subtitles import SubtitleCache, SubtitleDownload, SubtitleMatcher

        class FakeProvider:
            name = "fake"

            def enabled(self):
                return True

            def search(self, query, code=""):
                self.search_args = (query, code)
                return [{"id": "candidate-1"}]

            def download(self, candidate, query, code=""):
                return SubtitleDownload(
                    source=self.name,
                    provider_id="candidate-1",
                    filename="SSIS-218.sc.ass",
                    body=b"[Script Info]\n",
                    query=query,
                )

        with tempfile.TemporaryDirectory() as tmp:
            cache = SubtitleCache(tmp)
            provider = FakeProvider()
            matcher = SubtitleMatcher(cache, [provider], enabled=True, adult_only=True)
            result = matcher.match_task(
                "adult",
                "SSIS-218",
                {"msg_media_id": "media-1", "openlist_adult_code": "SSIS-218"},
            )
            tracks = cache.list_tracks("media-1")

        self.assertEqual(result["subtitle_match_status"], "success")
        self.assertEqual(result["subtitle_match_source"], "fake")
        self.assertEqual(provider.search_args, ("SSIS-218", "SSIS-218"))
        self.assertEqual(len(tracks), 1)
        self.assertEqual(tracks[0]["source"], "fake")

    def test_subtitle_matcher_continues_after_candidate_download_error(self):
        from pipeline.external_subtitles import SubtitleCache, SubtitleDownload, SubtitleMatcher

        class FakeProvider:
            name = "fake"

            def __init__(self):
                self.download_calls = []

            def enabled(self):
                return True

            def search(self, query, code=""):
                return [{"id": "needs-token"}, {"id": "dev-mode-ok"}]

            def download(self, candidate, query, code=""):
                self.download_calls.append(candidate["id"])
                if candidate["id"] == "needs-token":
                    raise RuntimeError("missing token")
                return SubtitleDownload(
                    source=self.name,
                    provider_id="dev-mode-ok",
                    filename="Avatar.chs.srt",
                    body=b"subtitle-body",
                    query=query,
                )

        with tempfile.TemporaryDirectory() as tmp:
            cache = SubtitleCache(tmp)
            provider = FakeProvider()
            matcher = SubtitleMatcher(cache, [provider], enabled=True, adult_only=False)
            result = matcher.match_task("movie", "Avatar 2009", {"msg_media_id": "media-1"})

        self.assertEqual(result["subtitle_match_status"], "success")
        self.assertEqual(result["subtitle_match_source"], "fake")
        self.assertEqual(provider.download_calls, ["needs-token", "dev-mode-ok"])

    def test_subtitle_matcher_search_candidates_does_not_write_cache(self):
        from pipeline.external_subtitles import SubtitleCache, SubtitleMatcher

        class FakeProvider:
            name = "fake"

            def enabled(self):
                return True

            def search(self, query, code=""):
                return [
                    {"id": "candidate-1", "filename": "SSIS-218.zh.srt", "_score": 100},
                    {"id": "candidate-2", "filename": "SSIS-218.chs.ass", "_score": 80},
                ]

            def download(self, candidate, query, code=""):
                raise AssertionError("search candidates must not download subtitles")

        with tempfile.TemporaryDirectory() as tmp:
            cache = SubtitleCache(tmp)
            matcher = SubtitleMatcher(cache, [FakeProvider()], enabled=True, adult_only=True)
            candidates = matcher.search_task_candidates(
                "adult",
                "SSIS-218",
                {"msg_media_id": "media-1", "openlist_adult_code": "SSIS-218"},
                limit=5,
            )
            tracks = cache.list_tracks("media-1")

        self.assertEqual([item["provider_id"] for item in candidates], ["candidate-1", "candidate-2"])
        self.assertEqual([item["rank"] for item in candidates], [1, 2])
        self.assertEqual(tracks, [])

    def test_subtitle_matcher_apply_candidate_writes_selected_subtitle(self):
        from pipeline.external_subtitles import SubtitleCache, SubtitleDownload, SubtitleMatcher

        class FakeProvider:
            name = "fake"

            def enabled(self):
                return True

            def download(self, candidate, query, code=""):
                self.download_args = (candidate["id"], query, code)
                return SubtitleDownload(
                    source=self.name,
                    provider_id=candidate["id"],
                    filename="SSIS-218.selected.srt",
                    body=b"1\n00:00:01,000 --> 00:00:02,000\nhello\n",
                    query=query,
                )

        with tempfile.TemporaryDirectory() as tmp:
            cache = SubtitleCache(tmp)
            provider = FakeProvider()
            matcher = SubtitleMatcher(cache, [provider], enabled=True, adult_only=True)
            result = matcher.apply_candidate(
                "media-1",
                {
                    "provider": "fake",
                    "query": "SSIS-218",
                    "code": "SSIS-218",
                    "candidate": {"id": "candidate-2"},
                },
            )
            tracks = cache.list_tracks("media-1")

        self.assertEqual(result["subtitle_match_status"], "success")
        self.assertEqual(provider.download_args, ("candidate-2", "SSIS-218", "SSIS-218"))
        self.assertEqual(len(tracks), 1)
        self.assertEqual(tracks[0]["source"], "fake")

    def test_subtitle_matcher_preview_candidate_downloads_body_without_cache(self):
        from pipeline.external_subtitles import SubtitleCache, SubtitleDownload, SubtitleMatcher

        class FakeProvider:
            name = "fake"

            def enabled(self):
                return True

            def download(self, candidate, query, code=""):
                return SubtitleDownload(
                    source=self.name,
                    provider_id=candidate["id"],
                    filename="SSIS-218.zh.srt",
                    body="1\n00:00:01,000 --> 00:00:02,000\n这是中文字幕正文\n".encode("utf-8"),
                    query=query,
                )

        with tempfile.TemporaryDirectory() as tmp:
            cache = SubtitleCache(tmp)
            matcher = SubtitleMatcher(cache, [FakeProvider()], enabled=True, adult_only=True)
            preview = matcher.preview_candidate(
                {
                    "provider": "fake",
                    "query": "SSIS-218",
                    "code": "SSIS-218",
                    "candidate": {"id": "candidate-1"},
                },
                max_chars=50,
            )
            tracks = cache.list_tracks("media-1")

        self.assertIn("这是中文字幕正文", preview["content_sample"])
        self.assertGreater(preview["preview_char_count"], 0)
        self.assertEqual(tracks, [])

    def test_local_subtitle_provider_reads_cached_subtitle(self):
        from pipeline.external_subtitles import LocalSubtitleProvider, SubtitleCache, SubtitleDownload

        with tempfile.TemporaryDirectory() as tmp:
            cache = SubtitleCache(tmp)
            track = cache.save_download(
                "media-1",
                SubtitleDownload(source="assrt", provider_id="1", filename="SSIS-218.srt", body=b"body"),
            )
            provider = LocalSubtitleProvider(tmp)
            tracks = provider.tracks_for_media_id("media-1")
            body, filename = provider.read_subtitle(track["path"])

        self.assertEqual(len(tracks), 1)
        self.assertEqual(body, b"body")
        self.assertTrue(filename.endswith(".srt"))

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
        message = (
            "GET /api/subtitles/id?path=x&token=secret-value"
            "&X-Emby-Token=emby-secret&X-MediaBrowser-Token=browser-secret&name=y HTTP/1.1"
        )

        redacted = redact_sensitive_query_values(message)

        self.assertNotIn("secret-value", redacted)
        self.assertNotIn("emby-secret", redacted)
        self.assertNotIn("browser-secret", redacted)
        self.assertIn("token=REDACTED", redacted)
        self.assertIn("X-Emby-Token=REDACTED", redacted)
        self.assertIn("X-MediaBrowser-Token=REDACTED", redacted)

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

    def test_subtitle_body_to_vtt_converts_ass_for_emby_vtt_delivery(self):
        body = (
            "[Script Info]\n"
            "[Events]\n"
            "Format: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text\n"
            "Dialogue: 0,0:00:01.20,0:00:03.45,Default,,0,0,0,,{\\an8}第一行\\N第二行\n"
        ).encode("utf-8")

        converted, content_type = subtitle_body_to_vtt(body, path="subtitle.ass")

        text = converted.decode("utf-8")
        self.assertEqual(content_type, "text/vtt; charset=utf-8")
        self.assertIn("WEBVTT", text)
        self.assertIn("00:00:01.200 --> 00:00:03.450", text)
        self.assertIn("第一行\n第二行", text)
        self.assertNotIn("{\\an8}", text)

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

    def test_resume_runtime_patch_provides_ticks_for_playback_info_patch(self):
        resume_payload = {
            "Items": [
                {
                    "Id": "media-1",
                    "RunTimeTicks": 0,
                    "UserData": {"PlaybackPositionTicks": 60_000_0000, "PlayedPercentage": 0},
                    "MediaSources": [{"Id": "media-1", "RunTimeTicks": 0}],
                }
            ]
        }

        resume_changed = patch_emby_resume_runtime_fields(resume_payload)
        runtime_ticks = resume_payload["Items"][0]["RunTimeTicks"]
        playback_payload = {"MediaSources": [{"Id": "media-1", "RunTimeTicks": 0}]}
        playback_changed = patch_emby_playback_info_runtime(playback_payload, runtime_ticks, media_id="media-1")

        self.assertTrue(resume_changed)
        self.assertGreater(runtime_ticks, 60_000_0000)
        self.assertEqual(resume_payload["Items"][0]["MediaSources"][0]["RunTimeTicks"], runtime_ticks)
        self.assertTrue(playback_changed)
        self.assertEqual(playback_payload["MediaSources"][0]["RunTimeTicks"], runtime_ticks)

    def test_patch_emby_adult_code_titles_prefixes_adult_media_name(self):
        adult = category_to_msg_library_root("adult")
        payload = {
            "Items": [
                {
                    "Id": "media-1",
                    "Name": "无码标题",
                    "LibraryId": adult["library_id"],
                    "Path": "cloud://openlist/115/成人/SSIS-218/SSIS-218.mp4",
                }
            ]
        }

        changed = patch_emby_adult_code_titles(payload)

        self.assertTrue(changed)
        self.assertEqual(payload["Items"][0]["Name"], "SSIS-218 无码标题")

    def test_patch_emby_adult_code_titles_skips_already_prefixed_name(self):
        adult = category_to_msg_library_root("adult")
        payload = {"Id": "media-1", "Name": "SSIS-218 无码标题", "LibraryId": adult["library_id"]}

        changed = patch_emby_adult_code_titles(payload)

        self.assertFalse(changed)
        self.assertEqual(payload["Name"], "SSIS-218 无码标题")

    def test_patch_emby_adult_code_titles_skips_non_adult_library(self):
        movie = category_to_msg_library_root("movie")
        payload = {
            "Items": [
                {
                    "Id": "media-1",
                    "Name": "SSIS-218 Movie",
                    "LibraryId": movie["library_id"],
                    "Path": "cloud://openlist/115/电影/SSIS-218/SSIS-218.mp4",
                }
            ]
        }

        changed = patch_emby_adult_code_titles(payload)

        self.assertFalse(changed)
        self.assertEqual(payload["Items"][0]["Name"], "SSIS-218 Movie")

    def test_patch_emby_adult_code_titles_uses_adult_path_when_library_id_missing(self):
        payload = {"Id": "media-1", "Name": "标题", "Path": "cloud://openlist/115/成人/MIDE-882/MIDE-882.mp4"}

        changed = patch_emby_adult_code_titles(payload)

        self.assertTrue(changed)
        self.assertEqual(payload["Name"], "MIDE-882 标题")

    def test_patch_emby_adult_code_titles_prefers_path_code_over_title_noise(self):
        adult = category_to_msg_library_root("adult")
        payload = {
            "Id": "media-1",
            "Name": "合集标题 SSIS-001",
            "LibraryId": adult["library_id"],
            "Path": "cloud://openlist/115/成人/MIDE-882/MIDE-882.mp4",
        }

        changed = patch_emby_adult_code_titles(payload)

        self.assertTrue(changed)
        self.assertEqual(payload["Name"], "MIDE-882 合集标题 SSIS-001")

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

    def test_inject_emby_subtitle_streams_is_idempotent_by_track_path(self):
        payload = {"MediaSources": [{"Id": "media-1", "MediaStreams": [{"Index": 0, "Type": "Video"}]}]}
        tracks = [{"lang": "sc", "label": "sc", "path": "cloud://subtitle.sc.ass"}]

        first_changed = inject_emby_subtitle_streams(payload, "media-1", tracks)
        second_changed = inject_emby_subtitle_streams(payload, "media-1", tracks)

        streams = payload["MediaSources"][0]["MediaStreams"]
        self.assertTrue(first_changed)
        self.assertFalse(second_changed)
        self.assertEqual([stream["Type"] for stream in streams], ["Video", "Subtitle"])
        self.assertEqual(streams[1]["Path"], "cloud://subtitle.sc.ass")

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

    def test_openlist_subtitle_provider_discovers_matching_external_subtitles(self):
        from pipeline.subtitle_proxy import OpenListSubtitleProvider

        class FakeOpenListClient:
            def list_all(self, path, refresh=False):
                self.path = path
                self.refresh = refresh
                return [
                    {"name": "01.sc.ass", "is_dir": False},
                    {"name": "01.tc.srt", "is_dir": False},
                    {"name": "02.sc.ass", "is_dir": False},
                    {"name": "01.jpg", "is_dir": False},
                    {"name": "Subs", "is_dir": True},
                ]

        fake_client = FakeOpenListClient()
        provider = OpenListSubtitleProvider("http://127.0.0.1:5244", "media_scan", "secret")
        provider._client = fake_client

        tracks = provider.tracks_for_media_path("cloud://openlist/115/动漫/秋色之空/01.mkv")

        self.assertEqual(fake_client.path, "/115/动漫/秋色之空")
        self.assertFalse(fake_client.refresh)
        self.assertEqual(
            tracks,
            [
                {
                    "lang": "zh-Hans",
                    "label": "简体中文",
                    "path": "cloud://openlist/115/动漫/秋色之空/01.sc.ass",
                    "source": "openlist",
                },
                {
                    "lang": "zh-Hant",
                    "label": "繁体中文",
                    "path": "cloud://openlist/115/动漫/秋色之空/01.tc.srt",
                    "source": "openlist",
                },
            ],
        )

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

    def test_parse_emby_item_media_id_accepts_bare_emby_paths(self):
        media_id = parse_emby_item_media_id(
            "/Users/user-1/Items/7303b838-dab8-4eb7-a8b6-4dc761f69c18/PlaybackInfo?api_key=secret"
        )

        self.assertEqual(media_id, "7303b838-dab8-4eb7-a8b6-4dc761f69c18")

    def test_parse_emby_user_items_search_request_reads_search_term(self):
        parsed = parse_emby_user_items_search_request(
            "/emby/Users/user-1/Items?SearchTerm=MIDE&Limit=500&StartIndex=2"
        )

        self.assertEqual(parsed["user_id"], "user-1")
        self.assertEqual(parsed["term"], "MIDE")
        self.assertEqual(parsed["mode"], "SearchTerm")
        self.assertEqual(parsed["limit"], 100)
        self.assertEqual(parsed["start_index"], 2)

    def test_parse_emby_user_items_search_request_reads_name_starts_with(self):
        parsed = parse_emby_user_items_search_request("/Users/user-1/Items?NameStartsWith=MIDE&Limit=20")

        self.assertEqual(parsed["term"], "MIDE")
        self.assertEqual(parsed["mode"], "NameStartsWith")
        self.assertEqual(parsed["limit"], 20)

    def test_parse_emby_user_items_search_request_ignores_plain_items_list(self):
        parsed = parse_emby_user_items_search_request("/emby/Users/user-1/Items?Limit=20")

        self.assertIsNone(parsed)

    def test_parse_emby_item_image_request_accepts_primary_image_path(self):
        parsed = parse_emby_item_image_request("/emby/Items/library-1/Images/Primary?tag=old&maxWidth=400")

        self.assertEqual(parsed["item_id"], "library-1")
        self.assertEqual(parsed["image_type"], "Primary")
        self.assertEqual(parsed["query"], "tag=old&maxWidth=400")

    def test_parse_emby_item_image_request_accepts_bare_primary_image_path(self):
        parsed = parse_emby_item_image_request("/Items/library-1/Images/Primary?tag=old&maxWidth=400")

        self.assertEqual(parsed["item_id"], "library-1")
        self.assertEqual(parsed["image_type"], "Primary")
        self.assertEqual(parsed["query"], "tag=old&maxWidth=400")

    def test_emby_request_user_id_from_auth_reads_jwt_query_token(self):
        payload = base64.urlsafe_b64encode(json.dumps({"uid": "user-1"}).encode("utf-8")).decode("ascii").rstrip("=")
        token = "header.%s.signature" % payload

        user_id = emby_request_user_id_from_auth("/emby/Items/library-1/Images/Primary?api_key=%s" % token, {})

        self.assertEqual(user_id, "user-1")

    def test_emby_request_user_id_from_auth_reads_bare_user_path(self):
        user_id = emby_request_user_id_from_auth("/Users/user-1/Views?IncludeExternalContent=false", {})

        self.assertEqual(user_id, "user-1")

    def test_subtitle_proxy_timing_log_redacts_tokens(self):
        import contextlib

        handler = object.__new__(SubtitleProxyHandler)
        handler.timing_log_enabled = True
        timing = handler._new_timing("GET", "/emby/Users/u/Items/media-1/PlaybackInfo?api_key=secret-token")
        handler._mark_timing(timing, "upstream")
        out = io.StringIO()

        with contextlib.redirect_stdout(out):
            handler._log_timing(timing, 200, timing["path"])

        text = out.getvalue()
        self.assertIn("subtitle proxy timing status=200", text)
        self.assertIn("api_key=REDACTED", text)
        self.assertIn("upstream=", text)
        self.assertNotIn("secret-token", text)

    def test_patch_emby_collection_folder_item_cover_uses_self_grid_image(self):
        folder = {
            "Id": "library-1",
            "Name": "电影",
            "Type": "CollectionFolder",
            "ImageTags": {},
            "PrimaryImageItemId": "library-1",
        }
        covers = select_emby_folder_cover_items(
            [
                {"Id": "media-empty", "Name": "Empty", "ImageTags": {}},
                {"Id": "media-1", "Name": "Sintel", "ImageTags": {"Primary": "media-1-tag"}, "PrimaryImageAspectRatio": 0.7},
                {"Id": "media-2", "Name": "Tears", "ImageTags": {"Primary": "media-2-tag"}, "PrimaryImageAspectRatio": 0.7},
            ]
        )

        changed = patch_emby_collection_folder_item_cover(folder, covers)

        self.assertTrue(changed)
        self.assertNotIn("PrimaryImageItemId", folder)
        self.assertNotIn("PrimaryImageTag", folder)
        self.assertRegex(folder["ImageTags"]["Primary"], r"^[0-9a-f]{32}$")
        self.assertEqual(folder["PrimaryImageAspectRatio"], 16 / 9)

    def test_iter_emby_items_accepts_virtual_folder_list(self):
        items = list(
            iter_emby_items(
                [
                    {"Id": "library-1", "CollectionType": "movies"},
                    {"Id": "library-2", "CollectionType": "tvshows"},
                ]
            )
        )

        self.assertEqual([item["Id"] for item in items], ["library-1", "library-2"])

    def test_patch_emby_collection_folder_item_cover_matches_virtual_folder_shape(self):
        folder = {
            "Id": "library-1",
            "ItemId": "library-1",
            "Name": "Movies",
            "CollectionType": "movies",
            "Locations": ["cloud://openlist/115%2FMovies"],
            "PrimaryImageItemId": "library-1",
            "PrimaryImageTag": "old-direct-tag",
        }
        covers = [{"item_id": "media-1", "image_type": "Primary", "tag": "media-1-tag"}]

        changed = patch_emby_collection_folder_item_cover(folder, covers)

        self.assertTrue(changed)
        self.assertNotIn("PrimaryImageItemId", folder)
        self.assertNotIn("PrimaryImageTag", folder)
        self.assertRegex(folder["ImageTags"]["Primary"], r"^[0-9a-f]{32}$")
        self.assertEqual(folder["PrimaryImageAspectRatio"], 16 / 9)

    def test_patch_emby_collection_folder_item_cover_clears_placeholder_without_cover(self):
        folder = {
            "Id": "library-1",
            "Name": "Empty",
            "Type": "CollectionFolder",
            "ImageTags": {},
            "PrimaryImageItemId": "library-1",
            "PrimaryImageAspectRatio": 16 / 9,
        }

        changed = patch_emby_collection_folder_item_cover(folder, [])

        self.assertTrue(changed)
        self.assertEqual(folder["ImageTags"], {})
        self.assertNotIn("PrimaryImageItemId", folder)
        self.assertNotIn("PrimaryImageTag", folder)
        self.assertNotIn("PrimaryImageAspectRatio", folder)

    def test_is_emby_placeholder_image_body_detects_one_pixel_png(self):
        from PIL import Image

        one_pixel = io.BytesIO()
        Image.new("RGBA", (1, 1), (36, 40, 48, 255)).save(one_pixel, format="PNG")
        normal = io.BytesIO()
        Image.new("RGBA", (2, 1), (36, 40, 48, 255)).save(normal, format="PNG")

        self.assertTrue(is_emby_placeholder_image_body(one_pixel.getvalue()))
        self.assertFalse(is_emby_placeholder_image_body(normal.getvalue()))
        self.assertFalse(is_emby_placeholder_image_body(b"not an image"))

    def test_serve_emby_items_search_returns_msg_media_as_emby_items(self):
        handler = object.__new__(SubtitleProxyHandler)
        handler.path = "/emby/Users/user-1/Items?SearchTerm=MIDE&Limit=2"
        handler.folder_cover_cache_lock = threading.Lock()
        handler.folder_id_cache = {}
        handler.folder_cover_cache = {}
        handler.published_folder_cover_cache = {}
        written = io.BytesIO()
        sent_headers = []
        handler.wfile = written
        handler.send_response = lambda status: sent_headers.append(("status", status))
        handler.send_header = lambda key, value: sent_headers.append((key, value))
        handler.end_headers = lambda: sent_headers.append(("end", None))
        msg_calls = []
        upstream_calls = []

        def read_msg_api(path):
            msg_calls.append(path)
            body = json.dumps({"items": [{"id": "media-1"}, {"id": "media-2"}, {"id": "media-1"}]}).encode("utf-8")
            return 200, {"Content-Type": "application/json"}, body

        def read_upstream(path, request_headers):
            upstream_calls.append(path)
            media_id = path.rsplit("/", 1)[-1]
            body = json.dumps(
                {
                    "Id": media_id,
                    "Name": "MIDE result " + media_id,
                    "Type": "Movie",
                    "MediaType": "Video",
                    "ImageTags": {"Primary": media_id},
                }
            ).encode("utf-8")
            return 200, {"Content-Type": "application/json"}, body

        handler._read_msg_api = read_msg_api
        handler._read_upstream = read_upstream

        handled = handler._serve_emby_items_search({"X-Emby-Token": "token"})

        self.assertTrue(handled)
        self.assertEqual(msg_calls, ["/media?q=MIDE&limit=2"])
        self.assertEqual(
            upstream_calls,
            [
                "/emby/Users/user-1/Items/media-1",
                "/emby/Users/user-1/Items/media-2",
            ],
        )
        self.assertIn(("status", 200), sent_headers)
        payload = json.loads(written.getvalue().decode("utf-8"))
        self.assertEqual(payload["TotalRecordCount"], 2)
        self.assertEqual([item["Id"] for item in payload["Items"]], ["media-1", "media-2"])

    def test_serve_emby_items_search_retries_case_variants(self):
        handler = object.__new__(SubtitleProxyHandler)
        handler.path = "/emby/Users/user-1/Items?SearchTerm=mide&Limit=1"
        handler.folder_cover_cache_lock = threading.Lock()
        handler.folder_id_cache = {}
        handler.folder_cover_cache = {}
        handler.published_folder_cover_cache = {}
        written = io.BytesIO()
        sent_headers = []
        handler.wfile = written
        handler.send_response = lambda status: sent_headers.append(("status", status))
        handler.send_header = lambda key, value: sent_headers.append((key, value))
        handler.end_headers = lambda: sent_headers.append(("end", None))
        msg_calls = []

        def read_msg_api(path):
            msg_calls.append(path)
            if path == "/media?q=MIDE&limit=1":
                return 200, {"Content-Type": "application/json"}, json.dumps({"items": [{"id": "media-1"}]}).encode("utf-8")
            return 200, {"Content-Type": "application/json"}, json.dumps({"items": None}).encode("utf-8")

        handler._read_msg_api = read_msg_api
        handler._read_upstream = lambda path, request_headers: (
            200,
            {"Content-Type": "application/json"},
            json.dumps({"Id": path.rsplit("/", 1)[-1], "Name": "MIDE result", "Type": "Movie"}).encode("utf-8"),
        )

        handled = handler._serve_emby_items_search({"X-Emby-Token": "token"})

        self.assertTrue(handled)
        self.assertEqual(msg_calls, ["/media?q=mide&limit=1", "/media?q=MIDE&limit=1"])
        payload = json.loads(written.getvalue().decode("utf-8"))
        self.assertEqual(payload["TotalRecordCount"], 1)
        self.assertEqual(payload["Items"][0]["Id"], "media-1")

    def test_serve_emby_items_search_fails_when_all_item_details_fail(self):
        handler = object.__new__(SubtitleProxyHandler)
        handler.path = "/emby/Users/user-1/Items?SearchTerm=MIDE&Limit=2"
        written = io.BytesIO()
        sent_headers = []
        handler.wfile = written
        handler.send_response = lambda status: sent_headers.append(("status", status))
        handler.send_header = lambda key, value: sent_headers.append((key, value))
        handler.end_headers = lambda: sent_headers.append(("end", None))
        handler._read_msg_api = lambda path: (
            200,
            {"Content-Type": "application/json"},
            json.dumps({"items": [{"id": "media-1"}]}).encode("utf-8"),
        )
        handler._read_upstream = lambda path, request_headers: (404, {"Content-Type": "application/json"}, b"{}")

        handled = handler._serve_emby_items_search({"X-Emby-Token": "token"})

        self.assertTrue(handled)
        self.assertIn(("status", 502), sent_headers)
        self.assertIn("Emby item detail lookup returned no usable items", written.getvalue().decode("utf-8"))

    def test_write_response_patches_json_list_payload(self):
        handler = object.__new__(SubtitleProxyHandler)
        written = io.BytesIO()
        sent_headers = []
        handler.wfile = written
        handler.send_response = lambda status: sent_headers.append(("status", status))
        handler.send_header = lambda key, value: sent_headers.append((key, value))
        handler.end_headers = lambda: sent_headers.append(("end", None))

        def patch_payload(payload, request_headers, request_path):
            payload[0]["ImageTags"] = {"Primary": "0123456789abcdef0123456789abcdef"}
            return True

        handler._patch_emby_collection_folder_covers = patch_payload
        body = json.dumps(
            [
                {
                    "Id": "library-1",
                    "ItemId": "library-1",
                    "CollectionType": "movies",
                    "Locations": ["cloud://openlist/115%2FMovies"],
                    "PrimaryImageItemId": "library-1",
                }
            ]
        ).encode("utf-8")

        handler._write_response(
            200,
            {"Content-Type": "application/json; charset=utf-8", "Cache-Control": "public"},
            body,
            request_headers={"X-Emby-Token": "token"},
            request_path="/Library/VirtualFolders",
        )

        patched = json.loads(written.getvalue().decode("utf-8"))
        self.assertEqual(patched[0]["ImageTags"]["Primary"], "0123456789abcdef0123456789abcdef")
        self.assertIn(("Cache-Control", "no-store"), sent_headers)

    def test_select_emby_folder_cover_items_limits_to_four_unique_items(self):
        covers = select_emby_folder_cover_items(
            [
                {"Id": "media-1", "ImageTags": {"Primary": "tag-1"}},
                {"Id": "media-2", "ImageTags": {"Primary": "tag-2"}},
                {"Id": "media-3", "ImageTags": {"Primary": "tag-3"}},
                {"Id": "media-4", "ImageTags": {"Primary": "tag-4"}},
                {"Id": "media-5", "ImageTags": {"Primary": "tag-5"}},
            ]
        )

        self.assertEqual([cover["item_id"] for cover in covers], ["media-1", "media-2", "media-3", "media-4"])

    def test_emby_folder_cover_grid_tag_is_hex_and_versioned(self):
        tag = emby_folder_cover_grid_tag(
            "library-1",
            [{"item_id": "media-1", "image_type": "Primary", "tag": "media-1-tag"}],
        )

        self.assertRegex(tag, r"^[0-9a-f]{32}$")
        self.assertNotEqual(tag, "8fe669ca35dc3e79fa26747829372c53")

    def test_build_emby_folder_cover_grid_outputs_png(self):
        from PIL import Image

        bodies = []
        for color in ("red", "green", "blue", "yellow"):
            image = Image.new("RGB", (24, 36), color)
            buffer = io.BytesIO()
            image.save(buffer, format="PNG")
            bodies.append(buffer.getvalue())

        body = build_emby_folder_cover_grid(bodies, dimensions=(400, 225))

        self.assertTrue(body.startswith(b"\x89PNG"))
        with Image.open(io.BytesIO(body)) as image:
            self.assertEqual(image.format, "PNG")
            self.assertEqual(image.size, (400, 225))

    def test_emby_folder_cover_grid_dimensions_use_client_limit(self):
        self.assertEqual(emby_folder_cover_grid_dimensions("maxWidth=400&maxHeight=300"), (400, 225))
        self.assertEqual(emby_folder_cover_grid_dimensions("maxWidth=10"), (160, 90))
        self.assertEqual(emby_folder_cover_grid_dimensions(""), (960, 540))

    def test_emby_folder_cover_response_headers_match_emby_image_shape(self):
        headers = emby_folder_cover_response_headers("0123456789abcdef0123456789abcdef", now=0)

        self.assertEqual(headers["Content-Type"], "image/png")
        self.assertEqual(headers["Cache-Control"], "public, max-age=31536000")
        self.assertEqual(headers["ETag"], '"0123456789abcdef0123456789abcdef"')
        self.assertEqual(headers["Last-Modified"], "Thu, 01 Jan 1970 00:00:00 GMT")
        self.assertIn("1971", headers["Expires"])
        self.assertEqual(headers["Accept-Ranges"], "bytes")
        self.assertEqual(headers["Cross-Origin-Resource-Policy"], "cross-origin")
        self.assertEqual(headers["Access-Control-Allow-Origin"], "*")

    def test_emby_image_proxy_path_replaces_tag_and_preserves_client_query(self):
        path = emby_image_proxy_path(
            {"item_id": "media-1", "image_type": "Primary", "tag": "new-tag"},
            "tag=old-tag&maxWidth=400&api_key=secret",
        )

        self.assertEqual(path, "/emby/Items/media-1/Images/Primary?maxWidth=400&api_key=secret&tag=new-tag")

    def test_emby_image_request_tag_reads_tag_case_insensitively(self):
        self.assertEqual(emby_image_request_tag("maxWidth=400&Tag=folder-tag"), "folder-tag")

    def test_published_folder_cover_cache_requires_matching_tag(self):
        handler = object.__new__(SubtitleProxyHandler)
        handler.published_folder_cover_cache = {}
        handler.folder_cover_cache_lock = threading.Lock()
        covers = [{"item_id": "media-1", "image_type": "Primary", "tag": "media-1-tag"}]
        tag = emby_folder_cover_grid_tag("library-1", covers)

        handler._remember_published_emby_folder_covers("library-1", "Primary", covers)

        self.assertEqual(handler._find_published_emby_folder_covers("library-1", "Primary", tag), covers)
        self.assertEqual(handler._find_published_emby_folder_covers("library-1", "Primary", "wrong-tag"), [])
        self.assertEqual(handler._find_published_emby_folder_covers("library-1", "Primary", ""), [])

    def test_serve_emby_folder_image_uses_published_cache_without_auth(self):
        from PIL import Image

        cover = io.BytesIO()
        Image.new("RGB", (24, 36), "red").save(cover, format="PNG")
        covers = [{"item_id": "media-1", "image_type": "Primary", "tag": "media-1-tag"}]
        tag = emby_folder_cover_grid_tag("library-1", covers)
        handler = object.__new__(SubtitleProxyHandler)
        handler.published_folder_cover_cache = {}
        handler.folder_image_cache = {}
        handler.folder_cover_cache_lock = threading.Lock()
        handler._remember_published_emby_folder_covers("library-1", "Primary", covers)
        handler.path = "/emby/Items/library-1/Images/Primary?tag=%s&maxWidth=400" % tag
        written = io.BytesIO()
        sent_headers = []
        handler.wfile = written
        handler.send_response = lambda status: sent_headers.append(("status", status))
        handler.send_header = lambda key, value: sent_headers.append((key, value))
        handler.end_headers = lambda: sent_headers.append(("end", None))
        handler._read_upstream = lambda path, headers: (200, {"Content-Type": "image/png"}, cover.getvalue())

        handled = handler._serve_emby_folder_image({})

        self.assertTrue(handled)
        self.assertIn(("status", 200), sent_headers)
        self.assertIn(("Content-Type", "image/png"), sent_headers)
        self.assertTrue(written.getvalue().startswith(b"\x89PNG"))


class CategoryConfigTest(unittest.TestCase):
    def test_msgdb_groups_episode_rows_into_one_migration_candidate(self):
        from pipeline.msgdb import build_migration_candidates, build_migration_target, cloud_path_to_openlist_path

        rows = [
            {
                "id": "m1",
                "library_id": "test-tv-library",
                "library_root_id": "test-tv-root",
                "title": "成龙历险记",
                "path": "cloud://openlist/115/剧集/成龙历险记/成龙历险记 第01集.mp4",
                "root_path": "cloud://openlist/115%2F%E5%89%A7%E9%9B%86",
                "size_bytes": 100,
                "library_name": "剧集",
                "library_type": "tv",
            },
            {
                "id": "m2",
                "library_id": "test-tv-library",
                "library_root_id": "test-tv-root",
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
        from pipeline.config import category_maps, load_category_config

        folder_ids, _openlist_paths, _msg_roots = category_maps(load_category_config({}))

        self.assertEqual(folder_ids["movie"], "REPLACE_WITH_115_MOVIE_CID")
        self.assertEqual(folder_ids["tv"], "REPLACE_WITH_115_TV_CID")
        self.assertEqual(folder_ids["anime"], "REPLACE_WITH_115_ANIME_CID")
        self.assertEqual(folder_ids["adult"], "REPLACE_WITH_115_ADULT_CID")
        self.assertEqual(folder_ids["other"], "REPLACE_WITH_115_OTHER_CID")

    def test_routes_movie_tv_anime_adult_and_other_to_openlist_paths(self):
        from pipeline.config import category_maps, load_category_config

        _folder_ids, openlist_paths, _msg_roots = category_maps(load_category_config({}))

        self.assertEqual(openlist_paths["movie"], "/115/电影")
        self.assertEqual(openlist_paths["tv"], "/115/剧集")
        self.assertEqual(openlist_paths["anime"], "/115/动漫")
        self.assertEqual(openlist_paths["adult"], "/115/成人")
        self.assertEqual(openlist_paths["other"], "/115/其他")

    def test_routes_movie_tv_anime_adult_and_other_to_mediastation_roots(self):
        from pipeline.config import category_maps, load_category_config

        _folder_ids, _openlist_paths, msg_roots = category_maps(load_category_config({}))
        movie = msg_roots["movie"]
        tv = msg_roots["tv"]
        anime = msg_roots["anime"]
        adult = msg_roots["adult"]
        other = msg_roots["other"]

        self.assertEqual(movie["library_id"], "REPLACE_WITH_MSG_MOVIE_LIBRARY_ID")
        self.assertEqual(movie["root_id"], "REPLACE_WITH_MSG_MOVIE_ROOT_ID")
        self.assertEqual(tv["library_id"], "REPLACE_WITH_MSG_TV_LIBRARY_ID")
        self.assertEqual(tv["root_id"], "REPLACE_WITH_MSG_TV_ROOT_ID")
        self.assertEqual(tv["media_type"], "tv")
        self.assertEqual(anime["library_id"], "REPLACE_WITH_MSG_ANIME_LIBRARY_ID")
        self.assertEqual(anime["root_id"], "REPLACE_WITH_MSG_ANIME_ROOT_ID")
        self.assertEqual(anime["provider"], "tmdb")
        self.assertEqual(anime["media_type"], "anime")
        self.assertEqual(adult["library_id"], "REPLACE_WITH_MSG_ADULT_LIBRARY_ID")
        self.assertEqual(adult["root_id"], "REPLACE_WITH_MSG_ADULT_ROOT_ID")
        self.assertEqual(other["library_id"], "REPLACE_WITH_MSG_OTHER_LIBRARY_ID")
        self.assertEqual(other["root_id"], "REPLACE_WITH_MSG_OTHER_ROOT_ID")
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
        self.assertEqual(folder_ids["adult"], "REPLACE_WITH_115_ADULT_CID")

    def test_default_category_config_does_not_embed_instance_ids(self):
        from pipeline.config import DEFAULT_CATEGORY_CONFIG

        raw = json.dumps(DEFAULT_CATEGORY_CONFIG, ensure_ascii=False)
        self.assertNotRegex(raw, r"\b\d{16,22}\b")
        self.assertNotRegex(raw, r"\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b")

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

    def test_discovers_missing_msg_root_ids_from_database(self):
        from pipeline.config import MSG_LIBRARY_ROOTS, category_to_msg_library_root

        class FakeConn:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def execute(self, _sql):
                return self

            def fetchall(self):
                return [
                    {
                        "library_id": "movie-library-db",
                        "root_id": "movie-root-db",
                        "library_name": "电影",
                        "media_type": "movie",
                        "root_path": "cloud://openlist/115%2F%E7%94%B5%E5%BD%B1",
                    },
                    {
                        "library_id": "adult-library-db",
                        "root_id": "adult-root-db",
                        "library_name": "成人",
                        "media_type": "adult",
                        "root_path": "cloud://openlist/115%2F%E6%88%90%E4%BA%BA",
                    },
                ]

        original_roots = copy.deepcopy(MSG_LIBRARY_ROOTS)
        try:
            MSG_LIBRARY_ROOTS["movie"]["library_id"] = "REPLACE_WITH_MSG_MOVIE_LIBRARY_ID"
            MSG_LIBRARY_ROOTS["movie"]["root_id"] = "REPLACE_WITH_MSG_MOVIE_ROOT_ID"
            root = category_to_msg_library_root("movie", connect=lambda _dsn: FakeConn(), env={})
        finally:
            MSG_LIBRARY_ROOTS.clear()
            MSG_LIBRARY_ROOTS.update(original_roots)

        self.assertEqual(root["library_id"], "movie-library-db")
        self.assertEqual(root["root_id"], "movie-root-db")
        self.assertEqual(root["provider"], "tmdb")
        self.assertEqual(root["media_type"], "movie")

    def test_discovering_missing_msg_root_ids_rejects_ambiguous_matches(self):
        from pipeline.config import MSG_LIBRARY_ROOTS, category_to_msg_library_root

        class FakeConn:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def execute(self, _sql):
                return self

            def fetchall(self):
                return [
                    {
                        "library_id": "movie-library-1",
                        "root_id": "movie-root-1",
                        "library_name": "电影",
                        "media_type": "movie",
                        "root_path": "cloud://openlist/115%2F%E7%94%B5%E5%BD%B1",
                    },
                    {
                        "library_id": "movie-library-2",
                        "root_id": "movie-root-2",
                        "library_name": "电影",
                        "media_type": "movie",
                        "root_path": "cloud://openlist/115%2F%E7%94%B5%E5%BD%B1",
                    },
                ]

        original_roots = copy.deepcopy(MSG_LIBRARY_ROOTS)
        try:
            MSG_LIBRARY_ROOTS["movie"]["library_id"] = "REPLACE_WITH_MSG_MOVIE_LIBRARY_ID"
            MSG_LIBRARY_ROOTS["movie"]["root_id"] = "REPLACE_WITH_MSG_MOVIE_ROOT_ID"
            with self.assertRaisesRegex(RuntimeError, "multiple MediaStationGo roots matched"):
                category_to_msg_library_root("movie", connect=lambda _dsn: FakeConn(), env={})
        finally:
            MSG_LIBRARY_ROOTS.clear()
            MSG_LIBRARY_ROOTS.update(original_roots)

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
                            "root_folder_id": "test-root-folder-id",
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

        client.update_media_metadata(
            "media-1",
            {
                "poster_url": "https://img/poster.jpg",
                "title": "SSIS-218 Title",
                "nsfw": True,
                "unexpected": "ignored",
            },
        )

        call = transport.calls[1]
        self.assertEqual(call["method"], "PATCH")
        self.assertEqual(call["url"], "http://127.0.0.1:18080/api/media/media-1/metadata")
        self.assertEqual(call["data"], {"poster_url": "https://img/poster.jpg", "title": "SSIS-218 Title", "nsfw": True})

    def test_adult_metadata_base_urls_normalizes_and_deduplicates(self):
        self.assertEqual(
            normalize_adult_base_urls("javdb.com, https://javdb.com ; javbus.sbs"),
            ["https://javdb.com", "https://javbus.sbs"],
        )

    def test_adult_metadata_parse_javdb_detail(self):
        html = """
        <html>
          <h2>SSIS-218 测试标题</h2>
          <img class="video-cover" src="/covers/ssis218.jpg">
          <a class="sample-box" href="https://pics.dmm.co.jp/digital/video/ssis00218/ssis00218jp-1.jpg">sample</a>
          發行日期: 2024-01-02 score 8.1
        </html>
        """

        match = parse_adult_detail_html(html, "SSIS-218", "javdb", "https://javdb.com/v/abc")

        self.assertEqual(match.title, "测试标题")
        self.assertEqual(match.poster_url, "https://pics.dmm.co.jp/digital/video/ssis00218/ssis00218pl.jpg")
        self.assertEqual(match.backdrop_url, "https://pics.dmm.co.jp/digital/video/ssis00218/ssis00218jp-1.jpg")
        self.assertEqual(match.release_date, "2024-01-02")
        self.assertEqual(match.year, 2024)
        self.assertEqual(match.rating, 8.1)

    def test_cloudflare_script_on_normal_adult_page_is_not_challenge(self):
        normal_html = """
        <html>
          <head>
            <title>JavDB, Online information source for adult movies</title>
            <script src="/cdn-cgi/challenge-platform/scripts/jsd/main.js"></script>
          </head>
          <body>GANA-2525</body>
        </html>
        """
        challenge_html = """
        <html>
          <head><title>Just a moment...</title></head>
          <body>Cloudflare checking if the site connection is secure</body>
        </html>
        """

        self.assertFalse(looks_like_cloudflare_challenge(normal_html))
        self.assertTrue(looks_like_cloudflare_challenge(challenge_html))

    def test_adult_metadata_extracts_legacy_fc2_code_from_path(self):
        self.assertEqual(
            adult_metadata_codes_from_media({"path": "cloud://openlist/115/成人/[7sht.me]FC2-926114-C/FC2-926114-C.mp4"}),
            ["FC2-PPV-926114"],
        )

    def test_adult_metadata_provider_searches_onejav_direct_fc2_detail(self):
        class FakeAdultProvider(AdultHTMLMetadataProvider):
            def __init__(self):
                super().__init__(bases=["https://onejav.test"], timeout=1)
                self.urls = []

            def _fetch_text(self, url, referer="", allow_flaresolverr=False):
                self.urls.append(url)
                if url.endswith("/torrent/fc2ppv4661145"):
                    return """
                    <html>
                      <title>FC2PPV4661145 - OneJAV.com - Free JAV Torrents</title>
                      <img class="image" src="https://img.example/fc2ppv4661145.jpg">
                    </html>
                    """
                return ""

        provider = FakeAdultProvider()
        match = provider.search({"title": "FC2-PPV-4661145"}, ["FC2-PPV-4661145"])

        self.assertEqual(match.source, "onejav")
        self.assertEqual(match.code, "FC2-PPV-4661145")
        self.assertEqual(match.title, "FC2-PPV-4661145")
        self.assertEqual(match.poster_url, "https://img.example/fc2ppv4661145.jpg")
        self.assertEqual(provider.urls, ["https://onejav.test/torrent/fc2ppv4661145"])

    def test_adult_metadata_provider_onejav_search_requires_exact_fc2_match(self):
        class FakeAdultProvider(AdultHTMLMetadataProvider):
            def __init__(self):
                super().__init__(bases=["https://onejav.test"], timeout=1)

            def _fetch_text(self, url, referer="", allow_flaresolverr=False):
                if url.endswith("/torrent/fc2ppv926114"):
                    return ""
                return """
                <html>
                  <a href="/torrent/fc2ppv4661145">FC2PPV4661145</a>
                  <a href="/torrent/fc2ppv2511471">FC2PPV2511471</a>
                </html>
                """

        provider = FakeAdultProvider()

        self.assertIsNone(provider.search({"title": "FC2-PPV-926114"}, ["FC2-PPV-926114"]))

    def test_adult_metadata_provider_searches_configured_javdb_source(self):
        class FakeAdultProvider(AdultHTMLMetadataProvider):
            def __init__(self):
                super().__init__(bases=["https://javdb.test"], timeout=1)
                self.urls = []

            def _fetch_text(self, url, referer="", allow_flaresolverr=False):
                self.urls.append(url)
                if "/search?" in url:
                    return '<a class="box" href="/v/abc"><strong>SSIS-218</strong></a>'
                return '<h2>SSIS-218 站点标题</h2><img class="video-cover" src="/cover.jpg">'

        provider = FakeAdultProvider()
        match = provider.search({"title": "SSIS-218"}, ["SSIS-218"])

        self.assertEqual(match.title, "站点标题")
        self.assertEqual(match.source, "javdb")
        self.assertEqual(match.poster_url, "https://javdb.test/cover.jpg")
        self.assertEqual(provider.urls[0], "https://javdb.test/search?q=SSIS-218&f=all")

    def test_adult_metadata_provider_does_not_call_flaresolverr_when_direct_fetch_succeeds(self):
        class FakeResponse:
            def __init__(self, body, status=200):
                self.body = body.encode("utf-8")
                self.status = status
                self.headers = {}

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def read(self, size=-1):
                if size is None or size < 0:
                    size = len(self.body)
                chunk = self.body[:size]
                self.body = self.body[size:]
                return chunk

        calls = []

        def fake_urlopen(request, timeout=None):
            url = request.full_url
            calls.append(url)
            if url == "http://flaresolverr.test/v1":
                raise AssertionError("FlareSolverr should not be called")
            if "/search?" in url:
                return FakeResponse('<a class="box" href="/v/abc"><strong>SSIS-218</strong></a>')
            return FakeResponse('<h2>SSIS-218 direct title</h2><img class="video-cover" src="/cover.jpg">')

        provider = AdultHTMLMetadataProvider(
            bases=["https://javdb.test"],
            timeout=1,
            flaresolverr_url="http://flaresolverr.test",
            flaresolverr_timeout=2,
        )

        with patch("pipeline.adult_metadata.urllib.request.urlopen", side_effect=fake_urlopen):
            match = provider.search({"title": "SSIS-218"}, ["SSIS-218"])

        self.assertEqual(match.title, "direct title")
        self.assertEqual(calls, ["https://javdb.test/search?q=SSIS-218&f=all", "https://javdb.test/v/abc"])

    def test_adult_metadata_provider_uses_flaresolverr_after_javdb_403(self):
        class FakeResponse:
            def __init__(self, payload, status=200):
                self.body = json.dumps(payload).encode("utf-8")
                self.status = status
                self.headers = {}

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def read(self, size=-1):
                if size is None or size < 0:
                    size = len(self.body)
                chunk = self.body[:size]
                self.body = self.body[size:]
                return chunk

        flaresolverr_payloads = []

        def fake_urlopen(request, timeout=None):
            url = request.full_url
            if url == "http://flaresolverr.test/v1":
                payload = json.loads(request.data.decode("utf-8"))
                flaresolverr_payloads.append(payload)
                if "/search?" in payload["url"]:
                    body = '<a class="box" href="/v/abc"><strong>SSIS-218</strong></a>'
                else:
                    body = '<h2>SSIS-218 fallback title</h2><img class="video-cover" src="/cover.jpg">'
                return FakeResponse({"status": "ok", "solution": {"status": 200, "response": body}})
            raise urllib.error.HTTPError(url, 403, "Forbidden", {}, None)

        provider = AdultHTMLMetadataProvider(
            bases=["https://javdb.test"],
            timeout=1,
            flaresolverr_url="http://flaresolverr.test",
            flaresolverr_timeout=2,
        )

        with patch("pipeline.adult_metadata.urllib.request.urlopen", side_effect=fake_urlopen):
            match = provider.search({"title": "SSIS-218"}, ["SSIS-218"])

        self.assertEqual(match.title, "fallback title")
        self.assertEqual(match.poster_url, "https://javdb.test/cover.jpg")
        self.assertEqual([payload["cmd"] for payload in flaresolverr_payloads], ["request.get", "request.get"])
        self.assertEqual([payload["maxTimeout"] for payload in flaresolverr_payloads], [2000, 2000])
        self.assertEqual(flaresolverr_payloads[0]["url"], "https://javdb.test/search?q=SSIS-218&f=all")
        self.assertEqual(flaresolverr_payloads[1]["url"], "https://javdb.test/v/abc")

    def test_adult_metadata_flaresolverr_error_is_explicit(self):
        class FakeResponse:
            def __init__(self, payload):
                self.body = json.dumps(payload).encode("utf-8")
                self.status = 200
                self.headers = {}

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def read(self, size=-1):
                if size is None or size < 0:
                    size = len(self.body)
                chunk = self.body[:size]
                self.body = self.body[size:]
                return chunk

        def fake_urlopen(request, timeout=None):
            url = request.full_url
            if url == "http://flaresolverr.test/v1":
                return FakeResponse({"status": "error", "message": "blocked"})
            raise urllib.error.HTTPError(url, 403, "Forbidden", {}, None)

        provider = AdultHTMLMetadataProvider(
            bases=["https://javdb.test"],
            timeout=1,
            flaresolverr_url="http://flaresolverr.test",
            flaresolverr_timeout=2,
        )

        with patch("pipeline.adult_metadata.urllib.request.urlopen", side_effect=fake_urlopen):
            with self.assertRaisesRegex(RuntimeError, "FlareSolverr returned status blocked"):
                provider._fetch_text(
                    "https://javdb.test/search?q=SSIS-218&f=all",
                    referer="https://javdb.test",
                    allow_flaresolverr=True,
                )

    def test_adult_artwork_semantic_repair_swaps_portrait_and_landscape(self):
        class FakeFetcher:
            def fetch(self, candidate):
                dimensions_by_url = {
                    "https://img/current-poster.jpg": (800, 540),
                    "https://img/current-backdrop.jpg": (600, 900),
                }
                if candidate.url not in dimensions_by_url:
                    raise RuntimeError("not found")
                dimensions = dimensions_by_url[candidate.url]
                return AdultImageProbe(
                    candidate.url,
                    candidate.source,
                    candidate.role,
                    candidate.priority,
                    dimensions[0],
                    dimensions[1],
                    "image/jpeg",
                    b"image",
                )

        class FakeMetadataProvider:
            def search(self, media, codes):
                return parse_adult_detail_html(
                    '<h2>SSIS-218 新标题</h2><a class="sample-box" href="https://img/current-poster.jpg">sample</a>',
                    "SSIS-218",
                    "javdb",
                    "https://javdb.test/v/abc",
                )

        result = build_adult_artwork_repair(
            {
                "title": "SSIS-218",
                "poster_url": "https://img/current-poster.jpg",
                "backdrop_url": "https://img/current-backdrop.jpg",
            },
            fetcher=FakeFetcher(),
            metadata_provider=FakeMetadataProvider(),
        )

        self.assertEqual(result["status"], "success")
        self.assertEqual(result["patch"]["title"], "新标题")
        self.assertEqual(result["patch"]["original_name"], "SSIS-218")
        self.assertEqual(result["patch"]["poster_url"], "https://img/current-backdrop.jpg")
        self.assertEqual(result["patch"]["backdrop_url"], "https://img/current-poster.jpg")

    def test_adult_artwork_semantic_repair_generates_portrait_from_landscape(self):
        from PIL import Image

        source = io.BytesIO()
        image = Image.new("RGB", (800, 540), (200, 20, 20))
        for x in range(400, 800):
            for y in range(540):
                image.putpixel((x, y), (20, 40, 210))
        image.save(source, format="PNG")

        class FakeFetcher:
            def fetch(self, candidate):
                return AdultImageProbe(
                    candidate.url,
                    candidate.source,
                    candidate.role,
                    candidate.priority,
                    800,
                    540,
                    "image/jpeg",
                    source.getvalue(),
                )

        with tempfile.TemporaryDirectory() as tmp:
            result = build_adult_artwork_repair(
                {
                    "title": "SSIS-218",
                    "poster_url": "https://img/current-poster.jpg",
                    "backdrop_url": "",
                },
                cache_dir=tmp,
                public_base_url="https://privdo.example",
                fetcher=FakeFetcher(),
                metadata_provider=False,
            )

            self.assertEqual(result["status"], "success")
            self.assertEqual(result["reason"], "portrait_generated")
            self.assertIn("https://privdo.example/pipeline-artwork/adult/", result["patch"]["poster_url"])
            self.assertIn("https://privdo.example/pipeline-artwork/adult/", result["patch"]["backdrop_url"])
            self.assertNotEqual(result["patch"]["backdrop_url"], result["patch"]["poster_url"])
            self.assertTrue(os.path.exists(result["generated"]["path"]))
            backdrop_file = os.path.join(tmp, result["patch"]["backdrop_url"].rsplit("/", 1)[-1])
            self.assertTrue(os.path.exists(backdrop_file))
            self.assertEqual(image_orientation(result["generated"]["width"], result["generated"]["height"]), "portrait")
            with Image.open(result["generated"]["path"]) as generated:
                red, green, blue = generated.resize((1, 1)).getpixel((0, 0))
            self.assertLess(red, 80)
            self.assertGreater(blue, 150)

    def test_adult_artwork_semantic_repair_requires_public_base_url_to_generate(self):
        class FakeFetcher:
            def fetch(self, candidate):
                return AdultImageProbe(
                    candidate.url,
                    candidate.source,
                    candidate.role,
                    candidate.priority,
                    800,
                    540,
                    "image/jpeg",
                    b"image",
                )

        result = build_adult_artwork_repair(
            {
                "title": "SSIS-218",
                "poster_url": "https://img/current-poster.jpg",
                "backdrop_url": "https://img/current-poster.jpg",
            },
            public_base_url="",
            fetcher=FakeFetcher(),
            metadata_provider=False,
        )

        self.assertEqual(result["status"], "skipped")
        self.assertEqual(result["reason"], "public_base_url_missing")

    def test_adult_artwork_cache_serves_safe_generated_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "cover.jpg")
            with open(path, "wb") as handle:
                handle.write(b"jpeg-body")

            handler = object.__new__(SubtitleProxyHandler)
            handler.path = "/pipeline-artwork/adult/cover.jpg"
            handler.adult_artwork_cache_dir = tmp
            written = io.BytesIO()
            sent_headers = []
            handler.wfile = written
            handler.send_response = lambda status: sent_headers.append(("status", status))
            handler.send_header = lambda key, value: sent_headers.append((key, value))
            handler.end_headers = lambda: sent_headers.append(("end", None))
            handler._mark_timing = lambda timing, name: None
            handler._log_timing = lambda timing, status, request_path: None

            self.assertTrue(handler._serve_adult_artwork_cache())
            self.assertEqual(written.getvalue(), b"jpeg-body")
            self.assertIn(("status", 200), sent_headers)
            self.assertIn(("Content-Type", "image/jpeg"), sent_headers)

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

        result = client.add_offline_urls(["magnet:?xt=urn:btih:abc"], "REPLACE_WITH_115_MOVIE_CID")

        self.assertEqual(result["state"], True)
        self.assertEqual(len(transport.calls), 1)
        call = transport.calls[0]
        self.assertEqual(call["method"], "POST")
        self.assertEqual(call["url"], "https://proapi.115.com/open/offline/add_task_urls")
        self.assertEqual(call["headers"]["Authorization"], "Bearer access-token-value")
        self.assertEqual(call["data"], {"urls": "magnet:?xt=urn:btih:abc", "wp_path_id": "REPLACE_WITH_115_MOVIE_CID"})

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
        transport = FakeTransport({"state": True, "data": {"file_id": "REPLACE_WITH_115_MOVIE_CID", "file_name": "影视库-电影"}})
        client = Client115("access-token-value", transport=transport)

        result = client.get_folder_info("REPLACE_WITH_115_MOVIE_CID")

        self.assertEqual(result["state"], True)
        call = transport.calls[0]
        self.assertEqual(call["method"], "GET")
        self.assertEqual(
            call["url"],
            "https://proapi.115.com/open/folder/get_info?file_id=REPLACE_WITH_115_MOVIE_CID",
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

    def test_profile_search_records_partial_failure_without_dropping_successes(self):
        from pipeline.bot import SEARCH_PROFILE_GENERAL, search_profile_indexer_results
        from pipeline.search_stats import SearchStats

        fake_prowlarr = FakeProwlarr(
            [],
            indexers=[
                {"id": 1, "name": "FastIndexer", "enable": True, "capabilities": {"categories": [{"id": 2000}]}},
                {"id": 2, "name": "BrokenIndexer", "enable": True, "capabilities": {"categories": [{"id": 2000}]}},
            ],
            indexer_results={(1,): [{"title": "Sintel 1080p", "indexer": "FastIndexer", "seeders": 2, "infoHash": "F1"}]},
            indexer_errors={(2,): RuntimeError("upstream failed")},
        )
        stats = SearchStats()

        results = search_profile_indexer_results(
            fake_prowlarr,
            "sintel",
            SEARCH_PROFILE_GENERAL,
            100,
            indexers=fake_prowlarr.indexers(),
            stats=stats,
            max_workers=2,
        )
        metadata = stats.to_metadata(raw_count=len(results), selected_count=len(results))

        self.assertEqual([item["infoHash"] for item in results], ["F1"])
        self.assertEqual(metadata["success_count"], 1)
        self.assertEqual(metadata["failed_count"], 1)
        self.assertEqual({source["source"] for source in metadata["sources"]}, {"FastIndexer", "BrokenIndexer"})

    def test_profile_search_records_timeout_without_waiting_for_slow_source(self):
        import time

        from pipeline.bot import SEARCH_PROFILE_GENERAL, search_profile_indexer_results
        from pipeline.search_stats import SearchStats

        class SlowProwlarr(FakeProwlarr):
            def search(self, query, limit=20, indexer_ids=None, categories=None):
                if tuple(indexer_ids or []) == (2,):
                    time.sleep(0.05)
                return super().search(query, limit=limit, indexer_ids=indexer_ids, categories=categories)

        fake_prowlarr = SlowProwlarr(
            [],
            indexers=[
                {"id": 1, "name": "FastIndexer", "enable": True, "capabilities": {"categories": [{"id": 2000}]}},
                {"id": 2, "name": "SlowIndexer", "enable": True, "capabilities": {"categories": [{"id": 2000}]}},
            ],
            indexer_results={
                (1,): [{"title": "Sintel 1080p", "indexer": "FastIndexer", "seeders": 2, "infoHash": "F1"}],
                (2,): [{"title": "Sintel 720p", "indexer": "SlowIndexer", "seeders": 2, "infoHash": "S1"}],
            },
        )
        stats = SearchStats()

        results = search_profile_indexer_results(
            fake_prowlarr,
            "sintel",
            SEARCH_PROFILE_GENERAL,
            100,
            indexers=fake_prowlarr.indexers(),
            timeout_seconds=0.01,
            stats=stats,
            max_workers=2,
        )
        metadata = stats.to_metadata(raw_count=len(results), selected_count=len(results))

        self.assertEqual([item["infoHash"] for item in results], ["F1"])
        self.assertEqual(metadata["success_count"], 1)
        self.assertEqual(metadata["timeout_count"], 1)
        self.assertEqual({source["source"] for source in metadata["sources"]}, {"FastIndexer", "SlowIndexer"})

    def test_profile_search_early_returns_after_prioritized_sources_finish(self):
        import time

        from pipeline.bot import SEARCH_PROFILE_GENERAL, search_profile_indexer_results
        from pipeline.search_stats import SearchStats

        class DelayedProwlarr(FakeProwlarr):
            def search(self, query, limit=20, indexer_ids=None, categories=None):
                if tuple(indexer_ids or []) == (3,):
                    time.sleep(0.2)
                return super().search(query, limit=limit, indexer_ids=indexer_ids, categories=categories)

        fake_prowlarr = DelayedProwlarr(
            [],
            indexers=[
                {"id": 1, "name": "YTS", "enable": True, "priority": 5, "capabilities": {"categories": [{"id": 2000}]}},
                {"id": 2, "name": "BT4G", "enable": True, "priority": 10, "capabilities": {"categories": [{"id": 2000}]}},
                {"id": 3, "name": "SlowLowPriority", "enable": True, "priority": 25, "capabilities": {"categories": [{"id": 2000}]}},
            ],
            indexer_results={
                (1,): [{"title": "Sintel YTS", "indexer": "YTS", "seeders": 2, "infoHash": "Y1"}],
                (2,): [{"title": "Sintel BT4G", "indexer": "BT4G", "seeders": 2, "infoHash": "B1"}],
                (3,): [{"title": "Sintel Slow", "indexer": "SlowLowPriority", "seeders": 2, "infoHash": "S1"}],
            },
        )
        stats = SearchStats()

        started = time.monotonic()
        results = search_profile_indexer_results(
            fake_prowlarr,
            "sintel",
            SEARCH_PROFILE_GENERAL,
            100,
            indexers=fake_prowlarr.indexers(),
            timeout_seconds=0.5,
            stats=stats,
            max_workers=3,
            early_return_after_seconds=0.01,
            early_return_min_results=2,
            early_return_required_priority=10,
        )
        elapsed = time.monotonic() - started
        metadata = stats.to_metadata(raw_count=len(results), selected_count=len(results))

        self.assertLess(elapsed, 0.15)
        self.assertEqual({item["infoHash"] for item in results}, {"Y1", "B1"})
        self.assertEqual(metadata["success_count"], 2)
        self.assertEqual(metadata["skipped_count"], 1)
        self.assertEqual(next(source for source in metadata["sources"] if source["source"] == "SlowLowPriority")["status"], "skipped")

    def test_profile_search_does_not_early_return_before_required_priority_source_finishes(self):
        import time

        from pipeline.bot import SEARCH_PROFILE_GENERAL, search_profile_indexer_results
        from pipeline.search_stats import SearchStats

        class DelayedProwlarr(FakeProwlarr):
            def search(self, query, limit=20, indexer_ids=None, categories=None):
                key = tuple(indexer_ids or [])
                if key == (1,):
                    time.sleep(0.05)
                if key == (3,):
                    time.sleep(0.2)
                return super().search(query, limit=limit, indexer_ids=indexer_ids, categories=categories)

        fake_prowlarr = DelayedProwlarr(
            [],
            indexers=[
                {"id": 1, "name": "RequiredSlow", "enable": True, "priority": 5, "capabilities": {"categories": [{"id": 2000}]}},
                {"id": 2, "name": "FastLowPriority", "enable": True, "priority": 25, "capabilities": {"categories": [{"id": 2000}]}},
                {"id": 3, "name": "SlowLowPriority", "enable": True, "priority": 25, "capabilities": {"categories": [{"id": 2000}]}},
            ],
            indexer_results={
                (1,): [{"title": "Sintel Required", "indexer": "RequiredSlow", "seeders": 2, "infoHash": "R1"}],
                (2,): [
                    {"title": "Sintel Fast 1", "indexer": "FastLowPriority", "seeders": 2, "infoHash": "F1"},
                    {"title": "Sintel Fast 2", "indexer": "FastLowPriority", "seeders": 2, "infoHash": "F2"},
                ],
                (3,): [{"title": "Sintel Slow", "indexer": "SlowLowPriority", "seeders": 2, "infoHash": "S1"}],
            },
        )
        stats = SearchStats()

        started = time.monotonic()
        results = search_profile_indexer_results(
            fake_prowlarr,
            "sintel",
            SEARCH_PROFILE_GENERAL,
            100,
            indexers=fake_prowlarr.indexers(),
            timeout_seconds=0.5,
            stats=stats,
            max_workers=3,
            early_return_after_seconds=0.01,
            early_return_min_results=2,
            early_return_required_priority=10,
        )
        elapsed = time.monotonic() - started
        metadata = stats.to_metadata(raw_count=len(results), selected_count=len(results))

        self.assertGreaterEqual(elapsed, 0.04)
        self.assertEqual({item["infoHash"] for item in results}, {"R1", "F1", "F2"})
        self.assertEqual(metadata["success_count"], 2)
        self.assertEqual(metadata["skipped_count"], 1)
        self.assertEqual(next(source for source in metadata["sources"] if source["source"] == "SlowLowPriority")["status"], "skipped")

    def test_bot_search_uses_profile_specific_tuning_settings(self):
        from pipeline.bot import BotConfig, PipelineBotService, SEARCH_PROFILE_ADULT
        from pipeline.search_stats import search_result_metadata

        fake_prowlarr = FakeProwlarr(
            [],
            indexers=[
                {"id": 8, "name": "sukebei.nyaa.si", "enable": True, "capabilities": {"categories": [{"id": 6000}]}},
            ],
            indexer_results={
                (8,): [{"title": "MIDE-882 1080p", "indexer": "sukebei.nyaa.si", "seeders": 0, "infoHash": "S1"}],
            },
        )

        with patch("pipeline.bot.ProwlarrConfig") as config_cls, patch("pipeline.bot.ProwlarrClient", return_value=fake_prowlarr):
            config_cls.return_value.load_api_key.return_value = "prowlarr-key"
            service = PipelineBotService(
                BotConfig(
                    "token",
                    {700656624},
                    "/tmp/state.db",
                    search_profile_upstream_limits={SEARCH_PROFILE_ADULT: 42},
                    search_profile_timeout_seconds={SEARCH_PROFILE_ADULT: 2},
                    search_profile_max_workers={SEARCH_PROFILE_ADULT: 1},
                )
            )
            results = service.search_adult("MIDE-882", limit=5)

        metadata = search_result_metadata(results)
        self.assertEqual([item["infoHash"] for item in results], ["S1"])
        self.assertEqual(fake_prowlarr.search_calls, [("MIDE-882", 42, (8,), (6000,))])
        self.assertEqual(metadata["settings"]["upstream_limit"], 42)
        self.assertEqual(metadata["settings"]["timeout_seconds"], 2)
        self.assertEqual(metadata["settings"]["max_workers"], 1)

    def test_bot_search_bt4g_uses_only_bt4g_indexer(self):
        from pipeline.bot import BotConfig, PipelineBotService, SEARCH_PROFILE_GENERAL
        from pipeline.search_stats import search_result_metadata

        fake_prowlarr = FakeProwlarr(
            [],
            indexers=[
                {"id": 2, "name": "BT4G", "enable": True, "priority": 25, "capabilities": {"categories": [{"id": 2000}]}},
                {"id": 3, "name": "Knaben", "enable": True, "priority": 10, "capabilities": {"categories": [{"id": 2000}]}},
            ],
            indexer_results={
                (2,): [{"title": "Sintel BT4G 1080p", "indexer": "BT4G", "seeders": 5, "infoHash": "B1"}],
                (3,): [{"title": "Sintel Knaben 1080p", "indexer": "Knaben", "seeders": 5, "infoHash": "K1"}],
            },
        )

        with patch("pipeline.bot.ProwlarrConfig") as config_cls, patch("pipeline.bot.ProwlarrClient", return_value=fake_prowlarr):
            config_cls.return_value.load_api_key.return_value = "prowlarr-key"
            service = PipelineBotService(
                BotConfig(
                    "token",
                    {700656624},
                    "/tmp/state.db",
                    search_profile_upstream_limits={SEARCH_PROFILE_GENERAL: 40},
                )
            )
            results = service.search_bt4g("sintel", limit=5)

        metadata = search_result_metadata(results)
        self.assertEqual([item["infoHash"] for item in results], ["B1"])
        self.assertEqual(fake_prowlarr.search_calls, [("sintel", 40, (2,), (2000, 5000))])
        self.assertEqual(metadata["profile"], "bt4g")
        self.assertEqual(metadata["settings"]["indexers"], ["BT4G"])

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
        self.assertEqual(fake_115.folder_id, "REPLACE_WITH_115_MOVIE_CID")
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
