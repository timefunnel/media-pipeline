import copy
import json

from tests.test_pipeline_core import *


class ExternalSubtitleTest(unittest.TestCase):
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
        self.assertEqual(filename, "assrt-zh-Hans.srt")

    def test_subtitle_cache_uses_readable_suffix_for_same_source_and_language(self):
        from pipeline.external_subtitles import SubtitleCache, SubtitleDownload

        with tempfile.TemporaryDirectory() as tmp:
            cache = SubtitleCache(tmp)
            first = cache.save_download(
                "media-1",
                SubtitleDownload("subhd", "one", "one.srt", b"one", lang="zh-Hans"),
            )
            second = cache.save_download(
                "media-1",
                SubtitleDownload("subhd", "two", "two.srt", b"two", lang="zh-Hans"),
            )
            tracks = cache.list_tracks("media-1")

        self.assertEqual(first["filename"], "subhd-zh-Hans.srt")
        self.assertEqual(second["filename"], "subhd-zh-Hans-2.srt")
        self.assertEqual(
            [item["filename"] for item in tracks],
            ["subhd-zh-Hans.srt", "subhd-zh-Hans-2.srt"],
        )

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
        self.assertEqual(candidates[0]["language"], "zh-CN")
        self.assertEqual(download.source, "subtitlecat")
        self.assertEqual(download.filename, "MIMK-267-C-zh-CN.srt")
        self.assertEqual(download.body, b"subtitlecat-body")
        self.assertEqual(transport.download_urls, ["https://www.subtitlecat.com/subs/1470/MIMK-267-C-zh-CN.srt"])
        self.assertEqual(len(transport.text_urls), 3)

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

    def test_subtitlecat_manual_download_allows_user_to_judge_non_chinese_subtitle(self):
        from pipeline.external_subtitles import SubtitleCatProvider

        class FakeTransport:
            def __init__(self):
                self.text_urls = []
                self.download_urls = []

            def text_request(self, url, headers=None, timeout=None, max_bytes=None):
                self.text_urls.append(url)
                return '<a id="download_en" href="/subs/467/The-Devil-Conspiracy-en.srt">Download</a>'

            def download(self, url, headers=None, timeout=None, max_bytes=None):
                self.download_urls.append(url)
                return b"english-subtitle"

        transport = FakeTransport()
        provider = SubtitleCatProvider(transport=transport)
        download = provider.download(
            {"url": "https://www.subtitlecat.com/subs/467/The-Devil-Conspiracy.html"},
            "The Devil Conspiracy",
        )

        self.assertEqual(download.filename, "The-Devil-Conspiracy-en.srt")
        self.assertEqual(download.lang, "en")
        self.assertEqual(download.label, "en")
        self.assertEqual(download.body, b"english-subtitle")
        self.assertEqual(len(transport.text_urls), 1)
        self.assertEqual(transport.download_urls, ["https://www.subtitlecat.com/subs/467/The-Devil-Conspiracy-en.srt"])

    def test_subtitlecat_automatic_search_does_not_select_non_chinese_subtitle(self):
        from pipeline.external_subtitles import SubtitleCatProvider

        class FakeTransport:
            def text_request(self, url, headers=None, timeout=None, max_bytes=None):
                if "index.php" in url:
                    return """
                    <table><tbody>
                      <tr><td><a href="subs/467/The-Devil-Conspiracy.html">The Devil Conspiracy</a></td></tr>
                    </tbody></table>
                    """
                return '<a id="download_en" href="/subs/467/The-Devil-Conspiracy-en.srt">Download</a>'

        provider = SubtitleCatProvider(transport=FakeTransport())

        self.assertEqual(provider.search("The Devil Conspiracy"), [])

    def test_subtitlecat_search_filters_titles_without_opening_detail_pages(self):
        from pipeline.external_subtitles import SubtitleCatProvider

        class FakeTransport:
            def __init__(self):
                self.text_urls = []

            def text_request(self, url, headers=None, timeout=None, max_bytes=None):
                self.text_urls.append(url)
                if "index.php" in url:
                    return """
                    <table><tbody>
                      <tr><td><a href="subs/1/shopping-conspiracy.html">Buy Now: The Shopping Conspiracy</a></td></tr>
                      <tr><td><a href="subs/2/devil-conspiracy.html">The Devil Conspiracy (2022)</a></td></tr>
                    </tbody></table>
                    """
                raise AssertionError("search must not request SubtitleCat detail pages")

        transport = FakeTransport()
        provider = SubtitleCatProvider(transport=transport)
        candidates = provider.search_candidates("The Devil Conspiracy", limit=5)

        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0]["title"], "The Devil Conspiracy (2022)")
        self.assertEqual(candidates[0]["language"], "语言待预览确认")
        self.assertEqual(len(transport.text_urls), 1)

    def test_subhd_provider_only_returns_chinese_candidates_and_downloads_direct_subtitle(self):
        from pipeline.external_subtitles import SubHDProvider

        class FakeTransport:
            def __init__(self):
                self.text_urls = []
                self.json_calls = []
                self.download_urls = []

            def text_request(self, url, headers=None, timeout=None, max_bytes=None):
                self.text_urls.append(url)
                if "/search/" not in url:
                    return "ok"
                return """
                <a class="link-dark align-middle" href='/a/chinese1'>恶魔阴谋</a>
                <div class="view-text text-secondary"><a href='/a/chinese1'>The.Devil.Conspiracy.2022.1080p</a></div>
                <div class="text-truncate py-2 f11"><span>双语</span><span>简体</span><span>英语</span><span>SRT</span></div>
                <a class="link-dark align-middle" href='/a/english1'>恶魔阴谋</a>
                <div class="view-text text-secondary"><a href='/a/english1'>The.Devil.Conspiracy.English</a></div>
                <div class="text-truncate py-2 f11"><span>英语</span><span>SRT</span></div>
                """

            def json_request(self, method, url, headers=None, data=None, timeout=None):
                self.json_calls.append((method, url, data))
                if url.endswith("/prepare-download"):
                    return {"success": True, "url": "/down/chinese1"}
                return {"success": True, "pass": True, "url": "https://dlus.subhd.me/2026/07/chinese1.srt"}

            def download(self, url, headers=None, timeout=None, max_bytes=None):
                self.download_urls.append(url)
                return "1\n00:00:01,000 --> 00:00:02,000\n这是中文字幕正文\n".encode("utf-8")

        transport = FakeTransport()
        provider = SubHDProvider(transport=transport)
        candidates = provider.search("The Devil Conspiracy")
        download = provider.download(candidates[0], "The Devil Conspiracy")

        self.assertEqual([item["id"] for item in candidates], ["chinese1"])
        self.assertIn("简体", candidates[0]["language"])
        self.assertEqual(download.source, "subhd")
        self.assertEqual(download.lang, "zh-Hans")
        self.assertEqual(download.body.decode("utf-8").splitlines()[-1], "这是中文字幕正文")
        self.assertEqual(len(transport.text_urls), 3)
        self.assertEqual([item[0] for item in transport.json_calls], ["POST", "POST"])
        self.assertEqual(transport.download_urls, ["https://dlus.subhd.me/2026/07/chinese1.srt"])

    def test_subhd_search_excludes_bitmap_only_candidates(self):
        from pipeline.external_subtitles import extract_subhd_search_results

        html = """
        <a class="link-dark align-middle" href='/a/sup-only'>SUP only</a>
        <div class="view-text text-secondary"><a href='/a/sup-only'>SUP only</a></div>
        <div class="text-truncate py-2 f11"><span>双语</span><span>简体</span><span>SUP</span></div>
        <a class="link-dark align-middle" href='/a/ass-sup'>ASS and SUP</a>
        <div class="view-text text-secondary"><a href='/a/ass-sup'>ASS and SUP</a></div>
        <div class="text-truncate py-2 f11"><span>双语</span><span>简体</span><span>ASS</span><span>SUP</span></div>
        """

        items = extract_subhd_search_results(html)

        self.assertEqual([item["id"] for item in items], ["ass-sup"])
        self.assertTrue(items[0]["filename"].endswith(".ass"))

    def test_subhd_zip_selects_chinese_subtitle(self):
        import io
        import zipfile

        from pipeline.external_subtitles import extract_chinese_subtitle_from_archive

        archive = io.BytesIO()
        with zipfile.ZipFile(archive, "w") as output:
            output.writestr("movie.en.srt", "1\n00:00:01,000 --> 00:00:02,000\nEnglish only\n")
            output.writestr("movie.chs.srt", "1\n00:00:01,000 --> 00:00:02,000\n这是压缩包中的中文字幕\n")

        filename, body = extract_chinese_subtitle_from_archive(
            archive.getvalue(),
            ".zip",
            {"language": "简体"},
            1024 * 1024,
        )

        self.assertEqual(filename, "movie.chs.srt")
        self.assertIn("中文字幕", body.decode("utf-8"))

    def test_subhd_manual_review_allows_supported_text_without_chinese_body_gate(self):
        import io
        import zipfile

        from pipeline.external_subtitles import extract_chinese_subtitle_from_archive

        archive = io.BytesIO()
        with zipfile.ZipFile(archive, "w") as output:
            output.writestr("movie.srt", "1\n00:00:01,000 --> 00:00:02,000\nManual review text\n")

        filename, body = extract_chinese_subtitle_from_archive(
            archive.getvalue(),
            ".zip",
            {"language": "简体"},
            1024 * 1024,
            require_chinese_body=False,
        )

        self.assertEqual(filename, "movie.srt")
        self.assertIn(b"Manual review text", body)

    def test_subhd_rar_uses_bsdtar_for_supported_text_subtitle(self):
        import subprocess
        from pathlib import Path
        from unittest import mock

        from pipeline.external_subtitles import extract_chinese_subtitle_from_archive

        def fake_run(command, **kwargs):
            if command[1] == "-tvf":
                return subprocess.CompletedProcess(
                    command,
                    0,
                    stdout="-rw-r--r--  0 0 0 76 Jul 27 00:00 movie.chs.srt\n",
                    stderr="",
                )
            extract_root = Path(command[command.index("-C") + 1])
            (extract_root / "movie.chs.srt").write_text(
                "1\n00:00:01,000 --> 00:00:02,000\n这是中文字幕正文\n",
                encoding="utf-8",
            )
            return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

        with mock.patch("pipeline.external_subtitles.subprocess.run", side_effect=fake_run) as run:
            filename, body = extract_chinese_subtitle_from_archive(
                b"Rar!",
                ".rar",
                {"language": "简体"},
                1024 * 1024,
            )

        self.assertEqual(filename, "movie.chs.srt")
        self.assertIn("这是中文字幕正文", body.decode("utf-8"))
        self.assertEqual([call.args[0][0] for call in run.call_args_list], ["bsdtar", "bsdtar"])

    def test_subhd_archive_rejects_path_traversal(self):
        import io
        import zipfile

        from pipeline.external_subtitles import extract_chinese_subtitle_from_archive

        archive = io.BytesIO()
        with zipfile.ZipFile(archive, "w") as output:
            output.writestr("../outside.chs.srt", "1\n00:00:01,000 --> 00:00:02,000\n这是中文字幕正文\n")

        with self.assertRaisesRegex(RuntimeError, "archive path invalid"):
            extract_chinese_subtitle_from_archive(
                archive.getvalue(),
                ".zip",
                {"language": "简体"},
                1024 * 1024,
            )

    def test_assrt_provider_excludes_non_chinese_candidates(self):
        from pipeline.external_subtitles import AssrtSubtitleProvider

        class FakeTransport:
            def json_request(self, method, url, headers=None, data=None, timeout=None):
                return {
                    "status": 0,
                    "sub": {
                        "subs": [
                            {"id": 1, "native_name": "The Devil Conspiracy", "lang": {"desc": "English"}},
                            {"id": 2, "native_name": "恶魔阴谋", "lang": {"desc": "繁体"}},
                        ]
                    },
                }

        provider = AssrtSubtitleProvider("secret-token", transport=FakeTransport())
        candidates = provider.search("The Devil Conspiracy")

        self.assertEqual([item["id"] for item in candidates], [2])

    def test_opensubtitles_provider_excludes_non_chinese_responses(self):
        from pipeline.external_subtitles import OpenSubtitlesProvider

        class FakeTransport:
            def __init__(self):
                self.urls = []

            def json_request(self, method, url, headers=None, data=None, timeout=None):
                self.urls.append(url)
                return {
                    "data": [
                        {
                            "id": "english",
                            "attributes": {
                                "language": "en",
                                "release": "The Devil Conspiracy",
                                "files": [{"file_id": 1, "file_name": "movie.en.srt"}],
                            },
                        },
                        {
                            "id": "chinese",
                            "attributes": {
                                "language": "zh-cn",
                                "release": "The Devil Conspiracy",
                                "files": [{"file_id": 2, "file_name": "movie.zh-cn.srt"}],
                            },
                        },
                    ]
                }

        transport = FakeTransport()
        provider = OpenSubtitlesProvider("api-key", transport=transport)
        candidates = provider.search("The Devil Conspiracy")

        self.assertEqual([item["id"] for item in candidates], ["chinese"])
        self.assertIn("languages=zh-cn%2Czh-tw%2Cze", transport.urls[0])

    def test_chinese_subtitle_language_detection_requires_explicit_marker(self):
        from pipeline.external_subtitles import subtitle_lang_label, subtitle_language_value_is_chinese

        for value in ("zh-cn", "zh_Hant", "简体", "繁體中文", "movie.chs.srt", {"desc": "中文"}):
            with self.subTest(value=value):
                self.assertTrue(subtitle_language_value_is_chinese(value))
        for value in ("", "English", "Chinatown", "movie.en.srt", {"desc": "Japanese"}):
            with self.subTest(value=value):
                self.assertFalse(subtitle_language_value_is_chinese(value))

        self.assertEqual(
            subtitle_lang_label("Disclosure.Day.cmn-Hans-简.srt", "简体 繁体"),
            ("zh-Hans", "简体中文"),
        )

    def test_build_subtitle_matcher_includes_default_providers(self):
        from pipeline.external_subtitles import build_subtitle_matcher_from_config

        class Config:
            subtitle_auto_match_enabled = True

        matcher = build_subtitle_matcher_from_config(Config())

        self.assertEqual([provider.name for provider in matcher.providers], ["subhd", "subtitlecat", "assrt", "opensubtitles"])

    def test_subtitle_matcher_prioritizes_subhd_for_non_adult_and_excludes_it_for_adult(self):
        from pipeline.external_subtitles import SubtitleCache, SubtitleDownload, SubtitleMatcher

        class FakeProvider:
            def __init__(self, name):
                self.name = name
                self.search_calls = []

            def enabled(self):
                return True

            def search(self, query, code=""):
                self.search_calls.append((query, code))
                return [{"id": self.name + "-candidate", "language": "简体"}]

            def download(self, candidate, query, code=""):
                return SubtitleDownload(
                    source=self.name,
                    provider_id=candidate["id"],
                    filename=self.name + ".chs.srt",
                    body="1\n00:00:01,000 --> 00:00:02,000\n这是中文字幕正文\n".encode("utf-8"),
                    query=query,
                )

        with tempfile.TemporaryDirectory() as tmp:
            fallback = FakeProvider("fallback")
            subhd = FakeProvider("subhd")
            matcher = SubtitleMatcher(SubtitleCache(tmp), [fallback, subhd], enabled=True, adult_only=False)
            movie = matcher.match_task("movie", "Sintel", {"msg_media_id": "movie-1"})
            adult = matcher.match_task(
                "adult",
                "SSIS-218",
                {"msg_media_id": "adult-1", "openlist_adult_code": "SSIS-218"},
            )

        self.assertEqual(movie["subtitle_match_source"], "subhd")
        self.assertEqual(adult["subtitle_match_source"], "fallback")
        self.assertEqual(subhd.search_calls, [("Sintel", "")])
        self.assertEqual(fallback.search_calls, [("SSIS-218", "SSIS-218")])

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

    def test_subtitle_matcher_allows_unrelated_download_filename_when_candidate_matches(self):
        from pipeline.external_subtitles import SubtitleCache, SubtitleDownload, SubtitleMatcher

        class FakeProvider:
            name = "fake"

            def enabled(self):
                return True

            def search(self, query, code=""):
                return [{"id": "candidate-1", "title": code, "language": "zh-cn"}]

            def download(self, candidate, query, code=""):
                return SubtitleDownload(
                    source=self.name,
                    provider_id="candidate-1",
                    filename="unrelated.zh.srt",
                    body=b"[Script Info]\n",
                    query=query,
                )

        with tempfile.TemporaryDirectory() as tmp:
            cache = SubtitleCache(tmp)
            matcher = SubtitleMatcher(cache, [FakeProvider()], enabled=True, adult_only=True)
            result = matcher.match_task(
                "adult",
                "SSIS-218",
                {"msg_media_id": "media-1", "openlist_adult_code": "SSIS-218"},
            )
            tracks = cache.list_tracks("media-1")

        self.assertEqual(result["subtitle_match_status"], "success")
        self.assertEqual(result["subtitle_match_source"], "fake")
        self.assertEqual(len(tracks), 1)

    def test_candidate_code_score_rejects_adjacent_number_suffix(self):
        from pipeline.external_subtitles import candidate_code_score

        self.assertGreater(candidate_code_score({"title": "SSIS-218 CHS"}, "SSIS-218"), 0)
        self.assertGreater(candidate_code_score({"title": "SSIS-218-C CHS"}, "SSIS-218"), 0)
        self.assertGreater(candidate_code_score({"title": "SSIS 218 CHS"}, "SSIS-218"), 0)
        self.assertEqual(candidate_code_score({"title": "SSIS-2180 CHS"}, "SSIS-218"), 0)
        self.assertEqual(candidate_code_score({"title": "SSIS2180 CHS"}, "SSIS-218"), 0)

    def test_adult_source_declares_chinese_subtitles_from_explicit_markers_and_code_suffix(self):
        from pipeline.external_subtitles import adult_source_declares_chinese_subtitles

        positives = [
            ("[HD] HMN-720 [中文字幕] 标题", {}),
            ("SSIS-470 标题《FHD中文》", {}),
            ("SSIS-310", {"name": "SSIS-310_CH.HD"}),
            ("HMN-720", {"openlist_adult_video_old_path": "/115/成人/HMN-720/hmn-720ch.mp4"}),
            ("MIDE-882", {"name": "MIDE-882.CHS.1080p"}),
            ("MIDE-949", {"name": "MIDE-949 Chinese"}),
        ]
        negatives = [
            ("PRED-867", {"openlist_adult_video_old_path": "/115/成人/PRED-867/PRED-867J-UC.mp4"}),
            ("[无码破解] HMN-723", {"name": "HMN-723-U"}),
            ("SSIS-415 普通版本", {"name": "SSIS-415"}),
        ]

        for title, task in positives:
            with self.subTest(title=title, task=task):
                self.assertTrue(adult_source_declares_chinese_subtitles(title, task))
        for title, task in negatives:
            with self.subTest(title=title, task=task):
                self.assertFalse(adult_source_declares_chinese_subtitles(title, task))

    def test_subtitle_matcher_skips_declared_chinese_source_but_manual_force_can_override(self):
        from pipeline.external_subtitles import SubtitleCache, SubtitleDownload, SubtitleMatcher

        class FakeProvider:
            name = "fake"

            def __init__(self):
                self.search_calls = []

            def enabled(self):
                return True

            def search(self, query, code=""):
                self.search_calls.append((query, code))
                return [{"id": "candidate-1"}]

            def download(self, candidate, query, code=""):
                return SubtitleDownload(
                    source=self.name,
                    provider_id=candidate["id"],
                    filename="HMN-720.srt",
                    body=b"subtitle-body",
                    query=query,
                )

        with tempfile.TemporaryDirectory() as tmp:
            cache = SubtitleCache(tmp)
            provider = FakeProvider()
            matcher = SubtitleMatcher(cache, [provider], enabled=True, adult_only=True)
            task = {
                "msg_media_id": "media-1",
                "openlist_adult_code": "HMN-720",
                "openlist_adult_video_old_path": "/115/成人/HMN-720/hmn-720ch.mp4",
            }
            skipped = matcher.match_task("adult", "HMN-720", task)
            forced = matcher.match_task("adult", "HMN-720", task, force=True)

        self.assertEqual(skipped["subtitle_match_status"], "skipped")
        self.assertEqual(skipped["subtitle_match_reason"], "source_declares_chinese_subtitles")
        self.assertEqual(forced["subtitle_match_status"], "success")
        self.assertEqual(provider.search_calls, [("HMN-720", "HMN-720")])

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

    def test_manual_subtitle_search_is_not_blocked_by_auto_match_policy(self):
        from pipeline.external_subtitles import SubtitleCache, SubtitleMatcher

        class FakeProvider:
            name = "fake"

            def enabled(self):
                return True

            def search(self, query, code=""):
                return [{"id": "movie-subtitle", "filename": "Sintel.zh.srt", "_score": 10}]

        with tempfile.TemporaryDirectory() as tmp:
            matcher = SubtitleMatcher(SubtitleCache(tmp), [FakeProvider()], enabled=False, adult_only=True)
            candidates = matcher.search_task_candidates(
                "movie",
                "Sintel",
                {"msg_media_id": "media-movie", "msg_media_title": "Sintel"},
                manual=True,
            )

        self.assertEqual([item["provider_id"] for item in candidates], ["movie-subtitle"])

    def test_manual_subtitle_search_can_restrict_to_subhd(self):
        from pipeline.external_subtitles import SubtitleCache, SubtitleMatcher

        class FakeProvider:
            def __init__(self, name):
                self.name = name

            def enabled(self):
                return True

            def search(self, query, code=""):
                return [{"id": self.name + "-1", "filename": query + ".zh.srt", "_score": 10}]

        with tempfile.TemporaryDirectory() as tmp:
            matcher = SubtitleMatcher(
                SubtitleCache(tmp),
                [FakeProvider("subhd"), FakeProvider("subtitlecat")],
                enabled=False,
                adult_only=False,
            )
            candidates = matcher.search_task_candidates(
                "tv",
                "Alien: Earth S01",
                {"msg_media_id": "media-episode"},
                manual=True,
                provider_names=("subhd",),
            )

        self.assertEqual([item["provider"] for item in candidates], ["subhd"])

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



class CategoryConfigTest(unittest.TestCase):

    def test_build_migration_target_uses_target_category_root(self):
        from pipeline.migration import build_migration_target

        target = build_migration_target(
            {"category": "tv", "source_openlist_path": "/115/剧集/成龙历险记"},
            "anime",
        )

        self.assertEqual(target["target_category"], "anime")
        self.assertEqual(target["target_root_openlist_path"], "/115/动漫")
        self.assertEqual(target["target_openlist_path"], "/115/动漫/成龙历险记")

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
        self.assertTrue(movie["scrape_enabled"])
        self.assertFalse(other["scrape_enabled"])

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
                    "MEDIA_PIPELINE_MOVIE_MSG_SCRAPE_ENABLED": "false",
            }
        )
        folder_ids, openlist_paths, msg_roots = category_maps(config)

        self.assertEqual(folder_ids["movie"], "folder-override")
        self.assertEqual(openlist_paths["movie"], "/115/电影新")
        self.assertEqual(msg_roots["movie"]["library_id"], "library-override")
        self.assertEqual(msg_roots["movie"]["root_id"], "root-override")
        self.assertFalse(msg_roots["movie"]["scrape_enabled"])
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

    def test_rejects_missing_msg_root_ids_without_database_fallback(self):
        from pipeline.config import MSG_LIBRARY_ROOTS, category_to_msg_library_root

        original_roots = copy.deepcopy(MSG_LIBRARY_ROOTS)
        try:
            MSG_LIBRARY_ROOTS["movie"]["library_id"] = "REPLACE_WITH_MSG_MOVIE_LIBRARY_ID"
            MSG_LIBRARY_ROOTS["movie"]["root_id"] = "REPLACE_WITH_MSG_MOVIE_ROOT_ID"
            with self.assertRaisesRegex(RuntimeError, "root ids missing"):
                category_to_msg_library_root("movie")
        finally:
            MSG_LIBRARY_ROOTS.clear()
            MSG_LIBRARY_ROOTS.update(original_roots)

    def test_returns_explicit_msg_root_ids_without_discovery(self):
        from pipeline.config import MSG_LIBRARY_ROOTS, category_to_msg_library_root

        original_roots = copy.deepcopy(MSG_LIBRARY_ROOTS)
        try:
            MSG_LIBRARY_ROOTS["movie"]["library_id"] = "movie-library-explicit"
            MSG_LIBRARY_ROOTS["movie"]["root_id"] = "movie-root-explicit"
            root = category_to_msg_library_root("movie")
        finally:
            MSG_LIBRARY_ROOTS.clear()
            MSG_LIBRARY_ROOTS.update(original_roots)

        self.assertEqual(root["library_id"], "movie-library-explicit")
        self.assertEqual(root["root_id"], "movie-root-explicit")

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
    def test_reads_access_token_from_openlist_admin_api(self):
        transport = FakeTransport(
            {
                "code": 200,
                "data": {
                    "content": [
                        {
                            "id": 7,
                            "mount_path": "/115",
                            "driver": "115 Open",
                            "disabled": False,
                            "addition": json.dumps({"access_token": "api-access-token"}),
                        }
                    ]
                },
            }
        )

        token = load_access_token_from_api("https://openlist.example", "admin-token", transport=transport)

        self.assertEqual(token.storage_id, 7)
        self.assertEqual(token.mount_path, "/115")
        self.assertEqual(token.access_token, "api-access-token")
        self.assertEqual(transport.calls[0]["method"], "GET")
        self.assertEqual(transport.calls[0]["url"], "https://openlist.example/api/admin/storage/list")
        self.assertEqual(transport.calls[0]["headers"]["Authorization"], "admin-token")

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
    def test_subtitle_translation_accepts_direct_msg_response(self):
        transport = SequenceTransport(
            [
                {"tokens": {"access_token": "msg-token"}},
                {"translation": "不，挺好的，基本上刚刚好。"},
            ]
        )
        client = MediaStationClient(
            "http://127.0.0.1:18080/api", "admin", "secret", transport=transport
        )

        result = client.pipeline_translate_subtitle(
            "openai",
            "deepseek-v4-flash",
            "いやいいな 基本ちょうどいいわ。",
            [],
            "",
            "上次译文仍含日文假名，请全部翻译。",
        )

        self.assertEqual(result["translation"], "不，挺好的，基本上刚刚好。")
        call = transport.calls[1]
        self.assertEqual(call["url"], "http://127.0.0.1:18080/api/pipeline/subtitles/translate")
        self.assertEqual(call["data"]["retry_instruction"], "上次译文仍含日文假名，请全部翻译。")

    def test_pipeline_methods_use_authenticated_msg_endpoints(self):
        transport = SequenceTransport(
            [
                {"tokens": {"access_token": "msg-token"}},
                {"code": 0, "message": "ok", "data": {"mode": "smart", "media_id": "media-1"}},
                {
                    "code": 0,
                    "message": "ok",
                    "data": {
                        "media_id": "media-1",
                        "has_chinese": False,
                        "embedded_checked": True,
                        "embedded": [],
                        "external": [],
                        "unknown_embedded": 0,
                    },
                },
                {"code": 0, "message": "ok", "data": {"status": "success", "updated": 1}},
                {"code": 0, "message": "ok", "data": {"status": "success", "updated": 2}},
                {"code": 0, "message": "ok", "data": {"status": "success", "removed": 14, "preserved": 14}},
                {"code": 0, "message": "ok", "data": {"status": "success", "deleted": 1}},
                {"code": 0, "message": "ok", "data": {"items": [{"media_id": "deleted-1"}]}},
                {"code": 0, "message": "ok", "data": {"items": [{"title": "Show"}]}},
                {"code": 0, "message": "ok", "data": {"target_openlist_path": "/115/动漫/Show"}},
                {"code": 0, "message": "ok", "data": {"target_openlist_path": "/115/动漫/Show"}},
                {"code": 0, "message": "ok", "data": {"id": "ingest-1", "status": "running"}},
                {"code": 0, "message": "ok", "data": {"id": "ingest-1", "status": "completed"}},
            ]
        )
        client = MediaStationClient("http://127.0.0.1:18080/api", "admin", "secret", transport=transport)
        target = {"category": "movie", "library_id": "library-1", "root_id": "root-1", "root_openlist_path": "/115/电影"}
        migration_source = {
            "category": "tv",
            "library_id": "library-tv",
            "library_root_id": "root-tv",
            "source_openlist_path": "/115/剧集/Show",
            "source_kind": "folder",
        }
        migration_target = {
            "category": "anime",
            "library_id": "library-anime",
            "root_id": "root-anime",
            "root_openlist_path": "/115/动漫",
        }

        self.assertEqual(client.pipeline_scrape_media("media-1", "movie", "Movie", ["Movie"], "tmdb", "movie")["mode"], "smart")
        self.assertFalse(client.pipeline_subtitle_status("media-1")["has_chinese"])
        self.assertEqual(client.pipeline_repair_movie_extras("media-1", target)["updated"], 1)
        self.assertEqual(client.pipeline_repair_episode_visibility("media-1", target)["updated"], 2)
        self.assertEqual(
            client.pipeline_replace_work_source("old-episode", "new-episode", target, ["/115/剧集/Show-new"])["removed"],
            14,
        )
        self.assertEqual(client.pipeline_prune_deleted_media(target, ["/115/电影/Movie"])["deleted"], 1)
        self.assertEqual(client.pipeline_list_deleted_media_hide_candidates(100)["items"][0]["media_id"], "deleted-1")
        self.assertEqual(client.pipeline_search_migration_candidates("Show", 20)["items"][0]["title"], "Show")
        self.assertEqual(client.pipeline_validate_migration(migration_source, migration_target)["target_openlist_path"], "/115/动漫/Show")
        self.assertEqual(client.pipeline_apply_migration(migration_source, migration_target)["target_openlist_path"], "/115/动漫/Show")
        self.assertEqual(client.pipeline_start_ingest({"category": "movie"})["id"], "ingest-1")
        self.assertEqual(client.pipeline_get_ingest("ingest-1")["status"], "completed")

        self.assertEqual(transport.calls[1]["url"], "http://127.0.0.1:18080/api/pipeline/media/media-1/scrape")
        self.assertEqual(transport.calls[2]["method"], "GET")
        self.assertEqual(transport.calls[2]["url"], "http://127.0.0.1:18080/api/pipeline/media/media-1/subtitle-status")
        self.assertEqual(transport.calls[3]["url"], "http://127.0.0.1:18080/api/pipeline/media/media-1/repair-movie-extras")
        self.assertEqual(transport.calls[4]["url"], "http://127.0.0.1:18080/api/pipeline/media/media-1/repair-episode-visibility")
        self.assertEqual(transport.calls[5]["url"], "http://127.0.0.1:18080/api/pipeline/media/old-episode/replace-work-source")
        self.assertEqual(transport.calls[5]["data"]["new_media_id"], "new-episode")
        self.assertEqual(transport.calls[5]["data"]["new_openlist_paths"], ["/115/剧集/Show-new"])
        self.assertEqual(transport.calls[6]["url"], "http://127.0.0.1:18080/api/pipeline/deleted-media/prune")
        self.assertEqual(transport.calls[6]["data"]["openlist_paths"], ["/115/电影/Movie"])
        self.assertEqual(transport.calls[7]["url"], "http://127.0.0.1:18080/api/pipeline/deleted-media/hide-candidates")
        self.assertEqual(transport.calls[8]["url"], "http://127.0.0.1:18080/api/pipeline/migrations/search")
        self.assertEqual(transport.calls[9]["url"], "http://127.0.0.1:18080/api/pipeline/migrations/validate")
        self.assertEqual(transport.calls[10]["url"], "http://127.0.0.1:18080/api/pipeline/migrations/apply")
        self.assertEqual(transport.calls[11]["url"], "http://127.0.0.1:18080/api/pipeline/ingest")
        self.assertEqual(transport.calls[12]["url"], "http://127.0.0.1:18080/api/pipeline/ingest/ingest-1")

    def test_media_read_methods_use_authenticated_endpoints(self):
        transport = SequenceTransport(
            [
                {"tokens": {"access_token": "msg-token"}},
                {"data": {"items": []}},
                {"data": {"items": []}},
                {"data": {"id": "media-1"}},
                {"deleted_id": "media-1"},
            ]
        )
        client = MediaStationClient("http://127.0.0.1:18080/api", "admin", "secret", transport=transport)

        client.search_media("Movie", limit=20)
        client.list_library_media("library-1", page=2, page_size=100, group_versions=0)
        client.get_media("media-1")
        client.soft_delete_media_version("media-1", "media-1")

        self.assertEqual(transport.calls[1]["url"], "http://127.0.0.1:18080/api/media?q=Movie&limit=20")
        self.assertEqual(
            transport.calls[2]["url"],
            "http://127.0.0.1:18080/api/libraries/library-1/media?page=2&page_size=100&group_versions=0",
        )
        self.assertEqual(transport.calls[3]["url"], "http://127.0.0.1:18080/api/media/media-1")
        self.assertEqual(
            transport.calls[4]["url"],
            "http://127.0.0.1:18080/api/media/media-1/versions/media-1",
        )
        self.assertEqual(transport.calls[4]["method"], "DELETE")

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
        codes = extract_codes("[HEVC-10bit][H264-1080][WEB-DL.1080p][Sintel.2010][ABF-363][GANA-2525]")

        self.assertEqual(codes, {"ABF-363", "GANA-2525"})

    def test_movie_subtitle_queries_ignore_release_tokens_as_adult_codes(self):
        from pipeline.external_subtitles import subtitle_task_queries

        queries, code = subtitle_task_queries(
            "movie",
            "The Devil Conspiracy",
            {
                "msg_media_title": "恶魔阴谋",
                "msg_match_path": "/115/电影/The.Devil.Conspiracy.2022.WEB-DL.1080p.mkv",
            },
        )

        self.assertEqual(queries, ["The Devil Conspiracy", "恶魔阴谋"])
        self.assertEqual(code, "")

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

class Client115Test(unittest.TestCase):
    def test_folder_file_move_and_delete_use_official_open_endpoints(self):
        transport = FakeTransport({"state": True, "data": {"file_id": "task-folder"}})
        client = Client115("access-token-value", transport=transport)

        client.create_folder("task", "parent")
        client.move_files(["video-1", "subtitle-1"], "formal")
        client.delete_files(["task-folder"])

        self.assertEqual(transport.calls[0]["url"], "https://proapi.115.com/open/folder/add")
        self.assertEqual(transport.calls[0]["data"], {"file_name": "task", "pid": "parent"})
        self.assertEqual(transport.calls[1]["url"], "https://proapi.115.com/open/ufile/move")
        self.assertEqual(transport.calls[1]["data"], {"file_ids": "video-1,subtitle-1", "to_cid": "formal"})
        self.assertEqual(transport.calls[2]["url"], "https://proapi.115.com/open/ufile/delete")
        self.assertEqual(transport.calls[2]["data"], {"file_ids": "task-folder"})

    def test_list_all_files_accepts_nested_open_api_shape(self):
        transport = FakeTransport(
            {"state": True, "data": {"count": 2, "data": [{"fid": "1", "fn": "a"}, {"fid": "2", "fn": "b"}]}}
        )
        client = Client115("access-token-value", transport=transport)

        rows = client.list_all_files("parent")

        self.assertEqual([row["fid"] for row in rows], ["1", "2"])
        self.assertIn("/open/ufile/files?", transport.calls[0]["url"])

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

class SearchProfileTest(unittest.TestCase):
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

