import json
import sys
import tempfile
import time
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "app"))

from pipeline.danmaku import (
    AggregatorSource,
    DanmakuCache,
    DanmakuMatcher,
    DandanplayProtocolSource,
    apply_danmaku_offset,
    build_danmaku_payload,
    dandanplay_signature,
    dedupe_danmaku,
    filter_danmaku_keywords,
    normalize_danmaku_cid,
    normalize_danmaku_comments,
    parse_comment_p,
    sample_danmaku,
)


class FakeTransport:
    """按 URL 子串匹配返回预置响应，并记录全部请求。"""

    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def json_request(self, method, url, headers=None, data=None, timeout=None):
        self.calls.append(
            {"method": method, "url": url, "headers": dict(headers or {}), "data": data, "timeout": timeout}
        )
        for needle, payload in self.responses:
            if needle in url:
                if callable(payload):
                    return payload(url, data)
                return payload
        raise RuntimeError("unexpected request: %s" % url)


def danmaku_source(transport, **kwargs):
    options = {
        "app_id": "demo-app",
        "app_secret": "demo-secret",
        "base_url": "https://api.example.test",
        "transport": transport,
        "timeout": 5,
        "comment_timeout": 5,
    }
    options.update(kwargs)
    return DandanplayProtocolSource(**options)


class SignatureTest(unittest.TestCase):
    def test_signature_matches_documented_algorithm(self):
        # 期望值由独立脚本按 base64(sha256(AppId+Timestamp+Path+AppSecret)) 计算得出，
        # 锁定「拼接顺序 + 原始摘要 + 标准 Base64（非 hex、非 URL-safe）」这三件事。
        value = dandanplay_signature("demo-app", 1735660800, "/api/v2/comment/123450001", "demo-secret")
        self.assertEqual(value, "6h2z0ocbCPZGfOJG2CUUvAjG6cifAC9erivpK6GNX1M=")

    def test_signature_ignores_query_string_only_path(self):
        # 官方要求 Path 不含查询参数：同一 path 在不同查询串下签名必须一致。
        first = dandanplay_signature("a", 1, "/api/v2/comment/1", "s")
        second = dandanplay_signature("a", 1, "/api/v2/comment/1", "s")
        self.assertEqual(first, second)


class CommentParseTest(unittest.TestCase):
    def test_dandanplay_four_segment_format(self):
        # 实测：弹弹play 的 p 只有 4 段 —— 时间,模式,颜色,用户
        parsed = parse_comment_p("847.66,1,16020176,14b30012")
        self.assertEqual(parsed["time"], 847.66)
        self.assertEqual(parsed["mode"], 1)
        self.assertEqual(parsed["color"], 16020176)
        self.assertEqual(parsed["user"], "14b30012")
        self.assertEqual(parsed["mode_name"], "scroll")

    def test_bilibili_eight_segment_format_keeps_color_at_index_three(self):
        parsed = parse_comment_p("15.50,1,25,16777215,1690000000,0,abc123,42")
        self.assertEqual(parsed["time"], 15.5)
        self.assertEqual(parsed["size"], 25)
        self.assertEqual(parsed["color"], 16777215)
        self.assertEqual(parsed["user"], "abc123")
        self.assertEqual(parsed["row_id"], "42")

    def test_invalid_payloads_are_rejected(self):
        self.assertIsNone(parse_comment_p(""))
        self.assertIsNone(parse_comment_p("1,2,3"))
        self.assertIsNone(parse_comment_p("abc,1,2,3"))
        self.assertIsNone(parse_comment_p("-5,1,16777215,user"))

    def test_out_of_range_color_falls_back_to_white(self):
        parsed = parse_comment_p("1,1,99999999,user")
        self.assertEqual(parsed["color"], 0xFFFFFF)


class NormalizeTest(unittest.TestCase):
    def test_cid_is_stringified_because_it_exceeds_js_safe_integer(self):
        value = normalize_danmaku_cid(1542278977442529280)
        self.assertEqual(value, "1542278977442529280")
        self.assertIsInstance(value, str)

    def test_normalize_reports_skipped_entries_instead_of_swallowing_them(self):
        comments, skipped = normalize_danmaku_comments(
            [
                {"cid": 1542278977442529280, "p": "847.66,1,16020176,14b30012", "m": "前方高能"},
                {"cid": 2, "p": "broken", "m": "坏数据"},
                {"cid": 3, "p": "12,5,16777215,user", "m": "  "},
                {"cid": 4, "p": "0.5,5,16777215,user", "m": "顶部弹幕"},
            ]
        )
        self.assertEqual(len(comments), 2)
        self.assertEqual(skipped, 2)
        self.assertEqual(comments[0]["m"], "顶部弹幕")
        self.assertEqual(comments[0]["mode_name"], "top")
        self.assertEqual(comments[1]["cid"], "1542278977442529280")
        self.assertEqual(comments[1]["mode_name"], "scroll")


class TransformTest(unittest.TestCase):
    def _comments(self):
        return [
            {"cid": "1", "m": "AAA", "time": 1.0, "mode": 1, "color": 0xFFFFFF},
            {"cid": "2", "m": "AAA", "time": 1.0, "mode": 1, "color": 0xFFFFFF},
            {"cid": "3", "m": "BBB", "time": 2.0, "mode": 1, "color": 0xFFFFFF},
            {"cid": "4", "m": "广告加群", "time": 3.0, "mode": 1, "color": 0xFFFFFF},
        ]

    def test_dedupe_keeps_first_occurrence(self):
        result = dedupe_danmaku(self._comments())
        self.assertEqual([item["cid"] for item in result], ["1", "3", "4"])

    def test_keyword_filter(self):
        result = filter_danmaku_keywords(self._comments(), ["加群"])
        self.assertEqual([item["cid"] for item in result], ["1", "2", "3"])

    def test_offset_shifts_and_resorts(self):
        result = apply_danmaku_offset(self._comments(), 2.0)
        self.assertEqual([round(item["time"], 1) for item in result], [3.0, 3.0, 4.0, 5.0])

    def test_offset_clamps_negative_times_to_zero(self):
        result = apply_danmaku_offset([{"cid": "1", "m": "x", "time": 1.0}], -5.0)
        self.assertEqual(result[0]["time"], 0.0)

    def test_sampling_is_uniform_not_truncating(self):
        comments = [
            {"cid": str(index), "m": "x", "time": float(index), "mode": 1, "color": 0xFFFFFF}
            for index in range(100)
        ]
        sampled, truncated = sample_danmaku(comments, 10)
        self.assertTrue(truncated)
        self.assertEqual(len(sampled), 10)
        # 均匀采样：最后一条必须落在尾部区间，而不是被截断在中段
        self.assertGreaterEqual(sampled[-1]["time"], 80.0)

    def test_payload_applies_provider_shift_then_user_offset(self):
        payload = build_danmaku_payload(
            [{"cid": "1", "m": "x", "time": 10.0, "mode": 1, "color": 0xFFFFFF}],
            source="dandanplay",
            episode_id="95410010",
            provider_shift_seconds=2.0,
            offset_seconds=-1.0,
        )
        self.assertEqual(payload["comments"][0]["time"], 11.0)
        self.assertEqual(payload["provider_shift_seconds"], 2.0)
        self.assertEqual(payload["count"], 1)
        self.assertFalse(payload["truncated"])


class CacheTest(unittest.TestCase):
    def test_round_trip_and_ttl_expiry(self):
        with tempfile.TemporaryDirectory() as tmp:
            cache = DanmakuCache(tmp)
            cache.save("comment_dandanplay_1_1_0", {"comments": [{"cid": "1"}]})
            self.assertEqual(
                cache.load("comment_dandanplay_1_1_0", ttl_seconds=60)["comments"][0]["cid"], "1"
            )
            stale_path = cache._path("stale")
            stale_path.parent.mkdir(parents=True, exist_ok=True)
            stale_path.write_text(
                json.dumps({"fetched_at": time.time() - 7200, "body": {"comments": []}}),
                encoding="utf-8",
            )
            self.assertIsNone(cache.load("stale", ttl_seconds=60))
            self.assertIsNotNone(cache.load("stale", ttl_seconds=0))


class SourceTest(unittest.TestCase):
    def test_match_uses_placeholder_hash_and_reports_shift(self):
        transport = FakeTransport(
            [
                (
                    "/api/v2/match",
                    {
                        "isMatched": True,
                        "matches": [
                            {
                                "episodeId": 95410010,
                                "animeId": 9541,
                                "animeTitle": "进击的巨人",
                                "episodeTitle": "第10话",
                                "shift": 2,
                            }
                        ],
                        "success": True,
                    },
                )
            ]
        )
        source = danmaku_source(transport)
        result = source.match("some.file.mkv")
        self.assertTrue(result["is_matched"])
        self.assertEqual(result["matches"][0]["episode_id"], "95410010")
        self.assertEqual(result["matches"][0]["shift"], 2.0)
        body = transport.calls[0]["data"]
        self.assertEqual(body["matchMode"], "hashAndFileName")
        self.assertTrue(body["fileHash"])
        self.assertEqual(transport.calls[0]["headers"]["X-AppId"], "demo-app")
        self.assertIn("X-Signature", transport.calls[0]["headers"])

    def test_business_error_is_raised_not_swallowed(self):
        transport = FakeTransport(
            [("/api/v2/search/episodes", {"success": False, "errorCode": 3, "errorMessage": "应用不存在"})]
        )
        source = danmaku_source(transport)
        with self.assertRaises(RuntimeError) as ctx:
            source.search_episodes(anime="进击的巨人")
        self.assertIn("应用不存在", str(ctx.exception))

    def test_comment_parses_four_segment_payload(self):
        transport = FakeTransport(
            [
                (
                    "/api/v2/comment/95410010",
                    {
                        "count": 2376,
                        "comments": [
                            {"cid": 1542278977442529280, "p": "847.66,1,16020176,14b30012", "m": "前方高能"},
                            {"cid": 5, "p": "0.10,5,16777215,abc", "m": "顶部"},
                        ],
                    },
                )
            ]
        )
        source = danmaku_source(transport)
        result = source.comment("95410010")
        self.assertEqual(result["count"], 2376)
        self.assertEqual(result["comments"][0]["cid"], "5")
        self.assertEqual(result["comments"][0]["time"], 0.1)
        self.assertEqual(result["comments"][0]["mode_name"], "top")
        self.assertEqual(result["comments"][1]["cid"], "1542278977442529280")
        self.assertIn("withRelated=true", transport.calls[0]["url"])
        self.assertNotIn("from=", transport.calls[0]["url"])

    def test_searches_use_v2_engine_and_tmdb_id(self):
        transport = FakeTransport([("/api/v2/search/episodes", {"animes": []})])
        source = danmaku_source(transport)
        source.search_episodes(tmdb_id=100049, episode=2)
        url = transport.calls[0]["url"]
        self.assertIn("tmdbId=100049", url)
        self.assertIn("episode=2", url)
        self.assertIn("v2=true", url)

    def test_source_disabled_without_credentials(self):
        self.assertFalse(danmaku_source(FakeTransport([]), app_id="", app_secret="").enabled())
        self.assertFalse(danmaku_source(FakeTransport([]), app_id="id", app_secret="").enabled())

    def test_aggregator_needs_base_url_and_no_signature(self):
        with self.assertRaises(ValueError):
            AggregatorSource(base_url="")
        transport = FakeTransport([("/api/v2/comment/1", {"count": 0, "comments": []})])
        source = AggregatorSource(base_url="http://127.0.0.1:9321", transport=transport)
        self.assertTrue(source.enabled())
        source.comment("1")
        self.assertNotIn("X-Signature", transport.calls[0]["headers"])


class MatcherTest(unittest.TestCase):
    def _matcher(self, transport, sources=None, **kwargs):
        tmp = tempfile.mkdtemp()
        options = {"cache": DanmakuCache(tmp), "cache_ttl_seconds": 60, "max_comments": 6000}
        options.update(kwargs)
        return DanmakuMatcher(sources or [danmaku_source(transport)], **options)

    def test_tmdb_lookup_wins_over_filename(self):
        transport = FakeTransport(
            [
                (
                    "/api/v2/search/episodes",
                    {
                        "animes": [
                            {
                                "animeId": 9541,
                                "animeTitle": "进击的巨人",
                                "episodes": [
                                    {"episodeId": 95410001, "episodeTitle": "第1话", "episodeNumber": "1"},
                                    {"episodeId": 95410010, "episodeTitle": "第10话", "episodeNumber": "10"},
                                ],
                            }
                        ]
                    },
                ),
                ("/api/v2/match", {"isMatched": True, "matches": [{"episodeId": 999, "animeTitle": "错的"}]}),
            ]
        )
        result = self._matcher(transport).match(title="进击的巨人", episode=10, tmdb_id=1429, file_name="a.mkv")
        self.assertTrue(result["matched"])
        self.assertEqual(result["match_mode"], "tmdb")
        self.assertEqual(result["episode_id"], "95410010")
        self.assertEqual(len(transport.calls), 1)

    def test_falls_back_to_filename_match_when_tmdb_has_no_episode(self):
        transport = FakeTransport(
            [
                ("/api/v2/search/episodes", {"animes": []}),
                (
                    "/api/v2/match",
                    {
                        "isMatched": True,
                        "matches": [{"episodeId": 42, "animeTitle": "某番", "episodeTitle": "第3话", "shift": 1}],
                    },
                ),
            ]
        )
        result = self._matcher(transport).match(title="某番", episode=3, tmdb_id=1, file_name="a.mkv")
        self.assertEqual(result["match_mode"], "filename")
        self.assertEqual(result["episode_id"], "42")
        self.assertEqual(result["shift"], 1.0)

    def test_unmatched_result_is_reported_with_attempts(self):
        transport = FakeTransport(
            [
                ("/api/v2/search/episodes", {"animes": []}),
                ("/api/v2/match", {"isMatched": False, "matches": []}),
            ]
        )
        result = self._matcher(transport).match(title="不存在", episode=1, file_name="x.mkv")
        self.assertFalse(result["matched"])
        self.assertEqual(result["episode_id"], "")
        self.assertTrue(result["attempts"])

    def test_aggregator_is_used_when_primary_source_fails(self):
        primary_transport = FakeTransport(
            [("/api/v2/comment/1", {"success": False, "errorCode": 3, "errorMessage": "应用不存在"})]
        )
        fallback_transport = FakeTransport(
            [("/api/v2/comment/1", {"count": 1, "comments": [{"cid": 1, "p": "1,1,16777215,u", "m": "兜底"}]})]
        )
        sources = [
            danmaku_source(primary_transport),
            AggregatorSource(base_url="http://127.0.0.1:9321", transport=fallback_transport),
        ]
        payload = self._matcher(fallback_transport, sources=sources).comments("1", source_name="aggregator")
        self.assertEqual(payload["source"], "aggregator")
        self.assertEqual(payload["comments"][0]["m"], "兜底")

    def test_comments_are_cached_and_shift_and_blacklist_are_applied(self):
        transport = FakeTransport(
            [
                (
                    "/api/v2/comment/7",
                    {
                        "count": 3,
                        "comments": [
                            {"cid": 1, "p": "10,1,16777215,u", "m": "正常"},
                            {"cid": 2, "p": "11,1,16777215,u", "m": "广告加群"},
                        ],
                    },
                )
            ]
        )
        matcher = self._matcher(transport, blacklist=["加群"])
        first = matcher.comments("7", provider_shift_seconds=2.0)
        second = matcher.comments("7", provider_shift_seconds=2.0)
        self.assertEqual(len(transport.calls), 1, "第二次调用必须命中缓存")
        self.assertEqual(first["total"], 2)
        self.assertEqual(first["filtered"], 1)
        self.assertEqual(first["count"], 1)
        self.assertEqual(first["comments"][0]["time"], 12.0)
        self.assertEqual(second["comments"][0]["time"], 12.0)

    def test_sampling_caps_large_payloads(self):
        comments = [{"cid": str(i), "p": "%d.0,1,16777215,u" % i, "m": "d%d" % i} for i in range(50)]
        transport = FakeTransport([("/api/v2/comment/9", {"count": 50, "comments": comments})])
        matcher = self._matcher(transport, max_comments=10)
        payload = matcher.comments("9")
        self.assertTrue(payload["truncated"])
        self.assertEqual(payload["count"], 10)
        self.assertEqual(payload["total"], 50)

    def test_no_source_configured_is_an_error(self):
        matcher = DanmakuMatcher([], cache=DanmakuCache(tempfile.mkdtemp()))
        with self.assertRaises(RuntimeError):
            matcher.match(title="x")
        with self.assertRaises(RuntimeError):
            matcher.comments("1")


if __name__ == "__main__":
    unittest.main()
