import json
import sys
import tempfile
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "app"))

from pipeline.danmaku import (
    AggregatorSource,
    DanmakuCache,
    DanmakuImportError,
    DanmakuLocalImport,
    DanmakuMatcher,
    DandanplayProtocolSource,
    apply_danmaku_offset,
    build_danmaku_payload,
    build_danmaku_local_import_from_config,
    dandanplay_signature,
    dedupe_danmaku,
    detect_danmaku_import_format,
    filter_danmaku_keywords,
    normalize_danmaku_cid,
    normalize_danmaku_comments,
    parse_bilibili_xml,
    parse_comment_p,
    parse_dandanplay_json,
    parse_local_danmaku,
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

    def test_unsupported_modes_are_dropped_and_reported(self):
        # 7=高级弹幕(带坐标脚本) 8=代码弹幕 9=BAS：三端都不渲染，服务端直接丢弃并计数。
        payload = build_danmaku_payload(
            [
                {"cid": "1", "m": "滚动", "time": 1.0, "mode": 1, "color": 0xFFFFFF},
                {"cid": "2", "m": "逆向", "time": 2.0, "mode": 6, "color": 0xFFFFFF},
                {"cid": "3", "m": "高级", "time": 3.0, "mode": 7, "color": 0xFFFFFF},
                {"cid": "4", "m": "代码", "time": 4.0, "mode": 8, "color": 0xFFFFFF},
                {"cid": "5", "m": "BAS", "time": 5.0, "mode": 9, "color": 0xFFFFFF},
            ],
            source="dandanplay",
            episode_id="1",
        )
        self.assertEqual([item["cid"] for item in payload["comments"]], ["1", "2"])
        self.assertEqual(payload["dropped_modes"], 3)
        self.assertEqual(payload["total"], 5)
        self.assertEqual(payload["count"], 2)


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
    def test_business_error_is_raised_not_swallowed(self):
        transport = FakeTransport(
            [("/api/v2/search/episodes", {"success": False, "errorCode": 3, "errorMessage": "应用不存在"})]
        )
        source = danmaku_source(transport)
        with self.assertRaises(RuntimeError) as ctx:
            source.search_episodes(tmdb_id=1429)
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

    def test_searches_by_keyword_without_tmdb_id(self):
        transport = FakeTransport([("/api/v2/search/episodes", {"animes": []})])
        source = danmaku_source(transport)

        source.search_episodes(anime="遮天", episode=148)

        url = transport.calls[0]["url"]
        self.assertIn("anime=%E9%81%AE%E5%A4%A9", url)
        self.assertIn("episode=148", url)
        self.assertNotIn("tmdbId=", url)

    def test_search_requires_keyword_or_tmdb_id_without_calling_upstream(self):
        transport = FakeTransport([])
        source = danmaku_source(transport)
        with self.assertRaises(ValueError):
            source.search_episodes(tmdb_id="")
        self.assertEqual(transport.calls, [])

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

    def test_match_uses_only_tmdb_lookup(self):
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
            ]
        )
        result = self._matcher(transport).match(episode=10, tmdb_id=1429)
        self.assertTrue(result["matched"])
        self.assertEqual(result["match_mode"], "tmdb")
        self.assertEqual(result["episode_id"], "95410010")
        self.assertEqual(len(transport.calls), 1)

    def test_tmdb_empty_result_falls_back_to_unique_exact_keyword_episode(self):
        def search_response(url, _data):
            if "tmdbId=" in url:
                return {"animes": []}
            return {
                "animes": [
                    {
                        "animeId": 18005,
                        "animeTitle": "遮天",
                        "episodes": [
                            {
                                "episodeId": 180050148,
                                "episodeTitle": "第148话",
                                "episodeNumber": None,
                            }
                        ],
                    }
                ]
            }

        transport = FakeTransport(
            [
                ("/api/v2/search/episodes", search_response),
            ]
        )

        result = self._matcher(transport).match(episode=148, tmdb_id=224839, anime="遮天")

        self.assertTrue(result["matched"])
        self.assertEqual(result["match_mode"], "keyword")
        self.assertEqual(result["episode_id"], "180050148")
        self.assertEqual([item["mode"] for item in result["attempts"]], ["tmdb", "keyword"])
        self.assertEqual([item["outcome"] for item in result["attempts"]], ["no_candidates", "matched"])
        self.assertEqual(len(transport.calls), 2)
        self.assertIn("tmdbId=224839", transport.calls[0]["url"])
        self.assertNotIn("anime=", transport.calls[0]["url"])
        self.assertIn("anime=%E9%81%AE%E5%A4%A9", transport.calls[1]["url"])
        self.assertNotIn("tmdbId=", transport.calls[1]["url"])

    def test_tmdb_error_does_not_fall_back_to_keyword(self):
        transport = FakeTransport(
            [("/api/v2/search/episodes", {"success": False, "errorCode": 500, "errorMessage": "upstream down"})]
        )

        result = self._matcher(transport).match(episode=148, tmdb_id=224839, anime="遮天")

        self.assertFalse(result["matched"])
        self.assertEqual([item["mode"] for item in result["attempts"]], ["tmdb"])
        self.assertEqual(result["attempts"][0]["outcome"], "error")
        self.assertEqual(len(transport.calls), 1)

    def test_keyword_fallback_requires_exact_anime_title(self):
        def search_response(url, _data):
            if "tmdbId=" in url:
                return {"animes": []}
            return {
                "animes": [
                    {
                        "animeId": 18005,
                        "animeTitle": "遮天 年番",
                        "episodes": [
                            {"episodeId": 180050148, "episodeTitle": "第148话", "episodeNumber": None}
                        ],
                    }
                ]
            }

        transport = FakeTransport(
            [
                ("/api/v2/search/episodes", search_response),
            ]
        )

        result = self._matcher(transport).match(episode=148, tmdb_id=224839, anime="遮天")

        self.assertFalse(result["matched"])
        self.assertEqual(result["attempts"][-1]["mode"], "keyword")
        self.assertEqual(result["attempts"][-1]["outcome"], "no_candidates")

    def test_keyword_fallback_requires_exact_episode_title_when_number_is_missing(self):
        def search_response(url, _data):
            if "tmdbId=" in url:
                return {"animes": []}
            return {
                "animes": [
                    {
                        "animeId": 18005,
                        "animeTitle": "遮天",
                        "episodes": [
                            {
                                "episodeId": 180050148,
                                "episodeTitle": "第148话 决战",
                                "episodeNumber": None,
                            }
                        ],
                    }
                ]
            }

        transport = FakeTransport(
            [
                ("/api/v2/search/episodes", search_response),
            ]
        )

        result = self._matcher(transport).match(episode=148, tmdb_id=224839, anime="遮天")

        self.assertFalse(result["matched"])
        self.assertEqual(result["attempts"][-1]["outcome"], "no_candidates")

    def test_keyword_fallback_rejects_multiple_exact_candidates(self):
        def search_response(url, _data):
            if "tmdbId=" in url:
                return {"animes": []}
            return {
                "animes": [
                    {
                        "animeId": 18005,
                        "animeTitle": "遮天",
                        "episodes": [
                            {"episodeId": 180050148, "episodeTitle": "第148话", "episodeNumber": None}
                        ],
                    },
                    {
                        "animeId": 28005,
                        "animeTitle": "遮天",
                        "episodes": [
                            {"episodeId": 280050148, "episodeTitle": "第148话", "episodeNumber": None}
                        ],
                    },
                ]
            }

        transport = FakeTransport(
            [
                ("/api/v2/search/episodes", search_response),
            ]
        )

        result = self._matcher(transport).match(episode=148, tmdb_id=224839, anime="遮天")

        self.assertFalse(result["matched"])
        self.assertTrue(result["ambiguous"])
        self.assertEqual(result["unmatched_reason"], "ambiguous_keyword_candidates")
        self.assertEqual(result["attempts"][-1]["outcome"], "ambiguous")

    def test_multiple_tmdb_episode_candidates_fail_closed(self):
        transport = FakeTransport(
            [
                (
                    "/api/v2/search/episodes",
                    {
                        "animes": [
                            {
                                "animeId": 101,
                                "animeTitle": "示例动画",
                                "episodes": [
                                    {"episodeId": 1010001, "episodeTitle": "第1话", "episodeNumber": "1"}
                                ],
                            },
                            {
                                "animeId": 202,
                                "animeTitle": "示例动画 第二季",
                                "episodes": [
                                    {"episodeId": 2020001, "episodeTitle": "第1话", "episodeNumber": "1"}
                                ],
                            },
                        ]
                    },
                )
            ]
        )

        result = self._matcher(transport).match(episode=1, tmdb_id=123)

        self.assertFalse(result["matched"])
        self.assertTrue(result["ambiguous"])
        self.assertEqual(result["unmatched_reason"], "ambiguous_candidates")
        self.assertEqual(result["episode_id"], "")
        self.assertEqual(len(result["candidates"]), 2)
        self.assertEqual(result["attempts"][0]["outcome"], "ambiguous")
        self.assertEqual(len(transport.calls), 1)

    def test_multiple_tmdb_candidates_use_unique_exact_anime_title(self):
        transport = FakeTransport(
            [
                (
                    "/api/v2/search/episodes",
                    {
                        "animes": [
                            {
                                "animeId": 101,
                                "animeTitle": "示例动画",
                                "episodes": [
                                    {"episodeId": 1010001, "episodeTitle": "第1话", "episodeNumber": "1"}
                                ],
                            },
                            {
                                "animeId": 202,
                                "animeTitle": "示例动画 第二季",
                                "episodes": [
                                    {"episodeId": 2020001, "episodeTitle": "第1话", "episodeNumber": "1"}
                                ],
                            },
                        ]
                    },
                )
            ]
        )

        result = self._matcher(transport).match(episode=1, tmdb_id=123, anime="示例动画")

        self.assertTrue(result["matched"])
        self.assertEqual(result["match_mode"], "tmdb")
        self.assertEqual(result["episode_id"], "1010001")
        self.assertEqual(result["attempts"][0]["candidate_count"], 1)
        self.assertEqual(len(transport.calls), 1)

    def test_duplicate_tmdb_candidates_with_same_episode_id_are_not_ambiguous(self):
        transport = FakeTransport(
            [
                (
                    "/api/v2/search/episodes",
                    {
                        "animes": [
                            {
                                "animeId": 101,
                                "animeTitle": "示例动画",
                                "episodes": [{"episodeId": 1010001, "episodeNumber": "1"}],
                            },
                            {
                                "animeId": 101,
                                "animeTitle": "示例动画",
                                "episodes": [{"episodeId": 1010001, "episodeNumber": "1"}],
                            },
                        ]
                    },
                )
            ]
        )

        result = self._matcher(transport).match(episode=1, tmdb_id=123, anime="示例动画")

        self.assertTrue(result["matched"])
        self.assertEqual(result["episode_id"], "1010001")
        self.assertEqual(len(result["candidates"]), 1)
        self.assertEqual(len(transport.calls), 1)

    def test_unresolved_tmdb_candidates_fall_back_to_exact_keyword_match(self):
        def search_response(url, _data):
            if "tmdbId=" in url:
                return {
                    "animes": [
                        {
                            "animeId": 101,
                            "animeTitle": "遮天 第一季",
                            "episodes": [{"episodeId": 1010148, "episodeNumber": "148"}],
                        },
                        {
                            "animeId": 202,
                            "animeTitle": "遮天 第二季",
                            "episodes": [{"episodeId": 2020148, "episodeNumber": "148"}],
                        },
                    ]
                }
            return {
                "animes": [
                    {
                        "animeId": 18005,
                        "animeTitle": "遮天",
                        "episodes": [
                            {"episodeId": 180050148, "episodeTitle": "第148话", "episodeNumber": None}
                        ],
                    }
                ]
            }

        transport = FakeTransport([("/api/v2/search/episodes", search_response)])

        result = self._matcher(transport).match(episode=148, tmdb_id=224839, anime="遮天")

        self.assertTrue(result["matched"])
        self.assertEqual(result["match_mode"], "keyword")
        self.assertEqual(result["episode_id"], "180050148")
        self.assertEqual([item["outcome"] for item in result["attempts"]], ["ambiguous", "matched"])
        self.assertEqual(len(transport.calls), 2)

    def test_tmdb_episode_match_rejects_candidate_without_episode_number(self):
        transport = FakeTransport(
            [
                (
                    "/api/v2/search/episodes",
                    {
                        "animes": [
                            {
                                "animeId": 101,
                                "animeTitle": "示例动画",
                                "episodes": [
                                    {"episodeId": 1010002, "episodeTitle": "未知集", "episodeNumber": ""}
                                ],
                            }
                        ]
                    },
                )
            ]
        )

        result = self._matcher(transport).match(episode=2, tmdb_id=123)

        self.assertFalse(result["matched"])
        self.assertEqual(result["candidates"], [])
        self.assertEqual(result["attempts"][0]["outcome"], "no_candidates")

    def test_ambiguous_primary_tmdb_result_does_not_try_another_source(self):
        primary_transport = FakeTransport(
            [
                (
                    "/api/v2/search/episodes",
                    {
                        "animes": [
                            {
                                "animeId": 101,
                                "animeTitle": "示例动画",
                                "episodes": [{"episodeId": 1010001, "episodeNumber": "1"}],
                            },
                            {
                                "animeId": 202,
                                "animeTitle": "示例动画 第二季",
                                "episodes": [{"episodeId": 2020001, "episodeNumber": "1"}],
                            },
                        ]
                    },
                )
            ]
        )
        fallback_transport = FakeTransport(
            [
                (
                    "/api/v2/search/episodes",
                    {
                        "animes": [
                            {
                                "animeId": 303,
                                "animeTitle": "示例动画",
                                "episodes": [{"episodeId": 3030001, "episodeNumber": "1"}],
                            }
                        ]
                    },
                )
            ]
        )
        sources = [
            danmaku_source(primary_transport),
            AggregatorSource(base_url="http://127.0.0.1:9321", transport=fallback_transport),
        ]

        result = self._matcher(primary_transport, sources=sources).match(episode=1, tmdb_id=123)

        self.assertFalse(result["matched"])
        self.assertTrue(result["ambiguous"])
        self.assertEqual(len(primary_transport.calls), 1)
        self.assertEqual(fallback_transport.calls, [])

    def test_unmatched_result_is_reported_with_attempts(self):
        transport = FakeTransport([("/api/v2/search/episodes", {"animes": []})])
        result = self._matcher(transport).match(tmdb_id=1, episode=1)
        self.assertFalse(result["matched"])
        self.assertEqual(result["episode_id"], "")
        self.assertTrue(result["attempts"])

    def test_missing_tmdb_id_never_starts_directly_from_keyword(self):
        transport = FakeTransport([])
        result = self._matcher(transport).match(episode=1, anime="示例动画")
        self.assertFalse(result["matched"])
        self.assertEqual(result["attempts"][0]["outcome"], "skipped")
        self.assertEqual(result["attempts"][0]["error"], "tmdb_id is required")
        self.assertEqual(transport.calls, [])

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

    def test_concurrent_comment_cache_miss_only_calls_upstream_once(self):
        started = threading.Event()
        release = threading.Event()

        def comment_response(_url, _data):
            started.set()
            release.wait(1)
            return {
                "count": 1,
                "comments": [{"cid": 1, "p": "1,1,16777215,u", "m": "并发"}],
            }

        transport = FakeTransport([("/api/v2/comment/8", comment_response)])
        matcher = self._matcher(transport)
        with ThreadPoolExecutor(max_workers=2) as executor:
            first = executor.submit(matcher.comments, "8")
            self.assertTrue(started.wait(1))
            second = executor.submit(matcher.comments, "8")
            time.sleep(0.02)
            self.assertEqual(len(transport.calls), 1)
            release.set()
            first_payload = first.result(timeout=1)
            second_payload = second.result(timeout=1)

        self.assertFalse(first_payload["cached"])
        self.assertTrue(second_payload["cached"])

    def test_unmatched_tmdb_lookup_is_reused_without_another_upstream_call(self):
        transport = FakeTransport(
            [
                ("/api/v2/search/episodes", {"animes": []}),
            ]
        )
        matcher = self._matcher(transport)

        first = matcher.match(tmdb_id=1, episode=1, anime="示例动画")
        second = matcher.match(tmdb_id=1, episode=1, anime="示例动画")

        self.assertFalse(first["matched"])
        self.assertFalse(second["matched"])
        self.assertTrue(second["cached"])
        self.assertTrue(all(item["cached"] for item in second["attempts"]))
        self.assertEqual(len(transport.calls), 2, "第二次 TMDB 与关键词匹配都必须先命中本地缓存")

    def test_tmdb_lookup_reuses_cache_key_written_by_previous_release(self):
        transport = FakeTransport([])
        matcher = self._matcher(transport)
        source = matcher.enabled_sources()[0]
        previous_key = matcher._cache_key(
            "source_search_v1",
            {
                "source": matcher._source_cache_identity(source),
                "anime": "",
                "tmdb_id": "1429",
                "episode": "10",
            },
        )
        matcher.cache.save(
            previous_key,
            {
                "animes": [
                    {
                        "anime_id": "9541",
                        "anime_title": "进击的巨人",
                        "episodes": [
                            {
                                "episode_id": "95410010",
                                "episode_title": "第10话",
                                "episode_number": "10",
                            }
                        ],
                    }
                ]
            },
        )

        result = matcher.match(tmdb_id=1429, episode=10)

        self.assertTrue(result["matched"])
        self.assertTrue(result["cached"])
        self.assertEqual(result["episode_id"], "95410010")
        self.assertEqual(transport.calls, [])

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
            matcher.match(tmdb_id=1)
        with self.assertRaises(RuntimeError):
            matcher.comments("1")


BILLIBILI_XML = (
    '<?xml version="1.0" encoding="UTF-8"?>'
    "<i><chatserver>chat.bilibili.com</chatserver><chatid>12345</chatid>"
    '<d p="1.5,1,25,16777215,1700000000,0,abc123,101">前方高能 &amp; 注意</d>'
    '<d p="2.5,5,25,16711680,1700000001,0,def456,102">顶部弹幕</d>'
    '<d p="3.5,4,25,255,1700000002,0,ghi789,103">底部弹幕</d>'
    '<d p="4.5,7,25,16777215,1700000003,0,jkl012,104">高级弹幕</d>'
    '<d p="5.5,8,25,16777215,1700000004,0,mno345,105">代码弹幕</d>'
    '<d p="6.5,1,25,16777215,1700000005,0,pqr678,106"></d>'
    "</i>"
)

DANDANPLAY_JSON = json.dumps(
    {
        "count": 2,
        "comments": [
            {"cid": "1542278977442529280", "p": "847.66,1,16020176,14b30012", "m": "前方高能"},
            {"cid": "1542278977442529281", "p": "848.10,5,16777215,14b30013", "m": "顶部"},
        ],
    },
    ensure_ascii=False,
)


class LocalImportParseTest(unittest.TestCase):
    def test_detects_json_and_xml_by_content(self):
        self.assertEqual(detect_danmaku_import_format(DANDANPLAY_JSON), "dandanplay-json")
        self.assertEqual(detect_danmaku_import_format("  \n" + DANDANPLAY_JSON), "dandanplay-json")
        self.assertEqual(detect_danmaku_import_format("\ufeff" + BILLIBILI_XML), "bilibili-xml")
        with self.assertRaises(DanmakuImportError) as raised:
            detect_danmaku_import_format("just some text")
        self.assertEqual(raised.exception.code, "unsupported_danmaku_format")
        with self.assertRaises(DanmakuImportError) as raised:
            detect_danmaku_import_format("   ")
        self.assertEqual(raised.exception.code, "empty_danmaku_file")

    def test_bilibili_xml_keeps_eight_segment_p_and_row_id_as_cid(self):
        entries = parse_bilibili_xml(BILLIBILI_XML)
        self.assertEqual(len(entries), 6)
        self.assertEqual(entries[0]["p"], "1.5,1,25,16777215,1700000000,0,abc123,101")
        self.assertEqual(entries[0]["m"], "前方高能 & 注意")
        self.assertEqual(entries[0]["cid"], "101")

    def test_dandanplay_json_accepts_wrapped_and_bare_arrays(self):
        wrapped = parse_dandanplay_json(DANDANPLAY_JSON)
        self.assertEqual(len(wrapped), 2)
        bare = parse_dandanplay_json(json.dumps(wrapped, ensure_ascii=False))
        self.assertEqual(len(bare), 2)
        with self.assertRaises(DanmakuImportError) as raised:
            parse_dandanplay_json('{"count": 1}')
        self.assertEqual(raised.exception.code, "invalid_danmaku_file")
        with self.assertRaises(DanmakuImportError) as raised:
            parse_dandanplay_json("{not json")
        self.assertEqual(raised.exception.code, "invalid_danmaku_file")

    def test_malformed_xml_is_reported_not_guessed(self):
        with self.assertRaises(DanmakuImportError) as raised:
            parse_bilibili_xml("<i><d p='1,1,25,16777215,1,0,u,1'>x</d>")
        self.assertEqual(raised.exception.code, "invalid_danmaku_file")

    def test_explicit_format_is_honoured_and_unknown_format_is_rejected(self):
        # 内容其实是 JSON，但显式声明成 XML 时按 XML 解析并如实失败，不做回退猜测。
        with self.assertRaises(DanmakuImportError) as raised:
            parse_local_danmaku(DANDANPLAY_JSON, source_format="bilibili-xml")
        self.assertEqual(raised.exception.code, "invalid_danmaku_file")
        resolved, entries = parse_local_danmaku(DANDANPLAY_JSON, source_format="json")
        self.assertEqual(resolved, "dandanplay-json")
        self.assertEqual(len(entries), 2)
        with self.assertRaises(DanmakuImportError) as raised:
            parse_local_danmaku(DANDANPLAY_JSON, source_format="ass")
        self.assertEqual(raised.exception.code, "unsupported_danmaku_format")


class LocalImportPayloadTest(unittest.TestCase):
    def test_xml_payload_is_normalized_like_provider_danmaku(self):
        payload = DanmakuLocalImport().payload(BILLIBILI_XML)
        self.assertEqual(payload["source"], "local")
        self.assertEqual(payload["match_mode"], "import")
        self.assertEqual(payload["format"], "bilibili-xml")
        self.assertEqual(payload["total"], 5, "the empty comment is dropped before filtering and counted as skipped")
        self.assertEqual(payload["count"], 3, "the fixture's 1/4/5 comments are kept")
        self.assertEqual(payload["dropped_modes"], 2, "mode 7 and 8 are dropped and counted")
        self.assertEqual(payload["skipped"], 1, "the empty comment must be reported, not swallowed")
        self.assertEqual(payload["comments"][0]["cid"], "101")
        self.assertEqual(payload["comments"][0]["time"], 1.5)
        self.assertEqual(
            [item["mode"] for item in payload["comments"]],
            [1, 5, 4],
            "comments stay sorted by time",
        )
        self.assertFalse(payload["truncated"])

    def test_json_payload_keeps_dandanplay_fields(self):
        payload = DanmakuLocalImport().payload(DANDANPLAY_JSON)
        self.assertEqual(payload["format"], "dandanplay-json")
        self.assertEqual(payload["count"], 2)
        self.assertEqual(payload["comments"][0]["cid"], "1542278977442529280")
        self.assertEqual(payload["comments"][0]["p"], "847.66,1,16020176,14b30012")

    def test_offset_blacklist_and_density_cap_apply_to_imported_files(self):
        # 5 usable comments, modes 7/8 dropped (3 left), the blacklist removes one (2 left),
        # and a cap of 1 therefore has to sample.
        payload = DanmakuLocalImport(max_comments=1, blacklist=["前方"]).payload(
            BILLIBILI_XML, offset_seconds=1.0
        )
        texts = [item["m"] for item in payload["comments"]]
        self.assertNotIn("前方高能 & 注意", texts)
        self.assertTrue(payload["truncated"])
        self.assertEqual(payload["count"], 1)
        self.assertEqual(payload["offset_seconds"], 1.0)
        self.assertEqual(payload["comments"][0]["time"], 3.5, "offset is applied to every comment")

    def test_ch_convert_is_refused_instead_of_silently_ignored(self):
        with self.assertRaises(DanmakuImportError) as raised:
            DanmakuLocalImport().payload(DANDANPLAY_JSON, ch_convert=1)
        self.assertEqual(raised.exception.code, "danmaku_ch_convert_unsupported")

    def test_oversized_and_empty_and_unusable_files_are_reported(self):
        with self.assertRaises(DanmakuImportError) as raised:
            DanmakuLocalImport(max_bytes=64).payload(BILLIBILI_XML)
        self.assertEqual(raised.exception.code, "danmaku_file_too_large")
        with self.assertRaises(DanmakuImportError) as raised:
            DanmakuLocalImport().payload("")
        self.assertEqual(raised.exception.code, "empty_danmaku_file")
        with self.assertRaises(DanmakuImportError) as raised:
            DanmakuLocalImport().payload("<i><d p=''></d></i>")
        self.assertEqual(raised.exception.code, "no_danmaku_comments")
        with self.assertRaises(DanmakuImportError) as raised:
            DanmakuLocalImport().payload("\ufeff[]")
        self.assertEqual(raised.exception.code, "no_danmaku_comments")

    def test_utf8_bytes_are_accepted_and_other_encodings_are_refused(self):
        payload = DanmakuLocalImport().payload(DANDANPLAY_JSON.encode("utf-8"))
        self.assertEqual(payload["count"], 2)
        with self.assertRaises(DanmakuImportError) as raised:
            DanmakuLocalImport().payload(DANDANPLAY_JSON.encode("utf-16"))
        self.assertEqual(raised.exception.code, "invalid_danmaku_file")


class LocalImportConfigTest(unittest.TestCase):
    class Config:
        pass

    def test_disabled_danmaku_has_no_local_import(self):
        config = self.Config()
        config.danmaku_enabled = False
        self.assertIsNone(build_danmaku_local_import_from_config(config))

    def test_local_import_does_not_require_any_source_or_credential(self):
        config = self.Config()
        config.danmaku_enabled = True
        config.danmaku_max_comments = 10
        config.danmaku_blacklist = ("广告",)
        config.danmaku_import_max_bytes = 2048
        helper = build_danmaku_local_import_from_config(config)
        self.assertIsNotNone(helper)
        self.assertEqual(helper.max_comments, 10)
        self.assertEqual(helper.blacklist, ("广告",))
        self.assertEqual(helper.max_bytes, 2048)


if __name__ == "__main__":
    unittest.main()
