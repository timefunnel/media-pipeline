import os
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "app"))

for _category, _prefix in {
    "movie": "MEDIA_PIPELINE_MOVIE",
    "tv": "MEDIA_PIPELINE_TV",
    "anime": "MEDIA_PIPELINE_ANIME",
    "adult": "MEDIA_PIPELINE_ADULT",
    "other": "MEDIA_PIPELINE_OTHER",
}.items():
    for _suffix, _value in {
        "MSG_LIBRARY_ID": "test-%s-library" % _category,
        "MSG_ROOT_ID": "test-%s-root" % _category,
    }.items():
        os.environ[_prefix + "_" + _suffix] = _value

from pipeline.bot import BotConfig, CandidateStore, TelegramBot
from pipeline.pansou import PanSouClient, pansou_115_candidates
from pipeline.telegram_ui import format_library_choice_message, format_search_page_message


class FakeTelegram:
    def __init__(self):
        self.messages = []
        self.answers = []

    def send_message(self, chat_id, text, reply_markup=None):
        self.messages.append({"chat_id": chat_id, "text": text, "reply_markup": reply_markup})
        return {"ok": True, "result": {"message_id": 1000 + len(self.messages)}}

    def answer_callback_query(self, callback_query_id, text=None):
        self.answers.append({"callback_query_id": callback_query_id, "text": text})

    def send_chat_action(self, chat_id, action="typing"):
        return None


class FakePanSouService:
    def __init__(self, candidates):
        self.candidates = candidates
        self.calls = []

    def search_pansou(self, query, limit=100):
        self.calls.append((query, limit))
        return self.candidates


class FakePanSouTransport:
    def __init__(self, response):
        self.response = response
        self.calls = []

    def request(self, method, url, headers=None, data=None, timeout=None):
        self.calls.append({"method": method, "url": url, "headers": headers or {}, "data": data, "timeout": timeout})
        return self.response


class PanSouIntegrationTest(unittest.TestCase):
    def test_pansou_115_candidates_reads_merged_results(self):
        candidates = pansou_115_candidates(
            {
                "code": 0,
                "message": "success",
                "data": {
                    "merged_by_type": {
                        "115": [
                            {
                                "url": "https://115cdn.com/s/swabc123",
                                "password": "xy99",
                                "note": "低智商犯罪 2025",
                                "source": "tg:test",
                            }
                        ]
                    }
                },
            }
        )

        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0]["source_kind"], "115_share")
        self.assertEqual(candidates[0]["shareCode"], "swabc123")
        self.assertIn("password=xy99", candidates[0]["download_uri"])
        self.assertEqual(candidates[0]["title"], "低智商犯罪 2025")

    def test_pansou_client_requests_all_results_and_reads_tg_links(self):
        transport = FakePanSouTransport(
            {
                "code": 0,
                "message": "success",
                "data": {
                    "total": 1,
                    "results": [
                        {
                            "channel": "lgnb_fan",
                            "title": "【LGNB素鸡顶封】",
                            "content": "名称：速度与激情7 描述：动作电影",
                            "links": [
                                {
                                    "type": "115",
                                    "url": "https://115cdn.com/s/swsqs5n3h88",
                                    "password": "LGNB",
                                    "work_title": "【LGNB素鸡顶封】",
                                }
                            ],
                        }
                    ],
                },
            }
        )
        client = PanSouClient("http://pansou.local", transport=transport, timeout=7)

        candidates = client.search("速度与激情7", limit=5, cloud_types=("115",), source_type="all")

        self.assertEqual(transport.calls[0]["data"]["res"], "all")
        self.assertEqual(transport.calls[0]["data"]["cloud_types"], ["115"])
        self.assertEqual(candidates[0]["source_kind"], "115_share")
        self.assertEqual(candidates[0]["indexer"], "tg:lgnb_fan")
        self.assertEqual(candidates[0]["shareCode"], "swsqs5n3h88")
        self.assertIn("password=LGNB", candidates[0]["download_uri"])
        self.assertEqual(candidates[0]["title"], "速度与激情7 【LGNB素鸡顶封】")
        self.assertEqual(candidates[0]["pansou_channel"], "lgnb_fan")
        self.assertIn("动作电影", candidates[0]["pansou_summary"])

    def test_pansou_client_prioritizes_query_match_from_result_content(self):
        transport = FakePanSouTransport(
            {
                "code": 0,
                "message": "success",
                "data": {
                    "results": [
                        {
                            "channel": "vip115hot",
                            "title": "黑吃黑",
                            "content": "名称：黑吃黑",
                            "links": [
                                {
                                    "type": "115",
                                    "url": "https://115cdn.com/s/swbad111",
                                    "password": "x",
                                    "work_title": "黑吃黑",
                                }
                            ],
                        },
                        {
                            "channel": "lgnb_fan",
                            "title": "【LGNB素鸡顶封】",
                            "content": "【LGNB素鸡顶封】🎬 名称：速度与激情7📝 描述：动作电影",
                            "links": [
                                {
                                    "type": "115",
                                    "url": "https://115cdn.com/s/swsqs5n3h88",
                                    "password": "LGNB",
                                    "work_title": "【LGNB素鸡顶封】",
                                }
                            ],
                        },
                    ]
                },
            }
        )
        client = PanSouClient("http://pansou.local", transport=transport)

        candidates = client.search("速度与激情7", limit=2)

        self.assertEqual(candidates[0]["indexer"], "tg:lgnb_fan")
        self.assertEqual(candidates[0]["title"], "速度与激情7 【LGNB素鸡顶封】")
        self.assertEqual(candidates[0]["rank"], 1)

    def test_pansou_client_filters_zero_relevance_results(self):
        transport = FakePanSouTransport(
            {
                "code": 0,
                "data": {
                    "results": [
                        {
                            "channel": "noise",
                            "title": "回到未来2",
                            "content": "名称：回到未来2",
                            "links": [{"type": "115", "url": "https://115.com/s/noise111"}],
                        },
                        {
                            "channel": "exact",
                            "title": "鬼父",
                            "content": "名称：鬼父 完整版",
                            "links": [{"type": "115", "url": "https://115.com/s/exact111"}],
                        },
                    ]
                },
            }
        )

        candidates = PanSouClient("http://pansou.local", transport=transport).search("鬼父", limit=10)

        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0]["indexer"], "tg:exact")

    def test_pansou_client_accepts_all_separated_query_terms(self):
        transport = FakePanSouTransport(
            {
                "code": 0,
                "data": {
                    "results": [
                        {
                            "channel": "match",
                            "title": "清纯少女可爱写真",
                            "content": "名称：清纯少女可爱写真",
                            "links": [{"type": "115", "url": "https://115.com/s/match111"}],
                        },
                        {
                            "channel": "partial",
                            "title": "可爱写真",
                            "content": "名称：可爱写真",
                            "links": [{"type": "115", "url": "https://115.com/s/partial111"}],
                        },
                    ]
                },
            }
        )

        candidates = PanSouClient("http://pansou.local", transport=transport).search("可爱 清纯", limit=10)

        self.assertEqual([candidate["title"] for candidate in candidates], ["清纯少女可爱写真"])

    def test_pansou_client_extracts_lgnb_structured_fields(self):
        content = (
            "【LGNB素鸡顶封】🎬 名称：速度与激情7📝 描述：经历了紧张刺激的伦敦大战，多米尼克重新回归平静生活。"
            "🌏 国家：美国🔗 链接：115网盘🧩 版本：加长版"
            "🔊 音频：英语+中影国语+TVB粤语+日语"
            "💬 字幕：中英特效字幕+R3简繁中文字幕"
            "📄 文件名：Furious.7.2015.Extended.UHD.BluRay.REMUX.2160p.HEVC.DV.HDR.DTS-HD.MA.7.1.18Audios-LGNB@oSpecialCN"
            "🎞 资源类型：蓝光 REMUX🆔 TMDB：168259📁 大小：63.61 GB"
            "🏷 标签：#2160p / #速度与激情 / #动作 / #犯罪 / #惊悚❤️ 捐助LGNB？"
        )
        transport = FakePanSouTransport(
            {
                "code": 0,
                "message": "success",
                "data": {
                    "results": [
                        {
                            "channel": "lgnb_fan",
                            "title": "【LGNB素鸡顶封】",
                            "content": content,
                            "links": [
                                {
                                    "type": "115",
                                    "url": "https://115cdn.com/s/swsqs5n3h88",
                                    "password": "LGNB",
                                    "work_title": "【LGNB素鸡顶封】",
                                }
                            ],
                        }
                    ]
                },
            }
        )
        client = PanSouClient("http://pansou.local", transport=transport)

        candidate = client.search("速度与激情7", limit=1)[0]

        self.assertEqual(candidate["title"], "速度与激情7 【LGNB素鸡顶封】")
        self.assertEqual(candidate["pansou_size_text"], "63.61 GB")
        self.assertGreater(candidate["size"], 60 * 1024 * 1024 * 1024)
        self.assertEqual(candidate["pansou_fields"]["version"], "加长版")
        self.assertEqual(candidate["pansou_fields"]["resource_type"], "蓝光 REMUX")
        self.assertEqual(candidate["pansou_fields"]["tmdb"], "168259")
        self.assertIn("Furious.7.2015", candidate["pansou_fields"]["filename"])
        self.assertIn("中英特效字幕", candidate["pansou_fields"]["subtitles"])
        self.assertEqual(candidate["pansou_fields"]["tags"], "#2160p / #速度与激情 / #动作 / #犯罪 / #惊悚")

    def test_pansou_client_prefers_result_content_and_infers_title_features(self):
        title = "异形(七部合集)【4K.REMUX UHD原盘】【HDR&杜比视界】【国英多音轨】【简繁英双语特效字幕】"
        content = (
            "名称：%s描述：《异形七部合集》完整收录了 1979 年至 2024 年的全部异形系列电影。"
            "夸克：https://pan.quark.cn/s/e1432e99a1b2"
            "115：https://115cdn.com/s/swwctfu36ep?password=c565"
            "📁 大小：347GB🏷 标签：#异形 #UHD原盘 #4K #特效字幕投稿: @pinuo_bot"
        ) % title
        transport = FakePanSouTransport(
            {
                "code": 0,
                "message": "success",
                "data": {
                    "merged_by_type": {
                        "115": [
                            {
                                "url": "https://115cdn.com/s/swwctfu36ep",
                                "password": "c565",
                                "note": title,
                                "source": "tg:vip115hot",
                            }
                        ]
                    },
                    "results": [
                        {
                            "channel": "vip115hot",
                            "title": title,
                            "content": content,
                            "links": [
                                {
                                    "type": "115",
                                    "url": "https://115cdn.com/s/swwctfu36ep",
                                    "password": "c565",
                                    "work_title": title,
                                }
                            ],
                        }
                    ],
                },
            }
        )
        client = PanSouClient("http://pansou.local", transport=transport)

        candidate = client.search("异形", limit=1)[0]

        self.assertEqual(candidate["title"], title)
        self.assertEqual(candidate["pansou_size_text"], "347 GB")
        self.assertEqual(candidate["pansou_fields"]["resource_type"], "4K.REMUX UHD原盘")
        self.assertEqual(candidate["pansou_fields"]["audio"], "国英多音轨")
        self.assertEqual(candidate["pansou_fields"]["subtitles"], "简繁英双语特效字幕")
        self.assertEqual(candidate["pansou_fields"]["tags"], "#异形 #UHD原盘 #4K #特效字幕")

    def test_pansou_client_extracts_gimy_style_fields(self):
        content = (
            "🎬 电影：聊斋：魅首诡案 (2026)"
            "🍿 TMDB ID: 1677967"
            "🎭 类型: 奇幻,悬疑"
            "📂 分类: 华语电影"
            "🎞️ 质量: WEB-DL 2160p HDR10"
            "📦 文件: 1 个"
            "💾 大小: 7.48 GB"
            "👥 主演: 吴添豪,马丽亚"
            "📝 简介: 来自西域的舞姬阿离三年前惨死客乡。"
            "🔗 链接: https://115cdn.com/s/swfxnji36ty?password=v077#华语电影"
        )
        transport = FakePanSouTransport(
            {
                "code": 0,
                "message": "success",
                "data": {
                    "results": [
                        {
                            "channel": "gimy115",
                            "title": "🎬 电影：聊斋：魅首诡案 (2026)",
                            "content": content,
                            "links": [
                                {
                                    "type": "115",
                                    "url": "https://115cdn.com/s/swfxnji36ty",
                                    "password": "v077",
                                    "work_title": "🎬 电影：聊斋：魅首诡案 (2026)",
                                }
                            ],
                        }
                    ]
                },
            }
        )
        client = PanSouClient("http://pansou.local", transport=transport)

        candidate = client.search("聊斋", limit=1)[0]

        self.assertEqual(candidate["title"], "聊斋：魅首诡案 (2026)")
        self.assertEqual(candidate["pansou_size_text"], "7.48 GB")
        self.assertEqual(candidate["pansou_fields"]["tmdb"], "1677967")
        self.assertEqual(candidate["pansou_fields"]["resource_type"], "WEB-DL 2160p HDR10")
        self.assertEqual(candidate["pansou_fields"]["genre"], "奇幻,悬疑")
        self.assertEqual(candidate["pansou_fields"]["category"], "华语电影")

    def test_pansou_client_infers_size_quality_and_subtitles_from_plain_title(self):
        title = "速度与激情8部系列合集 1080P 中文字幕.46.15GB【电影系列合集】"
        content = title + " https://115.com/s/sw3rjki33cg# 访问码：1111"
        transport = FakePanSouTransport(
            {
                "code": 0,
                "message": "success",
                "data": {
                    "results": [
                        {
                            "channel": "vip115hot",
                            "title": title,
                            "content": content,
                            "links": [
                                {
                                    "type": "115",
                                    "url": "https://115.com/s/sw3rjki33cg",
                                    "password": "1111",
                                    "work_title": title,
                                }
                            ],
                        }
                    ]
                },
            }
        )
        client = PanSouClient("http://pansou.local", transport=transport)

        candidate = client.search("速度与激情", limit=1)[0]

        self.assertEqual(candidate["pansou_size_text"], "46.15 GB")
        self.assertEqual(candidate["pansou_fields"]["resource_type"], "1080P")
        self.assertEqual(candidate["pansou_fields"]["subtitles"], "中文字幕")

    def test_pansou_search_message_shows_share_details_without_115_lookup(self):
        candidate = {
            "title": "速度与激情7 【LGNB素鸡顶封】",
            "download_uri": "https://115cdn.com/s/swsqs5n3h88?password=LGNB",
            "indexer": "tg:lgnb_fan",
            "rank": 1,
            "source_kind": "115_share",
            "shareCode": "swsqs5n3h88",
            "sharePassword": "LGNB",
            "pansou_size_text": "63.61 GB",
            "pansou_summary": "经历了紧张刺激的伦敦大战，多米尼克和伙伴们重新回归平静生活。",
            "pansou_fields": {
                "version": "加长版",
                "audio": "英语+中影国语+TVB粤语+日语",
                "subtitles": "中英特效字幕+R3简繁中文字幕",
                "filename": "Furious.7.2015.Extended.UHD.BluRay.REMUX.2160p.HEVC.DV.HDR.DTS-HD.MA.7.1.18Audios-LGNB@oSpecialCN",
                "resource_type": "蓝光 REMUX",
                "tmdb": "168259",
                "tags": "#2160p / #速度与激情 / #动作 / #犯罪 / #惊悚",
                "size": "63.61 GB",
            },
        }

        text = format_search_page_message("速度与激情7", [(1, candidate)], 0, 1, 1, title="网盘搜索结果")
        choice_text = format_library_choice_message(candidate)

        self.assertIn("大小：63.61 GB  规格：蓝光 REMUX  版本：加长版", text)
        self.assertIn("来源：tg:lgnb_fan  类型：115分享", text)
        self.assertIn("文件：Furious.7.2015.Extended", text)
        self.assertIn("音频：英语+中影国语+TVB粤语+日语", text)
        self.assertIn("字幕：中英特效字幕+R3简繁中文字幕", text)
        self.assertIn("TMDB：168259", text)
        self.assertIn("标签：#2160p / #速度与激情", text)
        self.assertIn("分享：swsqs5n3h88  提取：LGNB", text)
        self.assertNotIn("摘要：", text)
        self.assertIn("链接类型：115分享", choice_text)

    def test_bot_pansou_button_creates_search_session(self):
        candidate = {
            "title": "低智商犯罪 2025",
            "download_uri": "https://115cdn.com/s/swabc123?password=xy99",
            "indexer": "tg:test",
            "seeders": None,
            "size": None,
            "rank": 1,
            "source_kind": "115_share",
            "shareCode": "swabc123",
        }
        with tempfile.TemporaryDirectory() as tmp:
            config = BotConfig(
                token="token",
                allowed_user_ids={1},
                state_db_path=os.path.join(tmp, "state.db"),
                pansou_enabled=True,
            )
            store = CandidateStore(config.state_db_path)
            session_id = store.save_search_session(1, 10, "movie", "低智商犯罪", [])
            telegram = FakeTelegram()
            service = FakePanSouService([candidate])
            bot = TelegramBot(config, telegram, store, service)

            bot.handle_update(
                {
                    "callback_query": {
                        "id": "cb-pansou",
                        "from": {"id": 1},
                        "message": {"chat": {"id": 10}, "message_id": 11},
                        "data": "pansou_search:%s" % session_id,
                    }
                }
            )

        self.assertEqual(service.calls, [("低智商犯罪", 100)])
        self.assertEqual(telegram.answers[-1]["text"], "正在搜索网盘")
        self.assertIn("网盘搜索结果", telegram.messages[-1]["text"])
        self.assertIn("低智商犯罪 2025", telegram.messages[-1]["text"])
        self.assertEqual(telegram.messages[-1]["reply_markup"]["inline_keyboard"][0][0]["callback_data"], "choose:1")


if __name__ == "__main__":
    unittest.main()
