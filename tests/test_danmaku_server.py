import concurrent.futures
import json
import sys
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'app'))
from danmaku.config import Config
from danmaku.core import DanmakuCache, DanmakuMatcher, build_danmaku_matcher_from_config
from danmaku.native import NativeSource
from danmaku.server import Application, Server, validate_target

TARGET = {'title': '爱情公寓', 'original_title': 'iPartment', 'tmdb_id': '68809', 'season': 4, 'episode': 16}


class Source:
    base_url = 'fake'

    def __init__(self, name, count=1, error=False):
        self.name, self.count, self.error = name, count, error
        self.calls = self.comment_calls = 0

    def enabled(self):
        return True

    def search_target(self, target):
        self.calls += 1
        if self.error:
            raise RuntimeError('source failed')
        return [{'anime_title': target['title'], 'episodes': [
            {'episode_id': str(index + 1), 'episode_number': str(target['episode']), 'episode_title': '第16集'}
            for index in range(self.count)]}] if self.count else []

    def comment(self, episode_id, **kwargs):
        self.comment_calls += 1
        if self.error:
            raise RuntimeError('comment failed')
        return {'comments': [{'cid': '1', 'p': '1,1,16777215,user', 'm': '弹幕', 'time': 1, 'mode': 1,
                              'color': 16777215, 'size': 25, 'user': 'user'}]}


class ProtocolSource:
    """官方协议没有原生 search_target，不应进入并行预取。"""
    base_url = 'fake-official'
    enabled = Source.enabled
    comment = Source.comment

    def __init__(self):
        Source.__init__(self, 'dandanplay')

    def search_episodes(self, tmdb_id=None, episode=None, anime=None):
        self.calls += 1
        return [{'anime_title': '爱情公寓4', 'episodes': [
            {'episode_id': '1', 'episode_number': str(episode), 'episode_title': '第16集'}]}]


class DanmakuServerTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.cache = DanmakuCache(self.tmp.name)
        self.config = Config(token='test-token-16-chars', port=0, danmaku_cache_dir=self.tmp.name, danmaku_prewarm_delay_seconds=0)

    def matcher(self, sources):
        matcher = DanmakuMatcher(sources, cache=self.cache)
        self.addCleanup(matcher.close)
        return matcher

    def match(self, matcher):
        return matcher.match(tmdb_id='68809', anime='爱情公寓', season=4, episode=16)

    def test_cached_priority_hit_skips_later_search_and_reuses_comment_cache(self):
        first, second, last = Source('tencent'), Source('iqiyi'), ProtocolSource()
        matcher = self.matcher([first, second, last])
        self.assertTrue(self.match(matcher)['matched'])
        matcher.close()
        matcher = self.matcher([first, second, last])
        calls = (first.calls, second.calls, last.calls)
        self.assertTrue(self.match(matcher)['cached'])
        matcher.comments('1', source_name='tencent')
        self.assertTrue(matcher.comments('1', source_name='tencent')['cached'])
        self.assertEqual((first.calls, second.calls, last.calls), calls)
        self.assertEqual(first.comment_calls, 1)
        restarted = self.matcher([first, second, last])
        self.assertTrue(self.match(restarted)['cached'])
        self.assertEqual(first.calls, 1)
        self.assertEqual(last.calls, calls[2])

    def test_default_official_source_is_disabled_even_with_credentials(self):
        config = Config.from_env({'DANMAKU_SERVER_TOKEN': 'test-token-16-chars',
                                  'DANDANPLAY_APP_ID': 'test', 'DANDANPLAY_APP_SECRET': 'test'})
        self.assertEqual(config.danmaku_providers, ('tencent', 'iqiyi', 'youku'))
        matcher = build_danmaku_matcher_from_config(config)
        self.addCleanup(matcher.close)
        self.assertEqual([source.name for source in matcher.sources], ['tencent', 'iqiyi', 'youku'])
        Config.from_env({'DANMAKU_SERVER_TOKEN': 'test-token-16-chars'})
        with self.assertRaisesRegex(ValueError, 'disabled'):
            Config.from_env({'DANMAKU_SERVER_TOKEN': 'test-token-16-chars', 'DANMAKU_PROVIDERS': 'tencent,dandanplay',
                             'DANDANPLAY_APP_ID': 'test', 'DANDANPLAY_APP_SECRET': 'test'})

    def test_disabled_official_association_can_only_use_existing_comments_cache(self):
        matcher = build_danmaku_matcher_from_config(self.config)
        self.addCleanup(matcher.close)
        matcher.cache.save('comment_dandanplay_123_1_0', Source('dandanplay').comment('123'))
        with mock.patch('danmaku.core.DandanplayProtocolSource.comment', side_effect=AssertionError('disabled source requested')) as upstream:
            payload = matcher.comments('123', source_name='dandanplay')
            self.assertTrue(payload['cached'])
            self.assertEqual((payload['source'], payload['count']), ('dandanplay', 1))
            for kwargs in ({'episode_id': '124'}, {'episode_id': '123', 'with_related': False}):
                with self.assertRaisesRegex(RuntimeError, 'disabled'):
                    matcher.comments(source_name='dandanplay', **kwargs)
            with mock.patch('danmaku.core.time.time', return_value=time.time() + 604801):
                with self.assertRaisesRegex(RuntimeError, 'disabled'):
                    matcher.comments('123', source_name='dandanplay')
            upstream.assert_not_called()

    def test_official_source_runs_only_after_both_native_misses_and_is_cached(self):
        for second_count in (0, 1):
            with self.subTest(second_count=second_count), tempfile.TemporaryDirectory() as directory:
                sources = [Source('tencent', count=0), Source('iqiyi', count=second_count), ProtocolSource()]
                matcher = DanmakuMatcher(sources, cache=DanmakuCache(directory))
                result = self.match(matcher)
                matcher.close()
                self.assertEqual(result['source'], 'iqiyi' if second_count else 'dandanplay')
                self.assertEqual([source.calls for source in sources[:2]], [1, 1])
                self.assertEqual(sources[2].calls, 0 if second_count else 1)
                calls = [source.calls for source in sources]
                restarted = DanmakuMatcher(sources, cache=DanmakuCache(directory))
                try:
                    self.match(restarted)
                    self.assertEqual([source.calls for source in sources], calls)
                finally:
                    restarted.close()

    def test_youku_hit_stops_before_official_and_next_episode_uses_season_cache(self):
        sources = [Source('tencent', 0), Source('iqiyi', 0), Source('youku'), ProtocolSource()]
        sources[2].search_target = mock.Mock(return_value=[{'anime_title': '爱情公寓 第一季', 'episodes': [
            {'episode_id': '101', 'episode_number': '1'}, {'episode_id': '102', 'episode_number': '2'}]}])
        matcher = self.matcher(sources)
        for episode in (1, 2):
            result = matcher.match(tmdb_id='68809', anime='爱情公寓', season=1, episode=episode)
            self.assertEqual(result['source'], 'youku')
            self.assertEqual(result['episode_id'], str(100 + episode))
            self.assertEqual(result['cached'], episode == 2)
        matcher.close()
        self.assertEqual([source.calls for source in sources[:3]], [1, 1, 0])
        self.assertEqual(sources[2].search_target.call_count, 1)
        self.assertEqual(sources[3].calls, 0)
        app = Application(self.config, self.matcher(sources))
        result = app.match({'target': {**TARGET, 'season': 1, 'episode': 2}})
        self.assertEqual(result['match']['attempts'][0]['mode'], 'priority_v2')

    def test_no_match_and_ambiguity_allow_next_source(self):
        for count in (0, 2):
            with self.subTest(count=count), tempfile.TemporaryDirectory() as directory:
                first, second = Source('tencent', count=count), Source('iqiyi')
                matcher = DanmakuMatcher([first, second], cache=DanmakuCache(directory))
                try:
                    result = self.match(matcher)
                finally:
                    matcher.close()
                self.assertEqual(result['source'], 'iqiyi')
                self.assertEqual((first.calls, second.calls), (1, 1))

    def test_next_episode_reuses_whole_season_list_without_upstream_search(self):
        source = Source('tencent')
        source.search_target = mock.Mock(return_value=[{'anime_title': '爱情公寓4', 'episodes': [
            {'episode_id': '16', 'episode_number': '16'}, {'episode_id': '17', 'episode_number': '17'}]}])
        matcher = self.matcher([source])
        self.assertEqual(self.match(matcher)['episode_id'], '16')
        result = matcher.match(tmdb_id='68809', anime='爱情公寓', season=4, episode=17)
        self.assertEqual(result['episode_id'], '17')
        self.assertTrue(result['cached'])
        self.assertEqual(source.search_target.call_count, 1)

    def test_source_error_is_recorded_and_never_cached_as_no_match(self):
        broken = Source('tencent', error=True)
        app = Application(self.config, self.matcher([broken]))
        for _ in range(2):
            with self.assertRaisesRegex(RuntimeError, 'source failed'):
                app.match({'target': TARGET})
        self.assertEqual(broken.calls, 2)
        later = Source('iqiyi')
        result = self.match(self.matcher([broken, later]))
        self.assertEqual(result['source'], 'iqiyi')
        self.assertEqual(result['attempts'][0]['outcome'], 'error')

    def test_matched_source_comment_error_does_not_fall_back_to_later_source(self):
        first, second = Source('tencent'), Source('iqiyi')
        app = Application(self.config, self.matcher([first, second]))
        first.comment = mock.Mock(side_effect=RuntimeError('comment failed'))
        with self.assertRaisesRegex(RuntimeError, 'comment failed'):
            app.danmaku_comments('media', target=TARGET)
        self.assertEqual(second.comment_calls, 0)

    def test_concurrent_cache_miss_is_singleflight(self):
        first = Source('tencent')
        matcher = self.matcher([first])
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            results = list(executor.map(lambda _: self.match(matcher), range(8)))
        self.assertTrue(all(result['matched'] for result in results))
        self.assertEqual(first.calls, 1)

    def test_three_native_searches_overlap_and_selection_preserves_priority(self):
        sources = [Source('tencent'), Source('iqiyi'), Source('youku')]
        barrier = threading.Barrier(3)
        completion = []
        for index, source in enumerate(sources):
            original = source.search_target
            def search(target, index=index, original=original):
                barrier.wait(timeout=2)
                time.sleep((2 - index) * 0.03)
                completion.append(index)
                return original(target)
            source.search_target = search
        result = self.match(self.matcher(sources))
        self.assertEqual(result['source'], 'tencent')
        self.assertEqual(completion, [2, 1, 0])
        self.assertEqual([source.calls for source in sources], [1, 1, 1])

    def test_high_priority_hit_does_not_wait_for_running_lower_source(self):
        first, second = Source('tencent'), Source('iqiyi')
        entered, release = threading.Event(), threading.Event()
        original = second.search_target
        def slow(target):
            entered.set()
            if not release.wait(timeout=3):
                raise RuntimeError('test lower source timed out')
            return original(target)
        second.search_target = slow
        first_search = first.search_target
        first.search_target = lambda target: (entered.wait(timeout=2), first_search(target))[1]
        try:
            result = self.match(self.matcher([first, second]))
            self.assertEqual(result['source'], 'tencent')
            self.assertTrue(entered.is_set())
            self.assertFalse(release.is_set())
        finally:
            release.set()

    def test_cached_high_priority_hit_never_starts_cold_lower_sources(self):
        first = Source('tencent')
        self.match(self.matcher([first]))
        lower = [Source('iqiyi'), Source('youku')]
        result = self.match(self.matcher([first, *lower]))
        self.assertTrue(result['cached'])
        self.assertEqual([source.calls for source in lower], [0, 0])

    def test_all_sources_errors_are_explicit_and_not_negative_cached(self):
        sources = [Source(name, error=True) for name in ('tencent', 'iqiyi', 'youku')]
        app = Application(self.config, self.matcher(sources))
        for _ in range(2):
            with self.assertRaisesRegex(RuntimeError, 'source matching failed'):
                app.match({'target': TARGET})
        self.assertEqual([source.calls for source in sources], [2, 2, 2])

    def test_explicit_official_enable_and_segment_concurrency_validation(self):
        base = {'DANMAKU_SERVER_TOKEN': 'test-token-16-chars'}
        for bad in ('0', '9', '-1'):
            with self.subTest(bad=bad), self.assertRaisesRegex(ValueError, 'CONCURRENCY'):
                Config.from_env({**base, 'DANMAKU_SEGMENT_CONCURRENCY': bad})
        with self.assertRaisesRegex(ValueError, 'must be true or false'):
            Config.from_env({**base, 'DANMAKU_DANDANPLAY_ENABLED': 'invalid'})
        enabled = {**base, 'DANMAKU_PROVIDERS': 'dandanplay', 'DANMAKU_DANDANPLAY_ENABLED': 'true'}
        with self.assertRaisesRegex(ValueError, 'requires DANDANPLAY'):
            Config.from_env(enabled)
        config = Config.from_env({**enabled, 'DANDANPLAY_APP_ID': 'test', 'DANDANPLAY_APP_SECRET': 'test'})
        matcher = build_danmaku_matcher_from_config(config)
        self.addCleanup(matcher.close)
        self.assertEqual([source.name for source in matcher.sources], ['dandanplay'])

    def test_negative_cache_expires_before_positive_cache(self):
        matcher = self.matcher([Source('tencent')])
        self.cache.save('negative', {'animes': []})
        with mock.patch('danmaku.core.time.time', return_value=time.time() + 3601):
            value, cached = matcher._cached_call('negative', 86400, lambda: {'animes': ['new']})
        self.assertFalse(cached)
        self.assertEqual(value, {'animes': ['new']})

    def test_native_reference_is_persisted_and_reusable_without_search(self):
        source = NativeSource('iqiyi', self.cache, 'unused')
        self.addCleanup(source.close)
        with mock.patch.object(source, '_call', return_value=[{'animeTitle': '爱情公寓4', 'episodes': [
            {'episodeId': '123', 'episodeTitle': '第16集', 'episodeNumber': '16', 'url': 'https://www.iqiyi.com/v_19rrgzfn2k.html'}]}]):
            source.search_target(TARGET)
            with mock.patch.object(self.cache, 'save', side_effect=AssertionError('unchanged reference was rewritten')):
                source.search_target(TARGET)
        restarted = NativeSource('iqiyi', self.cache, 'unused')
        self.addCleanup(restarted.close)
        with mock.patch.object(restarted, '_call', return_value={'comments': []}) as call:
            restarted.comment('123')
            self.assertEqual(call.call_args.args[1]['url'], 'https://www.iqiyi.com/v_19rrgzfn2k.html')

    def test_known_season_rejects_wrong_or_unknown_season(self):
        matcher = self.matcher([Source('tencent')])
        animes = [{'anime_title': title} for title in ('爱情公寓', '爱情公寓3', '爱情公寓4', '爱情公寓 第四季', '另一个作品 第四季')]
        kept = matcher._filter_season(animes, 4, '爱情公寓', '', require_title=True)
        self.assertEqual([item['anime_title'] for item in kept], ['爱情公寓4', '爱情公寓 第四季'])

    def test_config_rejects_duplicate_unknown_and_uncredentialed_sources(self):
        for sources in ('tencent,tencent', 'unknown', 'dandanplay', 'aggregator'):
            with self.subTest(sources=sources), self.assertRaises(ValueError):
                Config.from_env({'DANMAKU_SERVER_TOKEN': 'test-token-16-chars', 'DANMAKU_PROVIDERS': sources})

    def test_target_requires_identified_exact_episode_metadata(self):
        for bad in ({}, {**TARGET, 'tmdb_id': ''}, {**TARGET, 'episode': 0}, {**TARGET, 'season': True}):
            with self.assertRaises(ValueError):
                validate_target(bad)

    def test_http_health_auth_match_parse_and_error_contracts(self):
        first, second = Source('tencent'), Source('iqiyi')
        app = Application(self.config, self.matcher([first, second]))
        server = Server(self.config, app)
        server.start()
        self.addCleanup(server.stop)
        base = 'http://127.0.0.1:%s' % server.httpd.server_address[1]

        def request(path, body=None, authorized=True):
            headers = {'Authorization': 'Bearer ' + self.config.token} if authorized else {}
            data = json.dumps(body).encode() if body is not None else None
            req = urllib.request.Request(base + path, data=data, headers=headers)
            try:
                response = urllib.request.urlopen(req, timeout=5)
            except urllib.error.HTTPError as error:
                response = error
            with response:
                return response.status, json.load(response)

        self.assertEqual(request('/healthz', authorized=False)[0], 200)
        self.assertEqual(request('/v1/danmaku/match', {'target': TARGET}, False)[0], 401)
        status, body = request('/v1/danmaku/match', {'media_id': 'm', 'target': TARGET})
        self.assertEqual(status, 200)
        self.assertEqual(body['match']['source'], 'tencent')
        self.assertEqual(second.comment_calls, 0)
        self.assertEqual(request('/v1/danmaku/match', {'target': {}})[0], 400)
        self.assertEqual(request('/v1/danmaku/parse', {'content': '<i><d p="1,1,25,16777215,0,0,user,1">hello</d></i>'})[1]['count'], 1)
        self.assertEqual(request('/unknown')[0], 404)
        self.assertEqual(request('/v1/danmaku/season/prewarm/missing')[0], 404)
        self.assertEqual(request('/v1/danmaku/comment', {'episode_id': '1', 'source': 'not-configured'})[0], 400)
        self.assertEqual(request('/v1/danmaku/comment', {'episode_id': '1', 'source': 'tencent', 'offset_seconds': 601})[0], 400)
        self.assertEqual(request('/v1/danmaku/parse', {'content': 'invalid'})[0], 400)
        self.assertEqual(request('/v1/danmaku/match', {'padding': 'a' * (256 * 1024)})[0], 413)
        status, task = request('/v1/danmaku/season/prewarm', {'owner_id': 'owner', 'media_id': 'm', 'season': 4,
            'episodes': [{'media_id': 'm', 'episode_key': 'S04E16', 'target': TARGET}]})
        self.assertEqual(status, 202)
        for _ in range(100):
            _, progress = request('/v1/danmaku/season/prewarm/' + task['task_id'])
            if progress['status'] in ('completed', 'failed'):
                break
            time.sleep(0.01)
        self.assertEqual(progress['status'], 'completed')
        self.assertEqual(progress['matched'], 1)
        self.assertEqual(second.comment_calls, 0)
        self.assertEqual(request('/v1/danmaku/season/prewarm?limit=1')[1]['items'][0]['task_id'], task['task_id'])
        self.assertEqual(request('/v1/danmaku/season/prewarm?limit=51')[0], 400)
        broken = Source('broken', error=True)
        app.matcher = self.matcher([broken])
        self.assertEqual(request('/v1/danmaku/match', {'target': TARGET})[0], 502)


if __name__ == '__main__':
    unittest.main()
