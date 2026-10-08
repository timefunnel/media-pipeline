import concurrent.futures
import json
import sys
import tempfile
import threading
import time
import unittest
import urllib.error
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'app'))
from danmaku.circuit import SOURCE_COOLDOWN_SECONDS, SourceCircuitBreaker, SourceCircuitOpenError, SourceRiskControlError
from danmaku.config import Config
from danmaku.core import DanmakuCache, DanmakuMatcher
from danmaku.server import Application
from danmaku.transport import DanmakuHttpTransport

IDENTITY = {'name': 'iqiyi', 'base_url': 'fake-iqiyi'}
TARGET = {'title': '测试作品', 'tmdb_id': '123', 'season': 1, 'episode': 1}


class Source:
    def __init__(self, name='iqiyi'):
        self.name, self.base_url = name, 'fake-' + name
        self.search_calls = self.comment_calls = 0
        self.error = None

    def enabled(self):
        return True

    def search_target(self, target):
        self.search_calls += 1
        if self.error:
            raise self.error
        return [{'anime_title': target['title'], 'episodes': [
            {'episode_id': str(target['episode']), 'episode_number': str(target['episode'])}]}]

    def comment(self, episode_id, **kwargs):
        self.comment_calls += 1
        if self.error:
            raise self.error
        return {'comments': [{'cid': '1', 'p': '1,1,16777215,user', 'm': '弹幕', 'time': 1,
                              'mode': 1, 'color': 16777215, 'size': 25, 'user': 'user'}]}


class SourceCircuitTest(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.cache = DanmakuCache(directory.name)
        self.now = 10000
        self.breaker = SourceCircuitBreaker(self.cache, clock=lambda: self.now)

    def open(self):
        with self.assertRaises(SourceRiskControlError):
            self.breaker.call(IDENTITY, mock.Mock(side_effect=SourceRiskControlError('http_429')))

    def test_fixed_thirty_minutes_blocks_only_the_affected_source(self):
        self.assertEqual(SOURCE_COOLDOWN_SECONDS, 1800)
        self.open()
        loader = mock.Mock(return_value={'comments': []})
        with self.assertRaises(SourceCircuitOpenError) as caught:
            self.breaker.call(IDENTITY, loader)
        self.assertEqual(caught.exception.retry_after_seconds, 1800)
        self.now += 1799
        with self.assertRaises(SourceCircuitOpenError) as caught:
            self.breaker.call(IDENTITY, loader)
        self.assertEqual(caught.exception.retry_after_seconds, 1)
        loader.assert_not_called()
        other = {**IDENTITY, 'name': 'youku'}
        self.assertEqual(self.breaker.call(other, loader), {'comments': []})
        # 同名但不同回源地址也不共享风控状态。
        self.assertEqual(self.breaker.call({**IDENTITY, 'base_url': 'another-origin'}, loader), {'comments': []})
        self.now += 1
        self.assertEqual(self.breaker.call(IDENTITY, loader), {'comments': []})
        self.assertEqual(self.breaker.call(IDENTITY, loader), {'comments': []})

    def test_only_one_half_open_probe_is_allowed_and_success_restores_normal_calls(self):
        self.open()
        self.now += 1800
        started, release = threading.Event(), threading.Event()
        def probe():
            started.set()
            if not release.wait(3):
                raise RuntimeError('test probe release timed out')
            return 'recovered'
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            future = executor.submit(self.breaker.call, IDENTITY, probe)
            try:
                self.assertTrue(started.wait(3))
                def blocked():
                    with self.assertRaises(SourceCircuitOpenError) as caught:
                        self.breaker.call(IDENTITY, lambda: self.fail('second probe started'))
                    self.assertEqual(caught.exception.state, 'half_open')
                list(executor.map(lambda _: blocked(), range(7)))
            finally:
                release.set()
            self.assertEqual(future.result(), 'recovered')
        self.assertEqual(self.breaker.call(IDENTITY, lambda: 'normal'), 'normal')

    def test_failed_probe_restarts_thirty_minutes_even_for_an_ordinary_error(self):
        for error in (SourceRiskControlError('http_403'), RuntimeError('network failed')):
            self.open()
            self.now += 1800
            with self.assertRaises(type(error)):
                self.breaker.call(IDENTITY, mock.Mock(side_effect=error))
            with self.assertRaises(SourceCircuitOpenError) as caught:
                self.breaker.call(IDENTITY, lambda: self.fail('failed probe must cool down again'))
            self.assertEqual(caught.exception.retry_after_seconds, 1800)
            self.now += 1800

    def test_ordinary_closed_state_errors_and_successful_empty_results_do_not_open(self):
        for error in (RuntimeError('HTTP 500'), ValueError('season conflict')):
            with self.assertRaises(type(error)):
                self.breaker.call(IDENTITY, mock.Mock(side_effect=error))
            self.assertEqual(self.breaker.call(IDENTITY, lambda: []), [])
        self.assertEqual(list(self.cache.root_dir.iterdir()), [])

    def test_invalid_probe_input_does_not_extend_cooldown_or_leak_probe_lock(self):
        self.open()
        self.now += 1800
        with self.assertRaises(ValueError):
            self.breaker.call(IDENTITY, mock.Mock(side_effect=ValueError('unknown episode reference')))
        self.assertEqual(self.breaker.call(IDENTITY, lambda: 'valid probe'), 'valid probe')

    def test_restart_preserves_original_deadline_and_does_not_extend_on_denial(self):
        self.open()
        self.now += 900
        restarted = SourceCircuitBreaker(self.cache, clock=lambda: self.now)
        with self.assertRaises(SourceCircuitOpenError) as caught:
            restarted.call(IDENTITY, lambda: self.fail('restart must not reset cooldown'))
        self.assertEqual(caught.exception.retry_after_seconds, 900)
        self.now += 900
        self.assertEqual(restarted.call(IDENTITY, lambda: 'success'), 'success')
        self.assertEqual(SourceCircuitBreaker(self.cache, clock=lambda: self.now).call(IDENTITY, lambda: 'closed'), 'closed')

    def test_old_inflight_success_cannot_clear_newly_opened_circuit(self):
        started, release = threading.Event(), threading.Event()
        def inflight():
            started.set()
            if not release.wait(3):
                raise RuntimeError('test inflight release timed out')
            return 'success'
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(self.breaker.call, IDENTITY, inflight)
            try:
                self.assertTrue(started.wait(3))
                self.open()
            finally:
                release.set()
            self.assertEqual(future.result(), 'success')
        with self.assertRaises(SourceCircuitOpenError):
            self.breaker.check(IDENTITY)

    def test_invalid_persistent_state_and_write_failure_are_explicit(self):
        self.open()
        path = next(self.cache.root_dir.glob('source_circuit_v1_*.json'))
        for until in ('invalid', -1, True, float('nan')):
            with self.subTest(until=until):
                record = {'fetched_at': self.now, 'body': {'opened_until': until}}
                with mock.patch.object(Path, 'read_text', return_value=json.dumps(record)):
                    broken = SourceCircuitBreaker(self.cache, clock=lambda: self.now)
                    with self.assertRaisesRegex(RuntimeError, 'invalid persisted'):
                        broken.call(IDENTITY, lambda: self.fail('invalid cooldown was ignored'))
        with mock.patch.object(Path, 'read_text', return_value='not-json'):
            with self.assertRaisesRegex(RuntimeError, 'invalid persisted'):
                SourceCircuitBreaker(self.cache).check(IDENTITY)
        with mock.patch.object(self.cache, 'save', side_effect=RuntimeError('disk write failed')):
            self.now += 1800
            with self.assertRaisesRegex(RuntimeError, 'disk write failed'):
                self.breaker.call(IDENTITY, mock.Mock(side_effect=SourceRiskControlError('http_403')))
        with self.assertRaises(SourceCircuitOpenError):
            self.breaker.check(IDENTITY)
        self.assertTrue(path.is_file())


class SourceCircuitIntegrationTest(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.cache = DanmakuCache(directory.name)
        self.now = 10000
        self.config = Config(token='test-token-16-chars', danmaku_cache_dir=directory.name, danmaku_prewarm_delay_seconds=0)

    def matcher(self, sources):
        matcher = DanmakuMatcher(sources, cache=self.cache)
        matcher.circuit = SourceCircuitBreaker(self.cache, clock=lambda: self.now)
        self.addCleanup(matcher.close)
        return matcher

    def match(self, matcher, title='测试作品', episode=1):
        return matcher.match(tmdb_id='123', anime=title, season=1, episode=episode)

    def test_search_and_comment_cache_still_work_during_shared_cooldown(self):
        source = Source()
        matcher = self.matcher([source])
        self.assertTrue(self.match(matcher)['matched'])
        self.assertEqual(matcher.comments('1', source_name=source.name)['count'], 1)
        source.error = SourceRiskControlError('iqiyi_search_risk_control')
        self.assertFalse(self.match(matcher, title='另一作品')['matched'])
        self.assertTrue(self.match(matcher)['cached'])
        self.assertTrue(matcher.comments('1', source_name=source.name)['cached'])
        with self.assertRaises(SourceCircuitOpenError):
            matcher.comments('2', source_name=source.name)
        self.assertEqual((source.search_calls, source.comment_calls), (2, 1))
        source.error = None
        self.now += 1800
        # 错误未缓存成空结果，到期后相同目标会真正回源并恢复。
        self.assertTrue(self.match(matcher, title='另一作品')['matched'])
        self.assertEqual(source.search_calls, 3)

    def test_cold_targets_do_not_repeat_source_calls_but_other_priority_sources_remain_usable(self):
        first, second = Source('tencent'), Source('youku')
        first.error = SourceRiskControlError('http_429')
        matcher = self.matcher([first, second])
        for title in ('作品一', '作品二', '作品三'):
            result = self.match(matcher, title=title)
            self.assertTrue(result['matched'])
            self.assertEqual(result['source'], 'youku')
            self.assertEqual(result['attempts'][0]['outcome'], 'error')
        self.assertEqual((first.search_calls, second.search_calls), (1, 3))

    def test_comment_risk_blocks_search_and_search_risk_blocks_comments_after_restart(self):
        source = Source()
        source.error = SourceRiskControlError('http_403')
        matcher = self.matcher([source])
        with self.assertRaises(SourceRiskControlError):
            matcher.comments('1', source_name=source.name)
        matcher.close()
        restarted = self.matcher([source])
        result = self.match(restarted)
        self.assertFalse(result['matched'])
        self.assertIn('source_circuit_open', result['attempts'][0]['error'])
        self.assertEqual((source.search_calls, source.comment_calls), (0, 1))

    def test_protocol_search_is_guarded_and_does_not_cache_risk_as_empty(self):
        source = mock.Mock(name='protocol-source')
        source.name, source.base_url = 'aggregator', 'https://example.test'
        source.search_episodes.side_effect = SourceRiskControlError('http_429')
        matcher = self.matcher([source])
        with self.assertRaises(SourceRiskControlError):
            matcher._search_source(source, tmdb_id='123', episode=1)
        with self.assertRaises(SourceCircuitOpenError):
            matcher._search_source(source, tmdb_id='456', episode=2)
        self.assertEqual(source.search_episodes.call_count, 1)
        self.assertEqual(list(self.cache.root_dir.glob('source_search_v1_*.json')), [])

    def test_prewarm_different_episodes_share_cooldown_and_report_errors_not_empty(self):
        source = Source()
        source.error = SourceRiskControlError('http_429')
        app = Application(self.config, self.matcher([source]))
        app.prewarm.start()
        self.addCleanup(app.prewarm.stop)
        task = app.prewarm.submit('admin', 'test-season', 1, [
            {'media_id': str(index), 'target': {**TARGET, 'episode': index, 'title': '作品' + str(index)}}
            for index in range(1, 4)])
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            result = app.prewarm.get(task['task_id'])
            if result['status'] == 'completed':
                break
            threading.Event().wait(0.01)
        self.assertEqual((result['status'], result['processed'], result['failed'], result['empty']), ('completed', 3, 3, 0))
        self.assertEqual(source.search_calls, 1)
        self.assertIn('source_circuit_open', result['details'][1]['error'])

    def test_http_transport_403_and_429_are_typed_but_other_failures_are_not(self):
        transport = DanmakuHttpTransport()
        for status in (403, 429, 500):
            error = urllib.error.HTTPError('https://example.test/api', status, 'failed', {}, None)
            with mock.patch.object(transport.opener, 'open', side_effect=error) as request:
                with self.assertRaises(RuntimeError) as caught:
                    transport.json_request('GET', 'https://example.test/api')
                self.assertEqual(isinstance(caught.exception, SourceRiskControlError), status in (403, 429))
                request.assert_called_once()


if __name__ == '__main__':
    unittest.main()
