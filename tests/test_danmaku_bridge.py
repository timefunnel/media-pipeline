import concurrent.futures
import shutil
import sys
import threading
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'app'))
from danmaku.bridge import BridgePool
from danmaku.config import Config
from danmaku.core import build_danmaku_matcher_from_config


@unittest.skipUnless(shutil.which('node'), 'Node.js is required for bridge process tests')
class DanmakuBridgeTest(unittest.TestCase):
    def setUp(self):
        self.pool = BridgePool(Path(__file__).parent / 'fixtures' / 'danmaku_bridge.mjs')
        self.addCleanup(self.pool.close)

    def call(self, action='ping', source='youku', data=None, timeout=5):
        return self.pool.call(action, source, data or {}, timeout)

    def test_sources_share_workers_and_warm_calls_reuse_process(self):
        first = self.call()
        for source in ('tencent', 'iqiyi', 'youku'):
            result = self.call(source=source)
            self.assertEqual(result['pid'], first['pid'])
            self.assertEqual(result['source'], source)
        matcher = build_danmaku_matcher_from_config(Config(danmaku_providers=('tencent', 'iqiyi', 'youku')))
        self.addCleanup(matcher.close)
        self.assertTrue(all(source.pool is matcher.sources[0].pool for source in matcher.sources))

    def test_concurrent_requests_have_at_most_three_processes_and_preserve_responses(self):
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            results = list(executor.map(lambda index: self.call(source=str(index), data={'delay': 20}), range(8)))
        self.assertEqual([result['source'] for result in results], [str(index) for index in range(8)])
        self.assertEqual(len({result['pid'] for result in results}), 3)

    def test_failure_does_not_retry_or_poison_the_next_request(self):
        for action in ('crash', 'invalid', 'error'):
            with self.subTest(action=action):
                pid = self.call()['pid']
                with self.assertRaises(RuntimeError):
                    self.call(action)
                self.assertNotEqual(self.call()['pid'], pid)

    def test_timeout_kills_process_before_next_request(self):
        pid = self.call()['pid']
        with self.assertRaisesRegex(RuntimeError, 'timed out'):
            self.call(data={'delay': 1000}, timeout=0.05)
        self.assertNotEqual(self.call()['pid'], pid)

    def test_queue_wait_consumes_the_same_timeout_budget(self):
        # 占满三个 worker，第四个请求不得在队列上额外等待一个完整处理超时。
        self.call()
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            barrier = threading.Barrier(4)
            def occupy():
                barrier.wait()
                return self.call(data={'delay': 200})
            futures = [executor.submit(occupy) for _ in range(3)]
            barrier.wait()
            # 等三个 worker 确实从池中取走；只等待本地状态，不发上游请求。
            for _ in range(100):
                if self.pool.available.empty():
                    break
                threading.Event().wait(0.005)
            with self.assertRaisesRegex(RuntimeError, 'queue timed out'):
                self.call(timeout=0.03)
            for future in futures:
                future.result()

    def test_shutdown_reaps_workers_and_rejects_new_requests(self):
        self.call()
        processes = [worker.process for worker in self.pool.workers if worker.process is not None]
        self.pool.close()
        self.pool.close()
        self.assertTrue(all(process.poll() is not None for process in processes))
        with self.assertRaisesRegex(RuntimeError, 'closed'):
            self.call()
