import io
import threading
import time
import unittest
import urllib.error
from unittest.mock import patch

from pipeline.prowlarr import ProwlarrApiError, ProwlarrClient, ProwlarrSearchCache, ProwlarrTransport


class FakeTransport:
    def __init__(self, response):
        self.response = response
        self.calls = []

    def request(self, method, url, headers=None, data=None, timeout=None):
        self.calls.append(
            {
                "method": method,
                "url": url,
                "headers": headers or {},
                "data": data,
                "timeout": timeout,
            }
        )
        return self.response


class ProwlarrSearchCacheTest(unittest.TestCase):
    def test_reuses_successful_empty_results_across_clients(self):
        transport = FakeTransport([])
        cache = ProwlarrSearchCache(ttl_seconds=60)
        first = ProwlarrClient("http://127.0.0.1:9696", "prowlarr-key-value", transport=transport, search_cache=cache)
        second = ProwlarrClient("http://127.0.0.1:9696", "prowlarr-key-value", transport=transport, search_cache=cache)

        self.assertEqual(first.search("SCUTE-550", limit=100, indexer_ids=[14], categories=[6000]), [])
        self.assertEqual(second.search("SCUTE-550", limit=100, indexer_ids=[14], categories=[6000]), [])

        self.assertEqual(len(transport.calls), 1)

    def test_does_not_reuse_expired_results(self):
        now = [100.0]
        transport = FakeTransport([])
        cache = ProwlarrSearchCache(ttl_seconds=60, clock=lambda: now[0])
        client = ProwlarrClient("http://127.0.0.1:9696", "prowlarr-key-value", transport=transport, search_cache=cache)

        client.search("SCUTE-550", indexer_ids=[14])
        now[0] += 61
        client.search("SCUTE-550", indexer_ids=[14])

        self.assertEqual(len(transport.calls), 2)

    def test_coalesces_concurrent_searches_across_clients(self):
        entered = threading.Event()
        release = threading.Event()

        class BlockingTransport(FakeTransport):
            def request(self, method, url, headers=None, data=None, timeout=None):
                self.calls.append({"method": method, "url": url, "timeout": timeout})
                entered.set()
                if not release.wait(1):
                    raise TimeoutError("test transport was not released")
                return self.response

        transport = BlockingTransport([{"title": "BT4G result"}])
        cache = ProwlarrSearchCache(ttl_seconds=60)
        first = ProwlarrClient("http://127.0.0.1:9696", "key", transport=transport, timeout=1, search_cache=cache)
        second = ProwlarrClient("http://127.0.0.1:9696", "key", transport=transport, timeout=1, search_cache=cache)
        results = []

        leader = threading.Thread(target=lambda: results.append(first.search("IPZZ-912", limit=200, indexer_ids=[14], categories=[6000, 2000, 5000])))
        follower = threading.Thread(target=lambda: results.append(second.search("IPZZ-912", limit=200, indexer_ids=[14], categories=[6000, 2000, 5000])))
        leader.start()
        self.assertTrue(entered.wait(1))
        follower.start()
        time.sleep(0.02)
        release.set()
        leader.join(1)
        follower.join(1)

        self.assertFalse(leader.is_alive())
        self.assertFalse(follower.is_alive())
        self.assertEqual(results, [[{"title": "BT4G result"}], [{"title": "BT4G result"}]])
        self.assertEqual(len(transport.calls), 1)

    def test_waiter_timeout_does_not_cancel_leading_search(self):
        entered = threading.Event()
        release = threading.Event()

        class BlockingTransport(FakeTransport):
            def request(self, method, url, headers=None, data=None, timeout=None):
                self.calls.append({"method": method, "url": url, "timeout": timeout})
                entered.set()
                release.wait(1)
                return self.response

        transport = BlockingTransport([])
        cache = ProwlarrSearchCache(ttl_seconds=60)
        leader_client = ProwlarrClient("http://127.0.0.1:9696", "key", transport=transport, timeout=1, search_cache=cache)
        waiter_client = ProwlarrClient("http://127.0.0.1:9696", "key", transport=transport, timeout=0.01, search_cache=cache)
        leader = threading.Thread(target=lambda: leader_client.search("IPZZ-912", indexer_ids=[14]))
        leader.start()
        self.assertTrue(entered.wait(1))

        with self.assertRaisesRegex(TimeoutError, "in-flight Prowlarr search"):
            waiter_client.search("IPZZ-912", indexer_ids=[14])

        release.set()
        leader.join(1)
        self.assertFalse(leader.is_alive())
        self.assertEqual(cache.get("http://127.0.0.1:9696/api/v1/search?query=IPZZ-912&limit=20&indexerIds=14"), (True, []))
        self.assertEqual(len(transport.calls), 1)

    def test_uses_dedicated_timeout_only_for_a_single_configured_indexer(self):
        transport = FakeTransport([])
        client = ProwlarrClient("http://127.0.0.1:9696", "key", transport=transport, timeout=10)
        client.set_indexer_search_timeout([14], 65)

        client.search("IPZZ-912", indexer_ids=[14])
        client.search("IPZZ-912", indexer_ids=[15])
        client.search("IPZZ-912", indexer_ids=[14, 15])

        self.assertEqual([call["timeout"] for call in transport.calls], [65.0, 10.0, 10.0])


class ProwlarrTransportTest(unittest.TestCase):
    def test_exposes_structured_prowlarr_error_message(self):
        error = urllib.error.HTTPError(
            "http://127.0.0.1:9696/api/v1/search",
            400,
            "Bad Request",
            {},
            io.BytesIO(b'{"message":"Search failed due to all selected indexers being unavailable"}'),
        )

        with patch("urllib.request.urlopen", side_effect=error):
            with self.assertRaisesRegex(ProwlarrApiError, "all selected indexers being unavailable") as raised:
                ProwlarrTransport().request("GET", "http://127.0.0.1:9696/api/v1/search")

        self.assertEqual(raised.exception.status_code, 400)


if __name__ == "__main__":
    unittest.main()
