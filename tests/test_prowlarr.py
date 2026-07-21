import io
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
