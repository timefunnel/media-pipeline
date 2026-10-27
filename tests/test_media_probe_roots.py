import sys
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))

from pipeline.internal_api import ApiError, InternalApiApplication


class MediaProbeRootsTest(unittest.TestCase):
    def test_returns_existing_root_mapping_without_service_calls(self):
        application = InternalApiApplication(object(), object(), object())
        with mock.patch("pipeline.internal_api.configured_msg_library_roots", return_value={
            "movie": {"root_id": "root-a", "library_id": "library-a"}
        }), mock.patch("pipeline.internal_api.category_to_folder_id", return_value="700"), \
                mock.patch("pipeline.internal_api.category_to_openlist_path", return_value="/Synthetic"):
            self.assertEqual(application.media_probe_roots(), {"roots": [{
                "root_id": "root-a", "library_id": "library-a",
                "path": "cloud://openlist/Synthetic", "directory_id": "700",
            }]})

    def test_rejects_unconfigured_or_account_root(self):
        application = InternalApiApplication(object(), object(), object())
        for value in ("0", "REPLACE_WITH_CID", "-1", "１２３"):
            with self.subTest(value=value), mock.patch(
                "pipeline.internal_api.configured_msg_library_roots", return_value={
                    "movie": {"root_id": "root-a", "library_id": "library-a"}
                }
            ), mock.patch("pipeline.internal_api.category_to_folder_id", return_value=value):
                with self.assertRaises(ApiError):
                    application.media_probe_roots()

    def test_unselected_library_does_not_require_configuration(self):
        application = InternalApiApplication(object(), object(), object())
        with mock.patch("pipeline.internal_api.configured_msg_library_roots", return_value={
            "movie": {"root_id": "root-a", "library_id": "library-a"},
            "tv": {"root_id": "root-unused", "library_id": "library-unused"},
        }), mock.patch("pipeline.internal_api.category_to_folder_id", side_effect=lambda kind: "700" if kind == "movie" else "UNCONFIGURED"), \
                mock.patch("pipeline.internal_api.category_to_openlist_path", return_value="/Synthetic"):
            self.assertEqual(len(application.media_probe_roots(["root-a"])["roots"]), 1)


if __name__ == "__main__":
    unittest.main()
