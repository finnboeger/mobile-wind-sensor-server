import unittest
from unittest import mock

import requests

from src.pocketbase_reader import PocketbaseReader


def response(status=200, json_body=None, text=""):
    resp = mock.Mock(status_code=status, ok=200 <= status < 300, text=text)
    resp.json.return_value = json_body
    resp.raise_for_status.side_effect = None if resp.ok else requests.HTTPError(status)
    return resp


class PocketbaseReaderTest(unittest.TestCase):
    def test_lists_records_stored_after_cursor_in_arrival_order(self):
        reader = PocketbaseReader("http://pocketbase:8090")
        reader.session = mock.Mock()
        reader.session.get.return_value = response(json_body={"items": [{"id": "a"}]})

        items = reader.list_new("sensor_1", "2026-10-31 12:00:00.000Z", limit=50)

        self.assertEqual(items, [{"id": "a"}])
        url = reader.session.get.call_args.args[0]
        params = reader.session.get.call_args.kwargs["params"]
        self.assertEqual(url, "http://pocketbase:8090/api/collections/measurements/records")
        self.assertEqual(params["filter"], 'source_id = "sensor_1" && created_at > "2026-10-31 12:00:00.000Z"')
        self.assertEqual((params["sort"], params["perPage"]), ("created_at", 50))

    def test_range_pages_until_short_page(self):
        reader = PocketbaseReader("http://pocketbase:8090")
        reader.session = mock.Mock()
        reader.session.get.side_effect = [
            response(json_body={"items": [{"id": "a"}, {"id": "b"}]}),
            response(json_body={"items": [{"id": "c"}]}),
        ]
        pages = list(reader.iter_range("sensor_1", "from", "to", page_size=2))
        self.assertEqual(pages, [[{"id": "a"}, {"id": "b"}], [{"id": "c"}]])
        self.assertIn('ts <= "to"', reader.session.get.call_args.kwargs["params"]["filter"])


if __name__ == "__main__":
    unittest.main()
