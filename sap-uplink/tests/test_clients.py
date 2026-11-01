import unittest
from unittest import mock

import requests

from src.pocketbase_reader import PocketbaseReader
from src.sap_client import SapClient, SapError, SapRejectedError


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


class SapClientTest(unittest.TestCase):
    def test_put_wind_body_and_bearer_token(self):
        client = SapClient("https://dev.sapsailing.com", "EXPEDITION", bearer_token="tok")
        self.assertEqual(client.session.headers["Authorization"], "Bearer tok")
        client.session = mock.Mock()
        answers = [{"regattaNameAndRaceName": {"regattaName": "R", "raceName": "1"}, "answer": [True]}]
        client.session.put.return_value = response(json_body=answers)
        races = [{"regattaName": "R", "raceName": "1"}]

        self.assertEqual(client.put_wind("windbot1", [{"timepoint": 1}], races), answers)

        url = client.session.put.call_args.args[0]
        body = client.session.put.call_args.kwargs["json"]
        self.assertEqual(url, "https://dev.sapsailing.com/sailingserver/api/v1/wind/putWind")
        self.assertEqual(
            body,
            {
                "windData": [{"timepoint": 1}],
                "windSourceType": "EXPEDITION",
                "windSourceId": "windbot1",
                "regattaNamesAndRaceNames": races,
            },
        )

    def test_no_race_list_omits_field(self):
        client = SapClient("https://dev.sapsailing.com", "WEB", user="u", password="p")
        self.assertEqual(client.session.auth, ("u", "p"))
        client.session = mock.Mock()
        client.session.put.return_value = response(json_body=[])
        client.put_wind("windbot1", [{"timepoint": 1}])
        self.assertNotIn("regattaNamesAndRaceNames", client.session.put.call_args.kwargs["json"])

    def test_errors(self):
        client = SapClient("https://dev.sapsailing.com", "EXPEDITION", bearer_token="tok")
        client.session = mock.Mock()
        client.session.put.return_value = response(400, text="bad")
        with self.assertRaises(SapRejectedError):
            client.put_wind("windbot1", [])
        client.session.put.return_value = response(503, text="maintenance")
        with self.assertRaises(SapError):
            client.put_wind("windbot1", [])
        client.session.put.side_effect = requests.ConnectionError("offline")
        with self.assertRaises(SapError):
            client.put_wind("windbot1", [])


if __name__ == "__main__":
    unittest.main()
