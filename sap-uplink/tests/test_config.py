import os
import unittest
from unittest import mock

from src.config import UplinkConfig

BASE_ENV = {
    "SAP_URL": "https://dev.sapsailing.com/",
    "SAP_BEARER_TOKEN": "token",
    "POCKETBASE_URL": "http://pocketbase:8090",
}


def load(**env):
    with mock.patch.dict(os.environ, {**BASE_ENV, **env}, clear=True):
        return UplinkConfig()


class ConfigTest(unittest.TestCase):
    def test_requires_races_or_explicit_all_races(self):
        # SAP would otherwise add the wind to every race the login may update
        with self.assertRaises(ValueError):
            load()
        self.assertTrue(load(SAP_ALL_RACES="true").all_races)

    def test_parses_races(self):
        config = load(SAP_RACES='[{"regattaName":"Windbot Test","raceName":"Windbot Test R1 Default"}]')
        self.assertEqual(config.races, [{"regattaName": "Windbot Test", "raceName": "Windbot Test R1 Default"}])
        self.assertEqual(config.sap_url, "https://dev.sapsailing.com")

    def test_parses_excluded_sources(self):
        self.assertEqual(load(SAP_ALL_RACES="true").exclude_sources, set())
        config = load(SAP_ALL_RACES="true", SAP_EXCLUDE_SOURCES=" bench, ,test_1")
        self.assertEqual(config.exclude_sources, {"bench", "test_1"})

    def test_rejects_non_positive_poll_interval(self):
        with self.assertRaises(ValueError):
            load(SAP_ALL_RACES="true", SAP_POLL_SECONDS="0")

    def test_rejects_url_without_scheme_or_with_sailingserver_path(self):
        for url in ("dev.sapsailing.com", "https://dev.sapsailing.com/sailingserver/"):
            with self.assertRaisesRegex(ValueError, "SAP_URL"):
                load(SAP_ALL_RACES="true", SAP_URL=url)
        with self.assertRaisesRegex(ValueError, "POCKETBASE_URL"):
            load(SAP_ALL_RACES="true", POCKETBASE_URL="pocketbase:8090")

    def test_errors_name_the_setting(self):
        for env in ({"SAP_POLL_SECONDS": "abc"}, {"SAP_RACES": "[{"}, {"SAP_START_FROM": "yesterday"}):
            name = next(iter(env))
            with self.assertRaisesRegex(ValueError, name):
                load(SAP_ALL_RACES="true", **env)

    def test_rejects_unknown_flag_value(self):
        with self.assertRaisesRegex(ValueError, "SAP_ALL_RACES"):
            load(SAP_ALL_RACES="on")
        self.assertFalse(load(SAP_RACES='[{"regattaName":"R","raceName":"1"}]', SAP_ALL_RACES="no").all_races)

    def test_rejects_race_without_name(self):
        with self.assertRaises(ValueError):
            load(SAP_RACES='[{"regattaName":"Windbot Test"}]')


if __name__ == "__main__":
    unittest.main()
