import os
import unittest
from unittest import mock

from src.config import UplinkConfig

BASE_ENV = {
    "SAP_URL": "https://dev.sapsailing.com/",
    "SAP_BEARER_TOKEN": "token",
    "SAP_SOURCES": "sensor_1=windbot1",
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

    def test_rejects_race_without_name(self):
        with self.assertRaises(ValueError):
            load(SAP_RACES='[{"regattaName":"Windbot Test"}]')


if __name__ == "__main__":
    unittest.main()
