import unittest

from src.settings import SapSettings

ENV = SapSettings(sap_url="https://env.sapsailing.com", access_token="env-token", event_id="env-event", course_area="Default")


class MergedOverTest(unittest.TestCase):
    def test_empty_fields_fall_back_to_the_environment(self):
        self.assertEqual(SapSettings().merged_over(ENV), ENV)

    def test_env_token_is_not_sent_to_another_server(self):
        merged = SapSettings(sap_url="https://other.sapsailing.com").merged_over(ENV)
        self.assertEqual(merged.access_token, "")
        self.assertEqual(merged.problem(), "No access token set for https://other.sapsailing.com")

    def test_env_token_is_kept_for_the_same_server(self):
        merged = SapSettings(sap_url=ENV.sap_url, event_id="other").merged_over(ENV)
        self.assertEqual(merged.access_token, "env-token")

    def test_env_course_area_only_applies_to_the_env_event(self):
        self.assertEqual(SapSettings(event_id="other").merged_over(ENV).course_area, "")
        self.assertEqual(SapSettings(event_id="env-event").merged_over(ENV).course_area, "Default")
        self.assertEqual(SapSettings(course_area="Alpha").merged_over(ENV).course_area, "Alpha")


if __name__ == "__main__":
    unittest.main()
