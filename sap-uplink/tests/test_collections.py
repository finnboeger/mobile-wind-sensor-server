import unittest

from src.collections import ensure_collections


class FakeStore:
    def __init__(self, collections):
        self.collections = collections
        self.saved = []

    def get_collection(self, name):
        return self.collections.get(name)

    def save_collection(self, name, data):
        self.saved.append((name, data))


class EnsureCollectionsTest(unittest.TestCase):
    def test_creates_missing_collection(self):
        store = FakeStore({})
        ensure_collections(store)
        name, data = store.saved[0]
        self.assertIsNone(name)
        self.assertEqual((data["name"], data["listRule"]), ("sap_settings", None))

    def test_adds_missing_fields_and_keeps_existing_ones(self):
        existing = [{"id": "f1", "name": "id", "type": "text"}, {"id": "f2", "name": "sap_url", "type": "text"}]
        store = FakeStore({"sap_settings": {"fields": existing, "indexes": []}})
        ensure_collections(store)
        name, data = store.saved[0]
        self.assertEqual(name, "sap_settings")
        self.assertEqual(data["fields"][:2], existing)
        self.assertIn("access_token", [field["name"] for field in data["fields"]])
        self.assertEqual(sum(field["name"] == "sap_url" for field in data["fields"]), 1)


if __name__ == "__main__":
    unittest.main()
