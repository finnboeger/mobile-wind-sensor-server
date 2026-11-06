import unittest
from types import SimpleNamespace

from src.pocketbase_store import PocketbaseStore


def response(status, body=None):
    def raise_for_status():
        if status >= 400:
            raise RuntimeError(status)

    return SimpleNamespace(status_code=status, json=lambda: body or {}, raise_for_status=raise_for_status)


class FakeSession:
    def __init__(self, answers):
        self.answers = answers
        self.logins = 0
        self.headers = []

    def post(self, url, json, timeout):
        self.logins += 1
        return response(200, {"token": f"token{self.logins}"})

    def request(self, method, url, headers, timeout, **kwargs):
        self.headers.append(headers["Authorization"])
        return self.answers.pop(0)


class PocketbaseStoreTest(unittest.TestCase):
    def make_store(self, answers):
        store = PocketbaseStore("http://pb", "admin@example.com", "secret")
        store.session = FakeSession(answers)
        return store

    def test_logs_in_again_when_the_token_is_refused(self):
        store = self.make_store([response(401), response(200, {"id": "a"})])
        self.assertEqual(store.get("c", "a"), {"id": "a"})
        self.assertEqual(store.session.headers, ["token1", "token2"])

    def test_upsert_creates_a_missing_record(self):
        store = self.make_store([response(404), response(200, {"id": "a"})])
        store.upsert("c", "a", {"x": 1})
        self.assertEqual(store.session.logins, 1)
        self.assertEqual(store.session.answers, [])


if __name__ == "__main__":
    unittest.main()
