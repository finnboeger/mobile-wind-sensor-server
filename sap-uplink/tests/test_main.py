import unittest
from contextlib import redirect_stderr
from io import StringIO

from src.main import parse_args


class ParseArgsTest(unittest.TestCase):
    def assert_rejected(self, argv, message):
        stderr = StringIO()
        with redirect_stderr(stderr), self.assertRaises(SystemExit):
            parse_args(argv)
        self.assertIn(message, stderr.getvalue())

    def test_backfill_range(self):
        args = parse_args(["--backfill", "2027-06-20T09:00:00Z", "--until", "2027-06-20T15:00:00Z"])
        self.assertEqual((args.end - args.start).total_seconds(), 6 * 3600)
        self.assertIsNone(parse_args([]).start)

    def test_rejects_bad_backfill_arguments(self):
        self.assert_rejected(["--until", "2027-06-20T15:00:00Z"], "--until needs --backfill")
        self.assert_rejected(["--backfill", "yesterday"], "--backfill must be an ISO time")
        self.assert_rejected(["--backfill", "2027-06-20T15:00:00Z", "--until", "2027-06-20T09:00:00Z"], "--until must be after --backfill")


if __name__ == "__main__":
    unittest.main()
