import os
import sys
import unittest
from datetime import datetime

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils import get_most_recent_monday


class TestGetMostRecentMonday(unittest.TestCase):
    def test_known_cases(self):
        cases = [
            (datetime(2024, 9, 2),  datetime(2024, 9, 2)),   # Monday  → same day
            (datetime(2024, 9, 3),  datetime(2024, 9, 2)),   # Tuesday → Mon 9/2
            (datetime(2024, 9, 4),  datetime(2024, 9, 2)),   # Wednesday
            (datetime(2024, 9, 5),  datetime(2024, 9, 2)),   # Thursday
            (datetime(2024, 9, 6),  datetime(2024, 9, 2)),   # Friday
            (datetime(2024, 9, 7),  datetime(2024, 9, 2)),   # Saturday
            (datetime(2024, 9, 8),  datetime(2024, 9, 2)),   # Sunday  → Mon 9/2 (was broken)
            (datetime(2024, 9, 9),  datetime(2024, 9, 9)),   # Monday  → same day
            (datetime(2024, 9, 10), datetime(2024, 9, 9)),   # Tuesday → Mon 9/9
        ]
        for input_date, expected in cases:
            with self.subTest(date=input_date.date()):
                result = get_most_recent_monday(input_date)
                self.assertEqual(
                    result, expected,
                    f"For {input_date.date()}: expected {expected.date()}, got {result.date()}",
                )

    def test_no_arg_returns_a_monday(self):
        result = get_most_recent_monday()
        self.assertEqual(result.weekday(), 0, "Result must be Monday (weekday 0)")


if __name__ == "__main__":
    unittest.main()
