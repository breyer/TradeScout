"""
Integration-style tests for calculate_total_PL using an in-memory SQLite database.
No real TAT database file required.
"""
import os
import sys
import sqlite3
import unittest
from contextlib import contextmanager
from datetime import datetime
from unittest.mock import patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from PL_Summary import calculate_total_PL
from utils import FILETIME_EPOCH_OFFSET, FILETIME_TICKS_PER_SECOND


def _ft(dt: datetime) -> int:
    posix = (dt - datetime(1970, 1, 1)).total_seconds()
    return int(posix * FILETIME_TICKS_PER_SECOND + FILETIME_EPOCH_OFFSET)


def _make_db(entries) -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.execute("""
        CREATE TABLE DailyLog (
            DailyLogID INTEGER PRIMARY KEY,
            LogDate INTEGER,
            PL REAL,
            SPX REAL
        )
    """)
    for i, (dt, pl, spx) in enumerate(entries, start=1):
        conn.execute("INSERT INTO DailyLog VALUES (?, ?, ?, ?)", (i, _ft(dt), pl, spx))
    conn.commit()
    return conn


def _mock_connect(conn):
    @contextmanager
    def _cm(retries=5, delay=1):
        try:
            yield conn
        finally:
            conn.close()
    return _cm


class TestCalculateTotalPL(unittest.TestCase):
    def test_sums_three_days_correctly(self):
        conn = _make_db([
            (datetime(2024, 9, 23, 16, 0), 300.0, 5762.0),
            (datetime(2024, 9, 24, 16, 0), -100.0, 5800.0),
            (datetime(2024, 9, 25, 16, 0), 200.0, 5780.0),
        ])
        with patch('PL_Summary.connect_db', _mock_connect(conn)):
            result = calculate_total_PL("20240923", "20240925")
        self.assertAlmostEqual(result, 400.0)

    def test_empty_database_returns_zero(self):
        conn = _make_db([])
        with patch('PL_Summary.connect_db', _mock_connect(conn)):
            result = calculate_total_PL("20240901", "20240930")
        self.assertEqual(result, 0.0)

    def test_invalid_start_date_returns_none(self):
        result = calculate_total_PL("not-a-date")
        self.assertIsNone(result)

    def test_missing_start_date_raises(self):
        with self.assertRaises(ValueError):
            calculate_total_PL("")

    def test_last_entry_per_day_used(self):
        conn = _make_db([
            (datetime(2024, 9, 23, 10, 0), 100.0, 5000.0),  # earlier entry
            (datetime(2024, 9, 23, 16, 0), 200.0, 5100.0),  # last → should be used
        ])
        with patch('PL_Summary.connect_db', _mock_connect(conn)):
            result = calculate_total_PL("20240923", "20240923")
        self.assertAlmostEqual(result, 200.0)


if __name__ == "__main__":
    unittest.main()
