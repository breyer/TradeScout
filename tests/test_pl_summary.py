"""Unit tests for PL_Summary.py using in-memory SQLite."""
import os
import sys
import sqlite3
import unittest
from contextlib import contextmanager
from datetime import datetime
from unittest.mock import patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from PL_Summary import calculate_total_PL, to_filetime
from utils import FILETIME_EPOCH_OFFSET, FILETIME_TICKS_PER_SECOND, convert_to_human_readable


def _make_filetime(dt: datetime) -> int:
    posix = (dt - datetime(1970, 1, 1)).total_seconds()
    return int(posix * FILETIME_TICKS_PER_SECOND + FILETIME_EPOCH_OFFSET)


def _build_dailylog_db(entries) -> sqlite3.Connection:
    """entries: list of (datetime, pl, spx) tuples."""
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
        conn.execute(
            "INSERT INTO DailyLog VALUES (?, ?, ?, ?)",
            (i, _make_filetime(dt), pl, spx),
        )
    conn.commit()
    return conn


def _mock_connect(conn: sqlite3.Connection):
    @contextmanager
    def _cm(retries=5, delay=1):
        try:
            yield conn
        finally:
            conn.close()
    return _cm


# ---------------------------------------------------------------------------
# to_filetime
# ---------------------------------------------------------------------------
class TestToFiletime(unittest.TestCase):
    def test_unix_epoch_gives_offset(self):
        self.assertEqual(to_filetime(datetime(1970, 1, 1)), FILETIME_EPOCH_OFFSET)

    def test_round_trips_with_convert_to_human_readable(self):
        dt = datetime(2024, 9, 23, 14, 30, 0)
        ft = to_filetime(dt)
        result = convert_to_human_readable(ft)
        self.assertEqual(result.month, 9)
        self.assertEqual(result.day, 23)
        self.assertEqual(result.hour, 14)
        self.assertEqual(result.minute, 30)

    def test_returns_int(self):
        self.assertIsInstance(to_filetime(datetime(2024, 1, 1)), int)

    def test_increases_with_time(self):
        earlier = to_filetime(datetime(2024, 9, 23))
        later = to_filetime(datetime(2024, 9, 24))
        self.assertGreater(later, earlier)


# ---------------------------------------------------------------------------
# calculate_total_PL
# ---------------------------------------------------------------------------
class TestCalculateTotalPL(unittest.TestCase):
    def _run(self, entries, start, end=None):
        conn = _build_dailylog_db(entries)
        with patch('PL_Summary.connect_db', _mock_connect(conn)):
            return calculate_total_PL(start, end)
        # conn is closed by _mock_connect's finally block

    def test_sums_one_entry_per_day(self):
        entries = [
            (datetime(2024, 9, 23, 16, 0), 300.0, 5762.0),
            (datetime(2024, 9, 24, 16, 0), -100.0, 5800.0),
            (datetime(2024, 9, 25, 16, 0), 200.0, 5780.0),
        ]
        result = self._run(entries, "20240923", "20240925")
        self.assertAlmostEqual(result, 400.0)

    def test_uses_last_entry_per_day(self):
        # Two entries on same day — only last (200.0) should be counted
        entries = [
            (datetime(2024, 9, 23, 10, 0), 100.0, 5000.0),
            (datetime(2024, 9, 23, 16, 0), 200.0, 5100.0),
        ]
        result = self._run(entries, "20240923", "20240923")
        self.assertAlmostEqual(result, 200.0)

    def test_empty_range_returns_zero(self):
        result = self._run([], "20240901", "20240930")
        self.assertEqual(result, 0.0)

    def test_single_day_range(self):
        entries = [(datetime(2024, 9, 23, 16, 0), 500.0, 5762.0)]
        result = self._run(entries, "20240923", "20240923")
        self.assertAlmostEqual(result, 500.0)

    def test_negative_pl_summed_correctly(self):
        entries = [
            (datetime(2024, 9, 23, 16, 0), -300.0, 5762.0),
            (datetime(2024, 9, 24, 16, 0), -200.0, 5800.0),
        ]
        result = self._run(entries, "20240923", "20240924")
        self.assertAlmostEqual(result, -500.0)

    def test_invalid_start_date_returns_none(self):
        result = calculate_total_PL("not-a-date")
        self.assertIsNone(result)

    def test_invalid_end_date_returns_none(self):
        result = calculate_total_PL("20240901", "bad")
        self.assertIsNone(result)

    def test_missing_start_date_raises_value_error(self):
        with self.assertRaises(ValueError):
            calculate_total_PL("")

    def test_no_end_date_defaults_to_full_month(self):
        # Entries cover all of Sept 2024; no end_date → should use 2024-09-30 as end
        entries = [
            (datetime(2024, 9, 1, 16, 0), 100.0, 5000.0),
            (datetime(2024, 9, 30, 16, 0), 200.0, 5100.0),
        ]
        result = self._run(entries, "20240901")
        self.assertAlmostEqual(result, 300.0)

    def test_data_outside_range_excluded(self):
        entries = [
            (datetime(2024, 9, 22, 16, 0), 999.0, 5000.0),  # before start → excluded
            (datetime(2024, 9, 23, 16, 0), 100.0, 5100.0),
            (datetime(2024, 9, 26, 16, 0), 888.0, 5200.0),  # after end → excluded
        ]
        result = self._run(entries, "20240923", "20240923")
        self.assertAlmostEqual(result, 100.0)

    def test_returns_none_on_database_error(self):
        """sqlite3.Error during DB query must return None, not raise."""
        with patch('PL_Summary.connect_db', side_effect=sqlite3.Error("disk I/O error")):
            result = calculate_total_PL("20240923", "20240923")
        self.assertIsNone(result)


# ---------------------------------------------------------------------------
# calculate_premium_captured_over_range
# ---------------------------------------------------------------------------
class TestCalculatePremiumCapturedOverRange(unittest.TestCase):
    def _make_trade_db(self, entries) -> sqlite3.Connection:
        """entries: list of (year, month, day, profit_loss, total_premium) tuples."""
        conn = sqlite3.connect(":memory:")
        conn.execute("""
            CREATE TABLE Trade (
                TradeID INTEGER PRIMARY KEY,
                DateOpened INTEGER, DateClosed INTEGER,
                TradeType TEXT,
                ShortPut REAL, LongPut REAL, ShortCall REAL, LongCall REAL,
                Qty INTEGER, StopType TEXT,
                PriceOpen REAL, PriceStopTarget REAL,
                ProfitLoss REAL, PriceClose REAL,
                ClosingProcessed INTEGER, TotalPremium REAL,
                Commission REAL, CommissionClose REAL,
                Year INTEGER, Month INTEGER, Day INTEGER,
                TATTradeID TEXT
            )
        """)
        for i, (yr, mo, dy, pl, prem) in enumerate(entries, 1):
            opened = _make_filetime(datetime(yr, mo, dy, 9, 30))
            closed = _make_filetime(datetime(yr, mo, dy, 16, 0))
            conn.execute(
                "INSERT INTO Trade VALUES (?,?,?,'Put',0,0,0,0,1,'LIMIT',1.0,0.2,?,0.1,0,?,0,0,?,?,?,'T%d')" % i,
                (i, opened, closed, pl, prem, yr, mo, dy),
            )
        conn.commit()
        return conn

    def test_sums_premium_over_two_days(self):
        from PL_Summary import calculate_premium_captured_over_range
        conn = self._make_trade_db([
            (2024, 9, 23, 500.0, 1000.0),
            (2024, 9, 24, -200.0, 500.0),
        ])
        try:
            result = calculate_premium_captured_over_range(
                datetime(2024, 9, 23), datetime(2024, 9, 24), conn
            )
        finally:
            conn.close()
        self.assertAlmostEqual(result, 300.0)

    def test_empty_range_returns_zero(self):
        from PL_Summary import calculate_premium_captured_over_range
        conn = self._make_trade_db([])
        try:
            result = calculate_premium_captured_over_range(
                datetime(2024, 9, 23), datetime(2024, 9, 23), conn
            )
        finally:
            conn.close()
        self.assertEqual(result, 0.0)


if __name__ == "__main__":
    unittest.main()
