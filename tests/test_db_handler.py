"""Unit tests for db_handler.py using in-memory SQLite — no real TAT DB required."""
import os
import sys
import sqlite3
import unittest
from contextlib import contextmanager
from datetime import date, datetime
from unittest.mock import patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from db_handler import get_spx_data_from_db, get_trades, get_trades_by_type
from utils import FILETIME_EPOCH_OFFSET, FILETIME_TICKS_PER_SECOND


def _to_filetime(dt: datetime) -> int:
    posix = (dt - datetime(1970, 1, 1)).total_seconds()
    return int(posix * FILETIME_TICKS_PER_SECOND + FILETIME_EPOCH_OFFSET)


def _build_test_db() -> sqlite3.Connection:
    """Return an in-memory SQLite DB pre-populated with sample TAT data."""
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
    conn.execute("""
        CREATE TABLE DailyLog (
            DailyLogID INTEGER PRIMARY KEY,
            LogDate INTEGER,
            PL REAL,
            SPX REAL
        )
    """)

    opened_23 = _to_filetime(datetime(2024, 9, 23, 9, 30))
    closed_23 = _to_filetime(datetime(2024, 9, 23, 16, 0))
    opened_24 = _to_filetime(datetime(2024, 9, 24, 9, 30))
    closed_24 = _to_filetime(datetime(2024, 9, 24, 16, 0))

    # 9/23: one Put, one Call
    conn.execute(
        "INSERT INTO Trade VALUES (1,?,?,'Put',0,0,0,0,2,'LIMIT',1.00,0.20,500.0,0.10,0,1000.0,0,0,2024,9,23,'T1')",
        (opened_23, closed_23),
    )
    conn.execute(
        "INSERT INTO Trade VALUES (2,?,?,'Call',0,0,0,0,1,'LIMIT',0.80,0.15,-200.0,0.0,1,400.0,0,0,2024,9,23,'T2')",
        (opened_23, closed_23),
    )
    # 9/24: one Put
    conn.execute(
        "INSERT INTO Trade VALUES (3,?,?,'Put',0,0,0,0,1,'LIMIT',1.00,0.20,300.0,0.10,0,800.0,0,0,2024,9,24,'T3')",
        (opened_24, closed_24),
    )
    # Trade without TATTradeID — must be excluded
    conn.execute(
        "INSERT INTO Trade VALUES (4,?,?,'Put',0,0,0,0,1,'LIMIT',1.00,0.20,100.0,0.10,0,500.0,0,0,2024,9,23,NULL)",
        (opened_23, closed_23),
    )

    # DailyLog entries
    conn.execute(
        "INSERT INTO DailyLog VALUES (1, ?, 300.0, 5762.48)",
        (_to_filetime(datetime(2024, 9, 23, 16, 0)),),
    )
    conn.execute(
        "INSERT INTO DailyLog VALUES (2, ?, 600.0, 5800.00)",
        (_to_filetime(datetime(2024, 9, 24, 16, 0)),),
    )
    conn.commit()
    return conn


def _mock_connect(conn: sqlite3.Connection):
    """Return a contextmanager that yields the given connection and closes it on exit."""
    @contextmanager
    def _cm(retries=5, delay=1):
        try:
            yield conn
        finally:
            conn.close()
    return _cm


class TestGetTrades(unittest.TestCase):
    def setUp(self):
        self.conn = _build_test_db()

    def tearDown(self):
        self.conn.close()

    def test_returns_correct_row_count(self):
        df = get_trades(self.conn, 2024, 9, 23)
        # 2 trades with TATTradeID; 1 NULL excluded
        self.assertEqual(len(df), 2)

    def test_returns_empty_for_missing_date(self):
        df = get_trades(self.conn, 2024, 9, 25)
        self.assertTrue(df.empty)

    def test_required_columns_present(self):
        df = get_trades(self.conn, 2024, 9, 23)
        for col in ('ProfitLoss', 'TotalPremium', 'ClosingProcessed'):
            self.assertIn(col, df.columns)

    def test_dates_converted_from_filetime(self):
        df = get_trades(self.conn, 2024, 9, 23)
        # DateOpened column should be datetime objects, not raw integers
        self.assertIsInstance(df['DateOpened'].iloc[0], datetime)

    def test_null_tat_trade_id_excluded(self):
        df = get_trades(self.conn, 2024, 9, 23)
        self.assertEqual(len(df), 2)  # trade 4 (NULL TATTradeID) must be excluded


class TestGetTradesByType(unittest.TestCase):
    def _run(self, trade_type, start):
        conn = _build_test_db()
        with patch('db_handler.connect_db', _mock_connect(conn)):
            return get_trades_by_type(trade_type, start)
        # conn closed by _mock_connect's finally block

    def test_filters_by_trade_type(self):
        df = self._run('Call', date(2024, 9, 23))
        self.assertEqual(len(df), 1)
        self.assertTrue((df['TradeType'] == 'Call').all())

    def test_returns_aliased_columns(self):
        df = self._run('Put', date(2024, 9, 23))
        for col in ('PL', 'OpenDate', 'CloseDate', 'Contracts'):
            self.assertIn(col, df.columns)

    def test_start_date_inclusive(self):
        # start = 9/23 → should include trades on 9/23 and 9/24
        df = self._run('Put', date(2024, 9, 23))
        self.assertEqual(len(df), 2)

    def test_start_date_excludes_earlier_trades(self):
        # start = 9/24 → only the trade on 9/24
        df = self._run('Put', date(2024, 9, 24))
        self.assertEqual(len(df), 1)

    def test_no_trades_of_type(self):
        # No Call trades on 9/24
        df = self._run('Call', date(2024, 9, 24))
        self.assertTrue(df.empty)


class TestGetSpxDataFromDb(unittest.TestCase):
    def _run(self):
        conn = _build_test_db()
        with patch('db_handler.connect_db', _mock_connect(conn)):
            return get_spx_data_from_db()
        # conn closed by _mock_connect's finally block

    def test_returns_spx_close_column(self):
        df = self._run()
        self.assertIn('SPX_Close', df.columns)

    def test_sorted_by_log_date(self):
        df = self._run()
        self.assertTrue(df['LogDate'].is_monotonic_increasing)

    def test_correct_spx_values(self):
        df = self._run()
        self.assertAlmostEqual(df.iloc[0]['SPX_Close'], 5762.48)
        self.assertAlmostEqual(df.iloc[1]['SPX_Close'], 5800.00)

    def test_not_empty(self):
        df = self._run()
        self.assertFalse(df.empty)


class TestConnectDb(unittest.TestCase):
    @patch('db_handler.load_config', return_value={'db_path': '/nonexistent/path.db3'})
    def test_raises_file_not_found_when_db_missing(self, _):
        from db_handler import connect_db
        with self.assertRaises(FileNotFoundError):
            with connect_db():
                pass


if __name__ == "__main__":
    unittest.main()
