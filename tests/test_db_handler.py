"""Unit tests for db_handler.py using in-memory SQLite — no real TAT DB required."""
import os
import sys
import sqlite3
import unittest
from contextlib import contextmanager
from datetime import date, datetime
from unittest.mock import patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from db_handler import get_recent_trading_days, get_spx_data_from_db, get_trades, get_trades_by_type, get_trading_days_in_range, has_dailylog_rows, is_trading_day
from utils import FILETIME_EPOCH_OFFSET, FILETIME_TICKS_PER_SECOND, NET_EPOCH_OFFSET_SECONDS, NET_TICKS_PER_SECOND


def _to_net_ticks(dt: datetime) -> int:
    """Convert datetime to .NET DateTime ticks (100-ns since 0001-01-01), matching has_dailylog_rows."""
    posix = (dt - datetime(1970, 1, 1)).total_seconds()
    return int((posix + NET_EPOCH_OFFSET_SECONDS) * NET_TICKS_PER_SECOND)


def _build_trade_only_db() -> sqlite3.Connection:
    """In-memory DB with only a Trade table — used by get_recent_trading_days tests."""
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
    conn.commit()
    return conn


def _insert_trade(conn, trade_id, year, month, day, tat_id='T1', pl=100.0):
    conn.execute(
        "INSERT INTO Trade VALUES (?,0,0,'Put',0,0,0,0,1,'LIMIT',1.0,0.2,?,0.1,0,500.0,0,0,?,?,?,?)",
        (trade_id, pl, year, month, day, tat_id),
    )
    conn.commit()


def _build_dailylog_db() -> sqlite3.Connection:
    """In-memory DB with only a DailyLog table using .NET DateTime ticks."""
    conn = sqlite3.connect(":memory:")
    conn.execute("""
        CREATE TABLE DailyLog (
            DailyLogID INTEGER PRIMARY KEY,
            LogDate INTEGER,
            PL REAL,
            SPX REAL
        )
    """)
    conn.commit()
    return conn


def _insert_dailylog(conn, log_id, dt: datetime, pl=0.0, spx=5000.0):
    conn.execute(
        "INSERT INTO DailyLog VALUES (?, ?, ?, ?)",
        (log_id, _to_net_ticks(dt), pl, spx),
    )
    conn.commit()


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

    def test_retries_then_succeeds(self):
        """Fail twice with OperationalError, succeed on the third attempt."""
        import tempfile, os
        db_file = tempfile.NamedTemporaryFile(suffix='.db3', delete=False)
        db_file.close()
        try:
            call_count = {'n': 0}
            real_connect = sqlite3.connect

            def flaky_connect(path):
                call_count['n'] += 1
                if call_count['n'] < 3:
                    raise sqlite3.OperationalError("locked")
                return real_connect(path)

            with patch('db_handler.load_config', return_value={'db_path': db_file.name}), \
                 patch('db_handler.sqlite3.connect', side_effect=flaky_connect), \
                 patch('db_handler.time.sleep'):
                from db_handler import connect_db
                with connect_db(retries=5, delay=0) as conn:
                    self.assertIsNotNone(conn)
            self.assertEqual(call_count['n'], 3)
        finally:
            os.unlink(db_file.name)

    def test_raises_connection_error_after_all_retries_exhausted(self):
        """Always raise OperationalError → ConnectionError after retries."""
        import tempfile, os
        db_file = tempfile.NamedTemporaryFile(suffix='.db3', delete=False)
        db_file.close()
        try:
            with patch('db_handler.load_config', return_value={'db_path': db_file.name}), \
                 patch('db_handler.sqlite3.connect', side_effect=sqlite3.OperationalError("locked")), \
                 patch('db_handler.time.sleep'):
                from db_handler import connect_db
                with self.assertRaises(ConnectionError):
                    with connect_db(retries=3, delay=0):
                        pass
        finally:
            os.unlink(db_file.name)


class TestGetTradesByTypeDirectConnection(unittest.TestCase):
    """Verify get_trades_by_type works when a connection is passed directly."""

    def test_uses_provided_connection_without_calling_connect_db(self):
        conn = _build_test_db()
        with patch('db_handler.connect_db') as mock_connect:
            df = get_trades_by_type('Put', date(2024, 9, 23), connection=conn)
            mock_connect.assert_not_called()
        conn.close()
        self.assertEqual(len(df), 2)

    def test_returns_same_results_as_patched_path(self):
        """Results via direct connection must match results via patched connect_db."""
        conn1 = _build_test_db()
        conn2 = _build_test_db()
        with patch('db_handler.connect_db', _mock_connect(conn2)):
            df_patched = get_trades_by_type('Put', date(2024, 9, 23))
        df_direct = get_trades_by_type('Put', date(2024, 9, 23), connection=conn1)
        conn1.close()
        self.assertEqual(len(df_direct), len(df_patched))
        self.assertAlmostEqual(df_direct['PL'].sum(), df_patched['PL'].sum())


class TestGetRecentTradingDays(unittest.TestCase):
    def setUp(self):
        self.conn = _build_trade_only_db()

    def tearDown(self):
        self.conn.close()

    def test_returns_n_most_recent_trading_days(self):
        for i, (y, m, d) in enumerate([(2024,9,23),(2024,9,24),(2024,9,25),(2024,9,26),(2024,9,27)], start=1):
            _insert_trade(self.conn, i, y, m, d)
        result = get_recent_trading_days(self.conn, datetime(2024, 9, 27), 5)
        self.assertEqual(len(result), 5)
        self.assertEqual(result[0], datetime(2024, 9, 23))
        self.assertEqual(result[-1], datetime(2024, 9, 27))

    def test_sorted_ascending_oldest_first(self):
        _insert_trade(self.conn, 1, 2024, 9, 27)
        _insert_trade(self.conn, 2, 2024, 9, 23)
        _insert_trade(self.conn, 3, 2024, 9, 25)
        result = get_recent_trading_days(self.conn, datetime(2024, 9, 27), 10)
        dates = [r.date() for r in result]
        self.assertEqual(dates, sorted(dates))

    def test_skips_days_without_trades(self):
        # Mon/Wed/Fri only — Tue/Thu have no trades
        _insert_trade(self.conn, 1, 2024, 9, 23)  # Mon
        _insert_trade(self.conn, 2, 2024, 9, 25)  # Wed
        _insert_trade(self.conn, 3, 2024, 9, 27)  # Fri
        result = get_recent_trading_days(self.conn, datetime(2024, 9, 27), 5)
        self.assertEqual(len(result), 3)
        self.assertIn(datetime(2024, 9, 23), result)
        self.assertIn(datetime(2024, 9, 25), result)
        self.assertIn(datetime(2024, 9, 27), result)

    def test_fewer_days_than_requested_returns_all(self):
        _insert_trade(self.conn, 1, 2024, 9, 23)
        _insert_trade(self.conn, 2, 2024, 9, 24)
        result = get_recent_trading_days(self.conn, datetime(2024, 9, 27), 10)
        self.assertEqual(len(result), 2)

    def test_no_trades_returns_empty_list(self):
        result = get_recent_trading_days(self.conn, datetime(2024, 9, 27), 5)
        self.assertEqual(result, [])

    def test_target_date_is_inclusive(self):
        _insert_trade(self.conn, 1, 2024, 9, 27)
        result = get_recent_trading_days(self.conn, datetime(2024, 9, 27), 5)
        self.assertIn(datetime(2024, 9, 27), result)

    def test_excludes_null_tattradeid(self):
        _insert_trade(self.conn, 1, 2024, 9, 23, tat_id=None)
        result = get_recent_trading_days(self.conn, datetime(2024, 9, 27), 5)
        self.assertEqual(result, [])


class TestHasDailylogRows(unittest.TestCase):
    def setUp(self):
        self.conn = _build_dailylog_db()

    def tearDown(self):
        self.conn.close()

    def test_returns_true_when_rows_exist_for_date(self):
        _insert_dailylog(self.conn, 1, datetime(2024, 9, 23, 10, 0))
        self.assertTrue(has_dailylog_rows(self.conn, datetime(2024, 9, 23)))

    def test_returns_false_when_no_rows(self):
        self.assertFalse(has_dailylog_rows(self.conn, datetime(2024, 9, 23)))

    def test_returns_false_for_different_date(self):
        # Only Friday has data; querying Saturday returns False
        _insert_dailylog(self.conn, 1, datetime(2024, 9, 20, 16, 0))  # Friday
        self.assertFalse(has_dailylog_rows(self.conn, datetime(2024, 9, 21)))  # Saturday

    def test_multiple_rows_same_day_returns_true(self):
        _insert_dailylog(self.conn, 1, datetime(2024, 9, 23, 9, 30))
        _insert_dailylog(self.conn, 2, datetime(2024, 9, 23, 16, 0))
        self.assertTrue(has_dailylog_rows(self.conn, datetime(2024, 9, 23)))

    def test_end_of_day_boundary_is_inclusive(self):
        _insert_dailylog(self.conn, 1, datetime(2024, 9, 23, 23, 59, 59))
        self.assertTrue(has_dailylog_rows(self.conn, datetime(2024, 9, 23)))


def _build_trade_and_dailylog_db() -> sqlite3.Connection:
    """In-memory DB with both Trade and DailyLog tables."""
    conn = _build_trade_only_db()
    conn.execute("""
        CREATE TABLE DailyLog (
            DailyLogID INTEGER PRIMARY KEY,
            LogDate INTEGER,
            PL REAL,
            SPX REAL
        )
    """)
    conn.commit()
    return conn


class TestIsTradingDay(unittest.TestCase):
    def setUp(self):
        self.conn = _build_trade_and_dailylog_db()

    def tearDown(self):
        self.conn.close()

    def test_true_when_trade_exists(self):
        _insert_trade(self.conn, 1, 2024, 9, 23)
        self.assertTrue(is_trading_day(self.conn, datetime(2024, 9, 23)))

    def test_true_when_only_dailylog_exists(self):
        _insert_dailylog(self.conn, 1, datetime(2024, 9, 23, 10, 0))
        self.assertTrue(is_trading_day(self.conn, datetime(2024, 9, 23)))

    def test_false_when_neither_trade_nor_dailylog(self):
        self.assertFalse(is_trading_day(self.conn, datetime(2024, 9, 23)))

    def test_false_for_different_date(self):
        _insert_trade(self.conn, 1, 2024, 9, 23)
        self.assertFalse(is_trading_day(self.conn, datetime(2024, 9, 24)))


class TestGetTradingDaysInRange(unittest.TestCase):
    def setUp(self):
        self.conn = _build_trade_only_db()

    def tearDown(self):
        self.conn.close()

    def test_returns_days_within_range(self):
        _insert_trade(self.conn, 1, 2024, 9, 23)
        _insert_trade(self.conn, 2, 2024, 9, 24)
        _insert_trade(self.conn, 3, 2024, 9, 25)
        result = get_trading_days_in_range(
            self.conn, datetime(2024, 9, 23), datetime(2024, 9, 25)
        )
        self.assertEqual(len(result), 3)

    def test_excludes_days_outside_range(self):
        _insert_trade(self.conn, 1, 2024, 9, 22)  # before start
        _insert_trade(self.conn, 2, 2024, 9, 23)
        _insert_trade(self.conn, 3, 2024, 9, 26)  # after end
        result = get_trading_days_in_range(
            self.conn, datetime(2024, 9, 23), datetime(2024, 9, 25)
        )
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0], datetime(2024, 9, 23))

    def test_returns_empty_when_no_trades_in_range(self):
        result = get_trading_days_in_range(
            self.conn, datetime(2024, 9, 23), datetime(2024, 9, 25)
        )
        self.assertEqual(result, [])

    def test_sorted_ascending(self):
        _insert_trade(self.conn, 1, 2024, 9, 25)
        _insert_trade(self.conn, 2, 2024, 9, 23)
        result = get_trading_days_in_range(
            self.conn, datetime(2024, 9, 23), datetime(2024, 9, 25)
        )
        self.assertEqual(result[0], datetime(2024, 9, 23))
        self.assertEqual(result[1], datetime(2024, 9, 25))


if __name__ == "__main__":
    unittest.main()
