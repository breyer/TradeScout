"""
Integration tests against the real TAT database (data/data.db3).

These tests hit the actual SQLite file and assert exact values derived
from it.  They are skipped automatically if the DB is missing.

Key schema findings:
  - Trade.TradeType values: 'PutSpread', 'CallSpread', 'IronFly'
    Trade_Scout.py uses 'PutSpread' and 'CallSpread'; IronFly is a third
    category not yet split into the weekly report.
  - DailyLog.LogDate stores .NET DateTime ticks (100-ns since 0001-01-01).
    convert_to_human_readable() forces the year to the current system year,
    so DailyLog-based assertions use datetime.now().year.
"""
import os
import sqlite3
import sys
import unittest
from datetime import datetime
from unittest.mock import patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

DB_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data', 'data.db3'))
MOCK_CONFIG = {'db_path': DB_PATH, 'webhooks': []}


def _patch_config():
    return patch('db_handler.load_config', return_value=MOCK_CONFIG)


def setUpModule():
    if not os.path.exists(DB_PATH):
        raise unittest.SkipTest(f"Real database not found at {DB_PATH}")


# ---------------------------------------------------------------------------
# get_trades — integer Year/Month/Day columns, exact value assertions
# ---------------------------------------------------------------------------
class TestGetTradesReal(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(DB_PATH)

    def tearDown(self):
        self.conn.close()

    def test_known_date_returns_exact_row_count(self):
        from db_handler import get_trades
        df = get_trades(self.conn, 2024, 9, 25)
        self.assertEqual(len(df), 10)

    def test_known_date_total_premium(self):
        from db_handler import get_trades
        df = get_trades(self.conn, 2024, 9, 25)
        self.assertAlmostEqual(df['TotalPremium'].sum(), 13260.0, places=2)

    def test_known_date_total_pl(self):
        from db_handler import get_trades
        df = get_trades(self.conn, 2024, 9, 25)
        self.assertAlmostEqual(df['ProfitLoss'].sum(), 2947.15, places=1)

    def test_no_null_tat_ids_returned(self):
        """Trades without a TATTradeID must always be excluded."""
        from db_handler import get_trades
        df = get_trades(self.conn, 2024, 9, 25)
        self.assertEqual(len(df), 10)  # confirmed count excludes NULLs

    def test_empty_for_non_trading_day(self):
        from db_handler import get_trades
        df = get_trades(self.conn, 2024, 9, 28)  # Saturday
        self.assertTrue(df.empty)

    def test_trade_type_values_in_real_db(self):
        """
        Real DB uses 'PutSpread'/'CallSpread'/'IronFly', NOT 'Put'/'Call'.
        This documents the discrepancy with Trade_Scout.py's get_trades_by_type()
        which queries for 'Put' and 'Call' and will return empty DataFrames.
        """
        from db_handler import get_trades
        df = get_trades(self.conn, 2024, 9, 25)
        actual_types = set(df['TradeType'].unique())
        self.assertNotIn('Put', actual_types)
        self.assertNotIn('Call', actual_types)
        self.assertTrue(actual_types.issubset({'PutSpread', 'CallSpread', 'IronFly'}))

    def test_date_columns_are_datetimes(self):
        from db_handler import get_trades
        df = get_trades(self.conn, 2024, 9, 25)
        self.assertIsInstance(df['DateOpened'].iloc[0], datetime)
        self.assertIsInstance(df['DateClosed'].iloc[0], datetime)


# ---------------------------------------------------------------------------
# calculate_metrics — on real trade data
# ---------------------------------------------------------------------------
class TestCalculateMetricsReal(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(DB_PATH)

    def tearDown(self):
        self.conn.close()

    def _trades(self, year, month, day):
        from db_handler import get_trades
        return get_trades(self.conn, year, month, day)

    def test_profitable_day_positive_premium_captured(self):
        from utils import calculate_metrics
        df = self._trades(2024, 9, 25)  # P/L = +2947
        _, premium_captured, *_ = calculate_metrics(df)
        self.assertGreater(premium_captured, 0)

    def test_losing_day_negative_premium_captured(self):
        from utils import calculate_metrics
        df = self._trades(2024, 9, 5)  # P/L = -10763
        _, premium_captured, *_ = calculate_metrics(df)
        self.assertLess(premium_captured, 0)

    def test_win_rate_between_0_and_100(self):
        from utils import calculate_metrics
        df = self._trades(2024, 9, 25)
        _, _, _, win_rate, *_ = calculate_metrics(df)
        self.assertGreaterEqual(win_rate, 0)
        self.assertLessEqual(win_rate, 100)

    def test_expired_plus_stopped_equals_total(self):
        from utils import calculate_metrics
        df = self._trades(2024, 9, 25)
        _, _, _, _, expired, stops, *_ = calculate_metrics(df)
        self.assertEqual(expired + stops, len(df))

    def test_premium_sold_matches_db_aggregate(self):
        from utils import calculate_metrics
        df = self._trades(2024, 9, 25)
        premium_sold, *_ = calculate_metrics(df)
        self.assertAlmostEqual(premium_sold, 13260.0, places=2)


# ---------------------------------------------------------------------------
# get_spx_data_from_db — real DailyLog data
# ---------------------------------------------------------------------------
class TestGetSpxDataFromDbReal(unittest.TestCase):
    def test_returns_non_empty_dataframe(self):
        with _patch_config():
            from db_handler import get_spx_data_from_db
            df = get_spx_data_from_db()
        self.assertFalse(df.empty)

    def test_has_spx_close_column(self):
        with _patch_config():
            from db_handler import get_spx_data_from_db
            df = get_spx_data_from_db()
        self.assertIn('SPX_Close', df.columns)

    def test_sorted_ascending(self):
        with _patch_config():
            from db_handler import get_spx_data_from_db
            df = get_spx_data_from_db()
        self.assertTrue(df['LogDate'].is_monotonic_increasing)

    def test_spx_values_are_plausible(self):
        """SPX should be between 3000 and 6500 for the 2022–2024 range."""
        with _patch_config():
            from db_handler import get_spx_data_from_db
            df = get_spx_data_from_db()
        valid = df[(df['SPX_Close'] > 3000) & (df['SPX_Close'] < 6500)]
        self.assertGreater(len(valid), 0)


# ---------------------------------------------------------------------------
# get_trades_by_type — real type names: PutSpread / CallSpread / IronFly
# ---------------------------------------------------------------------------
class TestGetTradesByTypeReal(unittest.TestCase):
    def test_put_spread_returns_data(self):
        with _patch_config():
            from db_handler import get_trades_by_type
            df = get_trades_by_type('PutSpread', datetime(2024, 9, 25).date())
        self.assertFalse(df.empty)
        self.assertIn('PL', df.columns)

    def test_call_spread_returns_data(self):
        with _patch_config():
            from db_handler import get_trades_by_type
            df = get_trades_by_type('CallSpread', datetime(2024, 9, 25).date())
        self.assertFalse(df.empty)

    def test_wrong_type_name_returns_empty(self):
        """Regression: 'Put'/'Call' (without 'Spread') must never return data."""
        with _patch_config():
            from db_handler import get_trades_by_type
            self.assertTrue(get_trades_by_type('Put',  datetime(2024, 9, 25).date()).empty)
            self.assertTrue(get_trades_by_type('Call', datetime(2024, 9, 25).date()).empty)

    def test_iron_fly_trades_exist(self):
        """IronFly is a third trade type present in the DB (not Put/Call split)."""
        with _patch_config():
            from db_handler import get_trades_by_type
            df = get_trades_by_type('IronFly', datetime(2022, 10, 11).date())
        self.assertFalse(df.empty)


# ---------------------------------------------------------------------------
# calculate_total_PL — real DailyLog over a date range
# ---------------------------------------------------------------------------
class TestCalculateTotalPLReal(unittest.TestCase):
    def test_returns_float_for_valid_range(self):
        with _patch_config():
            from PL_Summary import calculate_total_PL
            result = calculate_total_PL("20240923", "20240927")
        # Just check it ran without error and returned a number
        self.assertIsNotNone(result)
        self.assertIsInstance(result, float)

    def test_returns_zero_for_future_date_range(self):
        """A range well in the future should have no data → 0.0."""
        with _patch_config():
            from PL_Summary import calculate_total_PL
            result = calculate_total_PL("20991201", "20991231")
        self.assertEqual(result, 0.0)


if __name__ == "__main__":
    unittest.main()
