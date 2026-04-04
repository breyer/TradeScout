"""Unit tests for utils.py — no database or network required."""
import os
import sqlite3
import sys
import unittest
import unittest.mock
from datetime import datetime, timedelta

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pandas as pd

from utils import (
    FILETIME_EPOCH_OFFSET,
    FILETIME_TICKS_PER_SECOND,
    NET_EPOCH_OFFSET_SECONDS,
    NET_TICKS_PER_SECOND,
    calculate_metrics,
    convert_to_human_readable,
    get_most_recent_monday,
    get_specified_date,
)


def _make_filetime(dt: datetime) -> int:
    posix = (dt - datetime(1970, 1, 1)).total_seconds()
    return int(posix * FILETIME_TICKS_PER_SECOND + FILETIME_EPOCH_OFFSET)


# ---------------------------------------------------------------------------
# convert_to_human_readable
# ---------------------------------------------------------------------------
class TestConvertToHumanReadable(unittest.TestCase):
    def test_month_day_hour_preserved(self):
        dt = datetime(2024, 9, 23, 14, 30, 0)
        result = convert_to_human_readable(_make_filetime(dt))
        self.assertEqual(result.month, 9)
        self.assertEqual(result.day, 23)
        self.assertEqual(result.hour, 14)
        self.assertEqual(result.minute, 30)

    def test_year_forced_to_current_year(self):
        dt = datetime(2019, 6, 15, 8, 0, 0)
        result = convert_to_human_readable(_make_filetime(dt))
        self.assertEqual(result.year, datetime.now().year)

    def test_unix_epoch_offset(self):
        # FILETIME_EPOCH_OFFSET should represent 1970-01-01 00:00:00
        result = convert_to_human_readable(FILETIME_EPOCH_OFFSET)
        self.assertEqual(result.month, 1)
        self.assertEqual(result.day, 1)
        self.assertEqual(result.hour, 0)


# ---------------------------------------------------------------------------
# get_most_recent_monday
# ---------------------------------------------------------------------------
class TestGetMostRecentMonday(unittest.TestCase):
    def test_monday_returns_same_day(self):
        monday = datetime(2024, 9, 2)  # confirmed Monday
        self.assertEqual(get_most_recent_monday(monday), monday)

    def test_tuesday_returns_monday(self):
        self.assertEqual(get_most_recent_monday(datetime(2024, 9, 3)), datetime(2024, 9, 2))

    def test_wednesday_returns_monday(self):
        self.assertEqual(get_most_recent_monday(datetime(2024, 9, 4)), datetime(2024, 9, 2))

    def test_thursday_returns_monday(self):
        self.assertEqual(get_most_recent_monday(datetime(2024, 9, 5)), datetime(2024, 9, 2))

    def test_friday_returns_monday(self):
        self.assertEqual(get_most_recent_monday(datetime(2024, 9, 6)), datetime(2024, 9, 2))

    def test_saturday_returns_monday(self):
        self.assertEqual(get_most_recent_monday(datetime(2024, 9, 7)), datetime(2024, 9, 2))

    def test_sunday_returns_previous_monday(self):
        # Previously broken: formula returned Sunday itself
        sunday = datetime(2024, 9, 8)
        expected = datetime(2024, 9, 2)
        result = get_most_recent_monday(sunday)
        self.assertEqual(result, expected, f"Sunday {sunday.date()} should map to {expected.date()}, got {result.date()}")

    def test_no_arg_defaults_to_a_monday(self):
        result = get_most_recent_monday()
        self.assertIsInstance(result, datetime)
        self.assertEqual(result.weekday(), 0, "Result must be a Monday (weekday 0)")

    def test_week_boundary_next_monday(self):
        # The Monday immediately after the prior set
        self.assertEqual(get_most_recent_monday(datetime(2024, 9, 9)), datetime(2024, 9, 9))
        self.assertEqual(get_most_recent_monday(datetime(2024, 9, 10)), datetime(2024, 9, 9))


# ---------------------------------------------------------------------------
# get_specified_date
# ---------------------------------------------------------------------------
class TestGetSpecifiedDate(unittest.TestCase):
    def test_parses_yyyymmdd(self):
        self.assertEqual(get_specified_date("20240923"), datetime(2024, 9, 23))

    def test_no_arg_returns_today(self):
        result = get_specified_date()
        self.assertEqual(result.date(), datetime.now().date())


# ---------------------------------------------------------------------------
# calculate_metrics
# ---------------------------------------------------------------------------
class TestCalculateMetrics(unittest.TestCase):
    _COLS = ['TotalPremium', 'ProfitLoss', 'ClosingProcessed', 'PriceClose', 'PriceStopTarget']

    def _df(self, rows):
        return pd.DataFrame(rows, columns=self._COLS)

    def test_basic_sums(self):
        df = self._df([
            (1000.0, 500.0, 1, 0.10, 0.20),   # stop, win, no bad slip
            (500.0, -200.0, 0, 0.00, 0.20),   # expired, loss → negative_exp
            (800.0, 300.0, 1, 0.15, 0.20),    # stop, win, no bad slip
        ])
        sold, captured, pcr, win_rate, expired, stops, bad_slip, _, neg_exp = calculate_metrics(df)
        self.assertAlmostEqual(sold, 2300.0)
        self.assertAlmostEqual(captured, 600.0)
        self.assertEqual(expired, 1)
        self.assertEqual(stops, 2)
        self.assertEqual(neg_exp, 1)
        self.assertEqual(bad_slip, 0)

    def test_bad_slip_threshold(self):
        df = self._df([
            (1000.0, -100.0, 1, 1.20, 0.20),  # |1.20| - 0.20 = 1.00 >= 0.50 → bad
            (500.0, 200.0, 0, 0.10, 0.20),    # |0.10| - 0.20 = -0.10 < 0.50 → ok
        ])
        _, _, _, _, _, _, bad_slip, bad_slip_max, _ = calculate_metrics(df)
        self.assertEqual(bad_slip, 1)
        self.assertAlmostEqual(bad_slip_max, 1.00)

    def test_bad_slip_clearly_above_threshold(self):
        # Use values that are unambiguously above threshold after floating-point arithmetic
        df = self._df([
            (100.0, 0.0, 1, 0.75, 0.20),  # 0.75 - 0.20 = 0.55 >= 0.50
        ])
        _, _, _, _, _, _, bad_slip, _, _ = calculate_metrics(df)
        self.assertEqual(bad_slip, 1)

    def test_empty_df_returns_zeros(self):
        df = self._df([])
        sold, captured, pcr, win_rate, exp, stops, bad_slip, bad_slip_max, neg_exp = calculate_metrics(df)
        self.assertEqual(sold, 0)
        self.assertEqual(captured, 0)
        self.assertEqual(pcr, 0)
        self.assertEqual(win_rate, 0)
        self.assertEqual(bad_slip, 0)

    def test_pcr_calculation(self):
        df = self._df([
            (1000.0, 500.0, 1, 0.0, 0.0),  # 50 % capture rate
        ])
        _, _, pcr, _, _, _, _, _, _ = calculate_metrics(df)
        self.assertAlmostEqual(pcr, 50.0)

    def test_win_rate_three_wins_one_loss(self):
        df = self._df([
            (100.0, 50.0, 1, 0.0, 0.0),
            (100.0, 30.0, 1, 0.0, 0.0),
            (100.0, 20.0, 1, 0.0, 0.0),
            (100.0, -10.0, 1, 0.0, 0.0),
        ])
        _, _, _, win_rate, _, _, _, _, _ = calculate_metrics(df)
        self.assertAlmostEqual(win_rate, 75.0)

    def test_zero_premium_sold_gives_zero_pcr(self):
        df = self._df([
            (0.0, 100.0, 1, 0.0, 0.0),
        ])
        _, _, pcr, win_rate, _, _, _, _, _ = calculate_metrics(df)
        self.assertEqual(pcr, 0)
        self.assertEqual(win_rate, 0)


# ---------------------------------------------------------------------------
# load_yaml_config
# ---------------------------------------------------------------------------
class TestLoadYamlConfig(unittest.TestCase):
    def test_loads_valid_yaml(self):
        import tempfile, yaml
        cfg = {'db_path': 'data/test.db3', 'webhooks': []}
        with tempfile.TemporaryDirectory() as tmpdir:
            config_dir = os.path.join(tmpdir, 'config')
            os.makedirs(config_dir)
            config_path = os.path.join(config_dir, 'config.yaml')
            with open(config_path, 'w') as f:
                yaml.dump(cfg, f)

            # Temporarily redirect load_yaml_config to the temp dir
            import utils as utils_mod
            original = utils_mod.__file__
            utils_mod.__file__ = os.path.join(tmpdir, 'utils.py')
            try:
                result = utils_mod.load_yaml_config()
                self.assertEqual(result['db_path'], 'data/test.db3')
            finally:
                utils_mod.__file__ = original

    def test_raises_file_not_found_for_missing_config(self):
        import utils as utils_mod
        original = utils_mod.__file__
        utils_mod.__file__ = '/nonexistent/dir/utils.py'
        try:
            with self.assertRaises(FileNotFoundError):
                utils_mod.load_yaml_config()
        finally:
            utils_mod.__file__ = original


# ---------------------------------------------------------------------------
# format_message
# ---------------------------------------------------------------------------
class TestFormatMessage(unittest.TestCase):
    def _call(self, **overrides):
        defaults = dict(
            date=datetime(2024, 9, 23),
            premium_sold=10000.0,
            premium_captured=5000.0,
            pcr=50.0,
            win_rate=75.0,
            expired_trades=3,
            stops=7,
            bad_slip=2,
            bad_slip_max=1.50,
            spx_last=5762.48,
            negative_exp=0,
            weekly_pl=5000.0,
            monthly_pl=12000.0,
        )
        defaults.update(overrides)
        from utils import format_message
        return format_message(**defaults)

    def test_contains_date_header(self):
        msg = self._call()
        self.assertIn('2024 Sep 23', msg)
        self.assertIn('Monday', msg)

    def test_positive_pl_has_dollar_sign(self):
        msg = self._call(premium_captured=5000.0)
        self.assertIn('$5,000.00', msg)

    def test_negative_premium_captured_uses_parentheses(self):
        msg = self._call(premium_captured=-3000.0)
        self.assertIn('($3,000.00)', msg)

    def test_none_spx_last_shows_empty(self):
        msg = self._call(spx_last=None)
        self.assertIn('SPX Last', msg)

    def test_no_bad_slip_max_omits_max(self):
        msg = self._call(bad_slip=0, bad_slip_max=None)
        self.assertNotIn('max)', msg)

    def test_bad_slip_with_max_shown(self):
        msg = self._call(bad_slip=3, bad_slip_max=2.50)
        self.assertIn('2.50 max', msg)

    def test_message_wrapped_in_code_block(self):
        msg = self._call()
        self.assertIn('```', msg)

    def test_negative_weekly_pl_parentheses(self):
        msg = self._call(weekly_pl=-500.0)
        self.assertIn('($500.00)', msg)


# ---------------------------------------------------------------------------
# get_last_spx_value
# ---------------------------------------------------------------------------
def _make_net_ticks(dt: datetime) -> int:
    """Convert a datetime to .NET DateTime ticks (100-ns since 0001-01-01), matching the real TAT DB."""
    epoch = datetime(1970, 1, 1)
    return int(((dt - epoch).total_seconds() + NET_EPOCH_OFFSET_SECONDS) * NET_TICKS_PER_SECOND)


class TestGetLastSpxValue(unittest.TestCase):
    def _make_dailylog_db(self, entries):
        """entries: list of (datetime, spx) — stored as real .NET ticks matching the live DB format."""
        conn = sqlite3.connect(":memory:")
        conn.execute("""
            CREATE TABLE DailyLog (
                DailyLogID INTEGER PRIMARY KEY, LogDate INTEGER, PL REAL, SPX REAL
            )
        """)
        for i, (dt, spx) in enumerate(entries, 1):
            conn.execute("INSERT INTO DailyLog VALUES (?, ?, 0, ?)", (i, _make_net_ticks(dt), spx))
        conn.commit()
        return conn

    def test_returns_last_spx_for_date(self):
        conn = self._make_dailylog_db([
            (datetime(2026, 9, 23, 10, 0), 5700.0),
            (datetime(2026, 9, 23, 16, 0), 5762.48),  # latest → correct value
        ])
        from utils import get_last_spx_value
        result = get_last_spx_value(conn, 2026, 9, 23)
        conn.close()
        self.assertAlmostEqual(result, 5762.48)

    def test_returns_latest_timestamp_not_insertion_order(self):
        # Regression: later timestamp inserted first — must still return the latest by time.
        conn = self._make_dailylog_db([
            (datetime(2026, 9, 23, 16, 0), 5762.48),  # later timestamp, inserted first
            (datetime(2026, 9, 23, 10, 0), 5700.0),   # earlier timestamp, inserted last
        ])
        from utils import get_last_spx_value
        result = get_last_spx_value(conn, 2026, 9, 23)
        conn.close()
        self.assertAlmostEqual(result, 5762.48)

    def test_does_not_confuse_same_day_different_year(self):
        # Root-cause regression: 2024-09-23 and 2026-09-23 entries both present.
        # Must return the value for the requested year only.
        conn = self._make_dailylog_db([
            (datetime(2024, 9, 23, 16, 0), 9999.0),   # last year — must NOT be returned
            (datetime(2026, 9, 23, 10, 0), 5762.48),  # this year — correct value
        ])
        from utils import get_last_spx_value
        result = get_last_spx_value(conn, 2026, 9, 23)
        conn.close()
        self.assertAlmostEqual(result, 5762.48)

    def test_returns_none_when_no_data(self):
        conn = self._make_dailylog_db([])
        from utils import get_last_spx_value
        result = get_last_spx_value(conn, 2026, 9, 23)
        conn.close()
        self.assertIsNone(result)

    def test_returns_none_on_sqlite_error(self):
        mock_conn = unittest.mock.MagicMock()
        mock_conn.execute.side_effect = sqlite3.Error("disk I/O error")
        from utils import get_last_spx_value
        result = get_last_spx_value(mock_conn, 2026, 9, 23)
        self.assertIsNone(result)

    def test_returns_none_on_unexpected_exception(self):
        mock_conn = unittest.mock.MagicMock()
        mock_conn.execute.side_effect = RuntimeError("unexpected")
        from utils import get_last_spx_value
        result = get_last_spx_value(mock_conn, 2026, 9, 23)
        self.assertIsNone(result)

    def test_prefers_regular_session_close_over_late_same_day_after_hours_rows(self):
        conn = self._make_dailylog_db([
            (datetime(2026, 4, 2, 16, 3, 13), 6582.69),
            (datetime(2026, 4, 2, 23, 46, 6), 6581.56),
        ])
        from utils import get_last_spx_value
        result = get_last_spx_value(conn, 2026, 4, 2)
        conn.close()
        self.assertAlmostEqual(result, 6582.69)

    def test_falls_back_to_full_day_when_no_regular_session_spx_exists(self):
        conn = self._make_dailylog_db([
            (datetime(2026, 4, 2, 18, 0, 0), 6500.25),
        ])
        from utils import get_last_spx_value
        result = get_last_spx_value(conn, 2026, 4, 2)
        conn.close()
        self.assertAlmostEqual(result, 6500.25)


# ---------------------------------------------------------------------------
# should_post_equity_curve
# ---------------------------------------------------------------------------
class TestShouldPostEquityCurve(unittest.TestCase):
    """
    Dates used:
      2026-04-03 = Friday  (weekday 4)
      2026-04-06 = Monday  (weekday 0)
      2026-04-07 = Tuesday (weekday 1)
      2026-04-08 = Wednesday (weekday 2)
    """

    def _conn_with_trade(self, year, month, day):
        """Return an in-memory connection with one trade on the given date."""
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
                LogDate INTEGER, PL REAL, SPX REAL
            )
        """)
        conn.execute(
            "INSERT INTO Trade VALUES (1,0,0,'Put',0,0,0,0,1,'LIMIT',1,0.2,100,0.1,0,500,0,0,?,?,?,'T1')",
            (year, month, day),
        )
        conn.commit()
        return conn

    def _empty_conn(self):
        """Return an in-memory connection with no trades and no DailyLog rows."""
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
                LogDate INTEGER, PL REAL, SPX REAL
            )
        """)
        conn.commit()
        return conn

    def test_friday_always_posts(self):
        from utils import should_post_equity_curve
        conn = self._empty_conn()
        self.assertTrue(should_post_equity_curve(datetime(2026, 4, 3), conn))
        conn.close()

    def test_monday_friday_was_open_does_not_post(self):
        from utils import should_post_equity_curve
        # Friday 2026-04-03 had a trade → it was an open market day
        conn = self._conn_with_trade(2026, 4, 3)
        self.assertFalse(should_post_equity_curve(datetime(2026, 4, 6), conn))
        conn.close()

    def test_monday_friday_was_closed_no_days_between_posts(self):
        from utils import should_post_equity_curve
        # Friday 2026-04-03 had no trade (closed); no trades on the weekend → Mon posts
        conn = self._empty_conn()
        self.assertTrue(should_post_equity_curve(datetime(2026, 4, 6), conn))
        conn.close()

    def test_tuesday_friday_was_closed_monday_was_trading_day_does_not_post(self):
        from utils import should_post_equity_curve
        # Friday closed, Monday was open → Monday was the "first open day", Tuesday should not post
        conn = self._conn_with_trade(2026, 4, 6)  # Monday trade only (Friday empty)
        self.assertFalse(should_post_equity_curve(datetime(2026, 4, 7), conn))
        conn.close()

    def test_wednesday_midweek_does_not_post(self):
        from utils import should_post_equity_curve
        # Friday 2026-03-27 was open (has trade); Wednesday 2026-04-01 is mid-week → no post
        conn = self._conn_with_trade(2026, 3, 27)
        self.assertFalse(should_post_equity_curve(datetime(2026, 4, 1), conn))
        conn.close()


if __name__ == "__main__":
    unittest.main()
