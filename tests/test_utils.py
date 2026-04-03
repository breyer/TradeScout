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
class TestGetLastSpxValue(unittest.TestCase):
    def _make_dailylog_db(self, entries):
        conn = sqlite3.connect(":memory:")
        conn.execute("""
            CREATE TABLE DailyLog (
                DailyLogID INTEGER PRIMARY KEY, LogDate INTEGER, PL REAL, SPX REAL
            )
        """)
        for i, (dt, spx) in enumerate(entries, 1):
            ft = _make_filetime(dt)
            conn.execute("INSERT INTO DailyLog VALUES (?, ?, 0, ?)", (i, ft, spx))
        conn.commit()
        return conn

    def test_returns_last_spx_for_date(self):
        # convert_to_human_readable forces year to current year; pass current year here too
        yr = datetime.now().year
        conn = self._make_dailylog_db([
            (datetime(yr, 9, 23, 10, 0), 5700.0),
            (datetime(yr, 9, 23, 16, 0), 5762.48),  # last entry → this value
        ])
        from utils import get_last_spx_value
        result = get_last_spx_value(conn, yr, 9, 23)
        conn.close()
        self.assertAlmostEqual(result, 5762.48)

    def test_returns_latest_timestamp_not_insertion_order(self):
        # Regression: entries inserted with the LATER timestamp first.
        # Before the fix, iloc[-1] returned the last-inserted row (5700.0).
        # After the fix, sort_values('LogDate') ensures the latest timestamp wins (5762.48).
        yr = datetime.now().year
        conn = self._make_dailylog_db([
            (datetime(yr, 9, 23, 16, 0), 5762.48),  # later timestamp, inserted first
            (datetime(yr, 9, 23, 10, 0), 5700.0),   # earlier timestamp, inserted last
        ])
        from utils import get_last_spx_value
        result = get_last_spx_value(conn, yr, 9, 23)
        conn.close()
        self.assertAlmostEqual(result, 5762.48)

    def test_returns_none_when_no_data(self):
        conn = self._make_dailylog_db([])
        from utils import get_last_spx_value
        result = get_last_spx_value(conn, datetime.now().year, 9, 23)
        conn.close()
        self.assertIsNone(result)

    def test_returns_none_on_sqlite_error(self):
        """sqlite3.Error during query must return None, not raise."""
        conn = self._make_dailylog_db([])
        with unittest.mock.patch('utils.pd.read_sql_query', side_effect=sqlite3.Error("disk I/O error")):
            from utils import get_last_spx_value
            result = get_last_spx_value(conn, datetime.now().year, 9, 23)
        conn.close()
        self.assertIsNone(result)

    def test_returns_none_on_unexpected_exception(self):
        """Any unexpected exception during query must return None, not raise."""
        conn = self._make_dailylog_db([])
        with unittest.mock.patch('utils.pd.read_sql_query', side_effect=RuntimeError("unexpected")):
            from utils import get_last_spx_value
            result = get_last_spx_value(conn, datetime.now().year, 9, 23)
        conn.close()
        self.assertIsNone(result)


if __name__ == "__main__":
    unittest.main()
