"""Unit tests for Trade_Scout.py — database and Discord calls are mocked."""
import os
import sys
import unittest
from contextlib import contextmanager
from datetime import datetime
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pandas as pd

from Trade_Scout import calculate_trade_stats, create_trade_scout_message


@contextmanager
def _noop_connect(*args, **kwargs):
    """Stub for connect_db — yields a dummy connection so unit tests need no DB."""
    yield MagicMock()


def _trade_df(rows):
    """Build a minimal trades DataFrame with the columns expected by calculate_trade_stats."""
    return pd.DataFrame(rows, columns=['PL', 'OpenDate', 'CloseDate', 'Contracts'])


def _empty_trade_df():
    return _trade_df([])


def _spx_df(value: float = 5762.48):
    return pd.DataFrame({'SPX_Close': [value], 'LogDate': [datetime(2024, 9, 23)]})


def _empty_spx_df():
    return pd.DataFrame(columns=['SPX_Close', 'LogDate'])


# ---------------------------------------------------------------------------
# calculate_trade_stats
# ---------------------------------------------------------------------------
class TestCalculateTradeStats(unittest.TestCase):
    def test_empty_df_title_mentions_trade_type(self):
        result = calculate_trade_stats(_empty_trade_df(), "Put")
        self.assertIn("Put", result['title'])

    def test_empty_df_default_stats(self):
        result = calculate_trade_stats(_empty_trade_df(), "Call")
        self.assertEqual(result['stats']['Total P/L'], '$0')
        self.assertEqual(result['stats']['Win Rate'], 'N/A')

    def test_total_pl_summed_correctly(self):
        df = _trade_df([
            (500.0, '2024-09-23 09:00', '2024-09-23 16:00', 2),
            (-200.0, '2024-09-23 09:00', '2024-09-23 16:00', 1),
        ])
        result = calculate_trade_stats(df, "Put")
        self.assertEqual(result['stats']['Total P/L'], '$300.00')

    def test_win_rate_two_out_of_three(self):
        df = _trade_df([
            (100.0, '2024-09-23 09:00', '2024-09-23 16:00', 1),
            (50.0,  '2024-09-23 09:00', '2024-09-23 16:00', 1),
            (-30.0, '2024-09-23 09:00', '2024-09-23 16:00', 1),
        ])
        result = calculate_trade_stats(df, "Call")
        self.assertEqual(result['stats']['Win Rate'], '66.67%')

    def test_total_contracts_summed(self):
        df = _trade_df([
            (100.0, '2024-09-23 09:00', '2024-09-23 16:00', 3),
            (50.0,  '2024-09-23 09:00', '2024-09-23 16:00', 2),
        ])
        result = calculate_trade_stats(df, "Put")
        self.assertEqual(result['stats']['Total Contracts'], '5')

    def test_does_not_mutate_input_dataframe(self):
        df = _trade_df([
            (100.0, '2024-09-23 09:00', '2024-09-23 16:00', 1),
        ])
        original_columns = set(df.columns)
        original_dtypes = df.dtypes.to_dict()
        calculate_trade_stats(df, "Put")
        # Columns and dtypes must be unchanged on the original
        self.assertEqual(set(df.columns), original_columns)
        self.assertEqual(df.dtypes.to_dict(), original_dtypes)

    def test_negative_total_pl_formatted_correctly(self):
        df = _trade_df([
            (-1500.50, '2024-09-23 09:00', '2024-09-23 16:00', 1),
        ])
        result = calculate_trade_stats(df, "Put")
        self.assertIn('-', result['stats']['Total P/L'])

    def test_avg_duration_computed(self):
        # Open 9 AM, close 4 PM same day → 0 days (integer floor)
        df = _trade_df([
            (100.0, '2024-09-23 09:00', '2024-09-24 09:00', 1),  # 1-day trade
        ])
        result = calculate_trade_stats(df, "Put")
        self.assertIn('1.00 days', result['stats']['Avg Duration'])


# ---------------------------------------------------------------------------
# create_trade_scout_message
# ---------------------------------------------------------------------------
class TestCreateTradeScoutMessage(unittest.TestCase):
    @patch('Trade_Scout.connect_db', new=_noop_connect)
    @patch('Trade_Scout.get_spx_data_from_db')
    @patch('Trade_Scout.get_trades_by_type')
    def test_contains_analysis_since_date(self, mock_trades, mock_spx):
        mock_trades.return_value = _empty_trade_df()
        mock_spx.return_value = _spx_df()
        msg = create_trade_scout_message(datetime(2024, 9, 23))
        self.assertIn('2024-09-23', msg)

    @patch('Trade_Scout.connect_db', new=_noop_connect)
    @patch('Trade_Scout.get_spx_data_from_db')
    @patch('Trade_Scout.get_trades_by_type')
    def test_contains_spx_value(self, mock_trades, mock_spx):
        mock_trades.return_value = _empty_trade_df()
        mock_spx.return_value = _spx_df(5762.48)
        msg = create_trade_scout_message(datetime(2024, 9, 23))
        self.assertIn('5762.48', msg)

    @patch('Trade_Scout.connect_db', new=_noop_connect)
    @patch('Trade_Scout.get_spx_data_from_db')
    @patch('Trade_Scout.get_trades_by_type')
    def test_empty_spx_shows_na(self, mock_trades, mock_spx):
        mock_trades.return_value = _empty_trade_df()
        mock_spx.return_value = _empty_spx_df()
        msg = create_trade_scout_message(datetime(2024, 9, 23))
        self.assertIn('N/A', msg)

    @patch('Trade_Scout.connect_db', new=_noop_connect)
    @patch('Trade_Scout.get_spx_data_from_db')
    @patch('Trade_Scout.get_trades_by_type')
    def test_calls_get_trades_for_both_types(self, mock_trades, mock_spx):
        mock_trades.return_value = _empty_trade_df()
        mock_spx.return_value = _empty_spx_df()
        create_trade_scout_message(datetime(2024, 9, 23))
        calls = [c[0][0] for c in mock_trades.call_args_list]
        self.assertIn('PutSpread', calls)
        self.assertIn('CallSpread', calls)
        self.assertIn('IronFly', calls)

    @patch('Trade_Scout.connect_db', new=_noop_connect)
    @patch('Trade_Scout.get_spx_data_from_db')
    @patch('Trade_Scout.get_trades_by_type')
    def test_total_pl_shown_in_message(self, mock_trades, mock_spx):
        def _side(trade_type, _date, _conn=None):
            return _trade_df([(200.0, '2024-09-23 09:00', '2024-09-23 16:00', 1)])

        mock_trades.side_effect = _side
        mock_spx.return_value = _spx_df()
        msg = create_trade_scout_message(datetime(2024, 9, 23))
        # Puts + Calls + IronFly = 600
        self.assertIn('600.00', msg)

    @patch('Trade_Scout.connect_db', new=_noop_connect)
    @patch('Trade_Scout.get_spx_data_from_db')
    @patch('Trade_Scout.get_trades_by_type')
    def test_pcr_na_when_no_calls(self, mock_trades, mock_spx):
        def _side(trade_type, _date, _conn=None):
            if trade_type == 'Put':
                return _trade_df([(100.0, '2024-09-23 09:00', '2024-09-23 16:00', 2)])
            return _empty_trade_df()

        mock_trades.side_effect = _side
        mock_spx.return_value = _spx_df()
        msg = create_trade_scout_message(datetime(2024, 9, 23))
        self.assertIn('N/A', msg)


if __name__ == "__main__":
    unittest.main()
