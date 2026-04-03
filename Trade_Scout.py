import argparse
import logging
import os
import sys
from datetime import datetime
from typing import Dict, Optional

import pandas as pd
try:
    from dotenv import load_dotenv
except ImportError:
    def load_dotenv():  # type: ignore[misc]
        pass

# Add the project root to the Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, project_root)

from db_handler import connect_db, get_spx_data_from_db, get_trades, get_trades_by_type
from discord_messenger import delete_messages, send_message_to_discord
from PL_Summary import calculate_premium_captured_over_range
from utils import (
    calculate_metrics,
    format_message,
    get_last_spx_value,
    get_most_recent_monday,
    get_specified_date,
    input_with_timeout,
)

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Weekly summary report (kept for reference / future use)
# ---------------------------------------------------------------------------

def calculate_trade_stats(df: pd.DataFrame, trade_type: str) -> Dict:
    """
    Compute summary statistics for *df* (a DataFrame of trades of *trade_type*).
    Works on a copy so the caller's DataFrame is never mutated.
    """
    df = df.copy()

    if df.empty:
        return {
            "title": f"No {trade_type} trades found for the selected period.",
            "stats": {
                "Total P/L": "$0",
                "Win Rate": "N/A",
                "Avg P/L per trade": "$0",
                "Avg Duration": "N/A",
                "Total Contracts": 0,
            },
        }

    total_pl = df['PL'].sum()
    wins = df[df['PL'] > 0]
    win_rate = (len(wins) / len(df)) * 100
    avg_pl = df['PL'].mean()

    df['OpenDate'] = pd.to_datetime(df['OpenDate'])
    df['CloseDate'] = pd.to_datetime(df['CloseDate'])
    df['Duration'] = (df['CloseDate'] - df['OpenDate']).dt.days
    avg_duration = df['Duration'].mean()
    total_contracts = df['Contracts'].sum()

    return {
        "title": f"{trade_type} Trades Analysis",
        "stats": {
            "Total P/L": f"${total_pl:,.2f}",
            "Win Rate": f"{win_rate:.2f}%",
            "Avg P/L per trade": f"${avg_pl:,.2f}",
            "Avg Duration": f"{avg_duration:.2f} days",
            "Total Contracts": str(total_contracts),
        },
    }


def create_trade_scout_message(start_date: datetime) -> str:
    """Build a formatted weekly summary Discord message starting at *start_date*."""
    with connect_db() as conn:
        puts_df = get_trades_by_type('PutSpread', start_date, conn)
        calls_df = get_trades_by_type('CallSpread', start_date, conn)
        ironfly_df = get_trades_by_type('IronFly', start_date, conn)
        spx_data = get_spx_data_from_db(conn)

    put_stats = calculate_trade_stats(puts_df, "PutSpread")
    call_stats = calculate_trade_stats(calls_df, "CallSpread")
    ironfly_stats = calculate_trade_stats(ironfly_df, "IronFly")

    total_pl = (
        (puts_df['PL'].sum() if not puts_df.empty else 0)
        + (calls_df['PL'].sum() if not calls_df.empty else 0)
        + (ironfly_df['PL'].sum() if not ironfly_df.empty else 0)
    )

    total_puts = int(puts_df['Contracts'].sum()) if not puts_df.empty else 0
    total_calls = int(calls_df['Contracts'].sum()) if not calls_df.empty else 0
    pcr_display: str = f"{total_puts / total_calls:.2f}" if total_calls > 0 else "N/A"

    spx_close = spx_data['SPX_Close'].iloc[-1] if not spx_data.empty else 'N/A'

    message = "## TradeScout Weekly Report\n"
    message += f"**Analysis since:** {start_date.strftime('%Y-%m-%d')}\n\n"
    message += "### Overview\n"
    message += f"- **Total P/L:** ${total_pl:,.2f}\n"
    message += f"- **Put/Call Ratio (contracts):** {pcr_display}\n"
    message += f"- SPX closed at: {spx_close}\n\n"
    message += "---\n"

    message += f"### {put_stats['title']}\n"
    if puts_df.empty:
        message += f"*{put_stats['title']}*\n"
    else:
        for key, value in put_stats['stats'].items():
            message += f"- **{key}:** {value}\n"

    message += f"\n### {call_stats['title']}\n"
    if calls_df.empty:
        message += f"*{call_stats['title']}*\n"
    else:
        for key, value in call_stats['stats'].items():
            message += f"- **{key}:** {value}\n"

    if not ironfly_df.empty:
        message += f"\n### {ironfly_stats['title']}\n"
        for key, value in ironfly_stats['stats'].items():
            message += f"- **{key}:** {value}\n"

    return message


# ---------------------------------------------------------------------------
# Entry point — daily trade report (original format)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="TradeScout: Analyze daily trades and post to Discord."
    )
    parser.add_argument(
        '--date', type=str, nargs='?', const=None,
        help="Date in YYYYMMDD format (e.g. 20240920). Defaults to today.",
    )
    parser.add_argument(
        '--win', type=str, choices=['max', 'restore'], default='max',
        help="TAT window state before screenshot: max or restore (default: max).",
    )
    parser.add_argument(
        '--noimage', action='store_true',
        help="Skip the screenshot attachment.",
    )
    parser.add_argument(
        '--debug', action='store_true',
        help="Print message to console instead of posting to Discord.",
    )
    args = parser.parse_args()

    specified_date = get_specified_date(args.date)
    year, month, day = specified_date.year, specified_date.month, specified_date.day

    with connect_db() as connection:
        most_recent_monday = get_most_recent_monday(specified_date)
        weekly_pl = calculate_premium_captured_over_range(
            most_recent_monday, specified_date, connection
        )

        first_day_of_month = specified_date.replace(day=1)
        monthly_pl = calculate_premium_captured_over_range(
            first_day_of_month, specified_date, connection
        )

        df_trades_ordered = get_trades(connection, year, month, day)
        (
            premium_sold, premium_captured, pcr,
            win_rate, expired_trades, stops,
            bad_slip, bad_slip_max, negative_exp,
        ) = calculate_metrics(df_trades_ordered)

        spx_last = get_last_spx_value(connection, year, month, day)

    formatted_message = format_message(
        specified_date, premium_sold, premium_captured, pcr, win_rate,
        expired_trades, stops, bad_slip, bad_slip_max, spx_last,
        negative_exp, weekly_pl, monthly_pl,
    )

    message_ids = send_message_to_discord(formatted_message, args.noimage, args.win, args.debug)

    user_input = input_with_timeout("Do you want to delete the posting? (Y/N): ", 30)
    if user_input and user_input.strip().lower() in ['yes', 'y']:
        delete_messages(message_ids)
