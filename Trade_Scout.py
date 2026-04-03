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

from db_handler import connect_db, get_spx_data_from_db, get_trades_by_type
from discord_messenger import send_message_to_discord
from utils import get_most_recent_monday

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
logger = logging.getLogger(__name__)


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
    """Build the formatted Discord message for the week starting at *start_date*."""
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
    pcr_display: str
    if total_calls > 0:
        pcr_display = f"{total_puts / total_calls:.2f}"
    else:
        pcr_display = "N/A"

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


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="TradeScout: Analyze and report trade performance."
    )
    parser.add_argument(
        '--start-date',
        type=str,
        help="Start date in YYYY-MM-DD format. Defaults to the most recent Monday.",
    )
    parser.add_argument(
        '--win',
        type=str,
        choices=['max', 'restore'],
        default='max',
        help="TAT window state before screenshot: max or restore (default: max).",
    )
    parser.add_argument(
        '--noimage',
        action='store_true',
        help="Skip the screenshot attachment.",
    )
    args = parser.parse_args()

    if args.start_date:
        try:
            start_date: datetime = datetime.strptime(args.start_date, '%Y-%m-%d')
        except ValueError:
            logger.error("Date format must be YYYY-MM-DD.")
            sys.exit(1)
    else:
        start_date = get_most_recent_monday()

    message = create_trade_scout_message(start_date)
    logger.info("Sending report to Discord...")
    send_message_to_discord(message, noimage=args.noimage, win=args.win)
