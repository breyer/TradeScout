import argparse
import os
import sys
from datetime import datetime

import pandas as pd
from discord_messenger import send_discord_message
from dotenv import load_dotenv

# Add the project root to the Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, project_root)

from db_handler import get_spx_data_from_db, get_trades
from utils import get_most_recent_monday

# Load environment variables
load_dotenv()
DISCORD_WEBHOOK_URL = os.getenv('DISCORD_WEBHOOK_URL')

def calculate_trade_stats(df, trade_type):
    """Calculates statistics for a given set of trades."""
    if df.empty:
        return {
            "title": f"No {trade_type} trades found for the selected period.",
            "stats": {
                "Total P/L": "$0",
                "Win Rate": "N/A",
                "Avg P/L per trade": "$0",
                "Avg Duration": "N/A",
                "Total Contracts": 0
            }
        }

    total_pl = df['PL'].sum()
    wins = df[df['PL'] > 0]
    win_rate = (len(wins) / len(df)) * 100 if not df.empty else 0
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
            "Total Contracts": f"{total_contracts}"
        }
    }

def create_trade_scout_message(start_date):
    """
    Creates a formatted message with calculated trade statistics.
    
    Args:
        start_date (datetime.date): The start date for fetching trades.
    """
    
    # Fetch data
    puts_df = get_trades('Put', start_date)
    calls_df = get_trades('Call', start_date)
    spx_data = get_spx_data_from_db()

    # Calculate stats
    put_stats = calculate_trade_stats(puts_df, "Put")
    call_stats = calculate_trade_stats(calls_df, "Call")
    
    # Combine P/L
    total_pl = (puts_df['PL'].sum() if not puts_df.empty else 0) + \
               (calls_df['PL'].sum() if not calls_df.empty else 0)
    
    # Put/Call Ratio
    total_puts = puts_df['Contracts'].sum() if not puts_df.empty else 0
    total_calls = calls_df['Contracts'].sum() if not calls_df.empty else 0
    pcr = total_puts / total_calls if total_calls > 0 else "N/A"
    pcr_text = f"{pcr:.2f}" if isinstance(pcr, float) else pcr

    # SPX Info
    spx_close = spx_data['SPX_Close'].iloc[-1] if not spx_data.empty else 'N/A'
    spx_message = f"SPX closed at: {spx_close}"

    # Construct the message
    message = f"##  ट्रेड स्काउट सप्ताहिक रिपोर्ट\n"
    message += f"**Analysis since:** {start_date.strftime('%Y-%m-%d')}\n\n"
    message += "### 종합 개요\n"
    message += f"- **총 손익:** ${total_pl:,.2f}\n"
    message += f"- **풋/콜 비율 (계약 기준):** {pcr_text}\n"
    message += f"- {spx_message}\n\n"
    
    message += "--- \n"
    
    message += f"### {put_stats['title']}\n"
    if puts_df.empty:
        message += f"*{put_stats['title']}*\n"
    else:
        for key, value in put_stats['stats'].items():
            message += f"- **{key}:** {value}\n"
            
    message += "\n"
    message += f"### {call_stats['title']}\n"
    if calls_df.empty:
        message += f"*{call_stats['title']}*\n"
    else:
        for key, value in call_stats['stats'].items():
            message += f"- **{key}:** {value}\n"
            
    return message

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="TradeScout: Analyze and report trade performance.")
    parser.add_argument(
        '--start-date',
        type=str,
        help="Start date for trade analysis in YYYY-MM-DD format. Defaults to the most recent Monday."
    )
    
    args = parser.parse_args()
    
    if args.start_date:
        try:
            start_date = datetime.strptime(args.start_date, '%Y-%m-%d').date()
        except ValueError:
            print("Error: Date format must be YYYY-MM-DD.")
            sys.exit(1)
    else:
        start_date = get_most_recent_monday()

    # Generate and send the message
    trade_scout_message = create_trade_scout_message(start_date)
    print(trade_scout_message)
    # send_discord_message(DISCORD_WEBHOOK_URL, trade_scout_message)
