import calendar
import logging
import os
import sqlite3
import sys
import tempfile
import threading
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd
import yaml

logger = logging.getLogger(__name__)

# Windows FILETIME constants
FILETIME_EPOCH_OFFSET = 116_444_736_000_000_000  # 100-ns ticks between 1601-01-01 and 1970-01-01
FILETIME_TICKS_PER_SECOND = 10_000_000


def load_yaml_config() -> dict:
    """Load config.yaml from the config/ folder next to this script (or next to the executable)."""
    if hasattr(sys, '_MEIPASS'):
        config_path = os.path.join(os.path.dirname(sys.executable), 'config.yaml')
    else:
        script_dir = os.path.dirname(__file__)
        config_path = os.path.join(script_dir, 'config', 'config.yaml')

    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Configuration file not found at {config_path}")

    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


def take_screenshot_of_app(app_name: str, win: str) -> Optional[str]:
    """Screenshot the named application window; returns a temp file path or None on failure."""
    import pyautogui  # noqa: PLC0415 — GUI dependency; import lazily so tests can run without it
    import pygetwindow as gw  # noqa: PLC0415

    try:
        app_windows = [w for w in gw.getWindowsWithTitle(app_name) if w.title == app_name]

        if not app_windows:
            logger.warning("Application window '%s' not found.", app_name)
            return None

        app_window = app_windows[0]

        if win == 'max' and not app_window.isMaximized:
            app_window.maximize()
        elif win == 'restore' and app_window.isMinimized:
            app_window.restore()

        app_window.activate()
        pyautogui.sleep(2)

        if not app_window.isActive:
            logger.warning("Application window '%s' is not active.", app_name)
            return None

        screenshot = pyautogui.screenshot(
            region=(app_window.left, app_window.top, app_window.width, app_window.height)
        )
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.png')
        screenshot.save(temp_file.name)
        temp_file.close()
        return temp_file.name

    except IndexError:
        logger.warning("Application window '%s' not found.", app_name)
        return None
    except Exception as e:
        logger.error("Error capturing screenshot of '%s': %s", app_name, e)
        return None


def convert_to_human_readable(bigint_timestamp: int) -> datetime:
    """
    Convert a Windows FILETIME integer to a datetime, forcing the year to the
    current system year. TAT stores dates with a fixed year; this normalises them.
    """
    unix_time = (bigint_timestamp - FILETIME_EPOCH_OFFSET) / FILETIME_TICKS_PER_SECOND
    dt = datetime(1970, 1, 1) + timedelta(seconds=unix_time)
    return dt.replace(year=datetime.now().year)


def convert_filetime_series(series: pd.Series) -> pd.Series:
    """
    Vectorized FILETIME → datetime conversion for whole DataFrame columns.

    The subtraction and division run in NumPy space; datetime construction uses
    .apply() because some TAT tables (DailyLog) store .NET DateTime ticks whose
    unix-second values (~5×10¹⁰) exceed pandas Timestamp's max year (~2262).
    Python datetime + timedelta handles arbitrary years; year-replacement then
    normalises them to the current system year, matching convert_to_human_readable.
    """
    unix_s = (series - FILETIME_EPOCH_OFFSET) / FILETIME_TICKS_PER_SECOND
    current_year = datetime.now().year
    epoch = datetime(1970, 1, 1)
    return unix_s.apply(lambda s: (epoch + timedelta(seconds=s)).replace(year=current_year))


def get_most_recent_monday(date: Optional[datetime] = None) -> datetime:
    """
    Return the most recent Monday on or before *date*.
    Defaults to today when *date* is omitted.

    Formula: date.weekday() gives 0 for Monday … 6 for Sunday.
    Subtracting weekday() days always lands on the preceding (or same) Monday.
    """
    if date is None:
        date = datetime.today()
    days_ago = date.weekday()  # Monday=0 → stay; Sunday=6 → go back 6
    return date - timedelta(days=days_ago)


def get_specified_date(date_str: Optional[str] = None) -> datetime:
    """Parse a YYYYMMDD string to datetime, or return now() if omitted."""
    if date_str:
        return datetime.strptime(date_str, "%Y%m%d")
    return datetime.now()


def format_message(
    date: datetime,
    premium_sold: float,
    premium_captured: float,
    pcr: float,
    win_rate: float,
    expired_trades: int,
    stops: int,
    bad_slip: int,
    bad_slip_max: Optional[float],
    spx_last: Optional[float],
    negative_exp: int,
    weekly_pl: float,
    monthly_pl: float,
) -> str:
    ALIGN_WIDTH = 12

    premium_sold_str = f"${premium_sold:,.2f}"
    premium_captured_str = (
        f"(${abs(premium_captured):,.2f})" if premium_captured < 0 else f"${premium_captured:,.2f}"
    )
    pcr_str = f"{pcr:.2f}%"
    win_rate_str = f"{win_rate:.2f}%"
    exp_stp_str = f"{expired_trades}:{stops}"
    bad_slip_str = f"{int(bad_slip):,}"
    bad_slip_max_str = f"{abs(bad_slip_max):,.2f}" if bad_slip_max else ""
    spx_last_str = f"{spx_last:,.2f}" if spx_last is not None else ""

    combined_bad_slip = f"{bad_slip_str}({bad_slip_max_str} max)" if bad_slip_max else bad_slip_str
    bad_slip_combined_str = f"{combined_bad_slip:>{ALIGN_WIDTH}}"

    negative_exp_str = str(negative_exp)
    weekly_pl_str = f"(${abs(weekly_pl):,.2f})" if weekly_pl < 0 else f"${weekly_pl:,.2f}"
    monthly_pl_str = f"(${abs(monthly_pl):,.2f})" if monthly_pl < 0 else f"${monthly_pl:,.2f}"

    formatted_date = date.strftime("%Y %b %d")
    day_of_week = calendar.day_name[date.weekday()]
    full_date_header = f"{formatted_date} ({day_of_week})"

    message = "\n\n" + "```" + f"""
{full_date_header}
----------|------------
SPX Last  | {spx_last_str:>{ALIGN_WIDTH}}
Prem Sold | {premium_sold_str:>{ALIGN_WIDTH}}
Prem Cap  | {premium_captured_str:>{ALIGN_WIDTH}}
PCR       | {pcr_str:>{ALIGN_WIDTH}}
Win %     | {win_rate_str:>{ALIGN_WIDTH}}
Exp : Stp | {exp_stp_str:>{ALIGN_WIDTH}}
Bad Slip  | {bad_slip_combined_str:>{ALIGN_WIDTH}}
-ve Exprd | {negative_exp_str:>{ALIGN_WIDTH}}
WTD PL    | {weekly_pl_str:>{ALIGN_WIDTH}}
MTD PL    | {monthly_pl_str:>{ALIGN_WIDTH}}
""" + "```"
    return message


def calculate_metrics(
    df_trades_ordered: pd.DataFrame,
) -> tuple:
    """
    Compute trade performance metrics from a DataFrame of TAT trades.

    Returns: (premium_sold, premium_captured, pcr, win_rate,
               expired_trades, stops, bad_slip, bad_slip_max, negative_exp)
    """
    premium_sold = df_trades_ordered['TotalPremium'].sum()
    premium_captured = df_trades_ordered['ProfitLoss'].sum()

    if premium_sold != 0:
        pcr = (premium_captured / premium_sold) * 100
        win_rate = (df_trades_ordered['ProfitLoss'] > 0).mean() * 100
    else:
        pcr, win_rate = 0.0, 0.0

    expired_trades = int((df_trades_ordered['ClosingProcessed'] == 0).sum())
    stops = int((df_trades_ordered['ClosingProcessed'] == 1).sum())

    bad_slip_data = df_trades_ordered['PriceClose'].abs() - df_trades_ordered['PriceStopTarget']
    bad_slip_condition = bad_slip_data >= 0.50
    bad_slip = int(bad_slip_condition.sum())
    bad_slip_max = float(bad_slip_data[bad_slip_condition].max()) if bad_slip > 0 else 0.0

    negative_exp = int(
        df_trades_ordered[
            (df_trades_ordered['ClosingProcessed'] == 0) & (df_trades_ordered['ProfitLoss'] < 0)
        ].shape[0]
    )

    return (
        premium_sold, premium_captured, pcr, win_rate,
        expired_trades, stops, bad_slip, bad_slip_max, negative_exp,
    )


def input_with_timeout(prompt: str, timeout: int) -> Optional[str]:
    """Read a line from stdin with a timeout; returns None if the timeout expires."""
    answer: list = [None]

    def _get_input() -> None:
        answer[0] = input(prompt)

    thread = threading.Thread(target=_get_input)
    thread.daemon = True
    thread.start()
    thread.join(timeout)

    if thread.is_alive():
        logger.info("No response received within %ds. Proceeding without deletion.", timeout)
        return None
    return answer[0]


def get_last_spx_value(
    connection: sqlite3.Connection, year: int, month: int, day: int
) -> Optional[float]:
    """Return the last SPX value recorded in DailyLog for the given date, or None."""
    try:
        target_date = datetime(year, month, day)
        query = "SELECT DailyLogID, LogDate, PL, SPX FROM DailyLog WHERE LogDate IS NOT NULL;"
        df = pd.read_sql_query(query, connection)
        df['LogDate'] = pd.to_datetime(df['LogDate'].apply(convert_to_human_readable))
        df_day = df[df['LogDate'].dt.date == target_date.date()].sort_values('LogDate')

        if not df_day.empty:
            value = float(df_day.iloc[-1]['SPX'])
            logger.info("Last SPX value for %s: %s", target_date.date(), value)
            return value

        logger.info("No SPX value found for %s.", target_date.date())
        return None

    except sqlite3.Error as e:
        logger.error("Database error during SPX lookup: %s", e)
        return None
    except Exception as e:
        logger.error("Unexpected error during SPX lookup: %s", e)
        return None
