import logging
import sqlite3
import tempfile
from datetime import datetime, timedelta
from typing import Optional

import matplotlib
matplotlib.use('Agg')  # non-interactive backend — must be set before pyplot import
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd

from db_handler import get_trades_range

logger = logging.getLogger(__name__)


def generate_equity_curve(
    connection: sqlite3.Connection, target_date: datetime, days: int = 60
) -> Optional[str]:
    """
    Generate an equity curve PNG for the last *days* calendar days ending on *target_date*.

    Queries the Trade table, aggregates ProfitLoss by calendar day, and plots
    the cumulative P&L as a line chart with a dark background.

    Returns a temp file path (caller is responsible for cleanup), or None if
    there is no trade data or chart generation fails.
    """
    start_date = target_date - timedelta(days=days - 1)
    df = get_trades_range(connection, start_date, target_date)

    if df.empty:
        logger.info(
            "No trade data for equity curve (%d days ending %s).", days, target_date.date()
        )
        return None

    df['date'] = df.apply(
        lambda r: datetime(int(r['Year']), int(r['Month']), int(r['Day'])), axis=1
    )
    daily = df.groupby('date')['ProfitLoss'].sum().reset_index()
    daily = daily.sort_values('date').reset_index(drop=True)
    daily['cumulative_pl'] = daily['ProfitLoss'].cumsum()

    try:
        fig, ax = plt.subplots(figsize=(10, 4))

        ax.plot(
            daily['date'], daily['cumulative_pl'],
            color='#2ecc71', linewidth=2, zorder=3
        )
        ax.axhline(0, color='#888', linewidth=0.8, linestyle='--', zorder=2)
        ax.fill_between(
            daily['date'], daily['cumulative_pl'], 0,
            where=(daily['cumulative_pl'] >= 0),
            alpha=0.15, color='#2ecc71', zorder=1
        )
        ax.fill_between(
            daily['date'], daily['cumulative_pl'], 0,
            where=(daily['cumulative_pl'] < 0),
            alpha=0.20, color='#e74c3c', zorder=1
        )

        ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %d'))
        ax.xaxis.set_major_locator(mdates.WeekdayLocator(byweekday=0, interval=2))
        fig.autofmt_xdate(rotation=30, ha='right')
        ax.yaxis.set_major_formatter(
            plt.FuncFormatter(lambda v, _: f'${v:,.0f}')
        )

        ax.set_title(f'Equity Curve — last {days} days', fontsize=11, pad=10)

        # Dark theme
        bg_dark = '#1e1e2e'
        ax.set_facecolor(bg_dark)
        fig.patch.set_facecolor('#16161e')
        for spine in ax.spines.values():
            spine.set_color('#444')
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        ax.tick_params(axis='both', colors='#aaa', labelsize=9)
        ax.title.set_color('#ddd')
        ax.yaxis.label.set_color('#aaa')
        ax.xaxis.label.set_color('#aaa')

        plt.tight_layout()

        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.png')
        fig.savefig(temp_file.name, dpi=150, bbox_inches='tight',
                    facecolor=fig.get_facecolor())
        temp_file.close()
        plt.close(fig)

        logger.info("Equity curve saved to %s", temp_file.name)
        return temp_file.name

    except Exception as e:
        logger.error("Failed to generate equity curve: %s", e)
        plt.close('all')
        return None
