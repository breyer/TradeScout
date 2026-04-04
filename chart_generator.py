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

from db_handler import get_dailylog_for_date, get_trades_range

logger = logging.getLogger(__name__)


def generate_daily_chart(
    connection: sqlite3.Connection, target_date: datetime
) -> Optional[str]:
    """
    Generate a TAT-style intraday PnL timeline PNG for *target_date*.

    Two lines on the left dollar axis, SPX on a right axis:
      - Blue step   : PremiumSold  (cumulative premium collected as trades open)
      - Orange line : PL           (running mark-to-market P&L from DailyLog)
      - Gray line   : SPX price    (right axis)

    Returns a temp file path (caller is responsible for cleanup), or None on failure.
    """
    df_log = get_dailylog_for_date(connection, target_date)
    if df_log.empty:
        logger.info("No DailyLog data for daily chart on %s.", target_date.date())
        return None

    try:
        bg_dark    = '#1e1e2e'
        bg_fig     = '#16161e'
        col_blue   = '#4a9eff'
        col_orange = '#ff8c42'
        col_spx    = '#888899'
        col_grid   = '#2a2a3a'
        col_tick   = '#aaaaaa'

        fig, ax = plt.subplots(figsize=(14, 5))
        fig.patch.set_facecolor(bg_fig)
        ax.set_facecolor(bg_dark)

        # Orange: DailyLog PL (mark-to-market)
        ax.plot(
            df_log['time'], df_log['PL'],
            color=col_orange, linewidth=1.2, zorder=3, label='PL'
        )

        # Blue step: cumulative PremiumSold
        ax.step(
            df_log['time'], df_log['PremiumSold'],
            color=col_blue, linewidth=1.8, zorder=4, where='post', label='Premium Sold'
        )

        # Zero baseline
        ax.axhline(0, color='#555566', linewidth=0.8, linestyle='--', zorder=2)

        # Annotations: final PL + minimum PL
        final_pl   = float(df_log['PL'].iloc[-1])
        final_time = df_log['time'].iloc[-1]
        ax.annotate(
            f"Final PnL: ${final_pl:,.2f}",
            xy=(final_time, final_pl),
            xytext=(10, 0), textcoords='offset points',
            color='#00e676', fontsize=8, fontweight='bold',
            va='center',
        )
        ax.plot(final_time, final_pl, 'o', color='#00e676', markersize=6, zorder=6)

        min_idx  = df_log['PL'].idxmin()
        min_pl   = float(df_log['PL'].iloc[min_idx])
        min_time = df_log['time'].iloc[min_idx]
        if min_pl < 0:
            ax.annotate(
                f"{min_time.strftime('%H:%M')}, ${min_pl:,.2f}",
                xy=(min_time, min_pl),
                xytext=(6, -14), textcoords='offset points',
                color=col_orange, fontsize=8,
                va='top',
            )

        # Right axis: SPX
        ax_spx = ax.twinx()
        ax_spx.plot(
            df_log['time'], df_log['SPX'],
            color=col_spx, linewidth=1.0, zorder=2, label='SPX', alpha=0.7
        )
        ax_spx.set_facecolor(bg_dark)
        ax_spx.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f'{v:,.0f}'))
        ax_spx.tick_params(axis='y', colors=col_spx, labelsize=9)
        ax_spx.spines['right'].set_color('#3a3a4a')
        ax_spx.spines['top'].set_visible(False)

        # Left axis formatting
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
        ax.xaxis.set_major_locator(mdates.HourLocator(interval=1))
        fig.autofmt_xdate(rotation=0, ha='center')
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f'${v:,.0f}'))

        ax.set_title(
            f"{target_date.strftime('%Y%m%d')} PnL Timeline",
            fontsize=12, pad=12, fontweight='bold', color='#e0e0e0'
        )

        ax.yaxis.grid(True, color=col_grid, linewidth=0.6, zorder=0)
        ax.set_axisbelow(True)

        for spine in ax.spines.values():
            spine.set_color('#3a3a4a')
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        ax.tick_params(axis='both', colors=col_tick, labelsize=9)

        # Combined legend from both axes
        lines1, labels1 = ax.get_legend_handles_labels()
        lines2, labels2 = ax_spx.get_legend_handles_labels()
        ax.legend(
            lines1 + lines2, labels1 + labels2,
            loc='lower right', fontsize=8,
            facecolor='#2a2a3a', edgecolor='#3a3a4a', labelcolor=col_tick,
        )

        plt.tight_layout(pad=1.2)

        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.png')
        fig.savefig(temp_file.name, dpi=150, bbox_inches='tight',
                    facecolor=fig.get_facecolor())
        temp_file.close()
        plt.close(fig)

        logger.info("Daily chart saved to %s", temp_file.name)
        return temp_file.name

    except Exception as e:
        logger.error("Failed to generate daily chart: %s", e)
        plt.close('all')
        return None


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
        fig, ax = plt.subplots(figsize=(12, 5))

        ax.plot(
            daily['date'], daily['cumulative_pl'],
            color='#2ecc71', linewidth=2.5, zorder=3
        )
        ax.axhline(0, color='#666', linewidth=1.0, linestyle='--', zorder=2)
        ax.fill_between(
            daily['date'], daily['cumulative_pl'], 0,
            where=(daily['cumulative_pl'] >= 0),
            alpha=0.18, color='#2ecc71', zorder=1
        )
        ax.fill_between(
            daily['date'], daily['cumulative_pl'], 0,
            where=(daily['cumulative_pl'] < 0),
            alpha=0.25, color='#e74c3c', zorder=1
        )

        ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %d'))
        ax.xaxis.set_major_locator(mdates.WeekdayLocator(byweekday=0, interval=2))
        fig.autofmt_xdate(rotation=25, ha='right')
        ax.yaxis.set_major_formatter(
            plt.FuncFormatter(lambda v, _: f'${v:,.0f}')
        )

        ax.set_title(f'Equity Curve — last {days} days', fontsize=13, pad=14, fontweight='bold')

        # Subtle grid
        ax.yaxis.grid(True, color='#333', linewidth=0.6, linestyle='-', zorder=0)
        ax.set_axisbelow(True)

        # Dark theme
        bg_dark = '#1e1e2e'
        ax.set_facecolor(bg_dark)
        fig.patch.set_facecolor('#16161e')
        for spine in ax.spines.values():
            spine.set_color('#3a3a4a')
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        ax.tick_params(axis='both', colors='#bbb', labelsize=10)
        ax.title.set_color('#e0e0e0')

        plt.tight_layout(pad=1.5)

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
