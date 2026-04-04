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

    Left axis  : PL (orange line) + PremiumSold (blue step)
    Right axis : SPX price (gray line)
    X range    : 09:00 – 16:30 (30 min padding around regular session)

    Annotates the lowest PL, highest PL, and final PnL with markers and labels.
    Returns a temp file path (caller is responsible for cleanup), or None on failure.
    """
    df_log = get_dailylog_for_date(connection, target_date)
    if df_log.empty:
        logger.info("No DailyLog data for daily chart on %s.", target_date.date())
        return None

    # Trim to visible window: 09:00 – 16:30
    d = target_date.date()
    win_start = datetime(d.year, d.month, d.day, 9, 0)
    win_end   = datetime(d.year, d.month, d.day, 16, 30)
    df = df_log[(df_log['time'] >= win_start) & (df_log['time'] <= win_end)].copy()
    if df.empty:
        df = df_log.copy()  # fallback: use all data

    try:
        bg_dark    = '#1a1a2a'
        bg_fig     = '#12121e'
        col_pl     = '#ff9944'
        col_prem   = '#4aa8ff'
        col_spx    = '#778899'
        col_grid   = '#252535'
        col_tick   = '#999aaa'
        col_low    = '#ff4455'
        col_high   = '#00e676'
        col_final  = '#00e676'
        label_bbox = dict(boxstyle='round,pad=0.35', facecolor='#12121e',
                          edgecolor='#444455', alpha=0.90)

        fig, ax = plt.subplots(figsize=(14, 5))
        fig.patch.set_facecolor(bg_fig)
        ax.set_facecolor(bg_dark)

        # Shaded regular session background (09:30 – 16:00)
        mkt_open  = datetime(d.year, d.month, d.day, 9, 30)
        mkt_close = datetime(d.year, d.month, d.day, 16, 0)
        ax.axvspan(mkt_open, mkt_close, color='#ffffff', alpha=0.025, zorder=0)
        ax.axvline(mkt_open,  color='#3a3a5a', linewidth=0.8, linestyle=':', zorder=1)
        ax.axvline(mkt_close, color='#3a3a5a', linewidth=0.8, linestyle=':', zorder=1)

        # PL fill — green above zero, red below
        ax.fill_between(df['time'], df['PL'], 0,
                        where=(df['PL'] >= 0), alpha=0.12, color=col_high, zorder=1)
        ax.fill_between(df['time'], df['PL'], 0,
                        where=(df['PL'] < 0),  alpha=0.15, color=col_low,  zorder=1)

        # Orange PL line
        ax.plot(df['time'], df['PL'],
                color=col_pl, linewidth=1.4, zorder=4, label='PL')

        # Blue PremiumSold step
        ax.step(df['time'], df['PremiumSold'],
                color=col_prem, linewidth=2.0, zorder=5, where='post', label='Premium Sold')

        # Zero baseline
        ax.axhline(0, color='#44445a', linewidth=0.8, linestyle='--', zorder=2)

        # ── Key point annotations ─────────────────────────────────────
        def _annotate(ax, t, val, label, color, y_offset):
            ax.plot(t, val, 'o', color=color, markersize=7, zorder=8,
                    markeredgecolor='#12121e', markeredgewidth=1.2)
            ax.annotate(
                label,
                xy=(t, val), xytext=(0, y_offset), textcoords='offset points',
                color=color, fontsize=8, fontweight='bold', ha='center',
                bbox=label_bbox,
                arrowprops=dict(arrowstyle='->', color=color, lw=1.0),
            )

        pl_series = df['PL']
        min_idx   = pl_series.idxmin()
        max_idx   = pl_series.idxmax()
        min_pl    = float(pl_series[min_idx])
        max_pl    = float(pl_series[max_idx])
        min_time  = df['time'][min_idx]
        max_time  = df['time'][max_idx]
        final_pl  = float(pl_series.iloc[-1])
        final_t   = df['time'].iloc[-1]

        _annotate(ax, min_time, min_pl,
                  f"{min_time.strftime('%H:%M')}  ${min_pl:,.0f}",
                  col_low, -36)

        # Only show max annotation if it's a distinct point from final
        if abs((max_time - final_t).total_seconds()) > 300:
            _annotate(ax, max_time, max_pl,
                      f"{max_time.strftime('%H:%M')}  ${max_pl:,.0f}",
                      col_high, 36)

        _annotate(ax, final_t, final_pl,
                  f"Final  ${final_pl:,.0f}",
                  col_final, 36)

        # ── Right axis: SPX ───────────────────────────────────────────
        ax_spx = ax.twinx()
        ax_spx.plot(df['time'], df['SPX'],
                    color=col_spx, linewidth=1.0, zorder=2, label='SPX', alpha=0.65)
        ax_spx.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f'{v:,.0f}'))
        ax_spx.tick_params(axis='y', colors=col_spx, labelsize=9)
        for sp in ax_spx.spines.values():
            sp.set_color('#2a2a3a')
        ax_spx.spines['top'].set_visible(False)

        # ── Left axis ─────────────────────────────────────────────────
        ax.set_xlim(win_start, win_end)
        ax.xaxis.set_major_locator(mdates.MinuteLocator(byminute=[0, 30]))
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
        fig.autofmt_xdate(rotation=0, ha='center')
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f'${v:,.0f}'))

        day_str = target_date.strftime('%A, %B %-d %Y')
        ax.set_title(f'PnL Timeline — {day_str}',
                     fontsize=12, pad=14, fontweight='bold', color='#e0e0e0')

        fig.text(0.99, 0.97, 'github.com/breyer/TradeScout',
                 ha='right', va='top', fontsize=7.5,
                 color='#555566', style='italic',
                 transform=fig.transFigure)

        ax.yaxis.grid(True, color=col_grid, linewidth=0.5, zorder=0)
        ax.xaxis.grid(True, color=col_grid, linewidth=0.4, zorder=0)
        ax.set_axisbelow(True)

        for spine in ax.spines.values():
            spine.set_color('#2a2a3a')
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        ax.tick_params(axis='both', colors=col_tick, labelsize=9)

        lines1, labels1 = ax.get_legend_handles_labels()
        lines2, labels2 = ax_spx.get_legend_handles_labels()
        ax.legend(lines1 + lines2, labels1 + labels2,
                  loc='upper left', fontsize=8,
                  facecolor='#1a1a2a', edgecolor='#3a3a4a', labelcolor=col_tick)

        plt.tight_layout(pad=1.4)

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
