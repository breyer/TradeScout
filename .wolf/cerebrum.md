# Cerebrum

> OpenWolf's learning memory. Updated automatically as the AI learns from interactions.
> Do not edit manually unless correcting an error.
> Last updated: 2026-04-03

## User Preferences

<!-- How the user likes things done. Code style, tools, patterns, communication. -->

## Key Learnings

- **TAT is a UWP app.** Detection via PowerShell `Get-AppxPackage -Name '*TradeAutomationToolbox*'`. The `PackageFamilyName` gives the install identity. The database `data.db3` lives at `%LOCALAPPDATA%\Packages\<PackageFamilyName>\LocalState\data.db3` — NOT in a `data/` subfolder. No registry key or filesystem EXE to scan for.
- **NSIS UWP detection:** Use `nsExec::ExecToStack` with `powershell -NoProfile -NonInteractive -Command "(Get-AppxPackage -Name '*TradeAutomationToolbox*').PackageFamilyName"`. Trim trailing `\r\n` with `StrCpy $1 $1 -2` (after checking `StrLen > 2`).

- **Project:** TradeScout
- Trade table uses integer Year/Month/Day columns (not timestamps) for date filtering. Use `(Year*10000 + Month*100 + Day) >= ?` for range queries.
- **CRITICAL — TradeType mismatch:** Real DB stores `PutSpread`, `CallSpread`, `IronFly` — NOT `Put`/`Call`. The refactored `Trade_Scout.py` calls `get_trades_by_type('Put', ...)` and `get_trades_by_type('Call', ...)` which return empty DataFrames against the real database. Must be fixed before Trade_Scout.py works end-to-end.
- DailyLog timestamps are Windows FILETIME integers (100-ns ticks since 1601-01-01). Constants FILETIME_EPOCH_OFFSET and FILETIME_TICKS_PER_SECOND are defined in utils.py and shared across modules.
- TAT stores dates with a fixed year; convert_to_human_readable() forces the year to the current system year as a workaround.
- get_trades_by_type() uses column aliases (PL, OpenDate, CloseDate, Contracts) matching Trade_Scout.py expectations; get_trades() uses the raw DB names (ProfitLoss, DateOpened, DateClosed, Qty).
- ClosingProcessed=0 → expired worthless; =1 → stopped out. Bad slippage = abs(PriceClose) - PriceStopTarget >= 0.50.
- **Project:** TradeScout
- **Description:** **TradeScout** is a tool that integrates with trades from [Trade Automation Toolbox (TAT)](https://tradeautomationtoolbox.com/). It provides detailed analytics and metrics to track trade performance b

## Do-Not-Repeat

<!-- Mistakes made and corrected. Each entry prevents the same mistake recurring. -->
<!-- Format: [YYYY-MM-DD] Description of what went wrong and what to do instead. -->
[2026-04-03] pyautogui and pygetwindow are GUI-only packages not available in the test environment. Import them lazily inside take_screenshot_of_app(), never at module level — otherwise all tests fail to import utils.py.
[2026-04-03] python-dotenv may not be installed. Wrap `from dotenv import load_dotenv` in a try/except ImportError with a no-op fallback so Trade_Scout.py can be imported in tests.
[2026-04-03] The get_most_recent_monday() Sunday bug: formula was (date.weekday()+1)%7 which returns 0 on Sundays (no-op). Correct formula is date.weekday() (0=Mon→0 days back, 6=Sun→6 days back).

## Decision Log

<!-- Significant technical decisions with rationale. Why X was chosen over Y. -->
