# TradeScout

**TradeScout** reads your daily options trades from [Trade Automation Toolbox (TAT)](https://tradeautomationtoolbox.com/) and posts a formatted performance summary to Discord — automatically, every trading day.

## Features

### Daily Report
Every trading day TradeScout posts a Discord message with:

| Metric | Description |
|--------|-------------|
| **SPX Last** | Last SPX index value for the session |
| **Prem Sold** | Total premium sold across all trades |
| **Prem Cap** | Total premium captured (realized P&L) |
| **PCR** | Premium Capture Rate — captured ÷ sold |
| **Win %** | Percentage of winning trades |
| **Exp : Stp** | Expired worthless : Stopped out count |
| **Bad Slip** | Trades with slippage ≥ $0.50 (with worst shown) |
| **-ve Exprd** | Trades that expired with a loss |
| **WTD PL** | Week-to-date P&L (since last Monday) |
| **MTD PL** | Month-to-date P&L |

### Rolling Benchmarks
Inline rolling stats for the past **5 / 20 / 60 trading days** — weekends and market holidays excluded automatically.

### Weekly Equity Curve
A 60-day cumulative P&L chart is attached every **Friday** (or the next open market day if Friday was a holiday).

### Smart Skipping
TradeScout detects market-closed days (no trades and no TAT DailyLog data) and exits silently — no empty posts.

---

## Installation

### Recommended — Windows Installer

1. Download `TradeScout-Setup.exe` from the [Releases](../../releases) page and run it.
2. The installer:
   - Auto-detects your TAT installation via the Windows app registry
   - Sets the correct path to `data.db3` in `config.yaml`
   - Asks for your Discord webhook URL (validated on entry)
   - Lets you enable/disable the equity curve and rolling benchmarks
   - Optionally creates a Windows Scheduled Task that runs at **4:35 PM ET** on weekdays
3. Done — no manual config editing required.

### Manual — Portable Archive

1. Download `TradeScout.7z` from the [Releases](../../releases) page and extract it.
2. Copy `config.demo.yaml` to `config.yaml` (in the same folder as `TradeScout.exe`) and fill in your values:

```yaml
# Full path to your TAT database file
# Find data.db3 at: %LOCALAPPDATA%\Packages\TradeAutomationToolbox_...\LocalState\
db_path: 'C:\Users\you\AppData\Local\Packages\TradeAutomationToolbox_xxxx\LocalState\data.db3'

webhooks:
  - url: "https://discord.com/api/webhooks/WEBHOOK_ID"
    thread_id: "THREAD_ID"   # optional — targets a specific thread
  - url: "https://discord.com/api/webhooks/ANOTHER_WEBHOOK"
    thread_id: null           # omit to post to the main channel

features:
  equity_curve:
    enabled: true
    days: 60
  rolling_benchmarks:
    enabled: true
    windows: [5, 20, 60]   # trading days
  skip_closed_market:
    enabled: true
```

3. Run `TradeScout.exe`.

---

## Usage

```
TradeScout.exe                    # today's trades, with TAT screenshot
TradeScout.exe --date 20260402    # specific date (YYYYMMDD)
TradeScout.exe --noimage          # skip TAT screenshot
TradeScout.exe --win restore      # restore TAT window instead of maximizing
TradeScout.exe --debug            # print to console, don't post to Discord
```

After posting, TradeScout waits 30 seconds and asks if you want to delete the message.

---

## Example Output

```
2026 Apr 04 (Friday)
----------|------------
SPX Last  |     5,396.63
Prem Sold |    $3,695.00
Prem Cap  |      $580.82
PCR       |       15.72%
Win %     |       77.78%
Exp : Stp |         12:6
Bad Slip  |            0
-ve Exprd |            0
WTD PL    |    $2,285.56
MTD PL    |      ($8.23)
----------|-------|-------|-------
Rolling   |   5d  |  20d  |  60d
----------|-------|-------|-------
PCR       | 18.3% | 22.1% | 19.8%
Win %     | 80.0% | 76.5% | 74.2%
Avg Day   |  $412 |  $388 |  $341
```

On Fridays an equity curve chart is also attached:

![Example Output](TradeScoutOutputExample.jpg)

---

## How to Compile Under Windows

### Prerequisites

- **Python 3.11** — [python.org](https://www.python.org/downloads/release/python-3119/) — check **"Add Python to PATH"**
- **NSIS 3.09** — [nsis.sourceforge.io](https://nsis.sourceforge.io/) — needed to build the installer
- **7-Zip** — [7-zip.org](https://www.7-zip.org/) — needed to create the portable archive

### Steps

```powershell
# 1. Clone the repository
git clone https://github.com/breyer/TradeScout
cd TradeScout

# 2. Install Python dependencies
pip install -r requirements.txt
pip install pyinstaller

# 3. Build the executable
pyinstaller --onedir --name TradeScout `
  --hidden-import pyscreeze `
  --hidden-import PIL `
  --hidden-import PIL.Image `
  --hidden-import matplotlib `
  --hidden-import matplotlib.backends.backend_agg `
  Trade_Scout.py

# 4. Build the installer
cd installer
makensis tradescout.nsi
cd ..

# 5. Build the portable archive
New-Item -ItemType Directory -Path release | Out-Null
Copy-Item dist\TradeScout release\ -Recurse
Copy-Item config.demo.yaml release\
& "C:\Program Files\7-Zip\7z.exe" a TradeScout.7z .\release\*
```

> **Why `--onedir`?** The `--onefile` mode extracts ~40 MB to a temp folder on every launch;
> Windows Defender scans each file, causing a 30–60 s startup delay.
> `--onedir` avoids this entirely — startup is near-instant.

### Directory Layout After Build

```
TradeScout\
  TradeScout.exe     <- run this
  config.yaml        <- copy from config.demo.yaml and fill in
  *.dll / *.pyd      <- bundled libraries (must stay in same folder)
```
