# Dealer Inventory Analytics

Streamlit dashboard showing daily sales rates for 7 auto dealerships, inferred from
VINs that disappear between daily inventory feed snapshots.

## Project structure

```
dealer_analytics/
  fetch_inventory.py       # Daily FTP fetch + SQLite storage
  dashboard.py             # Streamlit web app
  inventory.db             # SQLite DB (auto-created on first run)
  requirements.txt
  launchd/
    com.catalystmarketing.fetch_inventory.plist  # macOS daily scheduler
```

## Setup

```bash
cd ~/Documents/dealer_analytics
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## First run — seed from local backup

If you have backup files in `~/Documents/ftp_backup_santacruzclassics/`:

```bash
python fetch_inventory.py --from-local ~/Documents/ftp_backup_santacruzclassics/
```

The script handles:
- **Flat directories**: all CSVs treated as a single snapshot (uses `--date`, default today)
- **Date subdirectories** (`YYYY-MM-DD/`): each folder becomes a separate snapshot date

## Daily FTP fetch

```bash
# Set credentials (or add to ~/.zshrc / ~/.bash_profile)
export FTP_USER=your_username
export FTP_PASS=your_password

python fetch_inventory.py
```

Fetches are **idempotent** — running twice on the same day is safe (duplicate dates are skipped).

## Run the dashboard

```bash
streamlit run dashboard.py
```

Open http://localhost:8501 in your browser.

## Automate with macOS launchd

1. Edit the plist to set your Python path and FTP credentials:
   ```
   launchd/com.catalystmarketing.fetch_inventory.plist
   ```

2. Install:
   ```bash
   cp launchd/com.catalystmarketing.fetch_inventory.plist ~/Library/LaunchAgents/
   launchctl load ~/Library/LaunchAgents/com.catalystmarketing.fetch_inventory.plist
   ```

3. Verify it's loaded:
   ```bash
   launchctl list | grep catalystmarketing
   ```

4. Uninstall:
   ```bash
   launchctl unload ~/Library/LaunchAgents/com.catalystmarketing.fetch_inventory.plist
   ```

Logs are written to `fetch_inventory.log` and `fetch_inventory_error.log`.

## How "sales" are calculated

A **sale** = a VIN present in the feed on day D but absent on day D+1 for the same dealer.

> This is an inference from inventory data, not confirmed point-of-sale data.
> Disappearances may also include transfers, delistings, or feed errors.

## Dealers & file mapping

| Dealer | File(s) | CSV Format |
|---|---|---|
| Artioli Dodge | `artioli_dodge.csv.csv` | Google Shopping |
| Marcotte Ford | `marcotte_ford_new.csv`, `marcotte_ford.csv` | Google Shopping |
| Columbia Ford/KIA | `columbiafordkia.csv` | Google Shopping |
| Central Chevrolet | `central_chevrolet.csv` | DMS export |
| Gates GMC | `gatesgmc.csv` | Google Shopping |
| Suburban Subaru | `_Suburban Subaru.csv` | Type/Stock |
| Troiano CDJ | `TroianoCDJ.csv` | Type/Stock |

## Railway deployment (when ready)

1. Push to GitHub (add `.env` to `.gitignore`)
2. Deploy to Railway as a Python service
3. Add Railway cron job: `python fetch_inventory.py`
4. Streamlit dashboard is the main web process
5. For larger datasets, swap SQLite for Railway's Postgres addon

## Troubleshooting

**No data in dashboard:**
```bash
sqlite3 inventory.db "SELECT date, dealer, COUNT(*) FROM snapshots GROUP BY date, dealer ORDER BY date DESC LIMIT 20;"
```

**Check fetch log:**
```bash
sqlite3 inventory.db "SELECT * FROM fetch_log ORDER BY fetched_at DESC LIMIT 20;"
```

**CSV format not recognized:**
The script will print a warning and attempt best-effort column mapping. Check the
output for `[warn] unrecognized CSV format` and inspect the CSV headers manually.
