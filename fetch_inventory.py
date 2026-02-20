#!/usr/bin/env python3
"""
fetch_inventory.py — Daily FTP inventory fetch + PostgreSQL storage.

Usage:
    python fetch_inventory.py                          # fetch live FTP
    python fetch_inventory.py --from-local <dir>       # seed from local backup files
    python fetch_inventory.py --date 2025-01-15        # override snapshot date (for backfill)

Requires:
    DATABASE_URL environment variable pointing to a PostgreSQL instance.
    e.g. export DATABASE_URL=postgresql://localhost/dealer_analytics
"""

import argparse
import ftplib
import io
import os
from datetime import date, datetime
from pathlib import Path

import pandas as pd
import psycopg2
import psycopg2.extras

# Load .env file if present (for local development)
try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent / ".env")
except ImportError:
    pass

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

FTP_HOST = "ftp.santacruzclassics.com"
FTP_USER = os.environ.get("FTP_USER", "anonymous")
FTP_PASS = os.environ.get("FTP_PASS", "anonymous@")

# Dealer name → one or more filenames on the FTP server.
# When a list is given, rows from all files are combined and deduped by VIN.
DEALERS = {
    "Artioli Dodge":     ["artioli_dodge.csv.csv"],
    "Marcotte Ford":     ["marcotte_ford_new.csv"],
    "Columbia Ford/KIA": ["columbiafordkia.csv"],
    "Central Chevrolet": ["central_chevrolet.csv"],
    "Gates GMC":         ["gatesgmc.csv"],
    "Suburban Subaru":   ["_Suburban Subaru.csv"],
    "Troiano CDJ":       ["TroianoCDJ.csv"],
}

# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def get_db_url() -> str:
    url = os.environ.get("DATABASE_URL", "")
    # Railway sometimes issues postgres:// URLs; psycopg2 requires postgresql://
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql://", 1)
    if not url:
        raise ValueError(
            "DATABASE_URL environment variable not set.\n"
            "Example: export DATABASE_URL=postgresql://localhost/dealer_analytics"
        )
    return url


def get_conn() -> psycopg2.extensions.connection:
    conn = psycopg2.connect(get_db_url())
    _init_db(conn)
    return conn


def _init_db(conn: psycopg2.extensions.connection):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS snapshots (
                date      TEXT,
                dealer    TEXT,
                vin       TEXT,
                year      INTEGER,
                make      TEXT,
                model     TEXT,
                trim      TEXT,
                condition TEXT,
                price     REAL,
                PRIMARY KEY (date, vin, dealer)
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS fetch_log (
                fetched_at  TEXT,
                dealer      TEXT,
                file        TEXT,
                rows_parsed INTEGER,
                status      TEXT
            )
        """)
    conn.commit()


def snapshot_exists(conn: psycopg2.extensions.connection, snapshot_date: str, dealer: str) -> bool:
    """Return True if we already have rows for this date+dealer (idempotency guard)."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM snapshots WHERE date = %s AND dealer = %s LIMIT 1",
            (snapshot_date, dealer),
        )
        return cur.fetchone() is not None


def insert_snapshot(conn: psycopg2.extensions.connection, snapshot_date: str, dealer: str, df: pd.DataFrame):
    rows = [
        (
            snapshot_date,
            dealer,
            r.get("vin", ""),
            _int(r.get("year")),
            r.get("make", ""),
            r.get("model", ""),
            r.get("trim", ""),
            r.get("condition", ""),
            _float(r.get("price")),
        )
        for _, r in df.iterrows()
    ]
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """INSERT INTO snapshots (date, dealer, vin, year, make, model, trim, condition, price)
               VALUES %s ON CONFLICT DO NOTHING""",
            rows,
        )
    conn.commit()


def log_fetch(conn: psycopg2.extensions.connection, dealer: str, filename: str, rows: int, status: str):
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO fetch_log (fetched_at, dealer, file, rows_parsed, status) VALUES (%s,%s,%s,%s,%s)",
            (datetime.utcnow().isoformat(), dealer, filename, rows, status),
        )
    conn.commit()


# ---------------------------------------------------------------------------
# CSV format detection + normalization
# ---------------------------------------------------------------------------

def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Detect which of the 3 known CSV formats we have and normalize to a common
    schema: vin, year, make, model, trim, condition, price.
    """
    cols = {c.strip().lower() for c in df.columns}

    # Rename all columns to lowercase+stripped for easier matching
    df.columns = [c.strip() for c in df.columns]
    df = df.rename(columns={c: c.lower() for c in df.columns})

    # --- Format 1: Google Shopping feed ---
    # Key columns: brand, vehicle_msrp, (vin is already 'vin')
    if "brand" in cols and "vehicle_msrp" in cols:
        return _map_columns(df, {
            "vin":       "vin",
            "year":      "year",
            "make":      "brand",
            "model":     "model",
            "trim":      "trim",
            "condition": "condition",
            "price":     "vehicle_msrp",
        })

    # --- Format 2: DMS export (Central Chevrolet) ---
    # Key columns: Vin (capital V), InternetPrice
    if "internetprice" in cols or "vin" in cols and "make" in cols and "internetprice" in cols:
        col_map = {
            "vin":       _find_col(df, ["vin"]),
            "year":      _find_col(df, ["year"]),
            "make":      _find_col(df, ["make"]),
            "model":     _find_col(df, ["model"]),
            "trim":      _find_col(df, ["trim"]),
            "condition": _find_col(df, ["condition", "new_used", "type"]),
            "price":     _find_col(df, ["internetprice", "internet_price"]),
        }
        return _map_columns(df, col_map)

    # --- Format 3: Type/Stock (Suburban Subaru, Troiano CDJ) ---
    # Key columns: SellingPrice
    if "sellingprice" in cols or "selling_price" in cols or "selling price" in cols:
        col_map = {
            "vin":       _find_col(df, ["vin"]),
            "year":      _find_col(df, ["year"]),
            "make":      _find_col(df, ["make"]),
            "model":     _find_col(df, ["model"]),
            "trim":      _find_col(df, ["trim"]),
            "condition": _find_col(df, ["condition", "type", "new_used"]),
            "price":     _find_col(df, ["sellingprice", "selling_price", "selling price"]),
        }
        return _map_columns(df, col_map)

    # --- Fallback: best-effort mapping ---
    print("  [warn] unrecognized CSV format, attempting best-effort mapping")
    col_map = {
        "vin":       _find_col(df, ["vin"]),
        "year":      _find_col(df, ["year"]),
        "make":      _find_col(df, ["make", "brand"]),
        "model":     _find_col(df, ["model"]),
        "trim":      _find_col(df, ["trim"]),
        "condition": _find_col(df, ["condition", "type", "new_used"]),
        "price":     _find_col(df, ["price", "sellingprice", "vehicle_msrp", "internetprice", "msrp"]),
    }
    return _map_columns(df, col_map)


def _find_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    """Return the first candidate column name that exists in the DataFrame (lowercased)."""
    existing = {c.lower(): c for c in df.columns}
    for c in candidates:
        if c.lower() in existing:
            return existing[c.lower()]
    return None


def _map_columns(df: pd.DataFrame, mapping: dict) -> pd.DataFrame:
    """
    Given a dict of {target_name: source_col}, build a normalized DataFrame.
    Missing source cols become NaN.
    """
    result = {}
    for target, source in mapping.items():
        if source and source in df.columns:
            result[target] = df[source]
        else:
            result[target] = None
    out = pd.DataFrame(result)
    # Drop rows with no VIN
    out = out[out["vin"].notna() & (out["vin"].astype(str).str.strip() != "")]
    out["vin"] = out["vin"].astype(str).str.strip().str.upper()
    return out


def _int(val):
    try:
        return int(val)
    except (TypeError, ValueError):
        return None


def _float(val):
    try:
        # Strip currency symbols / commas
        return float(str(val).replace("$", "").replace(",", "").strip())
    except (TypeError, ValueError):
        return None


def parse_csv_bytes(data: bytes, dealer: str) -> pd.DataFrame:
    """Parse raw CSV bytes, normalize columns, deduplicate by VIN."""
    try:
        df = pd.read_csv(io.BytesIO(data), dtype=str, low_memory=False)
    except Exception:
        # Try latin-1 if utf-8 fails
        df = pd.read_csv(io.BytesIO(data), dtype=str, low_memory=False, encoding="latin-1")
    df = _normalize_columns(df)
    # Dedup by VIN within a single file
    df = df.drop_duplicates(subset=["vin"])
    return df


# ---------------------------------------------------------------------------
# FTP fetch
# ---------------------------------------------------------------------------

def fetch_from_ftp(snapshot_date: str):
    conn = get_conn()
    print(f"Connecting to FTP: {FTP_HOST}")
    try:
        ftp = ftplib.FTP(FTP_HOST)
        ftp.login(FTP_USER, FTP_PASS)
        ftp.set_pasv(True)
    except Exception as e:
        print(f"FTP connection failed: {e}")
        conn.close()
        return

    for dealer, filenames in DEALERS.items():
        if snapshot_exists(conn, snapshot_date, dealer):
            print(f"  [{dealer}] already fetched for {snapshot_date}, skipping")
            continue

        combined_frames = []
        for filename in filenames:
            buf = io.BytesIO()
            try:
                ftp.retrbinary(f"RETR {filename}", buf.write)
                data = buf.getvalue()
                df = parse_csv_bytes(data, dealer)
                combined_frames.append(df)
                print(f"  [{dealer}] {filename}: {len(df)} rows")
                log_fetch(conn, dealer, filename, len(df), "ok")
            except ftplib.error_perm as e:
                print(f"  [{dealer}] {filename}: FTP error — {e}")
                log_fetch(conn, dealer, filename, 0, f"error: {e}")
            except Exception as e:
                print(f"  [{dealer}] {filename}: parse error — {e}")
                log_fetch(conn, dealer, filename, 0, f"error: {e}")

        if combined_frames:
            merged = pd.concat(combined_frames, ignore_index=True)
            merged = merged.drop_duplicates(subset=["vin"])
            insert_snapshot(conn, snapshot_date, dealer, merged)
            print(f"  [{dealer}] stored {len(merged)} unique VINs for {snapshot_date}")

    ftp.quit()
    conn.close()
    print("Done.")


# ---------------------------------------------------------------------------
# Local seed (--from-local)
# ---------------------------------------------------------------------------

def fetch_from_local(local_dir: str, snapshot_date: str):
    """
    Seed the database from a local backup directory.
    Tries to match each dealer's filenames against files in the directory.
    Also handles subdirectories named by date (YYYY-MM-DD).
    """
    base = Path(local_dir)
    conn = get_conn()

    def index_dir(d: Path) -> dict[str, list[Path]]:
        """Index all CSV files in directory d by lowercased filename."""
        result: dict[str, list[Path]] = {}
        for f in d.iterdir():
            if f.is_file() and f.suffix.lower() == ".csv":
                result.setdefault(f.name.lower(), []).append(f)
        return result

    # Check if base contains date subdirectories
    date_dirs = sorted([
        d for d in base.iterdir()
        if d.is_dir() and _is_date_dir(d.name)
    ])

    if date_dirs:
        # Process each date directory as a separate snapshot
        for date_dir in date_dirs:
            dir_date = date_dir.name  # YYYY-MM-DD
            print(f"\nProcessing date directory: {dir_date}")
            _process_local_candidates(conn, index_dir(date_dir), dir_date)
    else:
        # Flat directory — use the provided snapshot_date
        flat_candidates = index_dir(base)
        # Also index any non-date subdirectories
        for sub in base.iterdir():
            if sub.is_dir() and not _is_date_dir(sub.name):
                for f in sub.iterdir():
                    if f.is_file() and f.suffix.lower() == ".csv":
                        flat_candidates.setdefault(f.name.lower(), []).append(f)
        _process_local_candidates(conn, flat_candidates, snapshot_date)

    conn.close()
    print("\nLocal seed complete.")


def _is_date_dir(name: str) -> bool:
    try:
        datetime.strptime(name, "%Y-%m-%d")
        return True
    except ValueError:
        return False


def _process_local_candidates(conn: psycopg2.extensions.connection, candidates: dict, snapshot_date: str):
    for dealer, filenames in DEALERS.items():
        if snapshot_exists(conn, snapshot_date, dealer):
            print(f"  [{dealer}] already loaded for {snapshot_date}, skipping")
            continue

        combined_frames = []
        for filename in filenames:
            matched = candidates.get(filename.lower(), [])
            if not matched:
                # Fuzzy: try stripping leading underscores / case differences
                for key, paths in candidates.items():
                    if key.lstrip("_") == filename.lower().lstrip("_"):
                        matched = paths
                        break
            if not matched:
                print(f"  [{dealer}] {filename}: not found locally, skipping")
                log_fetch(conn, dealer, filename, 0, "not found locally")
                continue

            for path in matched:
                try:
                    data = path.read_bytes()
                    df = parse_csv_bytes(data, dealer)
                    combined_frames.append(df)
                    print(f"  [{dealer}] {path.name}: {len(df)} rows")
                    log_fetch(conn, dealer, str(path), len(df), "ok (local)")
                except Exception as e:
                    print(f"  [{dealer}] {path.name}: error — {e}")
                    log_fetch(conn, dealer, str(path), 0, f"error: {e}")

        if combined_frames:
            merged = pd.concat(combined_frames, ignore_index=True)
            merged = merged.drop_duplicates(subset=["vin"])
            insert_snapshot(conn, snapshot_date, dealer, merged)
            print(f"  [{dealer}] stored {len(merged)} unique VINs for {snapshot_date}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch dealer inventory snapshots")
    parser.add_argument(
        "--from-local",
        metavar="DIR",
        help="Seed database from local backup directory instead of FTP",
    )
    parser.add_argument(
        "--date",
        metavar="YYYY-MM-DD",
        default=date.today().isoformat(),
        help="Snapshot date override (default: today)",
    )
    args = parser.parse_args()

    print(f"Snapshot date: {args.date}")
    print(f"Database:      {os.environ.get('DATABASE_URL', '(DATABASE_URL not set)')}")

    if args.from_local:
        print(f"Source:        local directory — {args.from_local}")
        fetch_from_local(args.from_local, args.date)
    else:
        print(f"Source:        FTP — {FTP_HOST}")
        fetch_from_ftp(args.date)
