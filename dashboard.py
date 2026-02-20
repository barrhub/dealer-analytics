"""
dashboard.py — Streamlit dealer inventory analytics dashboard.

Run:  streamlit run dashboard.py

Requires:
    DATABASE_URL environment variable pointing to a PostgreSQL instance.
    e.g. export DATABASE_URL=postgresql://localhost/dealer_analytics
"""

import os
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import psycopg2
import requests
import streamlit as st

# Load .env file if present (for local development)
try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent / ".env")
except ImportError:
    pass

from fetch_inventory import parse_csv_bytes, DEALERS, insert_snapshot, log_fetch, snapshot_exists

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="Dealer Inventory Analytics",
    page_icon="🚗",
    layout="wide",
)

# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def get_db_url() -> str:
    url = os.environ.get("DATABASE_URL", "")
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql://", 1)
    if not url:
        st.error("DATABASE_URL environment variable not set.")
        st.stop()
    return url


def query(sql: str, params=()) -> pd.DataFrame:
    conn = psycopg2.connect(get_db_url())
    try:
        with conn.cursor() as cur:
            cur.execute(sql, list(params) if params else None)
            columns = [desc[0] for desc in cur.description]
            return pd.DataFrame(cur.fetchall(), columns=columns)
    finally:
        conn.close()


def ensure_tables():
    """Create tables if they don't exist."""
    conn = psycopg2.connect(get_db_url())
    try:
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
    finally:
        conn.close()


if "tables_initialized" not in st.session_state:
    ensure_tables()
    st.session_state["tables_initialized"] = True


@st.cache_data(ttl=300)
def get_all_dealers() -> list[str]:
    df = query("SELECT DISTINCT dealer FROM snapshots ORDER BY dealer")
    return df["dealer"].tolist()


@st.cache_data(ttl=300)
def get_date_range() -> tuple[date, date]:
    df = query("SELECT MIN(date) as min_d, MAX(date) as max_d FROM snapshots")
    if df.empty or df["min_d"][0] is None:
        today = date.today()
        return today - timedelta(days=30), today
    return (
        date.fromisoformat(df["min_d"][0]),
        date.fromisoformat(df["max_d"][0]),
    )


@st.cache_data(ttl=300)
def get_makes_models(dealers: tuple) -> pd.DataFrame:
    placeholders = ",".join(["%s"] * len(dealers))
    return query(
        f"SELECT DISTINCT make, model FROM snapshots WHERE dealer IN ({placeholders}) ORDER BY make, model",
        dealers,
    )


# ---------------------------------------------------------------------------
# Sales detection logic
# ---------------------------------------------------------------------------

@st.cache_data(ttl=300)
def compute_sales(
    dealers: tuple,
    start: str,
    end: str,
    condition: str,
) -> pd.DataFrame:
    """
    A sale = VIN present on date D but absent on date D+1 for the same dealer.
    Returns a DataFrame with columns: date_sold, dealer, make, model, year, condition, price.
    """
    cond_filter = ""
    cond_params: list = []
    if condition != "Both":
        cond_filter = "AND LOWER(s1.condition) LIKE %s"
        cond_params = [f"%{condition.lower()}%"]

    dealer_placeholders = ",".join(["%s"] * len(dealers))

    sql = f"""
        SELECT
            s1.date   AS date_sold,
            s1.dealer,
            s1.make,
            s1.model,
            s1.year,
            s1.condition,
            s1.price
        FROM snapshots s1
        WHERE s1.dealer IN ({dealer_placeholders})
          AND s1.date BETWEEN %s AND %s
          {cond_filter}
          -- Only flag as sold if a subsequent snapshot exists for this dealer
          AND EXISTS (
              SELECT 1 FROM snapshots
              WHERE dealer = s1.dealer AND date > s1.date
          )
          AND NOT EXISTS (
              SELECT 1 FROM snapshots s2
              WHERE s2.vin    = s1.vin
                AND s2.dealer = s1.dealer
                AND s2.date   = (
                    SELECT MIN(date) FROM snapshots
                    WHERE dealer = s1.dealer AND date > s1.date
                )
          )
    """
    params = list(dealers) + [start, end] + cond_params
    return query(sql, params)


@st.cache_data(ttl=300)
def compute_inventory_over_time(
    dealers: tuple,
    start: str,
    end: str,
    condition: str,
) -> pd.DataFrame:
    cond_filter = ""
    cond_params: list = []
    if condition != "Both":
        cond_filter = "AND LOWER(condition) LIKE %s"
        cond_params = [f"%{condition.lower()}%"]

    dealer_placeholders = ",".join(["%s"] * len(dealers))
    sql = f"""
        SELECT date, dealer, COUNT(*) as inventory
        FROM snapshots
        WHERE dealer IN ({dealer_placeholders})
          AND date BETWEEN %s AND %s
          {cond_filter}
        GROUP BY date, dealer
        ORDER BY date, dealer
    """
    return query(sql, list(dealers) + [start, end] + cond_params)


@st.cache_data(ttl=300)
def compute_inventory_by_model(
    dealers: tuple,
    start: str,
    end: str,
    condition: str,
    makes: tuple,
) -> pd.DataFrame:
    """Inventory count per date grouped by make+model (across selected dealers)."""
    cond_filter = ""
    cond_params: list = []
    if condition != "Both":
        cond_filter = "AND LOWER(condition) LIKE %s"
        cond_params = [f"%{condition.lower()}%"]

    makes_filter = ""
    makes_params: list = []
    if makes:
        makes_placeholders = ",".join(["%s"] * len(makes))
        makes_filter = f"AND make IN ({makes_placeholders})"
        makes_params = list(makes)

    dealer_placeholders = ",".join(["%s"] * len(dealers))
    sql = f"""
        SELECT date, make, model, COUNT(*) as inventory
        FROM snapshots
        WHERE dealer IN ({dealer_placeholders})
          AND date BETWEEN %s AND %s
          {cond_filter}
          {makes_filter}
        GROUP BY date, make, model
        ORDER BY date, make, model
    """
    return query(sql, list(dealers) + [start, end] + cond_params + makes_params)


@st.cache_data(ttl=300)
def compute_detail_table(
    dealers: tuple,
    start: str,
    end: str,
    condition: str,
    period_days: int = 30,
) -> pd.DataFrame:
    """
    Per dealer/make/model/year:
    - units_sold in window
    - avg_days_on_lot  (first seen → sold, for sold VINs)
    - current_stock    (most recent snapshot)
    - days_to_sellthrough  (current_stock / daily_rate)
    """
    sales = compute_sales(dealers, start, end, condition)
    if sales.empty:
        return pd.DataFrame(columns=[
            "dealer", "make", "model", "year",
            "units_sold", "avg_days_on_lot", "current_stock", "days_to_sellthrough",
        ])

    sold_agg = (
        sales.groupby(["dealer", "make", "model", "year"])
        .size()
        .reset_index(name="units_sold")
    )

    dealer_placeholders = ",".join(["%s"] * len(dealers))
    cond_filter = ""
    cond_params: list = []
    if condition != "Both":
        cond_filter = "AND LOWER(condition) LIKE %s"
        cond_params = [f"%{condition.lower()}%"]

    # Avg days on lot for sold VINs.
    # PostgreSQL date subtraction returns an integer number of days directly.
    lot_sql = f"""
        SELECT
            s1.dealer, s1.make, s1.model, s1.year,
            AVG(
                s1.date::date - (
                    SELECT MIN(date)::date FROM snapshots
                    WHERE vin = s1.vin AND dealer = s1.dealer
                )
            ) AS avg_days_on_lot
        FROM snapshots s1
        WHERE s1.dealer IN ({dealer_placeholders})
          AND s1.date BETWEEN %s AND %s
          {cond_filter}
          AND NOT EXISTS (
              SELECT 1 FROM snapshots s2
              WHERE s2.vin    = s1.vin
                AND s2.dealer = s1.dealer
                AND s2.date   = (
                    SELECT MIN(date) FROM snapshots
                    WHERE dealer = s1.dealer AND date > s1.date
                )
          )
        GROUP BY s1.dealer, s1.make, s1.model, s1.year
    """
    lot = query(lot_sql, list(dealers) + [start, end] + cond_params)

    # Current stock from the latest available date
    stock_sql = f"""
        SELECT dealer, make, model, year, COUNT(*) as current_stock
        FROM snapshots
        WHERE dealer IN ({dealer_placeholders})
          AND date = (SELECT MAX(date) FROM snapshots WHERE dealer IN ({dealer_placeholders}))
          {cond_filter}
        GROUP BY dealer, make, model, year
    """
    stock = query(stock_sql, list(dealers) + list(dealers) + cond_params)

    merged = pd.merge(sold_agg, lot, on=["dealer", "make", "model", "year"], how="left")
    merged = pd.merge(merged, stock, on=["dealer", "make", "model", "year"], how="outer")
    merged["units_sold"] = merged["units_sold"].fillna(0).astype(int)
    merged["current_stock"] = merged["current_stock"].fillna(0).astype(int)
    merged["avg_days_on_lot"] = pd.to_numeric(merged["avg_days_on_lot"], errors="coerce").round(1)

    # Days to sell-through: current_stock ÷ (units_sold / period_days)
    daily_rate = merged["units_sold"] / max(period_days, 1)
    merged["days_to_sellthrough"] = (
        merged["current_stock"] / daily_rate.replace(0, float("nan"))
    ).round(0)

    return merged.sort_values("units_sold", ascending=False).reset_index(drop=True)


# ---------------------------------------------------------------------------
# FRED API helpers
# ---------------------------------------------------------------------------

FRED_BASE = "https://api.stlouisfed.org/fred/series/observations"
FRED_SERIES = {
    "Total Vehicle Sales (SAAR, millions)": "TOTALSA",
    "Light Auto Sales (SAAR, millions)":    "LAUTOSA",
}


@st.cache_data(ttl=86400)  # cache 24h
def fetch_fred(series_id: str, start: str, end: str) -> pd.DataFrame:
    try:
        resp = requests.get(
            FRED_BASE,
            params={
                "series_id":         series_id,
                "observation_start": start,
                "observation_end":   end,
                "file_type":         "json",
                "api_key":           os.environ.get("FRED_API_KEY", ""),
            },
            timeout=10,
        )
        data = resp.json()
        obs = data.get("observations", [])
        df = pd.DataFrame(obs)[["date", "value"]]
        df = df[df["value"] != "."]
        df["value"] = df["value"].astype(float)
        df["date"] = pd.to_datetime(df["date"])
        return df
    except Exception:
        return pd.DataFrame(columns=["date", "value"])


# ---------------------------------------------------------------------------
# UI helpers
# ---------------------------------------------------------------------------

def kpi_card(col, label: str, value: str, delta: str = ""):
    with col:
        st.metric(label=label, value=value, delta=delta or None)


def format_num(n: float, decimals: int = 1) -> str:
    if n >= 1_000_000:
        return f"{n/1_000_000:.{decimals}f}M"
    if n >= 1_000:
        return f"{n/1_000:.{decimals}f}K"
    return f"{n:.{decimals}f}"


# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------

st.sidebar.title("Filters")

all_dealers = get_all_dealers()
if not all_dealers:
    st.warning("No data in database. Run `python fetch_inventory.py --from-local <dir>` to seed.")
    st.stop()

selected_dealers = st.sidebar.multiselect(
    "Dealers",
    options=all_dealers,
    default=all_dealers,
)
if not selected_dealers:
    st.sidebar.warning("Select at least one dealer.")
    st.stop()

db_min, db_max = get_date_range()

quick_range = st.sidebar.selectbox(
    "Quick range",
    ["Last 7 days", "Last 30 days", "Last 90 days", "Custom"],
    index=1,
)
today = db_max
if quick_range == "Last 7 days":
    start_date = today - timedelta(days=7)
    end_date = today
elif quick_range == "Last 30 days":
    start_date = today - timedelta(days=30)
    end_date = today
elif quick_range == "Last 90 days":
    start_date = today - timedelta(days=90)
    end_date = today
else:
    start_date = st.sidebar.date_input("Start date", value=db_min, min_value=db_min, max_value=db_max)
    end_date = st.sidebar.date_input("End date", value=db_max, min_value=db_min, max_value=db_max)

condition_filter = st.sidebar.radio("Condition", ["Both", "New", "Used"], index=0)

# Make/model filter
makes_models_df = get_makes_models(tuple(selected_dealers))
all_makes = sorted(makes_models_df["make"].dropna().unique().tolist())
selected_makes = st.sidebar.multiselect("Make (optional)", options=all_makes, default=[])

# Comparison mode
st.sidebar.markdown("---")
compare_mode = st.sidebar.toggle("Compare Dealers side-by-side", value=False)

# FRED overlay
show_fred = st.sidebar.toggle("Show FRED industry benchmark", value=False)

# ---------------------------------------------------------------------------
# Load data
# ---------------------------------------------------------------------------

dealers_tuple = tuple(selected_dealers)
start_str = start_date.isoformat()
end_str = end_date.isoformat()
period_days = max((end_date - start_date).days, 1)

sales_df = compute_sales(dealers_tuple, start_str, end_str, condition_filter)
inv_df = compute_inventory_over_time(dealers_tuple, start_str, end_str, condition_filter)
detail_df = compute_detail_table(dealers_tuple, start_str, end_str, condition_filter, period_days)

# Apply make filter post-query (cheaper than re-querying)
if selected_makes:
    sales_df = sales_df[sales_df["make"].isin(selected_makes)]
    detail_df = detail_df[detail_df["make"].isin(selected_makes)]

# ---------------------------------------------------------------------------
# Page title
# ---------------------------------------------------------------------------

st.title("🚗 Dealer Inventory Analytics")
st.caption(
    "**Note:** 'Sold' = VIN disappeared from inventory feed. May include transfers, "
    "delistings, or data gaps — not confirmed point-of-sale data."
)

# ---------------------------------------------------------------------------
# Row 1 — KPI cards
# ---------------------------------------------------------------------------

total_sold = len(sales_df)
avg_daily = total_sold / period_days

# Largest current inventory
if not inv_df.empty:
    latest_inv = inv_df[inv_df["date"] == inv_df["date"].max()]
    largest_inv_row = latest_inv.loc[latest_inv["inventory"].idxmax()]
    largest_inv_label = f"{largest_inv_row['dealer']} ({largest_inv_row['inventory']:,})"
else:
    largest_inv_label = "—"

# Fastest mover (most sold per current stock)
if not detail_df.empty and "current_stock" in detail_df.columns:
    detail_df_filtered = detail_df[detail_df["current_stock"] > 0].copy()
    if not detail_df_filtered.empty:
        detail_df_filtered["velocity"] = detail_df_filtered["units_sold"] / detail_df_filtered["current_stock"]
        top_row = detail_df_filtered.loc[detail_df_filtered["velocity"].idxmax()]
        fastest_label = f"{top_row['year']:.0f} {top_row['make']} {top_row['model']}"
    else:
        fastest_label = "—"
else:
    fastest_label = "—"

col1, col2, col3, col4 = st.columns(4)
kpi_card(col1, "Total Sold (period)", f"{total_sold:,}")
kpi_card(col2, "Avg Daily Sales", f"{avg_daily:.1f}")
kpi_card(col3, "Largest Inventory", largest_inv_label)
kpi_card(col4, "Fastest Mover", fastest_label)

st.markdown("---")

# ---------------------------------------------------------------------------
# Row 2 — Sales rate bar chart (top make+model by dealer)
# ---------------------------------------------------------------------------

st.subheader("Sales by Make & Model")

if sales_df.empty:
    st.info("No sales detected in this date range.")
else:
    sales_by_model = (
        sales_df.groupby(["dealer", "make", "model"])
        .size()
        .reset_index(name="units_sold")
    )
    sales_by_model["make_model"] = sales_by_model["make"] + " " + sales_by_model["model"]
    # Top 25 make+model combos by total sold
    top_models = (
        sales_by_model.groupby("make_model")["units_sold"].sum()
        .nlargest(25)
        .index.tolist()
    )
    chart_df = sales_by_model[sales_by_model["make_model"].isin(top_models)]

    if not compare_mode:
        fig_bar = px.bar(
            chart_df.sort_values("units_sold", ascending=True),
            x="units_sold",
            y="make_model",
            color="dealer",
            orientation="h",
            labels={"units_sold": "Units Sold", "make_model": "Make & Model", "dealer": "Dealer"},
            height=max(400, len(chart_df) * 22),
        )
        fig_bar.update_layout(yaxis={"categoryorder": "total ascending"}, margin={"l": 200})
        st.plotly_chart(fig_bar, use_container_width=True)
    else:
        # Compare mode: one subplot per dealer
        dealer_cols = st.columns(min(len(selected_dealers), 3))
        for i, dealer in enumerate(selected_dealers):
            d_data = chart_df[chart_df["dealer"] == dealer].sort_values("units_sold", ascending=True)
            with dealer_cols[i % len(dealer_cols)]:
                st.markdown(f"**{dealer}**")
                if d_data.empty:
                    st.info("No sales")
                else:
                    fig_d = px.bar(
                        d_data,
                        x="units_sold",
                        y="make_model",
                        orientation="h",
                        labels={"units_sold": "Units Sold", "make_model": ""},
                        height=300,
                    )
                    fig_d.update_layout(margin={"l": 150, "t": 10})
                    st.plotly_chart(fig_d, use_container_width=True)

st.markdown("---")

# ---------------------------------------------------------------------------
# Row 3 — Inventory trend + Sales rate over time
# ---------------------------------------------------------------------------

trend_group = st.radio(
    "Group trend lines by",
    ["Dealer", "Make & Model"],
    horizontal=True,
    help="Switch between per-dealer lines and per-make/model lines. "
         "Use the Make filter in the sidebar to narrow down models.",
)

if trend_group == "Make & Model":
    makes_tuple = tuple(selected_makes) if selected_makes else ()
    inv_model_df = compute_inventory_by_model(
        dealers_tuple, start_str, end_str, condition_filter, makes_tuple
    )
    if not inv_model_df.empty:
        inv_model_df["make_model"] = inv_model_df["make"].fillna("") + " " + inv_model_df["model"].fillna("")
        inv_model_df["make_model"] = inv_model_df["make_model"].str.strip()
        # If too many lines, surface the top 15 by peak inventory
        top_mm = (
            inv_model_df.groupby("make_model")["inventory"].max()
            .nlargest(15).index.tolist()
        )
        if len(inv_model_df["make_model"].unique()) > 15:
            inv_model_df = inv_model_df[inv_model_df["make_model"].isin(top_mm)]
            st.caption("Showing top 15 make/models by peak inventory. Use the Make filter to narrow down.")

col_left, col_right = st.columns(2)

with col_left:
    st.subheader("Inventory Level Over Time")

    if trend_group == "Dealer":
        if inv_df.empty:
            st.info("No inventory data.")
        else:
            inv_df["date"] = pd.to_datetime(inv_df["date"]).dt.date
            fig_inv = px.line(
                inv_df,
                x="date",
                y="inventory",
                color="dealer",
                labels={"date": "Date", "inventory": "VINs in Feed", "dealer": "Dealer"},
                markers=True,
            )

            if show_fred:
                for series_label, series_id in FRED_SERIES.items():
                    fred_df = fetch_fred(series_id, start_str, end_str)
                    if not fred_df.empty:
                        fred_df["date_str"] = fred_df["date"].dt.strftime("%Y-%m-%d")
                        fig_inv.add_trace(
                            go.Scatter(
                                x=fred_df["date_str"],
                                y=fred_df["value"],
                                mode="lines",
                                name=f"FRED: {series_label}",
                                line={"dash": "dot", "width": 1},
                                yaxis="y2",
                                opacity=0.6,
                            )
                        )
                fig_inv.update_layout(
                    yaxis2={
                        "title": "SAAR (millions)",
                        "overlaying": "y",
                        "side": "right",
                        "showgrid": False,
                    }
                )

            st.plotly_chart(fig_inv, use_container_width=True)

    else:  # Make & Model
        if inv_model_df.empty:
            st.info("No inventory data for selected makes/models.")
        else:
            inv_model_df["date"] = pd.to_datetime(inv_model_df["date"]).dt.date
            fig_inv = px.line(
                inv_model_df,
                x="date",
                y="inventory",
                color="make_model",
                labels={"date": "Date", "inventory": "VINs in Feed", "make_model": "Model"},
                markers=True,
            )
            st.plotly_chart(fig_inv, use_container_width=True)

with col_right:
    st.subheader("Sales Rate Over Time")
    if sales_df.empty:
        st.info("No sales detected.")
    else:
        if trend_group == "Dealer":
            sales_ts = (
                sales_df.groupby(["date_sold", "dealer"])
                .size()
                .reset_index(name="units_sold")
            )
            sales_ts["date_sold"] = pd.to_datetime(sales_ts["date_sold"]).dt.date
            fig_sales = px.line(
                sales_ts,
                x="date_sold",
                y="units_sold",
                color="dealer",
                labels={"date_sold": "Date", "units_sold": "Units Sold", "dealer": "Dealer"},
                markers=True,
            )
        else:  # Make & Model
            sales_df_mm = sales_df.copy()
            sales_df_mm["make_model"] = (
                sales_df_mm["make"].fillna("") + " " + sales_df_mm["model"].fillna("")
            ).str.strip()
            sales_ts = (
                sales_df_mm.groupby(["date_sold", "make_model"])
                .size()
                .reset_index(name="units_sold")
            )
            sales_ts["date_sold"] = pd.to_datetime(sales_ts["date_sold"]).dt.date
            # Cap at top 15 by total sold
            top_mm_sales = (
                sales_ts.groupby("make_model")["units_sold"].sum()
                .nlargest(15).index.tolist()
            )
            if len(sales_ts["make_model"].unique()) > 15:
                sales_ts = sales_ts[sales_ts["make_model"].isin(top_mm_sales)]
                st.caption("Showing top 15 make/models by units sold.")
            fig_sales = px.line(
                sales_ts,
                x="date_sold",
                y="units_sold",
                color="make_model",
                labels={"date_sold": "Date", "units_sold": "Units Sold", "make_model": "Model"},
                markers=True,
            )

        st.plotly_chart(fig_sales, use_container_width=True)

st.markdown("---")

# ---------------------------------------------------------------------------
# Row 4 — Detail table
# ---------------------------------------------------------------------------

st.subheader("Detail Table")

if detail_df.empty:
    st.info("No data for selected filters.")
else:
    detail_display = detail_df.copy()
    # Sell-through %
    denom = detail_display["units_sold"] + detail_display["current_stock"]
    detail_display["sell_through_pct"] = (
        (detail_display["units_sold"] / denom.replace(0, 1) * 100).round(1)
    )
    detail_display["year"] = detail_display["year"].fillna(0).astype(int)
    detail_display = detail_display.rename(columns={
        "dealer":              "Dealer",
        "make":                "Make",
        "model":               "Model",
        "year":                "Year",
        "units_sold":          "Units Sold",
        "avg_days_on_lot":     "Avg Days on Lot",
        "current_stock":       "Current Stock",
        "days_to_sellthrough": "Days to Sell-Through",
        "sell_through_pct":    "Sell-Through %",
    })

    st.dataframe(
        detail_display,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Units Sold":           st.column_config.NumberColumn(format="%d"),
            "Avg Days on Lot":      st.column_config.NumberColumn(format="%.1f days", help="Average days a sold vehicle sat in the feed before disappearing"),
            "Current Stock":        st.column_config.NumberColumn(format="%d"),
            "Days to Sell-Through": st.column_config.NumberColumn(format="%.0f days", help="At the current sales pace, how many days until current stock is gone"),
            "Sell-Through %":       st.column_config.NumberColumn(format="%.1f%%", help="Units sold ÷ (units sold + current stock) — what fraction of total exposure has converted to a sale"),
        },
    )

st.markdown("---")

# ---------------------------------------------------------------------------
# Comparison mode — Sell-through % by dealer
# ---------------------------------------------------------------------------

if compare_mode and not detail_df.empty:
    st.subheader("Sell-Through % by Dealer")
    st_by_dealer = (
        detail_df.groupby("dealer")
        .apply(lambda g: g["units_sold"].sum() / max((g["units_sold"].sum() + g["current_stock"].sum()), 1) * 100)
        .reset_index(name="sell_through_pct")
    )
    fig_st = px.bar(
        st_by_dealer.sort_values("sell_through_pct", ascending=False),
        x="dealer",
        y="sell_through_pct",
        labels={"dealer": "Dealer", "sell_through_pct": "Sell-Through %"},
        color="dealer",
        text_auto=".1f",
    )
    fig_st.update_traces(texttemplate="%{text}%", textposition="outside")
    st.plotly_chart(fig_st, use_container_width=True)
    st.markdown("---")

# ---------------------------------------------------------------------------
# FRED benchmark expander
# ---------------------------------------------------------------------------

with st.expander("Industry Benchmarks (FRED)"):
    fred_cols = st.columns(len(FRED_SERIES))
    for i, (label, sid) in enumerate(FRED_SERIES.items()):
        fred_data = fetch_fred(sid, start_str, end_str)
        with fred_cols[i]:
            st.markdown(f"**{label}**")
            if fred_data.empty:
                st.info("FRED data unavailable (no API key or network issue).")
            else:
                latest_val = fred_data["value"].iloc[-1]
                prior_val = fred_data["value"].iloc[-2] if len(fred_data) > 1 else latest_val
                delta_pct = (latest_val - prior_val) / max(prior_val, 0.001) * 100
                st.metric(
                    label=f"{label} (latest)",
                    value=f"{latest_val:.2f}M",
                    delta=f"{delta_pct:+.1f}% vs prior period",
                )
                fig_fred = px.line(
                    fred_data,
                    x="date",
                    y="value",
                    labels={"date": "Date", "value": "SAAR (millions)"},
                )
                st.plotly_chart(fig_fred, use_container_width=True)

    st.caption(
        "FRED data sourced from the St. Louis Federal Reserve (fred.stlouisfed.org). "
        "TOTALSA = Total Vehicle Sales (SAAR). LAUTOSA = Light Auto Sales (SAAR)."
    )

# ---------------------------------------------------------------------------
# Upload Historical Data
# ---------------------------------------------------------------------------

with st.expander("Upload Historical Data"):
    st.markdown(
        "Manually seed the database with historical CSV exports. "
        "Select a dealer, pick the snapshot date the file represents, then upload one or more CSV files."
    )

    up_dealer = st.selectbox("Dealer", options=list(DEALERS.keys()), key="upload_dealer")
    up_date = st.date_input(
        "Snapshot date",
        value=date.today() - timedelta(days=1),
        key="upload_date",
    )
    up_files = st.file_uploader(
        "CSV file(s)",
        type=["csv"],
        accept_multiple_files=True,
        key="upload_files",
    )

    if st.button("Upload", key="upload_btn"):
        if not up_files:
            st.warning("Please select at least one CSV file.")
        else:
            date_str = up_date.isoformat()
            conn = psycopg2.connect(get_db_url())
            try:
                if snapshot_exists(conn, date_str, up_dealer):
                    st.warning(
                        f"A snapshot for **{up_dealer}** on **{date_str}** already exists — skipping."
                    )
                else:
                    frames: list[pd.DataFrame] = []
                    all_ok = True
                    for f in up_files:
                        try:
                            df = parse_csv_bytes(f.read(), up_dealer)
                            frames.append(df)
                            st.caption(f"Parsed **{f.name}**: {len(df)} VINs")
                        except Exception as exc:
                            st.error(f"Failed to parse **{f.name}**: {exc}")
                            all_ok = False

                    if frames:
                        merged = pd.concat(frames, ignore_index=True).drop_duplicates(subset=["vin"])
                        insert_snapshot(conn, date_str, up_dealer, merged)
                        for f in up_files:
                            log_fetch(conn, up_dealer, f.name, len(merged), "ok (manual upload)")
                        st.cache_data.clear()
                        st.success(
                            f"Uploaded **{len(merged)} VINs** for **{up_dealer}** on **{date_str}**. "
                            "Charts will refresh with the new data."
                        )
                    elif all_ok:
                        st.warning("No valid rows found in the uploaded file(s).")
            finally:
                conn.close()

    st.markdown("---")
    st.markdown("**Delete a snapshot**")
    st.caption("Use this to remove a snapshot that was uploaded to the wrong dealer or date.")

    del_dealer = st.selectbox("Dealer", options=list(DEALERS.keys()), key="delete_dealer")
    del_date = st.date_input(
        "Snapshot date",
        value=date.today() - timedelta(days=1),
        key="delete_date",
    )

    if st.button("Delete Snapshot", type="primary", key="delete_btn"):
        date_str = del_date.isoformat()
        conn = psycopg2.connect(get_db_url())
        try:
            if not snapshot_exists(conn, date_str, del_dealer):
                st.warning(f"No snapshot found for **{del_dealer}** on **{date_str}**.")
            else:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT COUNT(*) FROM snapshots WHERE date = %s AND dealer = %s",
                        (date_str, del_dealer),
                    )
                    count = cur.fetchone()[0]
                    cur.execute(
                        "DELETE FROM snapshots WHERE date = %s AND dealer = %s",
                        (date_str, del_dealer),
                    )
                conn.commit()
                st.cache_data.clear()
                st.success(f"Deleted **{count} VINs** for **{del_dealer}** on **{date_str}**.")
        finally:
            conn.close()

# ---------------------------------------------------------------------------
# Footer
# ---------------------------------------------------------------------------

st.markdown(
    "<hr><small>Dealer Inventory Analytics &nbsp;|&nbsp; "
    "'Sold' = VIN absent from next snapshot — not confirmed POS data &nbsp;|&nbsp; "
    f"Data through {end_str}</small>",
    unsafe_allow_html=True,
)
