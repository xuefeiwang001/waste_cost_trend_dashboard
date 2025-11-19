import streamlit as st
import pandas as pd
import numpy as np
import altair as alt
import snowflake.connector
import psycopg2
import re
from io import BytesIO
from pathlib import Path

# IMPORTANT: expand Streamlit page
st.set_page_config(layout="wide")

# === Unified Colors for all charts ===
COLOR_DBU = "#1f77b4"      
COLOR_OTHER = "#aec7e8"    

# ========================
# 0. Global config: demo vs live
# ========================

# True  -> use local CSV demo data (for GitHub / public)
# False -> connect to real Snowflake & PostgreSQL via st.secrets
USE_DEMO_DATA = True

DATA_DIR = Path("data")
DBU_DEMO_PATH = DATA_DIR / "df DBU.csv"
INROOISSY_DEMO_PATH = DATA_DIR / "df roissy.csv"


# ========================
# 1. Database Connections (only used when USE_DEMO_DATA = False)
# ========================

def get_snowflake_connection():
    conn = snowflake.connector.connect(
        user=st.secrets["snowflake"]["user"],
        password=st.secrets["snowflake"]["password"],
        account=st.secrets["snowflake"]["account"],
        warehouse=st.secrets["snowflake"]["warehouse"],
        database=st.secrets["snowflake"]["database"],
        schema=st.secrets["snowflake"]["schema"],
        role=st.secrets["snowflake"]["role"],
    )
    return conn


def get_postgres_connection():
    conn = psycopg2.connect(
        host=st.secrets["postgres"]["host"],
        port=st.secrets["postgres"].get("port", 5432),
        database=st.secrets["postgres"]["database"],
        user=st.secrets["postgres"]["user"],
        password=st.secrets["postgres"]["password"],
    )
    return conn


# ========================
# 2. Price data (from Excel)
# ========================

def load_and_clean_price_from_workbook(file) -> pd.DataFrame:
    """
    Read sheets like '原数据2024' / '原数据2025',
    build a clean daily price dataframe with columns:
    - date
    - Tot. H.T
    """
    xls = pd.ExcelFile(file)
    frames = []

    for sheet in xls.sheet_names:
        df = pd.read_excel(file, sheet_name=sheet)

        # only process sheets that contain 日期 & Tot. H.T
        if not (("日期" in df.columns) and ("Tot. H.T" in df.columns)):
            continue

        # extract year from sheet name, e.g. '原数据2024'
        m = re.search(r"(\d{4})", sheet)
        if not m:
            continue
        year = m.group(1)

        df["date"] = pd.to_datetime(
            df["日期"].astype(str) + f"/{year}",
            format="%d/%m/%Y",
            errors="coerce",
        )

        sub = df[["date", "Tot. H.T"]].copy()
        sub["Tot. H.T"] = pd.to_numeric(sub["Tot. H.T"], errors="coerce")
        sub = sub.dropna(subset=["date"])
        frames.append(sub)

    if not frames:
        raise ValueError(
            "No valid sheet found containing '日期' and 'Tot. H.T' "
            "(e.g., 原数据2024 / 原数据2025)."
        )

    df_all = pd.concat(frames, ignore_index=True)
    df_all = df_all.sort_values("date").reset_index(drop=True)
    return df_all


def agg_monthly_price(df_all_price: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate daily price to monthly level.
    """
    df = df_all_price.copy()
    df["year"] = df["date"].dt.year
    df["month"] = df["date"].dt.month

    df_price_y_m = (
        df.groupby(["year", "month"], as_index=False)["Tot. H.T"]
        .sum()
        .rename(columns={"Tot. H.T": "total_price"})
    )
    df_price_y_m["x_label"] = (
        df_price_y_m["year"].astype(str)
        + "-"
        + df_price_y_m["month"].astype(str).str.zfill(2)
    )
    df_price_y_m = df_price_y_m.sort_values(["year", "month"]).reset_index(drop=True)
    return df_price_y_m


@st.cache_data(ttl=3600) 
def load_price_monthly_from_bytes(excel_bytes: bytes) -> pd.DataFrame:
    bio = BytesIO(excel_bytes)
    df_all_price = load_and_clean_price_from_workbook(bio)
    df_price_y_m = agg_monthly_price(df_all_price)
    return df_price_y_m


# ========================
# 3. Weight data: DBU + inrooissy
#    - live loaders (Snowflake / Postgres)
#    - demo loaders (CSV)
# ========================

def fetch_dbu_from_snowflake(conn) -> pd.DataFrame:
    """
    Live mode: fetch DBU weight from Snowflake.
    """
    sql = """
    SELECT 
        BOX_ID,
        PRODUIT,
        BOX_WEIGHT,
        NET_WEIGHT,
        DBU_STOCK_IN_AT,
        DBU_STOCK_IN_PDA_VERSION,
        ROI_BIND_PMC
    FROM DBU
    WHERE PRODUIT IN ('FR-DBU-S', 'FR-DBU-R')
      AND ROI_BIND_PMC = TRUE
    """
    df = pd.read_sql(sql, conn)

    df.columns = [c.lower() for c in df.columns]

    df_dbu = df.rename(
        columns={
            "box_id": "reference",
            "produit": "transporter",
            "box_weight": "weight",
            "net_weight": "netweight",
            "dbu_stock_in_at": "stock_in_at",
            "dbu_stock_in_pda_version": "stock_in_pda_version",
        }
    )[
        [
            "reference",
            "transporter",
            "weight",
            "netweight",
            "stock_in_at",
            "stock_in_pda_version",
        ]
    ]

    df_dbu["transporter"] = "DBU-PMC"
    df_dbu["stock_in_at"] = pd.to_datetime(df_dbu["stock_in_at"])
    return df_dbu


def fetch_inrooissy_no_pmcdbu_from_postgres(conn) -> pd.DataFrame:
    """
    Live mode: fetch inrooissy weight from Postgres.
    """
    sql = """
    SELECT 
        wbo.reference,
        sob.transporter,
        sob.weight,
        sob.netweight,
        wbo.stock_in_at,
        wbo.stock_in_pda_version
    FROM whs_box_operation wbo
    JOIN sale_order_box sob ON wbo.reference = sob.id
    WHERE wbo.warehouse = 'EP_CL1' 
      AND wbo.stock_in_at > '2024-01-01' 
      AND wbo.bind_pmc = false 
      AND wbo.stock_in_pda_version IS NOT NULL 
    """
    df = pd.read_sql(sql, conn)
    df.columns = [c.lower() for c in df.columns]
    df["stock_in_at"] = pd.to_datetime(df["stock_in_at"])
    return df


def load_dbu_demo() -> pd.DataFrame:
    """
    Demo mode: load anonymised DBU data from CSV.

    支持两种列结构：
    1）原始 Snowflake 导出：BOX_ID, PRODUIT, BOX_WEIGHT, NET_WEIGHT, DBU_STOCK_IN_AT, DBU_STOCK_IN_PDA_VERSION, ...
    2）已经处理好的版本：reference, transporter, weight, netweight, stock_in_at, stock_in_pda_version
    """
    df = pd.read_csv(DBU_DEMO_PATH)
    df.columns = [c.lower() for c in df.columns]

    if "box_id" in df.columns:
        df = df.rename(
            columns={
                "box_id": "reference",
                "produit": "transporter",
                "box_weight": "weight",
                "net_weight": "netweight",
                "dbu_stock_in_at": "stock_in_at",
                "dbu_stock_in_pda_version": "stock_in_pda_version",
            }
        )

    df["stock_in_at"] = pd.to_datetime(df["stock_in_at"])
    df["transporter"] = "DBU-PMC" 
    return df[
        [
            "reference",
            "transporter",
            "weight",
            "netweight",
            "stock_in_at",
            "stock_in_pda_version",
        ]
    ]


def load_inrooissy_demo() -> pd.DataFrame:
    """
    Demo mode: load anonymised inrooissy data from CSV.

    期望包含列：
        reference, transporter, weight, netweight, stock_in_at, stock_in_pda_version
    """
    df = pd.read_csv(INROOISSY_DEMO_PATH)
    df.columns = [c.lower() for c in df.columns]
    df["stock_in_at"] = pd.to_datetime(df["stock_in_at"])
    return df[
        [
            "reference",
            "transporter",
            "weight",
            "netweight",
            "stock_in_at",
            "stock_in_pda_version",
        ]
    ]


def build_inrooissy_all(
    df_inrooissy_no_pmcdbu: pd.DataFrame, df_dbu: pd.DataFrame
) -> pd.DataFrame:
    """
    Concatenate inrooissy (non-PMC/DBU) with DBU weights.
    """
    df_inrooissy = pd.concat(
        [df_inrooissy_no_pmcdbu, df_dbu], ignore_index=True
    )
    df_inrooissy["stock_in_at"] = pd.to_datetime(df_inrooissy["stock_in_at"])
    return df_inrooissy


def summarize_inrooissy(df_inrooissy: pd.DataFrame) -> pd.DataFrame:
    """
    Summarize weight by year-month-transporter.
    """
    df = df_inrooissy.copy()

    if "stock_in_at" not in df.columns:
        raise KeyError(f"'stock_in_at' not in columns: {df.columns.tolist()}")

    df["year"] = df["stock_in_at"].dt.year
    df["month"] = df["stock_in_at"].dt.month

    df_summary = (
        df.groupby(["year", "month", "transporter"])
        .agg(
            reference_unique=("reference", "nunique"),
            total_weight=("weight", "sum"),
            total_netweight=("netweight", "sum"),
        )
        .reset_index()
        .sort_values(["year", "month", "transporter"])
    )
    return df_summary


def calc_dbu_share(df_summary: pd.DataFrame) -> pd.DataFrame:
    """
    Compute monthly total weight, DBU weight, and DBU share (%)
    使用附件中的逻辑：DBU transporter = 'DBU-PMC'
    """
    df = df_summary.copy()

    # total weight (all transporters)
    month_total = (
        df.groupby(["year", "month"], as_index=False)["total_weight"]
        .sum()
        .rename(columns={"total_weight": "total_weight_all"})
    )

    # DBU weight = sum over 'DBU-PMC' transporter
    dbu_mask = df["transporter"] == "DBU-PMC"
    dbu_total = (
        df[dbu_mask]
        .groupby(["year", "month"], as_index=False)["total_weight"]
        .sum()
        .rename(columns={"total_weight": "total_weight_dbu"})
    )

    merged = pd.merge(month_total, dbu_total, on=["year", "month"], how="left")
    merged["total_weight_dbu"] = merged["total_weight_dbu"].fillna(0)

    merged["dbu_ratio"] = np.where(
        merged["total_weight_all"] > 0,
        merged["total_weight_dbu"] / merged["total_weight_all"] * 100,
        np.nan,
    )

    merged["x_label"] = (
        merged["year"].astype(str)
        + "-"
        + merged["month"].astype(str).str.zfill(2)
    )

    merged = merged.sort_values(["year", "month"]).reset_index(drop=True)
    return merged


def merge_weight_and_price(
    dbu_share: pd.DataFrame, price_monthly: pd.DataFrame
) -> pd.DataFrame:
    """
    Merge weight summary with monthly price data.
    """
    merged_full = pd.merge(
        dbu_share,
        price_monthly[["year", "month", "total_price"]],
        on=["year", "month"],
        how="left",
    )
    merged_full["total_price"] = merged_full["total_price"].fillna(0)
    return merged_full


@st.cache_data(ttl=900) 
def load_weight_summary(use_demo: bool = USE_DEMO_DATA) -> pd.DataFrame:
    """
    Unified entry:
    - Demo mode: read from CSV
    - Live mode: query Snowflake + PostgreSQL via st.secrets
    """
    if use_demo:
        df_dbu = load_dbu_demo()
        df_inrooissy_no_pmcdbu = load_inrooissy_demo()
    else:
        sf_conn = get_snowflake_connection()
        pg_conn = get_postgres_connection()
        try:
            df_dbu = fetch_dbu_from_snowflake(sf_conn)
            df_inrooissy_no_pmcdbu = fetch_inrooissy_no_pmcdbu_from_postgres(pg_conn)
        finally:
            sf_conn.close()
            pg_conn.close()

    df_inrooissy = build_inrooissy_all(df_inrooissy_no_pmcdbu, df_dbu)
    df_summary = summarize_inrooissy(df_inrooissy)
    return df_summary


# ========================
# 4. Charts
# ========================

def chart_weight_price_combo(df: pd.DataFrame):

    df2 = df.copy()

    df2["year"] = df2["year"].astype(int)
    df2["month"] = df2["month"].astype(int)

    df2["x_label"] = (
        df2["month"].astype(str).str.zfill(2)
        + "-"
        + df2["year"].astype(str)
    )

    df2 = df2.sort_values(["month", "year"]).reset_index(drop=True)

    # other weight = total - DBU
    df2["other_weight"] = df2["total_weight_all"] - df2["total_weight_dbu"]

    # DBU ratio文本标签
    df2["ratio_label"] = df2["dbu_ratio"].round(1).astype(str) + "%"

    base = alt.Chart(df2).encode(
        x=alt.X(
            "x_label:N",
            title="Month",
            sort=df2["x_label"].tolist(),
        )
    )

    # stacked bars
    bar = (
        base.transform_fold(
            ["total_weight_dbu", "other_weight"],
            as_=["weight_type", "weight"],
        )
        .transform_calculate(
            weight_label=(
                "datum.weight_type == 'total_weight_dbu' ? "
                "'DBU Weight' : 'Other Weight'"
            )
        )
        .mark_bar()
        .encode(
            y=alt.Y(
                "weight:Q",
                title="Weight (kg)",
                axis=alt.Axis(titleColor="black"),
            ),
            color=alt.Color(
                "weight_label:N",
                title="Weight Type",
                scale=alt.Scale(
                    domain=["DBU Weight", "Other Weight"],
                    range=[COLOR_DBU, COLOR_OTHER],
                ),
            ),
        )
    )

    # price line
    line_price = base.mark_line(point=True, strokeDash=[5, 5]).encode(
        y=alt.Y(
            "total_price:Q",
            title="Total Price (€)",
            axis=alt.Axis(titleColor="red", orient="right"),
        ),
        color=alt.value("red"),
        tooltip=[
            alt.Tooltip("x_label:N", title="Month"),
            alt.Tooltip("total_price:Q", title="Price", format=",.0f"),
        ],
    )

    # DBU ratio text labels
    ratio_text = base.mark_text(
        align="center",
        baseline="bottom",
        dy=-5,
        color="black",
        fontWeight="bold",
        fontSize=11,
    ).encode(
        y=alt.Y("total_weight_all:Q", axis=None),
        text=alt.Text("ratio_label:N"),
        tooltip=[
            alt.Tooltip("x_label:N", title="Month"),
            alt.Tooltip("dbu_ratio:Q", title="DBU Ratio", format=".1f"),
        ],
    )

    chart = (
        alt.layer(bar, line_price, ratio_text)
        .resolve_scale(y="independent") 
        .properties(
            height=400,
            title="Total Weight & Waste Cost (€)",
        )
        .configure_axisX(labelAngle=-40)
    )

    return chart


def chart_single_month(df: pd.DataFrame, month: int):
    """
    For a given month (1–12), draw:
    - Stacked bar: DBU weight + other weight by year (左轴)
    - Line: total_price by year (右轴)
    - DBU ratio text labels
    """
    df2 = df[df["month"] == month].copy()
    if df2.empty:
        return None

    # compute other weight
    df2["other_weight"] = df2["total_weight_all"] - df2["total_weight_dbu"]

    # DBU ratio text labels
    df2["ratio_label"] = df2["dbu_ratio"].round(1).astype(str) + "%"

    df_melt = df2.melt(
        id_vars=["year", "month", "total_price", "dbu_ratio", "ratio_label"],
        value_vars=["total_weight_dbu", "other_weight"],
        var_name="weight_type",
        value_name="weight",
    )

    df_melt["weight_label"] = df_melt["weight_type"].map(
        {
            "total_weight_dbu": "DBU Weight",
            "other_weight": "Other Weight",
        }
    )

    color_scale = alt.Scale(
        domain=["DBU Weight", "Other Weight"],
        range=[COLOR_DBU, COLOR_OTHER],
    )

    base = alt.Chart(df_melt).encode(
        x=alt.X("year:O", title="Year")
    )

 
    bars = base.mark_bar().encode(
        y=alt.Y(
            "weight:Q",
            title="Weight (kg)",
            axis=alt.Axis(
                titleFontSize=11, labelFontSize=10, titleColor="black"
            ),
        ),
        color=alt.Color(
            "weight_label:N",
            scale=color_scale,
            legend=None,
        ),
        tooltip=[
            "year",
            "month",
            "weight_label",
            alt.Tooltip("weight:Q", format=",.0f"),
        ],
    )


    line_price = base.mark_line(
        point=True, strokeDash=[5, 5], color="red"
    ).encode(
        y=alt.Y(
            "total_price:Q",
            title="Price (€)",
            axis=alt.Axis(
                titleColor="red", orient="right",
                titleFontSize=11, labelFontSize=10
            ),
        ),
        tooltip=[
            "year",
            "month",
            alt.Tooltip("total_price:Q", title="Price", format=",.0f"),
        ],
    )

    ratio_text = alt.Chart(df2).mark_text(
        align="center",
        baseline="bottom",
        dy=-5,
        color="black",
        fontWeight="bold",
        fontSize=9,
    ).encode(
        x=alt.X("year:O"),
        y=alt.Y("total_weight_all:Q", axis=None),
        text=alt.Text("ratio_label:N"),
        tooltip=[
            "year",
            "month",
            alt.Tooltip("dbu_ratio:Q", title="DBU Ratio", format=".1f"),
        ],
    )

    chart = (
        alt.layer(bars, line_price, ratio_text)
        .resolve_scale(y="independent") 
        .properties(
            width=280,
            height=260,
            padding={"left": 5, "right": 70, "top": 10, "bottom": 40},
        )
        .configure_axis(labelFontSize=10, titleFontSize=11)
    )

    return chart


# ========================
# 5. Streamlit main app
# ========================

def main():
    st.title("📊 Waste Cost Dashboard")

    with st.sidebar:
        st.header("How to use")
        st.markdown(
            """
        1. **Upload** your Excel file  
           (Keep sheet and column formats consistent)
        
        2. **The dashboard will show:**
           - **Blue bars**: weight in kg  
           - **Red dashed line**: Waste cost in €  
           - **Text labels**: DBU weight ratio (%)
        """
        )

        if USE_DEMO_DATA:
            st.success(
                "Demo mode: using anonymised CSV samples from `data/`.\n\n"
                "No live database connection is required."
            )
        else:
            st.info(
                "Live mode: loading data from Snowflake and PostgreSQL\n"
                "using credentials defined in `.streamlit/secrets.toml`."
            )

    excel_file = st.file_uploader(
        "📂 Upload 2024–2025 price Excel", type=["xlsx", "xls"]
    )

    if excel_file is not None:
        # 1. Price part
        try:
            price_bytes = excel_file.getvalue()
            df_price_y_m = load_price_monthly_from_bytes(price_bytes)
            st.success("✅ Price data loaded and aggregated successfully.")

            with st.expander("🔍 View monthly price summary table"):
                st.dataframe(df_price_y_m)

        except Exception as e:
            st.error(f"❌ Error while parsing price data: {e}")
            return

        # 2. Weight + DBU part
        try:
            with st.spinner("Loading weight data ..."):
                df_summary = load_weight_summary()

            st.success("✅ Weight data loaded successfully.")

            dbu_share = calc_dbu_share(df_summary)
            merged_full = merge_weight_and_price(dbu_share, df_price_y_m)


            with st.sidebar:
                st.header("Data Summary")
                total_months = len(merged_full)
                total_dbu_weight = merged_full["total_weight_dbu"].sum()
                total_all_weight = merged_full["total_weight_all"].sum()
                avg_dbu_ratio = merged_full["dbu_ratio"].mean()
                total_price = merged_full["total_price"].sum()

                st.metric("Total Months", total_months)
                st.metric("Total DBU Weight", f"{total_dbu_weight:,.0f} kg")
                st.metric("Total All Weight", f"{total_all_weight:,.0f} kg")
                st.metric("Avg DBU Ratio", f"{avg_dbu_ratio:.1f}%")
                st.metric("Total Cost", f"€{total_price:,.0f}")

            # Main chart
            st.subheader("📊 Total Weight & Waste Cost")
            st.altair_chart(
                chart_weight_price_combo(merged_full),
                use_container_width=True,
            )

            with st.expander("🔍 View detailed data table"):
                st.dataframe(
                    merged_full[
                        [
                            "year",
                            "month",
                            "total_weight_all",
                            "total_weight_dbu",
                            "dbu_ratio",
                            "total_price",
                        ]
                    ].round(2)
                )

            # Small multiples by month
            st.subheader("📅 Monthly Analysis")

            months = sorted(merged_full["month"].dropna().unique())
            cols = st.columns(3)  # 3 charts per row

            for idx, m in enumerate(months):
                chart = chart_single_month(merged_full, m)
                if chart is None:
                    continue

                col = cols[idx % 3]
                with col:
                    st.markdown(f"**Month {int(m)}**")
                    st.altair_chart(chart, use_container_width=True)

        except Exception as e:
            st.error(f"❌ Error while loading or processing weight data: {e}")
            if not USE_DEMO_DATA:
                st.error("Please check your database connections and credentials.")
    else:
        st.info("👆 Please upload the Excel file first to start the analysis.")


if __name__ == "__main__":
    main()
