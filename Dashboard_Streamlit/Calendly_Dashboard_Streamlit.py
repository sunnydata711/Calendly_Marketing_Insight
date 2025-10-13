# Calendly_Dashboard_Tabs.py
# Tabs:
# 1.1 Daily Bookings by Channel
# 1.2 CPB by Channel (horizontal labels)
# 1.3 Bookings Trend Over Time (daily/weekly + cumulative area)
# 1.4 Channel Attribution (CPB & Volume)
# 1.5 Booking Volume by Time Slot / Day of Week  <-- NEW
# 1.6 Understand Meeting Load per Employee       <-- NEW

import time
import boto3
import pandas as pd
import altair as alt
import streamlit as st

# --- Secrets-driven AWS session for Streamlit Cloud ---
REGION     = st.secrets.get("AWS_REGION", "us-east-1")
WORKGROUP  = st.secrets.get("ATHENA_WORKGROUP", "primary")
OUTPUT_S3  = st.secrets.get("ATHENA_S3_OUTPUT", "s3://betty-athena-results/calendly/")  # new for streamlit
DATABASE_D = st.secrets.get("ATHENA_DATABASE", "marketing_data")
CATALOG    = st.secrets.get("ATHENA_CATALOG", "AwsDataCatalog")  # optional; Athena defaults to AwsDataCatalog

def make_session():
    # If you pasted keys into Streamlit Secrets, use them; otherwise credentials chain applies.
    return boto3.Session(
        aws_access_key_id=st.secrets.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=st.secrets.get("AWS_SECRET_ACCESS_KEY"),
        #aws_session_token=st.secrets.get("AWS_SESSION_TOKEN"),
        region_name=REGION,
    )


st.set_page_config(page_title="Calendly Marketing Dashboard", page_icon="📅", layout="wide")
st.title("📅 Calendly Marketing Dashboard")

# ===============================
# Sidebar – shared Athena settings
# ===============================
# with st.sidebar:
#     st.header("Athena Settings")
#     database  = st.text_input("Athena Database", value="marketing_data")
#     workgroup = st.text_input("Workgroup", value="primary")
#     output    = st.text_input(
#         "S3 output path",
#         value="s3://dea-calendly-data/tmp/athena-results/",
#         help="Athena writes query results here (must exist; require s3:PutObject)."
#     )

# ===============================
# Sidebar – shared Athena settings
# ===============================
with st.sidebar:
    st.header("Athena Settings")
    database  = st.text_input("Athena Database", value=DATABASE_D)
    workgroup = st.text_input("Workgroup", value=WORKGROUP)
    output    = st.text_input(
        "S3 output path",
        value=OUTPUT_S3,
        help="Athena writes query results here (must exist; require s3:PutObject)."
    )



# ------------
# Athena helper
# ------------
# def run_athena_query(sql: str, database: str, output: str, workgroup: str) -> pd.DataFrame:
#     client = boto3.client("athena")
#     q = client.start_query_execution(
#         QueryString=sql,
#         QueryExecutionContext={"Database": database},
#         ResultConfiguration={"OutputLocation": output},
#         WorkGroup=workgroup
#     )
#     qid = q["QueryExecutionId"]

#     # wait for completion
#     while True:
#         s = client.get_query_execution(QueryExecutionId=qid)
#         state = s["QueryExecution"]["Status"]["State"]
#         if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
#             break
#         time.sleep(0.4)
#     if state != "SUCCEEDED":
#         reason = s["QueryExecution"]["Status"].get("StateChangeReason", "Unknown")
#         raise RuntimeError(f"Athena query {state}: {reason}")

#     # paginate results
#     paginator = client.get_paginator("get_query_results")
#     cols, rows = None, []
#     for page in paginator.paginate(QueryExecutionId=qid):
#         if cols is None:
#             cols = [c["Label"] for c in page["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]]
#         for r in page["ResultSet"]["Rows"]:
#             data = [x.get("VarCharValue") for x in r["Data"]]
#             if data == cols:  # skip header row
#                 continue
#             rows.append(data)
#     return pd.DataFrame(rows, columns=cols or [])

# ------------
# Athena helper
# ------------
def run_athena_query(sql: str, database: str, output: str, workgroup: str) -> pd.DataFrame:
    client = make_session().client("athena", region_name=REGION)
    q = client.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={
            "Database": database,
            "Catalog": CATALOG,  # explicit; fine to keep
        },
        ResultConfiguration={"OutputLocation": output},
        WorkGroup=workgroup,
    )
    qid = q["QueryExecutionId"]

    # wait for completion
    while True:
        s = client.get_query_execution(QueryExecutionId=qid)
        state = s["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(0.4)
    if state != "SUCCEEDED":
        reason = s["QueryExecution"]["Status"].get("StateChangeReason", "Unknown")
        raise RuntimeError(f"Athena query {state}: {reason}")

    # paginate results
    paginator = client.get_paginator("get_query_results")
    cols, rows = None, []
    for page in paginator.paginate(QueryExecutionId=qid):
        if cols is None:
            cols = [c["Label"] for c in page["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]]
        for r in page["ResultSet"]["Rows"]:
            data = [x.get("VarCharValue") for x in r["Data"]]
            if data == cols:  # skip header row
                continue
            rows.append(data)
    return pd.DataFrame(rows, columns=cols or [])



# ===============================
# Tabs
# ===============================
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "1.1 Daily Bookings by Channel",
    "1.2 Cost Per Booking (CPB) by Channel",
    "1.3 Bookings Trend Over Time",
    "1.4 Channel Attribution (CPB & Volume)",
    "1.5 Time Slot / Day of Week",
    "1.6 Meeting Load per Employee",
])

# ------------------------------------------------
# TAB 1 — Daily Bookings by Channel
# ------------------------------------------------
with tab1:
    table_daily = st.text_input("Athena Table for daily bookings", value="daily_bookings_by_channel", key="t1")

    try:
        range_sql = f"SELECT MIN(booking_date) AS min_d, MAX(booking_date) AS max_d FROM {database}.{table_daily}"
        df_range = run_athena_query(range_sql, database, output, workgroup)
        min_d = pd.to_datetime(df_range.iloc[0]["min_d"]).date() if not df_range.empty and df_range.iloc[0]["min_d"] else None
        max_d = pd.to_datetime(df_range.iloc[0]["max_d"]).date() if not df_range.empty and df_range.iloc[0]["max_d"] else None
    except Exception as e:
        st.error("Failed to read date range for Tab 1.")
        st.exception(e)
        st.stop()

    if not min_d or not max_d:
        st.info("No data in daily table yet.")
    else:
        dr = st.date_input("Date range", (pd.to_datetime(min_d), pd.to_datetime(max_d)), key="t1_dr")
        sql_main = f"""
        SELECT booking_date, channel, bookings
        FROM {database}.{table_daily}
        WHERE booking_date BETWEEN DATE '{dr[0]}' AND DATE '{dr[1]}'
        ORDER BY booking_date, channel
        """
        with st.expander("SQL (Tab 1)"):
            st.code(sql_main, language="sql")

        try:
            df = run_athena_query(sql_main, database, output, workgroup)
            if not df.empty:
                df["bookings"] = pd.to_numeric(df["bookings"])
                df["booking_date"] = pd.to_datetime(df["booking_date"]).dt.date
        except Exception as e:
            st.error("Daily bookings query failed.")
            st.exception(e)
            st.stop()

        if df.empty:
            st.info("No data for selected range.")
        else:
            c1, c2, c3 = st.columns(3)
            c1.metric("Total bookings", int(df["bookings"].sum()))
            c2.metric("Avg per day", round(df.groupby("booking_date")["bookings"].sum().mean(), 2))
            c3.metric("Channels", df["channel"].nunique())

            st.subheader("Trend by Channel")
            chart = (
                alt.Chart(df)
                .mark_line(point=True)
                .encode(
                    x=alt.X("booking_date:T", title="Date"),
                    y=alt.Y("bookings:Q", title="Bookings"),
                    color="channel:N",
                    tooltip=["booking_date:T", "channel:N", "bookings:Q"],
                )
                .properties(height=360)
            )
            st.altair_chart(chart, use_container_width=True)

            st.subheader("Daily comparison table")
            pivot = df.pivot_table(index="booking_date", columns="channel", values="bookings", fill_value=0)
            pivot["TOTAL"] = pivot.sum(axis=1)
            st.dataframe(pivot.reset_index(), use_container_width=True)

            st.download_button("Download filtered detail (CSV)", df.to_csv(index=False).encode(), "daily_bookings_by_channel.csv")
            st.download_button("Download comparison (CSV)", pivot.reset_index().to_csv(index=False).encode(), "daily_comparison_by_channel.csv")

# ------------------------------------------------
# TAB 2 — CPB by Channel  (horizontal, bigger x-axis labels)
# ------------------------------------------------
with tab2:
    st.markdown("### 1.2 Cost Per Booking (CPB) by Channel")
    st.caption("CPB = Total Spend / Total Booked Calls")

    table_cpb = st.text_input("Athena Table for CPB", value="cpb_by_channel", key="t2")

    sql_cpb = f"SELECT channel, total_spend, total_bookings, cpb FROM {database}.{table_cpb}"
    with st.expander("SQL (Tab 2)"):
        st.code(sql_cpb, language="sql")

    try:
        df2 = run_athena_query(sql_cpb, database, output, workgroup)
        if not df2.empty:
            for col in ("total_spend", "total_bookings", "cpb"):
                df2[col] = pd.to_numeric(df2[col])
            df2["channel_display"] = df2["channel"].str.replace("_", " ", regex=False).str.title()
    except Exception as e:
        st.error("CPB query failed.")
        st.exception(e)
        st.stop()

    if df2.empty:
        st.info("No rows in CPB table.")
    else:
        total_spend = float(df2["total_spend"].sum())
        total_bookings = int(df2["total_bookings"].sum())
        avg_cpb_weighted = (total_spend / total_bookings) if total_bookings else 0.0

        k1, k2, k3 = st.columns(3)
        k1.metric("Total Bookings", f"{total_bookings:,}")
        k2.metric("Total Spend", f"${total_spend:,.2f}")
        k3.metric("Average CPB (weighted)", f"${avg_cpb_weighted:,.2f}")

        st.subheader("Channel vs CPB (lower is better)")
        chart2 = (
            alt.Chart(df2.sort_values("cpb"))
            .mark_bar()
            .encode(
                x=alt.X(
                    "channel_display:N",
                    sort="-y",
                    title="Channel",
                    axis=alt.Axis(labelAngle=0, labelFontSize=14, titleFontSize=16, labelLimit=1000),
                ),
                y=alt.Y("cpb:Q", title="Cost per Booking (CPB)"),
                tooltip=[
                    alt.Tooltip("channel:N", title="Channel (raw)"),
                    alt.Tooltip("total_spend:Q", title="Total Spend", format="$.2f"),
                    alt.Tooltip("total_bookings:Q", title="Total Bookings", format=",.0f"),
                    alt.Tooltip("cpb:Q", title="CPB", format="$.2f"),
                ],
            )
            .properties(height=380)
        )
        st.altair_chart(chart2, use_container_width=True)

        st.subheader("Leaderboard: Channel | Spend | Bookings | CPB")
        st.dataframe(df2.sort_values("cpb").reset_index(drop=True), use_container_width=True)
        st.download_button("Download CPB table (CSV)", df2.to_csv(index=False).encode(), "cpb_by_channel.csv")

# ------------------------------------------------
# TAB 3 — Bookings Trend Over Time (Daily/Weekly + Cumulative)
# ------------------------------------------------

with tab3:
    st.markdown("### 1.3 Bookings Trend Over Time")
    st.caption("Track daily/weekly booking volume by source and cumulative growth.")

    table_cum = st.text_input(
        "Athena Table (daily, with cumulative)",
        value="cumulative_bookings_by_source_daily_dt",
        key="t3_table"
    )

    # Discover min/max dates & all sources
    try:
        # dt is DATE in the new table
        range_sql = f"SELECT MIN(dt) AS min_d, MAX(dt) AS max_d FROM {database}.{table_cum}"
        df_range = run_athena_query(range_sql, database, output, workgroup)
        min_d = pd.to_datetime(df_range.iloc[0]["min_d"]).date() if not df_range.empty and df_range.iloc[0]["min_d"] else None
        max_d = pd.to_datetime(df_range.iloc[0]["max_d"]).date() if not df_range.empty and df_range.iloc[0]["max_d"] else None

        src_sql = f"SELECT DISTINCT source FROM {database}.{table_cum} WHERE source IS NOT NULL ORDER BY 1"
        df_src = run_athena_query(src_sql, database, output, workgroup)
        all_sources = sorted(df_src["source"].dropna().tolist()) if not df_src.empty else []
    except Exception as e:
        st.error("Failed to discover date range or sources for Tab 3.")
        st.exception(e)
        st.stop()

    if not min_d or not max_d:
        st.info("No data in the cumulative table yet.")
        st.stop()

    colA, colB = st.columns([2, 3])
    with colA:
        dr = st.date_input("Date range", (pd.to_datetime(min_d), pd.to_datetime(max_d)), key="t3_dr")
        freq = st.radio("Aggregate", ["Daily", "Weekly"], horizontal=True, key="t3_freq")
    with colB:
        sel_src = st.multiselect("Sources", options=all_sources, default=all_sources, key="t3_sources")

    # ---------- Build SQL ----------
    date_from = pd.to_datetime(dr[0]).date().isoformat()
    date_to   = pd.to_datetime(dr[1]).date().isoformat()

    # NOTE: use dt instead of booking_date
    base_where = f"dt BETWEEN DATE '{date_from}' AND DATE '{date_to}'"

    # safer IN-list construction
    src_filter = ""
    if sel_src and len(sel_src) < len(all_sources):
        quoted = ("'" + s.replace("'", "''") + "'" for s in sel_src)
        in_list = ",".join(quoted)
        src_filter = f" AND source IN ({in_list})"

    if freq == "Daily":
        sql_daily = f"""
        SELECT dt, source,
               CAST(bookings AS BIGINT) AS bookings,
               CAST(cumulative_bookings AS BIGINT) AS cumulative_bookings
        FROM {database}.{table_cum}
        WHERE {base_where}{src_filter}
        ORDER BY dt, source
        """
        with st.expander("SQL (Daily)"):
            st.code(sql_daily, language="sql")
        df3 = run_athena_query(sql_daily, database, output, workgroup)
        if not df3.empty:
            df3["dt"] = pd.to_datetime(df3["dt"]).dt.date
            df3["bookings"] = pd.to_numeric(df3["bookings"])
            df3["cumulative_bookings"] = pd.to_numeric(df3["cumulative_bookings"])
            # unify the plotting column name expected downstream
            plot_df = df3.rename(columns={"dt": "booking_date"}).copy()
            cum_df = plot_df.sort_values(["source", "booking_date"]).copy()
        else:
            plot_df = pd.DataFrame()
            cum_df = pd.DataFrame()
    else:
        # Weekly aggregation: truncate dt to Monday-start week
        sql_week = f"""
        SELECT CAST(date_trunc('week', dt) AS DATE) AS week_start,
               source,
               SUM(CAST(bookings AS BIGINT)) AS bookings
        FROM {database}.{table_cum}
        WHERE {base_where}{src_filter}
        GROUP BY CAST(date_trunc('week', dt) AS DATE), source
        ORDER BY week_start, source
        """
        with st.expander("SQL (Weekly)"):
            st.code(sql_week, language="sql")
        dfw = run_athena_query(sql_week, database, output, workgroup)
        if not dfw.empty:
            dfw["week_start"] = pd.to_datetime(dfw["week_start"]).dt.date
            dfw["bookings"] = pd.to_numeric(dfw["bookings"])
            dfw = dfw.sort_values(["source", "week_start"])
            dfw["cumulative_bookings"] = dfw.groupby("source")["bookings"].cumsum()
            plot_df = dfw.rename(columns={"week_start": "booking_date"})
            cum_df = plot_df.copy()
        else:
            plot_df = pd.DataFrame()
            cum_df = pd.DataFrame()

    if plot_df.empty:
        st.info("No data for the selected filters.")
        st.stop()

    # ---------- KPIs ----------
    k1, k2, k3 = st.columns(3)
    k1.metric("Total Bookings", int(plot_df["bookings"].sum()))
    k2.metric("Avg per period", round(plot_df.groupby("booking_date")["bookings"].sum().mean(), 2))
    k3.metric("Sources", plot_df["source"].nunique())

    # ---------- Line chart: bookings by source ----------
    st.subheader("Trend: Bookings by Source")
    line = (
        alt.Chart(plot_df)
        .mark_line(point=True)
        .encode(
            x=alt.X("booking_date:T", title="Date"),
            y=alt.Y("bookings:Q", title="Bookings"),
            color=alt.Color("source:N", title="Source"),
            tooltip=["booking_date:T", "source:N", "bookings:Q"],
        )
        .properties(height=360)
    )
    st.altair_chart(line, use_container_width=True)

    # ---------- Area chart: cumulative bookings by source ----------
    st.subheader("Cumulative Bookings (Area)")
    area = (
        alt.Chart(cum_df)
        .mark_area(opacity=0.6)
        .encode(
            x=alt.X("booking_date:T", title="Date"),
            y=alt.Y("cumulative_bookings:Q", title="Cumulative Bookings"),
            color=alt.Color("source:N", title="Source"),
            tooltip=["booking_date:T", "source:N", "cumulative_bookings:Q"],
        )
        .properties(height=360)
    )
    st.altair_chart(area, use_container_width=True)

    # ---------- Tables + downloads ----------
    st.subheader("Detail (current aggregation)")
    st.dataframe(plot_df.sort_values(["booking_date", "source"]).reset_index(drop=True), use_container_width=True)

    st.download_button(
        "Download detailed time series (CSV)",
        plot_df.to_csv(index=False).encode(),
        "bookings_trend_over_time.csv"
    )

    st.subheader("Pivot: Date × Source (Bookings)")
    pivot = plot_df.pivot_table(index="booking_date", columns="source", values="bookings", fill_value=0)
    st.dataframe(pivot.reset_index(), use_container_width=True)
    st.download_button(
        "Download pivot (CSV)",
        pivot.reset_index().to_csv(index=False).encode(),
        "bookings_trend_pivot.csv"
    )



# ------------------------------------------------
# TAB 4 — Channel Attribution (CPB & Volume Leaderboard)
# ------------------------------------------------
with tab4:
    st.markdown("### 1.4 Channel Attribution (CPB & Volume Leaderboard)")
    st.caption("Leaderboard, CPB heatmap (source × campaign), and top sources by bookings.")

    table_cc = st.text_input(
        "Athena Table (source × campaign daily allocation)",
        value="channel_campaign_daily_dt",
        key="t4_table"
    )

    # --- Discover min/max date, sources, campaigns
    try:
        range_sql = f"SELECT MIN(dt) AS min_d, MAX(dt) AS max_d FROM {database}.{table_cc}"
        df_range = run_athena_query(range_sql, database, output, workgroup)
        min_d = pd.to_datetime(df_range.iloc[0]['min_d']).date() if not df_range.empty and df_range.iloc[0]['min_d'] else None
        max_d = pd.to_datetime(df_range.iloc[0]['max_d']).date() if not df_range.empty and df_range.iloc[0]['max_d'] else None

        src_sql = f"SELECT DISTINCT source FROM {database}.{table_cc} WHERE source IS NOT NULL ORDER BY 1"
        df_src = run_athena_query(src_sql, database, output, workgroup)
        all_sources = sorted(df_src['source'].dropna().tolist()) if not df_src.empty else []

        camp_sql = f"SELECT DISTINCT campaign FROM {database}.{table_cc} ORDER BY 1"
        df_camp = run_athena_query(camp_sql, database, output, workgroup)
        all_campaigns = [c for c in df_camp['campaign'].tolist() if c is not None] if not df_camp.empty else []
    except Exception as e:
        st.error("Failed to discover date range/sources/campaigns for Tab 4.")
        st.exception(e)
        st.stop()

    if not min_d or not max_d:
        st.info("No data in the channel_campaign_daily table yet.")
        st.stop()

    # --- Filters
    colA, colB = st.columns([2, 3])
    with colA:
        dr = st.date_input("Date range", (pd.to_datetime(min_d), pd.to_datetime(max_d)), key="t4_dr")
        top_n = st.number_input("Top N sources by bookings (bar chart)", min_value=3, max_value=50, value=10, step=1, key="t4_topn")
    with colB:
        sel_src = st.multiselect("Filter Sources", options=all_sources, default=all_sources, key="t4_sources")
        sel_cmp = st.multiselect("Filter Campaigns", options=all_campaigns, default=all_campaigns, key="t4_campaigns")

    # --- Build WHERE & filters (use dt)
    date_from = pd.to_datetime(dr[0]).date().isoformat()
    date_to   = pd.to_datetime(dr[1]).date().isoformat()
    where = [f"dt BETWEEN DATE '{date_from}' AND DATE '{date_to}'"]

    if sel_src and len(sel_src) < len(all_sources):
        quoted = ("'" + s.replace("'", "''") + "'" for s in sel_src)
        where.append(f"source IN ({','.join(quoted)})")

    if sel_cmp and len(sel_cmp) < len(all_campaigns):
        quoted = ("'" + c.replace("'", "''") + "'" for c in sel_cmp)
        where.append(f"campaign IN ({','.join(quoted)})")

    where_sql = "WHERE " + " AND ".join(where) if where else ""

    # --- Aggregate (leaderboard) query
    agg_sql = f"""
    SELECT
      source,
      campaign,
      SUM(CAST(total_bookings AS BIGINT)) AS bookings,
      ROUND(SUM(CAST(total_spend AS DOUBLE)), 2) AS spend,
      ROUND(SUM(CAST(total_spend AS DOUBLE)) / NULLIF(SUM(CAST(total_bookings AS BIGINT)), 0), 2) AS cpb
    FROM {database}.{table_cc}
    {where_sql}
    GROUP BY source, campaign
    """
    with st.expander("SQL (Aggregated source × campaign)"):
        st.code(agg_sql, language="sql")

    try:
        agg = run_athena_query(agg_sql, database, output, workgroup)
        if not agg.empty:
            agg["bookings"] = pd.to_numeric(agg["bookings"])
            agg["spend"] = pd.to_numeric(agg["spend"])
            agg["cpb"] = pd.to_numeric(agg["cpb"])
            agg["source_display"] = agg["source"].str.replace("_", " ", regex=False).str.title()
            agg["campaign_display"] = agg["campaign"].fillna("(none)").replace("", "(none)")
    except Exception as e:
        st.error("Attribution aggregation query failed.")
        st.exception(e)
        st.stop()

    if agg.empty:
        st.info("No data for current filters.")
        st.stop()

    # ---------- KPI rollups ----------
    total_bookings = int(agg["bookings"].sum())
    total_spend = float(agg["spend"].sum())
    blended_cpb = (total_spend / total_bookings) if total_bookings else 0.0

    k1, k2, k3 = st.columns(3)
    k1.metric("Total Bookings", f"{total_bookings:,}")
    k2.metric("Total Spend", f"${total_spend:,.2f}")
    k3.metric("Blended CPB", f"${blended_cpb:,.2f}")

    # ---------- Leaderboard table ----------
    st.subheader("Leaderboard — Source × Campaign")
    leaderboard = agg.sort_values(["bookings", "cpb"], ascending=[False, True]).reset_index(drop=True)
    st.dataframe(leaderboard[["source", "campaign", "bookings", "spend", "cpb"]], use_container_width=True)
    st.download_button("Download Leaderboard (CSV)", leaderboard.to_csv(index=False).encode(), "channel_campaign_leaderboard.csv")

    # ---------- Heatmap: CPB by Channel and Campaign ----------
    st.subheader("Heatmap — CPB by Channel and Campaign")
    heat_df = agg.copy()
    heat_df["campaign_display"] = heat_df["campaign_display"].astype(str)
    heat = (
        alt.Chart(heat_df)
        .mark_rect()
        .encode(
            x=alt.X("campaign_display:N", title="Campaign",
                    axis=alt.Axis(labelAngle=0, labelLimit=1000, labelFontSize=12, titleFontSize=14)),
            y=alt.Y("source_display:N", title="Channel",
                    axis=alt.Axis(labelLimit=200, labelFontSize=12, titleFontSize=14)),
            color=alt.Color("cpb:Q", title="CPB", scale=alt.Scale(scheme="blues")),
            tooltip=[
                alt.Tooltip("source:N", title="Source"),
                alt.Tooltip("campaign:N", title="Campaign"),
                alt.Tooltip("bookings:Q", title="Bookings", format=",.0f"),
                alt.Tooltip("spend:Q", title="Spend", format="$.2f"),
                alt.Tooltip("cpb:Q", title="CPB", format="$.2f"),
            ],
        )
        .properties(height=420)
    )
    st.altair_chart(heat, use_container_width=True)

    # ---------- Bar chart: Top-performing sources by bookings ----------
    st.subheader("Top Sources by Bookings")
    top_sources = (
        agg.groupby("source", as_index=False)[["bookings", "spend"]].sum()
        .assign(cpb=lambda d: d["spend"] / d["bookings"].where(d["bookings"] != 0, pd.NA))
        .sort_values("bookings", ascending=False)
        .head(int(top_n))
    )
    top_sources["source_display"] = top_sources["source"].str.replace("_", " ", regex=False).str.title()

    bars = (
        alt.Chart(top_sources)
        .mark_bar()
        .encode(
            x=alt.X("source_display:N", title="Channel",
                    axis=alt.Axis(labelAngle=0, labelLimit=1000, labelFontSize=13, titleFontSize=15),
                    sort="-y"),
            y=alt.Y("bookings:Q", title="Total Bookings"),
            tooltip=[
                alt.Tooltip("source:N", title="Source (raw)"),
                alt.Tooltip("bookings:Q", title="Bookings", format=",.0f"),
                alt.Tooltip("spend:Q", title="Spend", format="$.2f"),
                alt.Tooltip("cpb:Q", title="CPB", format="$.2f"),
            ],
        )
        .properties(height=380)
    )
    st.altair_chart(bars, use_container_width=True)

    # ---------- Pivots + downloads ----------
    st.subheader("Pivot — CPB Matrix (Channel × Campaign)")
    cpb_pivot = agg.pivot_table(index="source", columns="campaign", values="cpb", aggfunc="mean")
    st.dataframe(cpb_pivot.reset_index(), use_container_width=True)
    st.download_button("Download CPB Pivot (CSV)", cpb_pivot.reset_index().to_csv(index=False).encode(), "cpb_heatmap_pivot.csv")

    st.subheader("Pivot — Bookings Matrix (Channel × Campaign)")
    bk_pivot = agg.pivot_table(index="source", columns="campaign", values="bookings", aggfunc="sum", fill_value=0)
    st.dataframe(bk_pivot.reset_index(), use_container_width=True)
    st.download_button("Download Bookings Pivot (CSV)", bk_pivot.reset_index().to_csv(index=False).encode(), "bookings_matrix_pivot.csv")



# ------------------------------------------------
# TAB 5 — Booking Volume by Time Slot / Day of Week
# ------------------------------------------------
with tab5:
    st.markdown("### 1.5 Booking Volume by Time Slot / Day of Week")
    st.caption("Heatmap (Hour × DOW), histogram by hour, and pie by DOW. Data: booking_time_features")

    table_time = st.text_input(
        "Athena Table (time features)",
        value="booking_time_features",
        key="t5_table"
    )

    # Discover min/max dates & sources
    try:
        range_sql = f"SELECT MIN(booking_date_local) AS min_d, MAX(booking_date_local) AS max_d FROM {database}.{table_time}"
        df_range = run_athena_query(range_sql, database, output, workgroup)
        min_d = pd.to_datetime(df_range.iloc[0]["min_d"]).date() if not df_range.empty and df_range.iloc[0]["min_d"] else None
        max_d = pd.to_datetime(df_range.iloc[0]["max_d"]).date() if not df_range.empty and df_range.iloc[0]["max_d"] else None

        src_sql = f"SELECT DISTINCT source FROM {database}.{table_time} WHERE source IS NOT NULL ORDER BY 1"
        df_src = run_athena_query(src_sql, database, output, workgroup)
        all_sources = sorted(df_src["source"].dropna().tolist()) if not df_src.empty else []
    except Exception as e:
        st.error("Failed to discover date range/sources for Tab 5.")
        st.exception(e)
        st.stop()

    if not min_d or not max_d:
        st.info("No data in the time-features table yet.")
        st.stop()

    colA, colB = st.columns([2, 3])
    with colA:
        dr = st.date_input("Local Date range (NY)", (pd.to_datetime(min_d), pd.to_datetime(max_d)), key="t5_dr")
    with colB:
        sel_src = st.multiselect("Filter Sources", options=all_sources, default=all_sources, key="t5_sources")

    # WHERE
    date_from = pd.to_datetime(dr[0]).date().isoformat()
    date_to   = pd.to_datetime(dr[1]).date().isoformat()
    where = [f"booking_date_local BETWEEN DATE '{date_from}' AND DATE '{date_to}'"]
    if sel_src and len(sel_src) < len(all_sources):
        quoted = ("'" + s.replace("'", "''") + "'" for s in sel_src)
        where.append(f"source IN ({','.join(quoted)})")
    where_sql = "WHERE " + " AND ".join(where) if where else ""

    # 1) Heatmap Hour × DOW
    sql_heat = f"""
    SELECT dow_num, dow_name, hour_of_day, COUNT(DISTINCT booking_id) AS bookings
    FROM {database}.{table_time}
    {where_sql}
    GROUP BY dow_num, dow_name, hour_of_day
    ORDER BY dow_num, hour_of_day
    """
    with st.expander("SQL (Heatmap)"):
        st.code(sql_heat, language="sql")

    heat_df = run_athena_query(sql_heat, database, output, workgroup)
    if heat_df.empty:
        st.info("No data for selected filters.")
        st.stop()
    heat_df["dow_num"] = pd.to_numeric(heat_df["dow_num"])
    heat_df["hour_of_day"] = pd.to_numeric(heat_df["hour_of_day"])
    heat_df["bookings"] = pd.to_numeric(heat_df["bookings"])

    st.subheader("Heatmap — Hour of Day × Day of Week")
    heat = (
        alt.Chart(heat_df)
        .mark_rect()
        .encode(
            x=alt.X("hour_of_day:O", title="Hour (0–23)",
                    axis=alt.Axis(labelAngle=0, labelFontSize=12, titleFontSize=14)),
            y=alt.Y("dow_name:N", title="Day of Week",
                    sort=["Mon","Tue","Wed","Thu","Fri","Sat","Sun"],
                    axis=alt.Axis(labelFontSize=12, titleFontSize=14)),
            color=alt.Color("bookings:Q", title="Bookings"),
            tooltip=["dow_name:N", "hour_of_day:O", alt.Tooltip("bookings:Q", title="Bookings", format=",.0f")]
        )
        .properties(height=320)
    )
    st.altair_chart(heat, use_container_width=True)

    # 2) Histogram by Hour
    sql_hour = f"""
    SELECT hour_of_day, COUNT(DISTINCT booking_id) AS bookings
    FROM {database}.{table_time}
    {where_sql}
    GROUP BY hour_of_day
    ORDER BY hour_of_day
    """
    with st.expander("SQL (Histogram by Hour)"):
        st.code(sql_hour, language="sql")
    hour_df = run_athena_query(sql_hour, database, output, workgroup)
    if not hour_df.empty:
        hour_df["hour_of_day"] = pd.to_numeric(hour_df["hour_of_day"])
        hour_df["bookings"] = pd.to_numeric(hour_df["bookings"])

    st.subheader("Histogram — Bookings by Hour")
    hist = (
        alt.Chart(hour_df)
        .mark_bar()
        .encode(
            x=alt.X("hour_of_day:O", title="Hour (0–23)",
                    axis=alt.Axis(labelAngle=0, labelFontSize=12, titleFontSize=14)),
            y=alt.Y("bookings:Q", title="Bookings"),
            tooltip=[alt.Tooltip("hour_of_day:O", title="Hour"), alt.Tooltip("bookings:Q", title="Bookings", format=",.0f")]
        )
        .properties(height=300)
    )
    st.altair_chart(hist, use_container_width=True)

    # 3) Pie — Bookings by Day of Week
    sql_dow = f"""
    SELECT dow_num, dow_name, COUNT(DISTINCT booking_id) AS bookings
    FROM {database}.{table_time}
    {where_sql}
    GROUP BY dow_num, dow_name
    ORDER BY dow_num
    """
    with st.expander("SQL (Pie by DOW)"):
        st.code(sql_dow, language="sql")
    dow_df = run_athena_query(sql_dow, database, output, workgroup)
    if not dow_df.empty:
        dow_df["dow_num"] = pd.to_numeric(dow_df["dow_num"])
        dow_df["bookings"] = pd.to_numeric(dow_df["bookings"])

    st.subheader("Pie — Bookings by Day of Week")
    pie = (
        alt.Chart(dow_df)
        .mark_arc(innerRadius=60)
        .encode(
            theta=alt.Theta(field="bookings", type="quantitative"),
            color=alt.Color(field="dow_name", type="nominal", title="DOW"),
            tooltip=[alt.Tooltip("dow_name:N", title="DOW"), alt.Tooltip("bookings:Q", title="Bookings", format=",.0f")]
        )
        .properties(height=320)
    )
    st.altair_chart(pie, use_container_width=True)

    # Downloads
    with st.expander("Download data"):
        st.download_button("Heatmap (Hour × DOW) CSV", heat_df.to_csv(index=False).encode(), "bookings_hour_by_dow.csv")
        st.download_button("Histogram (Hour) CSV", hour_df.to_csv(index=False).encode(), "bookings_by_hour.csv")
        st.download_button("Pie (DOW) CSV", dow_df.to_csv(index=False).encode(), "bookings_by_dow.csv")

# ------------------------------------------------
# TAB 6 — Understand Meeting Load per Employee
# ------------------------------------------------
with tab6:
    st.markdown("### 1.6 Understand Meeting Load per Employee")
    st.caption("Average meetings/week = total meetings ÷ number of distinct weeks in range (per employee).")

    table_emp = st.text_input(
        "Athena Table (employee meetings features)",
        value="employee_meetings_features",
        key="t6_table"
    )

    # Discover min/max week_start_date & employees
    try:
        range_sql = f"SELECT MIN(week_start_date) AS min_w, MAX(week_start_date) AS max_w FROM {database}.{table_emp}"
        df_range = run_athena_query(range_sql, database, output, workgroup)
        min_w = pd.to_datetime(df_range.iloc[0]["min_w"]).date() if not df_range.empty and df_range.iloc[0]["min_w"] else None
        max_w = pd.to_datetime(df_range.iloc[0]["max_w"]).date() if not df_range.empty and df_range.iloc[0]["max_w"] else None

        emp_sql = f"SELECT DISTINCT employee_id FROM {database}.{table_emp} WHERE employee_id IS NOT NULL ORDER BY 1"
        df_emp = run_athena_query(emp_sql, database, output, workgroup)
        all_emps = sorted(df_emp["employee_id"].dropna().tolist()) if not df_emp.empty else []
    except Exception as e:
        st.error("Failed to discover week range/employees for Tab 6.")
        st.exception(e)
        st.stop()

    if not min_w or not max_w:
        st.info("No data in the employee meetings table yet.")
        st.stop()

    colA, colB = st.columns([2, 3])
    with colA:
        dr = st.date_input("Week range (use Mondays)", (pd.to_datetime(min_w), pd.to_datetime(max_w)), key="t6_dr")
    with colB:
        sel_emp = st.multiselect("Employees", options=all_emps, default=all_emps, key="t6_emps")

    # WHERE
    start_week = pd.to_datetime(dr[0]).date().isoformat()
    end_week   = pd.to_datetime(dr[1]).date().isoformat()
    where = [f"week_start_date BETWEEN DATE '{start_week}' AND DATE '{end_week}'"]
    if sel_emp and len(sel_emp) < len(all_emps):
        quoted = ("'" + e.replace("'", "''") + "'" for e in sel_emp)
        where.append(f"employee_id IN ({','.join(quoted)})")
    where_sql = "WHERE " + " AND ".join(where) if where else ""

    # Aggregate per employee & per week
    # We'll:
    # - Count meetings per employee per week
    # - Compute per-employee totals and number of distinct weeks present in the range
    # - Average = total_meetings / weeks_present
    agg_sql = f"""
    WITH weekly AS (
      SELECT employee_id,
             week_start_date,
             COUNT(DISTINCT meeting_id) AS meetings
      FROM {database}.{table_emp}
      {where_sql}
      GROUP BY employee_id, week_start_date
    )
    SELECT
      employee_id,
      SUM(meetings) AS total_meetings,
      COUNT(DISTINCT week_start_date) AS weeks_present,
      ROUND(SUM(meetings) / NULLIF(COUNT(DISTINCT week_start_date), 0), 2) AS avg_meetings_per_week
    FROM weekly
    GROUP BY employee_id
    """
    with st.expander("SQL (Employee aggregation)"):
        st.code(agg_sql, language="sql")

    agg = run_athena_query(agg_sql, database, output, workgroup)
    if agg.empty:
        st.info("No meetings in the selected range.")
        st.stop()
    for c in ("total_meetings", "weeks_present", "avg_meetings_per_week"):
        agg[c] = pd.to_numeric(agg[c])

    # KPIs
    total_meetings = int(agg["total_meetings"].sum())
    max_meetings = int(agg["total_meetings"].max())
    min_meetings = int(agg["total_meetings"].min())

    k1, k2, k3 = st.columns(3)
    k1.metric("Total Meetings (all employees)", f"{total_meetings:,}")
    k2.metric("Max Meetings (single employee)", f"{max_meetings:,}")
    k3.metric("Min Meetings (single employee)", f"{min_meetings:,}")

    # Bar: Employee vs Avg Meetings / Week
    st.subheader("Bar — Employee vs Avg Meetings / Week")
    agg_sorted = agg.sort_values("avg_meetings_per_week", ascending=False)
    bar = (
        alt.Chart(agg_sorted)
        .mark_bar()
        .encode(
            x=alt.X("employee_id:N", title="Employee",
                    axis=alt.Axis(labelAngle=0, labelLimit=1000, labelFontSize=12, titleFontSize=14),
                    sort="-y"),
            y=alt.Y("avg_meetings_per_week:Q", title="Avg Meetings / Week"),
            tooltip=[
                "employee_id:N",
                alt.Tooltip("total_meetings:Q", title="Total Meetings", format=",.0f"),
                alt.Tooltip("weeks_present:Q", title="Weeks Present", format=",.0f"),
                alt.Tooltip("avg_meetings_per_week:Q", title="Avg / Week"),
            ]
        )
        .properties(height=360)
    )
    st.altair_chart(bar, use_container_width=True)

    # Optional: weekly trend per employee
    trend_sql = f"""
    SELECT employee_id,
           week_start_date,
           COUNT(DISTINCT meeting_id) AS meetings
    FROM {database}.{table_emp}
    {where_sql}
    GROUP BY employee_id, week_start_date
    ORDER BY week_start_date, employee_id
    """
    with st.expander("SQL (Weekly trend)"):
        st.code(trend_sql, language="sql")

    trend = run_athena_query(trend_sql, database, output, workgroup)
    if not trend.empty:
        trend["week_start_date"] = pd.to_datetime(trend["week_start_date"]).dt.date
        trend["meetings"] = pd.to_numeric(trend["meetings"])

        st.subheader("Weekly Trend — Meetings per Employee")
        line = (
            alt.Chart(trend)
            .mark_line(point=True)
            .encode(
                x=alt.X("week_start_date:T", title="Week Start"),
                y=alt.Y("meetings:Q", title="Meetings"),
                color=alt.Color("employee_id:N", title="Employee"),
                tooltip=["week_start_date:T", "employee_id:N", "meetings:Q"]
            )
            .properties(height=360)
        )
        st.altair_chart(line, use_container_width=True)

    # Downloads
    with st.expander("Download data"):
        st.download_button("Employee Aggregation CSV", agg.to_csv(index=False).encode(), "employee_avg_meetings_per_week.csv")
        if not trend.empty:
            st.download_button("Weekly Trend CSV", trend.to_csv(index=False).encode(), "employee_weekly_meetings.csv")
