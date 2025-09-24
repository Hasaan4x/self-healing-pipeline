import os, pandas as pd, streamlit as st
from sqlalchemy import create_engine

DB_URL = os.getenv("DB_URL", "postgresql+psycopg2://hasaan:dev@localhost:5432/healing")
engine = create_engine(DB_URL, future=True)

st.set_page_config(page_title="Self-Healing Pipeline", layout="wide")
st.title("Self-Healing Pipeline — Health Dashboard")

# KPIs
c1, c2, c3, c4 = st.columns(4)
raw  = pd.read_sql("select count(*) as c from raw_orders", engine)["c"][0]
clean= pd.read_sql("select count(*) as c from orders_clean", engine)["c"][0]
quar = pd.read_sql("select count(*) as c from orders_quarantine", engine)["c"][0]
wm   = pd.read_sql("select last_updated_iso from pipeline_state where id=1", engine)["last_updated_iso"].iloc[0]
c1.metric("Raw rows", raw)
c2.metric("Clean rows", clean)
c3.metric("Quarantined rows", quar)
c4.metric("Watermark (last processed hour)", wm if wm else "—")

# Recent audit events
st.subheader("Recent audit events")
audit = pd.read_sql("""select stage, event, details, created_at
                       from audit_events order by created_at desc limit 50""", engine)
st.dataframe(audit, use_container_width=True)

# Temperature by city (last 48h)
st.subheader("Temperature (last 48h)")
temps = pd.read_sql("""
    select customer_id as city, last_updated as ts, amount as temperature_c
    from orders_clean
    where last_updated > now() - interval '48 hours'
    order by ts
""", engine, parse_dates=["ts"])
if temps.empty:
    st.info("No recent data yet.")
else:
    for city, dfc in temps.groupby("city"):
        st.line_chart(dfc.set_index("ts")["temperature_c"], height=200)

# Quarantine reasons breakdown (last 7 days)
st.subheader("Quarantine reasons (7d)")
q = pd.read_sql("""
    select reason, count(*) as cnt
    from orders_quarantine
    where quarantined_at > now() - interval '7 days'
    group by reason order by cnt desc
""", engine)
if not q.empty:
    st.bar_chart(q.set_index("reason")["cnt"])
else:
    st.write("No quarantined rows in the last 7 days.")
