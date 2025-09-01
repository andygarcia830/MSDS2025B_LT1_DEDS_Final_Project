import streamlit as st
import pandas as pd
import helper
from helper import fetch_latest_interests, to_ph_time


# --- page settings ---
st.set_page_config(page_title="Oncolens Dashboard", page_icon="static/proj_logo.png",
                   layout='wide', initial_sidebar_state='expanded', menu_items=None)

with open('style.css') as f:
    st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)


title1 = 'Subreddit Metrics'


SUBS = ['LivingWithMBC', 'breastcancer', 'ovariancancer_new', 'BRCA', 'cancer', 'IndustrialPharmacy']

# --- sidebar --- #
select_metric = ['active_users', 'subscribers']
st.sidebar.selectbox("Select Subreddit Metric:", select_metric, key='select_metric')


# --- Main Window ---#    

@st.cache_data(ttl=300)
def load_latest(cache_key_hour: str, metric_key: str, prev_mode: str):
    return fetch_latest_interests(SUBS, metric_key=metric_key, prev_mode=prev_mode)

current_hour_key = helper.last_complete_hour_utc().strftime("%Y-%m-%dT%H:%MZ")
df = load_latest(current_hour_key, metric_key=st.session_state.select_metric, prev_mode="prev_day_same_hour")

def metric_card(col, sub):
    row = df.loc[df["sub"] == sub].iloc[0] if (df["sub"] == sub).any() else None
    if row is None or pd.isna(row["timestamp"]):
        col.metric(sub, value="—", delta=None, help="No data in last 48h (PH)")
        return
    val = row["value"]; delta = row["delta"]
    value = "—" if val is None else f"{val:,}"
    delta_str = None if delta is None else f"{delta:+,}"
    asof_ph = to_ph_time(row["timestamp"])
    col.metric(sub, value=value, delta=delta_str, help=f"As of {asof_ph} PH (UTC+8)")

# section 1
st.subheader(f'{title1}', divider='gray')
col1, col2, col3 = st.columns(3)
for col, sub in zip((col1, col2, col3), SUBS[:3]):
    metric_card(col, sub)
c1, c2, c3 = st.columns(3)
for col, sub in zip((c1, c2, c3), SUBS[3:6]):
    metric_card(col, sub)


st.dataframe(df)