import datetime
import pandas as pd
import streamlit as st
from web.utils import fetch_data

def create_payload():
    start_date = st.sidebar.date_input("Start Date", value="2025-04-26")
    end_date = st.sidebar.date_input("End Date", value="2025-04-26")
    page_size = st.sidebar.number_input("Page Size", min_value=1000, step=1, value=1000)
    settlement_point_type = st.sidebar.selectbox("Settlement Point Type", ["HU", "LZ"], index=0)
    upload_data_to_s3 = st.sidebar.selectbox("Upload to S3", ['True', 'False'], index=1)
    return {
        "start_date": start_date.strftime("%Y-%m-%d"),
        "end_date": end_date.strftime("%Y-%m-%d"),
        "settlement_point_type": settlement_point_type,
        "settlement_point": "HB_HOUSTON",
        "upload_to_s3": upload_data_to_s3 if upload_data_to_s3 else "False",
        "page_size": page_size or 1000
    }


st.set_page_config(page_title="Settlement Price Point Data")
st.title("Settlement Price Point Data")
payload = create_payload()
fetch_button = st.sidebar.button("Fetch Data", key="fetch_data")
status_text = st.sidebar.empty()
if fetch_button:
    status_text.text("Fetching data...")
    spp_data_df = fetch_data('ercot/spp-data', payload)
    if (type(spp_data_df) == pd.DataFrame and spp_data_df.empty) or ('error' in spp_data_df):
        st.error("Error fetching data")
    else:
        status_text.text("Succesfully fetched data")
        st.dataframe(spp_data_df)

