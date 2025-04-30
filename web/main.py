import pandas as pd
import numpy as np
import streamlit as st
import requests
import matplotlib.pyplot as plt


def draw_timeseries_graph(df, delivery_date='2025-04-26', settlement_point='HB_HOUSTON'):
    # Filter data for a specific deliveryDate and settlementPoint
    filtered_data = df[(df['deliveryDate'] == delivery_date) & (df['settlementPoint'] == settlement_point)]

    # Create a time index for plotting
    filtered_data['time'] = filtered_data['deliveryHour'] + (filtered_data['deliveryInterval'] - 1) * 0.25

    # Sort by time for proper plotting
    filtered_data = filtered_data.sort_values(by='time')

    # Determine y-axis bins
    min_price = filtered_data['settlementPointPrice'].min()
    max_price = filtered_data['settlementPointPrice'].max()
    y_ticks = np.linspace(min_price, max_price, num=10)  # Divide into 10 equal bins

    # Plot the timeseries
    fig, ax = plt.subplots(figsize=(12, 6))
    ax.plot(filtered_data['time'], filtered_data['settlementPointPrice'], marker='o', label=f'{settlement_point} on {delivery_date}')
    ax.set_title(f'Settlement Point Price Timeseries for {settlement_point} on {delivery_date}')
    ax.set_xlabel('Time (Hours)')
    ax.set_ylabel('Settlement Point Price')
    ax.set_xlim(0, 24)  # Set x-axis limits from 0 to 24 hours
    ax.set_yticks(y_ticks)  # Set y-axis ticks
    ax.grid(True)
    ax.legend()
    st.pyplot(fig)

# Title of the app
st.title("Data Fetcher")

# Sidebar input
st.sidebar.header("API Input")
start_date = st.sidebar.date_input("Start Date", value="2025-04-26")
end_date = st.sidebar.date_input("End Date", value="2025-04-26")
page_size = st.sidebar.number_input("Page Size", min_value=1000, step=1, value=1000)
settlement_point_type = st.sidebar.selectbox("Settlement Point Type", ["HU", "LZ"], index=0)
upload_data_to_s3 = st.sidebar.selectbox("Upload to S3", [True, False], index=1)

# Fetch data from backend
if st.sidebar.button("Fetch Data"):
    api_url = "http://localhost:8000/ercot/spp-data"
    payload = {
        "start_date": start_date.strftime("%Y-%m-%d") if start_date else "2025-04-26",
        "end_date": end_date.strftime("%Y-%m-%d") if end_date else "2025-04-27",
        "settlement_point_type": settlement_point_type or "HU",
        "settlement_point": "HB_HOUSTON",
        "upload_to_s3": upload_data_to_s3 | False,
        "page_size": page_size or 1000
    }
    response = requests.post(api_url, json=payload)
    if response.status_code == 200:
        st.write("Data fetched successfully!")
        data = response.json()['data']
        data_df = pd.DataFrame(data)
        st.dataframe(data_df)
        data_df = data_df[data_df['settlementPoint'] == 'HB_HOUSTON']
        data_df['settlementPointPrice'] = pd.to_numeric(data_df['settlementPointPrice'], errors='coerce').fillna(0)
        data_df['deliveryHour'] = pd.to_numeric(data_df['deliveryHour'], errors='coerce').fillna(0)
        data_df['deliveryInterval'] = pd.to_numeric(data_df['deliveryInterval'], errors='coerce').fillna(0)
        # draw_timeseries_graph(data_df)
    else:
        st.error(f"Error: {response.status_code} - {response.text}")