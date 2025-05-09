import requests
from logger_config import logger
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

API_URL = "http://localhost:8000/"


def fetch_data(product: str, payload):
    try:
        response = requests.post('/'.join([API_URL, product]), json=payload)
        if not response.status_code == 200:
            raise
        data = response.json()['data']
        return pd.DataFrame(data)
    except Exception as e:
        logger.error(str(e))
        return {"error": "Unable to fetch data"}


def draw_timeseries_graph(df: pd.DataFrame, delivery_date: str, settlement_point: str):
    logger.info("Drawing timeseries graph")
    filtered_data = df[df['settlementPoint'].isin(settlement_point) & (df['deliveryDate'] == delivery_date)]

    # Create a time index for plotting
    filtered_data['time'] = filtered_data['deliveryHour'] + (filtered_data['deliveryInterval'] - 1) * 0.25

    # Sort by time for proper plotting
    filtered_data = filtered_data.sort_values(by='time')

    # Determine y-axis bins
    min_price = filtered_data['settlementPointPrice'].min()
    max_price = filtered_data['settlementPointPrice'].max()
    y_ticks = np.linspace(min_price, max_price, num=10)  # Divide into 10 equal bins

    x_ticks = np.arange(0, 25, 4)

    # Plot the timeseries
    fig, ax = plt.subplots(figsize=(12, 6))
    ax.plot(filtered_data['time'], filtered_data['settlementPointPrice'], marker='o',
            label=f'{settlement_point} on {delivery_date}', linewidth=3)
    ax.set_title(f'Settlement Point Price Timeseries for {settlement_point[0]} on {delivery_date}')
    ax.set_xlabel('Time (Hours)')
    ax.set_ylabel('Settlement Point Price')
    ax.set_xlim(0, 24)  # Set x-axis limits from 0 to 24 hours
    ax.set_xticks(x_ticks)  # Set x-axis ticks
    ax.set_yticks(y_ticks)  # Set y-axis ticks
    ax.grid(True)
    ax.legend()

    return fig
