"""
Settlement Price Point Predictions Dashboard

This module provides a Streamlit web interface for viewing and analyzing
settlement price point predictions from the ERCOT model.
"""

import logging
from datetime import date, timedelta
import pandas as pd
import streamlit as st
from web.utils import fetch_data, draw_timeseries_graph
from models.ercot_models import SettlementPointName

# Configure logging
logger = logging.getLogger(__name__)

# Constants
MAX_PREDICTION_DAYS = 7
DEFAULT_SETTLEMENT_POINT = 'HB_HOUSTON'
API_ENDPOINT = 'model/predictions'


@st.cache_data(ttl=3600)  # Cache data for 1 hour
def get_prediction_data(prediction_date, settlement_point_names):
    """
    Fetch prediction data from the API with caching.
    
    Args:
        prediction_date (str): Date for which to fetch predictions.
        settlement_point_names (list): List of settlement point names.
        
    Returns:
        pandas.DataFrame: DataFrame containing prediction data or error dict.
    """
    payload = {
        "prediction_date": prediction_date,
        "settlement_point_name": settlement_point_names,
    }
    
    try:
        return fetch_data(API_ENDPOINT, payload)
    except Exception as e:
        logger.error(f"Error fetching prediction data: {str(e)}")
        return {"error": f"Failed to fetch data: {str(e)}"}


def create_payload():
    """
    Create payload for API request based on user inputs.
    
    Returns:
        dict: Payload with prediction date and settlement point names.
    """
    prediction_date = st.sidebar.date_input(
        "Start Date", 
        value=date.today(), 
        min_value=date.today(),
        max_value=date.today() + timedelta(days=MAX_PREDICTION_DAYS)
    )
    
    settlement_point_names = st.sidebar.multiselect(
        "Settlement Point Type",
        list(SettlementPointName.__members__.keys()), 
        default=DEFAULT_SETTLEMENT_POINT
    )
    
    if not settlement_point_names:
        st.sidebar.warning("Please select at least one settlement point.")
        settlement_point_names = [DEFAULT_SETTLEMENT_POINT]
    
    return {
        "prediction_date": prediction_date.strftime("%Y-%m-%d"),
        "settlement_point_name": settlement_point_names,
    }


def display_graphs(data_df, prediction_date, settlement_point_names):
    """
    Generate and display time series graphs for each settlement point.
    
    Args:
        data_df (pandas.DataFrame): DataFrame containing prediction data.
        prediction_date (str): Start date for predictions.
        settlement_point_names (list): List of settlement point names.
    """
    st.subheader("Prediction Graphs")
    
    # Create tabs for each settlement point
    if len(settlement_point_names) > 1:
        tabs = st.tabs(settlement_point_names)
        for i, spp_name in enumerate(settlement_point_names):
            with tabs[i]:
                fig = draw_timeseries_graph(data_df, prediction_date, [spp_name])
                st.pyplot(fig)
    else:
        fig = draw_timeseries_graph(data_df, prediction_date, settlement_point_names)
        st.pyplot(fig)


def main():
    """Main function to run the Streamlit application."""
    # Page configuration
    st.set_page_config(
        page_title="Settlement Price Point Predictions",
        page_icon="📊",
        layout="wide"
    )
    
    # Header
    st.title("Settlement Price Point Predictions")
    st.markdown("View and analyze settlement price predictions for ERCOT settlement points.")
    
    # Sidebar inputs
    st.sidebar.header("Prediction Parameters")
    payload = create_payload()
    
    # Add fetch button and status indicator
    col1, col2 = st.sidebar.columns([3, 1])
    with col1:
        fetch_button = st.button("Fetch Data", key="fetch_data", use_container_width=True)
    
    status_text = st.sidebar.empty()
    
    # Main content
    if fetch_button or 'spp_pred_data_df' in st.session_state:
        try:
            if fetch_button:
                status_text.info("Fetching data...")
                spp_pred_data_df = get_prediction_data(
                    payload['prediction_date'],
                    payload['settlement_point_name']
                )
                st.session_state.spp_pred_data_df = spp_pred_data_df
            else:
                spp_pred_data_df = st.session_state.spp_pred_data_df
            
            if isinstance(spp_pred_data_df, dict) and 'error' in spp_pred_data_df:
                st.error(f"Error: {spp_pred_data_df['error']}")
                status_text.error("Failed to fetch data")
            elif isinstance(spp_pred_data_df, pd.DataFrame) and spp_pred_data_df.empty:
                st.warning("No prediction data available for the selected parameters")
                status_text.warning("No data available")
            else:
                status_text.success("Successfully fetched data")
                
                # Display data table with download option
                st.subheader("Prediction Data")
                st.dataframe(spp_pred_data_df, use_container_width=True)
                
                # Add download button
                csv = spp_pred_data_df.to_csv(index=False)
                st.download_button(
                    label="Download CSV",
                    data=csv,
                    file_name=f"price_predictions_{payload['prediction_date']}.csv",
                    mime="text/csv",
                )
                
                # Display graphs
                display_graphs(
                    spp_pred_data_df,
                    payload['prediction_date'],
                    payload['settlement_point_name']
                )
                
        except Exception as e:
            logger.exception("Unexpected error in prediction dashboard")
            st.error(f"An unexpected error occurred: {str(e)}")
            status_text.error("Error")
    else:
        st.info("Use the sidebar to set parameters and click 'Fetch Data' to view predictions.")


if __name__ == "__main__":
    main()
