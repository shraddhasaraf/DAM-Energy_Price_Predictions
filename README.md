# ERCOT Data Analytics Dashboard

This repository provides a solution for forecasting ERCOT energy data using various features such as solar, wind, and load data. The solution processes future data and merges it with forecasted features to generate predictions.

## Features

- Processes future delivery dates, hours, and intervals.
- Merges solar, wind, and load forecast data.
- Generates predictions for energy forecasting.
- Provides a web-based interface using Streamlit for visualization and CSV download

## Tech Stack

### Backend
- Python FastAPI
- Pandas for data processing
- AWS S3 for data storage
- Pydantic for data validation

## Prerequisites
- Python 3.8+
- AWS Account (for S3 storage)
- ERCOT API credentials
- pip (Python package manager)

## Installation

1. Clone the repository:
```bash
git clone [repository-url]
cd ercot-analytics
```
2. Set up the backend:
```bash
#Create and activate virtual environment
python -m venv venv

source venv/bin/activate  # On Windows: venv\Scripts\activate

#Install backend dependencies
pip install -r requirements.txt
```
3. Copy .env-sample and rename it to .env file in the root directory. Fill in the required environment variables.

## Running the Application
1. Start the backend server:
```bash
python -m uvicorn api.main:app --reload
```

2. Access the api application at http://localhost:8000/docs

### Running the Streamlit Web App
The solution includes a Streamlit-based web app for visualization and CSV downloads.

To run the app:
```bash
streamlit run app.py
```

Access the app in your browser at http://localhost:8501

3. API Integration
The solution fetches data from the ERCOT API. Ensure the API is accessible and update the API URL and parameters in the code if necessary.

## Running the Forecasting Script
To generate forecasts programmatically, run the main script:
```bash
python model_service.py
```

