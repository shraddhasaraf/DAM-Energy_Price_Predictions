# ERCOT Data Analytics Dashboard

A web application for fetching and analyzing ERCOT (Electric Reliability Council of Texas) power market data.

## Features

- Real-time data fetching from ERCOT Public API
- Data export to AWS S3
- Date range selection
- Automated data processing

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

