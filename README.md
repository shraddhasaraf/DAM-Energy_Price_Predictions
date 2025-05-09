# Energy Price Prediction Dashboard for ERCOT

This dashboard helps predict energy prices in Texas by analyzing data from ERCOT (Electric Reliability Council of Texas). It considers various factors like solar power, wind power, and electricity demand to make these predictions.

## What Can This Tool Do?

- Predicts future energy prices
- Shows forecasts for solar and wind power generation
- Displays expected electricity demand
- Provides an easy-to-use website interface to view and download data

## What You'll Need

- A computer with Python installed (version 3.8 or newer)
- An AWS account for data storage
- ERCOT access credentials
- Basic familiarity with running commands in a terminal

## Getting Started

1. Get the code:
```bash
git clone [repository-url]
cd energy-price-predictions
```

2. Set up your workspace:
- Open a terminal window
- Run these commands one at a time:
```bash
python -m venv venv

# If you're using Mac or Linux:
source venv/bin/activate

# If you're using Windows:
venv\Scripts\activate

# Install required software:
pip install -r requirements.txt
```

3. Create your settings file:
- Find the file named `.env-sample`
- Make a copy of it and name the copy `.env`
- Open the `.env` file and fill in your personal access details

## Using the Dashboard

1. Start the system:
```bash
python -m uvicorn api.main:app --reload
```

2. View the data:
- Open your web browser
- Go to http://localhost:8000/docs

### Using the Visual Dashboard

We've created an easy-to-use website to view the predictions:

1. Start the website:
```bash
streamlit run app.py
```

2. View the dashboard:
- Open your web browser
- Go to http://localhost:8501
- You can now view predictions and download data as CSV files
