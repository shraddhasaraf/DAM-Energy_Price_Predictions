import os
import jwt
import time
import datetime
import requests
import pandas as pd
import numpy as np
from logger_config import logger
from typing import Dict
from fastapi import HTTPException
from api.utils import get_field_names


async def get_ercot_access_token() -> str:
    """
    Fetches a new access token from the ERCOT Public API.

    This function retrieves the access token by making a POST request to the ERCOT
    authentication endpoint using the username and password stored in environment variables.

    Returns:
        str: The access token retrieved from the ERCOT API.

    Raises:
        Exception: If the username or password is not set in the environment variables.
    """

    # Retrieve the access token URL, username, and password from environment variables
    access_token_url = os.getenv("ERCOT_ACCESS_TOKEN_URL")
    username = os.getenv("ERCOT_USERNAME")
    password = os.getenv("ERCOT_PASSWORD")

    # Check if username or password is missing
    if not username or not password:
        logger.error("ERCOT username or password not set in environment variables.")
        raise Exception("ERCOT username or password not set in environment variables.")

    # Construct the authorization URL for signing into the ERCOT Public API account
    auth_url = "{url}?username={username}&password={password}\
    &grant_type=password&scope=openid+fec253ea-0d06-4272-a5e6-b478baeecd70+offline_access\
    &client_id=fec253ea-0d06-4272-a5e6-b478baeecd70\
    &response_type=id_token"

    # Sign in and authenticate by making a POST request to the authorization URL
    auth_response = requests.post(auth_url.format(url=access_token_url, username=username, password=password))

    # Sign in and authenticate by making a POST request to the authorization URL
    access_token = auth_response.json().get("access_token")
    return access_token


async def get_valid_access_token(access_token=None) -> str:
    """
    Ensures the provided access token is valid and not expired. If the token is expired or invalid,
    it fetches a new token using the `get_ercot_access_token` function.

    Args:
        access_token (str, optional): The current access token to validate. Defaults to None.

    Returns:
        str: A valid access token, either the provided one (if valid) or a newly fetched one.
    """
    try:
        # Decode the token without verifying the signature to extract its payload
        payload = jwt.decode(access_token, options={"verify_signature": False})

        # Check if the token has expired by comparing the "exp" field with the current time
        if payload["exp"] < time.time():
            # Fetch a new token if the current one is expired
            return await get_ercot_access_token()
    except (jwt.ExpiredSignatureError, jwt.DecodeError):
        # If the token is invalid or expired, fetch a new token
        return await get_ercot_access_token()
    return access_token


async def make_ercot_api_request(product, params, access_token, max_retries=3, timeout=120) -> Dict:
    """
    Makes a request to the ERCOT Public API for the specified product, handling rate limits.
    """
    subscription_key = os.getenv("ERCOT_SUBSCRIPTION_KEY")
    public_url = os.getenv("ERCOT_PUBLIC_API_URL")
    url = '/'.join((public_url, product))

    # Ensure the access token is valid
    access_token = await get_valid_access_token(access_token)

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Ocp-Apim-Subscription-Key": subscription_key
    }

    for attempt in range(1, max_retries + 1):
        try:
            # Log the attempt
            logger.info(f"Attempt {attempt} to fetch data from {url} with params {params}")

            # Send the GET request
            response = requests.get(url, headers=headers, params=params, timeout=timeout)

            # Raise an exception for HTTP errors
            response.raise_for_status()

            # Return the JSON response if successful
            return response.json()

        except requests.exceptions.HTTPError as e:
            if response.status_code == 429:  # Rate limit exceeded
                logger.warning(f"Rate limit exceeded on attempt {attempt}. Retrying after delay...")
                time.sleep(15)  # Wait longer before retrying
            else:
                logger.error(f"HTTP error on attempt {attempt} for {url}: {e}")
                if attempt == max_retries:
                    raise Exception(f"Failed to fetch data from {url} after {max_retries} attempts.") from e

        except requests.exceptions.Timeout:
            logger.warning(f"Timeout occurred on attempt {attempt} for {url}. Retrying...")

        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed on attempt {attempt} for {url}: {e}")
            if attempt == max_retries:
                raise Exception(f"Failed to fetch data from {url} after {max_retries} attempts.") from e

        # Wait 2 seconds between requests to respect the rate limit
        time.sleep(2)

    # If all retries fail, raise an exception
    raise Exception(f"Failed to fetch data from {url} after {max_retries} attempts.")


async def fetch_ercot_data(product, params) -> pd.DataFrame:
    """
    Fetches and processes data from the ERCOT Public API for a specific product.

    This function handles pagination, processes the data, and combines it into a single DataFrame.

    Args:
        product (str): The specific product or endpoint to request from the ERCOT API.
        params (dict): Query parameters to include in the API request.

    Returns:
        pd.DataFrame: A DataFrame containing the processed data.

    Raises:
        HTTPException: If data fetching or processing fails.
    """
    page = 1
    total_pages = 1
    access_token = await get_ercot_access_token()
    final_data = pd.DataFrame()

    while page <= total_pages:
        params["page"] = page
        try:
            logger.info(f"Fetching page {page} of data")
            response = await make_ercot_api_request(product, params, access_token)

            if not response or "fields" not in response or "data" not in response or "_meta" not in response:
                logger.error(f"Invalid response structure for page {page}: {response}")
                raise ValueError("Invalid response structure from ERCOT API")

            total_pages = response["_meta"].get("totalPages", 1)
            field_names = await get_field_names(response["fields"])
            df = pd.DataFrame(np.array(response["data"]), columns=field_names)
            final_data = pd.concat([final_data, df], ignore_index=True)

        except Exception as e:
            logger.error(f"Error fetching or processing data on page {page}: {e}")
            if page > 1:
                break  # Stop further processing if an error occurs after the first page
            raise HTTPException(status_code=500, detail=f"Failed to fetch data: {str(e)}")

        page += 1
    return final_data


async def process_spp_data(spp_data_df: pd.DataFrame) -> pd.DataFrame:
    """
    Processes Settlement Point Price (SPP) data into a sorted Pandas DataFrame.

    This function takes a DataFrame containing SPP data, converts specific columns to appropriate
    data types, and sorts the data by delivery date, hour, and interval.

    Args:
        spp_data_df (pd.DataFrame): The raw SPP data to process.

    Returns:
        pd.DataFrame: A processed and sorted DataFrame. If the input DataFrame is empty, it is returned as is.
    """
    if spp_data_df.empty:
        return spp_data_df
    spp_data_df['deliveryDate'] = pd.to_datetime(spp_data_df['deliveryDate'])
    spp_data_df['deliveryHour'] = pd.to_numeric(spp_data_df['deliveryHour'])
    spp_data_df['deliveryInterval'] = pd.to_numeric(spp_data_df['deliveryInterval'])
    spp_data_df.drop(columns=['DSTFlag'])
    spp_data_df = spp_data_df.sort_values(by=['deliveryDate', 'deliveryHour', 'deliveryInterval'])
    return spp_data_df


async def process_solar_data(solar_data_df: pd.DataFrame) -> pd.DataFrame:
    """
    Processes solar power production data into a sorted Pandas DataFrame.

    This function takes a DataFrame containing raw solar data, extracts and transforms
    specific columns to appropriate data types, removes duplicates, and sorts the data
    by delivery date, hour, and interval.

    Args:
        solar_data_df (pd.DataFrame): The raw solar data to process.

    Returns:
        pd.DataFrame: A processed and sorted DataFrame. If the input DataFrame is empty, it is returned as is.
    """
    if solar_data_df.empty:
        return solar_data_df
    solar_data_df["intervalEnding"] = pd.to_datetime(solar_data_df["intervalEnding"])
    solar_data_df["deliveryDate"] = solar_data_df["intervalEnding"].dt.date
    solar_data_df["deliveryHour"] = solar_data_df["intervalEnding"].dt.hour + 1
    solar_data_df["deliveryInterval"] = (solar_data_df["intervalEnding"].dt.minute // 15) + 1
    solar_data_df = solar_data_df.sort_values(by=['intervalEnding'])
    solar_data_df = solar_data_df.drop_duplicates(subset=['intervalEnding'])
    solar_data_df = solar_data_df.drop(columns=["postedDatetime", "HSLSystemWide", "DSTFlag"])

    return solar_data_df


async def process_wind_data(wind_data_df: pd.DataFrame) -> pd.DataFrame:
    """
    Processes wind power production data into a sorted Pandas DataFrame.

    This function takes a DataFrame containing raw wind data, extracts and transforms
    specific columns to appropriate data types, removes duplicates, and sorts the data
    by delivery date, hour, and interval.

    Args:
        wind_data_df (pd.DataFrame): The raw wind data to process.

    Returns:
        pd.DataFrame: A processed and sorted DataFrame. If the input DataFrame is empty, it is returned as is.
    """
    if wind_data_df.empty:
        return wind_data_df

    wind_data_df["intervalEnding"] = pd.to_datetime(wind_data_df["intervalEnding"])
    wind_data_df["deliveryDate"] = wind_data_df["intervalEnding"].dt.date
    wind_data_df["deliveryHour"] = wind_data_df["intervalEnding"].dt.hour + 1
    wind_data_df["deliveryInterval"] = (wind_data_df["intervalEnding"].dt.minute // 15) + 1
    wind_data_df = wind_data_df.sort_values(by=['intervalEnding'])
    wind_data_df = wind_data_df.drop_duplicates(subset=['intervalEnding'])
    wind_data_df = wind_data_df.drop(columns=["postedDatetime", "HSLSystemWide", "DSTFlag"])

    return wind_data_df


async def process_load_data(data):
    """
    Processes load data into a Pandas DataFrame.

    Args:
        data (list): The raw load data to process.

    Returns:
        pd.DataFrame: A DataFrame containing the processed load data.
    """
    columns = ["date", "hour", "load_north", "load_south", "load_west", "load_houston", "load_total", "dst_flag"]
    if not data:
        print("No Load data")
        return pd.DataFrame(columns=columns)
    df = pd.DataFrame(np.array(data), columns=columns)
    df["date"] = df["date"].apply(lambda x: datetime.datetime.strptime(x, "%Y-%m-%d"))
    df['hour'] = df['hour'].str.split(':').str[0].astype(int) - 1
    df = df.drop(columns=["dst_flag"])
    return df
