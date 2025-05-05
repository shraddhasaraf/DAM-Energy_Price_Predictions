import os
import boto3
import datetime
import pandas as pd
from io import BytesIO
from logger_config import logger
from fastapi import HTTPException
from typing import List
from botocore.exceptions import BotoCoreError, ClientError


async def get_s3_client():
    """
    Asynchronously initializes and returns an Amazon S3 client.

    This function creates an S3 client using the `boto3` library and sets the region
    based on the environment variable `AWS_REGION`. The client can be used to interact
    with Amazon S3 services, such as uploading files, listing buckets, or retrieving objects.

    Returns:
        boto3.client: An initialized S3 client object.

    Raises:
        ValueError: If the `AWS_REGION` environment variable is not set.
        Exception: For any unexpected errors during client initialization.
    """
    region = os.environ.get("AWS_REGION")
    if not region:
        raise ValueError("AWS_REGION environment variable is not set.")

    try:
        s3_client = boto3.client("s3", region_name=region)
        return s3_client
    except (BotoCoreError, ClientError) as e:
        logger.error(f"Error initializing S3 client: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error while initializing S3 client: {e}")
        raise


async def get_field_names(field_data) -> List:
    """
    Extracts and returns a list of field names from the provided field data.

    Args:
        field_data (list): A list of dictionaries containing field metadata.

    Returns:
        List: A list of field names extracted from the field data.
    """
    field_names = []
    for field in field_data:
        field_names.append(field["name"])
    return field_names


async def get_file_from_s3(file_path: str, s3_client=None) -> bytes:
    """
    Retrieves a file from an Amazon S3 bucket.

    Args:
        file_path (str): The path to the file in the S3 bucket.

    Returns:
        bytes: The file content as bytes.

    Raises:
        ValueError: If the S3 bucket name is not set in the environment variables.
        FileNotFoundError: If the file does not exist in the S3 bucket.
        Exception: For any other unexpected errors.
    """
    bucket_name = os.environ.get("S3_BUCKET_NAME")
    if not bucket_name:
        raise ValueError("S3_BUCKET_NAME environment variable is not set.")

    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=file_path)
        return response["Body"].read()
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            raise FileNotFoundError(f"The file '{file_path}' does not exist in S3.")
        logger.error(f"ClientError while accessing S3: {e}")
        raise
    except BotoCoreError as e:
        logger.error(f"BotoCoreError while accessing S3: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise


async def get_dataframe_from_s3(file_path: str, s3_client=None) -> pd.DataFrame:
    """
    Retrieves a CSV file from an S3 bucket and converts it to a DataFrame.

    Args:
        file_path (str): The path to the file in the S3 bucket.
        s3_client: The S3 client dependency for interacting with Amazon S3.

    Returns:
        pd.DataFrame: The DataFrame containing the data from the CSV file.

    Raises:
        ValueError: If the S3 bucket name is not set in the environment variables.
        FileNotFoundError: If the file does not exist in the S3 bucket.
        Exception: For any other unexpected errors.
    """
    try:
        response = await get_file_from_s3(file_path, s3_client)
        return pd.read_csv(BytesIO(response))
    except FileNotFoundError as e:
        logger.error(f"File not found in S3: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error while reading CSV from S3: {e}")
        raise


async def update_data_in_s3(file_name: str, latest_data_df: pd.DataFrame, max_date: str, s3_client=None):
    """
    Updates data in an S3 bucket by merging new data with existing data, keeping only the last 90 days.

    Args:
        file_name (str): The name of the file in the S3 bucket.
        latest_data_df (pd.DataFrame): The latest data to be merged.
        max_date (str): The maximum date for filtering data (in "YYYY-MM-DD" format).
        s3_client: The S3 client dependency for interacting with Amazon S3.

    Returns:
        None
    """
    logger.info(f"Updating data in S3 for file: {file_name}")

    # Validate inputs
    if latest_data_df.empty:
        logger.warning("Latest data is empty. Skipping update.")
        return

    try:
        # Fetch existing data from S3
        logger.info(f"Fetching existing data from S3 for file: {file_name}")
        previous_data = await get_file_from_s3(file_name, s3_client)
        previous_data_df = pd.read_csv(BytesIO(previous_data))

        # Merge and filter data
        logger.info("Merging new data with existing data.")
        combined_df = pd.concat([previous_data_df, latest_data_df], ignore_index=True)

        # Ensure deliveryDate is in datetime format
        combined_df["deliveryDate"] = pd.to_datetime(combined_df["deliveryDate"], format="%Y-%m-%d")

        max_date = datetime.datetime.strptime(max_date, "%Y-%m-%d")
        min_date = max_date - datetime.timedelta(days=90)

        filtered_df = combined_df[(combined_df["deliveryDate"] >= min_date) &
                                  (combined_df["deliveryDate"] <= max_date)]

        # Upload updated data to S3
        logger.info(f"Uploading updated data to S3 for file: {file_name}")
        await upload_data_to_s3(filtered_df, file_name, s3_client)
        logger.info(f"Data successfully updated in S3 for file: {file_name} with date range {min_date} to {max_date}")

    except FileNotFoundError:
        logger.warning(f"File {file_name} not found in S3. Uploading new data.")
        await upload_data_to_s3(latest_data_df, file_name, s3_client)
    except (ClientError, BotoCoreError) as e:
        logger.error(f"S3 error while updating data: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error while updating data in S3: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An unexpected error occurred while updating data in S3.")


async def upload_data_to_s3(df: pd.DataFrame, file_path: str, s3_client=None):
    """
    Uploads a DataFrame to an S3 bucket as a CSV file.

    Args:
        df (pd.DataFrame): The DataFrame to upload.
        file_path (str): The path (key) for the file in the S3 bucket.
        s3_client: The S3 client dependency for interacting with Amazon S3.

    Returns:
        str: The public URL of the uploaded file.

    Raises:
        ValueError: If required environment variables are not set or the DataFrame is empty.
        HTTPException: For any unexpected errors during the upload process.
    """
    bucket_name = os.environ.get("S3_BUCKET_NAME")
    if not bucket_name:
        raise ValueError("S3_BUCKET_NAME environment variable is not set.")

    if df.empty:
        logger.warning("DataFrame is empty. Skipping upload.")
        raise ValueError("Cannot upload an empty DataFrame to S3.")

    try:
        # Save DataFrame to in-memory CSV
        csv_buffer = BytesIO()
        df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)

        # Upload to S3
        logger.info(f"Uploading file to S3 bucket '{bucket_name}' with key '{file_path}'")
        s3_client.upload_fileobj(csv_buffer, bucket_name, file_path)

        # Generate public URL
        public_url = f"https://{bucket_name}.s3.amazonaws.com/{file_path}"
        logger.info(f"File successfully uploaded to S3: {public_url}")
        return public_url

    except (ClientError, BotoCoreError) as e:
        logger.error(f"S3 error during upload: {e}")
        raise HTTPException(status_code=500, detail="Failed to upload file to S3.")
    except Exception as e:
        logger.error(f"Unexpected error during S3 upload: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An unexpected error occurred while uploading to S3.")
