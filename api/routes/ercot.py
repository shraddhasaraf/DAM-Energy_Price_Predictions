import pandas as pd
from fastapi import APIRouter, HTTPException, Depends, Response
from datetime import date, timedelta
from typing import Dict, Any, Callable, Optional, TypeVar, Awaitable, Union

from logger_config import logger
from api.ercot_service import (
    get_ercot_access_token,
    process_spp_data,
    process_solar_data,
    process_wind_data,
    process_any_data,
    fetch_ercot_data,
)
from models.ercot_models import (
    SppRequestBody,
    SolarRequestBody,
    WindRequestBody,
    LoadRequestBody,
    ForecastRequestBody,
    ErcotProductRoute,
    S3FileNameEnum,
)
from api.utils import get_s3_client, update_data_in_s3, upload_data_to_s3

router = APIRouter()

# Type definition for data processing functions
T = TypeVar('T', bound=pd.DataFrame)
ProcessFunction = Callable[[pd.DataFrame], Awaitable[T]]


async def handle_ercot_request(
    product: str,
    params: Dict[str, Any],
    process_func: ProcessFunction,
    data_type: str,
    file_name: str,
    upload_to_s3: bool = False,
    s3_client: Any = None
) -> Dict[str, Any]:
    """
    Generic function to handle ERCOT API requests, data processing, and S3 uploads.

    Args:
        product: The ERCOT API product route
        params: Query parameters for the API request
        process_func: Function to process the raw data
        data_type: Type of data being processed (for logging/error messages)
        file_name: Name of the file for S3 upload
        upload_to_s3: Whether to upload the processed data to S3
        s3_client: S3 client for AWS interactions

    Returns:
        Dict containing the processed data
    """
    try:
        # Fetch data from ERCOT API
        data_df = await fetch_ercot_data(product, params)
        if data_df.empty:
            logger.warning(f"No {data_type} data returned from the API.")
            raise HTTPException(
                status_code=404, 
                detail=f"No {data_type} data found for the given parameters."
            )

        logger.info(f"{data_type} data fetched successfully")
        final_data_df = await process_func(data_df)

        # Upload to S3 if required
        if upload_to_s3 and s3_client:
            await upload_data_to_s3(final_data_df, file_name, s3_client)
            logger.info(f"{data_type} data uploaded to S3: {file_name}")

        records = final_data_df.to_dict(orient='records')
        return {"data": records}
    
    except HTTPException:
        # Re-raise HTTPException to preserve status code and details
        raise
    except Exception as e:
        logger.error(f"Unexpected error processing {data_type} data: {e}", exc_info=True)
        raise HTTPException(
            status_code=500, 
            detail=f"An unexpected error occurred while processing {data_type} data."
        )


@router.get("/ercot-access-token")
async def get_access_token():
    """Get an ERCOT API access token"""
    try:
        token = await get_ercot_access_token()
        return {"access_token": token}
    except Exception as e:
        logger.error(f"Failed to get ERCOT access token: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="Failed to retrieve ERCOT access token"
        )


@router.post("/spp-data")
async def get_spp_data(body: SppRequestBody, s3_client=Depends(get_s3_client)):
    """
    Fetches and processes Settlement Point Price (SPP) data from the ERCOT API.

    Args:
        body (SppRequestBody): The request body contains the following fields:
            - start_date (str): The start date for the data retrieval (YYYY-MM-DD).
            - end_date (str): The end date for the data retrieval (YYYY-MM-DD).
            - settlement_point_type (str): The type of settlement point (e.g., "LZ", "HU").
            - page_size (int): The number of records per page.
            - upload_to_s3 (bool): Whether to upload the processed data to S3.
        s3_client: The S3 client dependency for interacting with Amazon S3.

    Returns:
        dict: A JSON response containing the processed SPP data or an error message.
    """
    logger.info(f"Fetching SPP data for {body.start_date} to {body.end_date} with type {body.settlement_point_type}")

    product = ErcotProductRoute.SPP.value
    params = {
        "deliveryDateFrom": body.start_date,
        "deliveryDateTo": body.end_date,
        "settlementPointType": body.settlement_point_type,
        "size": body.page_size
    }
    file_name = f"spp_data_{body.settlement_point_type}.csv"
    
    return await handle_ercot_request(
        product=product,
        params=params,
        process_func=process_spp_data,
        data_type="SPP",
        file_name=file_name,
        upload_to_s3=body.upload_to_s3,
        s3_client=s3_client
    )


@router.post("/solar-data")
async def get_solar_data(body: SolarRequestBody, s3_client=Depends(get_s3_client)):
    """
    Fetches and processes solar power production data from the ERCOT API.

    Args:
        body (SolarRequestBody): The request body contains the following fields:
            - start_date (str): The start date for the data retrieval (YYYY-MM-DD).
            - end_date (str): The end date for the data retrieval (YYYY-MM-DD).
            - page_size (int): The number of records per page.
            - upload_to_s3 (bool): Whether to upload the processed data to S3.
        s3_client: The S3 client dependency for interacting with Amazon S3.

    Returns:
        dict: A JSON response containing the processed solar data or an error message.
    """
    logger.info(f"Fetching Solar data for {body.start_date} to {body.end_date}")

    product = ErcotProductRoute.SOLAR.value
    params = {
        "intervalEndingFrom": body.start_date,
        "intervalEndingTo": body.end_date,
        "size": body.page_size
    }
    
    return await handle_ercot_request(
        product=product,
        params=params,
        process_func=process_solar_data,
        data_type="Solar",
        file_name=S3FileNameEnum.SOLAR.value,
        upload_to_s3=body.upload_to_s3,
        s3_client=s3_client
    )


@router.post("/wind-data")
async def get_wind_data(body: WindRequestBody, s3_client=Depends(get_s3_client)):
    """
    Fetches and processes wind power production data from the ERCOT API.

    Args:
        body (WindRequestBody): The request body contains the following fields:
            - start_date (str): The start date for the data retrieval (YYYY-MM-DD).
            - end_date (str): The end date for the data retrieval (YYYY-MM-DD).
            - page_size (int): The number of records per page.
            - upload_to_s3 (bool): Whether to upload the processed data to S3.
        s3_client: The S3 client dependency for interacting with Amazon S3.

    Returns:
        dict: A JSON response containing the processed wind data or an error message.
    """
    logger.info(f"Fetching Wind data for {body.start_date} to {body.end_date}")

    product = ErcotProductRoute.WIND.value
    params = {
        "intervalEndingFrom": body.start_date,
        "intervalEndingTo": body.end_date,
        "size": body.page_size
    }
    
    return await handle_ercot_request(
        product=product,
        params=params,
        process_func=process_wind_data,
        data_type="Wind",
        file_name=S3FileNameEnum.WIND.value,
        upload_to_s3=body.upload_to_s3,
        s3_client=s3_client
    )


@router.post("/load-data")
async def get_load_data(body: LoadRequestBody, s3_client=Depends(get_s3_client)):
    """
    Fetches and processes load power production data from the ERCOT API.

    Args:
        body (LoadRequestBody): The request body contains the following fields:
            - start_date (str): The start date for the data retrieval (YYYY-MM-DD).
            - end_date (str): The end date for the data retrieval (YYYY-MM-DD).
            - page_size (int): The number of records per page.
            - upload_to_s3 (bool): Whether to upload the processed data to S3.
        s3_client: The S3 client dependency for interacting with Amazon S3.

    Returns:
        dict: A JSON response containing the processed solar data or an error message.
    """
    logger.info(f"Fetching Load data for {body.start_date} to {body.end_date}")

    product = ErcotProductRoute.LOAD.value
    params = {
        "operatingDayFrom": body.start_date,
        "operatingDayTo": body.end_date,
        "size": body.page_size
    }

    return await handle_ercot_request(
        product=product,
        params=params,
        process_func=process_any_data,
        data_type="Load",
        file_name=S3FileNameEnum.LOAD.value,
        upload_to_s3=body.upload_to_s3,
        s3_client=s3_client
    )


@router.post("/forecast-data")
async def get_forecast_data(body: ForecastRequestBody, s3_client=Depends(get_s3_client)):
    """
    Fetches and processes load power production data from the ERCOT API.

    Args:
        body (LoadRequestBody): The request body contains the following fields:
            - start_date (str): The start date for the data retrieval (YYYY-MM-DD).
            - end_date (str): The end date for the data retrieval (YYYY-MM-DD).
            - page_size (int): The number of records per page.
            - upload_to_s3 (bool): Whether to upload the processed data to S3.
        s3_client: The S3 client dependency for interacting with Amazon S3.

    Returns:
        dict: A JSON response containing the processed solar data or an error message.
    """
    logger.info(f"Fetching forecast data for {body.post_from} to {body.post_to}")

    product = ErcotProductRoute[body.product].value
    params = {
        "postedDatetimeFrom": body.post_from.strftime("%Y-%m-%dT%H:%M:%S"),
        "postedDatetimeTo": body.post_to.strftime("%Y-%m-%dT%H:%M:%S"),
        "size": body.page_size
    }

    return await handle_ercot_request(
        product=product,
        params=params,
        process_func=process_any_data,
        data_type="Forecast",
        file_name=S3FileNameEnum.DUMMY.value,
        upload_to_s3=False,
        s3_client=s3_client
    )

@router.get("/update-daily-data")
async def update_daily_data(s3_client=Depends(get_s3_client)):
    """
    Updates daily data for SPP, Solar, and Wind by fetching the latest data from the ERCOT API
    and uploading it to S3.

    Args:
        s3_client: The S3 client dependency for interacting with Amazon S3.

    Returns:
        dict: A JSON response indicating the success or failure of the operation.
    """
    start_date = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
    end_date = date.today().strftime("%Y-%m-%d")
    logger.info(f"Updating daily data for {start_date} to {end_date}")
    
    results = {}
    update_failed = False

    try:
        # Function to safely fetch and process each data type
        async def fetch_data_safely(data_type: str, fetch_func: Callable, *args) -> Optional[pd.DataFrame]:
            try:
                logger.info(f"Fetching {data_type} data...")
                data_result = await fetch_func(*args)
                
                if not data_result or 'data' not in data_result:
                    logger.warning(f"No {data_type} data found for {start_date}")
                    results[data_type] = {"status": "failed", "reason": f"No data found"}
                    return None
                    
                df = pd.DataFrame(data_result['data'])
                results[data_type] = {"status": "success"}
                return df
                
            except Exception as e:
                logger.error(f"Failed to fetch {data_type} data: {e}", exc_info=True)
                results[data_type] = {"status": "failed", "reason": str(e)}
                return None

        # Fetch all data types
        spp_hu_df = await fetch_data_safely(
            "SPP_HU", 
            get_spp_data, 
            SppRequestBody(start_date=start_date, end_date=start_date, settlement_point_type="HU", page_size=5000)
        )
        
        spp_lz_df = await fetch_data_safely(
            "SPP_LZ", 
            get_spp_data, 
            SppRequestBody(start_date=start_date, end_date=start_date, settlement_point_type="LZ", page_size=5000)
        )
        
        solar_df = await fetch_data_safely(
            "SOLAR", 
            get_solar_data, 
            SolarRequestBody(start_date=start_date, end_date=end_date, page_size=5000)
        )
        
        wind_df = await fetch_data_safely(
            "WIND", 
            get_wind_data, 
            WindRequestBody(start_date=start_date, end_date=end_date, page_size=5000)
        )

        # Upload successful results to S3
        if spp_hu_df is not None:
            await update_data_in_s3(S3FileNameEnum.SPP_HU.value, spp_hu_df, start_date, s3_client)
            
        if spp_lz_df is not None:
            await update_data_in_s3(S3FileNameEnum.SPP_LZ.value, spp_lz_df, start_date, s3_client)
            
        if solar_df is not None:
            await update_data_in_s3(S3FileNameEnum.SOLAR.value, solar_df, start_date, s3_client)
            
        if wind_df is not None:
            await update_data_in_s3(S3FileNameEnum.WIND.value, wind_df, start_date, s3_client)

        # Check if any updates failed
        update_failed = any(result["status"] == "failed" for result in results.values())
        
        if update_failed:
            logger.warning("Some data updates failed. See details in response.")
            return {
                "message": "Daily data update partially completed",
                "details": results
            }
        else:
            logger.info("Daily data updated successfully in S3")
            return {
                "message": "Daily data updated successfully in S3",
                "details": results
            }

    except Exception as e:
        logger.error(f"Unexpected error updating daily data: {e}", exc_info=True)
        raise HTTPException(
            status_code=500, 
            detail="An unexpected error occurred while updating daily data."
        )
