import pandas as pd
from fastapi import APIRouter, HTTPException, Depends
from datetime import date, timedelta

from logger_config import logger
from api.ercot_service import (
    get_ercot_access_token,
    make_ercot_api_request,
    process_spp_data,
    process_solar_data,
    process_wind_data,
    process_load_data,
    fetch_ercot_data,
)
from models.ercot_models import (
    SppRequestBody,
    SolarRequestBody,
    WindRequestBody,
    LoadRequestBody,
    ErcotProductRoute,
    S3FileNameEnum,
)
from api.utils import get_s3_client, update_data_in_s3, upload_data_to_s3

router = APIRouter()


@router.get("/ercot-access-token")
async def get_access_token():
    return await get_ercot_access_token()


@router.post("/spp-data")
async def get_spp_data(body: SppRequestBody, s3_client=Depends(get_s3_client)):
    """
    Fetches and processes Settlement Point Price (SPP) data from the ERCOT API.

    Args:
        body (SppRequestBody): The request body containing the following fields:
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

    # Define the product and query parameters for the API request
    product = ErcotProductRoute.SPP.value
    params = {
        "deliveryDateFrom": body.start_date,
        "deliveryDateTo": body.end_date,
        "settlementPointType": body.settlement_point_type,
        "size": body.page_size
    }

    try:
        # Fetch and process the SPP data
        spp_data_df = await fetch_ercot_data(product, params)
        if spp_data_df.empty:
            logger.error("No SPP data returned from the API.")
            raise HTTPException(status_code=404, detail="No SPP data found for the given parameters.")

        logger.info("SPP data fetched successfully")
        final_data_df = await process_spp_data(spp_data_df)

        # Upload to S3 if required
        file_name = f"spp_data_{body.settlement_point_type}.csv"
        if body.upload_to_s3:
            await upload_data_to_s3(final_data_df, file_name, s3_client)
            logger.info(f"SPP data uploaded to S3: {file_name}")

        records = final_data_df.to_dict(orient='records')
        return {"data": records}
    except HTTPException as e:
        logger.error(f"HTTPException: {e.detail}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An unexpected error occurred while processing SPP data.")


@router.post("/solar-data")
async def get_solar_data(body: SolarRequestBody, s3_client=Depends(get_s3_client)):
    """
    Fetches and processes solar power production data from the ERCOT API.

    Args:
        body (SolarRequestBody): The request body containing the following fields:
            - start_date (str): The start date for the data retrieval (YYYY-MM-DD).
            - end_date (str): The end date for the data retrieval (YYYY-MM-DD).
            - page_size (int): The number of records per page.
            - upload_to_s3 (bool): Whether to upload the processed data to S3.
        s3_client: The S3 client dependency for interacting with Amazon S3.

    Returns:
        dict: A JSON response containing the processed solar data or an error message.
    """
    logger.info(f"Fetching Solar data for {body.start_date} to {body.end_date}")

    # Define the product and query parameters for the API request
    product = ErcotProductRoute.SOLAR.value
    params = {
        "intervalEndingFrom": body.start_date,
        "intervalEndingTo": body.end_date,
        "size": body.page_size
    }

    try:
        # Fetch and process the SPP data
        solar_data_df = await fetch_ercot_data(product, params)
        if solar_data_df.empty:
            logger.warning("No Solar data returned from the API.")
            raise HTTPException(status_code=404, detail="No Solar data found for the given parameters.")

        logger.info("Solar data fetched successfully")
        final_data_df = await process_solar_data(solar_data_df)

        file_name = S3FileNameEnum.SOLAR.value
        if body.upload_to_s3:
            await upload_data_to_s3(final_data_df, file_name, s3_client)
            logger.info(f"Solar data uploaded to S3: {file_name}")

        records = final_data_df.to_dict(orient='records')
        return {"data": records}
    except HTTPException as e:
        logger.error(f"HTTPException: {e.detail}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An unexpected error occurred while processing Solar data.")


@router.post("/wind-data")
async def get_wind_data(body: WindRequestBody, s3_client=Depends(get_s3_client)):
    """
    Fetches and processes wind power production data from the ERCOT API.

    Args:
        body (WindRequestBody): The request body containing the following fields:
            - start_date (str): The start date for the data retrieval (YYYY-MM-DD).
            - end_date (str): The end date for the data retrieval (YYYY-MM-DD).
            - page_size (int): The number of records per page.
            - upload_to_s3 (bool): Whether to upload the processed data to S3.
        s3_client: The S3 client dependency for interacting with Amazon S3.

    Returns:
        dict: A JSON response containing the processed wind data or an error message.
    """
    logger.info(f"Fetching Wind data for {body.start_date} to {body.end_date}")

    # Define the product and query parameters for the API request
    product = ErcotProductRoute.WIND.value
    params = {
        "intervalEndingFrom": body.start_date,
        "intervalEndingTo": body.end_date,
        "size": body.page_size
    }

    try:
        # Fetch and process the SPP data
        wind_data_df = await fetch_ercot_data(product, params)
        if wind_data_df.empty:
            logger.warning("No Wind data returned from the API.")
            raise HTTPException(status_code=404, detail="No Wind data found for the given parameters.")

        logger.info("Wind data fetched successfully")
        final_data_df = await process_wind_data(wind_data_df)

        file_name = S3FileNameEnum.WIND.value
        if body.upload_to_s3:
            await upload_data_to_s3(final_data_df, file_name, s3_client)
            logger.info(f"Wind data uploaded to S3: {file_name}")

        records = final_data_df.to_dict(orient='records')
        return {"data": records}
    except HTTPException as e:
        logger.error(f"HTTPException: {e.detail}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An unexpected error occurred while processing Wind data.")


@router.post("/load-data")
async def get_load_data(body: LoadRequestBody):
    product = "/np6-346-cd/act_sys_load_by_fzn"
    params = {"operatingDayFrom": body.start_date, "operatingDayTo": body.end_date, "DSTFlag": body.dst_flag,
              "size": body.page_size}

    page = 1
    total_pages = 2
    access_token = await get_ercot_access_token()
    final_data = pd.DataFrame()
    file_name = f"load_data.csv"
    while page <= total_pages:
        params["page"] = page
        response = await make_ercot_api_request(product, params, access_token)
        if response:
            total_pages = response["_meta"]["totalPages"]
            processed_load_data = await process_load_data(data=response["data"])
            final_data = pd.concat([final_data, processed_load_data], ignore_index=True)
        page += 1
    # final_data.to_csv(load_data_dir + "/" + file_name, index=False)
    return final_data.to_json(orient="records")


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
    # Fetch the latest data from ERCOT API
    start_date = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
    end_date = date.today().strftime("%Y-%m-%d")
    logger.info(f"Updating daily data for {start_date} to {end_date}")

    try:
        # Fetch and process SPP HU data
        logger.info("Fetching SPP HU data...")
        spp_hu_data = await get_spp_data(SppRequestBody(
            start_date=start_date,
            end_date=start_date,
            settlement_point_type="HU",
            page_size=5000
        ))
        if not spp_hu_data or 'data' not in spp_hu_data:
            logger.warning(f"No SPP HU data found for {start_date}")
            raise HTTPException(status_code=404, detail=f"No SPP HU data found for {start_date}")
        spp_hu_df = pd.DataFrame(spp_hu_data['data'])

        # Fetch and process SPP LZ data
        logger.info("Fetching SPP LZ data...")
        spp_lz_data = await get_spp_data(SppRequestBody(
            start_date=start_date,
            end_date=start_date,
            settlement_point_type="LZ",
            page_size=5000
        ))
        if not spp_lz_data or 'data' not in spp_lz_data:
            logger.warning(f"No SPP LZ data found for {start_date}")
            raise HTTPException(status_code=404, detail=f"No SPP LZ data found for {start_date}")
        spp_lz_df = pd.DataFrame(spp_lz_data['data'])

        # Fetch and process Solar data
        logger.info("Fetching Solar data...")
        solar_data = await get_solar_data(SolarRequestBody(
            start_date=start_date,
            end_date=end_date,
            page_size=5000
        ))
        if not solar_data or 'data' not in solar_data:
            logger.warning(f"No Solar data found for {start_date} to {end_date}")
            raise HTTPException(status_code=404, detail=f"No Solar data found for {start_date} to {end_date}")
        solar_df = pd.DataFrame(solar_data['data'])

        # Fetch and process Wind data
        logger.info("Fetching Wind data...")
        wind_data = await get_wind_data(WindRequestBody(
            start_date=start_date,
            end_date=end_date,
            page_size=5000
        ))
        if not wind_data or 'data' not in wind_data:
            logger.warning(f"No Wind data found for {start_date} to {end_date}")
            raise HTTPException(status_code=404, detail=f"No Wind data found for {start_date} to {end_date}")
        wind_df = pd.DataFrame(wind_data['data'])

        # Update data in S3
        logger.info("Uploading data to S3...")
        await update_data_in_s3(S3FileNameEnum.SPP_HU.value, spp_hu_df, start_date, s3_client)
        await update_data_in_s3(S3FileNameEnum.SPP_LZ.value, spp_lz_df, start_date, s3_client)
        await update_data_in_s3(S3FileNameEnum.SOLAR.value, solar_df, start_date, s3_client)
        await update_data_in_s3(S3FileNameEnum.WIND.value, wind_df, start_date, s3_client)

        logger.info("Daily data updated successfully in S3")
        return {"message": "Daily data updated successfully in S3"}

    except HTTPException as e:
        logger.error(f"HTTPException: {e.detail}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An unexpected error occurred while updating daily data.")
