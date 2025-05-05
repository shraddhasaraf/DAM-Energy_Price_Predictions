import pandas as pd
from io import BytesIO

from api.routes.ercot import get_forecast_data
from logger_config import logger
from datetime import datetime, timedelta, date
from fastapi import APIRouter, Depends, HTTPException
from api.model_service import predict_settlement_point
from models.ercot_models import (
    PredictionRequestBody,
    S3FileNameEnum,
    SettlementPointName, ForecastRequestBody, ErcotProductRoute
)
from api.utils import (
    upload_data_to_s3,
    get_s3_client,
    get_file_from_s3,
    get_dataframe_from_s3,
)

router = APIRouter()


@router.post("/predictions")
async def get_predictions(body: PredictionRequestBody, s3_client=Depends(get_s3_client)):
    try:
        # get predictions file from s3
        prediction_file = await get_file_from_s3(S3FileNameEnum.PREDICTIONS.value, s3_client)
        if not prediction_file:
            return HTTPException(status_code=404, detail="Prediction file not found")
        prediction_df = pd.read_csv(BytesIO(prediction_file))

        spp_name_list = [spp_name.value for spp_name in body.settlement_point_name]

        prediction_for_date_df = prediction_df[(prediction_df['deliveryDate'] == str(body.prediction_date)) & (
            prediction_df['settlementPoint'].isin(spp_name_list))]
        if prediction_for_date_df.empty:
            return {"error": f"No predictions found for the date {body.prediction_date}"}
        records = prediction_for_date_df.to_dict(orient='records')
        return {"data": records}
    except Exception as e:
        logger.error(str(e))
        return {"error": "Error fetching predictions"}


@router.get("/update-predictions")
async def make_predictions(s3_client=Depends(get_s3_client)):
    prediction_start_date = date.today().strftime("%Y-%m-%d")
    prediction_end_date = (date.today() + timedelta(days=7)).strftime("%Y-%m-%d")
    logger.info(f"Making predictions for {prediction_start_date} to {prediction_end_date}")
    # Fetch historical data from S3
    spp_hu_df = await get_dataframe_from_s3(S3FileNameEnum.SPP_HU.value, s3_client)
    spp_lz_df = await get_dataframe_from_s3(S3FileNameEnum.SPP_LZ.value, s3_client)
    wind_df = await get_dataframe_from_s3(S3FileNameEnum.WIND.value, s3_client)
    solar_df = await get_dataframe_from_s3(S3FileNameEnum.SOLAR.value, s3_client)
    load_df = await get_dataframe_from_s3(S3FileNameEnum.LOAD.value, s3_client)

    # Fetch forecast data from S3
    load_forcast_data = await get_forecast_data(ForecastRequestBody(
        product=ErcotProductRoute.LOAD_FORECAST.value,
        post_from=datetime.now() - timedelta(hours=1),
        post_to=datetime.now()
    )
    )
    if not load_forcast_data or 'data' not in load_forcast_data:
        raise HTTPException(status_code=404, detail="Load forecast data not found")
    load_forecast_df = pd.DataFrame(load_forcast_data['data'])

    wind_forcast_data = await get_forecast_data(ForecastRequestBody(
        product=ErcotProductRoute.WIND_FORECAST.value,
        post_from=datetime.now() - timedelta(hours=1),
        post_to=datetime.now()
    )
    )
    if not wind_forcast_data or 'data' not in wind_forcast_data:
        raise HTTPException(status_code=404, detail="Wind forecast data not found")
    wind_forecase_df = pd.DataFrame(wind_forcast_data['data'])

    solar_forcast_data = await get_forecast_data(ForecastRequestBody(
        product=ErcotProductRoute.SOLAR_FORECAST.value,
        post_from=datetime.now() - timedelta(hours=1),
        post_to=datetime.now()
    )
    )
    if not solar_forcast_data or 'data' not in solar_forcast_data:
        raise HTTPException(status_code=404, detail="Solar forecast data not found")
    solar_forcast_df = pd.DataFrame(solar_forcast_data['data'])

    prediction_df = pd.DataFrame()
    for spp_name in SettlementPointName:
        df = predict_settlement_point(
            spp_hu_df, spp_lz_df, solar_df,
            wind_df, load_df, load_forecast_df,
            wind_forecase_df, solar_forcast_df,
            spp_name.value
        )
        prediction_df = pd.concat([prediction_df, df], ignore_index=True)
    file_name = f"predictions_latest.csv"
    await upload_data_to_s3(prediction_df, file_name, s3_client)
    return {"message": "Predictions made successfully"}
