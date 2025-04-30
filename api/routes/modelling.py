import json
import pandas as pd
from io import BytesIO
from logger_config import logger
from datetime import datetime, timedelta, date
from fastapi import APIRouter, Depends
from models.ercot_models import (
    PredictionRequestBody,
    SppRequestBody,
    SolarRequestBody,
    WindRequestBody
)
from api.routes.ercot import (
    get_spp_data,
    get_solar_data,
    get_wind_data,
)
from api.utils import (
    upload_data_to_s3,
    get_s3_client,
    get_file_from_s3,
)

router = APIRouter()


@router.post("/predictions")
async def get_predictions(body: PredictionRequestBody, s3_client=Depends(get_s3_client)):
    try:
        # get predictions file from gdrive
        prediction_file = await get_file_from_s3("spp_data_HU.csv", s3_client)
        prediction_df = pd.read_csv(BytesIO(prediction_file))
        prediction_for_date_df = prediction_df[prediction_df['date'] == body.prediction_date]
        if prediction_for_date_df.empty:
            return {"error": f"No predictions found for the date {body.prediction_date}"}
        records = prediction_for_date_df.to_dict(orient='records')
        json_response = {"data": records}
        json_string = json.dumps(json_response, indent=4)
        return json_string
    except Exception as e:
        return {"error": str(e)}


@router.get("/update-predictions")
async def make_predictions(s3_client=Depends(get_s3_client)):
    prediction_start_date = date.today().strftime("%Y-%m-%d")
    prediction_end_date = (date.today() + timedelta(days=7)).strftime("%Y-%m-%d")
    logger.info(f"Making predictions for {prediction_start_date} to {prediction_end_date}")

    # predictions_df = make_price_predictions(spp_data_df, solar_data_df, wind_data_df, prediction_start_date,
    # prediction_end_date)
    file_name = f"predictions_latest.csv"
    # save_file_to_drive(predictions_df, folder, file_name)
    # await upload_data_to_s3(predictions_df, file_name, s3_client)
    return {"message": "Predictions made successfully"}
