import datetime
from enum import Enum
from pydantic import BaseModel, field_validator


class ErcotProductRoute(Enum):
    SPP = "/np6-905-cd/spp_node_zone_hub"
    SOLAR = "/np4-746-cd/spp_actual_5min_avg_values_geo"
    WIND = "/np4-733-cd/wpp_actual_5min_avg_values"
    LOAD = "/np6-346-cd/act_sys_load_by_fzn"


class SettlementPointName(Enum):
    HB_HOUSTON = "HB_HOUSTON"
    HB_NORTH = "HB_NORTH"
    HB_PAN = "HB_PAN"
    HB_SOUTH = "HB_SOUTH"
    HB_WEST = "HB_WEST"
    LZ_AEN = "LZ_AEN"
    LZ_CPS = "LZ_CPS"
    LZ_HOUSTON = "LZ_HOUSTON"
    LZ_LCRA = "LZ_LCRA"
    LZ_NORTH = "LZ_NORTH"
    LZ_RAYBN = "LZ_RAYBN"
    LZ_SOUTH = "LZ_SOUTH"
    LZ_WEST = "LZ_WEST"


class S3FileNameEnum(Enum):
    SPP_HU = "spp_data_HU.csv"
    SPP_LZ = "spp_data_LZ.csv"
    SOLAR = "solar_data.csv"
    WIND = "wind_data.csv"


class SppRequestBody(BaseModel):
    start_date: datetime.date
    end_date: datetime.date
    settlement_point_type: str
    upload_to_s3: bool = False
    page: int = 1
    page_size: int = 5000

    class Config:
        json_schema_extra = {
            "example": {
                "start_date": "2024-01-01",
                "end_date": "2024-01-31",
                "settlement_point_type": "LZ",
                "upload_to_s3": True,
                "page_size": 5000
            }
        }


class SolarRequestBody(BaseModel):
    start_date: datetime.date
    end_date: datetime.date
    upload_to_s3: bool = False
    page: int = 1
    page_size: int = 5000

    class Config:
        json_schema_extra = {
            "example": {
                "start_date": "2024-01-01",
                "end_date": "2024-01-31",
                "upload_to_s3": True,
                "page_size": 5000
            }
        }


class WindRequestBody(BaseModel):
    start_date: datetime.date
    end_date: datetime.date
    upload_to_s3: bool = False
    page: int = 1
    page_size: int = 5000

    class Config:
        json_schema_extra = {
            "example": {
                "start_date": "2024-01-01",
                "end_date": "2024-01-31",
                "upload_to_s3": True,
                "page_size": 5000
            }
        }


class LoadRequestBody(BaseModel):
    start_date: datetime.date
    end_date: datetime.date
    dst_flag: bool = False
    page: int = 1
    page_size: int = 5000


class PredictionRequestBody(BaseModel):
    prediction_date: datetime.date = datetime.date.today()
    settlement_point_name: SettlementPointName

    @classmethod
    @field_validator("prediction_date")
    def validate_prediction_date(cls, value: datetime.date):
        today = datetime.date.today()
        seventh_day = today + datetime.timedelta(days=7)
        if not (today <= value <= seventh_day):
            raise ValueError(f"prediction_date must be between {today} and {seventh_day}")
        return value