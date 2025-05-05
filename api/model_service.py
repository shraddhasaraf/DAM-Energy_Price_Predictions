import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler
from lightgbm import LGBMRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from datetime import timedelta


def predict_settlement_point(spp_hu_df, spp_lz_df, solar_df, wind_df, load_df, load_forecast, wind_forecast,
                             solar_forecast, settlement_point_name):
    """
    Predicts Settlement Price for the next 7 days for the given settlement point.

    Args:
        settlement_point_name (str): Name of the Settlement Point (e.g., 'HB_HOUSTON')

    Returns:
        pd.DataFrame: Predicted Settlement Prices for the next 7 days
    """

    # ===== Load Historical Data =====
    solar_df['deliveryDate'] = pd.to_datetime(solar_df['deliveryDate'])
    wind_df['deliveryDate'] = pd.to_datetime(wind_df['deliveryDate'])
    spp_hu_df['deliveryDate'] = pd.to_datetime(spp_hu_df['deliveryDate'])
    spp_lz_df['deliveryDate'] = pd.to_datetime(spp_lz_df['deliveryDate'])
    load_df = load_df.rename(columns={
        "OperDay": "deliveryDate",
        "HourEnding": "deliveryHour",
        "TOTAL": "SystemTotal"
    })
    # Convert deliveryDate to datetime
    load_df["deliveryDate"] = pd.to_datetime(load_df["deliveryDate"])

    # Extract numeric hour from HourEnding and compute the true hour (HourEnding 01:00 = 00:00–01:00)
    load_df["HourEnding"] = load_df["deliveryHour"].str.extract(r"(\d+)").astype(int)
    load_df["Timestamp"] = load_df["deliveryDate"] + pd.to_timedelta(load_df["HourEnding"] - 1, unit='h')

    # Expand each hour into 4 intervals, dividing SystemTotal equally
    expanded_rows = []
    for _, row in load_df.iterrows():
        per_interval_value = row["SystemTotal"] / 4
        for interval in range(1, 5):
            expanded_rows.append({
                "deliveryDate": row["Timestamp"].normalize(),
                "deliveryHour": row["Timestamp"].hour,
                "deliveryInterval": interval,
                "SystemTotal": per_interval_value
            })
    load_df = pd.DataFrame(expanded_rows)  # Update load_df with expanded rows
    load_df = load_df.ffill()

    # ===== Load Forecast Data =====
    solar_forecast['DELIVERY_DATE'] = pd.to_datetime(solar_forecast['DELIVERY_DATE'])
    wind_forecast['DELIVERY_DATE'] = pd.to_datetime(wind_forecast['DELIVERY_DATE'])

    # ===== Process Load Forecast Data =====
    load_forecast["DELIVERY_DATE"] = pd.to_datetime(load_forecast["DeliveryDate"], format="%m/%d/%Y")
    load_forecast["HOUR_ENDING"] = load_forecast["HourEnding"].str.extract(r"(\d+)").astype(int)
    load_forecast["Timestamp"] = pd.to_datetime(load_forecast["DELIVERY_DATE"]) + pd.to_timedelta(
        load_forecast["HOUR_ENDING"] - 1, unit="h")

    def expand_load_forecast(df):
        rows = []
        for _, row in df.iterrows():
            per_interval_value = row["SystemTotal"] / 4  # divide the hourly load
            for interval in range(1, 5):
                r = row.copy()
                r["deliveryDate"] = r["Timestamp"].normalize()
                r["deliveryHour"] = r["Timestamp"].hour
                r["deliveryInterval"] = interval
                r["SystemTotal"] = per_interval_value  # assign divided value
                rows.append(r)
        return pd.DataFrame(rows)[["deliveryDate", "deliveryHour", "deliveryInterval", "SystemTotal"]]

    # ===== Filter forecast data from current date =====
    last_timestamp = pd.concat([spp_hu_df, spp_lz_df])["deliveryDate"].max()
    forecast_start = last_timestamp + pd.Timedelta(minutes=15)
    forecast_end = forecast_start + pd.Timedelta(days=7)

    solar_forecast = solar_forecast[
        (solar_forecast["DELIVERY_DATE"] >= forecast_start) &
        (solar_forecast["DELIVERY_DATE"] < forecast_end)
        ]
    wind_forecast = wind_forecast[
        (wind_forecast["DELIVERY_DATE"] >= forecast_start) &
        (wind_forecast["DELIVERY_DATE"] < forecast_end)
        ]

    # Reconstruct timestamps
    solar_forecast["Timestamp"] = pd.to_datetime(solar_forecast["DELIVERY_DATE"]) + pd.to_timedelta(
        solar_forecast["HOUR_ENDING"] - 1, unit="h")
    wind_forecast["Timestamp"] = pd.to_datetime(wind_forecast["DELIVERY_DATE"]) + pd.to_timedelta(
        wind_forecast["HOUR_ENDING"] - 1, unit="h")

    # Expand to 15-minute intervals
    def expand_hourly_forecast(df):
        expanded_rows = []
        for _, row in df.iterrows():
            for interval in range(1, 5):
                r = row.copy()
                timestamp = pd.to_datetime(r["Timestamp"])
                r["deliveryDate"] = timestamp.normalize()
                r["deliveryHour"] = timestamp.hour
                r["deliveryInterval"] = interval
                expanded_rows.append(r)
        return pd.DataFrame(expanded_rows).drop(columns=["DELIVERY_DATE", "HOUR_ENDING", "Timestamp"])

    load_df_expanded = expand_load_forecast(load_forecast)

    solar_df_expanded = expand_hourly_forecast(solar_forecast)
    wind_df_expanded = expand_hourly_forecast(wind_forecast)
    wind_df_expanded = wind_df_expanded.rename(columns={"COP_HSL_SYSTEM_WIDE": "COP_HSL_SYSTEMWIDE"})

    # ===== Merge SPP datasets =====
    df = pd.concat([spp_hu_df, spp_lz_df], ignore_index=True)

    # ===== Aggregate Solar and Wind to 15-min intervals =====
    solar_agg = (
        solar_df.groupby(["deliveryDate", "deliveryHour", "deliveryInterval"])
        .sum()
        .reset_index()
        .rename(columns={
            "genSystemWide": "systemWide_GEN",
            "genCenterWest": "centerWest_GEN",
            "genNorthWest": "northWest_GEN",
            "genFarWest": "farWest_GEN",
            "genFarEast": "farEast_GEN",
            "genSouthEast": "southEast_GEN",
            "genCenterEast": "centerEast_GEN"
        })
    )

    wind_agg = (
        wind_df.groupby(["deliveryDate", "deliveryHour", "deliveryInterval"])
        .sum()
        .reset_index()
    )

    # ===== Merge with main SPP data =====
    df = df.merge(wind_agg, on=["deliveryDate", "deliveryHour", "deliveryInterval"], how="left")
    df = df.merge(solar_agg, on=["deliveryDate", "deliveryHour", "deliveryInterval"], how="left")

    # ===== Merge with Load data ===== # This is the critical change
    df = df.merge(load_df, on=["deliveryDate", "deliveryHour", "deliveryInterval"], how="left")

    # ===== Feature Engineering =====
    df.fillna(method='ffill', inplace=True)
    df["DayOfWeek"] = df["deliveryDate"].dt.dayofweek
    df["Month"] = df["deliveryDate"].dt.month
    df["Year"] = df["deliveryDate"].dt.year

    # Clip outliers
    lower_bound = df['settlementPointPrice'].quantile(0.01)
    upper_bound = df['settlementPointPrice'].quantile(0.99)
    df['settlementPointPrice'] = np.clip(df['settlementPointPrice'], lower_bound, upper_bound)

    df = df.sort_values(by=["settlementPoint", "deliveryDate", "deliveryHour", "deliveryInterval"])
    df["Timestamp"] = df["deliveryDate"] + pd.to_timedelta(
        df["deliveryHour"] * 60 + (df["deliveryInterval"] - 1) * 15, unit='m'
    )

    # Lag and Rolling Features
    df["Lag_1"] = df.groupby("settlementPoint")["settlementPointPrice"].shift(1)
    df["Lag_96"] = df.groupby("settlementPoint")["settlementPointPrice"].shift(96)
    df["Lag_288"] = df.groupby("settlementPoint")["settlementPointPrice"].shift(288)
    df["RollingMean_3"] = df.groupby("settlementPoint")["settlementPointPrice"].shift(1).rolling(3).mean().reset_index(
        0, drop=True)
    df["RollingStd_3"] = df.groupby("settlementPoint")["settlementPointPrice"].shift(1).rolling(3).std().reset_index(0,
                                                                                                                     drop=True)

    df = df.dropna()

    # ===== Filter for specific settlement point =====
    df_sp = df.copy()

    # Feature list (training)
    common_features = [
        "deliveryHour", "deliveryInterval", "DayOfWeek", "Month", "Year",
        'genSystemWide',  # wind
        'systemWide_GEN',
        'SystemTotal'  # load
    ]
    target = "settlementPointPrice"

    # ===== Train Model =====
    X = df_sp[common_features]
    y = df_sp[target]

    model_lgbm = LGBMRegressor(n_estimators=300, learning_rate=0.01, max_depth=10)
    model_lgbm.fit(X, y)

    # ===== Predict Next 7 Days =====
    last_timestamp = df_sp["Timestamp"].max()

    future_dates = []
    future_hours = []
    future_intervals = []

    current_time = last_timestamp + timedelta(minutes=15)

    for _ in range(672):  # 7 days × 24 hours × 4 intervals
        future_dates.append(current_time.date())
        future_hours.append(current_time.hour)
        future_intervals.append((current_time.minute // 15) + 1)
        current_time += timedelta(minutes=15)

    future_df = pd.DataFrame({
        "deliveryDate": future_dates,
        "deliveryHour": future_hours,
        "deliveryInterval": future_intervals
    })

    future_df["DayOfWeek"] = pd.to_datetime(future_df["deliveryDate"]).dt.dayofweek
    future_df["Month"] = pd.to_datetime(future_df["deliveryDate"]).dt.month
    future_df["Year"] = pd.to_datetime(future_df["deliveryDate"]).dt.year

    # Merge forecasted features
    for df_ in [future_df, solar_df_expanded, wind_df_expanded, load_df_expanded]:
        df_["deliveryDate"] = pd.to_datetime(df_["deliveryDate"])
        df_["deliveryHour"] = df_["deliveryHour"].astype(int)
        df_["deliveryInterval"] = df_["deliveryInterval"].astype(int)

    future_df = future_df.merge(solar_df_expanded, on=["deliveryDate", "deliveryHour", "deliveryInterval"], how="left")
    future_df = future_df.merge(wind_df_expanded, on=["deliveryDate", "deliveryHour", "deliveryInterval"], how="left")
    future_df = future_df.merge(load_df_expanded, on=["deliveryDate", "deliveryHour", "deliveryInterval"], how="left")

    # Rename the columns in future_df to match features_future
    future_df = future_df.rename(columns={
        "COP_HSL_SYSTEM_WIDE": "systemWide_GEN",  # Solar
        "COP_HSL_CenterWest": "centerWest_GEN",  # Solar
        "COP_HSL_NorthWest": "northWest_GEN",  # Solar
        "COP_HSL_FarWest": "farWest_GEN",  # Solar
        "COP_HSL_FarEast": "farEast_GEN",  # Solar
        "COP_HSL_SouthEast": "southEast_GEN",  # Solar
        "COP_HSL_CenterEast": "centerEast_GEN",  # Solar
        "COP_HSL_SYSTEMWIDE": "genSystemWide"  # Wind
    })

    # Ensure complete cases
    future_df = future_df.dropna(subset=common_features)

    # Predict
    X_future = future_df[common_features]
    future_df["PredictedSettlementPrice"] = model_lgbm.predict(X_future)

    # Final formatting
    future_df["Timestamp"] = pd.to_datetime(future_df["deliveryDate"]) + pd.to_timedelta(
        future_df["deliveryHour"] * 60 + (future_df["deliveryInterval"] - 1) * 15, unit='m'
    )
    future_df["settlementPoint"] = settlement_point_name
    future_df["settlementPointPrice"] = future_df["PredictedSettlementPrice"]

    return future_df[[
        "settlementPoint", "deliveryDate", "deliveryHour",
        "deliveryInterval", "settlementPointPrice"
    ]]
