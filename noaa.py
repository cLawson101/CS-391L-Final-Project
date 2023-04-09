"""
Load NOAA climate data from .csv files in the repository
"""

import numpy as np
import pandas as pd


def climate_data_etl(
    path    
):
    """
    ETL for NOAA hourly climate observations data from .csv file

    Parameters
    __________
    path : str
        Path to .csv file containing Local Climatological Data (LCD) from NOAA

    Returns
    _______
    DataFrame
        Clean NOAA climate data
    """

    raw_data = pd.read_csv(filepath_or_buffer=path)

    hourly_raw_data = raw_data.loc[
        raw_data.reportType == 'FM-15'  # select hourly reporting
    ]

    hourly_raw_data = hourly_raw_data[[  # filter useful columns
        'STATION', 
        'STATION_NAME', 
        'ELEVATION', 
        'LATITUDE', 
        'LONGITUDE', 
        'DATE', 
        # 'reportType', 
        # 'HourlySkyConditions', 
        # 'HourlyVisibility', 
        # 'HourlyPresentWeatherType', 
        'HourlyDryBulbTemperatureF', 
        # 'HourlyDryBulbTemperatureC', 
        # 'HourlyWetBulbTemperatureF', 
        # 'HourlyWetBulbTemperatureC', 
        'HourlyDewPointTemperatureF', 
        # 'HourlyDewPointTemperatureC', 
        'HourlyRelativeHumidity', 
        # 'HourlyWindSpeed', 
        # 'HourlyWindDirection', 
        # 'HourlyWindGustSpeed', 
        'HourlyStationPressure', 
        'HourlyPressureTendency',  # prev 3 hrs: 0-3 increase, 5-8 decrease,  4 no change 
        'HourlyPressureChange',  # 
        # 'HourlySeaLevelPressure', 
        'HourlyPrecipitation',  # precip in inches to hundredths over past hour, “T” trace amount
        # 'HourlyAltimeterSetting', 
        # 'DailyMaximumDryBulbTemperature', 
        # 'DailyMinimumDryBulbTemperature', 
        # 'DailyAverageDryBulbTemperature', 
        # 'DailyDepartureFromNormalAverageTemperature', 
        # 'DailyAverageRelativeHumidity', 
        # 'DailyAverageDewPointTemperature', 
        # 'DailyAverageWetBulbTemperature', 
        # 'DailyHeatingDegreeDays', 
        # 'DailyCoolingDegreeDays', 
        # 'DailySunrise', 
        # 'DailySunset', 
        # 'DailyPrecipitation', 
        # 'DailySnowfall', 
        # 'DailySnowDepth', 
        # 'DailyAverageStationPressure', 
        # 'DailyAverageSeaLevelPressure', 
        # 'DailyAverageWindSpeed', 
        # 'DailyPeakWindSpeed', 
        # 'PeakWindDirection', 
        # 'DailySustainedWindSpeed', 
        # 'DailySustainedWindDirection', 
        # 'MonthlyMaximumTemperature', 
        # 'MonthlyMinimumTemperature', 
        # 'MonthlyMeanTemperature', 
        # 'MonthlyAverageRH', 
        # 'MonthlyDewpointTemperature', 
        # 'MonthlyWetBulb', 
        # 'MonthlyHeatingDegreeDays', 
        # 'MonthlyCoolingDegreeDays', 
        # 'MonthlyStationPressure', 
        # 'MonthlySeaLevelPressure', 
        # 'MonthlyAverageWindSpeed', 
        # 'MonthlyTotalSnowfall', 
        # 'MonthlyDepartureFromNormalMaximumTemperature', 
        # 'MonthlyDepartureFromNormalMinimumTemperature', 
        # 'MonthlyDepartureFromNormalAverageTemperature', 
        # 'MonthlyDepartureFromNormalPrecipitation', 
        # 'MonthlyTotalLiquidPrecipitation', 
        # 'MonthlyGreatestPrecip', 
        # 'MonthlyGreatestPrecipDate', 
        # 'MonthlyGreatestSnowfall', 
        # 'MonthlyGreatestSnowfallDate', 
        # 'MonthlyGreatestSnowDepth', 
        # 'MonthlyGreatestSnowDepthDate', 
        # 'MonthlyDaysWithGT90Temp', 
        # 'MonthlyDaysWithLT32Temp', 
        # 'MonthlyDaysWithGT32Temp', 
        # 'MonthlyDaysWithLT0Temp', 
        # 'MonthlyDaysWithGT001Precip', 
        # 'MonthlyDaysWithGT010Precip', 
        # 'MonthlyMaxSeaLevelPressureValue', 
        # 'MonthlyMaxSeaLevelPressureValueDate', 
        # 'MonthlyMaxSeaLevelPressureValueTime', 
        # 'MonthlyMinSeaLevelPressureValue', 
        # 'MonthlyMinSeaLevelPressureValueDate', 
        # 'MonthlyMinSeaLevelPressureValueTime', 
        # 'MonthlyTotalHeatingDegreeDays', 
        # 'MonthlyTotalCoolingDegreeDays', 
        # 'MonthlyDepartureFromNormalHeatingDD', 
        # 'MonthlyDepartureFromNormalCoolingDD'
    ]]

    hourly_raw_data['DATE'] = pd.to_datetime(hourly_raw_data['DATE'])
    hourly_raw_data['YEAR'] = [_.year for _ in hourly_raw_data['DATE']]
    hourly_raw_data['MONTH'] = [_.month for _ in hourly_raw_data['DATE']]
    hourly_raw_data['DAY'] = [_.day for _ in hourly_raw_data['DATE']]
    hourly_raw_data['HOUR_ENDING'] =  [int(np.ceil(_.hour + _.minute / 60)) for _ in hourly_raw_data['DATE']]  # assume based on timestamp

    return hourly_raw_data
