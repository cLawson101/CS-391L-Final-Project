"""
Load NOAA climate data from .csv files in the repository
"""

import numpy as np
import pandas as pd


def stations_wban(
    path=None,
    save_csv=True
):
    """
    WBAN, stations metdata set from .txt file

    Parameters
    __________
    path : str, default None
        Path to .txt file
    save_csv : bool, default True
        Should .csv be saved for later refernce

    Returns
    _______
    DataFrame
        Stations metadata
    """

    if path:
        raw_data = pd.read_fwf(
            filepath_or_buffer=path,
            header=None,
            colspecs=[(22, 28), (50, 71), (71, 74), (74, 105), (111, 142), (142, 173)]
        )[1:]
    else:
        raw_data = pd.read_fwf(
            filepath_or_buffer=f"{__file__.split('CS-391L-Final-Project')[0]}/CS-391L-Final-Project/data/noaa/mshr_standard.txt",
            header=None,
            colspecs=[(22, 28), (50, 71), (71, 74), (74, 105), (111, 142), (142, 173)]
        )[1:]

    stations_wban = raw_data
    stations_wban.columns = ['WBAN', 'COUNTRY', 'STATE', 'CITY', 'NAME_1', 'NAME_2']

    if save_csv:
        stations_wban.to_csv(
            path_or_buf=f"{__file__.split('CS-391L-Final-Project')[0]}/CS-391L-Final-Project/data/noaa/stations_wban.csv",
            index=False
        )

    return raw_data


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

    raw_data = pd.read_csv(
        filepath_or_buffer=path, 
        low_memory=False
    )
    try:
        stations_wban = pd.read_csv(
            filepath_or_buffer=f"{__file__.split('CS-391L-Final-Project')[0]}/CS-391L-Final-Project/data/noaa/stations_wban.csv",
            low_memory=False
        )
    except FileNotFoundError:
        stations_wban = stations_wban()  # slower to infer fixed width text columns

    hourly_raw_data = raw_data.loc[
        raw_data.REPORT_TYPE == 'FM-15'  # select hourly reporting
    ]
    with pd.option_context('mode.chained_assignment', None):
        hourly_raw_data['WBAN'] = [int(str(_)[-5:]) for _ in hourly_raw_data.STATION]
    hourly_raw_data = hourly_raw_data.merge(stations_wban, how='left', on='WBAN')

    hourly_raw_data = hourly_raw_data[[  # filter useful columns
        'STATION',
        'WBAN',
        'DATE',
        'COUNTRY',
        'STATE',
        'CITY',
        'NAME_1',
        'NAME_2',
        'BackupName',
        'BackupLatitude',
        'BackupLongitude',
        'HourlyDewPointTemperature',
        'HourlyDryBulbTemperature',
        'HourlyPrecipitation',
        'HourlyPressureChange',
        'HourlyPressureTendency',
        'HourlyRelativeHumidity',
    ]]
    hourly_raw_data.rename({
        'BackupLatitude': 'LATITUDE',
        'BackupLongitude': 'LONGITUDE',
        'BackupName': 'BACKUP_NAME',
        'HourlyDewPointTemperature': 'DEW_POINT_TEMPERATURE',
        'HourlyDryBulbTemperature': 'DRY_BULB_TEMPERATURE',
        'HourlyPrecipitation': 'PRECIPITATION',
        'HourlyPressureChange': 'PRESSURE_CHANGE',
        'HourlyPressureTendency': 'PRESSURE_TENDENCY',
        'HourlyRelativeHumidity': 'RELATIVE_HUMIDITY'
        }, 
        axis=1, 
        inplace=True
    )

    hourly_raw_data['DATE'] = pd.to_datetime(hourly_raw_data['DATE'])
    hourly_raw_data['YEAR'] = [_.year for _ in hourly_raw_data['DATE']]
    hourly_raw_data['MONTH'] = [_.month for _ in hourly_raw_data['DATE']]
    hourly_raw_data['DAY'] = [_.day for _ in hourly_raw_data['DATE']]
    hourly_raw_data['HOUR_ENDING'] =  [int(np.ceil(_.hour + _.minute / 60)) for _ in hourly_raw_data['DATE']]  # assume based on timestamp

    return hourly_raw_data
