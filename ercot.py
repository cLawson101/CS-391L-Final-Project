"""
Load ERCOT load data from .csv files in the repository
"""

import numpy as np
import pandas as pd

def round_down(item):
    idx = item.find("24:00")
    if idx >= 0:
        item = item[:idx] + "23:59"
    return item

def ercot_data(
    path    
):
    """
   Load ERCOT load data from .csv files in the repository

    Parameters
    __________
    path : str
        Path to .csv file containing demand values from different load zones from ERCOT

    Returns
    _______
    DataFrame
        cleaner ERCOT data
    """

    df = pd.read_excel(path)
    
    # Removing the ERCOT value as it is just the sum
    df = df[df.columns[:-1]]
    if "Hour_End" in df.columns:
        df["DATE"] = pd.to_datetime(df["Hour_End"])
    elif "HourEnding" in df.columns:
        df["HourEnding"] = df["HourEnding"].apply(round_down)
        df["DATE"] = pd.to_datetime(df["HourEnding"])
    df["YEAR"] = [_.year for _ in df["DATE"]]
    df["MONTH"] = [_.month for _ in df["DATE"]]
    df["DAY"] = [_.day for _ in df["DATE"]]
    df["HOUR_ENDING"] = [int(np.ceil(_.hour + _.minute / 60)) for _ in df["DATE"]]
    
    # Reformatting column order to better view time info
    cols = df.columns.tolist()
    cols = cols[-5:] + cols[1:-5]
    df = df[cols]

    return df
