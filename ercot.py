"""
Load ERCOT load data from .csv files in the repository
"""

import numpy as np
import pandas as pd


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

    raw_data = pd.read_excel(filepath_or_buffer=path)
    
    # Removing the ERCOT value as it is just the sum
    df = df[df.columns[:-1]]
    df.head()
    
    df["DATE"] =  pd.to_datetime(df["Hour_End"])
    df["YEAR"] = [_.year for _ in df["DATE"]]
    df["MONTH"] = [_.month for _ in df["DATE"]]
    df["DAY"] = [_.day for _ in df["DATE"]]
    df["HOUR_ENDING"] = [int(np.ceil(_.hour + _.minute / 60)) for _ in df["DATE"]]
    
    # Reformatting column order to better view time info
    cols = df.columns.tolist()
    cols = cols[-5:] + cols[1:-5]
    df = df[cols]

    return df
