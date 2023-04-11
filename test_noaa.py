"""
Test loading NOAA climate data from .csv files in the repository
"""

import pandas as pd
from noaa import climate_data_etl


kaus = climate_data_etl(path=f"{__file__.split('CS-391L-Final-Project')[0]}/CS-391L-Final-Project/data/noaa/3297764.csv")
kiah = climate_data_etl(path=f"{__file__.split('CS-391L-Final-Project')[0]}/CS-391L-Final-Project/data/noaa/3297770.csv")

tx_data = pd.concat([
    kaus, 
    kiah
])
