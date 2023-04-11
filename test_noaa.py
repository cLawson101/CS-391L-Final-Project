"""
Test loading NOAA climate data from .csv files in the repository
"""

import pandas as pd
from noaa import climate_data_etl


base_path = f"{__file__.split('CS-391L-Final-Project')[0]}/CS-391L-Final-Project/data/noaa/"

kaus = climate_data_etl(path=f"{base_path}3297764.csv")
kiah = climate_data_etl(path=f"{base_path}3297770.csv")
kdfw = climate_data_etl(path=f"{base_path}3298637.csv")
ksat = climate_data_etl(path=f"{base_path}3298642.csv")
khrl = climate_data_etl(path=f"{base_path}3298648.csv")
kmaf = climate_data_etl(path=f"{base_path}3298651.csv")
kama = climate_data_etl(path=f"{base_path}3298653.csv")

tx_data = pd.concat([
    kaus, 
    kiah,
    kdfw,
    ksat,
    khrl,
    kmaf, 
    kama 
])
