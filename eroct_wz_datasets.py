"""
Assemble datasets for each relvant ERCOT Weather Zone (WZ)
"""

import numpy as np
import pandas as pd
from noaa import climate_data_etl
from ercot import ercot_data

# Load NOAA climate data

noaa_base_path = f"{__file__.split('CS-391L-Final-Project')[0]}/CS-391L-Final-Project/data/noaa/"

noaa_kaus = climate_data_etl(path=f"{noaa_base_path}3297764.csv")
noaa_kiah = climate_data_etl(path=f"{noaa_base_path}3297770.csv")
noaa_kdfw = climate_data_etl(path=f"{noaa_base_path}3298637.csv")
noaa_ksat = climate_data_etl(path=f"{noaa_base_path}3298642.csv")
noaa_khrl = climate_data_etl(path=f"{noaa_base_path}3298648.csv")
noaa_kmaf = climate_data_etl(path=f"{noaa_base_path}3298651.csv")
noaa_kama = climate_data_etl(path=f"{noaa_base_path}3298653.csv")

noaa_tx_data = pd.concat([
    noaa_kaus, 
    noaa_kiah,
    noaa_kdfw,
    noaa_ksat,
    noaa_khrl,
    noaa_kmaf, 
    noaa_kama 
])

# Load ERCOT load data

ercot_base_path = f"{__file__.split('CS-391L-Final-Project')[0]}/CS-391L-Final-Project/data/ERCOT/"

ercot_tx_years = {
    ercot_data(path=f"{ercot_base_path}Native_Load_{year}.xlsx")
    for year in np.arange(2012, 2023)
}
eroct_tx_data = pd.concat([ercot_tx_years.values()])

# Assemble a dataset for South Central WZ
