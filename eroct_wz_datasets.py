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

noaa_select_attributes = ['DEW_POINT_TEMPERATURE', 'DRY_BULB_TEMPERATURE', 'PRECIPITATION', 
    'PRESSURE_CHANGE', 'PRESSURE_TENDENCY', 'RELATIVE_HUMIDITY']
noaa_select_attributes_plus = noaa_select_attributes + ['YEAR', 'MONTH', 'DAY', 'HOUR_ENDING']

# Load ERCOT load data

ercot_base_path = f"{__file__.split('CS-391L-Final-Project')[0]}/CS-391L-Final-Project/data/ERCOT/"

ercot_tx_years_first = [
    ercot_data(path=f"{ercot_base_path}Native_Load_{year}.xls")
    for year in np.arange(2012, 2016)
]

ercot_tx_years_second = [
    ercot_data(path=f"{ercot_base_path}Native_Load_{year}.xlsx")
    for year in np.arange(2016, 2023)
]

ercot_tx_years_total = ercot_tx_years_first + ercot_tx_years_second

eroct_tx_data = pd.concat(ercot_tx_years_total, sort=True)

# Assemble a dataset for South Central WZ

south_central_wz = eroct_tx_data[['YEAR', 'MONTH', 'DAY', 'HOUR_ENDING', 'SOUTH_C']].merge(
    noaa_kaus[noaa_select_attributes_plus].rename({_: f"{_}_KAUS" for _ in noaa_select_attributes}, axis=1), 
    how='inner', on=['YEAR', 'MONTH', 'DAY', 'HOUR_ENDING']
)
south_central_wz = south_central_wz.merge(
    noaa_ksat[noaa_select_attributes_plus].rename({_: f"{_}_KSAT" for _ in noaa_select_attributes}, axis=1), 
    how='inner', on=['YEAR', 'MONTH', 'DAY', 'HOUR_ENDING']
)

# Assemble a dataset for North WZ

north_wz = eroct_tx_data[['YEAR', 'MONTH', 'DAY', 'HOUR_ENDING', 'NORTH']].merge(
    noaa_kama[noaa_select_attributes_plus].rename({_: f"{_}_KAMA" for _ in noaa_select_attributes}, axis=1), 
    how='inner', on=['YEAR', 'MONTH', 'DAY', 'HOUR_ENDING']
)

# Assemble a dataset for North Central WZ

north_central_wz = eroct_tx_data[['YEAR', 'MONTH', 'DAY', 'HOUR_ENDING', 'NCENT']].merge(
    noaa_kdfw[noaa_select_attributes_plus].rename({_: f"{_}_kdfw" for _ in noaa_select_attributes}, axis=1), 
    how='inner', on=['YEAR', 'MONTH', 'DAY', 'HOUR_ENDING']
)

# Assemble a dataset for Central WZ
    
coast_wz = eroct_tx_data[['YEAR', 'MONTH', 'DAY', 'HOUR_ENDING', 'COAST']].merge(
    noaa_kiah[noaa_select_attributes_plus].rename({_: f"{_}_KIAH" for _ in noaa_select_attributes}, axis=1), 
    how='inner', on=['YEAR', 'MONTH', 'DAY', 'HOUR_ENDING']
)

# Assemble a dataset for Far West WZ
    
far_west_wz = eroct_tx_data[['YEAR', 'MONTH', 'DAY', 'HOUR_ENDING', 'FWEST']].merge(
    noaa_kmaf[noaa_select_attributes_plus].rename({_: f"{_}_KMAF" for _ in noaa_select_attributes}, axis=1), 
    how='inner', on=['YEAR', 'MONTH', 'DAY', 'HOUR_ENDING']
)

# Assemble a dataset for South WZ
    
south_wz = eroct_tx_data[['YEAR', 'MONTH', 'DAY', 'HOUR_ENDING', 'SOUTH']].merge(
    noaa_khrl[noaa_select_attributes_plus].rename({_: f"{_}_KHRL" for _ in noaa_select_attributes}, axis=1), 
    how='inner', on=['YEAR', 'MONTH', 'DAY', 'HOUR_ENDING']
)
