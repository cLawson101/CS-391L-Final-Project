CS 391L Machine Learning Final Project  
Electric Load Forecasting  

- Applied machine learning project in the area of electric load forecasting
- Focused on ERCOT


Files of interest - Cleaning:
- noaa.py
    - Provides functions to help with our first stage of cleaning our data
- eroct_wz_datasets.py
    - Generates the final parquet files necessary for us to run our modeling notebooks
- ercot.py
    - Provides functions to help with formatting the raw ercot data we have
    
Files of interest - Modeling:
- Linear Regression:
    - south_central_wz_mlr.ipynb
- Transformer:
    - south_central_wz_transformer-v2.ipynb
        - Contains the original base transformer to make sure it works
    - south_central_wz_nn_and_dl_exploration.ipynb
        - Testing different hypertuning parameters to see if we get better results
- Prophet:
    - south_central_wz_transformer-prophet.ipynb