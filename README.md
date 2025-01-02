# ucb-aiml-capstone-2



SUMMARY 

This is the UCB AIML Capstone Project for Mike Jones.

The overall aim of the project is to use data from the CAISO Electrical Energy Markets 
in order to do short term forecasts for two timeseries: 

    [1] the total electrical power production / consumtion in the system, as well as
    [2] the duck curve, which is side effect of increased use of solar and wind power.

The data is obtained from an API source for roughly six years of data that has been
refined and reduced to one record per hour of the two data sets: total laod and fuel 
mix, which are used in combination to compute The Duck Curve. 

The timeseries for Total Load and Duck Curve are analysed by first seperating out the
trend, seasonality and residue by using Seasonal Decomposition.
Then the trends are further analyzed by feeding them into the ARIMA model to produce
7-day forecasts. In order to determine which forecasts are more accurate, several 
ARIMA hyperparameters are tested in a manner similar to Grid Search and Cross Validation.
In the end the SARIMAX method is also used to produce forecasts for both Total Load and
Duck Curve. 

An accompanying paper describes all of this in more detail, along with a report in
excel and other images produced during the research.



DETAILS 


The following table gives an overview of the files in this project. 
```

NUM  FILE / DIRECTORY                           DESCRIPTION
===  =========================================  =======================================
  1  README.md                                  This file
  2  _report/ucb_aiml_capstone_final_paper.pdf  Final paper/report
  3  _report/rpt_0_caiso_load_duck_matrix.xlsx  Final Result data 
  4  ucb_aiml_caiso_capstone.ipynb              Notebook containing the code.
  5  _other                                     Directory with ancillary files. 
  6  _other/etl_0_caiso_files.py                ETL file for loading Database
  7  _other/data_0_prep_full.bsh                Bash script for reducing data size
  8  _other/data_processing.sql                 SQL script for data prep
  9  _other/ddl_0_caiso_db_setup.sql            SQL script for database setup
 10  _dat                                       Data Directory
 11  _dat/fuel_mix_20190101_20241003.csv        Fuel Mix data direct from the API
 12  _dat/load_full_20190101_20241003.csv       Load data direct from the API
 13  _dat/ucb_aiml_capstone_caiso.csv           Fuel and Load after data prep
 14  _out                                       Target directory for output files
 15  _out_bak_2024_12_26                        Directory containing reference output

```

The suggested order for getting started is to:

   [A] Begin by looking at [1] this Readme file to get oriented. 
   [B] Read through the files [2] Final Report and [3] Final Result Data.
   [C] Read through and run the [4] Jupyter Notebook. 
   [D] Look at the generated output in [14] _out directory.
   [E] Look at the previously generated output in [15] _out_bak_2024_12_26.
   [F] Read through other ancillary files to see details of data preparation and etc.









