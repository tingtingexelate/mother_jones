This directory contains python scripts for two grouped insights of Mother Jones users:
1. Overall summary of MoJo donors, subscribers and newsletter readers.
2. User behavior comparison

All the Python scripts were written in Python 3.

Before running any of the scripts, please:

1. install dependencies by running $pip install -r requirements.txt

2. prepare the 'data' folder.

(1) 'data' folder should already be cloned from git repo along with this directory.
'data' folder should contain three existed files:
  - ICN FULFILLMENT SOURCE LIST.csv
  - lda20_results.csv
  - state_population.csv

(2) Manually download the following raw files and folders to 'data' folder:
  - DON_datakind.csv
  - SDN_datakind.csv
  - MJN_datakind.csv
  - motherjones_clicks_2017_datakind/
  - motherjones_clicks_2018_datakind/
  Please note that these are large files that should not be checked into git repo.


  Once the 'data' folder is prepared, first run data_preprocessing.ipynb. It will access
  the raw data, clean and reformat them, and tehn save the processed data as csv files 
  in the 'data' folder:
    - email_clean_2017_18.csv
    - DON_datakind_filtered.csv
    - SDN_datakind_filtered.csv
    - MJN_datakind_filtered.csv


Finally, run user_exploration_grouped_insights_1.ipynb to get detailed user insights
