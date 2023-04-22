# Project Summary

The project is designed to create an ETL pipeline that will load cleaned data, and build a start schema model for better analysis. Apart from I94 immigration and US city demographic data provided by Udacity, a small dataset of global Gini index is introduced to see if there is an obivious trend fueled by income equality in the source countries of the US immigration.

The output tables are stored in the folder and the code was tested in the workspace using Submission.ipnb file.


# Data sources

This project will gather the data from 3 datasets.

- I94 immigration data comes from the US National Tourism and Trade Office. It consists of facts of applications into the US. This dataset will be the source of the fact table and dimension tables such as country and visa types.

- US city demographic data serves as the source of the US state dimension table.

- World Gini index measures the degree of income inequality. Its value ranges from 0, indicating perfect equality (where everyone receives an equal share), to 1, perfect inequality. This dataset was downloaded from https://www.kaggle.com/datasets/mannmann2/world-income-inequality-database with source being The World Income Inequality Database (WIID) which is maintained by the United Nations University-World Institute for Development Economics Research (UNU-WIDER).


# Model

Please refer to the model_view file in the folder for the conceptual model.

### Four dimension tables and two fact tables comprise of the model.

#### Dimension tables

The following three dimension tables are from the I94 Immigration Data dataset:

 - dim_date: It contains all possible dates from the columns in the source dataset and links to the fact_immigration table via column arrdate.
 
 - dim_vasa_type: links to the fact_immigration table via column visa_type_code.
 
 - dim_from_country: The name of the country was mapped into the dimension table using the i94 SAS country list in the workspace. This dataset indicates the origin of the immigration at country level and links to the fact_immigration table via column country_code.
 
US state dimension comes from the U.S. City Demographic Data dataset:

 - dim_us_state: Since the fact data is at state level, the dimension table keeps properties only state level, such as population, age, etc.. It links to the fact_immigration table via column state_code. 

#### Fact tables

 - fact_immigration: comes from the I94 Immigration Data dataset and represent the applications received by the US National Tourism and Trade Office.
 
 - fact_gini_index: contains the gini index by year by country. links to dim_date via year and to dim_from_country via country_name.
 
 
# ETL Steps

- load data into staging dataframes
- clean the raw data
- stage the data and write the output into Fact & Dimension tables which is in parquet files stored in a table folder in this workspace
- perform a quality check


# Files submitted

1. submission.ipynb displays the process of ETL.
2. create_tables.py in tools folder contains functions that stage and write data into parquet files.
3. data_cleaning.py in tools folder includes functions that clean the data, such as dropping rows with all columns empty and removing columns that has 90% value empty.
4. data distionary.
5. Folder tables is where all parquet files are stored.
6. README.md outlines the project contents and discussion on the project.


# Discussion

* The technologies chosen in this project are Apache Spark and Pandas.
* With sufficient resource bandwidth, the data is suggested to be updated daily in order to keep it up to date, apart from the Gini index data which should be updated annualy. Also if data analysts do not need a daily refresh it can also be updated monthly. 
* How to approach the problem differently under the following scenarios:
   - The data was increased by 100x
   I would use more powerful tools to store and process the data such as AWS redshift,AWS EMR and S3 bukets. Also the data should be loaded incrementally instead of overwritten the full table. With respect of table design, the fact data needs to be partitioned.
   - The data populates a dashboard that must be updated on a daily basis by 7am every day.
The database needed to be accessed by 100+ people. 
   The ETL pipeline has to be automated and easy to be debugged. Thus I would adopt the Apache Airflow to ensure that the pipeline will run on accurately schedules and can be monitored on smaller task basis. It is also easy for collaboration.
   If the databse need to be accessed by 100+ people at the same time, I would suggest migrate it to Amazon redshift cluster which offers fast query performance.