# Import Libraries
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark import SparkContext
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import *


# create spark session
def create_spark_session() -> SparkSession:
    """Get or create a spark session."""
    spark = SparkSession.builder.\
        config("spark.jars.repositories", "https://repos.spark-packages.org/").\
        config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11").\
        config("spark.driver.memory", "15g"). \
        appName('my-cool-app') .\
        enableHiveSupport().getOrCreate()

    return spark

# data cleansing functions
def remove_invalid_data(df):
    """
        Data cleansing with 2 steps:
        - drop columns with empty values >= 90% 
        - drop rows where all fields are empty
    """
    # show initial row number
    row_count_before = df.shape[0]
    print(f'Initial row number in dataframe: {row_count_before:,}')
    print("Drop missing data...")
    
    # step 1: drop columns with >=90% empty
    columns_to_drop = []
    
    # Scanning each columns' values for large df may return a OutOfMemory error.
    for column in df:
        values = df[column].unique()        
        # find columns with >= 90% empty values
        if (True in pd.isnull(values)):
            percentage_missing = 100 * pd.isnull(df[column]).sum() / df.shape[0]            
            if (percentage_missing >= 90):
                columns_to_drop.append(column)
    
    # show columns that have been dropped
    print("Columns dropped: ")
    print(columns_to_drop)
    df = df.drop(columns=columns_to_drop)
    
    # step 2: drop rows with all fields empty
    df = df.dropna(how='all')
    
    # show row number after cleaning
    row_count_after = df.shape[0]
    print(f'Row number after: {row_count_after:,}')
    
    print("Cleaning complete!")
    print("\n- - - - - - - -\n")
    
    return df


def drop_duplicate_rows(df, cols=[]):
    """
        Drop duplicate data within the dataframe.
    """
    print("Dropping duplicate rows...")
    row_count_before = df.shape[0]
    print("Initial row number: ")
    print(row_count_before)
    
    # drop duplicate rows
    df = df.drop_duplicates()
    
    row_count_after = df.shape[0]
    print(row_count_before - row_count_after)
    print(" rows dropped.")
    print("\n- - - - - - - - \n")
    
    return df


def remove_invalid_immigration(spark: SparkSession, df_spark):
    """
        Data cleansing with 2 steps:
        - drop columns with empty values >= 90% 
        - drop rows where all fields are empty
        return: the cleaned dataframe
    """
    # show initial row number
    row_count_before = df_spark.count()
    print(f'Initial row number in dataframe: {row_count_before:,}')
    print("Drop missing data...")
    
    # step 1: get columns with <90% empty which has been validated during EDA
    df_spark.createOrReplaceTempView("immigration_view")
    df_spark = spark.sql(
        """
        select
            cicid 
            ,i94yr 
            ,i94mon 
            ,i94cit 
            ,i94res 
            ,i94port 
            ,arrdate 
            ,i94mode 
            ,i94addr 
            ,depdate 
            ,i94bir 
            ,i94visa 
            ,count 
            ,dtadfile 
            ,visapost
            ,entdepa 
            ,entdepd 
            ,matflag 
            ,biryear 
            ,dtaddto 
            ,gender 
            ,airline 
            ,admnum 
            ,fltno 
            ,visatype 
        from immigration_view
        """
    )
    
    # step 2: drop rows with all fields empty
    df_spark = df_spark.dropna(how='all')
    
    row_count_after = df_spark.count()
    print(f'Row number after: {row_count_after:,}')
    
    print("Cleaning complete!")
    print("\n- - - - - - - - \n")
    return df_spark


def clean_country_mapping(spark, df_spark):
    """
        drop rows where country name contains 'INVALID', 'Collapsed', 'No Country Code'
    """
    # show columns that have been dropped
    row_count_before = df_spark.count()
    print(f'Initial row number in dataframe: {row_count_before:,}')
    print("Drop missing data...")
    
    # step 1: get rows whose country_name does not contain 'INVALID', 'Collapsed', 'No Country Code'
    df_spark.createOrReplaceTempView("mapping_view")
    df_spark = spark.sql(
        """
        select
            code, 
            country_name 
        from mapping_view
        where country_name not like 'INVALID%'
            and country_name not like 'Collapsed%'
            and country_name not like 'No Country Code%'
        """
    )
    
    # step 2: drop rows with all fields empty
    df_spark = df_spark.dropna(how='all')
    
    row_count_after = df_spark.count()
    print(f'Row number after: {row_count_after:,}')
    
    print("Cleaning complete!")
    print("\n- - - - - - - - \n")
    return df_spark




def data_quality_check(input_df, table_name):
    """
        Check data completeness by ensuring there are records in each table.
    """   
    input_count = input_df.count()
    print(input_count)
    if (input_count == 0):
        print("Data quality check failed for {} with zero records!".format(table_name))
    else:
        print("Data quality check passed for {} with record_count: {} records.".format(table_name, input_count))
        
#     return 0