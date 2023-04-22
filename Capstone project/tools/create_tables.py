# Import Libraries
import pandas as pd
import configparser
import os
from datetime import datetime, timedelta

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark import SparkContext
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import types as T


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
    
    
# create dimension tables
def create_dim_us_state(spark, input_df, output_path):
    """ creates a US state dimension table from the US cities demographics data.
    :param input_df: dataframe of US cities
    :param output_path: path to write dimension dataframe to
    :return: overwrite dim data as spark dataframe
    """
    table_name = "dim_us_state"

    # get columns needed from the US cities demographics data
    input_df.createOrReplaceTempView("state_view")
    
    df_spark = spark.sql(
        """
        select
             State as state,
             `State Code` as state_code,
             avg(`Median Age`) as median_age,
             avg(`Total Population`) as total_population,
             avg(`Average Household Size`) as average_household_size             
        from state_view
        group by State, `State Code`
        """
    )
    # add id
    df_spark = df_spark.withColumn('id', monotonically_increasing_id())

    # write dimension to parquet file    
    write_to_parquet(df_spark, output_path, table_name)

    return df_spark


def create_dim_from_country(spark, input_df, mapping_df, output_path):
    """ creates country of residence dimension table from the immigration data.
    :param input_df: spark dataframe of immigration events
    :param output_path: path to write dimension dataframe to
    :return: overwrite dim data as spark dataframe
    """
    table_name = "dim_from_country"
    
    # get residence country from i94res column
    df = input_df.select(['i94res']).distinct()
    df = df.withColumnRenamed('i94res', 'country_code')
    
    df.createOrReplaceTempView("immigration_view")
    mapping_df.createOrReplaceTempView("mapping_view")
    
    df = spark.sql(
        """
        select
             immi.country_code,
             m.country_name             
        from immigration_view immi
        left join mapping_view m on
            immi.country_code = m.code
        
        """
    )
    # add id
    df = df.withColumn('id', monotonically_increasing_id())

    # write dimension to parquet file    
    write_to_parquet(df, output_path, table_name)

    return df


def create_dim_visa_type(input_df, output_path):
    """ creates a visa type dimension table from the immigration data.
    :param input_df: spark dataframe of immigration events
    :param output_path: path to write dimension dataframe to
    :return: overwrite dim data as spark dataframe
    """
    table_name = "dim_visa_type"
    
    # get visatype from visatype column
    df = input_df.select(['visatype','i94visa']).distinct()
    df = df.withColumnRenamed('visatype', 'visa_type')\
            .withColumnRenamed('i94visa','visa_type_code')
    
    # add an id column
    df = df.withColumn('id', monotonically_increasing_id())

    # write dimension to parquet file    
    write_to_parquet(df, output_path, table_name)

    return df


def create_dim_date(input_df, output_path):
    """ creates a time dimension table from the immigration data.
    :param input_df: spark dataframe of immigration events
    :param output_path: path to write dimension dataframe to
    :return: overwrite dim data as spark dataframe
    """
    table_name = "dim_date"

    # convert sas date
    def convert_datetime(x):
        try:
            start = datetime(1960, 1, 1)
            return start + timedelta(days=int(x))
        except:
            return None
    
    udf_datetime_from_sas = udf(lambda x: convert_datetime(x), DateType())
    
    df_spark = input_df.select(["arrdate"])\
                .withColumn("arrival_date", udf_datetime_from_sas("arrdate")) \
                .withColumn('day', F.dayofmonth('arrival_date')) \
                .withColumn('month', F.month('arrival_date')) \
                .withColumn('year', F.year('arrival_date')) \
                .withColumn('week', F.weekofyear('arrival_date')) \
                .withColumn('weekday', F.dayofweek('arrival_date'))\
                .select(["arrdate", "arrival_date", "day", "month", "year", "week", "weekday"])\
                .dropDuplicates(["arrdate"])
    
    # add id
    df_spark = df_spark.withColumn('id', monotonically_increasing_id())

    # write dimension to parquet file    
    write_to_parquet(df_spark, output_path, table_name)

    return df_spark


def create_fact_immigration(spark, input_df, output_path):
    """ creates fact table from the immigration data.
    :param input_df: spark dataframe of immigration events
    :param output_path: path to write dimension dataframe to
    :return: overwrite fact data as spark dataframe
    """
    table_name = "fact_immigration"

     # get columns needed from the US cities demographics data
    input_df.createOrReplaceTempView("immigration_view")
    
    df_spark = spark.sql(
        """
        select
             cicid as file_id,
             i94res as residence_country_code,
             i94cit as birth_country_code,
             i94port as admission_port_code,
             arrdate,
             i94addr as arrival_state_code,
             depdate as departure_date_sas,
             i94bir as applicant_age,
             biryear as applicant_birth_year,
             gender as applicant_gender,
             i94visa as visa_type_code,
             entdepa as arrival_flag,
             entdepd as departure_flag,
             airline,
             admnum as admission_number,
             fltno as flight_number            
        from immigration_view
        """
    )
    # add id
    df_spark = df_spark.withColumn('id', monotonically_increasing_id())

    # write dimension to parquet file    
    write_to_parquet(df_spark, output_path, table_name)

    return df_spark


def create_fact_gini_index(spark, input_df, output_path):
    """ creates fact table from the Gini index data.
    :param input_df: spark dataframe of gini index csv
    :param output_path: path to write fact dataframe to
    :return: overwrite dim data as spark dataframe
    """
    table_name = "fact_gini_index"
    
    # get visatype from visatype column
    input_df.createOrReplaceTempView("gini_view")
    
    df = spark.sql(
        """
        select
            distinct UPPER(country_name) AS country_name, year, value as index_value
        from gini_view
        """
    )
    
    # add an id column
    df = df.withColumn('id', monotonically_increasing_id())

    # write dimension to parquet file    
    write_to_parquet(df, output_path, table_name)

    return df


def write_to_parquet(df, output_path, table_name):
    """
        Writes the dataframe to a parquet file.  
        :param output_path: where data is write to, usually /table
        :param table_name: name of the output table
    """
    
    file_path = output_path + table_name    
    print("Writing table {} to {}".format(table_name, file_path))
    
    df.write.mode("overwrite").parquet(file_path)
    
    print("Write complete!")
    print("\n- - - - - - - - -\n")
