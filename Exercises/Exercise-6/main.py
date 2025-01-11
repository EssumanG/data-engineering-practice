from datetime import timedelta
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, TimestampType, DoubleType
import zipfile
import os, glob
from pathlib import Path
from typing import Union
from pyspark.sql import functions as sp_func


def create_directory(directory: str):
    try:
        os.makedirs(directory, exist_ok=True)
        print(f'The directory {directory} has been created succesfully!')
    except FileExistsError:
        print(f"Directory {directory} already exist")


def unzip_file(file_path: str, dest_path: Union[str, None] = None):
    try:
        with zipfile.ZipFile(file_path, 'r') as zip_file:
            zip_file.extractall(dest_path)
            print(f"succesfully extracted files in {file_path}")
    except Exception as Error:
        print(f"Couldn't extract the zip file with file name, {file_path} \n Exception Error: {Error}")

def read_csv_file(spark: SparkSession, file_path: str, header: bool=True):
    if os.path.exists(file_path):
        try:
            df = spark.read.csv(file_path, header=header)
            return df
        except Exception as error:
            print(f"Couldn't read the file, {file_path}: {error}")
    else:
        print("File does not exist")
        return None

def get_files(directory: str, extension: Union[str,None] = None, recursive: bool=True):
    if os.path.exists(directory):
        try:
            list_files = []
            if extension:
                print('there is an extension')
                extension_pattern = directory+ "/**/*." + extension
                print(extension_pattern)
                list_files = glob.glob(extension_pattern, recursive=recursive)
            else:
                print("no extension provided")
                extension_pattern = directory + "/*"
                print(extension_pattern)
                list_files = glob.glob(extension_pattern, recursive=recursive)
                print(f"Print files, {list_files}")
            return list_files
        except Exception as error:
            print(f"An error occurred getting list of files: {error}")
            return None
    else:
        print("File does not exist")
        return None



csvSchema = StructType(
    [
        StructField("trip_id",IntegerType(),True),
        StructField("start_time",TimestampType(), True),
        StructField("end_time",TimestampType(), True),
        StructField("bikeid",IntegerType(), True),
        StructField("tripduration",StringType(), True),
        StructField("from_station_id",IntegerType(), True),
        StructField("from_station_name",StringType(), True),
        StructField("to_station_id",IntegerType(), True),
        StructField("to_station_name",StringType(), True),
        StructField("usertype",StringType(), True),
        StructField("gender",StringType(), True),
        StructField("birthyear",IntegerType(), True),
    ] )

def average_trip_duration_per_day(df):
    df_with_start_day_col = df.withColumn("start_day", sp_func.split(df.start_time," ")[0])
    df_group_by_start_day = df_with_start_day_col.groupBy("start_day").agg(
        sp_func.avg("tripduration").alias("avg_tripduration"),
        sp_func.count("trip_id").alias("trip_count"))
    # print(f"Grouped By Start_day:\n{df_group_by_start_day.show()}")
    df_group_by_start_day.write.csv("reports/average_trip_duration_per_day.csv", header=True)

def popular_starting_trip_station(df):
    df_with_start_month_col = df.withColumn("start_month", sp_func.date_format(df.start_time, "yyyy-MM"))
    df_group_by_start_month = df_with_start_month_col.groupBy("start_month", "from_station_name").agg(
        sp_func.count(df.trip_id).alias("count_trip")
    ).orderBy(sp_func.desc("count_trip"))
    df_group_by_start_month.write.csv("reports/popular_starting_trip_station.csv", header=True)
    # print(f"The Most Popular Starting Trip Station For each Month:\n{df_group_by_start_month.show()}")


def top_3_trip_stations_each_day_for_the_last_two_weeks(df):
    #Top 3 trip stations each day for the last two weeks
    max_date = df.select(sp_func.max(sp_func.date_format(df.start_time, "yyyy-MM-dd"))).alias('max_date').collect()[0][0]

    max_date_literal = sp_func.lit(max_date).cast("date")
    threshold_date_column =sp_func.date_sub(max_date_literal, 14)
    threshold_date = df.select(threshold_date_column.alias("threshold_date")).collect()[0][0]

    filtered_df =  df.filter(sp_func.to_date(df.start_time) >= sp_func.to_date(sp_func.lit(threshold_date))).groupBy(sp_func.date_format("start_time", "yyyy-MM-dd").alias("day"), "from_station_name").agg(
        sp_func.count("trip_id").alias("trip_count")
    ).orderBy(sp_func.desc("trip_count")).limit(10)
    print(f"filtered_df: {filtered_df.show()}")
    filtered_df.write.csv("reports/top_3_trip_stations_each_day_for_the_last_two_weeks.csv", header=True)


def gender_with_longer_avg_trip(df):
    df_group_by_gender =df.filter(df.gender.isNotNull()).groupBy("gender").agg(
        sp_func.count(df.trip_id).alias("count_trip"),
        sp_func.avg("tripduration")
    )
    # print(f"Gender with longer avg trip duration:\n{df_group_by_gender.show()}")
    df_group_by_gender.write.csv("reports/gender_with_longer_avg_trip.csv", header=True)


def top_10_ages_with_longer_trip(df):
    #filter those that include year of birth
    filtered_with_birthyear =df.filter(df.birthyear.isNotNull())
    #add a column that show their ages
    current_year = sp_func.year(sp_func.current_date())

    df_with_ages = filtered_with_birthyear.withColumn("age", current_year - df.birthyear)
    df_with_ages.show()

    longest_trip = df_with_ages.orderBy(sp_func.desc("tripduration")).limit(10)
    # shortest_trip = df_with_ages.orderBy(sp_func.asc("tripduration"))
    longest_trip.write.csv("reports/top_10_ages_with_longer_trip.csv", header=True)



    # print(f"longest_trip:")
    # print(longest_trip.show())


    
def top_10_ages_with_shotest_trip(df):
    #filter those that include year of birth
    filtered_with_birthyear =df.filter(df.birthyear.isNotNull())
    #add a column that show their ages
    current_year = sp_func.year(sp_func.current_date())

    df_with_ages = filtered_with_birthyear.withColumn("age", current_year - df.birthyear)
    df_with_ages.show()

    # longest_trip = df_with_ages.orderBy(sp_func.desc("tripduration"))
    shortest_trip = df_with_ages.orderBy(sp_func.asc("tripduration")).limit(10)
    # print(f"shortest_trip:")
    # print(shortest_trip.show())
    shortest_trip.write.csv("reports/top_10_ages_with_shotest_trip.csv", header=True)



def main():
    spark = SparkSession.builder.appName("Exercise6").enableHiveSupport().getOrCreate()
    # your code here
    base_dir = "data"
    lists = get_files(base_dir, "zip", recursive=True)
    print("helllo")

    create_directory("reports")
    for file in lists:
        unzip_file(file, base_dir)
    df = spark.read.csv("data/Divvy_Trips_2019_Q4.csv", header=True, schema=csvSchema)
    df = df.withColumn(
    "tripduration",
    sp_func.regexp_replace(df.tripduration, ",", "").cast(DoubleType())
)
    df.printSchema()

    print("Initial DataFrame:")
    print(df.show())


    average_trip_duration_per_day(df)
    top_3_trip_stations_each_day_for_the_last_two_weeks(df)


    #The Most Popular Starting Trip Station For each Month
    popular_starting_trip_station(df)
    #

    # #Gender with the longer average trip duration
    gender_with_longer_avg_trip(df)


    

    # Top 10 ages of those the tak the longest trips, and shortest?
    #longest trips
    top_10_ages_with_longer_trip(df)

    #shortest trips
    top_10_ages_with_shotest_trip(df)


if __name__ == "__main__":
    main()
