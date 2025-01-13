import polars as pl
import os




def read_csv_data(source_file: str) -> pl.DataFrame:
    """
    Reads data from the given csv file_path and return a polars DataFrame.

    Args: 
        source_file (str): The path of the csv file.

    Returns:
        pl.DataFrame: The extracted data in a polars DataFrame form 
    """

    #check if the source_file path exists
    if os.path.exists(source_file):
         #Read the csv file with its corresponding schema
        schema = {
    "ride_id": pl.String,
    "rideable_type": pl.String,
    "started_at": pl.Datetime,
    "ended_at": pl.Datetime,
    "start_station_name":pl.String,
    "start_station_id": pl.String,
    "end_station_name": pl.String,
    "end_station_id": pl.String,
    "start_lat": pl.Decimal(scale=2),
    "start_lng": pl.Decimal(scale=2),
    "end_lat": pl.Decimal(scale=2), 
    "end_lng": pl.Decimal(scale=2),
    "member_casual": pl.String,
}
    df = pl.read_csv(source=source_file, has_header=True, schema=schema)

    return df

def get_bike_rides_per_day(df: pl.DataFrame) -> pl.DataFrame:
    """
    Calculates the number of bikes rides per day from the given dataframe.

    `### Args`:
        df (pl.DataFrame): The DataFrame containing the bike rides data
    
    `### Returns`:
        pl.DataFrame: The Daframe with the number of bikes rides per day
    """

    #Add new column `start_date` the contain the date part of the started_at column
    df = df.with_columns(pl.col("started_at").dt.date().alias("start_date") )
    daily_rides = df.group_by("start_date").agg(
        pl.count("ride_id").alias("num_bike").cast(pl.Int16)
    )

    return daily_rides

def get_aggrerates_per_week(df: pl.DataFrame) -> pl.DataFrame:
    """
    Calculates the aggregates(mean, min, max) of number of rides per week.

    `### Args`:
        df (pl.DataFrame): The DataFrame containing the bike rides data
    
    `### Returns`:
        pl.DataFrame: The Daframe calculating the aggregates
    """
    #Add new column `week_num` the contain the week number of the started_at column
    weekly_rides = df.with_columns(pl.col("started_at").dt.week().alias("week_num") )
    df = weekly_rides.group_by("week_num").agg(pl.count("ride_id").alias("num_rides"))

    aggregated_week_rides = df.select(
        pl.mean("num_rides").alias("avg_ride"),
        pl.max("num_rides").alias("max_ride"),
        pl.min("num_rides").alias("min_ride"),
    )

    return aggregated_week_rides


def diff_last_week_rides(df: pl.DataFrame) -> pl.DataFrame:
    """
    Calculates how many rides that day is above or below the same day last week.

    `### Args`:
        df (pl.DataFrame): The DataFrame containing the bike rides data
    
    `### Returns`:
        pl.DataFrame: The DataFrame with the number of rides, the number of rides from the previous week, and their difference.
    """
    daily_rides = df.with_columns(
    pl.col("num_bike").shift(7).alias("rides_last_week")    
    )
    
    daily_rides = daily_rides.fill_null(0)
    
    diff_rides = daily_rides.with_columns(
    pl.col("num_bike").sub("rides_last_week").alias("diff")
    )
    return diff_rides

def main():
    source_file = "data/202306-divvy-tripdata.csv"
    df = read_csv_data(source_file)
    daily_rides = get_bike_rides_per_day(df)
    aggregated_weekly_rides = get_aggrerates_per_week(df)
    diff_weekly_rides = diff_last_week_rides(daily_rides)    
    print(f"Daily Rides:\n{daily_rides}")
    print(f"Aggregated Weekly Rides:\n{aggregated_weekly_rides}")
    print(f"Difference Weekly Rides:\n{diff_weekly_rides}")

    
    
if __name__ == "__main__":
    main()
