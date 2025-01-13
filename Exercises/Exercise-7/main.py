from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window
import zipfile
import os
from typing import Union

#unzip the file


def create_directory(directory: str):
    try:
        os.makedirs(directory, exist_ok=True)
        print(f'The directory {directory} has been created succesfully!')
    except FileExistsError:
        print(f"Directory {directory} already exist")


def unzip_file(file_path: str, dest_path: Union[str ,None] = None):
    try:
        with zipfile.ZipFile(file_path, 'r') as zip_file:
            zip_file.extractall(dest_path)
            print(f"succesfully extracted files in {file_path}")
    except Exception as Error:
        print(f"Couldn't extract the zip file with file name, {file_path} \n Exception Error: {Error}")

def read_csv_file(spark: SparkSession, file_name: str):
    if os.path.exists(file_name):
       df =  spark.read.csv(file_name, header=True)
       return df
    else:
        print(f"File in not found: {file_name}")
        return None
#add file_name as the column
def create_filename_col(df):
    df = df.withColumn("source_file", F.input_file_name())
    df = df.withColumn("source_file", F.regexp_extract("source_file", r"[^/]+$", 0))
    return df

def extract_date_from_source_file(df):
    df = df.withColumn("file_date", F.to_date(F.regexp_extract("source_file", r"(\d{4}-\d{2}-\d{2})", 1)))
    return df
    
def add_brand_column(df):
    df = df.withColumn("brand",
    F.when(df.model.contains(" "), F.split(df.model, " ")[0])
    .otherwise(F.lit("unknown")))
    return df

def add_primary_key(df):
    return df.withColumn("primary_key", F.sha1(F.concat_ws("_", df.serial_number, df.failure, df.model)))

def main():
    spark = SparkSession.builder.appName("Exercise7").enableHiveSupport().getOrCreate()
    # your code here
    #unzip the file
    unzip_file("data/hard-drive-2022-01-01-failures.csv.zip", "data")

    df = read_csv_file(spark, "data/hard-drive-2022-01-01-failures.csv")

    #add file_name as a column
    df.printSchema()
    df_with_filename_col = create_filename_col(df)
    # df_with_filename_col.show(vertical=True)

    #extract the date forme source_file column to a date or timestamp data-type
    df_with_file_date = extract_date_from_source_file(df_with_filename_col)
    filtered_df_with_filedate = df_with_file_date.select("date", "serial_number", "model", "capacity_bytes", "failure","source_file", "file_date")
    # filtered_df_with_filedate.show()
   
    #add brand as a column
    df_with_brand_column = add_brand_column(filtered_df_with_filedate)
    df_with_brand_column.show()


    #add primary_key column
    df_with_pk = add_primary_key(df_with_brand_column)
    df_with_pk = df_with_pk.withColumn("capacity_bytes", df.capacity_bytes.cast("long"))


    # #select capacity_bytes and model from the dataframe
    capacity_bytes = df_with_pk.select("capacity_bytes", ).distinct().orderBy(F.desc("capacity_bytes"))
    window_spec = Window.orderBy(F.desc("capacity_bytes"))
    ranked_df_2 = capacity_bytes.withColumn("storage_ranking",F.dense_rank().over(window_spec))
    

    final_df = df_with_pk.join(ranked_df_2,
                               df_with_pk.capacity_bytes == ranked_df_2.capacity_bytes, "inner").drop(df_with_pk['capacity_bytes'])
                            

    create_directory('report')

    final_df.write.csv("report/analysis.csv",header=True)






if __name__ == "__main__":
    main()
