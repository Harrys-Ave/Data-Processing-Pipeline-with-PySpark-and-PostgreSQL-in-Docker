from pyspark.sql import SparkSession
import os
import requests
import re
from pyspark.sql.functions import col

# Paths
CSV_URL = "https://raw.githubusercontent.com/Seondong/Customs-Declaration-Datasets/refs/heads/en/data/df_syn_eng.csv"
CSV_FILE_NAME = "df_syn_eng.csv"
CSV_FILE_PATH = f"/data/{CSV_FILE_NAME}" 

# Database connection properties
jdbcHostname = "db"  # Docker service name for PostgreSQL
jdbcPort = 5432      # PostgreSQL's port within Docker network
jdbcDatabase = "assignment"
jdbcUrl = f"jdbc:postgresql://{jdbcHostname}:{jdbcPort}/{jdbcDatabase}" # Java API
connectionProperties = {
    "user": "harryave",
    "password": "1234assignment",
    "driver": "org.postgresql.Driver"
}

def download_csv():
    # Download the CSV file and save it
    response = requests.get(CSV_URL)
    response.raise_for_status() #for error checking
    with open(CSV_FILE_PATH, 'wb') as file:
        file.write(response.content)
    print(f"File downloaded successfully and saved to {CSV_FILE_PATH}")

#cleaning the data a little bit
def clean_column_name(col_name):
    # Replace spaces with underscores and remove special characters
    col_name = col_name.replace(" ", "_")
    col_name = re.sub(r'\W+', '', col_name)
    return col_name

def load_data_to_postgresql():
    # SparkSession
    #specifying the name of the app and checks if a spark session already exists
    spark = SparkSession.builder \
        .appName("Landing Data to PostgreSQL") \
        .getOrCreate()

    # Make the CSV file a DataFrame
    df = spark.read.option("header", "true").csv(CSV_FILE_PATH)

    # Clean column names
    new_column_names = [clean_column_name(col_name) for col_name in df.columns]
    df = df.toDF(*new_column_names) # unpacking the list of new column names

    # Show the DataFrame schema
    df.printSchema()

    # Write DataFrame to PostgreSQL using java API
    df.write \
        .format("jdbc") \
        .option("url", jdbcUrl) \
        .option("dbtable", "public.customs_data") \
        .option("user", connectionProperties["user"]) \
        .option("password", connectionProperties["password"]) \
        .option("driver", connectionProperties["driver"]) \
        .mode("overwrite") \
        .save()

    print("Data has been successfully written to PostgreSQL.")

    # Stop the SparkSession
    spark.stop()

if __name__ == "__main__":
    download_csv()
    load_data_to_postgresql()
