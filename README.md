# Data Processing with PySpark and PostgreSQL

This project demonstrates a complete data pipeline using PySpark, PostgreSQL, and Docker. The pipeline involves downloading, normalizing, manipulating, and denormalizing data using PySpark DataFrame APIs and PostgreSQL as the data storage.

## Project Overview

The project is divided into four main scripts:

1. **`landing2.py`**: Downloads a CSV file from a public URL and loads it into a PostgreSQL database.
2. **`normalize.py`**: Transforms the denormalized data from the PostgreSQL database into a normalized format.
3. **`produce-formula.py`**: Generates a new data dimension based on a user-defined formula and appends it to the normalized data.
4. **`denormalize.py`**: Denormalizes the data back into a wider table based on selected columns and dimensions, storing the result in a new PostgreSQL table.

## Prerequisites

- **Docker** installed and running
- **Python 3.x**
- **PySpark** installed
- **PostgreSQL JDBC Driver** (e.g., `postgresql-42.7.4.jar`)

## Project Structure/directory

├── docker-compose.yml       # Docker setup for Spark and PostgreSQL
├── data/                    # Data directory (mounted in Docker)
├── scripts/                 # PySpark scripts
│   ├── landing2.py
│   ├── normalize.py
│   ├── produce-formula.py
│   └── denormalize.py
└── extra-jars/                    # Directory for storing JDBC driver
    └── postgresql-42.7.4.jar


## Setup Instructions

### 1. Ensure JDBC Driver and scripts Placement
Place the postgresql-42.7.4.jar driver in the extra-jars/ directory to enable PySpark to connect to the PostgreSQL database.
Place the scripts (e.g landing2.py) in the scripts directory

### 2. Start the Docker Containers

Ensure you have the docker-compose.yml file configured by using the following line in the command prompt (cmd):
docker-compose -f docker-compose.yml up -d

The `docker-compose.yml` file is used to set up and run the Spark and PostgreSQL services in separate containers for easy management. Here's an overview of its purpose and configuration:
- Spark Master and Workers: Configured for running PySpark scripts. The Spark master coordinates the cluster, while the workers handle job execution.
- PostgreSQL Database: Used as the data storage for the pipeline. The database is initialized within a container and can be accessed through JDBC.

## Explanation of the scripts

### landing2.py
Downloads data from a CSV URL.
Cleans column names and loads the data into PostgreSQL.

### normalize.py
Reads data from the PostgreSQL table.
Transforms the data into a normalized format with columns: time, entity, dimension, and dimension_value.

### produce-formula.py
Accepts user-defined dimensions and a formula.
Creates a new dimension based on the formula and appends it to the normalized data.

### denormalize.py
Denormalizes the data into a wider format by pivoting selected dimensions.
Saves the output to a new PostgreSQL table.


## Example of how to run each script in the command prompt.

### for the script landing2.py: 
docker exec spark-master python /opt/bitnami/spark/scripts/landing2.py

### for the script normalize.py: 
docker exec spark-master spark-submit --master spark://spark-master:7077 /opt/bitnami/spark/scripts/normalize.py

### for the script produce-formula.py (have in mind that the you can change the parameters here if you want. For example the formula "item_price / net_mass"): 
docker exec spark-master spark-submit --master spark://spark-master:7077 --jars /opt/bitnami/spark/extra-jars/postgresql-42.7.4.jar /opt/bitnami/spark/scripts/produce-formula.py --dimensions net_mass item_price --formula "item_price / net_mass" --new_dimension_name "Price_per_Unit_Mass"

### for the script denormalize.py:
docker exec spark-master spark-submit --master spark://spark-master:7077 --jars /opt/bitnami/spark/extra-jars/postgresql-42.7.4.jar /opt/bitnami/spark/scripts/denormalize.py --selected_columns time entity --dimensions net_mass item_price price_per_unit_mass country_of_origin --new_table_name final_denormalized_data


## Example Data
The dataset used in this project is a public customs declaration dataset:
URL: [https://www.sefidian.com/2023/09/12/run-spark-submit-for-apache-spark-pyspark-using-docker/](https://raw.githubusercontent.com/Seondong/Customs-Declaration-Datasets/refs/heads/en/data/df_syn_eng.csv)
