from pyspark.sql import SparkSession
from pyspark.sql.functions import col, first, lit, lower
import argparse

def main():
    # Parse Command-Line Arguments
    parser = argparse.ArgumentParser(description='Denormalize data by selecting a subset of columns and pivoting dimensions.')
    parser.add_argument('--selected_columns', nargs='+', required=True, 
                        help='List of columns to include in the denormalized table (e.g., time entity)')
    parser.add_argument('--dimensions', nargs='+', required=True, 
                        help='List of dimensions to pivot into columns (e.g., net_mass item_price)')
    parser.add_argument('--new_table_name', required=True, 
                        help='Name of the new denormalized table to create in PostgreSQL')

    args = parser.parse_args()

    selected_columns = args.selected_columns
    selected_dimensions = [dim.lower() for dim in args.dimensions]  # lowercase for consistency
    new_table_name = args.new_table_name


    # SparkSession
    spark = SparkSession.builder \
        .appName("DenormalizeData") \
        .config("spark.jars", "/opt/bitnami/spark/extra-jars/postgresql-42.7.4.jar") \
        .getOrCreate()


    # JDBC Connection Properties
    jdbc_url = "jdbc:postgresql://db:5432/assignment"
    connection_properties = {
        "user": "harryave",
        "password": "1234assignment",
        "driver": "org.postgresql.Driver"
    }


    # Read Data from PostgreSQL
    df_normalized = spark.read.jdbc(
        url=jdbc_url,
        table="normalized_data",
        properties=connection_properties
    )


    # Handle Case Sensitivity
    # Convert dimension names in the DataFrame to lowercase
    df_normalized = df_normalized.withColumn('dimension', lower(col('dimension')))


    # Filter for Selected Dimensions
    df_selected = df_normalized.filter(col('dimension').isin(selected_dimensions))


    # Pivot the Data
    # We transform the DataFrame by grouping it based on selected_columns (for example time and entity)
    df_pivot = df_selected.groupBy(selected_columns).pivot('dimension').agg(first('dimension_value'))


    #------------------------------------------- debuging ------------------------------------------------

    # Select Subset of Columns
    # To be sure that selected_columns are present
    for column in selected_columns:
        if column not in df_pivot.columns:
            raise ValueError(f"Selected column '{column}' is not present in the data.")

    # Check if pivoted dimensions exist as columns
    for dim in selected_dimensions:
        if dim not in df_pivot.columns:
            print(f"Warning: Dimension '{dim}' not found in the data. It will be filled with nulls.")
            df_pivot = df_pivot.withColumn(dim, lit(None).cast("double"))

    # Selecting columns and the pivoted dimensions
    final_columns = selected_columns + selected_dimensions
    df_final = df_pivot.select(*final_columns)

    #------------------------------------------- debuging ------------------------------------------------



    # Write Data Back to PostgreSQL
    # Write the denormalized data to a new table in PostgreSQL
    df_final.write.jdbc(
        url=jdbc_url,
        table=new_table_name,
        mode="overwrite", 
        properties=connection_properties
    )

    # Stop SparkSession
    spark.stop()


"""
For this script, I used the following command-line arguments:
docker exec spark-master spark-submit --master spark://spark-master:7077 --jars /opt/bitnami/spark/extra-jars/postgresql-42.7.4.jar /opt/bitnami/spark/scripts/denormalize.py --selected_columns time entity --dimensions net_mass item_price price_per_unit_mass country_of_origin --new_table_name final_denormalized_data

"""


if __name__ == "__main__":
    main()
