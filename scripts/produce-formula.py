from pyspark.sql import SparkSession
from pyspark.sql.functions import col, first, lit, expr, lower
import argparse

# Parse command-line arguments
parser = argparse.ArgumentParser(description='Produce a new dimension based on a formula.')
parser.add_argument('--dimensions', nargs='+', required=True, help='List of dimensions to use in the formula')
parser.add_argument('--formula', required=True, help='The formula to calculate the new dimension')
parser.add_argument('--new_dimension_name', required=True, help='The name of the new dimension')

args = parser.parse_args()

# Convert selected_dimensions to lower case
selected_dimensions = [dim.lower() for dim in args.dimensions]
formula = args.formula.lower()
new_dimension_name = args.new_dimension_name

"""
By using argparse we can change the parameters fromt the command line. For example we can change the formula of the new dimension.

For example I used the following in the command line to run the script: 
docker exec spark-master spark-submit --master spark://spark-master:7077 --jars /opt/bitnami/spark/extra-jars/postgresql-42.7.4.jar /opt/bitnami/spark/scripts/produce-formula.py --dimensions net_mass item_price --formula "item_price / net_mass" --new_dimension_name "Price_per_Unit_Mass"

"""

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("ProduceFormula") \
    .config("spark.jars", "/opt/bitnami/spark/extra-jars/postgresql-42.7.4.jar") \
    .getOrCreate()

# JDBC connection properties
jdbc_url = "jdbc:postgresql://db:5432/assignment"
connection_properties = {
    "user": "harryave",
    "password": "1234assignment",
    "driver": "org.postgresql.Driver"
}

# Read data from the normalized_data table
df_normalized = spark.read.jdbc(
    url=jdbc_url,
    table="normalized_data",
    properties=connection_properties
)

# Convert dimension names to lower case
df_normalized = df_normalized.withColumn('dimension', lower(col('dimension')))

# List all distinct dimensions
dimensions_df = df_normalized.select('dimension').distinct()
dimensions_list = [row['dimension'] for row in dimensions_df.collect()]
print("Available dimensions:")
print(dimensions_list)

# Filter for the selected dimensions
df_selected = df_normalized.filter(col('dimension').isin(selected_dimensions))



# ---------------The following are for visualization and for checking that everything is okay.------------------
# Show distinct dimensions after filtering
print("Distinct dimensions after filtering:")
df_selected.select('dimension').distinct().show()
# Show sample data from df_selected
print("Sample data from df_selected:")
df_selected.show(5)
#---------------------------------------------------------------------------------------------------------------


# Change the data to have dimensions as columns
# unique values of the selected dimensions (time and entity) will be the columns
df_pivot = df_selected.groupBy('time', 'entity').pivot('dimension').agg(first('dimension_value'))

# Change/cast dimension values to numeric types (float)
for dim in selected_dimensions:
    df_pivot = df_pivot.withColumn(dim, col(dim).cast('double'))

# Apply the formula to calculate the new dimension using expr()
df_pivot = df_pivot.withColumn(new_dimension_name, expr(formula))

# Normalize the new dimension
df_new_dimension = df_pivot.select(
    'time',
    'entity',
    lit(new_dimension_name).alias('dimension'),
    col(new_dimension_name).cast('string').alias('dimension_value')
)

# Write only the new dimension back to PostgreSQL using 'append'
df_new_dimension.write.jdbc(
    url=jdbc_url,
    table="normalized_data",
    mode="append",
    properties=connection_properties
)

# Stop the SparkSession
spark.stop()
