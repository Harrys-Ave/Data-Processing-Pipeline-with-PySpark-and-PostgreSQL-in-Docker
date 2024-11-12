from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

# Initialize SparkSession
# config: path to the PostgreSQL JDBC driver. This connects spark with the PostgreSQL database
spark = SparkSession.builder \
    .appName("NormalizeData") \
    .config("spark.jars", "/opt/bitnami/spark/extra-jars/postgresql-42.7.4.jar") \
    .getOrCreate()

# JDBC connection
jdbc_url = "jdbc:postgresql://db:5432/assignment"
connection_properties = {
    "user": "harryave",
    "password": "1234assignment",
    "driver": "org.postgresql.Driver"
}

# Read data from the customs_data table
# customs_data is the name I gave to the table in postgreSQL
df = spark.read.jdbc(
    url=jdbc_url,
    table="customs_data",
    properties=connection_properties
)

# Exclude entity and time columns
# dimension_columns are the columns to be normalized
dimension_columns = [col for col in df.columns if col not in ['Declaration_ID', 'Date']]

# Number of dimensions
n = len(dimension_columns)

# Create the stack expression
# Essentially a list of column-value pairs
stack_expr = ', '.join([f"'{col}', {col}" for col in dimension_columns]) 

# Apply the stack function
# I also rename  some columns (e.g 'date' to 'time' and 'declaration_id' to 'entity')
df_normalized = df.select(
    df['Date'].alias('time'),
    df['Declaration_ID'].alias('entity'),
    expr(f"stack({n}, {stack_expr}) as (dimension, dimension_value)")
)

# Making dimension_value a string.
df_normalized = df_normalized.withColumn(
    'dimension_value',
    df_normalized['dimension_value'].cast('string')
)

# Write the normalized data to a new table normalized_data
df_normalized.write.jdbc(
    url=jdbc_url,
    table="normalized_data",
    mode="overwrite",
    properties=connection_properties
)

# Stop the SparkSession
spark.stop()
