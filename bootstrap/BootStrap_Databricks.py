# Databricks notebook source
# DBTITLE 1,Create a simple dataset with PySpark
# Create a Spark DataFrame from a list of tuples with predefined schema
data = [
    (1, "John", 25, "Engineer", "USA"),
    (2, "Anna", 30, "Data Scientist", "UK"),
    (3, "Kyle", 28, "Designer", "Canada"),
    (4, "Laura", 22, "Developer", "USA"),
    (5, "Mike", 35, "Manager", "Australia"),
    (6, "Nina", 27, "Analyst", "Germany"),
    (7, "Tom", 29, "Architect", "USA"),
    (8, "Sara", 24, "Tester", "UK"),
    (9, "Leo", 33, "Support", "France"),
    (10, "Zara", 29, "HR", "India"),
]

# Define the schema (column names) for the DataFrame
columns = ["id", "name", "age", "job", "country"]

# Create the DataFrame using the data and schema
df = spark.createDataFrame(data, schema=columns)

# COMMAND ----------

# DBTITLE 1,Read files
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Define a schema for reading JSON and CSV data
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("ip_address", StringType(), True)
])

# Specify the folder path where the data files are located
folder_mock = "/FileStore/tables/mock"

# Read a JSON file into a DataFrame using the predefined schema and enabling multiline option
df_json = spark.read.option("multiline", "true").schema(schema).json(f"{folder_mock}/MOCK_DATA.json")

# Read a CSV file into a DataFrame using the predefined schema and considering the first row as header
df_csv = spark.read.schema(schema).option("header", "true").csv(f"{folder_mock}/MOCK_DATA.csv")

# COMMAND ----------

# DBTITLE 1,Display MOCK_DATA CSV


# COMMAND ----------

# DBTITLE 1,Display MOCK_DATA JSON


# COMMAND ----------

# DBTITLE 1,Simple JOIN with different datasources

