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
from pyspark.sql import functions as f

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

# DBTITLE 1,Simple JOIN with different datasources
df_joined = df_json.join(df_csv, df_json["id"] == df_csv["id"], "inner")
display(df_joined)

# COMMAND ----------

# DBTITLE 1,Split Columns
df_csv_limit = df_csv.limit(1)
col_split = f.split(df_csv_limit["ip_address"], "\.")
df_split_column = df_csv_limit.withColumns({
    "part1": col_split.getItem(0),
    "part2": col_split.getItem(1),
    "part3": col_split.getItem(2),
    "part4": col_split.getItem(3)
})
df_split_column.display()

# COMMAND ----------

# DBTITLE 1,Split a column into multiple rows
df_split_row = df_csv_limit.withColumn("split_row", f.explode(col_split))
df_split_row.display()

# COMMAND ----------

spark.sql("SELECT * FROM {_df}", _df=df_csv).show()

# COMMAND ----------

df_json.createOrReplaceTempView("tmp_df_json")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tmp_df_json
