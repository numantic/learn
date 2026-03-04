# Databricks notebook source
# MAGIC %md
# MAGIC ### Databricks Demo: Unity Catalog, Volumes, and the Medallion Architecture
# MAGIC 
# MAGIC **Presenter Talk Track: Introduction to Catalogs and Schemas**
# MAGIC > "Welcome everyone. Today we are going to look at how Databricks organizes and processes data using Unity Catalog. 
# MAGIC >
# MAGIC > Think of Unity Catalog as the central hub for data governance. It uses a three-level namespace: **Catalog > Schema > Table or Volume**. 
# MAGIC > * The **Catalog** is the top level. 
# MAGIC > * Inside a Catalog, we have **Schemas** (or databases). These logically group related data together.
# MAGIC > 
# MAGIC > The real power of this hierarchy is governance. It provides a unified place to manage users, assign roles, and enforce security policies across all your data assets."

# COMMAND ----------

# MAGIC %md
# MAGIC **Presenter Talk Track: Explaining Volumes**
# MAGIC > "While Tables handle structured data, **Volumes** handle everything else. A Volume represents a logical volume of storage in a cloud object store. Volumes let you govern, read, and write non-tabular data—like raw Parquet files or CSVs. We can create a folder structure directly inside a Volume to organize incoming data."

# COMMAND ----------

# Define our Unity Catalog hierarchy variables for the demo
catalog_name = "main"
schema_name = "default"
volume_name = "demo_volume"

# Switch to the correct catalog and schema
spark.sql(f"USE CATALOG {catalog_name}")
spark.sql(f"USE SCHEMA {schema_name}")

# Create a volume to hold our raw files
spark.sql(f"CREATE VOLUME IF NOT EXISTS {volume_name}")
print(f"Environment ready. Using Volume: /Volumes/{catalog_name}/{schema_name}/{volume_name}")

# COMMAND ----------

# Define the path to our volume and create a landing folder
volume_path = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}"
raw_data_folder = f"{volume_path}/landing_zone/incoming_parquet"
dbutils.fs.mkdirs(raw_data_folder)

print(f"Successfully created folder at: {raw_data_folder}")

# COMMAND ----------

# Create some sample data to simulate an incoming file
data = [
    (1, "Alice", "Engineering", 75000),
    (2, "Bob", "Sales", 60000),
    (3, "Charlie", "Engineering", 80000)
]
columns = ["employee_id", "name", "department", "salary"]

# Write it as a Parquet file to our new Volume folder
df_mock = spark.createDataFrame(data, columns)
parquet_file_path = f"{raw_data_folder}/employees_raw.parquet"
df_mock.write.mode("overwrite").parquet(parquet_file_path)

print(f"Mock Parquet file landed in Volume at: {parquet_file_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC **Presenter Talk Track: Building a Bronze Delta Table**
# MAGIC > "The first step in the Medallion Architecture is the **Bronze layer**, a raw, unvalidated copy of the data stored in the highly optimized Delta format. We will read the raw Parquet file from our Volume and save it as a managed Delta Table in Unity Catalog."

# COMMAND ----------

# Read the raw Parquet file and define the Bronze table name
df_raw = spark.read.format("parquet").load(parquet_file_path)
bronze_table_name = "bronze_employee_data"

# Write the data out as a Delta Table
df_raw.write.format("delta").mode("overwrite").saveAsTable(bronze_table_name)

print(f"Successfully built Delta Table: {catalog_name}/{schema_name}/{bronze_table_name}")
display(spark.sql(f"SELECT * FROM {bronze_table_name}"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Presenter Talk Track: Handling Multiple Files**
# MAGIC > "In reality, data ingestion is continuous. Let's simulate a second batch of data arriving. Spark can read an entire directory using wildcards, combining all underlying files into a single DataFrame to update our Bronze table."

# COMMAND ----------

# Create a second batch of sample data
data_batch_2 = [
    (4, "Diana", "Marketing", 65000),
    (5, "Evan", "Sales", 62000)
]

# Write as a second Parquet file in the same folder
df_batch_2 = spark.createDataFrame(data_batch_2, columns)
parquet_file_path_2 = f"{raw_data_folder}/employees_raw_batch2.parquet"
df_batch_2.write.mode("overwrite").parquet(parquet_file_path_2)

print("\nCurrent files in our incoming folder:")
display(dbutils.fs.ls(raw_data_folder))

# COMMAND ----------

# Read ALL parquet files in the folder at once using a wildcard
df_combined = spark.read.format("parquet").load(f"{raw_data_folder}/*.parquet")

# Overwrite our Bronze Delta table with the combined data
df_combined.write.format("delta").mode("overwrite").saveAsTable(bronze_table_name)

display(spark.sql(f"SELECT * FROM {bronze_table_name} ORDER BY employee_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Presenter Talk Track: The Silver Layer and Data Cleansing**
# MAGIC > "The **Silver layer** represents our filtered, cleansed, and augmented data. We are going to use PySpark to read our Bronze table, apply some standard transformations, and write the results to a new Silver table."

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, upper

# Read our Bronze table
df_bronze = spark.read.table(f"{catalog_name}.{schema_name}.{bronze_table_name}")

# Apply Silver-level transformations
df_silver = df_bronze \
    .withColumn("department_standardized", upper(col("department"))) \
    .drop("department") \
    .withColumnRenamed("department_standardized", "department") \
    .filter(col("salary") > 0) \
    .withColumn("silver_processing_timestamp", current_timestamp())

# Define and write the Silver Delta Table
silver_table_name = "silver_employee_data"
df_silver.write.format("delta").mode("overwrite").saveAsTable(f"{catalog_name}.{schema_name}.{silver_table_name}")

display(spark.sql(f"SELECT * FROM {silver_table_name} ORDER BY employee_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Presenter Talk Track: The Gold Layer and Business Value**
# MAGIC > "Finally, we reach the **Gold layer**. Gold tables are highly refined and aggregated for business use cases. We will store this in its own dedicated 'gold' schema so we can lock down our Unity Catalog permissions for reporting tools like Power BI."

# COMMAND ----------

from pyspark.sql.functions import count, avg, round

# Create a dedicated schema for our Gold tables
gold_schema_name = "gold"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{gold_schema_name}")

# Read the clean Silver table
df_silver_clean = spark.read.table(f"{catalog_name}.{schema_name}.{silver_table_name}")

# Create a Gold-level aggregation
df_gold_summary = df_silver_clean \
    .groupBy("department") \
    .agg(
        count("employee_id").alias("headcount"),
        round(avg("salary"), 2).alias("average_salary")
    )

# Write the aggregated DataFrame to the Gold schema
gold_table_name = "department_salary_summary"
df_gold_summary.write.format("delta").mode("overwrite").saveAsTable(f"{catalog_name}.{gold_schema_name}.{gold_table_name}")

display(spark.sql(f"SELECT * FROM {catalog_name}.{gold_schema_name}.{gold_table_name} ORDER BY headcount DESC"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Presenter Talk Track: Power BI Integration**
# MAGIC > *(Switch to Power BI Desktop)* "To finish our pipeline, we connect Power BI directly to this Gold schema using DirectQuery. When users authenticate, Unity Catalog permissions seamlessly pass through, ensuring they only see authorized data."

# COMMAND ----------

# MAGIC %md
# MAGIC **Presenter Talk Track: Cleaning Up the Environment**
# MAGIC > "To wrap up, we will clean our environment. Because everything is managed by Unity Catalog, executing a few SQL commands logically removes these assets and safely handles the deletion of underlying files."

# COMMAND ----------

print("Starting environment teardown...")

# Drop the Gold schema and tables
spark.sql(f"DROP SCHEMA IF EXISTS {catalog_name}.{gold_schema_name} CASCADE")

# Drop the Silver and Bronze tables
spark.sql(f"DROP TABLE IF EXISTS {catalog_name}.{schema_name}.{silver_table_name}")
spark.sql(f"DROP TABLE IF EXISTS {catalog_name}.{schema_name}.{bronze_table_name}")

# Clean up physical files and drop the Volume
dbutils.fs.rm(raw_data_folder, True)
spark.sql(f"DROP VOLUME IF EXISTS {catalog_name}.{schema_name}.{volume_name}")

print("Teardown complete. The environment is clean and ready for the next demo!")
