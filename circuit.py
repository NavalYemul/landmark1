# Databricks notebook source
# MAGIC %fs ls dbfs:/FileStore/tables/raw

# COMMAND ----------

# MAGIC %sql
# MAGIC select "Landmark"

Final code

# COMMAND ----------

# MAGIC %md
# MAGIC #### STEP 1: Extract/ Read

# COMMAND ----------
chnage this to 
df=spark.read.schema(user_schema)
df=spark.read.csv("dbfs:/FileStore/tables/raw/circuits.csv",header=True,inferSchema=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Load/ Write 

# COMMAND ----------

df.write.saveAsTable("naval.circuit")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Transformation

# COMMAND ----------

Dataframe functions
1. select 
2. alias
3. withColumnRenamed
4. column
5. toDF
5. withColumn

functions
1. col
2. concat
3. lit
4. 

# COMMAND ----------

help(df.select)

# COMMAND ----------

select c1 as col1, c2 from tblname

# COMMAND ----------

df.select("circuitId").display()

# COMMAND ----------

df.select("circuitId".alias("circuit_id")).display()

# COMMAND ----------

df.select(col("circuitId").alias("circuit_id")).display()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df.select(col("circuitId").alias("circuit_id"))display()

# COMMAND ----------

display(df)

# COMMAND ----------

df.select("*",concat("location"," ", "country")).display()

# COMMAND ----------

df.select("*",concat("location",lit(" "), "country")).display()

# COMMAND ----------

df.withColumnRenamed("circuitId","circuit_id").withColumnRenamed("circuitRef","circuit_ref").display()

# COMMAND ----------

(df.withColumnRenamed("circuitId","circuit_id")
.withColumnRenamed("circuitRef","circuit_ref")
.display())

# COMMAND ----------

df.columns

# COMMAND ----------

new_columns= ['col1',
 'circuit_ref',
 'name',
 'location',
 'country',
 'latitude',
 'longitude',
 'altitude',
 'url']

# COMMAND ----------

df1=df.toDF(*new_columns)

# COMMAND ----------

df1.display()

# COMMAND ----------

df.withColumn("ingestion_date",current_date()).display()

# COMMAND ----------

df.withColumn("location",upper("location")).display()

# COMMAND ----------


