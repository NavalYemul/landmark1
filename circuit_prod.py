# Databricks notebook source
# MAGIC %run /Workspace/Users/navallyemul@gmail.com/Spark/includes

# COMMAND ----------

df=spark.read.csv(f"{input_path}/circuits.csv",header=True,inferSchema=True)

# COMMAND ----------

df_final=df.withColumn("ingestion_date",current_timestamp())\
.withColumn("source_path",input_file_name()).drop("url")

# COMMAND ----------

df_final.write.mode("ignore").saveAsTable("naval.circuit")
