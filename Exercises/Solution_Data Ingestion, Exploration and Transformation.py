# Databricks notebook source
#Uncomment the following line, click on the button to install bamboolib. Run the couple of cells thus created to install and import Bamboolib
import pandas as pd

# COMMAND ----------

#List all the mounts available to you in workspace
dbutils.fs.mounts()

# COMMAND ----------

#List the contents of the mount 
dbutils.fs.ls('dbfs:<fill-in-here>')

# COMMAND ----------

#Locate the file you want to read in and read it into a pyspark dataframe
taxi_fares = spark.read.csv(<fill-in-here>)

# COMMAND ----------

#Display this pyspark dataframe and create a data profile. Then create a visualization of the distribution of total_amount Y axis is Count(*), x axis is total_amount
taxi_data = spark.read.format("csv").option("header", "true").load('dbfs:/FileStore/shared_uploads/avinash.sooriyarachchi@databricks.com/yellow_tripdata.csv')
display(taxi_data)

# COMMAND ----------

#Convert to Pandas API on PySpark and cast total_amount as float

taxi_data_spd = taxi_data.to_pandas_on_spark()
display(taxi_data_spd)

# COMMAND ----------

taxi_data_spd['total_amount'] = taxi_data_spd['total_amount'].astype(float)


# COMMAND ----------

taxi_data_spd['Expensive'] = taxi_data_spd['total_amount']>20.00
display(taxi_data_spd)

# COMMAND ----------

#Sample 1/100th of the overall dataset and drop the VendorID, RatecodeID and toal_amount columns using bamboolib. Then export to a variable named final_pandas_df
small_taxi_data = taxi_data_spd.sample(frac=0.01).to_pandas()
small_taxi_data


# COMMAND ----------

display(final_pandas_df)



# COMMAND ----------

#Convert this pandas df to spark dataframe
final_df = spark.createDataFrame(final_pandas_df)
display(final_df)

# COMMAND ----------

#Create database named SCJ_Base
#Save this dataframe as taxi_table
#First create the database below

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS SCJ_BASE

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCJ_BASE

# COMMAND ----------

final_df.write.saveAsTable('taxi_table')

# COMMAND ----------

# MAGIC %md
# MAGIC Congratulations!!! Now you've loaded data, transformed the data using both Apache Spark and Pandas, Visualized the data and created a Delta table

# COMMAND ----------


