# Databricks notebook source
# MAGIC %pip install bamboolib

# COMMAND ----------

#Uncomment the following line, click on the button to install bamboolib. Run the couple of cells thus created to install and import Bamboolib
import pandas as pd
import bamboolib as bam

# COMMAND ----------

bam

# COMMAND ----------

#List all the mounts available to you in workspace
dbutils.fs.mounts()

# COMMAND ----------

#List the contents of the databricks-datasets mount and explore the nyctaxi dataset. Then ultimately get to: dbfs:/databricks-datasets/nyctaxi/tables/nyctaxi_yellow
dbutils.fs.ls(<fill-in-here>)

# COMMAND ----------

#Locate the file you want to read in and read it into a pyspark dataframe. File is at: 'dbfs:/databricks-datasets/nyctaxi/tripdata/yellow/yellow_tripdata_2019-01.csv.gz'
#Display this pyspark dataframe and create a data profile. Then create a visualization of the distribution of total_amount Y axis is Count(*), x axis is total_amount
taxi_data = spark.read.format("csv").option("header", "true").load(<fill-in-here>)


# COMMAND ----------

#Display this pyspark dataframe and create a data profile. Then create a visualization of the distribution of total_amount Y axis is Count(*), x axis is total_amount
display(<fill-in-here>)

# COMMAND ----------

#Convert to Pandas API on PySpark and cast total_amount as float

taxi_data_spd = taxi_data.<fill-in-here>
display(taxi_data_spd)

# COMMAND ----------

#Cast the total_amount column to data type float
taxi_data_spd['total_amount'] = taxi_data_spd[<fill-in-here>].astype(<fill-in-here>)


# COMMAND ----------

#Create a binary variable named 'Expensive' such that it is Boolean value True if the total_amount is greater than 20.0 and false if not. 
taxi_data_spd[<fill-in-here>] = taxi_data_spd['total_amount']>20.00
display(taxi_data_spd)

# COMMAND ----------

#Sample 1/100th of the overall dataset Then export to a variable named small_taxi_data
small_taxi_data = taxi_data_spd.sample(<fill-in-here>).to_pandas()
small_taxi_data


# COMMAND ----------

#drop the VendorID, RatecodeID and toal_amount columns using bamboolib. Then export to a variable named final_pandas_df. If you type in the name of the dataframe and hit run, you will see an option to open up the bamboolib UI

# COMMAND ----------

display(final_pandas_df)


# COMMAND ----------

#Convert this pandas df to spark dataframe
final_df = spark.createDataFrame(<fill-in-here>)
display(final_df)

# COMMAND ----------

#Create database named CIRCANA_Base
#Save this dataframe as taxi_table_<your-initials>
#First create the database below

# COMMAND ----------

# MAGIC %<fill-in-here>
# MAGIC CREATE DATABASE IF NOT EXISTS <fill-in-here>

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CIRCANA_BASE

# COMMAND ----------

#Write final_df to a table named taxi_table_<your-name>
final_df.write.saveAsTable(<fill-in-here>)

# COMMAND ----------

# MAGIC %md
# MAGIC Congratulations!!! Now you've loaded data, transformed the data using both Apache Spark and Pandas, Visualized the data and created a Delta table
