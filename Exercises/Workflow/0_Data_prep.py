# Databricks notebook source
import mlflow
import numpy as np
import pandas as pd

# COMMAND ----------

# Load and preprocess data
white_wine = pd.read_csv("/dbfs/databricks-datasets/wine-quality/winequality-white.csv", sep=';')
red_wine = pd.read_csv("/dbfs/databricks-datasets/wine-quality/winequality-red.csv", sep=';')
white_wine['is_red'] = 0
red_wine['is_red'] = 1
data_df = pd.concat([white_wine, red_wine], axis=0)
# Remove spaces from column names
data_df.rename(columns=lambda x: x.replace(' ', '_'), inplace=True)
#Drop the target label i.e.'quality' so the entire dataset can be used for inference (for the sake of demonstraing workflows functionality)
data_df = data_df.drop([<fill-in-here>], axis=1)

# COMMAND ----------

wine_df = spark.createDataFrame(data_df)

# COMMAND ----------

display(wine_df)

# COMMAND ----------

#Create a database named SCJ_SCORE_BASE_<your-initials> and set it as default

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS <fill-in-here>

# COMMAND ----------

# MAGIC %sql
# MAGIC USE <fill-in-here>

# COMMAND ----------

#Write the wine_df dataframe to a wine_data table in the SCJ_SCORE_BASE_<your_initials> names "wine_data"

# COMMAND ----------

wine_df.write.saveAsTable(<fill-in-here>)


# COMMAND ----------


