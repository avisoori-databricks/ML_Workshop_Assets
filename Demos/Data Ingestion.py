# Databricks notebook source
#Copy and paste after downloading .csv file from: https://www.kaggle.com/datasets/roopalik/amazon-baby-dataset


# COMMAND ----------

# MAGIC %md
# MAGIC How to mount: https://learn.microsoft.com/en-us/azure/databricks/dbfs/mounts

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

#dbutils.fs.ls('dbfs:/mnt/field-demos-azure/iris/')
dbutils.fs.ls('/databricks-datasets')

# COMMAND ----------


#iris = spark.read.csv('dbfs:/mnt/field-demos-azure/iris/iris.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reading data into a Pandas DataFrame

# COMMAND ----------

movie_df = spark.read.format('csv').option('header','true').load('dbfs:/FileStore/shared_uploads/avinash.sooriyarachchi@databricks.com/movies.csv')


# COMMAND ----------

display(movie_df), type(movie_df)

# COMMAND ----------


#Get a unique row for each movie+ genre combination 
from pyspark.sql.functions import explode, split
movie_df2 = movie_df.select(movie_df.title,explode(split(movie_df.genres,'\\|')))

display(movie_df2)

# COMMAND ----------

#Rename col column to 'genre'
movie_df2 = movie_df2.withColumnRenamed('col', 'genre')
display(movie_df2)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Convert Pyspark Dataframe to Pandas on Pyspark Dataframe 

# COMMAND ----------

movie_spd = movie_df.to_pandas_on_spark()
display(movie_spd)

# COMMAND ----------

movie_spd['genres'] = movie_spd['genres'].str.split('|')
movie_spd = movie_spd.explode('genres')
display(movie_spd)

# COMMAND ----------

#Back to spark
refined_movie = movie_spd.to_spark()
type(refined_movie), display(refined_movie)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Let's create a delta table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS SCJ_DB

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCJ_DB

# COMMAND ----------

refined_movie.write.saveAsTable("refined_movie_table")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM refined_movie_table

# COMMAND ----------

# MAGIC %md
# MAGIC ## Converting to Pandas(*No Spark this time)

# COMMAND ----------

movie_df_pd = movie_df.toPandas()
type(movie_df_pd), display(movie_df_pd)

# COMMAND ----------

movie_df_pd

# COMMAND ----------

movie_df_pd['genres'] = movie_df_pd['genres'].str.split('|')
movie_df_pd = movie_df_pd.explode('genres')
display(movie_df_pd)

# COMMAND ----------


