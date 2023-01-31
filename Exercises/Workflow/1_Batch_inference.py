# Databricks notebook source
# MAGIC %md
# MAGIC This is an auto-generated notebook to perform batch inference on a Spark DataFrame using a selected model from the model registry. This feature is in preview, and we would greatly appreciate any feedback through this form: https://databricks.sjc1.qualtrics.com/jfe/form/SV_1H6Ovx38zgCKAR0.
# MAGIC 
# MAGIC ## Instructions:
# MAGIC 1. To best re-create the training environment, use the same cluster runtime and version from training.
# MAGIC 2. Add additional data processing on your loaded table to match the model schema if necessary (see the "Define input and output" section below).
# MAGIC 3. "Run All" the notebook.
# MAGIC 4. Note: If the `%pip` does not work for your model (i.e. it does not have a `requirements.txt` file logged), modify to use `%conda` if possible.

# COMMAND ----------

#Add your initials below to retrieve the model you just registered and in subsequent placeholders to retrieve and create tables unique to you

# COMMAND ----------

model_name = "scj_wine_model_<your_initials>"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Environment Recreation
# MAGIC To best re-create the training environment, use the same cluster runtime and version from training.. The cell below downloads the model artifacts associated with your model in the remote registry, which include `conda.yaml` and `requirements.txt` files. In this notebook, `pip` is used to reinstall dependencies by default.
# MAGIC 
# MAGIC ### (Optional) Conda Instructions
# MAGIC Models logged with an MLflow client version earlier than 1.18.0 do not have a `requirements.txt` file. If you are using a Databricks ML runtime (versions 7.4-8.x), you can replace the `pip install` command below with the following lines to recreate your environment using `%conda` instead of `%pip`.
# MAGIC ```
# MAGIC conda_yml = os.path.join(local_path, "conda.yaml")
# MAGIC %conda env update -f $conda_yml
# MAGIC ```

# COMMAND ----------

from mlflow.store.artifact.models_artifact_repo import ModelsArtifactRepository
import os

model_uri = f"models:/{model_name}/Production"
local_path = ModelsArtifactRepository(model_uri).download_artifacts("") # download model from remote registry

requirements_path = os.path.join(local_path, "requirements.txt")
if not os.path.exists(requirements_path):
  dbutils.fs.put("file:" + requirements_path, "", True)

# COMMAND ----------

# MAGIC %pip install -r $requirements_path

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define input and output
# MAGIC The table path assigned to`input_table_name` will be used for batch inference and the predictions will be saved to `output_table_path`. After the table has been loaded, you can perform additional data processing, such as renaming or removing columns, to ensure the model and table schema matches.

# COMMAND ----------

# redefining key variables here because %pip and %conda restarts the Python interpreter
model_name = "scj_wine_model_<fill-in-your-initials>"
input_table_name = "scj_score_base_<fill-in-your-initials>.wine_data"
output_table_path = "/FileStore/batch-inference/scj_wine_model_<fill-in-your-initials>"

# COMMAND ----------

# load table as a Spark DataFrame
table = spark.table(input_table_name)
# optionally, perform additional data processing (may be necessary to conform the schema)


# COMMAND ----------

# MAGIC %md ## Load model and run inference
# MAGIC **Note**: If the model does not return double values, override `result_type` to the desired type.

# COMMAND ----------

import mlflow
from pyspark.sql.functions import struct

model_uri = f"models:/{model_name}/Production"

# create spark user-defined function for model prediction
predict = mlflow.pyfunc.spark_udf(spark, model_uri, result_type="double")

# COMMAND ----------

output_df = table.withColumn("prediction", predict(struct(*table.columns)))

# COMMAND ----------

display(output_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCJ_SCORE_BASE_<fill-in-your-initials>

# COMMAND ----------

output_df.write.saveAsTable('batch_inferences_<fill-in-your-initials>')

# COMMAND ----------


