# Databricks notebook source
import pandas as pd

# COMMAND ----------

# STEP 1: RUN THIS CELL TO INSTALL BAMBOOLIB

# You can also install bamboolib on the cluster. Just talk to your cluster admin for that
%pip install bamboolib  

# Heads up: this will restart your python kernel, so you may need to re-execute some of your other code cells.

# COMMAND ----------

# STEP 2: RUN THIS CELL TO IMPORT AND USE BAMBOOLIB

import bamboolib as bam

# This opens a UI from which you can import your data
bam  

# Already have a pandas data frame? Just display it!
# Here's an example
# import pandas as pd
# df_test = pd.DataFrame(dict(a=[1,2]))
# df_test  # <- You will see a green button above the data set if you display it

# COMMAND ----------

# Display all of the rows where item_type is Baby Food:
# Add to this code so that it displays only those rows where order_prio is C
# Sort the rows by region in ascending order:


# COMMAND ----------

import pandas as pd

df = pd.read_csv(bam.sales_csv)
df

# COMMAND ----------

import pandas as pd; import numpy as np
# Step: Keep rows where item_type is one of: Baby Food
df = df.loc[df['item_type'].isin(['Baby Food'])]

# Step: Keep rows where order_prio is one of: C
df = df.loc[df['order_prio'].isin(['C'])]

# COMMAND ----------

df

# COMMAND ----------


