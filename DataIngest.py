# Databricks notebook source
from sklearn import datasets
import pandas as pd
import datetime
import os

# COMMAND ----------

data = datasets.load_breast_cancer()

# COMMAND ----------

# Read the DataFrame, first using the feature data
df = pd.DataFrame(data.data, columns=data.feature_names)
# Add a target column, and fill it with the target data
df['target'] = data.target
# Show the first five rows
df.rename(columns = lambda x : x.replace(' ', '_'),inplace=True)
df.head()

# COMMAND ----------

df =spark.createDataFrame(df)

# COMMAND ----------

dataset_name='mlops_'+datetime.datetime.today().strftime('%Y%m%d')

# COMMAND ----------

conf=spark.createDataFrame(pd.DataFrame(data=[dataset_name],columns=['DataSetName']))

# COMMAND ----------

#conf.write.format('parquet').save(os.path.join('dbfs:/tmp/','conf'))
conf.write.mode('overwrite').parquet(os.path.join('dbfs:/tmp/','conf'))

# COMMAND ----------

#df.write.format('parquet').save(os.path.join('dbfs:/tmp/',dataset_name))
df.write.mode('overwrite').parquet(os.path.join('dbfs:/tmp/',dataset_name))

# COMMAND ----------

def load_dataset(dataset_name):
  return spark.read.parquet('dbfs:/tmp/'+dataset_name)

# COMMAND ----------

dataset_renames= 'dbfs:/tmp/'+dataset_name

# COMMAND ----------

df_reload=load_dataset(dataset_name)

# COMMAND ----------

df_reload.show(10)

# COMMAND ----------

conf = load_dataset('conf')

# COMMAND ----------

conf.show()

# COMMAND ----------


