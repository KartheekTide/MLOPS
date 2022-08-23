# Databricks notebook source
def load_dataset(dataset_name):
  return spark.read.parquet('dbfs:/tmp/'+dataset_name)

# COMMAND ----------

conf=load_dataset('conf')

# COMMAND ----------

pandasDF = conf.toPandas()

# COMMAND ----------

pandasDF

# COMMAND ----------

df=load_dataset(pandasDF['DataSetName'][0])

# COMMAND ----------

df = df.toPandas()

# COMMAND ----------

df.isnull().sum()

# COMMAND ----------

df.dtypes

# COMMAND ----------


