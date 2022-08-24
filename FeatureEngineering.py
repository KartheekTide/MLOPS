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

from sklearn import preprocessing
import pandas as pd
import datetime
import os

# COMMAND ----------

X = df.iloc[:,:-1]
y = df.iloc[:,-1]

# COMMAND ----------

mm_scaler = preprocessing.MinMaxScaler()
preprocess = mm_scaler.fit_transform(X)


# COMMAND ----------

preprocess = pd.DataFrame(data=preprocess,columns=X.columns)

# COMMAND ----------

preprocess['target'] = y

# COMMAND ----------

preprocess = spark.createDataFrame(preprocess)

# COMMAND ----------

dataset_name='final_'+datetime.datetime.today().strftime('%Y%m%d')

# COMMAND ----------

pandasDF = pandasDF.assign(final_dataset_path=[dataset_name])

# COMMAND ----------

pandasDF

# COMMAND ----------

#preprocess.write.format('parquet').save(os.path.join('dbfs:/tmp/',dataset_name))
preprocess.write.mode('overwrite').parquet(os.path.join('dbfs:/tmp/',dataset_name))

# COMMAND ----------

conf = spark.createDataFrame(pandasDF)

# COMMAND ----------

conf.write.mode('overwrite').parquet(os.path.join('dbfs:/tmp/','conf'))

# COMMAND ----------


