# Databricks notebook source
def load_dataset(dataset_name):
  return spark.read.parquet('dbfs:/tmp/'+dataset_name)

# COMMAND ----------

conf=load_dataset('conf')

# COMMAND ----------

conf = conf.toPandas()

# COMMAND ----------

conf

# COMMAND ----------

final = load_dataset(conf['final_dataset_path'][0])

# COMMAND ----------

final = final.toPandas()

# COMMAND ----------

final.head(10)

# COMMAND ----------

from sklearn.linear_model import LogisticRegression

# COMMAND ----------

X = final.iloc[:,:-1]
y = final.iloc[:,-1]

# COMMAND ----------

model = LogisticRegression()
model = model.fit(X,y)

# COMMAND ----------


