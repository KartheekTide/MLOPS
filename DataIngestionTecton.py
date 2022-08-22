# Databricks notebook source
import tecton
feature_service = tecton.get_workspace('prod').get_feature_service("invoice_transaction_matching_feature_service")

# COMMAND ----------

feature_service.feature_views

# COMMAND ----------

feature_view = tecton.get_feature_view("invoice_transaction_datetime_features")

# COMMAND ----------

result_dataframe = feature_view.run()
#print(display(result_dataframe.to_spark()))

# COMMAND ----------

result_dataframe

# COMMAND ----------

from datetime import datetime, timedelta
start_time = datetime.today() - timedelta(days=2)
results = feature_view.get_historical_features(start_time=start_time)
display(results)

# COMMAND ----------


