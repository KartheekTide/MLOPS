# Databricks notebook source
import tecton
feature_service = tecton.get_workspace('prod').get_feature_service("invoice_transaction_matching_feature_service")

# COMMAND ----------

feature_service.feature_views

# COMMAND ----------


