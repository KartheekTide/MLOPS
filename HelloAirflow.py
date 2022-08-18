# Databricks notebook source
dbutils.widgets.text("greeting", "world", "Greeting")
greeting = dbutils.widgets.get("greeting")

# COMMAND ----------

print("hello {}".format(greeting))

# COMMAND ----------

# MAGIC %run ./HelloAirflow2
