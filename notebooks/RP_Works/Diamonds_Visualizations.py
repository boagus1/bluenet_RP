# Databricks notebook source



diamonds = spark.read.csv("/FileStore/tables/Diamonds.csv", header="true", inferSchema="true")
display(diamonds)
diamonds.createOrReplaceTempView("diamonds_tbl")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from diamonds_tbl 
# MAGIC 
# MAGIC 
# MAGIC --where color="E"