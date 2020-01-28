# Databricks notebook source
babynames = spark.read.csv("/FileStore/tables/BabyNames.csv", header="true", inferSchema="true")
display(babynames)


# COMMAND ----------

