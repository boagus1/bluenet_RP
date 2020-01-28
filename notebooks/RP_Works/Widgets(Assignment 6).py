# Databricks notebook source
displayHTML("""<font size = 6 color="red" face="sans sarif"> Data Analysis Dashboard </font> """)

# COMMAND ----------

# MAGIC %md # Dashboard Label

# COMMAND ----------

txnsDF =  spark.read.format("csv")\
               .option("header", "True")\
               .option("inferSchema", "true")\
               .load("/databricks-datasets/bikeSharing/data-001/day.csv")
display(txnsDF)

txnsDF.createOrReplaceTempView("bikeshare")

# COMMAND ----------

# MAGIC %md Biking Conditions Across Seasons

# COMMAND ----------



# spark.sql("Select season, MAX(temp) as temperature, MAX(hum) as humidity, MAX(windspeed) as wind from bikeshare")

display(spark.sql("select season, MAX(temp) as temperature, MAX(hum) as humidity, MAX(windspeed) as windspeed from bikeshare group by season order by season"))



# COMMAND ----------

# MAGIC  %sql
# MAGIC 
# MAGIC use db_rp;
# MAGIC 
# MAGIC --drop table txns
# MAGIC 
# MAGIC -- create table txns (txnno INT, txndate STRING, custno INT, amount DOUBLE, category STRING, product STRING, city STRING, state STRING, spendby STRING)
# MAGIC -- Using CSV
# MAGIC -- Options (path "/FileStore/tables/txns", header="True")
# MAGIC 
# MAGIC --select count(*) from txns
# MAGIC 
# MAGIC --select * from txns order by txnno
# MAGIC 
# MAGIC  
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC --SELECT COUNT(*) AS numChoices, getArgument("categories") AS categories FROM txns WHERE category = getArgument("categories")
# MAGIC --SELECT * , getArgument("categories") AS categories FROM txns WHERE category = getArgument("categories")
# MAGIC 
# MAGIC --SELECT * FROM txns WHERE category = getArgument("categories")
# MAGIC 
# MAGIC --SELECT * FROM txns WHERE category = getArgument("combo_categories2")
# MAGIC 
# MAGIC --SELECT * FROM txns WHERE category = getArgument("multi_categories")
# MAGIC 
# MAGIC --SELECT * FROM txns WHERE category IN getArgument("categories") and getArgument("combo_categories2")
# MAGIC 
# MAGIC --OPTIMIZE delta.txns
# MAGIC 
# MAGIC --OPTIMIZE txns
# MAGIC 
# MAGIC -- select custno, category, COUNT(*) FROM txns 
# MAGIC -- group by custno, category 
# MAGIC --  having COUNT(*) > 1
# MAGIC 
# MAGIC --having  COUNT(*) > 1

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC 
# MAGIC CREATE WIDGET TEXT y DEFAULT "10"
# MAGIC 
# MAGIC 
# MAGIC -- CREATE WIDGET MULTISELECT multi_categories DEFAULT "Air Sports" CHOICES SELECT DISTINCT category FROM txns

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC CREATE WIDGET DROPDOWN categories DEFAULT "Air Sports" CHOICES SELECT DISTINCT category FROM txns

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE WIDGET COMBOBOX combo_categories2 DEFAULT "Air Sports" CHOICES SELECT DISTINCT category FROM txns

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE WIDGET MULTISELECT multi_categories DEFAULT "Air Sports" CHOICES SELECT DISTINCT category FROM txns

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM txns WHERE category = getArgument("categories")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM txns WHERE category = getArgument("combo_categories2")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC Select a.category, a.product, a.city, a.state, SUM(a.amount + b.amount) from txns a JOIN txns b 
# MAGIC on a.category = b.category and
# MAGIC a.city = b.city and
# MAGIC a.state = b.state
# MAGIC --where a.product =!= b.product
# MAGIC group by a.category, a.product, a.city, a.state, a.amount order by a.city, a.category

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC use db_rp;
# MAGIC 
# MAGIC --select count(distinct(custno)) from txns
# MAGIC select custno, SUM(amount) as ttl_sales from txns group by custno, amount order by ttl_sales desc limit 10

# COMMAND ----------

# txns =  spark.read.format("csv")\
#             .option("header", "True")\
#             .option("delimeter", ",")\
#             .option("inferSchema", "true")\
#             .load("/FileStore/tables/txns")


# txns.columns = ['txnno', 'txndate', 'custno', 'amount', 'category', 'product', 'city', 'state', 'spendby']
# #display(txns)

# COMMAND ----------


categories = spark.sql("select distinct(category) from txns")
display(categories)


# categories = txns.select(category).distinct().collect()
# display(categories)

# COMMAND ----------

dbutils.widgets.text("age2", "4", "employee_age")
age = dbutils.widgets.get("age2")
print(age)


# COMMAND ----------

