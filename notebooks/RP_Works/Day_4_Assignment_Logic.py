# Databricks notebook source
print("Databricks is an awesome platform")


# COMMAND ----------

rdd = sc.textFile("/FileStore/tables/employee.csv")
rdd.take(5)

# COMMAND ----------

df = spark.read.format("csv")\
      .option("header", "true")\
      .option("inferSchema", "true")\
      .option("escape", '"')\
      .load("/FileStore/tables/employee.csv")

#.option("delimiter", " ")\
#df.show(8)
#df.count()
display(df) #databricks function

# COMMAND ----------

 import pandas as pd
  
 pandas_df = pd.read_csv("/dbfs/FileStore/tables/employee.csv")

 #print(pandas_df)
  
 display(pandas_df)

# COMMAND ----------

# # = single line comment
# CTRL + / = Multi-line selected comment

with open("/dbfs/FileStore/tables/employee.csv") as f_read:
  for line in f_read:
    print(line)

# with open("/dbfs/FileStore/tables/employee.csv", "w") as f:
#   f.write("welcome,to,databricks,training")


# COMMAND ----------

# MAGIC %sh
# MAGIC 
# MAGIC ls -ltr
# MAGIC pwd
# MAGIC cd /databricks
# MAGIC pwd
# MAGIC cd /
# MAGIC cd ..

# COMMAND ----------

import pandas as pd
  
pandas_df = pd.read_csv("/dbfs/FileStore/tables/employee.csv")

pandas_df.sort_values("firstName", inplace = True)
filter1 = pandas_df["salary"]>120000
pandas_df.where(filter1, inplace = True)
pandas_df.to_parquet('/dbfs/FileStore/Bluenet-parque/employee>120000_1.gzip', compression='gzip' )
pandas_df.to_csv('/dbfs/FileStore/Bluenet-parque/employee>120000_1.csv')
#pandas_df.columns[4]


# COMMAND ----------

#dbutils.fs.help()

#display(dbutils.fs.ls("/databricks-datasets/airlines"))
display(dbutils.fs.ls("/"))
#dbutils.fs.mkdirs("/FileStore/Bluenet-parque") #directory file size always 0
#dbutils.fs.head("/databricks-datasets/airlines/part-00000") #head=read
#dbutils.fs.put("/bluenet/sample.txt","Welcome to the Terrordome") #put=write
#dbutils.fs.head("/bluenet/sample.txt")
#dbutils.fs.mv("/bluenet/sample.txt", "/")
#dbutils.fs.rm("/FileStore/Bluenet_RP")

# COMMAND ----------

import pandas as pd
  
df = pd.read_csv("/dbfs/FileStore/Bluenet-parque/employee>120000_1.csv")
df

# COMMAND ----------

