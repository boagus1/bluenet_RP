# Databricks notebook source
#file location & type
file_location = "/FileStore/tables/Flight_data.csv"
file_type = "csv"

#csv options
infer_schema = "false"
first_row_is_header = "false"
delimeter = ","

#create dataframe using option
df = spark.read.format(file_type)\
      .option("header", first_row_is_header)\
      .option("infer_schema", infer_schema)\
      .option("sep", delimeter)\
      .load(file_location)

display(df)

#create as a view or temp table

df.createOrReplaceTempView("Flight_data_csv_temp")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from Flight_data_csv_temp limit 100

# COMMAND ----------

#file location & type
file_location = "/FileStore/tables/airport.csv"
file_type = "csv"

#csv options
infer_schema = "false"
first_row_is_header = "false"
delimeter = ","

#create dataframe using option
df = spark.read.format(file_type)\
      .option("header", first_row_is_header)\
      .option("infer_schema", infer_schema)\
      .option("sep", delimeter)\
      .load(file_location)

display(df)

#create as a view or temp table

df.createOrReplaceTempView("airport_csv_temp")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from airport_csv_temp

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC use DB_RP;
# MAGIC 
# MAGIC --drop table airport_managed
# MAGIC 
# MAGIC create table airport_managed(Airport_name String, Country_name String, number Int, code String)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC use DB_RP;
# MAGIC 
# MAGIC Load data INPATH "/FileStore/tables/airport.csv" Overwrite into table airport_managed;
# MAGIC 
# MAGIC 
# MAGIC describe formatted airport_managed;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from airport_managed

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC use DB_RP;
# MAGIC 
# MAGIC --drop table Flight_data_managed
# MAGIC 
# MAGIC create table Flight_data_managed( year INT,month INT,day INT, day_of_week INT, dep_time INT, crs_dep_time INT, arr_time INT, crs_arr_time INT, unique_carrier STRING, flight_num INT, tail_num STRING,actual_elapsed_time INT,crs_elapsed_time INT,air_time INT,arr_delay INT,dep_delay INT,origin STRING,dest STRING,distance INT,taxi_in INT,taxi_out INT,cancelled INT,cancellation_code STRING,diverted INT,carrier_delay STRING,weather_delay STRING,nas_delay STRING,security_delay STRING,late_aircraft_delay STRING)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC use DB_RP;
# MAGIC 
# MAGIC Load data INPATH "/FileStore/tables/Flight_data.csv" Overwrite into table Flight_data_managed;
# MAGIC 
# MAGIC describe formatted Flight_data_managed;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from Flight_data_managed

# COMMAND ----------

# MAGIC %sh
# MAGIC date

# COMMAND ----------

