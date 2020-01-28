-- Databricks notebook source
-- DBTITLE 1,Create tables in new database
-- MAGIC %sql
-- MAGIC 
-- MAGIC 
-- MAGIC use db_rp;
-- MAGIC -- create table Flight_data ( year INT,month INT,day INT, day_of_week INT, dep_time INT, crs_dep_time INT, arr_time INT, crs_arr_time INT, unique_carrier STRING, flight_num INT, tail_num STRING,actual_elapsed_time INT,crs_elapsed_time INT,air_time INT,arr_delay INT,dep_delay INT,origin STRING,dest STRING,distance INT,taxi_in INT,taxi_out INT,cancelled INT,cancellation_code STRING,diverted INT,carrier_delay STRING,weather_delay STRING,nas_delay STRING,security_delay STRING,late_aircraft_delay STRING)
-- MAGIC -- Using CSV
-- MAGIC -- Options (path "/FileStore/tables/Flight_data.csv", header="True")
-- MAGIC 
-- MAGIC 
-- MAGIC --create database bluenet5;
-- MAGIC create table airport (Airport_name String, Country_name String, number Int, code String)
-- MAGIC Using CSV
-- MAGIC Options (path "/FileStore/tables/airport.csv", header="True")

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC 
-- MAGIC use bluenet5;
-- MAGIC 
-- MAGIC --drop table airport
-- MAGIC --select * from Flight_data limit 100
-- MAGIC --select max(month) from Flight_data
-- MAGIC 
-- MAGIC select * from airport

-- COMMAND ----------

