# Databricks notebook source
# MAGIC %md
# MAGIC ###1. Connect to Azure Data Lake using Access Key

# COMMAND ----------

spark.conf.set(
                  "fs.azure.account.key.*** Data Lake Name ***.dfs.core.windows.net",
  
                  "*** Data Lake Access Key ***"
              )

# COMMAND ----------

# MAGIC %md
# MAGIC ###2. List files in Data Lake

# COMMAND ----------

display(
          dbutils.fs.ls("abfss://*** Container Name ***@*** Data Lake Name ***.dfs.core.windows.net/*** Path ***")
       )

# COMMAND ----------

# MAGIC %md
# MAGIC ###3. Read file from Data Lake

# COMMAND ----------

inputFolderPath = "abfss://*** Container Name ***@*** Data Lake Name ***.dfs.core.windows.net/Raw/"

outputFolderPath = "abfss://*** Container Name ***@*** Data Lake Name ***.dfs.core.windows.net/Output/"

# COMMAND ----------

# Read TaxiRides csv file to create DataFrame

taxiRidesDF = (
                  spark    
                    .read
                    
                    .option("header", "true")
                    .option("inferSchema", "true")

                    .csv(inputFolderPath + "TaxiRides.csv")
              )

# Show DataFrame content
display( taxiRidesDF )

# COMMAND ----------

# MAGIC %md
# MAGIC ###4. Write data in Parquet format

# COMMAND ----------

(
    taxiRidesDF
        .write
        .mode("overwrite")
  
        .partitionBy("VendorId")
  
        .format("parquet")
  
        .save(outputFolderPath + "TaxiRides.parquet")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ###5. Write data in Delta format

# COMMAND ----------

(
    taxiRidesDF
        .write
        .mode("overwrite")
  
        .partitionBy("VendorId")
  
        .format("delta")
  
        .save(outputFolderPath + "TaxiRides.delta")
)

# COMMAND ----------

# MAGIC %md Check output folder to see folders for Parquet and Delta
# MAGIC
# MAGIC Check for the presence of _delta_log subfolder in Delta folder

# COMMAND ----------

# MAGIC %md
# MAGIC ###6. Create Database

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE DATABASE IF NOT EXISTS TaxisDB

# COMMAND ----------

# MAGIC %md
# MAGIC ###7. Create Spark Table referencing Delta folder

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Create table based on Delta data
# MAGIC
# MAGIC CREATE TABLE TaxisDB.TaxiRides
# MAGIC
# MAGIC   USING DELTA 
# MAGIC   
# MAGIC   LOCATION "abfss://*** Container Name ***@*** Data Lake Name ***.dfs.core.windows.net/Output/TaxiRides.delta"

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC SELECT COUNT(*) FROM TaxisDB.TaxiRides

# COMMAND ----------

# MAGIC %md
# MAGIC ###8. Audit History of Delta Table
# MAGIC
# MAGIC This shows transaction log of Delta Table

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE HISTORY TaxisDB.TaxiRides

# COMMAND ----------

# MAGIC %md
# MAGIC ###9. Options to Add Data to Delta Table

# COMMAND ----------

# MAGIC %md
# MAGIC ####Option 9.1: Insert command
# MAGIC
# MAGIC Use typical SQL Insert command to add data to table

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC INSERT INTO TaxisDB.TaxiRides
# MAGIC
# MAGIC (RideId, VendorId, PickupTime, DropTime, PickupLocationId, DropLocationId, CabNumber, DriverLicenseNumber, PassengerCount, TripDistance, RateCodeId, PaymentType, TotalAmount, FareAmount, Extra, MtaTax, TipAmount, TollsAmount, ImprovementSurcharge)
# MAGIC
# MAGIC VALUES (10000, 3, '2021-12-01T00:00:00.000Z', '2021-12-01T00:15:34.000Z', 170, 140, 'TAC399', '5131685', 1, 2.9, 1, 1, 15.3, 13.0, 0.5, 0.5, 1.0, 0.0, 0.3)

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC SELECT COUNT(*) FROM TaxisDB.TaxiRides

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE HISTORY TaxisDB.TaxiRides

# COMMAND ----------

# MAGIC %md
# MAGIC ####Option 9.2: Append a DataFrame
# MAGIC
# MAGIC Read data as a DataFrame and append to Delta Table using PySpark

# COMMAND ----------

# Extract new records from Data Lake

taxiRidesAppendDF = (
                        spark    
                          .read
                          .option("header", "true")
                          .option("inferSchema", "true")

                          .csv(inputFolderPath + "TaxiRides_append.csv")
                    ) 

# Show DataFrame content
display( taxiRidesAppendDF )

# COMMAND ----------

# Append to data lake in delta format

(
    taxiRidesAppendDF
        .write
  
        .mode("append")
  
        .partitionBy("VendorId")  
        .format("delta")           
        .save(outputFolderPath + "TaxiRides.delta")
)

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC SELECT COUNT(*) FROM TaxisDB.TaxiRides

# COMMAND ----------


