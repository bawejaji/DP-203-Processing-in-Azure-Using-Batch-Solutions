-- Databricks notebook source
-- MAGIC %md ###1. Connect to Azure Data Lake using Access Key

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC spark.conf.set(
-- MAGIC                   "fs.azure.account.key.*** Data Lake Name ***.dfs.core.windows.net",
-- MAGIC   
-- MAGIC                   "*** Data Lake Access Key ***"
-- MAGIC               )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC inputFolderPath = "abfss://*** Container Name ***@*** Data Lake Name ***.dfs.core.windows.net/*** Path ***/"

-- COMMAND ----------

-- MAGIC %md ###2. UPDATE command

-- COMMAND ----------

SELECT RideId
     , VendorId
     , PassengerCount
     
FROM TaxisDB.TaxiRides

WHERE RideId = 9997

-- COMMAND ----------

UPDATE TaxisDB.TaxiRides

SET PassengerCount = 2

WHERE RideId = 9997

-- COMMAND ----------

SELECT RideId
     , VendorId
     , PassengerCount
     
FROM TaxisDB.TaxiRides

WHERE RideId = 9997

-- COMMAND ----------

DESCRIBE HISTORY TaxisDB.TaxiRides

-- COMMAND ----------

-- MAGIC %md ###3. DELETE command

-- COMMAND ----------

SELECT RideId
     , VendorId
     , PassengerCount
     
FROM TaxisDB.TaxiRides

WHERE RideId = 9999

-- COMMAND ----------

DELETE FROM TaxisDB.TaxiRides

WHERE RideId = 9999

-- COMMAND ----------

SELECT RideId
     , VendorId
     , PassengerCount
     
FROM TaxisDB.TaxiRides

WHERE RideId = 9999

-- COMMAND ----------

DESCRIBE HISTORY TaxisDB.TaxiRides

-- COMMAND ----------

-- MAGIC %md ###4. MERGE command

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Extract changed records from Data Lake
-- MAGIC taxiRidesChangesDF = (
-- MAGIC                         spark    
-- MAGIC                           .read
-- MAGIC                           .option("header", "true")
-- MAGIC                           .option("inferSchema", "true")
-- MAGIC
-- MAGIC                           .csv(inputFolderPath + "TaxiRides_changes.csv")
-- MAGIC                      )
-- MAGIC
-- MAGIC # Show DataFrame content
-- MAGIC display( taxiRidesChangesDF )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC taxiRidesChangesDF.createOrReplaceTempView("TaxiRideChanges")

-- COMMAND ----------

SELECT *
FROM TaxiRideChanges

-- COMMAND ----------

MERGE INTO TaxisDB.TaxiRides tgt

    USING TaxiRideChanges    src

        ON    tgt.VendorId  =  src.VendorId
          AND tgt.RideId    =  src.RideId
  
-- Update row if join conditions match
WHEN MATCHED
      
      THEN  
          UPDATE SET    tgt.PassengerCount = src.PassengerCount                        -- Use 'UPDATE SET *' to update all columns

-- Insert row if row is not present in target table
WHEN NOT MATCHED

      THEN 
          INSERT (RideId, VendorId, PickupTime, DropTime, PickupLocationId, DropLocationId, CabNumber, DriverLicenseNumber, PassengerCount, 
                  TripDistance, RateCodeId, PaymentType, TotalAmount, FareAmount, Extra, MtaTax, TipAmount, TollsAmount, ImprovementSurcharge)
          
          VALUES (RideId, VendorId, PickupTime, DropTime, PickupLocationId, DropLocationId, CabNumber, DriverLicenseNumber, PassengerCount, 
                  TripDistance, RateCodeId, PaymentType, TotalAmount, FareAmount, Extra, MtaTax, TipAmount, TollsAmount, ImprovementSurcharge)
                                                                         

-- COMMAND ----------

-- MAGIC %md ###5. Time Travel - Using Version Number

-- COMMAND ----------

SELECT RideId, PassengerCount

FROM TaxisDB.TaxiRides

WHERE RideId = 9997

-- COMMAND ----------

DESCRIBE HISTORY TaxisDB.TaxiRides

-- COMMAND ----------

SELECT RideId, PassengerCount

FROM TaxisDB.TaxiRides        VERSION AS OF 0

WHERE RideId = 9997

-- COMMAND ----------

SELECT RideId, PassengerCount

FROM TaxisDB.TaxiRides        VERSION AS OF ***Version Number***

WHERE RideId = 9997

-- COMMAND ----------

-- MAGIC %md ###6. Time Travel - Using Timestamp

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC
-- MAGIC SELECT RideId, PassengerCount
-- MAGIC
-- MAGIC FROM TaxisDB.TaxiRides        TIMESTAMP AS OF '***Timestamp***'
-- MAGIC
-- MAGIC WHERE RideId = 9997

-- COMMAND ----------

-- MAGIC %md ###7. Restore Table to Older Version

-- COMMAND ----------

RESTORE TABLE TaxisDB.TaxiRides    TO VERSION AS OF ***Version Number***

-- COMMAND ----------

DESCRIBE HISTORY TaxisDB.TaxiRides

-- COMMAND ----------

SELECT RideId, PassengerCount
FROM TaxisDB.TaxiRides
WHERE RideId = 9997

-- COMMAND ----------


