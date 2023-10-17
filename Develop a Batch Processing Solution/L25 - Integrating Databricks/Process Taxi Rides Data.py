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

filePath = "abfss://*** Container Name ***@*** Data Lake Name ***.dfs.core.windows.net/*** Path ***/TaxiRides.csv"

# COMMAND ----------

# Read TaxiRides csv file to create DataFrame
from pyspark.sql.functions import *

taxiRidesDF = (
                  spark    
                    .read    

                    .option("header", "true")

                    .option("inferSchema", "true")

                    .csv(filePath)
              )

# Show DataFrame content
display( taxiRidesDF )

# COMMAND ----------

# MAGIC %md
# MAGIC ###4. Analyze Data

# COMMAND ----------

taxiRidesAnalyzedDF = (
                            taxiRidesDF.describe
                            (
                                "PassengerCount",
                                "TripDistance"
                            )
                      )

display(taxiRidesAnalyzedDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ###5. Apply Transformations

# COMMAND ----------

# MAGIC %md
# MAGIC ####5.A. Filter Data

# COMMAND ----------

taxiRidesDF = (
                  taxiRidesDF
    
                      .where("PassengerCount > 0")

                      .where("TripDistance > 0.0")
              )

# COMMAND ----------

# MAGIC %md
# MAGIC ####5.B. Select Limited Columns

# COMMAND ----------

taxiRidesDF = (
                   taxiRidesDF

                        # Select only limited columns
                        .select(
                                  col("RideID").alias("ID"),
                             
                                  "PickupTime",
                                  "DropTime",
                                  "PickupLocationId",
                                  "DropLocationId",
                                  "PassengerCount",
                                  "TripDistance",
                                  "RateCodeId",
                                  "PaymentType",
                                  "TotalAmount"
                              )
              )

taxiRidesDF.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ####5.C. Create Derived Columns - TripYear, TripMonth, TripDay

# COMMAND ----------

taxiRidesDF = (
                  taxiRidesDF
    
                        .withColumn("TripYear"  , year(col("PickupTime")))

                        .withColumn("TripMonth" , month(col("PickupTime")))

                        .withColumn("TripDay"   , dayofmonth(col("PickupTime")))
              )

taxiRidesDF.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ####5.D. Create Derived Column - TripType

# COMMAND ----------

taxiRidesDF = (
                  taxiRidesDF
    
                        .withColumn("TripType", when(
                                                      col("RateCodeId") == 6,
                                                        "SharedTrip"
                                                    )
                                                .otherwise("SoloTrip")
                                   )
              )

taxiRidesDF.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ###6. Save Data in Parquet Format to Data Lake

# COMMAND ----------

(
    taxiRidesDF
            .write
    
            .mode("overwrite")
    
            .parquet("abfss://*** Container Name ***@*** Data Lake Name ***.dfs.core.windows.net/Output/TaxiRides.parquet")
)

# COMMAND ----------


