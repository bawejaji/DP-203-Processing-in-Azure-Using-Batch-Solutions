#!/usr/bin/env python
# coding: utf-8

# ## Process Taxi Rides
# 
# 
# 

# In[1]:


InputFilePath = ""

OutputFilePath = ""


# In[22]:


# Read csv file
taxiRidesDF = (
                  spark
                    .read
                    .csv(InputFilePath)
              )

# Print the schema

taxiRidesDF.printSchema()

# Display 5 records
display(taxiRidesDF.limit(5))


# In[23]:


# Read csv file
taxiRidesDF = (
                  spark
                    .read

                    .option("header", "true")
                    .option("inferSchema", "true")

                    .csv(InputFilePath)
              )

# Print the schema
taxiRidesDF.printSchema()

# Display 5 records
display(taxiRidesDF.limit(5))


# # Apply Transformations

# In[24]:


# Filter inaccurate data

taxiRidesDF = (
                  taxiRidesDF
                          .where("PassengerCount > 0")

                          .where("TripDistance > 0.0")
              )

print("Rides count after filter = " + str(taxiRidesDF.count()))


# In[25]:


from pyspark.sql.functions import *

taxiRidesDF = (
                  taxiRidesDF
                          .select(
                                        col("RideId").alias("ID"),

                                        col("VendorId"),
                                        col("PickupTime"),
                                        col("DropTime"), 
                                        col("PickupLocationId"),
                                        col("DropLocationId"),
                                        col("TotalAmount")
                                 )
              )

taxiRidesDF.printSchema()


# In[26]:


# Write the dataframe output as parquet to Data Lake

(
    taxiRidesDF  
      .write

      .mode("overwrite")

      .parquet(OutputFilePath)
)


# In[ ]:




