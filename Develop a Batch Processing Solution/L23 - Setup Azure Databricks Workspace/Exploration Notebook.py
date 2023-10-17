# Databricks notebook source
# Create multi-column DataFrame (tabular structure)

employees = sc.parallelize(
                            [
                                (1,  "Neha",  10000),
                                (2,  "Cathy", 20000),
                                (3,  "Ivan",  30000),
                                (4,  "Kerri", 40000),
                                (5,  "Mohit", 50000)
                            ]
                          )

employeesDF = employees.toDF()

# COMMAND ----------

# Define column names in DataFrame

employeesDF = employeesDF.toDF("Id", "Name", "Salary")

# COMMAND ----------

# Display data in DataFrame

display( employeesDF )

# COMMAND ----------

# Filter data in DataFrame

display( 
          employeesDF
            .where("Salary > 10000")
            .where("Id != 4")
       )

# COMMAND ----------


