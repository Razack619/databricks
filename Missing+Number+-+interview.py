# Databricks notebook source
# MAGIC %md 
# MAGIC # INTERVIEW QUESTION 
# MAGIC
# MAGIC #### 1. Find out the missing number 
# MAGIC
# MAGIC #### SOLVE USING PYSPARK AND SPARK SQL
# MAGIC

# COMMAND ----------

data=[(1,),(2,),(5,),(7,),(8,),(10,)]
column=['ID']
df=spark.createDataFrame(data,column)
df.show()


# COMMAND ----------

from pyspark.sql.types import IntegerType
list_new= range(1,11,1)
df_new=spark.createDataFrame(list_new,IntegerType())
df_new.show()

# COMMAND ----------

display(df_new.subtract(df))

# COMMAND ----------

