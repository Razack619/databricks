# Databricks notebook source
# MAGIC %md 
# MAGIC # INTERVIEW QUESTION 
# MAGIC
# MAGIC #### 1. Group multiple rows into single 
# MAGIC
# MAGIC #### SOLVE USING PYSPARK AND SPARK SQL
# MAGIC

# COMMAND ----------

data=[(1,'Manish','Mobile'),(1,'Manish','Washing Mavhine'),(2,'Rahul','Car'),(2,'Rahul','mobile'),(2,'Rahul','scooty'),(3,'Monu','Scooty')]
schema=["Customer_ID", "Customer_Name",'Purchase']

df = spark.createDataFrame(data,schema)
df.show()


# COMMAND ----------

df.createOrReplaceTempView('sample')

# COMMAND ----------

# MAGIC %sql select customer_id,customer_name,collect_set(purchase) purchase_new from sample
# MAGIC group by 1,2

# COMMAND ----------

from pyspark.sql.functions import * 
df.groupBy('customer_id','customer_name').agg(collect_set('purchase')).show()

# COMMAND ----------

# MAGIC %md 
# MAGIC # INTERVIEW QUESTION 
# MAGIC
# MAGIC #### 1. how to combine many list 
# MAGIC
# MAGIC #### SOLVE USING PYSPARK AND SPARK SQL
# MAGIC

# COMMAND ----------

list1 = ["a", "b", "c", "d"]
list2 = [1, 2, 3, 4]

rdd = spark.sparkContext.parallelize(list(zip(list1,list2)))

df=rdd.toDF(['column1','column2'])
df.show()





# COMMAND ----------

