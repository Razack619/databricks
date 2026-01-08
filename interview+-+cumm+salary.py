# Databricks notebook source
# MAGIC %md 
# MAGIC # INTERVIEW QUESTION 
# MAGIC ## 1 question
# MAGIC
# MAGIC #### 1. Find out cummulative sales or running total sales
# MAGIC #### 2. find out the prev sales
# MAGIC ####3. find out the nex sales
# MAGIC
# MAGIC #### SOLVE USING PYSPARK AND SPARK SQL 

# COMMAND ----------

data = [ ['2024-01-01',20000], ['2024-01-02',10000],[ '2024-01-03',150000], ['2024-01-04',100000], ['2024-01-05',210000]] 
  
#define column names
columns = ['date', 'sales'] 
  
#create dataframe using data and column names
df = spark.createDataFrame(data, columns) 
display(df)

# COMMAND ----------

# DBTITLE 1,sql
df.createOrReplaceTempView('sample')

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from sample

# COMMAND ----------

# DBTITLE 1,cumm_sales
# MAGIC %sql 
# MAGIC select *,sum(sales) over(order by date asc) cumm_sales, lag(sales) over(order by date asc) as prev_sal, lead(sales) over(order by date asc) nex_sal from sample

# COMMAND ----------

# DBTITLE 1,pyspark
display(df)

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql.functions import * 

window= Window.orderBy('date')
df1=df.withColumn('cumm_sales',sum(col('sales')).over(window))
df1.withColumn('prev_sales',lag(col('sales')).over(window)).show()

# COMMAND ----------

