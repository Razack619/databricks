-- Databricks notebook source
-- DBTITLE 1,database
create database core_dev.data

-- COMMAND ----------

-- DBTITLE 1,create table
create table core_dev.data.employee (
  emp_id int,
  emp_name string,
  country string ,
  salary int,
  dept_id int 
) using delta 

-- COMMAND ----------

-- DBTITLE 1,fetch table
select * from core_dev.data.employee

-- COMMAND ----------

-- DBTITLE 1,insert
insert into core_dev.data.employee values(1,'manish','india',10000,101)

-- COMMAND ----------

insert into core_dev.data.employee(emp_id,country,salary,dept_id) values(1,'india',10000,101)

-- COMMAND ----------

-- DBTITLE 1,truncate
truncate table core_dev.data.employee


-- COMMAND ----------

-- DBTITLE 1,drop
drop table core_dev.data.employee

-- COMMAND ----------

-- DBTITLE 1,delete
delete from core_dev.data.employee where emp_name is null

-- COMMAND ----------

select * from core_dev.data.employee

-- COMMAND ----------

-- DBTITLE 1,volume
create volume core_dev.data.files

-- COMMAND ----------

-- DBTITLE 1,pyspark
-- MAGIC %python
-- MAGIC df = spark.read.csv("/Volumes/core_dev/data/files/employees.csv",header=True,inferSchema=True)
-- MAGIC df.createOrReplaceTempView("employee")

-- COMMAND ----------

select * from employee

-- COMMAND ----------

-- DBTITLE 1,load into table
insert into core_dev.data.employee  select * from employee

-- COMMAND ----------

select * from core_dev.data.employee

-- COMMAND ----------

-- DBTITLE 1,filter data
select * from core_dev.data.employee where country='IN'

-- COMMAND ----------

select * from core_dev.data.employee where country='IN' AND salary>60000

-- COMMAND ----------

select * from core_dev.data.employee where country='IN' or salary>60000

-- COMMAND ----------

-- DBTITLE 1,sort the data
select * from core_dev.data.employee order by salary asc

-- COMMAND ----------

-- DBTITLE 1,aggregate
select sum(salary),avg(salary),count(*),max(salary),min(salary) from core_dev.data.employee

-- COMMAND ----------

-- DBTITLE 1,group by

select sum(salary),country from core_dev.data.employee group by country

-- COMMAND ----------


select sum(salary),dept_id from core_dev.data.employee group by dept_id

-- COMMAND ----------

-- DBTITLE 1,having
select sum(salary),country from core_dev.data.employee where dept_id>10 group by country having sum(salary)>120000

-- COMMAND ----------

-- DBTITLE 1,like operator
select * from core_dev.data.employee 

-- COMMAND ----------

select * from core_dev.data.employee WHERE emp_name like '%a'

-- COMMAND ----------

-- DBTITLE 1,limit
select * from core_dev.data.employee limit 2

-- COMMAND ----------

select * from core_dev.data.employee order by salary desc limit 2

-- COMMAND ----------

-- DBTITLE 1,distinct
select distinct dept_id from core_dev.data.employee 

-- COMMAND ----------

-- DBTITLE 1,join
select * from core_dev.data.employee 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.read.csv('/Volumes/core_dev/data/files/departments.csv',header=True,inferSchema=True)
-- MAGIC df.createOrReplaceTempView("department")
-- MAGIC

-- COMMAND ----------

select * from department

-- COMMAND ----------

create table core_dev.data.department select * from department

-- COMMAND ----------

-- DBTITLE 1,employee
select * from core_dev.data.employee

-- COMMAND ----------

select * from core_dev.data.department

-- COMMAND ----------

-- DBTITLE 1,inner
select * from  core_dev.data.employee emp join  core_dev.data.department dept
on emp.dept_id=dept.dept_id

-- COMMAND ----------

-- DBTITLE 1,left
select * from  core_dev.data.employee emp left join  core_dev.data.department dept
on emp.dept_id=dept.dept_id

-- COMMAND ----------

-- DBTITLE 1,right
select * from  core_dev.data.employee emp right join  core_dev.data.department dept
on emp.dept_id=dept.dept_id

-- COMMAND ----------

-- DBTITLE 1,full
select * from  core_dev.data.employee emp full join  core_dev.data.department dept
on emp.dept_id=dept.dept_id

-- COMMAND ----------

-- DBTITLE 1,union
select * from  core_dev.data.employee 

-- COMMAND ----------

select * from core_dev.data.employee_1

-- COMMAND ----------

create table core_dev.data.employee_1 select * from  core_dev.data.employee 

-- COMMAND ----------

-- DBTITLE 1,union
select * from core_dev.data.employee
union all
select * from core_dev.data.employee_1

-- COMMAND ----------

-- DBTITLE 1,windows function
CREATE TABLE employee_salaries (
    emp_id INT,
    emp_name STRING,
    dept_id INT,
    salary INT,
    country STRING
);

INSERT INTO employee_salaries VALUES
(101, 'Anita', 10, 75000, 'IN'),
(102, 'Raj', 10, 75000, 'IN'),     -- duplicate salary in same dept
(103, 'Meera', 10, 55000, 'IN'),
(104, 'John', 20, 80000, 'US'),
(105, 'Sophia', 20, 90000, 'US'),
(106, 'Liam', 20, 90000, 'US'),    -- duplicate salary in dept 20
(107, 'Olivia', 30, 72000, 'UK'),
(108, 'Emma', 30, 72000, 'UK'),    -- duplicate salary in dept 30
(109, 'Noah', 30, 60000, 'UK'),
(110, 'Aarav', 40, 50000, 'IN'),
(111, 'Anita', 10, 75000, 'IN');   -- duplicate emp_name + salary


-- COMMAND ----------

select * from employee_salaries

-- COMMAND ----------

-- DBTITLE 1,row number
select *,row_number() over(order by salary desc) rn from employee_salaries

-- COMMAND ----------

-- DBTITLE 1,rank
select *,rank() over(order by salary desc) rn from employee_salaries

-- COMMAND ----------

-- DBTITLE 1,dense rank
select *,dense_rank() over(order by salary desc) rn from employee_salaries

-- COMMAND ----------

-- DBTITLE 1,LEAD
SELECT *,lead(salary) over(order by emp_id) from employee_salaries

-- COMMAND ----------

-- DBTITLE 1,LAG
SELECT *,lag(salary) over(order by emp_id) from employee_salaries

-- COMMAND ----------

