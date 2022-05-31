# terms
- Spark workflows
- SparkSession
- getOrCreate
- getActiveSession
- handling null values in spark - https://mungingdata.com/pyspark/none-null/
- datatypes and shcemas - https://sparkbyexamples.com/pyspark/pyspark-find-datatype-column-names-of-dataframe/
- Adding constant columns with lit and typedLit to PySpark DataFrames  (https://mungingdata.com/pyspark/constant-column-lit-typedlit/)
- Python type conversions (https://mungingdata.com/pyspark/constant-column-lit-typedlit/)
- PySpark implicit type conversion (https://mungingdata.com/pyspark/constant-column-lit-typedlit/)
- Array constant column (The Scala API has a typedLit function to handle complex types like arrays, but there is no such method in the PySpark API, so hacks are required.)
- Working with PySpark ArrayType Columns (https://mungingdata.com/pyspark/array-arraytype-list/)
- Create ArrayType column (https://mungingdata.com/pyspark/array-arraytype-list/)
- List aggregations (collect_list)
- Exploding an array into multiple rows (https://mungingdata.com/pyspark/array-arraytype-list/)
- PySpark arrays can only hold one type (Python lists can hold values with different types.)
- select and add columns in PySpark (https://mungingdata.com/pyspark/select-add-columns-withcolumn/)
- Add multiple columns (withColumns)
```
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
data = [(1,"Robert"), (2,"Julia")]
df =spark.createDataFrame(data,["id","name"])

#Get All column names and it's types
for col in df.dtypes:
    print(col[0]+" , "+col[1])
```

# SparkSession
- https://mungingdata.com/pyspark/sparksession-getorcreate-getactivesession/
- This post explains how to create a SparkSession with `getOrCreate` and how to reuse the SparkSession with `getActiveSession`.
- You need a SparkSession to read data stored in files, when manually creating DataFrames, and to run arbitrary SQL queries.
- The SparkSession should be instantiated once and then reused throughout your application.
- Most applications should not create multiple sessions or shut down an existing session.

# getOrCreate
-
```
from pyspark.sql import SparkSession

spark = (SparkSession.builder
  .master("local")
  .appName("chispa")
  .getOrCreate())
```
- getOrCreate will either create the SparkSession if one does not already exist or reuse an existing SparkSession.

# getActiveSession

- Some functions can assume a SparkSession exists and should error out if the SparkSession does not exist.
- You should only be using getOrCreate in functions that should actually be creating a SparkSession. getActiveSession is more appropriate for functions that should only reuse an existing SparkSession.
-
```
from pyspark.sql import SparkSession
SparkSession.getActiveSession().createDataFrame(pretty_data, pretty_column_names)
```

# SparkSession from DataFram

- You can also grab the SparkSession that’s associated with a DataFrame.
```
data1 = [(1, "jose"), (2, "li")]
df1 = spark.createDataFrame(data1, ["num", "name"])
df1.sql_ctx.sparkSession
```
- The SparkSession that’s associated with df1 is the same as the active SparkSession and can also be accessed as follows:
```
from pyspark.sql import SparkSession
SparkSession.getActiveSession()
```
- If you have a DataFrame, you can use it to access the SparkSession, but it’s best to just grab the SparkSession with getActiveSession().

## Shut down spark session
- Let’s shut down the active SparkSession to demonstrate the getActiveSession() returns None when no session exists.
```
spark.stop()
SparkSession.getActiveSession() # None
```
- Here’s the error you’ll get if you try to create a DataFrame now that the SparkSession was stopped.

# chipsa
https://github.com/MrPowers/chispa

chispa

chispa provides fast PySpark test helper methods that output descriptive error messages.

This library makes it easy to write high quality PySpark code.

Fun fact: "chispa" means Spark in Spanish ;)

# Navigating None and null in PySpark

https://mungingdata.com/pyspark/none-null/

- This blog post shows you how to gracefully handle null in PySpark and how to avoid null input errors.
- Mismanaging the null case is a common source of errors and frustration in PySpark.

## Create DataFrames with null values

Let’s start by creating a DataFrame with null values:

```
df = spark.createDataFrame([(1, None), (2, "li")], ["num", "name"])
df.show()
```
```
+---+----+
|num|name|
+---+----+
|  1|null|
|  2|  li|
+---+----+
```
- You use None to create DataFrames with null values.

- null is not a value in Python, so this code will not work:
```
df = spark.createDataFrame([(1, null), (2, "li")], ["num", "name"])
```
It throws the following error:
```
NameError: name 'null' is not defined
```

## Read CSVs with null values

Suppose you have the following data stored in the some_people.csv file:
```
first_name,age
luisa,23
"",45
bill,
```
- Read this file into a DataFrame and then show the contents to demonstrate which values are read into the DataFrame as null.
```
path = "/Users/powers/data/some_people.csv"
df = spark.read.option("header", True).csv(path)
df.show()
+----------+----+
|first_name| age|
+----------+----+
|     luisa|  23|
|      null|  45|
|      bill|null|
+----------+----+
```
- The empty string in row 2 and the missing value in row 3 are both read into the PySpark DataFrame as null values.

# Python type conversions

implicit

```
3 + 1.2 # 4.2
```

explicit

```
float(3) + 1.2 # 4.2
```

error for implicit
```
"hi" + 3
```
explicit
```
"hi" + str(3) # 'hi3'
```

# PySpark implicit type conversions

```
# implicit conversion
>>> col("num") + 5
Column<'(num + 5)'>

# explicit conversion
>>> col("num") + lit(5)
Column<'(num + 5)'>
```

> It’s best to use lit and perform explicit conversions, so the intentions of your code are clear. You should avoid relying on implicit conversion rules that may behave unexpectedly in certain situations.

# Array constant column

The Scala API has a typedLit function to handle complex types like arrays, but there is no such method in the PySpark API, so hacks are required.

Here’s how to add a constant [5, 8] array column to the DataFrame.
```
df.withColumn("nums", array(lit(5), lit(8))).show()
+---+------+------+
|num|letter|  nums|
+---+------+------+
|  1|     a|[5, 8]|
|  2|     b|[5, 8]|
+---+------+------+
```

This code does not work.
```
df.withColumn("nums", lit([5, 8])).show()
```

# Create ArrayType column

Create a DataFrame with an array column.
```
df = spark.createDataFrame(
    [("abc", [1, 2]), ("cd", [3, 4])], ["id", "numbers"]
)
df.show()
+---+-------+
| id|numbers|
+---+-------+
|abc| [1, 2]|
| cd| [3, 4]|
+---+-------+
```
Print the schema of the DataFrame to verify that the numbers column is an array.

```
df.printSchema()
root
 |-- id: string (nullable = true)
 |-- numbers: array (nullable = true)
 |    |-- element: long (containsNull = true)
 ```
numbers is an array of long elements.

We can also create this DataFrame using the explicit StructType syntax.
```
from pyspark.sql.types import *
from pyspark.sql import Row
rdd = spark.sparkContext.parallelize(
    [Row("abc", [1, 2]), Row("cd", [3, 4])]
)
schema = StructType([
    StructField("id", StringType(), True),
    StructField("numbers", ArrayType(IntegerType(), True), True)
])
df = spark.createDataFrame(rdd, schema)
df.show()
+---+-------+
| id|numbers|
+---+-------+
|abc| [1, 2]|
| cd| [3, 4]|
+---+-------+
```
The explicit syntax makes it clear that we’re creating an ArrayType column.


# Exploding an array into multiple rows

A PySpark array can be exploded into multiple rows, the opposite of collect_list.

Create a DataFrame with an ArrayType column:
```
df = spark.createDataFrame(
    [("abc", [1, 2]), ("cd", [3, 4])], ["id", "numbers"]
)
df.show()
+---+-------+
| id|numbers|
+---+-------+
|abc| [1, 2]|
| cd| [3, 4]|
+---+-------+
```
Explode the array column, so there is only one number per DataFrame row.
```
df.select(col("id"), explode(col("numbers")).alias("number")).show()
+---+------+
| id|number|
+---+------+
|abc|     1|
|abc|     2|
| cd|     3|
| cd|     4|
+---+------+
```
collect_list collapses multiple rows into a single row. explode does the opposite and expands an array into multiple rows.


# select basic use case
Create a DataFrame with three columns.
```
df = spark.createDataFrame(
    [("jose", 1, "mexico"), ("li", 2, "china"), ("sandy", 3, "usa")],
    ["name", "age", "country"],
)
df.show()
+-----+---+-------+
| name|age|country|
+-----+---+-------+
| jose|  1| mexico|
|   li|  2|  china|
|sandy|  3|    usa|
+-----+---+-------+
```
Select the age and name columns:
```
df.select("age", "name").show()
+---+-----+
|age| name|
+---+-----+
|  1| jose|
|  2|   li|
|  3|sandy|
+---+-----+
```
The select method takes column names as arguments.

If you try to select a column that doesn’t exist in the DataFrame, your code will error out. Here’s the error you’ll see if you run `df.select("age", "name", "whatever")`

The select method can also take an array of column names as the argument.
```
df.select(["country", "name"]).show()
+-------+-----+
|country| name|
+-------+-----+
| mexico| jose|
|  china|   li|
|    usa|sandy|
+-------+-----+
```

```
df.select([col("age")]).show()
+---+
|age|
+---+
|  1|
|  2|
|  3|
+---+
```

# withColumn basic use case

withColumn adds a column to a DataFrame.

Create a DataFrame with two columns:

```
df = spark.createDataFrame(
    [("jose", 1), ("li", 2), ("luisa", 3)], ["name", "age"]
)
df.show()
```

```
+-----+---+
| name|age|
+-----+---+
| jose|  1|
|   li|  2|
|luisa|  3|
+-----+---+
```

Append a greeting column to the DataFrame with the string hello:

```
df.withColumn("greeting", lit("hello")).show()
```

```
+-----+---+--------+
| name|age|greeting|
+-----+---+--------+
| jose|  1|   hello|
|   li|  2|   hello|
|luisa|  3|   hello|
+-----+---+--------+
```

Now let’s use withColumn to append an upper_name column that uppercases the name column.
```
df.withColumn("upper_name", upper(col("name"))).show()
+-----+---+----------+
| name|age|upper_name|
+-----+---+----------+
| jose|  1|      JOSE|
|   li|  2|        LI|
|luisa|  3|     LUISA|
+-----+---+----------+
```
withColumn is often used to append columns based on the values of other columns.

# Add multiple columns (withColumns)

There isn’t a withColumns method, so most PySpark newbies call withColumn multiple times when they need to add multiple columns to a DataFrame.

Create a simple DataFrame:
```
df = spark.createDataFrame(
    [("cali", "colombia"), ("london", "uk")],
    ["city", "country"],
)
df.show()
+------+--------+
|  city| country|
+------+--------+
|  cali|colombia|
|london|      uk|
+------+--------+
```
Here’s how to append two columns with constant values to the DataFrame using select:
```
actual = df.select(["*", lit("val1").alias("col1"), lit("val2").alias("col2")])
actual.show()
+------+--------+----+----+
|  city| country|col1|col2|
+------+--------+----+----+
|  cali|colombia|val1|val2|
|london|      uk|val1|val2|
+------+--------+----+----+
```
The * selects all of the existing DataFrame columns and the other columns are appended. This design pattern is how select can append columns to a DataFrame, just like withColumn.

The code is a bit verbose, but it’s better than the following code that calls withColumn multiple times:
```
df.withColumn("col1", lit("val1")).withColumn("col2", lit("val2"))
```
There is a hidden cost of withColumn and calling it multiple times should be avoided.

The Spark contributors are considering adding withColumns to the API, which would be the best option. That’d give the community a clean and performant way to add multiple columns.


# RDD vs dataframe


# how to create dataframe

```
Create DataFrames
This example uses the Row class from Spark SQL to create several DataFrames. The contents of a few of these DataFrames are then printed.

Python
Copy to clipboardCopy
# import pyspark class Row from module sql
from pyspark.sql import *

# Create Example Data - Departments and Employees

# Create the Departments
department1 = Row(id='123456', name='Computer Science')
department2 = Row(id='789012', name='Mechanical Engineering')
department3 = Row(id='345678', name='Theater and Drama')
department4 = Row(id='901234', name='Indoor Recreation')

# Create the Employees
Employee = Row("firstName", "lastName", "email", "salary")
employee1 = Employee('michael', 'armbrust', 'no-reply@berkeley.edu', 100000)
employee2 = Employee('xiangrui', 'meng', 'no-reply@stanford.edu', 120000)
employee3 = Employee('matei', None, 'no-reply@waterloo.edu', 140000)
employee4 = Employee(None, 'wendell', 'no-reply@berkeley.edu', 160000)
employee5 = Employee('michael', 'jackson', 'no-reply@neverla.nd', 80000)

# Create the DepartmentWithEmployees instances from Departments and Employees
departmentWithEmployees1 = Row(department=department1, employees=[employee1, employee2])
departmentWithEmployees2 = Row(department=department2, employees=[employee3, employee4])
departmentWithEmployees3 = Row(department=department3, employees=[employee5, employee4])
departmentWithEmployees4 = Row(department=department4, employees=[employee2, employee3])

print(department1)
print(employee2)
print(departmentWithEmployees1.employees[0].email)
Output:

Copy to clipboardCopy
Row(id='123456', name='Computer Science')
Row(firstName='xiangrui', lastName='meng', email='no-reply@stanford.edu', salary=120000)
no-reply@berkeley.edu
```

# Create DataFrames from a list of the rows

https://docs.databricks.com/spark/latest/dataframes-datasets/introduction-to-dataframes-python.html

```
This example uses the createDataFrame method of the SparkSession (which is represented by the Databricks-provided spark variable) to create a DataFrame from a list of rows from the previous example.

Python
Copy to clipboardCopy
departmentsWithEmployeesSeq1 = [departmentWithEmployees1, departmentWithEmployees2]
df1 = spark.createDataFrame(departmentsWithEmployeesSeq1)

df1.show(truncate=False)

departmentsWithEmployeesSeq2 = [departmentWithEmployees3, departmentWithEmployees4]
df2 = spark.createDataFrame(departmentsWithEmployeesSeq2)

df2.show(truncate=False)
Output:

Copy to clipboardCopy
+--------------------------------+-----------------------------------------------------------------------------------------------------+
|department                      |employees                                                                                            |
+--------------------------------+-----------------------------------------------------------------------------------------------------+
|{123456, Computer Science}      |[{michael, armbrust, no-reply@berkeley.edu, 100000}, {xiangrui, meng, no-reply@stanford.edu, 120000}]|
|{789012, Mechanical Engineering}|[{matei, null, no-reply@waterloo.edu, 140000}, {null, wendell, no-reply@berkeley.edu, 160000}]       |
+--------------------------------+-----------------------------------------------------------------------------------------------------+

+---------------------------+------------------------------------------------------------------------------------------------+
|department                 |employees                                                                                       |
+---------------------------+------------------------------------------------------------------------------------------------+
|{345678, Theater and Drama}|[{michael, jackson, no-reply@neverla.nd, 80000}, {null, wendell, no-reply@berkeley.edu, 160000}]|
|{901234, Indoor Recreation}|[{xiangrui, meng, no-reply@stanford.edu, 120000}, {matei, null, no-reply@waterloo.edu, 140000}] |
+---------------------------+------------------------------------------------------------------------------------------------+
```

# SQS


I'm setting an API Gateway for a monitoring solution. It is sending a massive amount of message from terminals to the API Gateway and triggering lambda functions which make inserts in a MongoDB database running in EC2


# rdd and dataframes are immutable. (how we alter with withcolumns then)

If dataframes in Spark are immutable, why are we able to modify it with operations such as withColumn()?

![image](https://user-images.githubusercontent.com/6462531/170854273-fcd0974f-4ca2-4ac4-9be5-04a5ae0a3d85.png)

As per Spark Architecture DataFrame is built on top of RDDs which are immutable in nature, Hence Data frames are immutable in nature as well.

Regarding the withColumn or any other operation for that matter, when you apply such operations on DataFrames it will generate a new data frame instead of updating the existing data frame.

However, When you are working with python which is dynamically typed language you overwrite the value of the previous reference. Hence when you are executing below statement

    df = df.withColumn()

It will generate another dataframe and assign it to reference "`df`".

In order to verify the same, you can use `id()` method of rdd to get the unique identifier of your dataframe.

`df.rdd.id()`

 will give you unique identifier for your dataframe.

I hope the above explanation helps.


# What is Spark?

https://medium.com/free-code-camp/how-to-use-spark-clusters-for-parallel-processing-big-data-86a22e7f8b50

Spark uses Resilient Distributed Datasets (RDD) to perform parallel processing across a cluster or computer processors.

Basically, Spark uses a cluster manager to coordinate work across a cluster of computers. A cluster is a group of computers that are connected and coordinate with each other to process data and compute.

Spark applications consist of a driver process and executor processes.

Briefly put, the driver process runs the main function, and analyzes and distributes work across the executors. The executors actually do the tasks assigned — executing code and reporting to the driver node.

In real-world applications in business and emerging AI programming, parallel processing is becoming a necessity for efficiency, speed and complexity.

![image](https://user-images.githubusercontent.com/6462531/170855342-19c3f8a5-fd95-4e42-b60a-5008a5d97bb2.png)

# Great — so what is Databricks?
It makes it easy to launch cloud-optimized Spark clusters in minutes.

Think of it as an all-in-one package to write your code. You can use Spark (without worrying about the underlying details) and produce results.

Follow the directions there. They are clear, concise and easy:

- Create a cluster
- Attach a notebook to the cluster and run commands in the notebook on the cluster
- Manipulate the data and create a graph
- Operations on Python DataFrame API; create a DataFrame from a Databricks dataset
- Manipulate the data and display results


A cluster, or group of machines, pools the resources of many machines together allowing us to use all the cumulative resources as if they were one. Now a group of machines alone is not powerful, you need a framework to coordinate work across them. Spark is a tool for just that, managing and coordinating the execution of tasks on data across a cluster of computers.


Spark Application
 
**A Spark Application consists of:**

- Driver
- Executors (set of distributed worker processes)

**Driver**
 
The Driver runs the main() method of our application having the following duties:

- Runs on a node in our cluster, or on a client, and schedules the job execution with a cluster manager
- Responds to user’s program or input
- Analyzes, schedules, and distributes work across the executors

Executors
 
An executor is a distributed process responsible for the execution of tasks. Each Spark Application has its own set of executors, which stay alive for the life cycle of a single Spark application.

- Executors perform all data processing of a Spark job
- Stores results in memory, only persisting to disk when specifically instructed by the driver program
- Returns results to the driver once they have been completed
- Each node can have anywhere from 1 executor per node to 1 executor per core
- ** Node is single entity machine or server .



# Installing spark on archlinux

```
yay  -S apache-spark

# Java 17 isn't supported - Spark runs on Java 8/11 (source: https://spark.apache.org/docs/latest/).
# So install Java 11 and point Spark to that.

sudo pacman -S jre11-openjdk --needed --noconfirm

# set java11 as the default 

archlinux-java set java-11-openjdk

# start spark-shell and check everything is working well

$ spark-shell
Picked up _JAVA_OPTIONS: -Dawt.useSystemAAFontSettings=on
Picked up _JAVA_OPTIONS: -Dawt.useSystemAAFontSettings=on
WARNING: An illegal reflective access operation has occurred
WARNING: Illegal reflective access by org.apache.spark.unsafe.Platform (file:/opt/apache-spark/jars/spark-unsafe_2.12-3.2.0.jar) to constructor java.nio.DirectByteBuffer(long,int)
WARNING: Please consider reporting this to the maintainers of org.apache.spark.unsafe.Platform
WARNING: Use --illegal-access=warn to enable warnings of further illegal reflective access operations
WARNING: All illegal access operations will be denied in a future release
Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
22/05/31 08:26:28 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Spark context Web UI available at http://localhost:4040
Spark context available as 'sc' (master = local[*], app id = local-1653965790738).
Spark session available as 'spark'.
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 3.2.0
      /_/
         
Using Scala version 2.12.15 (OpenJDK 64-Bit Server VM, Java 11.0.12)
Type in expressions to have them evaluated.
Type :help for more information.

scala> 

```

goto http://localhost:4040

## start master

```
# change permissions
sudo chmod -R 777 /opt/apache-spark

# start master
/opt/apache-spark/sbin/start-master.sh
starting org.apache.spark.deploy.master.Master, logging to /opt/apache-spark/logs/spark-simha-org.apache.spark.deploy.master.Master-1-gauranga.out

# check log
cat /opt/apache-spark/logs/spark-simha-org.apache.spark.deploy.master.Master-1-gauranga.out

$ cat /opt/apache-spark/logs/spark-simha-org.apache.spark.deploy.master.Master-1-gauranga.out
Picked up _JAVA_OPTIONS: -Dawt.useSystemAAFontSettings=on
Spark Command: /usr/lib/jvm/default-runtime/bin/java -cp /opt/apache-spark/conf/:/opt/apache-spark/jars/* -Xmx1g org.apache.spark.deploy.master.Master --host gauranga --port 7077 --webui-port 8080
========================================
Picked up _JAVA_OPTIONS: -Dawt.useSystemAAFontSettings=on
Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
22/05/31 08:20:11 INFO Master: Started daemon with process name: 685396@gauranga
22/05/31 08:20:11 INFO SignalUtils: Registering signal handler for TERM
22/05/31 08:20:11 INFO SignalUtils: Registering signal handler for HUP
22/05/31 08:20:11 INFO SignalUtils: Registering signal handler for INT
22/05/31 08:20:11 WARN MasterArguments: SPARK_MASTER_IP is deprecated, please use SPARK_MASTER_HOST
WARNING: An illegal reflective access operation has occurred
WARNING: Illegal reflective access by org.apache.spark.unsafe.Platform (file:/opt/apache-spark/jars/spark-unsafe_2.12-3.2.0.jar) to constructor java.nio.DirectByteBuffer(long,int)
WARNING: Please consider reporting this to the maintainers of org.apache.spark.unsafe.Platform
WARNING: Use --illegal-access=warn to enable warnings of further illegal reflective access operations
WARNING: All illegal access operations will be denied in a future release
22/05/31 08:20:12 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
22/05/31 08:20:12 INFO SecurityManager: Changing view acls to: simha
22/05/31 08:20:12 INFO SecurityManager: Changing modify acls to: simha
22/05/31 08:20:12 INFO SecurityManager: Changing view acls groups to: 
22/05/31 08:20:12 INFO SecurityManager: Changing modify acls groups to: 
22/05/31 08:20:12 INFO SecurityManager: SecurityManager: authentication disabled; ui acls disabled; users  with view permissions: Set(simha); groups with view permissions: Set(); users  with modify permissions: Set(simha); groups with modify permissions: Set()
22/05/31 08:20:13 INFO Utils: Successfully started service 'sparkMaster' on port 7077.
22/05/31 08:20:13 INFO Master: Starting Spark master at spark://gauranga:7077
22/05/31 08:20:13 INFO Master: Running Spark version 3.2.0
22/05/31 08:20:14 WARN Utils: Service 'MasterUI' could not bind on port 8080. Attempting port 8081.
22/05/31 08:20:14 INFO Utils: Successfully started service 'MasterUI' on port 8081.
22/05/31 08:20:14 INFO MasterWebUI: Bound MasterWebUI to localhost, and started at http://localhost:8081
22/05/31 08:20:14 INFO Master: I have been elected leader! New state: ALIVE

# check http://localhost:8081
```
![image](https://user-images.githubusercontent.com/6462531/171084270-0cd72561-0f12-4f61-9a26-57fc1b109c35.png)

## pyspark

https://www.bmc.com/blogs/jupyter-notebooks-apache-spark/

There’s no need to install PySpark separately as it comes bundled with Spark.

```
$ pyspark
Picked up _JAVA_OPTIONS: -Dawt.useSystemAAFontSettings=on
Python 3.9.6 (default, Jun 30 2021, 10:22:16) 
[GCC 11.1.0] on linux
Type "help", "copyright", "credits" or "license" for more information.
Picked up _JAVA_OPTIONS: -Dawt.useSystemAAFontSettings=on
Picked up _JAVA_OPTIONS: -Dawt.useSystemAAFontSettings=on
WARNING: An illegal reflective access operation has occurred
WARNING: Illegal reflective access by org.apache.spark.unsafe.Platform (file:/opt/apache-spark/jars/spark-unsafe_2.12-3.2.0.jar) to constructor java.nio.DirectByteBuffer(long,int)
WARNING: Please consider reporting this to the maintainers of org.apache.spark.unsafe.Platform
WARNING: Use --illegal-access=warn to enable warnings of further illegal reflective access operations
WARNING: All illegal access operations will be denied in a future release
Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
22/05/31 09:05:23 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /__ / .__/\_,_/_/ /_/\_\   version 3.2.0
      /_/

Using Python version 3.9.6 (default, Jun 30 2021 10:22:16)
Spark context Web UI available at http://localhost:4040
Spark context available as 'sc' (master = local[*], app id = local-1653968125458).
SparkSession available as 'spark'.
>>> 
```

## pyspark and jupyter

```
export PYSPARK_DRIVER_PYTHON='jupyter'
export PYSPARK_DRIVER_PYTHON_OPTS='notebook --no-browser --port=8889'
pyspark

Picked up _JAVA_OPTIONS: -Dawt.useSystemAAFontSettings=on
[I 09:16:20.188 NotebookApp] Serving notebooks from local directory: /home/simha_personal_data/programming_arch_firefox/extra/Unsorted/vid/web_dev/hss_iqgateway/apache_spark/jupyter_notebooks
[I 09:16:20.188 NotebookApp] Jupyter Notebook 6.3.0 is running at:
[I 09:16:20.188 NotebookApp] http://localhost:8889/?token=1b42e18ede7dba65aaa6495a4ac45d18c70bacf6576206a1
[I 09:16:20.188 NotebookApp]  or http://127.0.0.1:8889/?token=1b42e18ede7dba65aaa6495a4ac45d18c70bacf6576206a1
[I 09:16:20.189 NotebookApp] Use Control-C to stop this server and shut down all kernels (twice to skip confirmation).
[C 09:16:20.231 NotebookApp] 
    
    To access the notebook, open this file in a browser:
        file:///home/simha/.local/share/jupyter/runtime/nbserver-758649-open.html
    Or copy and paste one of these URLs:
        http://localhost:8889/?token=1b42e18ede7dba65aaa6495a4ac45d18c70bacf6576206a1
     or http://127.0.0.1:8889/?token=1b42e18ede7dba65aaa6495a4ac45d18c70bacf6576206a1
```

Now in jupyter we can access sc and spark session as

![image](https://user-images.githubusercontent.com/6462531/171089204-c6e40c9e-dc7e-4a95-b172-f197e75f88a7.png)

## Another way to use jupyter and spark

### start master

```
# change permissions
sudo chmod -R 777 /opt/apache-spark

# start master
/opt/apache-spark/sbin/start-master.sh
starting org.apache.spark.deploy.master.Master, logging to /opt/apache-spark/logs/spark-simha-org.apache.spark.deploy.master.Master-1-gauranga.out

# check log
cat /opt/apache-spark/logs/spark-simha-org.apache.spark.deploy.master.Master-1-gauranga.out

$ cat /opt/apache-spark/logs/spark-simha-org.apache.spark.deploy.master.Master-1-gauranga.out
Picked up _JAVA_OPTIONS: -Dawt.useSystemAAFontSettings=on
Spark Command: /usr/lib/jvm/default-runtime/bin/java -cp /opt/apache-spark/conf/:/opt/apache-spark/jars/* -Xmx1g org.apache.spark.deploy.master.Master --host gauranga --port 7077 --webui-port 8080
========================================
Picked up _JAVA_OPTIONS: -Dawt.useSystemAAFontSettings=on
Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
22/05/31 08:20:11 INFO Master: Started daemon with process name: 685396@gauranga
22/05/31 08:20:11 INFO SignalUtils: Registering signal handler for TERM
22/05/31 08:20:11 INFO SignalUtils: Registering signal handler for HUP
22/05/31 08:20:11 INFO SignalUtils: Registering signal handler for INT
22/05/31 08:20:11 WARN MasterArguments: SPARK_MASTER_IP is deprecated, please use SPARK_MASTER_HOST
WARNING: An illegal reflective access operation has occurred
WARNING: Illegal reflective access by org.apache.spark.unsafe.Platform (file:/opt/apache-spark/jars/spark-unsafe_2.12-3.2.0.jar) to constructor java.nio.DirectByteBuffer(long,int)
WARNING: Please consider reporting this to the maintainers of org.apache.spark.unsafe.Platform
WARNING: Use --illegal-access=warn to enable warnings of further illegal reflective access operations
WARNING: All illegal access operations will be denied in a future release
22/05/31 08:20:12 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
22/05/31 08:20:12 INFO SecurityManager: Changing view acls to: simha
22/05/31 08:20:12 INFO SecurityManager: Changing modify acls to: simha
22/05/31 08:20:12 INFO SecurityManager: Changing view acls groups to: 
22/05/31 08:20:12 INFO SecurityManager: Changing modify acls groups to: 
22/05/31 08:20:12 INFO SecurityManager: SecurityManager: authentication disabled; ui acls disabled; users  with view permissions: Set(simha); groups with view permissions: Set(); users  with modify permissions: Set(simha); groups with modify permissions: Set()
22/05/31 08:20:13 INFO Utils: Successfully started service 'sparkMaster' on port 7077.
22/05/31 08:20:13 INFO Master: Starting Spark master at spark://gauranga:7077
22/05/31 08:20:13 INFO Master: Running Spark version 3.2.0
22/05/31 08:20:14 WARN Utils: Service 'MasterUI' could not bind on port 8080. Attempting port 8081.
22/05/31 08:20:14 INFO Utils: Successfully started service 'MasterUI' on port 8081.
22/05/31 08:20:14 INFO MasterWebUI: Bound MasterWebUI to localhost, and started at http://localhost:8081
22/05/31 08:20:14 INFO Master: I have been elected leader! New state: ALIVE
```
### install pyspark
```
pip install pyspark
```

### open jupyter notebook and then use spark
https://www.hackdeploy.com/how-to-run-pyspark-in-a-jupyter-notebook/

```
The two last lines of code print the version of spark we are using.

import os
import pyspark
from pyspark.sql import SQLContext, SparkSession
sc = SparkSession \
        .builder \
        .master('spark://xxx.xxx.xx.xx:7077') \
        .appName("sparkFromJupyter") \
        .getOrCreate()
sqlContext = SQLContext(sparkContext=sc.sparkContext, sparkSession=sc)
print("Spark Version: " + sc.version)
print("PySpark Version: " + pyspark.__version__)

```

### Run a Simple PySpark Command

To test our installation we will run a very basic pyspark code. We will create a dataframe and then display it.

```
df = sqlContext.createDataFrame(
    [(1, 'foo'),(2, 'bar')],#records
    ['col1', 'col2']#column names
)
df.show()
```

### jupyter and spark
https://opensource.com/article/18/11/pyspark-jupyter-notebook

Tirthajyoti Sarkar
Sr. Director of AI/ML platform | Stories on Artificial Intelligence, Data Science, and ML | Speaker, Open-source contributor, Author of multiple DS books

https://github.com/tirthajyoti

## first step in spark
https://www.kaggle.com/code/masumrumi/a-pyspark-tutorial-with-titanic/notebook

The first step in using Spark is connecting to a cluster. In practice, the cluster will be hosted on a remote machine that's connected to all other nodes. There will be one computer, called the master that manages splitting up the data and the computations. The master is connected to the rest of the computers in the cluster, which are called worker. The master sends the workers data and calculations to run, and they send their results back to the master.

We definitely don't need may clusters for Titanic dataset. In addition to that, the syntax for running locally or using many clusters are pretty similar. To start working with Spark DataFrames, we first have to create a SparkSession object from SparkContext. We can think of the SparkContext as the connection to the cluster and SparkSession as the interface with that connection. Let's create a SparkSession.

