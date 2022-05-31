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

# Various Entry Points for Apache Spark
https://www.npntraining.com/blog/various-entry-points-for-apache-spark/

In Spark 1.x, three entry points were introduced:

1. SparkContext,
2. SQLContext and
3. HiveContext

Since Spark 2.x, a new entry point called SparkSession has been introduced that essentially combined all functionalities available in the three aforementioned contexts. Note that all contexts are still available even in newest Spark releases, mostly for backward compatibility purposes.

## Spark Context

The Spark Context is used by the Driver Process of the Spark Application in order to establish a communication with the cluster and the resource managers in order to coordinate and execute jobs. SparkContext also enables the access to the other two contexts, namely SQLContext and HiveContext (more on these entry points later on).

In order to create a SparkContext, you will first need to create a Spark Configuration

```
from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName('app')
.setMaster(master)
sc = SparkContext(conf=conf)
```

Note : if you are using the spark-shell, SparkContext is already available through the variable called sc

## SqlContext

SQLContext is the entry point to SparkSQL which is a Spark module for structured data processing. Once SQLContext is initialized, the user can then use it in order to perform various “sql-like” operations over Datasets and Dataframes. In order to create a SQLContext, you first need to instantiate a SparkContext as shown below:

```
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
conf = SparkConf().setAppName('app')
.setMaster(master)
sc = SparkContext(conf=conf)
sql_context = SQLContext(sc)
```

## SparkSession

Spark 2.0 introduced a new entry point called SparkSession that essentially replaced both SQLContext and HiveContext. Additionally, it gives to developers immediate access to SparkContext. In order to create a SparkSession with Hive support, all you have to do is

```
from pyspark.sql import SparkSession
spark_session = SparkSession.builder.enableHiveSupport().getOrCreate()
```

Two ways you can access spark context from spark session

```
spark_context = spark_session._sc
spark_context = spark_session.sparkContex
```
# How to Check Spark Version
https://sparkbyexamples.com/spark/check-spark-version/

## 1. Spark Version Check from Command Line

```
spark-submit --version
spark-shell --version
spark-sql --version
```

```
$ spark-shell --version
Picked up _JAVA_OPTIONS: -Dawt.useSystemAAFontSettings=on
Picked up _JAVA_OPTIONS: -Dawt.useSystemAAFontSettings=on
WARNING: An illegal reflective access operation has occurred
WARNING: Illegal reflective access by org.apache.spark.unsafe.Platform (file:/opt/apache-spark/jars/spark-unsafe_2.12-3.2.0.jar) to constructor java.nio.DirectByteBuffer(long,int)
WARNING: Please consider reporting this to the maintainers of org.apache.spark.unsafe.Platform
WARNING: Use --illegal-access=warn to enable warnings of further illegal reflective access operations
WARNING: All illegal access operations will be denied in a future release
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 3.2.0
      /_/
                        
Using Scala version 2.12.15, OpenJDK 64-Bit Server VM, 11.0.12
Branch HEAD
Compiled by user ubuntu on 2021-10-06T12:46:30Z
Revision 5d45a415f3a29898d92380380cfd82bfc7f579ea
Url https://github.com/apache/spark
Type --help for more information.
```

## 2. Version Check From Spark Shell

Additionally, you are in spark-shell and you wanted to find out the spark version without exiting spark-shell, you can achieve this by using the sc.version. sc is a SparkContect variable that default exists in spark-shell

```
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
22/05/31 13:56:36 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
22/05/31 13:56:38 WARN Utils: Service 'SparkUI' could not bind on port 4040. Attempting port 4041.
Spark context Web UI available at http://localhost:4041
Spark context available as 'sc' (master = local[*], app id = local-1653985598610).
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

scala> sc.version
res0: String = 3.2.0

scala> spark.version
res1: String = 3.2.0
```

# PySpark – What is SparkSession?
https://sparkbyexamples.com/pyspark/pyspark-what-is-sparksession/

Since Spark 2.0 SparkSession has become an entry point to PySpark to work with RDD, and DataFrame. Prior to 2.0, SparkContext used to be an entry point. Here, I will mainly focus on explaining what is SparkSession by defining and describing how to create SparkSession and using default SparkSession spark variable from pyspark-shell.

- It’s object spark is default available in pyspark-shell and it can be created programmatically using SparkSession.
- With Spark 2.0 a new class SparkSession (pyspark.sql import SparkSession) has been introduced.
- SparkSession is a combined class for all different contexts we used to have prior to 2.0 release (SQLContext and HiveContext e.t.c).
- Since 2.0 SparkSession can be used in replace with SQLContext, HiveContext, and other contexts defined prior to 2.0.
- As mentioned in the beginning SparkSession is an entry point to PySpark and creating a SparkSession instance would be the first statement you would write to program with RDD, DataFrame, and Dataset. SparkSession will be created using SparkSession.builder builder patterns.

- You should also know that SparkSession internally creates SparkConfig and SparkContext with the configuration provided with SparkSession.

## How many SparkSessions can you create in a PySpark application?

- You can create as many SparkSession as you want in a PySpark application using either SparkSession.builder() or SparkSession.newSession(). 
- Many Spark session objects are required when you wanted to keep PySpark tables (relational entities) logically separated.

## SparkSession in PySpark shell

- Be default PySpark shell provides “spark” object; which is an instance of SparkSession class. We can directly use this object where required in spark-shell.

## Create SparkSession
- In order to create SparkSession programmatically (in .py file) in PySpark, you need to use the builder pattern method builder() as explained below. 
- getOrCreate() method returns an already existing SparkSession; if not exists, it creates a new SparkSession.

```
# Create SparkSession from builder
import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[1]") \
                    .appName('SparkByExamples.com') \
                    .getOrCreate()
```

- master() – If you are running it on the cluster you need to use your master name as an argument to master(). usually, it would be either yarn or mesos depends on your cluster setup.

  - Use local[x] when running in Standalone mode. x should be an integer value and should be greater than 0; this represents how many partitions it should create when using RDD, DataFrame, and Dataset. Ideally, x value should be the number of CPU cores you have.
- appName() – Used to set your application name.

- getOrCreate() – This returns a SparkSession object if already exists, and creates a new one if not exist.

Note:  SparkSession object spark is by default available in the PySpark shell.

## Create Another SparkSession

- You can also create a new SparkSession using newSession() method. This uses the same app name, master as the existing session. 
- Underlying SparkContext will be the same for both sessions as you can have only one context per PySpark application.

## Get Existing SparkSession
- You can get the existing SparkSession in PySpark using the builder.getOrCreate(), for example.
```
# Get Existing SparkSession
spark3 = SparkSession.builder.getOrCreate
print(spark3)
```

## Using Spark Config
- If you wanted to set some configs to SparkSession, use the config() method.
```
# Usage of config()
spark = SparkSession.builder \
      .master("local[1]") \
      .appName("SparkByExamples.com") \
      .config("spark.some.config.option", "config-value") \
      .getOrCreate()
```

## Using PySpark Configs

- Once the SparkSession is created, you can add the spark configs during runtime or get all configs.
```
# Set Config
spark.conf.set("spark.executor.memory", "5g")

# Get a Spark Config
partions = spark.conf.get("spark.sql.shuffle.partitions")
print(partions)
```

## Create PySpark DataFrame

- SparkSession also provides several methods to create a Spark DataFrame and DataSet. 
- The below example uses the createDataFrame() method which takes a list of data.
```
# Create DataFrame
df = spark.createDataFrame(
    [("Scala", 25000), ("Spark", 35000), ("PHP", 21000)])
df.show()

# Output
#+-----+-----+
#|   _1|   _2|
#+-----+-----+
#|Scala|25000|
#|Spark|35000|
#|  PHP|21000|
#+-----+-----+
```

## Working with Spark SQL

- Using SparkSession you can access PySpark/Spark SQL capabilities in PySpark.
- In order to use SQL features first, you need to create a temporary view in PySpark. 
- Once you have a temporary view you can run any ANSI SQL queries using spark.sql() method.
```
# Spark SQL
df.createOrReplaceTempView("sample_table")
df2 = spark.sql("SELECT _1,_2 FROM sample_table")
df2.show()
```
- PySpark SQL temporary views are session-scoped and will not be available if the session that creates it terminates.
- If you want to have a temporary view that is shared among all sessions and keep alive until the Spark application terminates
- you can create a global temporary view using `createGlobalTempView()`

## Create Hive Table

- As explained above SparkSession is used to create and query Hive tables. 
- Note that in order to do this for testing you don’t need Hive to be installed. 
- saveAsTable() creates Hive managed table. Query the table using spark.sql().

```
# Create Hive table & query it.  
spark.table("sample_table").write.saveAsTable("sample_hive_table")
df3 = spark.sql("SELECT _1,_2 FROM sample_hive_table")
df3.show()
```

## Working with Catalogs
- To get the catalog metadata, PySpark Session exposes catalog variable.
- Note that these methods spark.catalog.listDatabases and spark.catalog.listTables and returns the DataSet.

```
# Get metadata from the Catalog
# List databases
dbs = spark.catalog.listDatabases()
print(dbs)

# Output
#[Database(name='default', description='default database', 
#locationUri='file:/Users/admin/.spyder-py3/spark-warehouse')]

# List Tables
tbls = spark.catalog.listTables()
print(tbls)

#Output
#[Table(name='sample_hive_table', database='default', description=None, tableType='MANAGED', #isTemporary=False), Table(name='sample_hive_table1', database='default', description=None, #tableType='MANAGED', isTemporary=False), Table(name='sample_hive_table121', database='default', #description=None, tableType='MANAGED', isTemporary=False), Table(name='sample_table', database=None, #description=None, tableType='TEMPORARY', isTemporary=True)]
```
# What is Spark SQL?

- Spark SQL integrates relational processing with Spark’s functional programming. It provides support for various data sources and makes it possible to weave SQL queries with code transformations thus resulting in a very powerful tool.

# Why is Spark SQL used?
```
Spark SQL originated as Apache Hive to run on top of Spark and is now integrated with the Spark stack. Apache Hive had certain limitations as mentioned below. Spark SQL was built to overcome these drawbacks and replace Apache Hive.
```

# Is Spark SQL faster than Hive?

Spark SQL is faster than Hive when it comes to processing speed. Below I have listed down a few limitations of Hive over Spark SQL.

### Limitations With Hive:
- Hive launches MapReduce jobs internally for executing the ad-hoc queries. MapReduce lags in the performance when it comes to the analysis of medium-sized datasets (10 to 200 GB).
- Hive has no resume capability. This means that if the processing dies in the middle of a workflow, you cannot resume from where it got stuck.
- Hive cannot drop encrypted databases in cascade when the trash is enabled and leads to an execution error. To overcome this, users have to use the Purge option to skip trash instead of drop. 

These drawbacks gave way to the birth of Spark SQL. But the question which still pertains in most of our minds is,

## Stucture

- With Spark SQL, Apache Spark is accessible to more users and improves optimization for the current ones. 
- Spark runs on both Windows and UNIX-like systems (e.g. Linux, Microsoft, Mac OS). It is easy to run locally on one machine — all you need is to have java installed on your system PATH, or the JAVA_HOME environment variable pointing to a Java installation.

![image](https://user-images.githubusercontent.com/6462531/171136153-6174c073-20f4-44a3-b10d-e569e33d7b22.png)
