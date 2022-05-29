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

