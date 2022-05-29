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
