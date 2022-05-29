# terms
- Spark workflows
- SparkSession
- getOrCreate
- getActiveSession
- 
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

- This post explains how to create a SparkSession with `getOrCreate` and how to reuse the SparkSession with `getActiveSession`.
- You need a SparkSession to read data stored in files, when manually creating DataFrames, and to run arbitrary SQL queries.
- The SparkSession should be instantiated once and then reused throughout your application.
- Most applications should not create multiple sessions or shut down an existing session.
- 
