from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, count, avg, sum, max, min, desc, asc

# Initialize Spark session
spark = SparkSession.builder.appName("DataFrameOperationsTest").getOrCreate()

# Sample data
data = [
    (1, "Alice", 23, "A"),
    (2, "Bob", 29, "B"),
    (3, "Charlie", 31, "C"),
    (4, "David", 22, "B"),
    (5, "Eva", 25, "A"),
    (6, "Frank", None, "C")
]

# Create DataFrame
df = spark.createDataFrame(data, ["id", "name", "age", "grade"])

# Show original data
print("Original DataFrame:")
df.show()

# Select and Rename Columns
df.select("name", "age").show()
df.withColumnRenamed("grade", "class_grade").show()

# Filter Rows
df.filter(col("age") > 25).show()
df.filter(col("grade") == "A").show()

# Add New Column
df.withColumn("age_plus_10", col("age") + 10).show()

# Conditional Column
df.withColumn("status", when(col("age") >= 25, "Senior").otherwise("Junior")).show()

# Drop Column
df.drop("grade").show()

# GroupBy and Aggregations
df.groupBy("grade").agg(
    count("*").alias("count"),
    avg("age").alias("avg_age"),
    max("age").alias("max_age")
).show()

# Sort
df.sort("age").show()
df.sort(desc("age")).show()

# Join
df2 = spark.createDataFrame([(1, "Math"), (2, "Science"), (3, "History")], ["id", "subject"])
df.join(df2, on="id", how="inner").show()

# Union
df3 = spark.createDataFrame([(7, "Grace", 28, "B")], ["id", "name", "age", "grade"])
df.union(df3).show()

# Drop Duplicates
df_with_dupes = df.union(df)
df_with_dupes.dropDuplicates().show()

# Null Handling
df.filter(col("age").isNull()).show()
df.na.fill({"age": 0}).show()

# Describe
df.describe().show()

# Cache and Persist
df.cache()
df.persist()

# Stop Spark session
spark.stop()
