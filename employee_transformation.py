from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, when

# Step 1: Create Spark session
spark = SparkSession.builder \
    .appName("Employee Data Cleaning") \
    .getOrCreate()

# Step 2: Read CSV file
df = spark.read.csv("employee.csv", header=True, inferSchema=True)

# Step 3: Initial inspection
df.printSchema()
df.show()

# Step 4: Data Cleaning Transformations
cleaned_df = df \
    .dropDuplicates() \
    .na.drop(subset=["employee_id", "name"]) \
    .withColumn("name", trim(col("name"))) \
    .withColumn("email", trim(col("email"))) \
    .withColumn("salary", when(col("salary") < 0, None).otherwise(col("salary"))) \
    .na.fill({"salary": 0}) \
    .filter(col("age") >= 18)

# Step 5: Show cleaned data
cleaned_df.show()
