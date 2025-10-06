from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper, when

# Step 1: Create Spark session
spark = SparkSession.builder \
    .appName("Student Data Transformation") \
    .getOrCreate()

# Step 2: Read student data from CSV
# Assuming the file has columns: student_id, name, age, grade
df = spark.read.csv("students.csv", header=True, inferSchema=True)

# Step 3: Basic transformations
# - Convert names to uppercase
# - Add a new column 'status' based on grade
transformed_df = df.withColumn("name_upper", upper(col("name"))) \
    .withColumn("status", when(col("grade") >= 60, "Pass").otherwise("Fail"))

# Step 4: Show transformed data
transformed_df.show()

# Optional: Save transformed data
# transformed_df.write.csv("transformed_students.csv", header=True)
