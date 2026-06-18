import os
import sys
import shutil

# Set environment variables for PySpark worker on Windows (using global python for workers)
os.environ['PYSPARK_PYTHON'] = r'C:\Users\Razy77\AppData\Local\Programs\Python\Python312\python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, desc, round as spark_round

print("Initializing PySpark Session...")
try:
    spark = SparkSession.builder \
        .appName("ITJobMarketAnalysis") \
        .master("local[*]") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()
    print("Spark Session successfully created!")
except Exception as e:
    print(f"Error starting Spark: {e}")
    sys.exit(1)

# Ensure output directory exists and is clean
output_dir = "spark_outputs"
if os.path.exists(output_dir):
    shutil.rmtree(output_dir)
os.makedirs(output_dir, exist_ok=True)

try:
    # 1. Load merged dataset
    print("Reading merged_it_jobs.csv...")
    df = spark.read.options(header=True, delimiter=";", inferSchema=True).csv("merged_it_jobs.csv")
    df.show(5)
    
    # 2. Demand by Role (grouped by Keyword)
    print("Aggregating Job Demand by Role...")
    job_demand = df.groupBy("keyword") \
        .agg(count("*").alias("total_jobs")) \
        .orderBy(desc("total_jobs"))
    
    # Convert to Pandas and save
    job_demand.toPandas().to_csv(f"{output_dir}/job_demand.csv", sep=";", index=False, encoding="utf-8-sig")
    print("Saved job_demand.csv")

    # 3. Average Salary by Role (calculating average of min and max salary)
    print("Aggregating Salary by Role...")
    # Filter out records where salary is null
    df_with_salary = df.filter(col("salary_min").isNotNull() & col("salary_max").isNotNull())
    
    salary_by_role = df_with_salary.withColumn("avg_salary", (col("salary_min") + col("salary_max")) / 2) \
        .groupBy("keyword") \
        .agg(
            count("*").alias("total_jobs_with_salary"),
            spark_round(avg("salary_min"), 2).alias("avg_salary_min"),
            spark_round(avg("salary_max"), 2).alias("avg_salary_max"),
            spark_round(avg("avg_salary"), 2).alias("avg_salary_overall")
        ) \
        .orderBy(desc("avg_salary_overall"))
        
    salary_by_role.toPandas().to_csv(f"{output_dir}/salary_by_role.csv", sep=";", index=False, encoding="utf-8-sig")
    print("Saved salary_by_role.csv")

    # 4. Jobs by Platform
    print("Aggregating Jobs by Platform...")
    jobs_by_platform = df.groupBy("platform") \
        .agg(count("*").alias("total_jobs")) \
        .orderBy(desc("total_jobs"))
        
    jobs_by_platform.toPandas().to_csv(f"{output_dir}/jobs_by_platform.csv", sep=";", index=False, encoding="utf-8-sig")
    print("Saved jobs_by_platform.csv")

    # 5. Jobs by Country
    print("Aggregating Jobs by Country...")
    jobs_by_country = df.groupBy("country") \
        .agg(count("*").alias("total_jobs")) \
        .orderBy(desc("total_jobs"))
        
    jobs_by_country.toPandas().to_csv(f"{output_dir}/jobs_by_country.csv", sep=";", index=False, encoding="utf-8-sig")
    print("Saved jobs_by_country.csv")
    
    print("\nPySpark processing completed successfully! All files exported to spark_outputs/")

except Exception as e:
    print(f"Error during Spark execution: {e}")
finally:
    spark.stop()
    print("Spark Session stopped.")
