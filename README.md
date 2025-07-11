# NYC Taxi Dataset Analysis using PySpark

This project performs exploratory and analytical processing on the NYC Yellow Taxi Trip Dataset using Apache Spark in Python (PySpark).

## 📁 Dataset

**Source**: [Kaggle](https://www.kaggle.com/datasets)
**File Used**: [yellow_tripdata_2020-01.csv](https://www.kaggle.com/datasets/gauravpathak1789/yellow-tripdata-2020-01)  
**Format**: CSV (~7M records)

## 🔧 Tools & Environment

- Apache Spark (3.x)
- PySpark
- Google Colab / Azure Databricks / Local Spark
- Python 3.8+
- Dataset loaded via Spark DataFrame

## ✅ Tasks Completed

1. **Loaded dataset into PySpark DataFrame**
2. **Added `Revenue` column**:
   Sum of fare_amount, extra, mta_tax, tip_amount, tolls_amount, and total_amount
3. **Passenger count grouped by Pickup Location**
4. **Average total earning and fare by each vendor**
5. **Moving count of payments by payment type (per hour)**
6. **Top 2 highest-earning vendors per day (with passengers & distance)**
7. **Most common route based on passenger count**
8. **Top pickup locations in last 5–10 seconds (simulated window)**

## 📊 Sample PySpark Code

```python
df = spark.read.csv("yellow_tripdata_2020-01.csv", header=True, inferSchema=True)
df = df.withColumn("Revenue", col("fare_amount") + col("extra") + col("mta_tax") +
                   col("improvement_surcharge") + col("tip_amount") + col("tolls_amount") +
                   col("total_amount"))
