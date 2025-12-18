#!/usr/bin/env python3
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main():
    if len(sys.argv) != 3:
        print("Usage: ./main3.py <input_csv> <output_dir>")
        sys.exit(1)

    input_path = sys.argv[1]
    output_dir = sys.argv[2]

    # Create Spark session
    spark = (
        SparkSession.builder
        .appName("CS440-Proj2-Task3")
        .getOrCreate()
    )

    # Read input CSV
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(input_path)
    )

    # 1) Filter: trip_distance > 2
    df_filtered = df.filter(F.col("trip_distance") > 2)

    # 2) Extract pickup date and weekday name
    # Ensure we have a proper timestamp, then derive date and weekday
    pickup_ts = F.to_timestamp(F.col("tpep_pickup_datetime"))
    pickup_date = F.to_date(pickup_ts)

    df_with_date = df_filtered.withColumn("pickup_date", pickup_date)

    # 3) Count pickups per calendar date
    daily_counts = (
        df_with_date
        .groupBy("pickup_date")
        .agg(F.count("*").alias("daily_pickups"))
    )

    # 4) Attach weekday name (full name, e.g., Monday)
    daily_with_weekday = daily_counts.withColumn(
        "weekday",
        F.date_format(F.col("pickup_date"), "EEEE")
    )

    # 5) Average daily pickups per weekday
    weekday_avg = (
        daily_with_weekday
        .groupBy("weekday")
        .agg(F.avg("daily_pickups").alias("avg_pickups"))
    )

    # 6) Order: descending average pickups, then weekday name ascending (tie-break)
    top3 = (
        weekday_avg
        .orderBy(
            F.col("avg_pickups").desc(),
            F.col("weekday").asc()
        )
        .limit(3)
        .select("weekday")
    )

    # 7) Write output: one weekday per line, no header
    (
        top3
        .write
        .mode("overwrite")
        .option("header", "false")
        .csv(output_dir)
    )

    spark.stop()


if __name__ == "__main__":
    main()
