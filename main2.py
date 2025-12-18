#!/usr/bin/env python3
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main():
    if len(sys.argv) != 3:
        print("Usage: ./main2.py <input_csv> <output_dir>")
        sys.exit(1)

    input_path = sys.argv[1]
    output_dir = sys.argv[2]

    # Create Spark session
    spark = (
        SparkSession.builder
        .appName("CS440-Proj2-Task2")
        .getOrCreate()
    )

    # Read input CSV
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(input_path)
    )

    # Filter: trip_distance > 2 (required for all tasks)
    df_filtered = df.filter(F.col("trip_distance") > 2)

    # Build a long table of all location IDs involved in pickups and dropoffs
    # Activeness = count of being either a pickup or a dropoff
    pickup_ids = df_filtered.select(F.col("PULocationID").cast("int").alias("LocationID"))
    dropoff_ids = df_filtered.select(F.col("DOLocationID").cast("int").alias("LocationID"))

    all_ids = pickup_ids.union(dropoff_ids).na.drop(subset=["LocationID"])


    # Count total occurrences per LocationID (this IS the activeness)
    activity_counts = (
        all_ids
        .groupBy("LocationID")
        .count()
    )

    # Sort: descending activeness, then ascending LocationID
    top10 = (
        activity_counts
        .orderBy(
            F.col("count").desc(),
            F.col("LocationID").asc()
        )
        .limit(10)
        .select("LocationID")
    )

    # Write output: one ID per line, no header
    (
        top10
        .write
        .mode("overwrite")
        .option("header", "false")
        .csv(output_dir)
    )

    spark.stop()


if __name__ == "__main__":
    main()
