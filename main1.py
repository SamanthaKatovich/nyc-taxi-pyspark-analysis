#!/usr/bin/env python3
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def main():
    if len(sys.argv) != 3:
        print("Usage: ./main1.py <input_csv> <output_dir>")
        sys.exit(1)

    input_path = sys.argv[1]
    output_dir = sys.argv[2]

    spark = (
        SparkSession.builder
        .appName("CS440-Proj2-Task1")
        .getOrCreate()
    )

    # Read CSV
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(input_path)
    )

    # 🔹 Project-wide rule: ONLY consider trips > 2 miles
    df_filtered = df.filter(F.col("trip_distance") > 2)

    # Group by PULocationID and sum distances
    df_grouped = (
        df_filtered
        .groupBy("PULocationID")
        .agg(F.sum("trip_distance").alias("total_distance"))
    )

    # Format & sort results
    df_result = (
        df_grouped
        .select(
            F.col("PULocationID"),
            F.format_string("%.2f", F.col("total_distance")).alias("total_distance")
        )
        .orderBy(F.col("PULocationID").asc())
    )

    # Write output with NO header
    (
        df_result.write
        .mode("overwrite")
        .option("header", "false")
        .csv(output_dir)
    )

    spark.stop()

if __name__ == "__main__":
    main()
