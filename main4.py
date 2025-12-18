#!/usr/bin/env python3
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def main():
    if len(sys.argv) != 4:
        print("Usage: ./main4.py <input_csv> <map_csv> <output_dir>")
        sys.exit(1)

    input_path = sys.argv[1]
    map_path = sys.argv[2]
    output_dir = sys.argv[3]

    # Create Spark session
    spark = (
        SparkSession.builder
        .appName("CS440-Proj2-Task4")
        .getOrCreate()
    )

    # Read taxi trip data
    trips = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(input_path)
    )

    # Read zone/borough map data
    # Expecting typical TLC schema: LocationID, Borough, Zone, ...
    zones = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(map_path)
    )

    # 1) Filter: trip_distance > 2
    trips_f = trips.filter(F.col("trip_distance") > 2)

    # 2) Extract pickup hour (0-23) from tpep_pickup_datetime
    pickup_ts = F.to_timestamp(F.col("tpep_pickup_datetime"))
    trips_with_hour = trips_f.withColumn("hour", F.hour(pickup_ts))

    # 3) Keep only pickups whose PULocationID is in Brooklyn
    #    We assume map has columns: LocationID, Borough, Zone
    brooklyn_zones = zones.filter(F.col("Borough") == "Brooklyn")

    joined = (
        trips_with_hour
        .join(
            brooklyn_zones,
            trips_with_hour["PULocationID"] == brooklyn_zones["LocationID"],
            how="inner",
        )
    )

    # Now we have columns including: hour (int), Zone (string)
    # 4) Count pickups per (hour, Zone)
    hourly_counts = (
        joined
        .groupBy("hour", "Zone")
        .agg(F.count("*").alias("pickups"))
    )

    # 5) For each hour, pick the Zone with max pickups,
    #    tie-break by Zone alphabetical ascending
    w = Window.partitionBy("hour").orderBy(
        F.col("pickups").desc(),
        F.col("Zone").asc()
    )

    ranked = hourly_counts.withColumn("rn", F.row_number().over(w))

    best_per_hour = ranked.filter(F.col("rn") == 1)

    # 6) Format hour as two digits "00".."23" and select final columns
    result = (
        best_per_hour
        .select(
            F.lpad(F.col("hour").cast("string"), 2, "0").alias("hour"),
            F.col("Zone")
        )
        .orderBy(F.col("hour").asc())
    )

    # 7) Write output: "HH,Zone" per line, no header
    (
        result
        .write
        .mode("overwrite")
        .option("header", "false")
        .csv(output_dir)
    )

    spark.stop()


if __name__ == "__main__":
    main()
