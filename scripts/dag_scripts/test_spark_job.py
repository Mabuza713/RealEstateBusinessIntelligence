from pyspark.sql import SparkSession


def main() -> None:
    spark = (
        SparkSession.builder.appName("TestSparkAppFromAirflow")
        .getOrCreate()
    )
    df = spark.createDataFrame(
        [("hello", 1), ("world", 2)],
        ["word", "count"],
    )
    df.show()
    print(f"Row count: {df.count()}")
    spark.stop()


if __name__ == "__main__":
    main()
