from pyspark.sql import SparkSession

from src.config import SHUFFLE_PARTITIONS


def create_spark_session(
    app_name="YottaFlex_Skew_Aware_Pipeline"
):
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config(
            "spark.sql.shuffle.partitions",
            str(SHUFFLE_PARTITIONS)
        )
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    return spark
