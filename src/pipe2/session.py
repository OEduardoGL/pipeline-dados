from pyspark.sql import SparkSession


def create_spark(app_name: str, cfg: dict) -> SparkSession:
    spark_cfg = (cfg.get("spark") or {})
    builder = SparkSession.builder.appName(app_name)

    for k, v in (spark_cfg.get("config") or {}).items():
        builder = builder.config(k, v)

    spark = builder.getOrCreate()

    log_level = spark_cfg.get("logLevel", "WARN")
    spark.sparkContext.setLogLevel(log_level)

    return spark

