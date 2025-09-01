from typing import Iterable, Optional

from pyspark.sql import DataFrame, SparkSession


def read_csv(
    spark: SparkSession,
    path: str,
    schema,
    options: Optional[dict] = None,
) -> DataFrame:
    opts = options or {}
    return spark.read.options(**opts).schema(schema).csv(path)


def read_parquet(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.parquet(path)


def write_parquet(
    df: DataFrame,
    path: str,
    mode: str = "overwrite",
    partitionBy: Optional[Iterable[str]] = None,
) -> None:
    writer = df.write.mode(mode)
    if partitionBy:
        writer = writer.partitionBy(list(partitionBy))
    writer.parquet(path)

