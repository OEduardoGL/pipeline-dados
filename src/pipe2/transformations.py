from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def prepare_customers(df: DataFrame) -> DataFrame:
    out = (
        df.withColumn("customer_id", F.col("customer_id").cast("int"))
        .withColumn("signup_date", F.to_date(F.col("signup_date"), "yyyy-MM-dd"))
        .filter(F.col("customer_id").isNotNull())
        .dropDuplicates(["customer_id"])  # idempotência/estabilidade
    )
    return out


def prepare_orders(df: DataFrame) -> DataFrame:
    out = (
        df.withColumn("order_id", F.col("order_id").cast("int"))
        .withColumn("customer_id", F.col("customer_id").cast("int"))
        .withColumn("order_date", F.to_date(F.col("order_date"), "yyyy-MM-dd"))
        .withColumn("is_cancelled", F.when(F.lower(F.col("status")) == F.lit("cancelled"), F.lit(True)).otherwise(F.lit(False)))
        .filter(F.col("order_id").isNotNull())
        .dropDuplicates(["order_id"])  # garantir 1 linha por pedido
    )
    return out


def prepare_order_items(df: DataFrame) -> DataFrame:
    out = (
        df.withColumn("order_id", F.col("order_id").cast("int"))
        .withColumn("item_id", F.col("item_id").cast("int"))
        .withColumn("quantity", F.col("quantity").cast("int"))
        .withColumn("unit_price", F.col("unit_price").cast("double"))
        .withColumn("line_total", F.col("quantity") * F.col("unit_price"))
        .filter(F.col("order_id").isNotNull())
        .filter(F.col("item_id").isNotNull())
        .dropDuplicates(["order_id", "item_id"])  # único por item
    )
    return out


def gold_sales_by_date_category(orders: DataFrame, items: DataFrame) -> DataFrame:
    shipped = orders.filter(~F.col("is_cancelled")).select("order_id", "order_date")

    joined = (
        items.join(shipped, on="order_id", how="inner")
        .filter(F.col("line_total").isNotNull())
    )

    agg = (
        joined.groupBy("order_date", "category")
        .agg(
            F.sum("line_total").alias("revenue"),
            F.sum("quantity").alias("items_sold"),
            F.countDistinct("order_id").alias("orders"),
        )
        .orderBy("order_date", "category")
    )
    return agg

