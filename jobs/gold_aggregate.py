import argparse

from pipe2.config import load_config, resolve_path
from pipe2.session import create_spark
from pipe2.io import read_parquet, write_parquet
from pipe2.transformations import gold_sales_by_date_category


def main(config_path: str):
    cfg = load_config(config_path)
    spark = create_spark(cfg.get("app_name", "Pipe2"), cfg)

    silver = cfg["paths"]["silver"]
    gold = cfg["paths"]["gold"]
    write_mode = cfg.get("write", {}).get("mode", "overwrite")
    partition_gold = cfg.get("write", {}).get("partitionBy_gold")

    # read silver
    orders_sv = read_parquet(spark, resolve_path(silver, "orders"))
    items_sv = read_parquet(spark, resolve_path(silver, "order_items"))

    # aggregate
    sales = gold_sales_by_date_category(orders_sv, items_sv)

    # write gold
    write_parquet(
        sales,
        resolve_path(gold, "sales_by_date_category"),
        mode=write_mode,
        partitionBy=partition_gold,
    )

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Gold aggregates (business metrics)")
    parser.add_argument("--config", required=True, help="Path para configs JSON")
    args = parser.parse_args()
    main(args.config)

