import argparse

from pipe2.config import load_config, resolve_path
from pipe2.session import create_spark
from pipe2.io import read_parquet, write_parquet
from pipe2.transformations import (
    prepare_customers,
    prepare_orders,
    prepare_order_items,
)


def main(config_path: str):
    cfg = load_config(config_path)
    spark = create_spark(cfg.get("app_name", "Pipe2"), cfg)

    bronze = cfg["paths"]["bronze"]
    silver = cfg["paths"]["silver"]
    write_mode = cfg.get("write", {}).get("mode", "overwrite")
    partition_orders = cfg.get("write", {}).get("partitionBy_orders")

    # read bronze
    customers_bz = read_parquet(spark, resolve_path(bronze, "customers"))
    orders_bz = read_parquet(spark, resolve_path(bronze, "orders"))
    items_bz = read_parquet(spark, resolve_path(bronze, "order_items"))

    # transform
    customers_silver = prepare_customers(customers_bz)
    orders_silver = prepare_orders(orders_bz)
    items_silver = prepare_order_items(items_bz)

    # write silver
    write_parquet(customers_silver, resolve_path(silver, "customers"), mode=write_mode)
    write_parquet(
        orders_silver,
        resolve_path(silver, "orders"),
        mode=write_mode,
        partitionBy=partition_orders,
    )
    write_parquet(items_silver, resolve_path(silver, "order_items"), mode=write_mode)

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Silver refine (clean, types, dedup, joins)")
    parser.add_argument("--config", required=True, help="Path para configs JSON")
    args = parser.parse_args()
    main(args.config)

