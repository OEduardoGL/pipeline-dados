import argparse
import os

from pipe2.config import load_config, resolve_path
from pipe2.session import create_spark
from pipe2.io import read_csv, write_parquet
from pipe2.schemas import customers_schema, orders_schema, order_items_schema
from pipe2.quality import check_min_rows, check_non_nulls, validate_or_raise


def main(config_path: str):
    cfg = load_config(config_path)
    spark = create_spark(cfg.get("app_name", "Pipe2"), cfg)

    raw = cfg["paths"]["raw"]
    bronze = cfg["paths"]["bronze"]
    csv_opts = cfg.get("csv_options", {})
    write_mode = cfg.get("write", {}).get("mode", "overwrite")

    # customers
    customers_raw = resolve_path(raw, "customers.csv")
    customers_df = read_csv(spark, customers_raw, customers_schema, csv_opts)

    ok1 = check_min_rows(customers_df, 1)
    ok2 = check_non_nulls(customers_df, ["customer_id"])  # chave não nula
    validate_or_raise(ok1, ok2)

    write_parquet(customers_df, resolve_path(bronze, "customers"), mode=write_mode)

    # orders
    orders_raw = resolve_path(raw, "orders.csv")
    orders_df = read_csv(spark, orders_raw, orders_schema, csv_opts)
    ok3 = check_min_rows(orders_df, 1)
    ok4 = check_non_nulls(orders_df, ["order_id"])  # chave não nula
    validate_or_raise(ok3, ok4)

    write_parquet(
        orders_df,
        resolve_path(bronze, "orders"),
        mode=write_mode,
    )

    # order_items
    items_raw = resolve_path(raw, "order_items.csv")
    items_df = read_csv(spark, items_raw, order_items_schema, csv_opts)
    ok5 = check_min_rows(items_df, 1)
    ok6 = check_non_nulls(items_df, ["order_id", "item_id"])
    validate_or_raise(ok5, ok6)

    write_parquet(
        items_df,
        resolve_path(bronze, "order_items"),
        mode=write_mode,
    )

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Bronze ingestion (raw CSV -> bronze Parquet)")
    parser.add_argument("--config", required=True, help="Path para configs JSON")
    args = parser.parse_args()
    main(args.config)

