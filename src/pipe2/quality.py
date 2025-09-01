from typing import Iterable, Tuple

from pyspark.sql import DataFrame
from pyspark.sql.functions import col


def check_min_rows(df: DataFrame, min_rows: int) -> Tuple[bool, str]:
    count = df.count()
    ok = count >= min_rows
    return ok, f"min_rows>={min_rows} -> count={count}"


def check_non_nulls(df: DataFrame, cols: Iterable[str]) -> Tuple[bool, str]:
    for c in cols:
        n = df.filter(col(c).isNull()).limit(1).count()
        if n > 0:
            return False, f"column {c} contains NULLs"
    return True, f"non_nulls on {list(cols)}"


def check_unique(df: DataFrame, cols: Iterable[str]) -> Tuple[bool, str]:
    total = df.count()
    distinct = df.select(*list(cols)).dropDuplicates(list(cols)).count()
    ok = total == distinct
    return ok, f"unique on {list(cols)} -> total={total}, distinct={distinct}"


def validate_or_raise(*checks: Tuple[bool, str]) -> None:
    failures = [msg for ok, msg in checks if not ok]
    if failures:
        raise ValueError("Quality check(s) failed: " + "; ".join(failures))

