"""Reusable data-quality assertions for the Gold layer.

Design rule: a failing check RAISES (fails the pipeline). That is the difference between
"I wrote checks" and "I gate the pipeline on them" — say exactly that in interviews.
Each function does its counting with Spark aggregations, not by pulling data to the driver.
"""
from pyspark.sql import DataFrame


class DataQualityError(Exception):
    """Raised when a data-quality check fails."""


def assert_not_null(df: DataFrame, cols: list[str]) -> None:
    """Raise DataQualityError if any of `cols` contains a null.

    TODO: for each column, count nulls (filter isNull + count, or an agg); raise if > 0.
    """
    raise NotImplementedError("TODO: implement assert_not_null")


def assert_unique(df: DataFrame, keys: list[str]) -> None:
    """Raise DataQualityError if `keys` are not unique.

    TODO: groupBy(keys).count().filter("count > 1") — if any rows, raise.
    """
    raise NotImplementedError("TODO: implement assert_unique")


def assert_row_count_between(df: DataFrame, low: int, high: int) -> None:
    """Raise DataQualityError if the row count is outside [low, high].

    TODO: n = df.count(); raise if n < low or n > high.
    """
    raise NotImplementedError("TODO: implement assert_row_count_between")


def assert_referential_integrity(fact: DataFrame, dim: DataFrame, fk: str, pk: str) -> None:
    """Raise DataQualityError if any fact.fk has no matching dim.pk.

    TODO: left-anti join fact to dim on fk == pk; if the result has any rows, raise.
    """
    raise NotImplementedError("TODO: implement assert_referential_integrity")
