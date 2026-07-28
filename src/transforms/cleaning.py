"""Pure PySpark transforms for the Silver layer.

Keep every function PURE: DataFrame in -> DataFrame out. No I/O, no globals, no display().
That is what makes them unit-testable in CI (tests/test_cleaning.py) AND reusable inside
the Databricks notebooks. Never .collect() a full DataFrame here — stay distributed.
"""
from pyspark.sql import DataFrame


def filter_valid_trips(df: DataFrame) -> DataFrame:
    """Drop physically-impossible trips.

    TODO: filter out rows where any of these hold —
      - fare_amount < 0  or  total_amount < 0
      - passenger_count <= 0
      - trip_distance < 0
      - dropoff timestamp earlier than pickup timestamp
    Return the filtered DataFrame.
    """
    raise NotImplementedError("TODO: implement filter_valid_trips")


def cast_types(df: DataFrame) -> DataFrame:
    """Cast columns to explicit target types (ints, doubles, timestamps).

    TODO: cast the numeric + timestamp columns; don't rely on inferred types.
    """
    raise NotImplementedError("TODO: implement cast_types")


def deduplicate(df: DataFrame, keys: list[str]) -> DataFrame:
    """Drop duplicate rows on the given business key(s).

    TODO: decide the right trip key first, then df.dropDuplicates(keys).
    """
    raise NotImplementedError("TODO: implement deduplicate")


def add_pickup_date(df: DataFrame) -> DataFrame:
    """Derive a pickup_date (DateType) column from the pickup timestamp, for partitioning.

    TODO: use to_date() on the pickup datetime column.
    """
    raise NotImplementedError("TODO: implement add_pickup_date")
