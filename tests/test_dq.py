"""Unit tests for src/dq/checks.py."""
# from src.dq.checks import assert_unique, DataQualityError


def test_assert_unique_raises_on_duplicates(spark):
    # ARRANGE — a duplicate key:
    #   df = spark.createDataFrame([(1,), (1,), (2,)], ["id"])
    # ACT + ASSERT — the check should raise:
    #   import pytest
    #   with pytest.raises(DataQualityError):
    #       assert_unique(df, ["id"])
    # TODO: implement once assert_unique exists.
    pass


def test_assert_unique_passes_on_unique_keys(spark):
    # df = spark.createDataFrame([(1,), (2,), (3,)], ["id"])
    # assert_unique(df, ["id"])   # should NOT raise
    # TODO.
    pass
