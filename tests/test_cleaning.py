"""Unit tests for src/transforms/cleaning.py.

The pattern for every test: build a TINY DataFrame in memory (a few rows), run one
transform, assert on the result. No files, no cluster. This is what runs in CI.

Uncomment the import + body once you've implemented the function.
"""
# from src.transforms.cleaning import filter_valid_trips


def test_filter_valid_trips_drops_negative_fare(spark):
    # ARRANGE — one good row, one bad row (negative fare):
    #   rows = [(1, 10.0, 1), (2, -5.0, 1)]
    #   df = spark.createDataFrame(rows, ["trip_id", "fare_amount", "passenger_count"])
    # ACT:
    #   out = filter_valid_trips(df)
    # ASSERT:
    #   assert out.count() == 1
    #   assert out.collect()[0]["trip_id"] == 1
    # TODO: implement once filter_valid_trips exists.
    pass
