"""Shared pytest fixtures. Provides a local SparkSession for unit tests.

Requires a Python 3.11 venv with pyspark installed (see requirements.txt).
This is boilerplate — you don't need to change it.
"""
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("project1-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield session
    session.stop()
