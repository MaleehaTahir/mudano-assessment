from pyspark.sql import SparkSession
import pytest


@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder\
        .master("local[4]")\
        .appName("pytest_test")\
        .config("spark.executorEnv.PYTHONHASHSEED", "0")\
        .getOrCreate()
    return spark
