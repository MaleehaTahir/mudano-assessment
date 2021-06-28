from base.query_functions import (
    upper_middle_income_countries,
    get_row_count,
    low_income_regions)
import pytest, logging
import pandas as pd

# example below of how to setup a test with spark- though working with pyspark dfs is not very intuitive
logging.basicConfig(filename='test_log.log', level=logging.DEBUG)
logger = logging.getLogger(__name__)


@pytest.mark.usefixtures("spark")
def test_match_row_count(spark):
    logging.debug("testing process function")
    input = 297
    output = list(range(input + 1))
    df = pd.DataFrame(output)
    spark.createDataFrame(df).createOrReplaceTempView("test_country")

    expected = spark.sql("""SELECT COUNT(*) row_count from test_country""").collect()[0]
    actual = get_row_count(spark)

    assert actual == expected


df = pd.read_csv('../outputs/ingested/country.csv')


@pytest.fixture()
def test_upper_middle_countries():
    logging.debug("testing process function")
    result = upper_middle_income_countries(df)

    assert result["answer"] == {"high_income_countries": 56}


@pytest.fixture
def test_low_income_countries():
    logging.debug("testing process function")
    result = low_income_regions(df)

    assert result["answer"] == {"cancelled": 12}


if __name__ == "__main__":
    logging.info('test started')
    test_match_row_count()
    test_upper_middle_countries()
    test_low_income_countries()
    logging.info('test completed')
