import pandas as pd
import json
from typing import Dict, List
from pyspark.sql import SparkSession
from pandas.io.json import json_normalize


def process(json_data: List[Dict]) -> pd.DataFrame:
    response = json_normalize(json_data[1])

    return response


def get_row_count(spark: SparkSession):
    get_row_count = spark.sql("""SELECT COUNT(*) row_count from transactions""").collect()[0]
    # print(get_row_count)
    return get_row_count


df = pd.read_csv('../outputs/ingested/country.csv')


def upper_middle_income_countries(df: pd.DataFrame) -> Dict:

    metric_text = "count of countries with income level of 'Upper middle income'?"

    result = df[df.incomeLevelvalue == 'Upper middle income'].shape[0]
    response = {"metric": metric_text, "answer": str(result)}

    return response


def low_income_regions(df: pd.DataFrame) -> Dict:

    metric_text = "count of countries with income level of 'Low income' per region."

    result = df[df.incomeLevelvalue == 'Low income'].shape[0]
    response = {"metric": metric_text, "answer": str(result)}

    return response



