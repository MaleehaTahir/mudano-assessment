from base.ingestion import *
from datetime import datetime
from helper.cleaup import *
from base.conn import *
import pandas as pd
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(filename='main.log', level=logging.INFO)

start = 1
end = 7
# orchestrator

fileConfig = config('../database.ini')
load_date = datetime.today().strftime('%Y-%m-%d')


def extract_transform_load_geolocation():
    logger.info("starting ingestion for country data")

    for page in range(start, end):
        try:
            country_url = 'http://api.worldbank.org/v2/country/?page=%s&format=json' % page
            ingest_geolocation_data(country_url)
        except Exception as e:  # need to log exceptions
            raise e
    df = pd.concat(country_list).reset_index(drop=True)
    logger.info("successfully created a dataframe")

    try:
        df.columns = cleaup_columns(df, '.', '')
        logger.info("renamed column header values")
    except Exception as e:
        logger.exception("unable to get a response")
        raise print(str(e))

    try:
        df2_to_postgres(fileConfig, 'country', df)
        logger.info("successfully loaded data into database")
    except Exception as e:
        raise e


def extract_transform_load_gdp(download_url, keyCols):
    logger.info("strating ingestion process to download gdp files")

    extract_downloaded_files = ingest_gdp_data(download_url, str(load_date).replace('-', '/'))

    filepath = "../outputs/downloaded/"+str(load_date).replace('-', '/')+"/"+"api_ny_gdp_mktp_cd_"+str(load_date).replace('-','')+".csv"

    try:
        gdp_df = stack_columns(filepath, 4, keyCols, 'Year', 'GDPValue')
        gdp_df.columns = cleaup_columns(gdp_df, ' ', '')
        df2_to_postgres(fileConfig, 'countryGDP', gdp_df)
        logger.info("successfully loaded gdp data in the database")
    except Exception as e:
        raise e

    return extract_downloaded_files


if __name__ == "__main__":
    validate_database_objects('../database.ini')
    extract_transform_load_geolocation()

    key_cols = ['Country Name', 'Country Code', 'Indicator Name', 'Indicator Code']
    extract_transform_load_gdp('http://api.worldbank.org/v2/en/indicator/NY.GDP.MKTP.CD?downloadformat=csv', key_cols)

    export_from_postgres(fileConfig, 'country', str(load_date).replace('-', '/'), 'country.csv')
    export_from_postgres(fileConfig, '"countryGDP"', str(load_date).replace('-', '/'), 'countryGDP.csv')















