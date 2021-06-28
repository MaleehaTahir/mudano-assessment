from pandas.io.json import json_normalize
from helper.cleaup import *
import requests, zipfile, io


country_list = []


def ingest_geolocation_data(url):
    try:
        response = requests.get(url, headers={"content-type": "application/json"}).json()
        _df = json_normalize(response[1])
    except requests.exceptions.HTTPError as e:
        raise SystemExit(e)
    return country_list.append(_df)


def ingest_gdp_data(url, load_date):
    response = requests.get(url)
    zipped_file = zipfile.ZipFile(io.BytesIO(response.content))
    zipped_info = zipped_file.infolist()
    for file in zipped_info:
        outpath = "../outputs/downloaded/"+load_date+"/"
        file.filename = rename_csv_files(file.filename, '.', 0,'_GDP_MKTP_CD_', )
        zipped_file.extract(file, outpath)