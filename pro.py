
import json
import pathlib
import urllib.parse

import geopandas as gpd
import matplotlib
import matplotlib.pyplot as plt
import pandas as pd
import requests
from shapely.geometry import shape

import sqlalchemy as db
import sqlalchemy.dialects.postgresql as pg
import hashlib

from sqlalchemy.orm import declarative_base
from geoalchemy2.types import Geometry
from geoalchemy2.shape import from_shape

from typing import Any


from shapely.wkb import loads as wkb_loads
import geoplot as gplt
from shapely.geometry import Point

import traitlets

import ipywidgets
import unittest

import tempfile
import random
import string
import os

import warnings

# Where data files will be read from/written to - this should already exist
DATA_DIR = pathlib.Path("data")

# ZIPCODE_DATA_FILE = DATA_DIR / "zipcodes" / "ZIP_CODE_040114.shp"
ZIPCODE_DATA_FILE = DATA_DIR / "zipcodes" / "nyc_zipcodes.shp"

ZILLOW_DATA_FILE = DATA_DIR / "zillow_rent_data.csv"

NYC_DATA_APP_TOKEN = "EqSebYPKeoZPWmssyC2rvIPN8"
BASE_NYC_DATA_URL = "https://data.cityofnewyork.us/"
NYC_DATA_311 = "erm2-nwe9.geojson"
NYC_DATA_TREES = "5rq2-4hqu.geojson"

DB_NAME = "apartment"
DB_USER = "postgres"
DB_URL = f"postgresql+psycopg2://{DB_USER}@localhost/{DB_NAME}"
DB_SCHEMA_FILE = "schema.sql"
# directory where DB queries for Part 3 will be saved
QUERY_DIR = pathlib.Path("queries")

BASIC_USER = 'bo8yv64rbrt1cas4iyua598vp'
BASIC_PASS = '5vwh31bomglif6wi66lb1py390txqu57vkgv8319f2kg1hxkuk'


# When FLAG_DEBUG == True, record size will be limited to 100,000
FLAG_DEBUG = False

if not QUERY_DIR.exists():
    QUERY_DIR.mkdir()


# Part 1 start  
def avoid_nan(value):
    """Replace NaN in a cell with None to avoid errors when saving to the database"""
    if pd.isna(value):
        return None
    else:
        return value


def debug_warp(count: int):
    """When FLAG_DEBUG is True, limit record size to 10000"""
    if FLAG_DEBUG:
        return 100000
    else:
        return count


def build_query_url(base_url: str, queries: dict = {}, flag_use_token: bool = True):
    """Build queries into url query string, and add api token to url"""
    result = base_url[:] + "?"

    if flag_use_token:
        result += f"$$app_token={NYC_DATA_APP_TOKEN}&"

    for key in queries:
        result += f"{key}={queries[key]}&"

    return result[:-1]


def get_md5(content: str):
    """Calculate the md5 of a string"""
    if isinstance(content, str):
        content = content.encode()
    md5 = hashlib.md5()
    md5.update(content)
    return md5.hexdigest()


def get_with_cache(url: str, update: bool = False):
    """This function implements a get function with Cache"""
    url_md5 = get_md5(url)
    storage_path = DATA_DIR / url_md5

    print('update or (not storage_path.exists()):',
          update or (not storage_path.exists()))

    if update or (not storage_path.exists()):
        print(f"Downloading {url} ...")

        session = requests.Session()
        # session.headers.update({"X-App-Token": NYC_DATA_APP_TOKEN})
        basic = requests.auth.HTTPBasicAuth(BASIC_USER, BASIC_PASS)

        count = 5

        while count >= 0:
            response = None
            print(f'Download try: {5 + 1 - count}')
            try:
                response = session.get(url, auth=basic)
            except Exception:
                print('Network error, retry')
                count -= 1
                continue

            if response:
                with open(storage_path, "wb") as file_handle:
                    file_handle.write(response.content)
                print(f"Done downloading {url}.")
                break
            else:
                print(
                    f"Download {url} fail, reason:",
                    response.status_code,
                    "message:",
                    response.content.decode(),
                )
                continue

    return storage_path


def download_nyc_geojson_data(url: str, force: bool = False):
    """This function is deprecated because it doesn't support url with query's very well"""
    parsed_url = urllib.parse.urlparse(url)
    url_path = parsed_url.path.strip("/")

    filename = DATA_DIR / url_path

    if force or not filename.exists():
        print(f"Downloading {url} to {filename}...")

        response = requests.get(url)
        if response:
            with open(filename, "w") as file_handle:
                # json.dump(..., f)
                file_handle.write(response.content)
            print(f"Done downloading {url}.")
        else:
            print(f"Download {url} fail.")
            return None
    else:
        print(f"Reading from {filename}...")

    return filename

def load_and_clean_zipcodes(zipcode_datafile) :
    """Loading and cleaning the zip code dataset"""

    gdf = gpd.read_file(zipcode_datafile)

    mapping_k = ['ZIPCODE', 'PO_NAME', 'STATE', 'COUNTY', 'geometry']

    mapping_v = ['zipcode', 'neighborhood', 'state', 'county', 'geometry']

    name_mapping = dict(map(lambda value_key, value_value: (value_key, value_value), mapping_k, mapping_v))

    gdf.crs = 'epsg:2263'

    gdf = gdf.to_crs('epsg:4326')

    # result = pd.DataFrame(gdf)
    result = gdf

    column_to_delete = []

    for column_name in result.columns:

        if column_name not in mapping_k:

            column_to_delete.append(column_name)

    result = result.drop(column_to_delete, axis=1)

    result = result.rename(columns=name_mapping)

    return result


def download_and_clean_311_data():
    """Loading and Cleaning 311 Complaint Data Set"""
    # https://data.cityofnewyork.us/resource/erm2-nwe9.json

    params = {
        '$select': 'count(unique_key)',
        # '$where': 'complaint_type LIKE "%25Noise%25" AND created_date >= "2022-01-01T00:00:00"::floating_timestamp',
        '$where': 'created_date >= "2022-01-01T00:00:00"::floating_timestamp',
    }

    query_url = build_query_url(
        'https://data.cityofnewyork.us/resource/erm2-nwe9.json', params, False)

    print('Will call get_with_cache() ... 1')
    dataset_path = get_with_cache(query_url, update=False)
    print('     Call get_with_cache() ... 1 ... end')

    jsonStr = '[]'

    with open(dataset_path, 'rb') as file_handle:
        jsonStr = file_handle.read().decode()

    row_count_since_20220101 = debug_warp(
        int(json.loads(jsonStr)[0]['count_unique_key']))

    query_url = build_query_url(
        'https://data.cityofnewyork.us/resource/erm2-nwe9.json', params, False)

    collected_row_count = 0

    result = gpd.GeoDataFrame()

    print(f'Report {collected_row_count} / {row_count_since_20220101} ...')

    while collected_row_count < row_count_since_20220101:

        print(
            f'Collect {collected_row_count} / {row_count_since_20220101} ...')

        params = {
            '$select': 'unique_key, created_date, complaint_type, incident_zip, latitude, longitude',
            # '$where': 'complaint_type LIKE "%25Noise%25" AND created_date >= "2022-01-01T00:00:00"::floating_timestamp',
            '$where': 'created_date >= "2022-01-01T00:00:00"::floating_timestamp',
            '$limit': '150000',
            '$offset': collected_row_count
        }

        query_url = build_query_url(
            'https://data.cityofnewyork.us/resource/erm2-nwe9.json', params, False)

        print('Will call get_with_cache() ... 2')
        dataset_path = get_with_cache(query_url)

        jsonStr = '[]'

        with open(dataset_path, 'rb') as file_handle:
            jsonStr = file_handle.read().decode()

        jsonObject = json.loads(jsonStr)

        part_dataframe = pd.DataFrame.from_records(jsonObject)

        result = pd.concat([result, part_dataframe], ignore_index=True)

        collected_row_count += len(jsonObject)

    result['incident_zip'].fillna(value=-1, inplace=True)
    result['geometry'] = gpd.points_from_xy(
        result['longitude'], result['latitude'])
    result = result.drop(['latitude', 'longitude'], axis=1)

    return result

def download_and_clean_tree_data():
    """Load and clean the tree dataset"""

    # https://data.cityofnewyork.us/resource/5rq2-4hqu.json

    # ["tree_id", "spc_common", "zipcode", "status", "the_geom"]

    params = {
        "$select": "count(tree_id)"
    }

    query_url = build_query_url(
        "https://data.cityofnewyork.us/resource/5rq2-4hqu.json", params, False
    )

    dataset_path = get_with_cache(query_url, update=False)

    jsonStr = "[]"

    with open(dataset_path, "rb") as file_handle:

        jsonStr = file_handle.read().decode()

    row_count = debug_warp(int(json.loads(jsonStr)[0]["count_tree_id"]))

    collected_row_count = 0

    result = gpd.GeoDataFrame()

    while collected_row_count < row_count:

        print(f"Collect {collected_row_count} / {row_count} ...")

        params = {
            "$select": "tree_id, spc_common, zipcode, status, health, the_geom",
            "$limit": "150000",
            "$offset": collected_row_count,
        }

        query_url = build_query_url(
            "https://data.cityofnewyork.us/resource/5rq2-4hqu.json", params, False
        )

        dataset_path = get_with_cache(query_url)

        jsonStr = "[]"

        with open(dataset_path, "rb") as file_handle:

            jsonStr = file_handle.read().decode()

        jsonObject = json.loads(jsonStr)

        part_dataframe = pd.DataFrame.from_records(jsonObject)

        result = pd.concat([result, part_dataframe], ignore_index=True)

        collected_row_count += len(jsonObject)

    result = result.rename(columns={"the_geom": "geometry"})

    result['geometry'] = result['geometry'].apply(shape)

    return result

def load_and_clean_zillow_data():
    """Load and clean historical rental dataset"""

    df = pd.read_csv(ZILLOW_DATA_FILE)

    df = df.drop(['RegionID', 'SizeRank', 'RegionType', 'StateName',
                 'City', 'Metro', 'CountyName'], axis=1)

    df = df.rename(columns={'RegionName': 'zipcode'})

    columns = df.columns

    columns = columns[2:]

    result = pd.DataFrame()

    values = []

    for index, row in df.iterrows():

        for column in columns:

            new_row = {

                'zipcode': row['zipcode'],

                'state': row['State'],

                'date': column,

                'average_price': row[column]

            }

            values.append(new_row)

    result = pd.DataFrame.from_records(values)

    return result

def load_all_data():
    """Load all data"""

    geodf_zipcode_data = load_and_clean_zipcodes(ZIPCODE_DATA_FILE)

    geodf_311_data = download_and_clean_311_data()

    geodf_tree_data = download_and_clean_tree_data()

    df_zillow_data = load_and_clean_zillow_data()

    return (geodf_zipcode_data, geodf_311_data, geodf_tree_data, df_zillow_data)

geodf_zipcode_data, geodf_311_data, geodf_tree_data, df_zillow_data = load_all_data()

geodf_311_data.info()
geodf_311_data.head()
geodf_tree_data.info()
geodf_tree_data.head()
df_zillow_data.info()
df_zillow_data.head()