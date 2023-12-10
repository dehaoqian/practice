
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