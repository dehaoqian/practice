
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