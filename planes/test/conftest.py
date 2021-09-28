"""
This file contains functions that help me set up and write my
tests

- Producing CSVs
- Starting a requests session 
- Creating directories and destroying them afterwards
"""

import os
import pytest
import requests
import shutil

import pandas as pd
from unittest import mock

from .data_generation_helpers import (
    random_aircraft,
    random_dates,
    random_city,
    random_operator,
    random_int_string,
    random_registration,
)


@pytest.fixture(scope="session")
def requests_session():
    session = requests.session()
    yield session
    session.close()


@pytest.fixture(scope="session", autouse=True)
def env_vars(temp_dir):
    with mock.patch.dict(
        os.environ,
        {
            "SAVE_DIR": temp_dir,
            "FILENAME": "crash_data_table.csv",
            "PLANE_CRASH_DOMAIN": "http://www.planecrashinfo.com",
            "PLANE_CRASH_URL": "http://www.planecrashinfo.com/database.htm",
        },
    ):
        yield


@pytest.fixture(scope="session")
def temp_dir():
    temp_dir = "test_temp/"
    try:
        os.mkdir(temp_dir)
    except FileExistsError:
        pass
    yield temp_dir
    shutil.rmtree(temp_dir)


@pytest.fixture(scope="function")
def generate_crashes_df():
    size = 2000
    headers = [
        "Date",
        "Location",
        "Operator",
        "Aircraft",
        "Registration",
        "Deaths",
        "Aboard",
        "Ground_Deaths",
    ]

    df = pd.DataFrame(columns=headers)

    df["Date"] = random_dates(
        start=pd.to_datetime("1940-01-01"), end=pd.to_datetime("2008-01-01"), size=size
    )
    df["Location"] = random_city(size)
    df["Operator"] = random_operator(size)
    df["Aircraft"] = random_aircraft(size)
    df["Registration"] = random_registration(size)
    df["Deaths"] = random_int_string(size)
    df["Aboard"] = random_int_string(size)
    df["Ground_Deaths"] = random_int_string(size)

    return df
