"""
This file contains functions that help me set up and write my
tests

- Producing CSVs
- Replicating 
"""

import os
import pytest
import requests
import shutil


from unittest import mock


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
            "OUTPUT_DIR": temp_dir,
            "PLANE_CRASH_DOMAIN": "http://www.planecrashinfo.com",
            "PLANE_CRASH_URL": "http://www.planecrashinfo.com/database.htm",
        },
    ):
        yield


@pytest.fixture(scope="session")
def temp_dir():
    temp_dir = "temp/"
    os.mkdir(temp_dir)
    yield temp_dir
    shutil.rmtree(temp_dir)
