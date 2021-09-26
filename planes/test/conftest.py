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


@pytest.fixture(scope="function")
def requests_session():
    session = requests.session()
    yield session
    session.close()


@pytest.fixture(scope="session")
def env_vars(temp_dir, autouse=True):
    with mock.patch.dict(
        os.environ,
        {
            "OUTPUT_DIR": temp_dir,
            "PLANE_CRASH_DOMAIN": "http://www.planecrashinfo.com",
            "PLANE_CRASH_URL": "http://www.planecrashinfo.com/datkkkabase.htm",
        },
    ):
        yield


@pytest.fixture(scope="session")
def temp_dir(env_vars):
    temp_dir = "temp/"
    yield temp_dir
    shutil.rmtree(temp_dir)
