import random
import string

import numpy as np
import pandas as pd

from faker import Faker


def random_city(size):
    """
    Generate n-length ndarray of city names.
    """
    fake = Faker()
    return [fake.city() for _ in range(size)] 


def random_aircraft(size, p=None):
    """Generate n-length ndarray of genders."""
    if not p:
        # default probabilities
        p = (0.50, 0.50)
    aircraft = ("Fokker \n100", "Antonov\t26B")
    return np.random.choice(aircraft, size=size, p=p)


def random_operator(size, p=None):
    """Generate n-length ndarray of genders."""
    if not p:
        # default probabilities
        p = (0.50, 0.50)
    airlines = ("Busy Bee Congo", "South West Aviaiton")
    return np.random.choice(airlines, size=size, p=p)


def random_int_string(size, p=None):
    """"""
    if not p:
        # default probabilities
        p = (0.25, 0.25, 0.25, 0.25)
    ints = ("100", "", "?", "50")
    return np.random.choice(ints, size=size, p=p)


def random_registration(size, p=None):
    return ["".join(random.sample(string.ascii_lowercase, 5)) for _ in range(size)]


def random_dates(start, end, size):
    """
    Generate random dates within range between start and end.
    Adapted from: https://stackoverflow.com/a/50668285
    """
    # Unix timestamp is in nanoseconds by default, so divide it by
    # 24*60*60*10**9 to convert to days.
    divide_by = 24 * 60 * 60 * 10 ** 9
    start_u = start.value // divide_by
    end_u = end.value // divide_by
    return pd.to_datetime(np.random.randint(start_u, end_u, size), unit="D")


def assert_dataframe_equal(expected_df, result_df):
    """
    Wrapper around pandas testing assertions. It will ignore the order of the columns
    to make writing the tests slighly easier.
    """
    try:
        assert (
            pd.testing.assert_frame_equal(
                left=expected_df, right=result_df, check_like=True
            )
            is None
        )
    except AssertionError as e:
        print("Expected Table")
        print(expected_df.to_markdown())
        print("Result Table")
        print(result_df.to_markdown())
        raise e
