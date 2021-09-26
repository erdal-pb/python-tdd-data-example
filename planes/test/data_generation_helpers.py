
import numpy as np
import pandas as pd

from faker.providers.person.en import Provider


def random_names(name_type, size):
    """
    Generate n-length ndarray of person names.
    name_type: a string, either first_names or last_names
    """
    names = getattr(Provider, name_type)
    return np.random.choice(names, size=size)


def random_names(name_type, size):
    """
    Generate n-length ndarray of person names.
    name_type: a string, either first_names or last_names
    """
    names = getattr(Provider, name_type)
    return np.random.choice(names, size=size)


def random_genders(size, p=None):
    """Generate n-length ndarray of genders."""
    if not p:
        # default probabilities
        p = (0.49, 0.49, 0.01, 0.01)
    gender = ("M", "F", "O", "")
    return np.random.choice(gender, size=size, p=p)


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
