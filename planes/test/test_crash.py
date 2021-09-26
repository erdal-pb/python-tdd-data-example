"""

Thinking how I would test this...

- Make sure I can pull data from that page reliably
- Make sure I parse the data correctly
- Make sure I save the CSV in the format expected
- Make sure I read in the CSV correcly
- Make sure I transform the CSV correctly
- Make sure the numbers are correct 

This couples the test suite to the code

"""


import pytest

import pandas as pd


from .data_generation_helpers import (
    random_dates,
    random_names,
    random_genders,
    assert_dataframe_equal,
)


def test_fetch_year_list(requests_session):
    """
    @integration
    """
    from planes.better_crash import fetch_plane_crash_years_list

    year_urls = fetch_plane_crash_years_list(session=requests_session)

    assert len(year_urls) >= 102

    assert year_urls[0] == "http://www.planecrashinfo.com/1920/1920.htm"
    assert year_urls[-1] == "http://www.planecrashinfo.com/2021/2021.htm"


def test_fetch_year_details(requests_session):
    """
    @integration
    """
    from planes.better_crash import fetch_plane_crash_years_details

    plane_crash_details = fetch_plane_crash_years_details(
        url="http://www.planecrashinfo.com/1920/1920.htm", session=requests_session
    )
    assert len(plane_crash_details) == 52


def test_parse_planes_html(requests_session):
    from planes.better_crash import fetch_plane_crash_years_details

    plane_crash_details = fetch_plane_crash_years_details(
        url="http://www.planecrashinfo.com/1920/1920.htm", session=requests_session
    )
    assert len(plane_crash_details) == 52


def test_clean_parsed_crash_regex():
    pass


def test_pd_transformations():
    """
    Date,Location,Operator,Aircraft,Registration,Deaths,Aboard,Ground_Deaths
    17 Sep 1908,"Fort Myer, Virginia",Military - U.S. Army,Wright Flyer III,?,1,2,0
    """
    size = 2
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
    df["Location"] = random_names("last_names", size)



def test_total_deaths():
    pass


def test_worst_year():
    pass


def test_worst_flight_operators():
    pass


def test_worst_operator_png():
    pass
