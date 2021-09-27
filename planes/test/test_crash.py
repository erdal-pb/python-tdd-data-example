"""

Thinking how I would test this...

- Make sure I can pull data from that page reliably
- Make sure I parse the data correctly
- Make sure I save the CSV in the format expected
- Make sure I read in the CSV correcly
- Make sure I transform the CSV correctly
- Make sure the numbers are correct 

This couples the test suite to the code. If you wish to avoid
this kind of coupling then you need to have an interface agreement. 

"""
import pandas as pd
from planes.better_crash import clean_pandas_df

from .data_generation_helpers import (
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
    """
    for crash in plane_crash_details:
        print(crash.contents)
        for row in crash.select("td font"):
            print(row.contents)
    """
    print(plane_crash_details[0].select("td_font"))
    assert len(plane_crash_details) == 51


def test_parse_planes_html(requests_session):
    """
    @integration
    """
    from planes.better_crash import parse_plane_crash_html

    plane_crash_details = parse_plane_crash_html(
        url="http://www.planecrashinfo.com/1920/1920.htm", session=requests_session
    )


def test_clean_parsed_crash_regex():
    """
    (r"(^[ \t\n]+|[ \t\n]+(?=:)|\n$)", "", item, flags=re.M)
    """
    from planes.better_crash import clean_plane_crash_event

    pass


def test_write_csv():
    from planes.better_crash import make_plane_crash_csv


def test_pd_transformations(generate_crashes_df):
    """
    Testing that we read in the CSV correctly and parse it to the correct types

    """
    from planes.better_crash import clean_pandas_df

    old_dtypes = generate_crashes_df.dtypes 

    cleaned_df = clean_pandas_df(generate_crashes_df)

    print(cleaned_df)

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

    assert cleaned_df != old_dtypes 


def test_total_deaths(generate_crashes_df):
    from planes.better_crash import total_fatalites

    cleaned_df = clean_pandas_df(generate_crashes_df)
    expected_deaths = cleaned_df.Deaths.sum() + cleaned_df.Ground_Deaths.sum()

    assert total_fatalites(cleaned_df=cleaned_df) == expected_deaths

    print(total_fatalites(cleaned_df=cleaned_df))



def test_worst_year():
    from planes.better_crash import worst_flight_operators

    pass


def test_worst_flight_operators():
    from planes.better_crash import most_horrible_year

    pass


def test_worst_operator_png():
    from planes.better_crash import make_worst_flight_operators_graph

    pass
