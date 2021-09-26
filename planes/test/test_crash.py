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


def test_get_url():
    pass


def test_parse_planes_html():
    pass


def test_clean_parsed_crash_regex():
    pass


def test_pd_transformations():
    pass


def test_total_deaths():
    pass


def test_worst_year():
    pass


def test_worst_flight_operators():
    pass


def test_worst_operator_png():
    pass
