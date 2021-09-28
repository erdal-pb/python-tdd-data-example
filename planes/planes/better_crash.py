#!/usr/local/bin/python
"""
author: David O'Keeffe
date: 23/3/2018

plane_crashes.py
~~~~~

Steps:
Write the code to perform an ETL process to extract a data set
from the supplied source. Persist outputs and Visualise the data
in an accessible format

http://www.planecrashinfo.com/database.htm

Output:
    Total fatalities between period 1920-2016 period
    Top 3 airlines with the highest rate of incidents
    Year with the highest incidents
"""

import csv
import re
import os
import logging
import requests
import sys

from bs4 import BeautifulSoup

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


# Set the display backend so we can save the PNG
plt.switch_backend("agg")

logging.basicConfig(stream=sys.stdout, level="INFO")


def session():
    s = requests.Session()
    yield s


def fetch_plane_crash_years_list(session):
    """"""
    page = session.get(os.environ["PLANE_CRASH_URL"])
    crash_years = BeautifulSoup(page.content, "lxml").select("tr a")
    return [os.environ["PLANE_CRASH_DOMAIN"] + link.get("href") for link in crash_years]


def fetch_plane_crash_years_details(url, session):
    """"""
    if not re.match(r".*\.com\/", url):
        url = url.replace("com", "com/")
    logging.info(f"Scraping: {url}")
    page = session.get(url)
    crash_details = BeautifulSoup(page.content, "lxml").select("table tr")
    return crash_details[1:]


def parse_fatalities(crash_row):
    fatalities = re.search(r"(\d+|\?)\/(\d+|\?)\((\d+|\?)\)", crash_row[3][0])
    if fatalities:
        deaths = fatalities.group(1)
        aboard = fatalities.group(2)
        ground_deaths = fatalities.group(3)
        return deaths, aboard, ground_deaths
    return None, None, None


def parse_plane_crash_html(crash_row):
    """"""
    crash_row = crash_row.select("td font")

    date = crash_row[0][0].get_text()
    location = crash_row[1][0]
    airline = crash_row[1][2]
    aircraft = crash_row[2][0]
    registration = crash_row[2][2]
    deaths, aboard, ground_deaths = parse_fatalities(crash_row=crash_row)

    return [
        date,
        location,
        airline,
        aircraft,
        registration,
        deaths,
        aboard,
        ground_deaths,
    ]


def clean_plane_crash_event(crash_event):
    """"""
    for item in crash_event:
        return re.sub(r"(^[ \t\n]+|[ \t\n]+(?=:)|\n$)", "", item, flags=re.M)


def make_plane_crash_csv(parsed_data_array, filename):
    """"""
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
    try:
        os.mkdir(os.environ["SAVE_DIR"])
    except FileExistsError:
        pass
    with open(f"{os.environ['SAVE_DIR']}{filename}", "w+") as my_csv:
        csvWriter = csv.writer(my_csv, delimiter=",")
        csvWriter.writerow([h for h in headers])
        csvWriter.writerows(parsed_data_array)


def read_plane_crash_csv_pd(filename):
    """"""
    return pd.read_csv(f"{os.environ['SAVE_DIR']}{filename}")


def clean_pandas_df(df):
    """"""
    df.replace({r"\?": np.NaN}, regex=True, inplace=True)
    df.replace({r"\n": ""}, regex=True, inplace=True)
    df[["Deaths", "Aboard", "Ground_Deaths"]] = df[
        ["Deaths", "Aboard", "Ground_Deaths"]
    ].apply(pd.to_numeric)
    df["Date"] = df["Date"].apply(pd.to_datetime)
    df["Year"], df["Month"] = df["Date"].dt.year, df["Date"].dt.month
    return df


def total_fatalites(cleaned_df):
    """"""
    deaths = cleaned_df.Deaths.sum()
    logging.info(f"The total number of deaths (Air + Ground) is {int(deaths)}")
    return deaths


def worst_flight_operators(cleaned_df):
    """"""
    pass


def most_horrible_year(cleaned_df):
    """"""
    pass


def make_worst_flight_operators_graph(worst_operators_df):
    """"""
    pass


def main():
    """"""
    filename = os.environ["FILENAME"]
    df = pd.read_csv(f"{os.environ['SAVE_DIR']}{filename}")


if __name__ == "__main__":
    main()
