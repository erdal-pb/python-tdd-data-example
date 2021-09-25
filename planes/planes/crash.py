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
import logging
import requests
import sys

from bs4 import BeautifulSoup
from tabulate import tabulate
from urllib.request import urlopen

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


# Set the display backend so we can save the PNG
plt.switch_backend("agg")

logging.basicConfig(stream=sys.stdout, level="INFO")

# Get the URL
url = "http://www.planecrashinfo.com/database.htm"
s = requests.Session()
page = s.get(url)
soup = BeautifulSoup(page.content, "lxml")
crash_array = []

# Pull all the a tags inside of table cells
years = soup.select("tr a")
domain = "http://www.planecrashinfo.com"
year_links = [domain + link.get("href") for link in years]


# For each year fetch all the links then open each record
for count, year_url in enumerate(year_links):
    # Sometimes the slash is missing in the <a> tag
    if not re.match(r".*\.com\/", year_url):
        year_url = year_url.replace("com", "com/")
    logging.info(f"Scraping: {year_url} \nProgress: {count}/{len(year_links)}")
    page = s.get(year_url)
    soup = BeautifulSoup(page.content, "lxml")
    crash_data = soup.select("table tr")

    # Remove the first row
    crash_data.pop(0)

    for crash in crash_data:
        crash_temp = crash.select("td font")
        crash_row = [row.contents for row in crash_temp]
        date = crash_row[0][0].get_text()
        location = crash_row[1][0]
        airline = crash_row[1][2]
        aircraft = crash_row[2][0]
        registration = crash_row[2][2]

        # Extract out the rest with RegEx
        fatalities = re.search(r"(\d+|\?)\/(\d+|\?)\((\d+|\?)\)", crash_row[3][0])
        if fatalities:
            deaths = fatalities.group(1)
            aboard = fatalities.group(2)
            ground_deaths = fatalities.group(3)

        # Save
        crash_row_final = [
            date,
            location,
            airline,
            aircraft,
            registration,
            deaths,
            aboard,
            ground_deaths,
        ]

        # Remove trailing whitespace and tabs
        for i in range(0, 5):
            item = crash_row_final[i]
            item = re.sub(r"(^[ \t\n]+|[ \t\n]+(?=:)|\n$)", "", item, flags=re.M)
            crash_row_final[i] = item
        logging.info(f"Appending: {crash_row_final}")
        crash_array.append(crash_row_final)

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
# Write to CSV
with open("crash_data_table.csv", "w+") as my_csv:
    csvWriter = csv.writer(my_csv, delimiter=",")
    csvWriter.writerow([h for h in headers])
    csvWriter.writerows(crash_array)

# Import into Pandas and clean
df = pd.read_csv("crash_data_table.csv")
df.replace({"\?": np.NaN}, regex=True, inplace=True)
df.replace({"\n": ""}, regex=True, inplace=True)
df[["Deaths", "Aboard", "Ground_Deaths"]] = df[
    ["Deaths", "Aboard", "Ground_Deaths"]
].apply(pd.to_numeric)
df["Date"] = df["Date"].apply(pd.to_datetime)
df["Year"], df["Month"] = df["Date"].dt.year, df["Date"].dt.month

# Total number of fatalities:
deaths = df.Deaths.sum() + df.Ground_Deaths.sum()
logging.info(f"The total number of deaths (Air + Ground) is {int(deaths)}")

# Year of most incidents
year_df = df.groupby(["Year"]).agg(["sum", "count"])
highest_year = year_df.sort_values([("Deaths", "count")], ascending=False).iloc[0].name
logging.info(f"Year with the most number of incidents is {highest_year}")

# Worst operators to fly on
operator_df = df.groupby(["Operator"]).agg(["count"])
worst_ops = operator_df.sort_values([("Deaths", "count")], ascending=False).iloc[:3, 0]
print(worst_ops)

fig_title = "Top 3 Airlines with the Most Number of Incidents"
ax = worst_ops.plot(kind="bar", title=fig_title, rot=0)
ax.set_xlabel("Airline Operator")
ax.set_ylabel("Number of Incidents")
fig = ax.get_figure()
fig.savefig("2c - Worst Operators.png")
