#!/usr/local/bin/python
'''
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
'''

import csv
import re
import time

from bs4 import BeautifulSoup
from tabulate import tabulate
from urllib.request import urlopen

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


# Set the display backend so we can save the PNG
plt.switch_backend('agg')

def output(self):
    return luigi.LocalTarget("crash_data_table.csv")

def get_(self):
    # Get the URL
    url = "http://www.planecrashinfo.com/database.htm"
    page = urlopen(url)
    soup = BeautifulSoup(page, "lxml")

    # Pull all the a tags inside of table cells
    years = soup.select('tr a')
    domain = "http://www.planecrashinfo.com"
    year_links = [domain + link.get('href') for link in years]

    # For each year fetch all the links then open each record
    for year in year_links:
        # Sometimes the slash is missing in the <a> tag
        if not re.match(r".*\.com\/", year):
            year = year.replace("com", "com/")
        print(year)  # As a sort of Quasi Progress Bar
        page = urlopen(year)
        crash_table = BeautifulSoup(page, "lxml")
        self.get_table_crashes(crash_table)  # Use the faster method
        time.sleep(1)  # Sleep so the server doesn't get upset
    headers = ['Date', 'Location', 'Operator', 'Aircraft', 'Registration',
                'Deaths', 'Aboard', 'Ground_Deaths']
    # Initialize CSV
    with open("crash_data_table.csv", "w+") as my_csv:
        csvWriter = csv.writer(my_csv, delimiter=',')
        csvWriter.writerow([h for h in headers])
        csvWriter.writerows(self.crash_array)

def get_individual_crashes(self, crash, year, soup):
    """
    Provides more complete information but has to fetch for every crash
    hence it's rather slow
    """
    crashes = soup.select('tr a')
    crash_links = [year[:-8] + link.get('href') for link in crashes]

    for crash in crash_links:
        page = urlopen(crash)
        soup = BeautifulSoup(page, "lxml")
        data = soup.select('tr font')

        # Based off the table structure collect every odd index
        crash_temp = data[3::2]
        crash_row = [crash] + [row.get_text() for row in crash_temp]
        self.crash_array.append(crash_row)

def get_table_crashes(self, crash_table):
    """
    Only gets the information from the years itself, much faster but has
    less information
    """
    crash_data = crash_table.select('table tr')

    # Remove the first row
    crash_data.pop(0)

    for crash in crash_data:
        crash_temp = crash.select('td font')
        crash_row = [row.contents for row in crash_temp]
        date = crash_row[0][0].get_text()
        location = crash_row[1][0]
        airline = crash_row[1][2]
        aircraft = crash_row[2][0]
        registration = crash_row[2][2]

        # Extract out the rest with RegEx
        fatalities = re.search(r'(\d+|\?)\/(\d+|\?)\((\d+|\?)\)',
                                crash_row[3][0])
        if fatalities:
            deaths = fatalities.group(1)
            aboard = fatalities.group(2)
            ground_deaths = fatalities.group(3)

        # Save
        crash_row_final = [date, location, airline, aircraft, registration,
                            deaths, aboard, ground_deaths]

        # Remove trailing whitespace and tabs
        for i in range(0, 5):
            item = crash_row_final[i]
            item = re.sub(r'(^[ \t\n]+|[ \t\n]+(?=:)|\n$)', '', item,
                            flags=re.M)
            crash_row_final[i] = item
        self.crash_array.append(crash_row_final)

    def run(self):
        db = self.input().open()

        # Import into Pandas and clean
        df = pd.read_csv(db)
        df.replace({'\?': np.NaN}, regex=True, inplace=True)
        df.replace({'\n': ''}, regex=True, inplace=True)
        df[['Deaths', 'Aboard', 'Ground_Deaths']] = \
            df[['Deaths', 'Aboard', 'Ground_Deaths']].apply(pd.to_numeric)
        df['Date'] = df['Date'].apply(pd.to_datetime)
        df['Year'], df['Month'] = df['Date'].dt.year, df['Date'].dt.month

        # Total number of fatalities:
        deaths = df.Deaths.sum() + df.Ground_Deaths.sum()
        print("The total number of deaths (Air + Ground) is " +
              str(int(deaths)))

        # Year of most incidents
        year_df = df.groupby(['Year']).agg(['sum', 'count'])
        highest_year = year_df.sort_values([('Deaths', 'count')],
                                           ascending=False).iloc[0].name
        print("Year with the most number of incidents is " + str(highest_year))

        # Worst operators to fly on
        operator_df = df.groupby(['Operator']).agg(['count'])
        worst_ops = operator_df.sort_values([('Deaths', 'count')],
                                            ascending=False).iloc[:3, 0]
        print(tabulate(worst_ops))
        fig_title = 'Top 3 Airlines with the Most Number of Incidents'
        ax = worst_ops.plot('bar',
                            title=fig_title,
                            rot=0)
        ax.set_xlabel("Airline Operator")
        ax.set_ylabel("Number of Incidents")
        fig = ax.get_figure()
        fig.savefig("2c - Worst Operators.png")


if __name__ == "__main__":
    luigi.run()
