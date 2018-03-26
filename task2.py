#!/home/kazooy/anaconda3/bin/python
'''
author: David O'Keeffe
date: 23/3/2018

task_2
~~~~~

Steps:
Write the code to perform an ETL process to extract a data set from the supplied source
Persist outputs and Visualise the data in an accessible format

http://www.planecrashinfo.com/database.htm


Output:
    Total fatalities between period 1920-2016 period
    Top 3 airlines with the highest rate of incidents
    Year with the highest incidents


'''

import luigi
import csv
import re
from bs4 import BeautifulSoup
from urllib.request import urlopen
import pandas as pd
import seaborn as sns
import time

class GetPlaneData(luigi.Task):
    crash_array = [] 

    def output(self):
        return self.crash_array  

    def run(self):
        # Get the URL
        url = "http://www.planecrashinfo.com/database.htm"
        page = urlopen(url)
        soup = BeautifulSoup(page, "lxml")

        # Pull all the a tags inside of table cells
        years = soup.select('tr a')
        domain = "http://www.planecrashinfo.com"
        year_links = [domain + link.get('href') for link in years]
        com_regex = r".com\/"

        # For each year fetch all the links then open each record
        for year in year_links:
            # Sometimes the slash is missing in the <a> tag
            if not re.match(com_regex, year):
                year = year.replace("com", "com/")
            page = urlopen(year)
            soup = BeautifulSoup(page, "lxml")
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
            time.sleep(1)

        # Initialize CSV
        with open("crash_data.csv", "w+") as my_csv:
            csvWriter = csv.writer(my_csv, delimiter=',')
            csvWriter.writerows(crash_array)
             

if __name__ == "__main__":
    luigi.run()

