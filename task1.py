#!/home/kazooy/anaconda3/bin/python
#
#
#

# Imports
import luigi
import pandas as pd
import zipfile
import subprocess
import csv
import collections
import json
from io import BytesIO 
from pymongo import MongoClient

###############################################################################
class LoadData(luigi.Task):
    """ Download the source data and save it to an SQL server """

    def run(self):
        # Get the file and save it locally ----
        #url = "https://s3-ap-southeast-2.amazonaws.com/vibrato-data-test-" + 
        #      "public-datasets/world-food-facts.zip"
        #subprocess.call(["wget", url, "-P/ETL-with-Docker/Task-1"])
        #cnx = MySQLdb.connect(user='kazooy', password='G00gle!oogle', host='localhost', database='task1')

        # Extract and read in the data ----
        filename = "world-food-facts.zip"
        zip_ = zipfile.ZipFile(filename, 'r')
        file_to_unzip = zip_.namelist()[0]
        tsv_file = BytesIO(zip_.read(file_to_unzip))
        tsvin = pd.read_csv(tsv_file, sep='\t')

        # Initialize MongoClient and convert Pandas DF to JSON 
        client = MongoClient('localhost', 27017)
        data_json = json.loads(tsvin.to_json(orient='records')) 

        db_cm.insert(data_json)

        

class MakeOutput(luigi.Task):

    def output(self):
        return


    def requires(self):
        return LoadData()


    def run(self):
        return



if __name__ == '__main__':
    luigi.run()
         
