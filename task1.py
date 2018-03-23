#!/home/kazooy/anaconda3/bin/python
#
#
####


import luigi
from luigi.contrib.mongodb import MongoCollectionTarget
import pandas as pd
import zipfile
import subprocess
import csv
import collections
import json
from io import BytesIO 


class LoadData(luigi.Task):
    """ Download the source data and save it to a MongoDB server """
    client = MongoClient('localhost', 27017)

    def output(self):
        return MongoCollectionTarget(self.client, 'task-1', 'world-facts')


    def run(self):
        # Get the file and save it locally ----
        url = ('https://s3-ap-southeast-2.amazonaws.com/vibrato-data-test-' +
               'public-datasets/world-food-facts.zip')
        subprocess.run(["wget", url, "-P/tmp/task-1-data"])

        # Extract and read in the data ----
        filename = "/tmp/task-1-data/world-food-facts.zip"
        zip_ = zipfile.ZipFile(filename, 'r')
        file_to_unzip = zip_.namelist()[0]
        tsv_file = BytesIO(zip_.read(file_to_unzip))
        tsvin = pd.read_csv(tsv_file, sep='\t')

        # Convert DF to JSON then upload to MongoDB
        db = self.client['task-1']
        collection = db['world-facts']
        data_json = json.loads(tsvin.to_json(orient='records')) 
        collection.insert(data_json)


class MakeOutput(luigi.Task):
    """
    List the Top 5 Peanut Butters based in Australia and sort
    them by highest Energy content per 100g

    List the Top 10 Countries together with the number of products
    with Zinc content above ‘0.001’ and that have more than one product

    Grouping product categories by those that contain Chicken, Pork and Tofu
    list their Average, Median and Standard Deviation Protein content per 100g,
    excluding data that is not available (NaN)
    """

    def requires(self):
        return LoadData()

    def run(self):
        db_col = self.input().get_collection()
        self.peanut_butter(db_col)

    def peanut_butter(self, db_col):
        print(self.db_col.find_one())
        print(db_col.find({"product_name": "/.*peanut.butter.*/"}))
        

    def zinc_country(self):
        return

    def product_categories(self):
        return



if __name__ == '__main__':
    luigi.run()
         
