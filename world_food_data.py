#!/usr/local/bin/python
'''
author: David O'Keeffe
date: 23/3/2018

world_food_data.py
~~~~~

Uses Luigi (although it really doesn't have to) to create a data pipeline
which reads in a ZIP file from a URL online, uploads it to a MongoDB
server, does a bunch of queries and spits out some queries.
'''


# Stdlib imports
import zipfile
import os
from io import BytesIO 
import urllib.request
import json

# Library specific imports
import luigi
from luigi.contrib.mongodb import MongoCollectionTarget
import pandas as pd
import seaborn as sns
from pymongo import MongoClient
from tabulate import tabulate


class GetData(luigi.Task):
    """
    Download the source data and save it to a MongoDB server.
    
    Why MongoDB?

    I tried using SQL but the raw data didn't fit nicely and would have
    required a lot of cleaning so it was easier to put it into a NoSQL DB

    Plus never used MongoDB before so a good chance to pick it up
    """
    mongo_address = os.environ['MONGOSERVER_PORT_27017_TCP_ADDR']
    client = MongoClient(mongo_address, 27017)

    def output(self):
        return MongoCollectionTarget(self.client, 'task-1', 'world-facts')


    def run(self):
        # Get the file and save it locally ----
        print("Downloading file from source")
        url = ('https://s3-ap-southeast-2.amazonaws.com/vibrato-data-test-' +
               'public-datasets/world-food-facts.zip')
        file_name, headers = urllib.request.urlretrieve(url)

        # Extract and read in the data ----
        print("Got file now extracting and loading into Pandas")
        zip_ = zipfile.ZipFile(file_name, 'r')
        file_to_unzip = zip_.namelist()[0]
        tsv_file = BytesIO(zip_.read(file_to_unzip))
        tsvin = pd.read_csv(tsv_file, sep='\t')

        # Convert DF to JSON then upload to MongoDB
        print("Convert file to JSON then upload to MongoDB")
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
        return GetData()

    def run(self):
        db_col = self.input().get_collection()

        # I hope this is passing simply a pointer not the whole thing
        self.peanut_butter(db_col)
        self.zinc_country(db_col)
        self.product_categories(db_col)

    def peanut_butter(self, db_col):
        pipeline = [
            # SELECT Product Name, Energy, Categories, Country, and Brand
            {"$project": {"product_name":1, "energy_100g":1, "brands":1,
                          'countries_en':1}},

            # WHERE product_name LIKE "%peanut_butter%" AND
            # countries_en LIKE "%australia%"
            {"$match": {"product_name": {"$regex":".*peanut.butter.*",
                                         "$options": "i"},
                        "countries_en": {"$regex":".*australia.*",
                                         "$options": "i"}}},

            # ORDER BY energy_100g DESC 
            {"$sort":{"energy_100g":-1}},

            # LIMIT 5 
            {"$limit": 5} 
        ]
        
        # Read query into Pandas Dataframe and print list 
        pb_df = pd.DataFrame(list(db_col.aggregate(pipeline)))
        
        # Drop the _id column because it makes the table look bad
        pb_df.drop(["_id"], axis=1, inplace=True)
        print(tabulate(pb_df, headers='keys', tablefmt='psql'))

        # Hardcode nice titles b/c it's easy
        pb_df['brand'] = ["Coles Sanitarium - Crunchy", "The Forage Company - Smooth",
                          "Sanitarium - Crunchy", "Aldi Bramwells - Crunchy",
                          "Aldi Bramwells - American Style"]
        ax = sns.barplot(y="brand", x="energy_100g", data=pb_df, fill="brands", orient="h")

        # Can make a nice plot 
        ax.set_title('Top 5 Energy Rich Peanut Butters in Australia', fontsize=12)
        ax.set(xlabel='Energy (kJ) per 100g', ylabel='Peanut Butter Type')
        fig = ax.get_figure()
        fig.set_size_inches(11.7, 8.27)
        fig.savefig("1a - Peanut Butter.png")


    def zinc_country(self, db_col):
        pipeline = [
            # SELECT Zinc, Countries 
            {"$project": {"zinc_100g":1, 'countries_en':1}},
           
            # WHERE zinc_100g > 0.001
            {"$match": {"zinc_100g": {"$gt": 0.001}}},

            # GROUP BY countries_en
            {"$group": {"_id": "$countries_en",
                        "count":{"$sum": 1}}},
            
            # HAVING count(*) > 1
            {"$match": {"count": {"$gt": 1}}},
            
            # ORDER BY Count 
            {"$sort":{"count":-1}},

            # LIMIT 10 
            {"$limit": 10}
        ]
        zinc_df = pd.DataFrame(list(db_col.aggregate(pipeline)))
        zinc_df.rename(index=str, columns={"_id":"Country"}, inplace=True)
        print(tabulate(zinc_df, headers='keys', tablefmt='psql'))

    def product_categories(self, db_col):
        """
        I can try do this is pure MongoDB by splitting on whitespaces then
        using a switch statement, but it might not cover all cases, so
        better to use the RegEx capability inside Pandas 
        """
        pipeline = [
            # SELECT categories_en, proteins_100g
            {"$project": {"categories_en": 1, "proteins_100g":1}}, 
            
            # WHERE categories_en LIKE "%pork%" OR categories_en LIKE "%tofu%"
            # OR categories_en LIKE "%chicken%"
            {"$match": {"categories_en": {"$regex":"(pork|chicken|tofu)", 
                                         "$options": "i"}}}
        ]
        pc_df = pd.DataFrame(list(db_col.aggregate(pipeline)))

        # Use RegEx to make a new grouping
        pc_df["food_group"] = pc_df.categories_en.str.extract("(pork|chicken|tofu)")

        # Remove any rows which didn't match the RegEx
        nn_df = pc_df[pc_df.food_group.notnull()]

        # Group by this new column and compute the mean, median, and std dev.
        summary_df = nn_df.groupby(['food_group']).agg(['mean', 'median', 'std'])
        print(tabulate(summary_df, headers='keys', tablefmt='psql'))


if __name__ == '__main__':
    luigi.run()
         
