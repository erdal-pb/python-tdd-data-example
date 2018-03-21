#!/home/kazooy/anaconda3/bin/python3
import luigi
import subprocess
import zipfile

class LoadData(luigi.Task):
    """ Load the data and unzup it """
    def requires(self):
        return []
 
    def output(self):
        return luigi.LocalTarget("numberrs_up_to_10.txt") 
    
    def run(self):
        target = "https://s3-ap-southeast-2.amazonaws.com/vibrato-data-test-public-datasets/world-food-facts.zip"
        subprocess.run(['wget', target, "-P/tmp/task1"])


if __name__ == "__main__":
    luigi.run()
