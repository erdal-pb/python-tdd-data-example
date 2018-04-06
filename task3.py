#!/home/kazooy/anaconda3/bin/python
'''
author: David O'Keeffe
date: 30/3/2018

task_3
~~~~~

Gets data from a
'''

from flask import Flask

app = Flask(__name__)

@app.route('/')
def hello_world():
    return 'Hello, World!'
