import sqlite3
import os
import unittest
import json
import requests

# Team Name: Machos
# Group Members: Shin Cho and Rebecca Mao

def setUpDatabase(db_name):
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path+'/'+db_name)
    cur = conn.cursor()
    return cur, conn

def usfm_api():
    response_API = requests.get('USFR_StmtNetCost_20110930_20210930.json')
    data = response_API.text
    parse_json = json.loads(data)
    return parse_json