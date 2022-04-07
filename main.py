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
    response_API = requests.get('https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v2/accounting/od/statement_net_cost')
    data = response_API.text
    parse_json = json.loads(data)
    return parse_json

def bitcoin_api(date):
    base_url = "https://api.coinpaprika.com/v1/"
    response = requests.get(base_url + "coins/btc-bitcoin/ohlcv/historical?start=" + date)
    data = response.json()
    return data

def bitcoin_table(data, cur, conn):
    cur.execute("CREATE TABLE IF NOT EXISTS Bitcoin Table (date TEXT, open NUMBER, high NUMBER, low NUMBER, close NUMBER)")
    conn.commit()

def usfm_table(data, cur, conn):
    cur.execute("CREATE TABLE IF NOT EXISTS USFM Table (date TEXT, Total Net Cost NUMBER")
    conn.commit()