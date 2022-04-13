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

def stock_api():
    response_API = requests.get('https://api.twelvedata.com/time_series?symbol=DJI&interval=1day&start_date=2022-01-01&end_date=2022-04-02&apikey=e7702bf29d4148cca08ed5c4180e21eb')
    data = response_API.json()
    #parse_json = json.loads(data)
    return data

print(stock_api())


def stock_table(cur, conn):
    cur.execute("CREATE TABLE IF NOT EXISTS Stock (date TEXT, open NUMBER, high NUMBER, low NUMBER, close NUMBER)")
    
    data = stock_api()

    for i in data:
        date = i['datetime'][:10]
        start = float(i['open'])
        high = float(i['high'])
        low = float(i['low'])
        close = float(i['close'])

        cur.execute('INSERT OR IGNORE INTO Bitcoin (date, open, high, low, close) VALUES (?,?,?,?,?)', (date, start, high, low, close))

    conn.commit()


def bitcoin_api():
    base_url = "https://api.coinpaprika.com/v1/"
    start_date = "2022-01-01"
    end_date = "2022-04-01"
    response = requests.get(base_url + "coins/btc-bitcoin/ohlcv/historical?start=" + start_date + "&end=" + end_date)
    data = response.json()
    return data


def bitcoin_table(cur, conn):
    cur.execute("CREATE TABLE IF NOT EXISTS Bitcoin (date TEXT, open NUMBER, high NUMBER, low NUMBER, close NUMBER)")
    
    data = bitcoin_api()

    for i in data:
        date = i['time_open'][:10]
        start = float(i['open'])
        high = float(i['high'])
        low = float(i['low'])
        close = float(i['close'])

        cur.execute('INSERT OR IGNORE INTO Bitcoin (date, open, high, low, close) VALUES (?,?,?,?,?)', (date, start, high, low, close))

    conn.commit()



def main():
    cur, conn = setUpDatabase("project.db")
    
    bitcoin_table(cur, conn)
    stock_table(cur, conn)

main()