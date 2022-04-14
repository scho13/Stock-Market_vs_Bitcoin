from hashlib import new
import sqlite3
import os
import unittest
import json, collections
import requests

# Team Name: Machos
# Group Members: Shin Cho and Rebecca Mao

def setUpDatabase(db_name):
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path+'/'+db_name)
    cur = conn.cursor()
    return cur, conn

def stock_api():
    response_API = requests.get('https://api.twelvedata.com/time_series?symbol=DJI&interval=1day&start_date=2021-01-01&end_date=2022-01-01&apikey=e7702bf29d4148cca08ed5c4180e21eb')
    data = response_API.json()
    return data

def stock_table(cur, conn):
    cur.execute("CREATE TABLE IF NOT EXISTS Stock (date TEXT UNIQUE, stock_open NUMBER, stock_high NUMBER, stock_low NUMBER, stock_close NUMBER)")
    
    data = stock_api()
    new_data = sorted(data.items())

    print(new_data)

    for i in new_data['values']:
        date = i['datetime'][:10]
        start = float(i['open'])
        high = float(i['high'])
        low = float(i['low'])
        close = float(i['close'])

        cur.execute('INSERT OR IGNORE INTO Stock (date, stock_open, stock_high, stock_low, stock_close) VALUES (?,?,?,?,?)', (date, start, high, low, close))

    conn.commit()


def bitcoin_api():
    base_url = "https://api.coinpaprika.com/v1/"
    start_date = "2021-01-01"
    end_date = "2021-12-31"
    response = requests.get(base_url + "coins/btc-bitcoin/ohlcv/historical?start=" + start_date + "&end=" + end_date)
    data = response.json()
    return data


def bitcoin_table(cur, conn):
    cur.execute("CREATE TABLE IF NOT EXISTS Bitcoin (date TEXT UNIQUE, bitcoin_open NUMBER, bitcoin_high NUMBER, bitcoin_low NUMBER, bitcoin_close NUMBER)")
    
    data = bitcoin_api()

    for i in data:
        date = i['time_open'][:10]
        start = float(i['open'])
        high = float(i['high'])
        low = float(i['low'])
        close = float(i['close'])

        cur.execute('INSERT OR IGNORE INTO Bitcoin (date, bitcoin_open, bitcoin_high, bitcoin_low, bitcoin_close) VALUES (?,?,?,?,?)', (date, start, high, low, close))

    conn.commit()



def main():
    cur, conn = setUpDatabase("project.db")
    
    bitcoin_table(cur, conn)
    stock_table(cur, conn)



if __name__ == "__main__":
    main()
    unittest.main(verbosity = 2)