#from email import contentmanager
#from hashlib import new
import sqlite3
import os
import unittest
import json
import requests
#import numpy as np
#import pandas as pd
#from scipy.stats import pearsonr
import statistics
import math

# Team Name: Machos
# Group Members: Shin Cho and Rebecca Mao

def setUpDatabase(db_name):
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path+'/'+db_name)
    cur = conn.cursor()
    return cur, conn

def stock_api():
    response_API = requests.get('https://api.twelvedata.com/time_series?symbol=DJI&interval=1day&start_date=2021-01-01&end_date=2022-01-01&order=ASC&apikey=e7702bf29d4148cca08ed5c4180e21eb')
    data = response_API.json()
    return data

def create_stock_table(cur, conn):
    cur.execute("CREATE TABLE IF NOT EXISTS Stock (date TEXT UNIQUE, stock_open NUMBER, stock_high NUMBER, stock_low NUMBER, stock_close NUMBER)")
    conn.commit()

def add_into_stock_table(cur, conn, add):
    data = stock_api()
    starting = 0 + add
    limit = 25 + add
    data_lst = []
    for i in data['values'][starting:limit]:
        date = i['datetime'][:10]
        start = float(i['open'])
        high = float(i['high'])
        low = float(i['low'])
        close = float(i['close'])
        data_lst.append((date, start, high, low, close))
        for tup in data_lst:
            cur.execute('INSERT OR IGNORE INTO Stock (date, stock_open, stock_high, stock_low, stock_close) VALUES (?,?,?,?,?)', (tup[0], tup[1], tup[2], tup[3], tup[4]))

        conn.commit()


def bitcoin_api():
    base_url = "https://api.coinpaprika.com/v1/"
    start_date = "2021-01-01"
    end_date = "2021-12-31"
    
    response = requests.get(base_url + "coins/btc-bitcoin/ohlcv/historical?start=" + start_date + "&end=" + end_date)
    data = response.json()

    return data


def create_bitcoin_table(cur, conn):
    cur.execute("CREATE TABLE IF NOT EXISTS Bitcoin (date TEXT UNIQUE, bitcoin_open NUMBER, bitcoin_high NUMBER, bitcoin_low NUMBER, bitcoin_close NUMBER)")
    conn.commit()

def add_into_bitcoin_table(cur, conn, add):
    data = bitcoin_api()
    starting = 0 + add
    limit = 25 + add
    data_lst = []
    for i in data[starting:limit]:
        date = i['time_open'][:10]
        start = float(i['open'])
        high = float(i['high'])
        low = float(i['low'])
        close = float(i['close'])
        data_lst.append((date, start, high, low, close))
        for tup in data_lst:
            cur.execute('INSERT OR IGNORE INTO Bitcoin (date, bitcoin_open, bitcoin_high, bitcoin_low, bitcoin_close) VALUES (?,?,?,?,?)', (tup[0], tup[1], tup[2], tup[3], tup[4]))
         
        conn.commit()


def join_tables(cur,conn):
    cur.execute("SELECT Stock.date, Bitcoin.bitcoin_close, Stock.stock_close FROM Bitcoin JOIN Stock ON Bitcoin.date = Stock.date")
    results = cur.fetchall()
    conn.commit()
    return results

def correlation_calc(list_of_tuple):

    bitcoin_list = []
    stock_list = []
    bitcoin_calc = []
    stock_calc = []
    upper_function = []
    bitcoin_calc2 = []
    stock_calc2 = []

    #bitcoin and stock prices in a list
    for date, bitcoin_price, stock_price in list_of_tuple:
        bitcoin_list.append(bitcoin_price)
        stock_list.append(stock_price)

    #bitcoin and stock prices mean
    bitcoin_avg = statistics.mean(bitcoin_list)
    stock_avg = statistics.mean(stock_list)

    #bitcoin and stock prices upper function
    for i in range(len(bitcoin_list)):
        bitcoin_calc.append(bitcoin_list[i] - bitcoin_avg)
        stock_calc.append(stock_list[i] - stock_avg)

    for num1, num2 in zip(bitcoin_calc, stock_calc):
	    upper_function.append(num1 * num2)
        
    upper_final = sum(upper_function)

    #bitcoin and stock prices lower function
    for i in range(len(bitcoin_list)):
        bitcoin_calc2.append((bitcoin_list[i] - bitcoin_avg)**2)
        stock_calc2.append((stock_list[i] - stock_avg)**2)

    lower_function = sum(bitcoin_calc2) 
    lower_function2 = sum(stock_calc2)

    lower_function3 = lower_function*lower_function2

    lower_final = math.sqrt(lower_function3)

    final = upper_final/lower_final
    return final


def write_correlation_calc(filename, correlation):
    with open(filename, "w", newline="") as fileout:
        fileout.write("Correlation between bitcoin price and DJI stock price:\n")
        fileout.write("======================================================\n\n")
        fileout.write(f"The correlation coefficient between bitcoin price and DJI stock price was r = {correlation}.\n")
        fileout.close()

def main():
    cur, conn = setUpDatabase("project.db")
    
    #Creates Bitcoin table 
    create_bitcoin_table(cur, conn)
    cur.execute('SELECT COUNT(*) FROM Bitcoin')
    conn.commit()
    info = cur.fetchall()
    length = info[0][0]
    #print(length)
    if length < 25:
        add_into_bitcoin_table(cur, conn, 0)
    elif 25 <= length < 50:
        add_into_bitcoin_table(cur, conn, 25)
    elif 50 <= length < 75:
        add_into_bitcoin_table(cur, conn, 50)
    elif 75 <= length < 100:
        add_into_bitcoin_table(cur, conn, 75)
    elif 100 <= length < 125:
        add_into_bitcoin_table(cur, conn, 100)
    elif 125 <= length < 150:
        add_into_bitcoin_table(cur, conn, 125)
    print(length)
    
    #Creates Stock table
    create_stock_table(cur, conn)
    cur.execute('SELECT COUNT(*) FROM Stock')
    conn.commit()
    length = info[0][0]
    #print(length)
    if length < 25:
        add_into_stock_table(cur, conn, 0)
    elif 25 <= length < 50:
        add_into_stock_table(cur, conn, 25)
    elif 50 <= length < 75:
        add_into_stock_table(cur, conn, 50)
    elif 75 <= length < 100:
        add_into_stock_table(cur, conn, 75)
    elif 100 <= length < 125:
        add_into_stock_table(cur, conn, 100)
    elif 125 <= length < 150:
        add_into_stock_table(cur, conn, 125)
    print(length)
    

    set_up_calculations = join_tables(cur, conn)
    calculations = correlation_calc(set_up_calculations)
    write_correlation_calc("calculations.txt", calculations)




if __name__ == "__main__":
    main()
    unittest.main(verbosity = 2)