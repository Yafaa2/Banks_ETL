#importing the required libraries

import pandas as pd 
import numpy as np 
import requests
import sqlite3
from bs4 import BeautifulSoup
from datetime import datetime

''' Here, you define the required entities and call the relevant
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''

url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
csv_path = 'exchange_rate.csv'
db_name = 'Banks.db'
table_name = 'Largest_banks'
table_attribs = ['Name','MC_USD_Billion']
conn = sqlite3.connect(db_name)
query_statements = [
        'SELECT * FROM Largest_banks',
        'SELECT AVG(MC_GBP_Billion) FROM Largest_banks',
        'SELECT Name from Largest_banks LIMIT 5'
    ]
logfile = 'code_log.txt'
output_path = 'Largest_banks_data.csv'




def log_process (msg):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open (logfile,'a') as f:
        f.write(timestamp + " : " + msg + "\n")


def extract(url, table_attribs):
    df = pd.DataFrame(columns=table_attribs)
    page = requests.get(url).text
    data = BeautifulSoup(page, 'html.parser')
    tables = data.find_all('tbody')[0]
    rows = tables.find_all('tr')
    for row in rows:
        col = row.find_all('td')
        if len(col) != 0:
            get_data = col[1].find_all('a')[1]
            if get_data is not None:
                data_dict = {
                    'Name': get_data.contents[0],
                    'MC_USD_Billion': col[2].contents[0]
                }
                df1 = pd.DataFrame(data_dict, index=[0])
                df = pd.concat([df, df1], ignore_index=True)

    USD_list = list(df['MC_USD_Billion'])
    USD_list = [float(''.join(x.split('\n'))) for x in USD_list]
    df['MC_USD_Billion'] = USD_list

    return df

def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''

    csvfile = pd.read_csv(csv_path)
    exchange_rate = csvfile.set_index('Currency').to_dict()['Rate']
    df['MC_GBP_Billion'] = [np.round(x*exchange_rate['GBP'],2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x*exchange_rate['INR'],2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x*exchange_rate['EUR'],2) for x in df['MC_USD_Billion']]

    return df


def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''

    df.to_csv(output_path)


def load_to_db(df, sql_connection, table_name):
 
 df.to_sql(table_name, sql_connection , if_exists='replace', index=False)


def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''

    for query in query_statements:
        print(query)
        print(pd.read_sql(query, sql_connection), '\n')

log_process('Preliminaries complete. Initiating ETL process.')

df = extract(url, table_attribs)
log_process('Data extraction complete. Initiating Transformation process.')


df = transform(df, csv_path)
log_process('Data transformation complete. Initiating loading process.')

load_to_csv(df, output_path)
log_process('Data saved to CSV file.')

log_process('SQL Connection initiated.')


load_to_db(df, conn, table_name)
log_process('Data loaded to Database as table. Running the query.')

run_query(query_statements, conn)
conn.close()
log_process('Process Complete.')