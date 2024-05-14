# Scenario🗺

This project requires you to compile the list of the top 10 largest banks in the world ranked by market capitalization in billion USD. Further, you need to transform the data and store it in USD, GBP, EUR, and INR per the exchange rate information made available to you as a CSV file. You should save the processed information table locally in a CSV format and as a database table. Managers from different countries will query the database table to extract the list and note the market capitalization value in their own currency.

## Tasks
- Write a data extraction function to retrieve the relevant information from the required URL.
- Transform the available GDP information into 'Billion USD' from 'Million USD'.
- Load the transformed information to the required CSV file and as a database file.
- Run the required query on the database.
- Log the progress of the code with appropriate timestamps.

## Installation

Use the package manager [pip](https://pip.pypa.io/en/stable/) to install foobar.

```bash
pip install pandas
pip install numpy
pip install bs4
```

## Implementation
- Importing the required libraries
```python
import pandas as pd 
import numpy as np 
import requests
import sqlite3
from bs4 import BeautifulSoup
from datetime import datetime
```
## Tasks 📝
- Here, I defined the required entities

```python
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
```
-  This function logs the mentioned message of a given stage of the
    code execution to a log file.

```python
def log_process (msg):

    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open (logfile,'a') as f:
        f.write(timestamp + " : " + msg + "\n")
```
-  This function extracts the tabular information from the given URL under the heading By Market Capitalization by using bs4 and saves it to a data frame.

```python
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
```
-  This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies

```python
def transform(df, csv_path):
    
    csvfile = pd.read_csv(csv_path)
    exchange_rate = csvfile.set_index('Currency').to_dict()['Rate']
    df['MC_GBP_Billion'] = [np.round(x*exchange_rate['GBP'],2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x*exchange_rate['INR'],2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x*exchange_rate['EUR'],2) for x in df['MC_USD_Billion']]

    return df

```
-  This function saves the final data frame as a CSV file in
    the provided path.

```python
def load_to_csv(df, output_path):
    
    df.to_csv(output_path)

```
-  Here, I call the relevant functions in the correct order to complete the project.

```python
def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists = 'replace', index = False)
```
-  This function runs the query on the database table and
    prints the output on the terminal.
```python
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
```
## Results
![Capture](https://github.com/Yafaa2/Banks_ETL/assets/123892816/96527697-07af-4257-857a-c5cb712daae5)

![Capture 2](https://github.com/Yafaa2/Banks_ETL/assets/123892816/cbd0bb54-17a4-4783-908c-e32908ec3636)

![Capture 3](https://github.com/Yafaa2/Banks_ETL/assets/123892816/3b8672cb-9782-4cd2-9866-11d5d3a79fd8)

## Contributing

Pull requests are welcome. For major changes, please open an issue first
to discuss what you would like to change.

Please make sure to update tests as appropriate.
