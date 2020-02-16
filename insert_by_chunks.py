import pandas as pd
import sqlalchemy 
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, Float, Date, String, VARCHAR

import json
import pyodbc
import urllib

from class_flights import flights
from class_airlines import airlines

import multiprocessing as mp
import time
import concurrent.futures
params = urllib.parse.quote_plus("DRIVER={ODBC Driver 17 for SQL Server};SERVER=DESKTOP\SQLEXPRESS;DATABASE=airlines;UID=YourUserId;PWD=YourPwd;Trusted_Connection=yes;")

engine = sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect=%s" % params) #echo = True to see sql stuff
Base = declarative_base()
Base.metadata.create_all(engine)

file_name = 'data\\flights.csv'
def run_sql(chunk,tablename,index):
    start = time.perf_counter()
    chunk.to_sql(con=engine, name=tablename, if_exists='append',index=False)
    end = time.perf_counter() - start
    return f'--------- Process {index} took {end} to finish chunk {index}'

if __name__ == '__main__':
    # df_chunk = pd.read_csv(file_name,dtype={'YEAR': int,'MONTH': int,'DAY': int,'DAY_OF_WEEK': int,'FLIGHT_NUMBER':int}, chunksize=100_000) #nrows=5_000_000
    df_chunk = pd.read_csv(file_name, chunksize=100_000,nrows=1_000_000) #nrows=5_000_000
    
    start = 0
    with concurrent.futures.ProcessPoolExecutor() as executor:

        start = time.perf_counter() # time.perf_counter or time.process_time
        results = [executor.submit(run_sql,chunk,flights.__tablename__,index) for index,chunk in enumerate(df_chunk)]

        for res in concurrent.futures.as_completed(results):
            print(res.result())
           
    end = time.perf_counter() - start
    print(f"List processing complete. With {end}")

