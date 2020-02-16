import urllib
import pathlib
import sqlalchemy 
import pandas as pd
import numpy as np
from sqlalchemy.orm import Session,sessionmaker
from mapping_classes import Base

from mapping_classes.class_airlines import airlines
from mapping_classes.class_airports import airports
from mapping_classes.class_flights import flights

params = urllib.parse.quote_plus("DRIVER={ODBC Driver 17 for SQL Server};SERVER=DESKTOP\SQLEXPRESS;DATABASE=airlines;UID=YourUserId;PWD=YourPwd;Trusted_Connection=yes;")
engine = sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)

Base.metadata.create_all(engine)

Session = sessionmaker(bind=engine)
session = Session()

data_path = pathlib.Path('data')
files = data_path.glob('*')

# for file_path in files:
#     session = Session()
#     df = pd.read_csv(file_path,nrows=100_000,keep_default_na=False)
#     session.bulk_insert_mappings(file_path.stem, df.to_dict(orient="records")) #need to be
#     session.commit()
#     session.close()

session = Session()
df = pd.read_csv("data\\airlines.csv",nrows=100_000,keep_default_na=False)
session.bulk_insert_mappings(airlines, df.to_dict(orient="records")) #need to be

df = pd.read_csv("data\\airports.csv",nrows=100_000,keep_default_na=False,dtype={"LATITUDE":str,"LONGITUDE":str},decimal='.')	 # problem with reading latitude and longitude
# df = pd.to_numeric(df, downcast='float')
df = df.apply(pd.to_numeric, errors='ignore',downcast='float')
df = df.round(2)

session.bulk_insert_mappings(airports, df.to_dict(orient="records")) #need to be

df = pd.read_csv("data\\flights.csv",nrows=100_000,keep_default_na=False)
session.bulk_insert_mappings(flights, df.to_dict(orient="records")) #need to be

session.commit()
session.close()


