from sqlalchemy import create_engine, text
import pandas as pd
import psycopg2
import requests
from datetime import date

from functions import cargar_datos
from params import param_bd, param_api

conn = psycopg2.connect(**param_bd)

# insert data into device table
df_device = pd.read_csv("data/device.csv", sep=";")
cargar_datos(conn, "device", df_device)

# insert data into store table
df_store = pd.read_csv("data/store.csv", sep=";")
cargar_datos(conn, "store", df_store)

# insert data into transaction
df_transaction = pd.read_csv("data/transaction.csv", sep=";")
df_transaction['product_sku'] = df_transaction['product_sku'].map(lambda x: int(float(x.replace('v','')) ))
df_transaction['card_number'] = df_transaction['card_number'].map(lambda x: int(float(x.replace('v','').replace(' ','')) ))
cargar_datos(conn, "transaction", df_transaction)

# fetch today UF value from Banco Central API
usuario = param_api['user']
password = param_api['pass']
fecha = str(date.today())
base_url = f"https://si3.bcentral.cl/SieteRestWS/SieteRestWS.ashx?user={usuario}&pass={password}&function=GetSeries&timeseries=F073.UFF.PRE.Z.D&firstdate={fecha}&lastdate={fecha}"

r = requests.get(base_url)
data = r.json()["Series"]["Obs"]

# insert it into uf table
df = pd.DataFrame.from_dict(data)
df = df.rename(columns={'indexDateString':'value_date'})
df = df.drop(columns=['statusCode'])
engine = create_engine("postgresql+psycopg2://postgres:postgres@localhost:5432/DB")
df.to_sql('uf', engine, if_exists='replace')

engine.dispose()

conn.close()