from sqlalchemy import create_engine
import requests
import os
import pandas as pd 

url_covid = 'http://103.150.197.96:5005/api/v1/rekapitulasi_v2/jabar/harian?level=kab%22'


response = requests.get(url_covid)
data= response.json()['data']['content']
df = pd.DataFrame(data)
df = pd.json_normalize(data)

# Dataframe information
print(df.info())

mysql_username = 'root'
mysql_password = 'password'
mysql_host = 'mysql'
mysql_port = '3306'
mysql_database = 'my-db'

mysql_conn_string = f"mysql+mysqlconnector://{mysql_username}:{mysql_password}@{mysql_host}:{mysql_port}/{mysql_database}"
engine_mysql = create_engine(mysql_conn_string)


df.to_sql(name='covid_jabar', con=engine_mysql,index=False,if_exists='replace')
print('DATA INSERTED TO MYSQL SUCCESSFULLY')
