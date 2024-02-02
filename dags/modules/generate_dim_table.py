from sqlalchemy import create_engine
import pandas as pd
import numpy as np

# MySQL connection
mysql_username = 'root'
mysql_password = 'password'
mysql_host = 'mysql'
mysql_port = '3306'
mysql_database = 'my-db'

mysql_conn_string = f"mysql+mysqlconnector://{mysql_username}:{mysql_password}@{mysql_host}:{mysql_port}/{mysql_database}"
engine_mysql = create_engine(mysql_conn_string)

# PostgreSQL connection
postgres_username = 'airflow'
postgres_password = 'airflow'
postgres_host = 'postgres'
postgres_port = '5432'
postgres_database = 'covid'

postgres_conn_string = f"postgresql://{postgres_username}:{postgres_password}@{postgres_host}:{postgres_port}/{postgres_database}"
engine_postgre = create_engine(postgres_conn_string)

# Dim Province
df_province = pd.read_sql("SELECT kode_prov, nama_prov FROM covid_jabar;", con=engine_mysql)
dim_province = df_province.rename(columns={'kode_prov': 'province_id', 'nama_prov': 'province_name'}).drop_duplicates()
dim_province.to_sql(name='dim_province', con=engine_postgre, index=False, if_exists='replace')

# Dim District
df_district = pd.read_sql("SELECT DISTINCT kode_kab, nama_kab, nama_prov FROM covid_jabar;", con=engine_mysql)
dim_district = df_district.rename(columns={'kode_kab': 'district_id', 'nama_kab': 'district_name'}).drop_duplicates()
dim_district_sorted = dim_district.sort_values(by='district_id', ascending=True)
dim_district_sorted.to_sql(name='dim_district', con=engine_postgre, index=False, if_exists='replace')


# Dim Case
df_case = pd.read_sql("SELECT * FROM covid_jabar;", con=engine_mysql)
status_columns = ['suspect_diisolasi', 'suspect_discarded', 'suspect_meninggal', 'closecontact_dikarantina', 'closecontact_discarded', 'probable_diisolasi', 'probable_discarded', 'confirmation_sembuh', 'confirmation_meninggal', 'closecontact_meninggal', 'probable_meninggal']
status_names = ['suspect', 'closecontact', 'confirmation', 'probable']

df_melted = pd.melt(df_case, id_vars=['kode_kab', 'kode_prov', 'nama_kab', 'nama_prov', 'tanggal'], value_vars=status_columns, var_name='status_detail', value_name='total')
df_melted['status_name'] = df_melted['status_detail'].apply(lambda x: next((name for name in status_names if name in x), None))

df_melted_sorted = df_melted.sort_values(by=['status_detail', 'status_name'])
df_melted_sorted.drop_duplicates(subset=['status_detail', 'status_name'], inplace=True)

df_melted_sorted.reset_index(drop=True, inplace=True)
df_melted_sorted['ID'] = df_melted_sorted.index + 1
df_melted_sorted[['ID', 'status_name', 'status_detail']].to_sql(name='dim_case', con=engine_postgre, index=False, if_exists='replace')

print("SUCCESSFULLY TRANSFORMED DIMS TABLE AND LOADED TO POSTGRESQL")