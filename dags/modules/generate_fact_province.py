from sqlalchemy import create_engine
import pandas as pd

# Fungsi untuk membuat koneksi engine
def create_engine_db(username, password, host, port, database, db_type):
    conn_string = f"{db_type}://{username}:{password}@{host}:{port}/{database}"
    return create_engine(conn_string)

# Inisialisasi koneksi database
engine_mysql = create_engine_db('root', 'password', 'mysql', '3306', 'my-db', 'mysql+mysqlconnector')
engine_postgre = create_engine_db('airflow', 'airflow', 'postgres', '5432', 'covid', 'postgresql')

# Baca dan persiapkan data dasar
df_covid_jabar = pd.read_sql("SELECT * FROM covid_jabar;", con=engine_mysql)
dim_province = pd.read_sql("SELECT * FROM dim_province;", con=engine_postgre)
dim_case = pd.read_sql("SELECT * FROM dim_case;", con=engine_postgre)

# Gabungkan df_covid_jabar dengan dim_province
df_covid_jabar = df_covid_jabar.merge(dim_province, how='left', left_on='nama_prov', right_on='province_name')

status_columns = ['suspect_diisolasi', 'suspect_discarded', 'suspect_meninggal', 'closecontact_dikarantina', 'closecontact_discarded', 'probable_diisolasi', 'probable_discarded', 'confirmation_sembuh', 'confirmation_meninggal', 'closecontact_meninggal', 'probable_meninggal']
status_names = ['suspect', 'closecontact', 'probable', 'confirmation']

# Fungsi untuk memproses dan memuat data
def process_load(df, time_unit, dim_case, engine_postgre, table_name):
    df_melted = pd.melt(df, id_vars=['kode_kab', 'province_id', 'nama_kab', 'nama_prov', 'tanggal'], value_vars=status_columns, var_name='status_detail', value_name='total')
    df_melted['status_name'] = df_melted['status_detail'].apply(lambda x: next((name for name in status_names if name in x), None))
    df_melted = df_melted.merge(dim_case, how='left', on=['status_name', 'status_detail'])
    
    # Format tanggal sesuai unit waktu
    if time_unit == 'day':
        df_melted['date'] = pd.to_datetime(df_melted['tanggal']).dt.strftime('%Y-%m-%d')
    elif time_unit == 'month':
        df_melted['month'] = pd.to_datetime(df_melted['tanggal']).dt.strftime('%Y-%m')
    elif time_unit == 'year':
        df_melted['year'] = pd.to_datetime(df_melted['tanggal']).dt.strftime('%Y')
    
    time_col = 'date' if time_unit == 'day' else time_unit
    df_grouped = df_melted.groupby(['province_id', 'ID', time_col]).agg(total=('total', 'sum')).reset_index().rename(columns={'ID': 'case_id'})
    df_grouped['id'] = pd.RangeIndex(start=1, stop=len(df_grouped) + 1, step=1)
    
    df_grouped_sorted = df_grouped.sort_values(by=[time_col, 'case_id'], ascending=[True, True])
    df_grouped_sorted.to_sql(name=table_name, con=engine_postgre, index=False, if_exists='replace', method='multi')

# Proses dan muat data untuk daily, monthly, dan yearly
process_load(df_covid_jabar, 'day', dim_case, engine_postgre, 'province_daily')
process_load(df_covid_jabar, 'month', dim_case, engine_postgre, 'province_monthly')
process_load(df_covid_jabar, 'year', dim_case, engine_postgre, 'province_yearly')

print("SUCCESSFULLY CREATED AND LOADED PROVINCE TABLES TO POSTGRESQL")
