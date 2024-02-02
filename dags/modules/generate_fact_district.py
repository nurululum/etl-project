from sqlalchemy import create_engine
import pandas as pd

# Fungsi untuk membuat connection string
def create_engine_db(username, password, host, port, database, db_system):
    conn_string = f"{db_system}://{username}:{password}@{host}:{port}/{database}"
    return create_engine(conn_string)

def process_and_load_data(df, time_col, period_format, table_name, engine_postgre):
    df[time_col] = pd.to_datetime(df['tanggal']).dt.strftime(period_format)

    # Load dim_case dan dim_district dari PostgreSQL
    dim_case = pd.read_sql("SELECT * FROM dim_case", con=engine_postgre)
    dim_district = pd.read_sql("SELECT * FROM dim_district", con=engine_postgre)

    # Mengubah struktur data
    df_melted = pd.melt(df, id_vars=['kode_kab', time_col], var_name='status_detail', value_name='total')

    # Gabungkan dengan dim_case dan dim_district
    df_merged = pd.merge(df_melted, dim_case, on='status_detail')
    df_merged = pd.merge(df_merged, dim_district, left_on='kode_kab', right_on='district_id')

    # Kelompokkan data dan hitung total
    df_grouped = df_merged.groupby(['district_id', 'ID', time_col]).agg({'total': 'sum'}).reset_index()
    df_grouped.rename(columns={'ID': 'case_id'}, inplace=True)
    df_grouped['id'] = pd.RangeIndex(start=1, stop=len(df_grouped) + 1, step=1)

    df_grouped_sorted = df_grouped.sort_values(by=[time_col, 'district_id'], ascending=[True, True])
    # Load ke PostgreSQL
    df_grouped_sorted.to_sql(name=table_name, con=engine_postgre, index=False, if_exists='replace')

engine_mysql = create_engine_db('root', 'password', 'mysql', '3306', 'my-db', 'mysql+mysqlconnector')
engine_postgre = create_engine_db('airflow', 'airflow', 'postgres', '5432', 'covid', 'postgresql')

df_covid_jabar = pd.read_sql("SELECT * FROM covid_jabar;", con=engine_mysql)

# Proses dan load data untuk district_monthly dan district_yearly
process_and_load_data(df_covid_jabar, 'month', '%Y-%m', 'district_monthly', engine_postgre)
process_and_load_data(df_covid_jabar, 'year', '%Y', 'district_yearly', engine_postgre)

print("SUCCESSFULLY CREATED AND LOADED DISTRICT_MONTHLY AND DISTRICT_YEARLY TABLES TO POSTGRESQL")