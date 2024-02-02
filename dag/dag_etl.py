from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy import DummyOperator 
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime
from datetime import timedelta



path = '/opt/airflow/dags/modules'

with DAG(
        dag_id="dag_etl",
        schedule_interval= "0 0 * * *",
        start_date=datetime(2024, 1, 1),
        catchup=False
        ) as dag:

        start = DummyOperator(
            task_id='start')

        data_extraction = BashOperator(
            task_id = "data_extraction",
            bash_command = f'python3 {path}/data_extraction.py',
            execution_timeout=timedelta(minutes=5),
            retries=2,
            retry_delay=timedelta(minutes=5)
        )


        ddl_postgres = BashOperator(
            task_id = "ddl_postgres",
            bash_command = f'python3 {path}/ddl_postgres.py',
            execution_timeout=timedelta(minutes=5),
            retries=2,
            retry_delay=timedelta(minutes=5)
        )

        generate_dim_table =  BashOperator(
            task_id = "generate_dim_table",
            bash_command = f'python3 {path}/generate_dim_table.py',
            execution_timeout=timedelta(minutes=5),
            retries=2,
            retry_delay=timedelta(minutes=5)
        )

        generate_fact_province =  BashOperator(
            task_id = "generate_fact_province",
            bash_command = f'python3 {path}/generate_fact_province.py',
            execution_timeout=timedelta(minutes=5),
            retries=2,
            retry_delay=timedelta(minutes=5)
        )
        
        generate_fact_district = BashOperator(
            task_id = "generate_fact_district",
            bash_command = f'python3 {path}/generate_fact_district.py',
            execution_timeout=timedelta(minutes=5),
            retries=2,
            retry_delay=timedelta(minutes=5)
        )
        
        end = DummyOperator(
            task_id='end')

        start >> data_extraction >> ddl_postgres >> generate_dim_table >> [generate_fact_province,generate_fact_district] >> end
