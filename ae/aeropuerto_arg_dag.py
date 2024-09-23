from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago


args = {
    'owner': 'airflow',
}

with DAG(
    dag_id='aeropuerto_arg_dag',
    default_args= args,
    description='Pipeline con ingest y process',
    schedule_interval='0 0 * * *',
    start_date=days_ago(2),
    dagrun_timeout=timedelta(minutes=60),
    tags=['ingest', 'transform'],
    params={"example_key": "example_value"},

) as dag:
    
    inicia_proceso = DummyOperator(
        task_id='inicia_proceso',
    )

    ingest_files = BashOperator(
            task_id='ingest_files',
            bash_command='/usr/bin/sh /home/hadoop/scripts/ingest_aeropuertos_arg.sh ',
        )
    
    process_files = BashOperator(
            task_id='process_files',
            bash_command='ssh hadoop@172.17.0.2 /home/hadoop/spark/bin/spark-submit --files /home/hadoop/hive/conf/hive-site.xml /home/hadoop/scripts/aeropuerto_arg_trans.py ',
        )
    
    finaliza_proceso = DummyOperator(
        task_id='finaliza_proceso',
    )

    inicia_proceso >> ingest_files >> process_files >> finaliza_proceso
    

if __name__ == "__main__":
    dag.cli()


    
    


