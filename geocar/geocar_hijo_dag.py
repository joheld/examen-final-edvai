from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

# Definición del DAG hijo
geocar_hijo_dag = DAG(
    dag_id="geocar_hijo_dag",
    default_args={
        "owner": "airflow",
    },
    description="Transformación y carga en Hive",
    schedule_interval=None,
    start_date=days_ago(2),
)

# Definición de las tareas
inicia_proceso = DummyOperator(
    task_id='inicia_proceso',
    dag=geocar_hijo_dag,  # Asociar tarea con el DAG hijo
)

process_load_files = BashOperator(
    task_id='process_load_files',
    bash_command='ssh hadoop@172.17.0.2 /home/hadoop/spark/bin/spark-submit --files /home/hadoop/hive/conf/hive-site.xml /home/hadoop/scripts/car_rental_trans.py',
    dag=geocar_hijo_dag,  # Asociar tarea con el DAG hijo
)

finaliza_proceso = DummyOperator(
    task_id='finaliza_proceso',
    dag=geocar_hijo_dag,  # Asociar tarea con el DAG hijo
)

# Definición de la secuencia de tareas
inicia_proceso >> process_load_files >> finaliza_proceso