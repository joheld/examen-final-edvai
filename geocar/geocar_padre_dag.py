from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

# Definición del DAG padre
geocar_padre_dag = DAG(
    dag_id="geocar_padre_dag",
    default_args={
        "owner": "airflow",
    },
    description="Ingesta y llama a geocar_hijo_dag",
    schedule_interval='0 0 * * *',
    start_date=days_ago(2),
)

# Definición de las tareas
inicia_proceso = DummyOperator(
    task_id='inicia_proceso',
    dag=geocar_padre_dag,  # Asociar tarea con el DAG padre
)

ingest_files = BashOperator(
    task_id='ingest_files',
    bash_command='/usr/bin/sh /home/hadoop/scripts/ingest_car_rental.sh ',
    dag=geocar_padre_dag,  # Asociar tarea con el DAG padre
)

trigger_geocar_hijo_dag = TriggerDagRunOperator(
    task_id="trigger_geocar_hijo_dag",
    trigger_dag_id="geocar_hijo_dag",
    dag=geocar_padre_dag,  # Asociar tarea con el DAG padre
)

finaliza_proceso = DummyOperator(
    task_id='finaliza_proceso',
    dag=geocar_padre_dag,  # Asociar tarea con el DAG padre
)

# Definición de la secuencia de tareas
inicia_proceso >> ingest_files >> trigger_geocar_hijo_dag >> finaliza_proceso