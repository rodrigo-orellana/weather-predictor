from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
# Funciones:
from dag_function.function import *


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}

#Inicialización del grafo DAG de tareas para el flujo de trabajo
dag = DAG(
    'a_predictor_weather',
    default_args=default_args,
    description='Preditor de temperatura y humedad',
    schedule_interval=timedelta(days=1),
)



Download_A_zip = BashOperator(
    task_id='Download_A_zip',
    bash_command='wget https://github.com/manuparra/MaterialCC2020/raw/master/humidity.csv.zip -O /home/rockdrigo/CC2/tmp/data/humidity.csv.zip',
    dag=dag,
)

Unzip_A = BashOperator(
    task_id='unzip_A_zip',
    depends_on_past=False,
    bash_command='unzip -o /home/rockdrigo/CC2/tmp/data/humidity.csv.zip -d /home/rockdrigo/CC2/tmp/data/',
    dag=dag,
)

RM_zip_A = BashOperator(
    task_id='rm_A_zip',
    depends_on_past=False,
    bash_command='rm /home/rockdrigo/CC2/tmp/data/humidity.csv.zip',
    dag=dag,
)

Download_B_zip = BashOperator(
    task_id='Download_B_zip',
    bash_command='wget https://github.com/manuparra/MaterialCC2020/raw/master/temperature.csv.zip -O /home/rockdrigo/CC2/tmp/data/temperature.csv.zip',
    dag=dag,
)

Unzip_B_zip = BashOperator(
    task_id='unzip_B_zip',
    depends_on_past=False,
    bash_command='unzip -o /home/rockdrigo/CC2/tmp/data/temperature.csv.zip -d /home/rockdrigo/CC2/tmp/data/',
    dag=dag,
)

RM_B_zip = BashOperator(
    task_id='rm_B_zip',
    depends_on_past=False,
    bash_command='rm /home/rockdrigo/CC2/tmp/data/temperature.csv.zip',
    dag=dag,
)

AWK_humedad = BashOperator(
    task_id='awk_humedad',
    depends_on_past=False,    
    bash_command='cat /home/rockdrigo/CC2/tmp/data/humidity.csv |awk -F, \'{print $4}\' > /home/rockdrigo/CC2/tmp/data/humidity_sanfransisco.csv',
    dag=dag,
)

AWK_temp = BashOperator(
    task_id='awk_temperatura',
    depends_on_past=False, 
    bash_command='cat /home/rockdrigo/CC2/tmp/data/temperature.csv |awk -F, \'{print $1 ";" $4}\' > /home/rockdrigo/CC2/tmp/data/temperature_sanfransisco.csv',
    dag=dag,
)

Union_hum_temp = BashOperator(
    task_id='union_hum_tem',
    depends_on_past=False, 
    bash_command='paste -d \';\' /home/rockdrigo/CC2/tmp/data/temperature_sanfransisco.csv /home/rockdrigo/CC2/tmp/data/humidity_sanfransisco.csv > /home/rockdrigo/CC2/tmp/data/hum_temp_sanfrancisco.csv',
    dag=dag,
)


Quita_head = BashOperator(
    task_id='quita_cabecera',
    depends_on_past=False, 
    bash_command='tail -n +2 /home/rockdrigo/CC2/tmp/data/hum_temp_sanfrancisco.csv > /home/rockdrigo/CC2/tmp/data/hum_temp_sanfrancisco_sc.csv',
    dag=dag,
)

ADD_new_head = BashOperator(
    task_id='agrega_cabecera_correcta',
    depends_on_past=False, 
    bash_command='echo -e "DATE;TEMP;HUM\\n$(cat /home/rockdrigo/CC2/tmp/data/hum_temp_sanfrancisco_sc.csv)" > /home/rockdrigo/CC2/tmp/data/hum_temp_sanfrancisco_to_bd.csv',
    dag=dag,
)
"""
build_docker = BashOperator(
    task_id='build_docker',
    depends_on_past=True, 
    bash_command='cd /home/rockdrigo/CC2/practica2/docker/ && docker-compose build',
    dag=dag,
)
"""
""" Insert_db = BashOperator(
    task_id='insert_mongo',
    depends_on_past=True, 
    bash_command='python3 /home/rockdrigo/CC2/practica2/src/insert_mongo.py',
    dag=dag,
) """
Insert_db = PythonOperator(
    task_id='insert_mongo',
    depends_on_past=False, 
    provide_context=True,
    python_callable=insert_to_mongo,
    dag=dag,
)

Delete_and_make_pred = PythonOperator(
    task_id='delete_and_make_prediction',
    depends_on_past=False, 
    provide_context=True,
    python_callable=delete_and_make_prediction,
    dag=dag,
)
"""
DownloadApiSrc_1 = BashOperator(
    task_id='download_api_1',
    bash_command='wget https://raw.githubusercontent.com/rodrigo-orellana/weather-predictor/master/src/prediction_visor.py -O /home/rockdrigo/CC2/practica2/docker/app1/prediction_visor.py',
    dag=dag,
)

DownloadApiSrc_2 = BashOperator(
    task_id='download_api_2',
    bash_command='wget https://raw.githubusercontent.com/rodrigo-orellana/weather-predictor/master/src/scrapy_weather.py -O /home/rockdrigo/CC2/practica2/docker/app2/scrapy_weather.py',
    dag=dag,
)

DownloadTester = BashOperator(
    task_id='download_tester',
    bash_command='wget https://raw.githubusercontent.com/rodrigo-orellana/weather-predictor/master/src/scrapy_weather.py -O /home/rockdrigo/CC2/practica2/docker/app2/scrapy_weather.py',
    dag=dag,
)
"""
UpDockers = BashOperator(
    task_id='up_dockers',
    depends_on_past=False, 
    bash_command='cd /home/rockdrigo/CC2/tmp/docker/ && docker-compose up -d',
    dag=dag,
)

UpDockerMongo = BashOperator(
    task_id='up_docker_mongo',
    depends_on_past=False, 
    bash_command='cd /home/rockdrigo/CC2/tmp/docker/ && docker-compose up -d mongo',
    dag=dag,
)


UnitTest = BashOperator(
    task_id='uni_test',
    depends_on_past=False,
    bash_command='cd /home/rockdrigo/CC2/tmp/src/app2 && python3 test.py',
    dag=dag,
)
"""
UpAPP = BashOperator(
    task_id='make_prediction1',
    depends_on_past=True, 
    bash_command='wget  --timeout=1800 http://localhost:8001/servicio/v1/prediccion/24horas/ -O /home/rockdrigo/CC2/practica2/data/predictions_v1_24h.yml',
    dag=dag,
)
"""


ClonarRepo = BashOperator(
    task_id='ClonarRepo',
    depends_on_past=False,
    bash_command='rm -r -f /home/rockdrigo/CC2/tmp && git clone https://github.com/rodrigo-orellana/weather-predictor.git /home/rockdrigo/CC2/tmp',
    dag=dag,
)


[[ClonarRepo >> Download_A_zip >> Unzip_A >> [RM_zip_A, AWK_humedad]],[ClonarRepo >> Download_B_zip >> Unzip_B_zip >> [RM_B_zip, AWK_temp]]] 
[AWK_humedad, AWK_temp] >> Union_hum_temp >> Quita_head >> ADD_new_head >> Insert_db
#[DownloadApiSrc_1, DownloadApiSrc_1, DownloadTester] >> UnitTest >> UpDockerMongo >> Insert_db >> Delete_and_make_pred >> UpDockers
ClonarRepo >> UnitTest >> UpDockerMongo >> Insert_db >> Delete_and_make_pred >> UpDockers
