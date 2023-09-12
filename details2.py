import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import timedelta

default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 1,
}

dag = DAG(
    'details2',
    default_args=default_args,
    description='previous b.tech_details dag',
    schedule_interval=None,
    max_active_runs=1,
    catchup=False,
    dagrun_timeout=timedelta(minutes=10),
)

t1 = BashOperator(
    task_id='create_bucket',
    bash_command='gsutil mb -p cherry1 gs://revathiairflow',
    dag=dag,
    depends_on_past=False
)

t2 = BashOperator(
    task_id='load_file_one_bucket_to_another',
    bash_command='gsutil cp gs://revathicherry2/btech_details.csv gs://revathiairflow',
    dag=dag,
    depends_on_past=True
)

t3 = BashOperator(
    task_id='creating_dataset_for_raw_layer',
    bash_command='bq mk -d raw-1',
    dag=dag,
    depends_on_past=False
)

t4 = BashOperator(
    task_id='creating_table_for_raw_layer',
    bash_command='bq mk -t raw-1.tab1 id:string,name:string,sal:string,phone:string',
    dag=dag,
    depends_on_past=True
)

t5 = BashOperator(
    task_id='load_data_to_raw_layer',
    bash_command='bq load --source_format=CSV -skip_leading_rows=1 raw.tab1 gs://revathiairflow/b.tech_details.csv',
    dag=dag,
    depends_on_past=True
)

t6 = BashOperator(
    task_id='creating_dataset_for_refine_layer',
    bash_command='bq mk -d refine-1',
    dag=dag,
    depends_on_past=False
)

t7 = BashOperator(
    task_id='creating_table_to_refine_layer',
    bash_command='bq mk -t refine-1.tab2 id:integer,name:string,sal:int64,phone:int64,gender:string',
    dag=dag,
    depends_on_past=True
)

t8 = BashOperator(
    task_id='inserting_data_to_refine_layer',
    bash_command='bq query --use_legacy_sql=false insert into refine-1.tab2 select safe_cast(id as integer),name string,safe_cast(sal as int64),safe_cast(phone as int64),gender string from raw-1.tab1',
    dag=dag,
    depends_on_past=True
)

t9 = BashOperator(
    task_id='creating_view',
    bash_command='bq query --use_legacy_sql=false create view refine-1.view as select * from refine-1.tab2',
    dag=dag,
    depends_on_past=True
)

t1 >> [t2, t3] >> t4 >> [t5, t6] >> t7 >> t8 >> t9
