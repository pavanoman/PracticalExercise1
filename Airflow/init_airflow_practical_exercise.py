from airflow.operators import BashOperator
from airflow.models import DAG
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime.now() - timedelta(minutes=1),
    'email': [],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('init_airflow_practical_exercise', default_args=default_args, schedule_interval=None, start_date=datetime.now() - timedelta(minutes=1))

Init_MysqlToHive = BashOperator(
    task_id='Init_MysqlToHive',
    bash_command="""  sh /home/cloudera/Documents/PracticalExercise2/Init_MysqlToHive.sh """,
    dag=dag)
    
Init_csvToHive = BashOperator(
    task_id='Init_csvToHive',
    bash_command=""" sh /home/cloudera/Documents/PracticalExercise2/Init_csvToHive.sh  """,
    dag=dag)

Init_ReportingTables2 = BashOperator(
    task_id='Init_ReportingTables2',
    bash_command=""" sh /home/cloudera/Documents/PracticalExercise2/Init_ReportingTables2.sh  """,
    dag=dag)

Init_MysqlToHive.set_downstream(Init_csvToHive)
Init_csvToHive.set_downstream(Init_ReportingTables2)
