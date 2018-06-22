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

dag = DAG('airflow_practical_exercise', default_args=default_args, schedule_interval=None, start_date=datetime.now() - timedelta(minutes=1))

MysqlToHive = BashOperator(
    task_id='MysqlToHive',
    bash_command=""" sh /home/cloudera/Documents/PracticalExercise2/MysqlToHive.sh """,
    dag=dag)
    
csvToHive = BashOperator(
    task_id='csvToHive',
    bash_command=""" sh /home/cloudera/Documents/PracticalExercise2/csvToHive.sh """,
    dag=dag)

ReportingTables1= BashOperator(
    task_id='ReportingTables1',
    bash_command=""" sh /home/cloudera/Documents/PracticalExercise2/ReportingTables1.sh """,
    dag=dag)

ReportingTables2= BashOperator(
    task_id='ReportingTables2',
    bash_command=""" sh /home/cloudera/Documents/PracticalExercise2/ReportingTables2.sh  """,
    dag=dag)



MysqlToHive.set_downstream(csvToHive)
csvToHive.set_downstream(ReportingTables1)
ReportingTables1.set_downstream(ReportingTables2)
