import airflow
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator 
from airflow.utils.dates import days_ago

# Default arguments for the DAG
default_args = {
    'owner': 'your_username',  # Replace with your username
    'email': ['thuodenis97@gmail.com'],  # Replace with your email address
    'email_on_failure': True,  # Send email on task failure
    'retries': 1,  # Retry a failed task once
    'retry_delay': timedelta(minutes=5),  # Wait 5 minutes between retries
}
spark_dag = DAG(
        dag_id = "spark_airflow_dag",
        default_args=default_args,
        schedule_interval=None,	
        dagrun_timeout=timedelta(minutes=60),
        description='use case of sparkoperator in airflow',
        start_date = airflow.utils.dates.days_ago(1)
)

Extract = SparkSubmitOperator(
		application = "/opt/airflow/dags/spark_etl_script_docker.py",
		conn_id= 'spark_local', 
		task_id='spark_submit_task', 
		dag=spark_dag
		)

Extract