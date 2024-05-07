try:
  from airflow.providers.celery.executors import CeleryExecutor # type: ignore
  print("Module 'airflow.providers.celery.executors' imported successfully!")
except ModuleNotFoundError:
  print("Module 'airflow.providers.celery.executors' not found. Please install the apache-airflow-providers-celery package.")
