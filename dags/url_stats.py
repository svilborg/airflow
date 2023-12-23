from datetime import datetime
import requests
from airflow import DAG
from airflow.decorators import task

with DAG('UrlStats', start_date=datetime(2022, 1, 1), schedule_interval='@daily', catchup=False) as dag:
    @task
    def get_urls():
        return ['https://www.example.com', 'https://www.google.com', 'https://www.github.com']


    @task
    def access_url(url):
        start_time = datetime.now()
        response = requests.get(url)
        end_time = datetime.now()
        elapsed_time = end_time - start_time
        return {url: elapsed_time.total_seconds()}

    @task
    def log_access_times(times):
        for entry in times:
            for url, time in entry.items():
                print(f"Accessed {url} in {time} seconds")


    time_per_url = access_url.expand(url=get_urls())

    log_access_times(times=time_per_url)
