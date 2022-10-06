  # mlflow_tracking.py
import os
from random import random, randint
import datetime as dt
from airflow.decorators import dag
import pendulum
import numpy as np
import sklearn
from sklearn.linear_model import LogisticRegression

########################### Set Configs ########################
# SCHEDULE_INTERVAL = "0 0 * * *" 
SCHEDULE_INTERVAL = "@daily"
ETZ = pendulum.timezone("US/Eastern")
################################################################


default_args = {
  'owner': 'airflow'
}

@dag(
    default_args=default_args,
    dag_id="ml-mlflow_test",
    start_date=dt.datetime(2021, 12, 31, 0, 0, tzinfo=ETZ),
    schedule_interval=SCHEDULE_INTERVAL,
    max_active_runs=1,
)
def mlflow_test():
    from mlflow import mlflow,log_metric, log_param, log_artifacts
    mlflow.set_experiment("mlflow_test")
    mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI"))
    print("Running mlflow_tracking.py")

    log_param("param1", randint(0, 100))
        
    log_metric("foo", random())
    log_metric("foo", random() + 1)
    log_metric("foo", random() + 2)

    if not os.path.exists("outputs"):
        os.makedirs("outputs")
    with open("outputs/test.txt", "w") as f:
        f.write("hello world!")
    log_artifacts("outputs")

	

mlflow_test_dag = mlflow_test()
