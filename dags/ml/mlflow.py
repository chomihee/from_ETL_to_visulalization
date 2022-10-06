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
    dag_id="ml-mlflow_test",
    start_date=dt.datetime(2021, 12, 31, 0, 0, tzinfo=ETZ),
    schedule_interval=SCHEDULE_INTERVAL,
    max_active_runs=1,
)
def mlflow_test():
    from mlflow import mlflow, sklearn
    mlflow.set_experiment("mlflow_test")
    mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI"))
    X = np.array([-2, -1, 0, 1, 2, 1]).reshape(-1, 1)
    y = np.array([0, 0, 1, 1, 1, 0])
    lr = LogisticRegression()
    lr.fit(X, y)
    score = lr.score(X, y)
    print("Score: %s" % score)
    mlflow.log_metric("score", score)
    mlflow.sklearn.log_model(lr, "model")
    print("Model saved in run %s" % mlflow.active_run().info.run_uuid)
	

mlflow_test_dag = mlflow_test()
