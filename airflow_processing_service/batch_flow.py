import os
import json
import requests
from datetime import datetime, timedelta

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
from airflow.decorators import task

DATALAKE_HOST = "127.0.0.1"
DATALAKE_PORT = "5000"


def build_batch_workflow_dag_v0(user_id: str):

    with DAG(
        f"batch_flow_{user_id}",
        schedule=timedelta(minutes=10),
        start_date=datetime.now() + timedelta(minutes=1),
        max_active_runs=1,
        default_args={
            "depends_on_past": False,
            "email": ["wagnoleao@gmail.com"],
            "retries": 1,
            "retry_delay": timedelta(minutes=5),
            "max_active_tasks": 1,
            "max_active_runs": 1,
        },
        description="DAG that executes the batch processing of the data collect by a user",
        catchup=True,
        tags=["batch", "prototype_v0"],
    ) as dag:

        # @task(task_id="collect_health_data")
        # def load_data():
        #     return open(os.path.join("temp_data", "health_data.csv"), "r")

        @task(task_id="run_model_training")
        def make_post_request(user_id) -> dict:
            print(user_id)
            health_file = open(os.path.join(
                "temp_data", "health_data.csv"), "rb")
            print(health_file)
            predictions = requests.post(
                f"http://{DATALAKE_HOST}:{DATALAKE_PORT}/api/v1/mlflow/{user_id}",
                files={"health_file": health_file}
            ).json()

            print(predictions)
            return predictions

        @task(task_id="save_predictions")
        def save_predictions(user_id: str, predictions: dict) -> None:
            with open(os.path.join("temp_data", f"{user_id}_predictions.json"), "w") as out:
                json.dump(predictions, out)
            return None

        save_predictions(user_id, make_post_request(user_id))
        return dag


users_list = ["user1"]

for user in users_list:
    globals()[f"batch_flow_{user}"] = build_batch_workflow_dag_v0(user)
