import os
import json
import requests
import pandas as pd
from dataclasses import dataclass
from datetime import datetime, timedelta

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
from airflow.decorators import task

DATALAKE_HOST = "127.0.0.1"
DATALAKE_PORT = "5000"

CLOUD_HOST = "127.0.0.1"
CLOUD_PORT = "8000"


@dataclass
class User:
    id: str
    email: str


def build_batch_workflow_dag_v0(user: User):

    user_id = user.id
    user_email = user.email

    with DAG(
        f"batch_flow_{user_id}",
        schedule=timedelta(minutes=15),
        start_date=datetime.now() + timedelta(minutes=1),
        max_active_runs=1,
        default_args={
            "depends_on_past": False,
            "email": "wagnoleao@gmail.com",
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
        def make_post_request(user_id: str):
            health_file = open(os.path.join(
                "temp_data", "health_data.csv"), "rb")
            predictions = requests.post(
                f"http://{DATALAKE_HOST}:{DATALAKE_PORT}/api/v1/mlflow/{user_id}",
                files={"health_file": health_file}
            ).json()

            health_file.close()
            return predictions

        @task(task_id="process_anomalies")
        def process_anomalies(predictions):

            health_data = pd.read_csv(
                os.path.join(
                    "temp_data",
                    "health_data.csv"
                ),
                index_col=[0]
            )
            health_data = health_data.reset_index(drop=True)
            health_data.set_index("date", inplace=True)
            health_data.sort_index(inplace=True)

            preds_datafile = {"date": [], "heart_rate_pred": []}

            for pred in predictions['predictions']:
                preds_datafile['date'].append(pred[0])
                preds_datafile['heart_rate_pred'].append(pred[1])

            predictions = pd.DataFrame(preds_datafile)

            predictions.set_index("date", inplace=True)
            predictions.sort_index(inplace=True)

            divergences = []

            for index, pred in predictions.iterrows():
                diff = abs(health_data.loc[index, "heart"] - pred.values[0])
                diff = diff / health_data.loc[index, "heart"]
                if diff > 0.25:
                    divergences.append((index, diff))

            return divergences

        @task(task_id="process_anomalies")
        def send_results(divergences):
            data = []
            for div in divergences:
                data.append(
                    {
                        "id": user.id,
                        "email": user_email,
                        "date": div[0],
                        "message": """
                                It was detected that your heart rate
                                is instable at this time of the day.
                                """
                    }
                )

            response = requests.post(
                f"http://{CLOUD_HOST}:{CLOUD_PORT}/notification",
                json={"notifications": data}
            ).json()

            return None

        send_results(process_anomalies(make_post_request(user_id)))
        return dag


users_list = [("user1", "wagnoleao@gmal.com")]

for user in users_list:
    globals()[f"batch_flow_{user[0]}"] = build_batch_workflow_dag_v0(
        User(user[0], user[1]))
