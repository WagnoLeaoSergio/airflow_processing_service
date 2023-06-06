import os
import json
import requests
import pandas as pd
from dataclasses import dataclass
from datetime import datetime, timedelta

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.utils.edgemodifier import Label

DATALAKE_HOST = "127.0.0.1"
DATALAKE_PORT = "5002"

BATCH_SERVICE_HOST = "127.0.0.1"
BATCH_SERVICE_PORT = "5000"

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
        start_date=datetime.now() + timedelta(minutes=3),
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

        end_flow = EmptyOperator(
            task_id="finish_batch_flow",
            trigger_rule="none_failed_min_one_success"
        )

        branch_options = ["finish_batch_flow", "run_model_training"]

        @task.branch(task_id="check_and_collect_data")
        def collect_user_data(user: str):
            user_info = requests.get(
                f"http://{DATALAKE_HOST}:{DATALAKE_PORT}/api/v1/user/{user_id}",
            ).json()

            print(user_info)
            latest_data = user_info["latest_measure_date"]

            if not latest_data:
                return ["finish_batch_flow"]
            else:
                latest_data = datetime.strptime(
                    latest_data,
                    "%Y-%m-%d %H:%M:%S"
                )

                start_date = latest_data - timedelta(days=11)
                end_date = latest_data - timedelta(days=4)

                measures = requests.get(
                    f"http://{DATALAKE_HOST}:{DATALAKE_PORT}/api/v1/measure/{user_id}",
                ).json()

                measures = measures["measures"]

                if len(measures) < 3:
                    return ["finish_batch_flow"]

                # dict_data = {k: [] for k in measures.keys()}
                #
                # for measure in measures:
                #     dict_data["id"].append(measure["id"])
                #     dict_data["date"].append(measure["date"])
                #     dict_data["steps"].append(measure["steps"])
                #     dict_data["sleep"].append(measure["sleep"])
                #     dict_data["heart"].append(measure["heart"])
                #     dict_data["pressure_high"].append(measure["pressure_high"])
                #     dict_data["pressure_low"].append(measure["pressure_low"])
                #     dict_data["oxygen"].append(measure["oxygen"])
                #
                # df_data = pd.DataFrame(dict_data)

                df_data = pd.DataFrame.from_dict(measures)
                df_data.drop(["user"], axis=1, inplace=True)

                df_data.to_csv(
                    os.path.join("temp_data", "health_data.csv")
                )

                return ["run_model_training"]

        @task(task_id="run_model_training")
        def make_post_request(user_id: str):
            health_file = open(os.path.join(
                "temp_data", "health_data.csv"), "rb")
            predictions = requests.post(
                f"http://{BATCH_SERVICE_HOST}:{BATCH_SERVICE_PORT}/api/v1/mlflow/{user_id}",
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

        @task(task_id="send_results_to_cloud")
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

            print(response)
            return None

        check_data = collect_user_data(user_id)

        predictions = make_post_request(user_id)
        divergences = process_anomalies(predictions)
        send_results(divergences) >> end_flow
        check_data >> end_flow
        check_data >> predictions

        return dag


users_list = [("wagno", "wagnoleao@gmail.com")]

for user in users_list:
    globals()[f"batch_flow_{user[0]}"] = build_batch_workflow_dag_v0(
        User(user[0], user[1]))
