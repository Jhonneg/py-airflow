from datetime import datetime
import random
import os
import json

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


@dag(
    "average_page_visits",
    start_date=datetime(2024, 1, 1),
    schedule="* * * * *",
    catchup=False,
)
def average_page_visits():
    def get_data_path():
        context = get_current_context()
        execution_date = context["execution_date"]
        file_date = execution_date.strftime("%Y-%m-%d_%H%M")
        return f"/tmp/page_visits/{file_date}.json"

    @task
    def produce_page_visits_data():
        page_visits = [
            {
                "id": 1,
                "name": "cozy apartment",
                "price": 120,
                "page_visits": random.randint(0, 50),
            },
            {
                "id": 2,
                "name": "luxury condo",
                "price": 300,
                "page_visits": random.randint(0, 50),
            },
            {
                "id": 3,
                "name": "modern studio",
                "price": 180,
                "page_visits": random.randint(0, 50),
            },
            {
                "id": 4,
                "name": "charming loft",
                "price": 150,
                "page_visits": random.randint(0, 50),
            },
            {
                "id": 5,
                "name": "spacious villa ",
                "price": 410,
                "page_visits": random.randint(0, 50),
            },
        ]
        file_path = get_data_path()

        directory = os.path.dirname(file_path)
        if not os.path.exists(directory):
            os.makedirs(directory)

        with open(file_path, "w") as f:
            json.dump(page_visits, f)

        print(f"Written to file: {file_path}")

    @task
    def process_page_visits_data():
        file_path = get_data_path()

        with open(file_path, "r") as f:
            page_visits = json.load(f)

        average_price = sum(
            page_visit["page_visits"] for page_visit in page_visits
        ) / len(page_visits)
        print(f"Average number of page visits {average_price}")

    produce_page_visits_data() >> process_page_visits_data()


demo_dag = average_page_visits()
