from datetime import datetime
from datetime import timezone
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.models.variable import Variable
from airflow.providers.http.operators.http import SimpleHttpOperator
import json

cities_coordinates = {
    "Lviv": (49.84, 4.03),  # lat, lon
    "Kyiv": (50.45, 30.52),
    "Harkiv": (49.99, 36.23),
    "Odesa": (46.48, 30.72),
    "Zhmerynka": (49.04, 28.11),
}

with DAG(
    dag_id="weather_dag",
    schedule_interval="@daily",
    start_date=datetime(2023, 11, 15),
    catchup=True,
) as dag:
    db_create = SqliteOperator(
        task_id="create_table_sqlite",
        sqlite_conn_id="airflow_conn",
        sql="""
            CREATE TABLE IF NOT EXISTS measures
            (
            city VARCHAR(255),
            timestamp TIMESTAMP,
            temp FLOAT,
            hum FLOAT,
            clouds FLOAT,
            wind_speed FLOAT
            );
        """,
    )

    check_api = HttpSensor(
        task_id="check_api",
        http_conn_id="weather_conn",
        endpoint="data/3.0/onecall",
        request_params={
            "appid": Variable.get("WEATHER_API_KEY"),
            "lat": "49.84",
            "lon": "4.03",
        },
    )

    for city, coordinates in cities_coordinates.items():
        lat, lon = coordinates

        def _get_extract_params(ti, ds):
            print(ds)
            timestamp = int(
                datetime.strptime(ds, "%Y-%m-%d")
                .replace(tzinfo=timezone.utc)
                .timestamp()
            )
            return str(timestamp)

        get_extract_params = PythonOperator(
            task_id=f"get_extract_params_{city}", python_callable=_get_extract_params
        )

        extract_data = SimpleHttpOperator(
            task_id=f"extract_data_{city}",
            http_conn_id="weather_conn",
            endpoint="data/3.0/onecall/timemachine",
            data={
                "appid": Variable.get("WEATHER_API_KEY"),
                "lat": str(lat),
                "lon": str(lon),
                "dt": f"""{{{{ti.xcom_pull(task_ids='get_extract_params_{city}')}}}}""",
                "exclude": "minutely,hourly,daily,alerts",
            },
            method="GET",
            response_filter=lambda x: json.loads(x.text),
            log_response=True,
        )

        def _process_weather(ti, ds):
            print(ds)
            info = ti.xcom_pull(task_ids=f"extract_data_{city}")
            current = info["data"][0]
            timestamp = current["dt"]
            temp = current["temp"]
            hum = current["humidity"]
            clouds = float(current["clouds"])
            wind_speed = current["wind_speed"]
            return timestamp, temp, hum, clouds, wind_speed

        process_data = PythonOperator(
            task_id=f"process_data_{city}", python_callable=_process_weather
        )

        inject_data = SqliteOperator(
            task_id=f"inject_data_{city}",
            sqlite_conn_id="airflow_conn",
            sql=f"""
            INSERT INTO measures (city, timestamp, temp, hum, clouds, wind_speed) VALUES
            ('{city}',
            {{{{ti.xcom_pull(task_ids='process_data_{city}')[0]}}}},
            {{{{ti.xcom_pull(task_ids='process_data_{city}')[1]}}}},
            {{{{ti.xcom_pull(task_ids='process_data_{city}')[2]}}}},
            {{{{ti.xcom_pull(task_ids='process_data_{city}')[3]}}}},
            {{{{ti.xcom_pull(task_ids='process_data_{city}')[4]}}}});
            """,
        )

        (
            db_create
            >> check_api
            >> get_extract_params
            >> extract_data
            >> process_data
            >> inject_data
        )
