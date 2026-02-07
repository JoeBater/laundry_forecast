from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests, csv, os

def fetch_actual_weather():
    lat, lon = 52.2053, 0.1218

    url = (
        f"https://api.open-meteo.com/v1/forecast"
        f"?latitude={lat}&longitude={lon}"
        f"&past_days=1"
        f"&daily=temperature_2m_max,temperature_2m_min"
        f"&timezone=Europe%2FLondon"
    )

    data = requests.get(url).json()

    date = data["daily"]["time"][-1]
    tmax = data["daily"]["temperature_2m_max"][-1]
    tmin = data["daily"]["temperature_2m_min"][-1]

    with open("actual_weather.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["date", "temp_max", "temp_min"])
        writer.writerow([date, tmax, tmin])


def fetch_forecast_for_laundry():
    lat, lon = 52.2053, 0.1218

    url = (
        "https://api.open-meteo.com/v1/forecast"
        f"?latitude={lat}&longitude={lon}"
        "&daily=temperature_2m_max,temperature_2m_min,"
        "precipitation_probability_max,wind_speed_10m_max,"
        "shortwave_radiation_sum,relative_humidity_2m_min"
        "&forecast_days=7"
        "&timezone=Europe%2FLondon"
    )

    data = requests.get(url).json()
    daily = data["daily"]

    # FIXED: write to forecast_weather.csv
    with open("forecast_weather.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "date",
            "temp_max",
            "temp_min",
            "precip_prob",
            "wind_max",
            "sunshine",
            "humidity_min"
        ])

        for i in range(len(daily["time"])):
            writer.writerow([
                daily["time"][i],
                daily["temperature_2m_max"][i],
                daily["temperature_2m_min"][i],
                daily["precipitation_probability_max"][i],
                daily["wind_speed_10m_max"][i],
                daily["shortwave_radiation_sum"][i],
                daily["relative_humidity_2m_min"][i]
            ])


def compare_weather():
    with open('actual_weather.csv') as f1, open('forecast_weather.csv') as f2:
        actual = list(csv.DictReader(f1))[0]
        forecast = list(csv.DictReader(f2))[0]

    summary = f"""Weather Comparison for {actual['date']}:
    Forecast Max: {forecast['temp_max']}°C | Actual Max: {actual['temp_max']}°C
    Forecast Min: {forecast['temp_min']}°C | Actual Min: {actual['temp_min']}°C
    """

    with open('weather_summary.txt', 'w') as f:
        f.write(summary)


def delete_forecast():
    os.remove('forecast_weather.csv')


default_args = {
    'owner': 'joe',
    'start_date': datetime(2025, 10, 4),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='weather_truth_vs_forecast',
    default_args=default_args,
    schedule='0 2 * * *',   # <-- updated
    catchup=False,
    tags=['weather', 'comparison'],
) as dag:

    t1 = PythonOperator(task_id='fetch_actual_weather', python_callable=fetch_actual_weather)

    # FIXED: call the real forecast function
    t2 = PythonOperator(task_id='fetch_forecast', python_callable=fetch_forecast_for_laundry)

    t3 = PythonOperator(task_id='compare_weather', python_callable=compare_weather)
    t4 = PythonOperator(task_id='delete_forecast', python_callable=delete_forecast)

    t1 >> t2 >> t3 >> t4
