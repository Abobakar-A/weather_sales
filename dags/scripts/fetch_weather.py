# scripts/fetch_weather.py

import requests
import psycopg2
from datetime import datetime
import os
from airflow.models.connection import Connection
from airflow.models import Variable

def main():
    print("Starting to fetch weather data using Airflow Connections and Variables...")

    try:
        api_key = Variable.get("OPENWEATHER_API_KEY")
    except KeyError:
        print("Error: 'OPENWEATHER_API_KEY' variable not found in Airflow.")
        raise

    conn = None
    try:
        airflow_conn = Connection.get_connection_from_secrets('postgres_default')
        conn = psycopg2.connect(
            host=airflow_conn.host,
            port=airflow_conn.port,
            dbname=airflow_conn.schema,
            user=airflow_conn.login,
            password=airflow_conn.password
        )
        print("✅ Successfully connected to the database!")
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        raise

    CITIES = ["Dubai", "Abu Dhabi", "Sharjah", "Al Ain"]

    cursor = None
    try:
        cursor = conn.cursor()

        for city in CITIES:
            url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric"
            try:
                response = requests.get(url)
                response.raise_for_status()
                data = response.json()

                if data.get("cod") != 200:
                    print(f"Skipping {city}: API Error: {data.get('message', 'Unknown error')}")
                    continue

                temperature = data["main"]["temp"]
                humidity = data["main"]["humidity"]
                wind_speed = data["wind"]["speed"] * 3.6
                description = data["weather"][0]["description"]
                timestamp = datetime.utcnow()

                cursor.execute("""
                    INSERT INTO weather_data (city, temperature_celsius, humidity_percent, wind_speed_kph, weather_description, timestamp_utc)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (city, temperature, humidity, wind_speed, description, timestamp))
                conn.commit()
                print(f"✅ Data for {city} inserted successfully!")
            except requests.exceptions.RequestException as e:
                print(f"Error fetching data for {city} from API: {e}")
                continue
            except KeyError:
                print(f"Error for {city}: API response format is unexpected. Check city name or API key.")
                continue

    except psycopg2.Error as e:
        print(f"Database error: {e}")
        raise
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
    
    print("✅ Successfully fetched weather data.")