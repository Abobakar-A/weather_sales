# scripts/create_tables.py

import psycopg2
from airflow.models.connection import Connection

def main():
    print("Attempting to create tables in the 'airflow' database...")
    conn = None
    try:
        # Get connection details from Airflow
        airflow_conn = Connection.get_connection_from_secrets('postgres_default')
        conn = psycopg2.connect(
            host=airflow_conn.host,
            port=airflow_conn.port,
            dbname=airflow_conn.schema,
            user=airflow_conn.login,
            password=airflow_conn.password
        )
        cursor = conn.cursor()

        # Create weather_data table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS weather_data (
                id SERIAL PRIMARY KEY,
                city VARCHAR(100) NOT NULL,
                temperature_celsius FLOAT,
                humidity_percent FLOAT,
                wind_speed_kph FLOAT,
                weather_description VARCHAR(255),
                timestamp_utc TIMESTAMP
            );
        """)
        print("✅ 'weather_data' table created successfully.")

        # Create sales table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS sales (
                id SERIAL PRIMARY KEY,
                product_name VARCHAR(255) NOT NULL,
                quantity INTEGER,
                price FLOAT,
                sale_date DATE
            );
        """)
        print("✅ 'sales' table created successfully.")
        conn.commit()

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()