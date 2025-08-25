# scripts/generate_sales.py

import random
import psycopg2
from datetime import date
# Removed: from dotenv import load_dotenv
import os

# Import Airflow's Connection model
from airflow.models.connection import Connection


def main():
    """
    Main function to orchestrate the generation and insertion of sales data.
    """
    print("Starting to generate sales data based on weather...")

    conn = None
    try:
        # Get database connection details from Airflow
        airflow_conn = Connection.get_connection_from_secrets('postgres_default')
        conn = psycopg2.connect(
            host=airflow_conn.host,
            port=airflow_conn.port,
            dbname=airflow_conn.schema,
            user=airflow_conn.login,
            password=airflow_conn.password
        )
        print("✅ Successfully connected to the database!")
        
        cursor = conn.cursor()
        cursor.execute("SELECT temperature_celsius, weather_description FROM weather_data ORDER BY timestamp_utc DESC LIMIT 1")
        result = cursor.fetchone()
        
        temp, condition = result if result else (30, "Clear")
        
        def choose_products(temp, condition):
            condition = condition.lower()
            if "rain" in condition:
                return ["Umbrella", "Raincoat"], (5, 15)
            elif temp >= 30 and "clear" in condition:
                return ["AC", "Fan", "Cold Drink"], (3, 10)
            else:
                return ["Phone", "Laptop", "Tablet"], (1, 5)

        def generate_sales_data(products, qty_range, n=10):
            data = []
            for _ in range(n):
                product = random.choice(products)
                quantity = random.randint(*qty_range)
                price = round(random.uniform(100, 3000), 2)
                sale_date = date.today()
                data.append((product, quantity, price, sale_date))
            return data

        def insert_sales_data(data, conn):
            cursor = conn.cursor()
            cursor.executemany("""
                INSERT INTO sales (product_name, quantity, price, sale_date)
                VALUES (%s, %s, %s, %s)
            """, data)
            conn.commit()
            cursor.close()

        products, qty_range = choose_products(temp, condition)
        sales = generate_sales_data(products, qty_range)
        insert_sales_data(sales, conn)

        print(f"✅ Generated {len(sales)} sales records based on today's weather ({condition}, {temp}°C).")
    
    except Exception as e:
        print(f"An error occurred: {e}")
        raise
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()