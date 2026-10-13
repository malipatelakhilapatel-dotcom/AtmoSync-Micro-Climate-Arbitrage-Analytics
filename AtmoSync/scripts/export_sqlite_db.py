"""
AtmoSync — SQLite Local Database Generator
Converts sample JSON/CSV data into an SQLite database (data/atmosync_sample.db)
for instant zero-setup connection in Apache Superset.
"""

import json
import os
import sqlite3
import pandas as pd

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")
DB_PATH = os.path.join(DATA_DIR, "atmosync_sample.db")

def create_sample_sqlite():
    print(f"Creating local SQLite database at: {DB_PATH}")
    conn = sqlite3.connect(DB_PATH)

    # 1. Staging Sensor Telemetry
    sensor_json_path = os.path.join(DATA_DIR, "sample_sensor_data.json")
    if os.path.exists(sensor_json_path):
        with open(sensor_json_path, "r", encoding="utf-8") as f:
            sensor_data = json.load(f)
        df_sensor = pd.DataFrame(sensor_data)
        df_sensor.rename(columns={"timestamp": "recorded_at"}, inplace=True)
        df_sensor.to_sql("stg_sensor_telemetry", conn, if_exists="replace", index=False)
        print(f"Loaded {len(df_sensor)} rows into 'stg_sensor_telemetry'")

    # 2. Fact Container Health & Spoilage Arbitrage
    arbitrage_json_path = os.path.join(DATA_DIR, "demo_arbitrage_results.json")
    if os.path.exists(arbitrage_json_path):
        with open(arbitrage_json_path, "r", encoding="utf-8") as f:
            arbitrage_data = json.load(f)
        df_arbitrage = pd.DataFrame(arbitrage_data)
        
        # Container health view/table
        df_health = df_arbitrage[[
            "container_id", "shipment_id", "commodity", "latitude", "longitude",
            "temperature_c", "humidity_pct", "vibration_g", "spoilage_risk_score",
            "risk_level", "time_to_spoilage_hours"
        ]].copy()
        df_health.to_sql("fct_container_health", conn, if_exists="replace", index=False)
        print(f"Loaded {len(df_health)} rows into 'fct_container_health'")

        # Arbitrage table
        df_arbitrage.rename(columns={
            "current_price": "current_dest_price_per_kg",
            "recommended_price": "recommended_market_price",
            "action": "recommendation_action"
        }, inplace=True)
        df_arbitrage.to_sql("fct_spoilage_arbitrage", conn, if_exists="replace", index=False)
        print(f"Loaded {len(df_arbitrage)} rows into 'fct_spoilage_arbitrage'")

    # 3. Reference CSVs
    for csv_name, table_name in [
        ("commodity_prices.csv", "dim_commodity_prices"),
        ("markets.csv", "dim_markets"),
        ("routes.csv", "dim_routes")
    ]:
        csv_path = os.path.join(DATA_DIR, csv_name)
        if os.path.exists(csv_path):
            df_csv = pd.read_csv(csv_path)
            df_csv.to_sql(table_name, conn, if_exists="replace", index=False)
            print(f"Loaded {len(df_csv)} rows into '{table_name}'")

    conn.close()
    print("SQLite database successfully generated!")

if __name__ == "__main__":
    create_sample_sqlite()
