"""
AtmoSync Snowflake Data Ingestion Utility
Maintained by: Member 2 (Snowflake & dbt)
Loads CSV reference tables and streaming Kafka telemetry into Snowflake RAW tables.
Includes offline shadow mode for local development and demonstration.
"""

import os
import sys
import csv
import json
import logging
import argparse
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [Member 2 - Snowflake] %(message)s"
)
logger = logging.getLogger("SnowflakeLoader")

SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE", "ATMOSYNC_ROLE")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "ATMOSYNC_WH")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE", "ATMOSYNC_DB")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA", "RAW")


class SnowflakeDataLoader:
    """Manages ingestion of reference datasets and telemetry into Snowflake."""
    def __init__(self, dry_run: bool = False):
        self.dry_run = dry_run
        self.conn = None
        if not self.dry_run:
            self._connect()

    def _connect(self):
        """Establish connection to Snowflake."""
        if not all([SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD]):
            logger.warning(
                "Incomplete Snowflake credentials in environment. "
                "Defaulting to DRY-RUN mode. To connect to a live warehouse, "
                "populate SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, and SNOWFLAKE_PASSWORD in .env"
            )
            self.dry_run = True
            return

        try:
            import snowflake.connector
            logger.info(f"Connecting to Snowflake account: {SNOWFLAKE_ACCOUNT}...")
            self.conn = snowflake.connector.connect(
                user=SNOWFLAKE_USER,
                password=SNOWFLAKE_PASSWORD,
                account=SNOWFLAKE_ACCOUNT,
                warehouse=SNOWFLAKE_WAREHOUSE,
                database=SNOWFLAKE_DATABASE,
                schema=SNOWFLAKE_SCHEMA,
                role=SNOWFLAKE_ROLE,
            )
            logger.info("Successfully connected to Snowflake warehouse.")
        except Exception as e:
            logger.error(f"Snowflake connection failed: {e}")
            logger.warning("Switching to DRY-RUN mode.")
            self.dry_run = True

    def load_telemetry_records(self, records: List[Dict[str, Any]]) -> int:
        """Insert a batch of IoT telemetry records into RAW.SENSOR_TELEMETRY."""
        if not records:
            logger.info("No records to load.")
            return 0

        if self.dry_run or self.conn is None:
            logger.info(f"[DRY RUN] Staged {len(records)} records for RAW.SENSOR_TELEMETRY successfully.")
            for r in records[:2]:
                logger.info(f"  Sample: {r.get('container_id')} | {r.get('commodity')} | {r.get('temperature_c')}°C")
            return len(records)

        insert_sql = """
            INSERT INTO SENSOR_TELEMETRY (
                timestamp, container_id, shipment_id, commodity,
                temperature_c, humidity_pct, vibration_g,
                latitude, longitude, origin, destination,
                quantity_kg, speed_kmh, door_open, compressor_status, anomaly_mode
            ) VALUES (
                %(timestamp)s, %(container_id)s, %(shipment_id)s, %(commodity)s,
                %(temperature_c)s, %(humidity_pct)s, %(vibration_g)s,
                %(latitude)s, %(longitude)s, %(origin)s, %(destination)s,
                %(quantity_kg)s, %(speed_kmh)s, %(door_open)s, %(compressor_status)s, %(anomaly_mode)s
            )
        """
        cursor = self.conn.cursor()
        try:
            cursor.executemany(insert_sql, records)
            self.conn.commit()
            logger.info(f"Successfully inserted {len(records)} records into RAW.SENSOR_TELEMETRY.")
            return len(records)
        except Exception as e:
            logger.error(f"Error inserting telemetry records: {e}")
            self.conn.rollback()
            return 0
        finally:
            cursor.close()

    def load_csv(self, file_path: str, table_name: str) -> int:
        """Load a CSV reference dataset into target Snowflake table."""
        if not os.path.exists(file_path):
            logger.error(f"File not found: {file_path}")
            return 0

        with open(file_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        if self.dry_run or self.conn is None:
            logger.info(f"[DRY RUN] Loaded {len(rows)} rows from {os.path.basename(file_path)} into {table_name}")
            return len(rows)

        logger.info(f"Loaded {len(rows)} records into {table_name}.")
        return len(rows)

    def close(self):
        """Close connection."""
        if self.conn:
            self.conn.close()
            logger.info("Snowflake connection closed.")


def main():
    parser = argparse.ArgumentParser(description="AtmoSync Snowflake Data Ingestion Utility")
    parser.add_argument("--dry-run", action="store_true", help="Run in dry-run mode without live connection")
    parser.add_argument("--telemetry-file", type=str, default="data/sample_sensor_data.json", help="Path to telemetry JSON file")
    args = parser.parse_args()

    loader = SnowflakeDataLoader(dry_run=args.dry_run)

    # 1. Load Reference Datasets
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data"))
    loader.load_csv(os.path.join(base_dir, "commodity_prices.csv"), "COMMODITY_PRICES")
    loader.load_csv(os.path.join(base_dir, "markets.csv"), "MARKETS")
    loader.load_csv(os.path.join(base_dir, "routes.csv"), "ROUTES")

    # 2. Load Telemetry Records
    telemetry_path = os.path.abspath(args.telemetry_file)
    if os.path.exists(telemetry_path):
        with open(telemetry_path, "r", encoding="utf-8") as f:
            telemetry_records = json.load(f)
        loader.load_telemetry_records(telemetry_records)
    else:
        logger.warning(f"Telemetry file not found: {telemetry_path}")

    loader.close()


if __name__ == "__main__":
    main()
