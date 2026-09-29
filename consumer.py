"""
AtmoSync — Kafka Telemetry Consumer & Snowflake Staging Loader
Subscribes to 'sensor-data', validates incoming payloads, flags critical anomalies,
routes alerts to the 'alerts' topic, and batches records for Snowflake loading.
"""

import argparse
import datetime
import json
import logging
import os
import sys
import time
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv

load_dotenv()

# Add parent directory to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from simulator.iot_simulator import SensorTelemetryEvent
from simulator.config import COMMODITY_SPECS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [Kafka-Consumer] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("kafka_consumer")

try:
    from kafka import KafkaConsumer, KafkaProducer
    from kafka.errors import NoBrokersAvailable
except ImportError:
    logger.error("kafka-python-ng not found. Install it via 'pip install kafka-python-ng'.")
    sys.exit(1)

# Optional Snowflake Connector
try:
    import snowflake.connector
    HAS_SNOWFLAKE = True
except ImportError:
    HAS_SNOWFLAKE = False

DEFAULT_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_SENSOR_DATA = os.getenv("KAFKA_TOPIC_SENSOR_DATA", "sensor-data")
TOPIC_ALERTS = os.getenv("KAFKA_TOPIC_ALERTS", "alerts")
CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "atmosync-snowflake-loader")


class SnowflakeLoader:
    """Manages batch loading of sanitized telemetry into Snowflake RAW.SENSOR_TELEMETRY."""

    def __init__(self):
        self.account = os.getenv("SNOWFLAKE_ACCOUNT")
        self.user = os.getenv("SNOWFLAKE_USER")
        self.password = os.getenv("SNOWFLAKE_PASSWORD")
        self.database = os.getenv("SNOWFLAKE_DATABASE", "ATMOSYNC_DB")
        self.schema = os.getenv("SNOWFLAKE_SCHEMA", "RAW")
        self.warehouse = os.getenv("SNOWFLAKE_WAREHOUSE", "ATMOSYNC_WH")
        self.role = os.getenv("SNOWFLAKE_ROLE", "ATMOSYNC_ROLE")
        self.conn = None

    def connect(self) -> bool:
        if not HAS_SNOWFLAKE:
            logger.warning("snowflake-connector-python is not installed. Snowflake direct load disabled.")
            return False
        if not (self.account and self.user and self.password):
            logger.info("Snowflake credentials not configured in .env. Falling back to local staging file.")
            return False
        try:
            self.conn = snowflake.connector.connect(
                user=self.user,
                password=self.password,
                account=self.account,
                warehouse=self.warehouse,
                database=self.database,
                schema=self.schema,
                role=self.role,
            )
            logger.info("Connected to Snowflake warehouse successfully.")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to Snowflake: {e}")
            return False

    def insert_batch(self, batch: List[Dict[str, Any]]) -> bool:
        """Inserts a batch of validated telemetry records into Snowflake."""
        if not self.conn:
            return False

        query = f"""
        INSERT INTO {self.database}.{self.schema}.SENSOR_TELEMETRY (
            timestamp, container_id, shipment_id, commodity,
            temperature_c, humidity_pct, vibration_g,
            latitude, longitude, origin, destination, quantity_kg
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        rows = [
            (
                r["timestamp"],
                r["container_id"],
                r["shipment_id"],
                r["commodity"],
                r["temperature_c"],
                r["humidity_pct"],
                r["vibration_g"],
                r["latitude"],
                r["longitude"],
                r["origin"],
                r["destination"],
                r["quantity_kg"],
            )
            for r in batch
        ]

        try:
            cursor = self.conn.cursor()
            cursor.executemany(query, rows)
            self.conn.commit()
            cursor.close()
            logger.info(f"Loaded {len(rows)} records into Snowflake table {self.database}.{self.schema}.SENSOR_TELEMETRY.")
            return True
        except Exception as e:
            logger.error(f"Snowflake insert batch failed: {e}")
            return False

    def close(self):
        if self.conn:
            self.conn.close()


class StreamingTelemetryConsumer:
    """Consumes telemetry, validates against schema, fires alerts, and stages for warehouse."""

    def __init__(
        self,
        bootstrap_servers: str = DEFAULT_BOOTSTRAP,
        input_topic: str = TOPIC_SENSOR_DATA,
        alerts_topic: str = TOPIC_ALERTS,
        group_id: str = CONSUMER_GROUP,
        batch_size: int = 10,
        staging_file: str = "data/staged_telemetry.json",
    ):
        self.bootstrap_servers = bootstrap_servers
        self.input_topic = input_topic
        self.alerts_topic = alerts_topic
        self.group_id = group_id
        self.batch_size = batch_size
        self.staging_file = staging_file
        self.batch: List[Dict[str, Any]] = []

        self.consumer: Optional[KafkaConsumer] = None
        self.alert_producer: Optional[KafkaProducer] = None
        self.snowflake_loader = SnowflakeLoader()
        self.has_snowflake_conn = self.snowflake_loader.connect()

    def connect(self) -> bool:
        """Initializes Kafka consumer and optional alert producer."""
        try:
            self.consumer = KafkaConsumer(
                self.input_topic,
                bootstrap_servers=self.bootstrap_servers,
                group_id=self.group_id,
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                consumer_timeout_ms=10000,
            )
            # Producer for emitting critical alerts
            self.alert_producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            logger.info(f"Subscribed to topic '{self.input_topic}' (group: {self.group_id}).")
            return True
        except NoBrokersAvailable:
            logger.error(f"Cannot connect to Kafka broker at {self.bootstrap_servers}.")
            return False
        except Exception as e:
            logger.error(f"Kafka consumer initialization error: {e}")
            return False

    def check_critical_anomalies(self, record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Evaluates whether current reading breaches critical thresholds for its commodity."""
        commodity = record.get("commodity")
        specs = COMMODITY_SPECS.get(commodity)
        if not specs:
            return None

        temp = record.get("temperature_c", 0.0)
        hum = record.get("humidity_pct", 0.0)
        vib = record.get("vibration_g", 0.0)

        critical_reasons = []
        if temp >= specs["temp_critical"]:
            critical_reasons.append(f"Temperature {temp}°C exceeds critical threshold ({specs['temp_critical']}°C)")
        if hum > specs["humidity_max"]:
            critical_reasons.append(f"Humidity {hum}% exceeds safe ceiling ({specs['humidity_max']}%)")
        if vib > specs["vibration_max"]:
            critical_reasons.append(f"Vibration {vib}g exceeds mechanical limit ({specs['vibration_max']}g)")

        if critical_reasons:
            alert = {
                "alert_id": f"ALT_{record['container_id']}_{int(time.time())}",
                "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                "container_id": record["container_id"],
                "shipment_id": record["shipment_id"],
                "commodity": commodity,
                "reasons": critical_reasons,
                "current_readings": {"temperature_c": temp, "humidity_pct": hum, "vibration_g": vib},
                "location": {"latitude": record["latitude"], "longitude": record["longitude"]},
            }
            return alert
        return None

    def emit_alert(self, alert: Dict[str, Any]) -> None:
        """Publishes critical anomaly event to alerts topic and logs."""
        logger.warning(
            f"[CRITICAL ALERT] Container {alert['container_id']} [{alert['commodity']}]: "
            f"{' | '.join(alert['reasons'])}"
        )
        if self.alert_producer:
            try:
                self.alert_producer.send(self.alerts_topic, value=alert)
            except Exception as e:
                logger.error(f"Failed to publish alert to topic {self.alerts_topic}: {e}")

    def flush_batch(self) -> None:
        """Flushes accumulated batch to Snowflake or local staging file."""
        if not self.batch:
            return

        flushed = False
        if self.has_snowflake_conn:
            flushed = self.snowflake_loader.insert_batch(self.batch)

        if not flushed:
            # Fallback staging to disk
            os.makedirs(os.path.dirname(os.path.abspath(self.staging_file)), exist_ok=True)
            existing_data = []
            if os.path.exists(self.staging_file):
                try:
                    with open(self.staging_file, "r", encoding="utf-8") as f:
                        existing_data = json.load(f)
                except Exception:
                    existing_data = []

            existing_data.extend(self.batch)
            with open(self.staging_file, "w", encoding="utf-8") as f:
                json.dump(existing_data, f, indent=2)
            logger.info(f"Staged {len(self.batch)} validated events to local file: {self.staging_file}")

        self.batch.clear()

    def process_message(self, raw_value: Any) -> None:
        """Validates payload schema and queues for staging."""
        try:
            # Pydantic validation
            event = SensorTelemetryEvent(**raw_value)
            clean_record = event.model_dump()

            # Check anomaly trigger
            alert = self.check_critical_anomalies(clean_record)
            if alert:
                self.emit_alert(alert)

            self.batch.append(clean_record)
            if len(self.batch) >= self.batch_size:
                self.flush_batch()

        except Exception as e:
            logger.warning(f"Malformed message dropped: {e} | Raw payload: {raw_value}")

    def start_consuming(self, max_messages: int = 0) -> None:
        """Runs the continuous consumer loop."""
        if not self.connect():
            logger.warning("Kafka broker unreachable. Exiting consumer.")
            return

        logger.info("Entering continuous message consumption loop...")
        count = 0
        try:
            for message in self.consumer:
                count += 1
                self.process_message(message.value)

                if max_messages > 0 and count >= max_messages:
                    logger.info(f"Target count of {max_messages} messages reached.")
                    break
        except KeyboardInterrupt:
            logger.info("Consumer loop interrupted by user.")
        finally:
            self.flush_batch()
            if self.consumer:
                self.consumer.close()
            if self.alert_producer:
                self.alert_producer.close()
            self.snowflake_loader.close()
            logger.info("Consumer shutdown complete.")


def run_consumer_cli():
    parser = argparse.ArgumentParser(description="AtmoSync Kafka Consumer & Snowflake Staging Loader")
    parser.add_argument("--bootstrap-servers", type=str, default=DEFAULT_BOOTSTRAP)
    parser.add_argument("--topic", type=str, default=TOPIC_SENSOR_DATA)
    parser.add_argument("--batch-size", type=int, default=10)
    parser.add_argument("--max-messages", type=int, default=0, help="Stop after N messages (0 for continuous)")
    args = parser.parse_args()

    consumer = StreamingTelemetryConsumer(
        bootstrap_servers=args.bootstrap_servers,
        input_topic=args.topic,
        batch_size=args.batch_size,
    )
    consumer.start_consuming(max_messages=args.max_messages)


if __name__ == "__main__":
    run_consumer_cli()
