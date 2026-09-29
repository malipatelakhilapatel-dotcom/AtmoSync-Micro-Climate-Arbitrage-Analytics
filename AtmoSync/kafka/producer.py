"""
AtmoSync — Resilient Kafka Telemetry Producer
Streams validated container sensor readings to the Apache Kafka broker.
Includes exponential backoff reconnection logic, JSON serialization, and metrics tracking.
"""

import argparse
import json
import logging
import os
import sys
import time
from typing import Optional
from dotenv import load_dotenv

load_dotenv()

# Add parent directory to sys.path for simulator module import
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from simulator.iot_simulator import IoTFleetSimulator, SensorTelemetryEvent

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [Kafka-Producer] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("kafka_producer")

try:
    from kafka import KafkaProducer
    from kafka.errors import KafkaError, NoBrokersAvailable
except ImportError:
    logger.error("kafka-python-ng not found. Install it via 'pip install kafka-python-ng'.")
    sys.exit(1)

DEFAULT_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
DEFAULT_TOPIC = os.getenv("KAFKA_TOPIC_SENSOR_DATA", "sensor-data")


class ResilientTelemetryProducer:
    """Kafka producer wrapper with connection resiliency and automatic retry."""

    def __init__(
        self,
        bootstrap_servers: str = DEFAULT_BOOTSTRAP,
        topic: str = DEFAULT_TOPIC,
        max_retries: int = 5,
        dry_run: bool = False,
    ):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.max_retries = max_retries
        self.dry_run = dry_run
        self.producer: Optional[KafkaProducer] = None
        self.events_sent = 0

        if not self.dry_run:
            self._connect()

    def _connect(self) -> None:
        """Establishes connection to Kafka broker with exponential backoff."""
        delay = 1.0
        for attempt in range(1, self.max_retries + 1):
            try:
                logger.info(
                    f"Attempting connection to Kafka at {self.bootstrap_servers} (Attempt {attempt}/{self.max_retries})..."
                )
                self.producer = KafkaProducer(
                    bootstrap_servers=self.bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                    key_serializer=lambda k: k.encode("utf-8") if k else None,
                    acks="all",
                    retries=3,
                    request_timeout_ms=10000,
                    client_id="atmosync-telemetry-producer",
                )
                logger.info("Connected to Kafka broker successfully.")
                return
            except NoBrokersAvailable:
                logger.warning(
                    f"Kafka broker not available at {self.bootstrap_servers}. Retrying in {delay:.1f}s..."
                )
                time.sleep(delay)
                delay = min(delay * 2, 16.0)
            except Exception as e:
                logger.error(f"Unexpected connection error: {e}. Retrying in {delay:.1f}s...")
                time.sleep(delay)
                delay = min(delay * 2, 16.0)

        logger.warning(
            f"Failed to connect to Kafka after {self.max_retries} attempts. "
            "Running in DRY-RUN mode (logging events without network transmission)."
        )
        self.dry_run = True

    def publish_event(self, event: SensorTelemetryEvent) -> bool:
        """Publishes a validated sensor telemetry event to Kafka."""
        payload = event.model_dump()
        key = event.container_id

        if self.dry_run:
            self.events_sent += 1
            logger.info(
                f"[DRY-RUN] -> Topic '{self.topic}' | Key: {key} | "
                f"Temp: {event.temperature_c}°C | Hum: {event.humidity_pct}% | Vib: {event.vibration_g}g"
            )
            return True

        try:
            future = self.producer.send(self.topic, key=key, value=payload)
            record_metadata = future.get(timeout=10)
            self.events_sent += 1
            logger.info(
                f"SENT -> {self.topic} [P:{record_metadata.partition} O:{record_metadata.offset}] | "
                f"Key: {key} | Temp: {event.temperature_c}°C | Hum: {event.humidity_pct}%"
            )
            return True
        except KafkaError as ke:
            logger.error(f"Kafka publishing failed for container {key}: {ke}")
            return False
        except Exception as ex:
            logger.error(f"Unexpected error during publishing: {ex}")
            return False

    def close(self) -> None:
        """Flushes buffered records and closes producer."""
        if self.producer:
            logger.info(f"Flushing producer buffer. Total events sent: {self.events_sent}")
            self.producer.flush()
            self.producer.close()
            logger.info("Kafka producer connection closed.")


def run_producer_cli():
    parser = argparse.ArgumentParser(description="AtmoSync Resilient Kafka Producer")
    parser.add_argument("--bootstrap-servers", type=str, default=DEFAULT_BOOTSTRAP, help="Kafka bootstrap server(s)")
    parser.add_argument("--topic", type=str, default=DEFAULT_TOPIC, help="Kafka topic for sensor readings")
    parser.add_argument("--containers", type=int, default=5, help="Number of containers to simulate")
    parser.add_argument("--interval", type=float, default=2.0, help="Publishing interval in seconds")
    parser.add_argument("--iterations", type=int, default=0, help="Cycles to run (0 for continuous streaming)")
    parser.add_argument("--dry-run", action="store_true", help="Force dry-run mode without connecting to Kafka")

    args = parser.parse_args()

    producer = ResilientTelemetryProducer(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        dry_run=args.dry_run,
    )

    fleet = IoTFleetSimulator(
        num_containers=args.containers,
        abnormal_prob=0.35,
        force_demo_scenario=True,
    )

    cycles = 0
    logger.info(f"Starting producer loop across {args.containers} containers...")
    try:
        while True:
            cycles += 1
            events = fleet.generate_fleet_readings()
            for event in events:
                producer.publish_event(event)

            if args.iterations > 0 and cycles >= args.iterations:
                logger.info(f"Completed {args.iterations} cycles successfully.")
                break

            time.sleep(args.interval)
    except KeyboardInterrupt:
        logger.info("Interrupted by user. Shutting down producer...")
    finally:
        producer.close()


if __name__ == "__main__":
    run_producer_cli()
