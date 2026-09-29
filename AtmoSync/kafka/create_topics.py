"""
AtmoSync — Kafka Topic Provisioning
Initializes required topics ('sensor-data' and 'alerts') on the target Kafka cluster.
"""

import os
import sys
import logging
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [Kafka-Admin] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("kafka_admin")

try:
    from kafka.admin import KafkaAdminClient, NewTopic
    from kafka.errors import TopicAlreadyExistsError, NoBrokersAvailable
except ImportError:
    logger.error("kafka-python-ng not found. Install it via 'pip install kafka-python-ng'.")
    sys.exit(1)

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_SENSOR_DATA = os.getenv("KAFKA_TOPIC_SENSOR_DATA", "sensor-data")
TOPIC_ALERTS = os.getenv("KAFKA_TOPIC_ALERTS", "alerts")


def provision_topics(bootstrap_servers: str = BOOTSTRAP_SERVERS) -> bool:
    """Creates required streaming topics with configured partitions and replication."""
    logger.info(f"Connecting to Kafka cluster at {bootstrap_servers}...")

    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers,
            client_id="atmosync-topic-provisioner",
            request_timeout_ms=10000,
        )
    except NoBrokersAvailable:
        logger.error(f"Could not connect to Kafka at {bootstrap_servers}. Ensure Docker is running.")
        return False
    except Exception as e:
        logger.error(f"Failed to connect to Kafka admin client: {e}")
        return False

    existing_topics = set(admin_client.list_topics())
    logger.info(f"Existing topics found: {list(existing_topics)}")

    topics_to_create = []

    if TOPIC_SENSOR_DATA not in existing_topics:
        topics_to_create.append(
            NewTopic(
                name=TOPIC_SENSOR_DATA,
                num_partitions=3,
                replication_factor=1,
            )
        )
        logger.info(f"Scheduled creation: {TOPIC_SENSOR_DATA} (3 partitions, 1 replica)")
    else:
        logger.info(f"Topic '{TOPIC_SENSOR_DATA}' already exists.")

    if TOPIC_ALERTS not in existing_topics:
        topics_to_create.append(
            NewTopic(
                name=TOPIC_ALERTS,
                num_partitions=1,
                replication_factor=1,
            )
        )
        logger.info(f"Scheduled creation: {TOPIC_ALERTS} (1 partition, 1 replica)")
    else:
        logger.info(f"Topic '{TOPIC_ALERTS}' already exists.")

    if topics_to_create:
        try:
            admin_client.create_topics(new_topics=topics_to_create, validate_only=False)
            logger.info("Successfully provisioned new topics.")
        except TopicAlreadyExistsError:
            logger.info("One or more topics were created concurrently.")
        except Exception as e:
            logger.error(f"Failed to create topics: {e}")
            admin_client.close()
            return False

    admin_client.close()
    logger.info("Kafka topic provisioning complete.")
    return True


if __name__ == "__main__":
    servers = sys.argv[1] if len(sys.argv) > 1 else BOOTSTRAP_SERVERS
    success = provision_topics(servers)
    sys.exit(0 if success else 1)
