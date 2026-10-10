"""
Unit Tests: Telemetry Schema Validation & Dead Letter Routing
Maintained by: Member 1 (IoT & Kafka)
"""

import os
import sys
import pytest
from pydantic import ValidationError

# Insert both root and kafka directory to avoid namespace collision with third-party kafka library
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
KAFKA_DIR = os.path.join(ROOT_DIR, "kafka")
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)
if KAFKA_DIR not in sys.path:
    sys.path.insert(0, KAFKA_DIR)

from simulator.iot_simulator import TelemetryEvent
from consumer import AtmoSyncKafkaConsumer

def test_valid_telemetry_event():
    """Verify valid payload passes schema validation without exception."""
    valid_payload = {
        "timestamp": "2026-09-21T12:00:00Z",
        "container_id": "CONT_001",
        "shipment_id": "SHIP_001",
        "commodity": "Avocado",
        "temperature_c": 6.2,
        "humidity_pct": 87.5,
        "vibration_g": 0.09,
        "latitude": 19.9975,
        "longitude": 73.7898,
        "origin": "Nashik",
        "destination": "Mumbai",
        "quantity_kg": 12000,
        "speed_kmh": 60.0,
        "door_open": False,
        "compressor_status": "NORMAL",
        "anomaly_mode": "NORMAL"
    }
    event = TelemetryEvent(**valid_payload)
    assert event.container_id == "CONT_001"
    assert event.temperature_c == 6.2

def test_invalid_temperature_out_of_bounds():
    """Verify temperatures outside physical bounds raise ValidationError."""
    invalid_payload = {
        "timestamp": "2026-09-21T12:00:00Z",
        "container_id": "CONT_ERR",
        "shipment_id": "SHIP_ERR",
        "commodity": "Banana",
        "temperature_c": 125.0,  # Unphysical temp > 60°C
        "humidity_pct": 80.0,
        "vibration_g": 0.1,
        "latitude": 19.0,
        "longitude": 73.0,
        "origin": "Pune",
        "destination": "Mumbai",
        "quantity_kg": 10000,
    }
    with pytest.raises(ValidationError):
        TelemetryEvent(**invalid_payload)

def test_consumer_dlq_routing():
    """Verify malformed payload is routed to Dead Letter Queue."""
    consumer = AtmoSyncKafkaConsumer(mock_mode=True)
    corrupted_payload = {
        "container_id": "CONT_BROKEN",
        "temperature_c": "not_a_number"
    }
    is_valid = consumer.validate_and_stage(corrupted_payload)
    assert is_valid is False
    assert len(consumer.dead_letter_records) >= 1
    assert consumer.dead_letter_records[-1]["raw_payload"]["container_id"] == "CONT_BROKEN"
