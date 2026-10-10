"""
Unit Tests: IoT Telemetry Simulator
Maintained by: Member 1 (IoT & Kafka)
"""

import os
import sys
import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from simulator.iot_simulator import IoTTelemetrySimulator, TelemetryEvent
from simulator.config import COMMODITY_SPECS, DEFAULT_CORRIDORS

def test_simulator_event_generation():
    """Verify simulator generates the requested number of valid containers."""
    sim = IoTTelemetrySimulator(num_containers=4, random_seed=123)
    batch = sim.generate_tick()
    assert len(batch) == 4
    for event in batch:
        assert isinstance(event, TelemetryEvent)
        assert event.container_id.startswith("CONT_")
        assert event.commodity in COMMODITY_SPECS
        assert event.quantity_kg > 0
        assert -30.0 <= event.temperature_c <= 60.0
        assert 0.0 <= event.humidity_pct <= 100.0
        assert 0.0 <= event.vibration_g <= 5.0

def test_showcase_container_anomaly():
    """Verify CONT_001 is assigned the GRADUAL_THERMAL_RISE degradation mode."""
    sim = IoTTelemetrySimulator(num_containers=3, random_seed=42)
    cont1 = sim.containers[0]
    assert cont1.container_id == "CONT_001"
    assert cont1.commodity == "Avocado"
    assert cont1.anomaly_mode == "GRADUAL_THERMAL_RISE"

def test_stream_telemetry_ticks():
    """Verify stream_telemetry yields expected iterations."""
    sim = IoTTelemetrySimulator(num_containers=2, random_seed=99)
    ticks = list(sim.stream_telemetry(interval_sec=0.01, max_iterations=3))
    assert len(ticks) == 3
    for batch in ticks:
        assert len(batch) == 2
