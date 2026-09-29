"""
AtmoSync IoT Sensor Simulator
Generates realistic streaming telemetry for refrigerated shipping containers (reefers)
transporting perishable commodities across global trade routes.
"""

import argparse
import datetime
import json
import math
import random
import sys
import time
from typing import Dict, Any, Generator, Optional, List

try:
    from simulator.config import COMMODITY_PROFILES, ROUTES, CONTAINER_FLEET
except ModuleNotFoundError:
    # Allow execution directly from within the simulator/ directory
    from config import COMMODITY_PROFILES, ROUTES, CONTAINER_FLEET


class ContainerState:
    """Tracks the continuous internal physical state and trajectory of a shipping container."""

    def __init__(self, fleet_config: Dict[str, Any]):
        self.container_id = fleet_config["container_id"]
        self.commodity_name = fleet_config["commodity"]
        self.route_key = fleet_config["route_key"]
        self.behavior = fleet_config["behavior"]

        self.commodity_meta = COMMODITY_PROFILES[self.commodity_name]
        self.route_meta = ROUTES[self.route_key]

        # Initial conditions based on commodity baseline
        self.current_temp = self.commodity_meta["ideal_temp_c"]
        self.current_humidity = self.commodity_meta["ideal_humidity_pct"]
        self.current_vibration = 0.10  # Nominal baseline vibration in g

        # Route progression (0.0 = Origin, 1.0 = Destination)
        self.progress = random.uniform(0.05, 0.25)
        self.step_count = 0

    def calculate_current_coordinates(self) -> Dict[str, float]:
        """Interpolates geographic latitude & longitude along the route."""
        o_lat = self.route_meta["origin_coords"]["lat"]
        o_lon = self.route_meta["origin_coords"]["lon"]
        d_lat = self.route_meta["dest_coords"]["lat"]
        d_lon = self.route_meta["dest_coords"]["lon"]

        # Linear interpolation with slight nautical drift
        lat = o_lat + (d_lat - o_lat) * self.progress + random.gauss(0, 0.005)
        lon = o_lon + (d_lon - o_lon) * self.progress + random.gauss(0, 0.005)

        return {"latitude": round(lat, 4), "longitude": round(lon, 4)}

    def next_telemetry(self, timestamp: Optional[datetime.datetime] = None) -> Dict[str, Any]:
        """
        Advances the internal physical state by one tick and produces a telemetry reading.
        Applies physical laws, sensor noise, and programmed micro-climate anomalies.
        """
        if timestamp is None:
            timestamp = datetime.datetime.now(datetime.timezone.utc)

        self.step_count += 1
        # Advance route slightly
        self.progress = min(0.98, self.progress + 0.005)

        # Baseline sensor noise (Gaussian)
        temp_noise = random.gauss(0.0, 0.15)
        humidity_noise = random.gauss(0.0, 0.3)
        vibration_noise = random.gauss(0.0, 0.02)

        # Apply specific container behaviors
        if self.behavior == "nominal":
            # Normal healthy operation: stays tightly around ideal conditions
            self.current_temp = self.commodity_meta["ideal_temp_c"] + temp_noise
            self.current_humidity = self.commodity_meta["ideal_humidity_pct"] + humidity_noise
            self.current_vibration = max(0.02, 0.10 + vibration_noise)

        elif self.behavior == "cooling_failure":
            # Malfunctioning refrigeration unit: temperature gradually creeps upward!
            drift = 0.25 * (self.step_count ** 0.6)
            self.current_temp = self.commodity_meta["ideal_temp_c"] + drift + temp_noise
            # Warmer air can increase condensation/humidity
            self.current_humidity = min(98.0, self.commodity_meta["ideal_humidity_pct"] + (drift * 0.5) + humidity_noise)
            self.current_vibration = max(0.02, 0.12 + vibration_noise)

        elif self.behavior == "humidity_spike":
            # Dehumidifier failure or moisture ingress
            spike = 6.0 * (1 - math.exp(-self.step_count / 5.0))
            self.current_temp = self.commodity_meta["ideal_temp_c"] + temp_noise
            self.current_humidity = min(99.0, self.commodity_meta["ideal_humidity_pct"] + spike + humidity_noise)
            self.current_vibration = max(0.02, 0.09 + vibration_noise)

        elif self.behavior == "rough_transit":
            # High sea states or degraded road conditions causing heavy vibrations
            self.current_temp = self.commodity_meta["ideal_temp_c"] + temp_noise
            self.current_humidity = self.commodity_meta["ideal_humidity_pct"] + humidity_noise
            # Frequent vibration shocks between 0.8g and 1.6g
            is_shock = random.random() < 0.40
            self.current_vibration = round(random.uniform(0.75, 1.45), 2) if is_shock else max(0.05, 0.18 + vibration_noise)

        coords = self.calculate_current_coordinates()

        # Format timestamp to ISO 8601 UTC with 'Z' suffix
        ts_str = timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")

        payload = {
            "timestamp": ts_str,
            "container_id": self.container_id,
            "commodity": self.commodity_name,
            "origin": self.route_meta["origin"],
            "destination": self.route_meta["destination"],
            "latitude": coords["latitude"],
            "longitude": coords["longitude"],
            "temperature_c": round(self.current_temp, 2),
            "humidity_pct": round(min(100.0, max(0.0, self.current_humidity)), 1),
            "vibration_g": round(max(0.0, self.current_vibration), 2),
        }

        return payload


class AtmoSyncSimulator:
    """Coordinates simulation for the entire container fleet."""

    def __init__(self, fleet_configs: Optional[List[Dict[str, Any]]] = None):
        configs = fleet_configs if fleet_configs is not None else CONTAINER_FLEET
        self.containers: List[ContainerState] = [ContainerState(cfg) for cfg in configs]

    def emit_tick(self, timestamp: Optional[datetime.datetime] = None) -> List[Dict[str, Any]]:
        """Produces one telemetry event for each active container in the fleet."""
        current_time = timestamp or datetime.datetime.now(datetime.timezone.utc)
        return [c.next_telemetry(current_time) for c in self.containers]

    def stream(self, interval_sec: float = 2.0, max_ticks: Optional[int] = None) -> Generator[Dict[str, Any], None, None]:
        """Continuously yields individual container telemetry events."""
        ticks_emitted = 0
        while True:
            batch = self.emit_tick()
            for event in batch:
                yield event
            ticks_emitted += 1
            if max_ticks and ticks_emitted >= max_ticks:
                break
            time.sleep(interval_sec)


def main():
    parser = argparse.ArgumentParser(description="AtmoSync IoT Micro-Climate Telemetry Simulator")
    parser.add_argument("--ticks", type=int, default=5, help="Number of simulation cycles to execute (default: 5). Use 0 for infinite streaming.")
    parser.add_argument("--interval", type=float, default=2.0, help="Seconds to wait between cycles (default: 2.0)")
    parser.add_argument("--output", type=str, default=None, help="Optional path to output NDJSON file to save telemetry")
    parser.add_argument("--pretty", action="store_true", help="Print formatted multi-line JSON instead of compact NDJSON")
    args = parser.parse_args()

    simulator = AtmoSyncSimulator()
    max_ticks = None if args.ticks <= 0 else args.ticks

    out_file = open(args.output, "w", encoding="utf-8") if args.output else None

    print(f"=== AtmoSync IoT Simulator Initialized ===", file=sys.stderr)
    print(f"Active Containers: {len(simulator.containers)}", file=sys.stderr)
    print(f"Tick Interval: {args.interval}s | Cycles: {'Infinite' if max_ticks is None else max_ticks}", file=sys.stderr)
    print(f"=========================================\n", file=sys.stderr)

    try:
        for event in simulator.stream(interval_sec=args.interval, max_ticks=max_ticks):
            if args.pretty:
                formatted_json = json.dumps(event, indent=2)
            else:
                formatted_json = json.dumps(event)

            print(formatted_json)

            if out_file:
                out_file.write(json.dumps(event) + "\n")
                out_file.flush()

    except KeyboardInterrupt:
        print("\nSimulator stopped by user.", file=sys.stderr)
    finally:
        if out_file:
            out_file.close()


if __name__ == "__main__":
    main()
