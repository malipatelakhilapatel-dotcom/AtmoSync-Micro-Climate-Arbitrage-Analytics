"""
AtmoSync — Web Dashboard HTTP & REST API Server
Serves the AtmoSync Micro-Climate Arbitrage Console on http://localhost:8000
"""

import json
import logging
import mimetypes
import os
import sys
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from socketserver import ThreadingMixIn
from urllib.parse import urlparse, parse_qs

class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    daemon_threads = True

# Add root directory to sys.path
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from simulator.iot_simulator import IoTFleetSimulator
from simulator.config import COMMODITY_SPECS, MARKETS_CONFIG

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [Web-Server] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("web_server")

# Reference Catalogs
PRICES_CATALOG = {
    "Mumbai": {"Avocado": 180.0, "Banana": 48.0, "Tomato": 32.0, "Mango": 95.0, "Apple": 155.0},
    "Pune": {"Avocado": 185.0, "Banana": 45.0, "Tomato": 28.0, "Mango": 90.0, "Apple": 150.0},
    "Delhi": {"Avocado": 210.0, "Banana": 52.0, "Tomato": 36.0, "Mango": 115.0, "Apple": 160.0},
    "Bengaluru": {"Avocado": 175.0, "Banana": 42.0, "Tomato": 30.0, "Mango": 85.0, "Apple": 165.0},
    "Chennai": {"Avocado": 190.0, "Banana": 46.0, "Tomato": 34.0, "Mango": 92.0, "Apple": 170.0},
    "Hyderabad": {"Avocado": 182.0, "Banana": 44.0, "Tomato": 31.0, "Mango": 88.0, "Apple": 158.0},
}

ROUTES_CATALOG = {
    "Nashik->Mumbai": {"distance_km": 168.0, "transit_hours": 4.5, "toll_inr": 450.0},
    "Nashik->Pune": {"distance_km": 208.0, "transit_hours": 5.0, "toll_inr": 500.0},
    "Nashik->Delhi": {"distance_km": 1250.0, "transit_hours": 24.0, "toll_inr": 3200.0},
    "Hyderabad->Bengaluru": {"distance_km": 570.0, "transit_hours": 10.0, "toll_inr": 1400.0},
    "Bengaluru->Chennai": {"distance_km": 345.0, "transit_hours": 7.0, "toll_inr": 900.0},
}


class StateManager:
    """Maintains simulation state, telemetry history, and reroute overrides."""

    def __init__(self):
        self.lock = threading.Lock()
        self.reset()

    def reset(self):
        with self.lock:
            self.simulator = IoTFleetSimulator(
                num_containers=5,
                abnormal_prob=0.35,
                force_demo_scenario=True,
            )
            self.history = {cid: [] for cid in ["CONT_001", "CONT_002", "CONT_003", "CONT_004", "CONT_005"]}
            self.cycle_count = 0
            self.rerouted_containers = set()
            # Generate initial 3 cycles
            for _ in range(3):
                self._step_internal()

    def step(self):
        with self.lock:
            self._step_internal()
            return self._get_payload_internal()

    def _step_internal(self):
        self.cycle_count += 1
        readings = self.simulator.generate_fleet_readings()
        for r in readings:
            cid = r.container_id
            if cid not in self.history:
                self.history[cid] = []
            self.history[cid].append(r)
            if len(self.history[cid]) > 30:
                self.history[cid].pop(0)

    def reroute(self, container_id: str):
        with self.lock:
            self.rerouted_containers.add(container_id)
            return self._get_payload_internal()

    def get_data(self):
        with self.lock:
            return self._get_payload_internal()

    def _get_payload_internal(self):
        fleet_data = []
        arbitrage_data = []

        for cid in sorted(self.history.keys()):
            events = self.history[cid]
            if not events:
                continue

            latest = events[-1]
            comm = latest.commodity
            specs = COMMODITY_SPECS.get(comm, COMMODITY_SPECS["Tomato"])

            # 1. 3-Period Moving Averages
            recent = events[-3:] if len(events) >= 3 else events
            avg_temp = sum(e.temperature_c for e in recent) / len(recent)
            avg_hum = sum(e.humidity_pct for e in recent) / len(recent)

            # 2. Count anomalous events in trailing 5 readings
            window_5 = events[-5:] if len(events) >= 5 else events
            anom_count = sum(
                1 for e in window_5
                if (e.temperature_c > specs["temp_max"] or e.temperature_c < specs["temp_min"]
                    or e.humidity_pct > specs["humidity_max"] or e.vibration_g > specs["vibration_max"])
            )

            # 3. Spoilage Risk Score Formulation (0 - 100) exactly as in business_logic.md
            temp_penalty = min(45.0, max(0.0, ((avg_temp - specs["temp_opt"]) / (specs["temp_critical"] - specs["temp_opt"])) * 45.0))
            hum_penalty = min(20.0, max(0.0, (abs(avg_hum - 90.0) / 20.0) * 20.0))
            vib_penalty = min(15.0, max(0.0, ((latest.vibration_g - 0.10) / 0.35) * 15.0))
            exp_penalty = min(20.0, anom_count * 4.0)

            spoilage_score = round(min(100.0, max(0.0, temp_penalty + hum_penalty + vib_penalty + exp_penalty)), 1)

            if spoilage_score >= 75.0:
                risk_level = "Critical"
            elif spoilage_score >= 50.0:
                risk_level = "High"
            elif spoilage_score >= 25.0:
                risk_level = "Moderate"
            else:
                risk_level = "Low"

            # 4. Dynamic Time-to-Spoilage (TTS) Hours
            decay = max(0.05, 1.0 - (spoilage_score / 100.0))
            tts_hours = round(max(1.0, specs["base_shelf_life_hours"] * (decay ** 1.6)), 1)

            # Current route
            curr_dest = latest.destination
            curr_price = PRICES_CATALOG.get(curr_dest, {}).get(comm, specs["typical_market_price_inr"])

            route_key = f"{latest.origin}->{curr_dest}"
            curr_route = ROUTES_CATALOG.get(route_key, {"distance_km": 300.0, "transit_hours": 6.0, "toll_inr": 600.0})
            curr_transit_hours = curr_route["transit_hours"]

            # Potential Loss calculation
            if curr_transit_hours > tts_hours:
                curr_loss_pct = 0.80
            else:
                curr_loss_pct = min(0.70, (spoilage_score / 100.0) * 0.40)

            curr_exp_value = round(latest.quantity_kg * curr_price * (1.0 - curr_loss_pct), 2)
            potential_loss = round(latest.quantity_kg * curr_price * curr_loss_pct, 2)

            # Reroute candidates
            candidates = []
            for mkt_name in PRICES_CATALOG.keys():
                if mkt_name in (latest.origin, curr_dest):
                    continue

                cand_price = PRICES_CATALOG.get(mkt_name, {}).get(comm, specs["typical_market_price_inr"])
                cand_dist = 145.0 if mkt_name == "Pune" else 550.0
                cand_transit = 2.8 if mkt_name == "Pune" else 10.5
                cand_toll = 600.0 if mkt_name == "Pune" else 2100.0

                is_viable = cand_transit <= tts_hours

                if not is_viable:
                    cand_loss_pct = 0.85
                elif cand_transit <= (tts_hours * 0.5):
                    cand_loss_pct = 0.10
                else:
                    cand_loss_pct = 0.25

                reroute_cost = round((cand_dist * 35.0) + cand_toll, 2)
                reroute_exp_value = round(latest.quantity_kg * cand_price * (1.0 - cand_loss_pct), 2)
                spoilage_avoided = round(potential_loss - (latest.quantity_kg * cand_price * cand_loss_pct), 2)
                net_arbitrage = round((reroute_exp_value - reroute_cost) - curr_exp_value, 2)

                candidates.append({
                    "candidate_market": mkt_name,
                    "candidate_price": cand_price,
                    "transit_hours": cand_transit,
                    "distance_km": cand_dist,
                    "is_viable": is_viable,
                    "reroute_cost": reroute_cost,
                    "reroute_expected_value": reroute_exp_value,
                    "spoilage_avoided": spoilage_avoided,
                    "net_arbitrage": net_arbitrage,
                })

            candidates.sort(key=lambda c: (c["is_viable"], c["net_arbitrage"]), reverse=True)
            best_candidate = candidates[0] if candidates else None

            is_already_rerouted = cid in self.rerouted_containers

            if is_already_rerouted:
                action = "REROUTED"
            elif best_candidate and best_candidate["net_arbitrage"] > 0 and best_candidate["is_viable"] and risk_level in ("High", "Critical"):
                action = "EXECUTE_REROUTE"
            elif best_candidate and best_candidate["net_arbitrage"] > 0:
                action = "OPPORTUNITY_IDENTIFIED"
            else:
                action = "MAINTAIN_ROUTE"

            target_rec_market = best_candidate["candidate_market"] if best_candidate else "N/A"
            if is_already_rerouted and best_candidate:
                curr_dest = f"{target_rec_market} (Rerouted)"

            container_record = {
                "container_id": cid,
                "shipment_id": latest.shipment_id,
                "commodity": comm,
                "origin": latest.origin,
                "destination": curr_dest,
                "quantity_kg": latest.quantity_kg,
                "temperature_c": latest.temperature_c,
                "humidity_pct": latest.humidity_pct,
                "vibration_g": latest.vibration_g,
                "latitude": latest.latitude,
                "longitude": latest.longitude,
                "rolling_avg_temp": round(avg_temp, 2),
                "rolling_avg_hum": round(avg_hum, 2),
                "temp_penalty": round(temp_penalty, 1),
                "hum_penalty": round(hum_penalty, 1),
                "vib_penalty": round(vib_penalty, 1),
                "exp_penalty": round(exp_penalty, 1),
                "spoilage_risk_score": spoilage_score,
                "risk_level": risk_level,
                "time_to_spoilage_hours": tts_hours,
                "action": action,
                "is_rerouted": is_already_rerouted,
                "min_safe_temp": specs["temp_min"],
                "max_safe_temp": specs["temp_max"],
                "min_safe_hum": specs["humidity_min"],
                "max_safe_hum": specs["humidity_max"],
            }
            fleet_data.append(container_record)

            arbitrage_record = {
                "container_id": cid,
                "commodity": comm,
                "quantity_kg": latest.quantity_kg,
                "current_destination": latest.destination if not is_already_rerouted else f"{target_rec_market}",
                "status_quo_value": curr_exp_value,
                "potential_loss": potential_loss,
                "recommended_market": target_rec_market,
                "recommended_price": best_candidate["candidate_price"] if best_candidate else 0.0,
                "reroute_transit_hours": best_candidate["transit_hours"] if best_candidate else 0.0,
                "reroute_expected_value": best_candidate["reroute_expected_value"] if best_candidate else 0.0,
                "diversion_cost": best_candidate["reroute_cost"] if best_candidate else 0.0,
                "spoilage_loss_avoided": best_candidate["spoilage_avoided"] if best_candidate else 0.0,
                "net_arbitrage_inr": best_candidate["net_arbitrage"] if best_candidate else 0.0,
                "is_viable": best_candidate["is_viable"] if best_candidate else False,
                "directive": action,
            }
            arbitrage_data.append(arbitrage_record)

        # Historical series for charts
        history_series = {}
        for cid, events in self.history.items():
            history_series[cid] = [
                {
                    "step": idx + 1,
                    "temp": e.temperature_c,
                    "hum": e.humidity_pct,
                    "vib": e.vibration_g,
                    "time": e.timestamp.strftime("%H:%M:%S") if hasattr(e.timestamp, "strftime") else str(e.timestamp),
                }
                for idx, e in enumerate(events)
            ]

        # Top-level KPI Aggregations
        total_containers = len(fleet_data)
        at_risk = sum(1 for c in fleet_data if c["risk_level"] in ("High", "Critical"))
        critical_count = sum(1 for c in fleet_data if c["risk_level"] == "Critical")
        avg_temp = round(sum(c["temperature_c"] for c in fleet_data) / max(1, total_containers), 1)
        avg_hum = round(sum(c["humidity_pct"] for c in fleet_data) / max(1, total_containers), 1)
        total_arbitrage = sum(a["net_arbitrage_inr"] for a in arbitrage_data if a["net_arbitrage_inr"] > 0)
        total_loss_avoided = sum(a["spoilage_loss_avoided"] for a in arbitrage_data if a["spoilage_loss_avoided"] > 0)

        markets_payload = {
            name: {"lat": m["latitude"], "lon": m["longitude"], "state": m.get("state", "")}
            for name, m in MARKETS_CONFIG.items()
        }

        return {
            "cycle": self.cycle_count,
            "kpis": {
                "total_containers": total_containers,
                "at_risk_count": at_risk,
                "critical_count": critical_count,
                "average_temp_c": avg_temp,
                "average_humidity_pct": avg_hum,
                "total_potential_arbitrage_inr": round(total_arbitrage, 2),
                "total_loss_avoided_inr": round(total_loss_avoided, 2),
            },
            "fleet": fleet_data,
            "arbitrage": arbitrage_data,
            "history": history_series,
            "markets": markets_payload,
        }


STATE = StateManager()
WEB_DIR = os.path.join(os.path.dirname(__file__), "web")


class AtmoSyncRequestHandler(BaseHTTPRequestHandler):
    """Handles static web assets and REST API endpoints."""

    def log_message(self, format, *args):
        # Concise logging
        logger.debug("%s - %s", self.address_string(), format % args)

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path

        if path == "/api/data":
            self.send_json(STATE.get_data())
            return

        if path == "/api/step":
            self.send_json(STATE.step())
            return

        if path == "/" or path == "/index.html":
            self.serve_file(os.path.join(WEB_DIR, "index.html"), "text/html; charset=utf-8")
            return

        # Serve static file
        local_path = os.path.join(WEB_DIR, path.lstrip("/"))
        if os.path.exists(local_path) and os.path.isfile(local_path):
            ctype, _ = mimetypes.guess_type(local_path)
            self.serve_file(local_path, ctype or "application/octet-stream")
        else:
            self.send_error(404, f"File not found: {path}")

    def do_POST(self):
        parsed = urlparse(self.path)
        path = parsed.path

        if path == "/api/step":
            self.send_json(STATE.step())
            return

        if path == "/api/reset":
            STATE.reset()
            self.send_json(STATE.get_data())
            return

        if path == "/api/reroute":
            length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(length).decode("utf-8") if length > 0 else "{}"
            try:
                data = json.loads(body)
                cid = data.get("container_id", "CONT_001")
            except Exception:
                cid = "CONT_001"
            self.send_json(STATE.reroute(cid))
            return

        self.send_error(404, "Endpoint not found")

    def send_json(self, data: dict, status: int = 200):
        payload = json.dumps(data).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(payload)

    def serve_file(self, filepath: str, content_type: str):
        try:
            with open(filepath, "rb") as f:
                content = f.read()
            self.send_response(200)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(content)))
            self.send_header("Cache-Control", "no-cache")
            self.end_headers()
            self.wfile.write(content)
        except Exception as e:
            self.send_error(500, f"Error reading file: {e}")


def run_server(port: int = 8000):
    os.makedirs(WEB_DIR, exist_ok=True)
    server_address = ("", port)
    httpd = ThreadedHTTPServer(server_address, AtmoSyncRequestHandler)
    logger.info(f"AtmoSync Web Console running at http://localhost:{port}")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        logger.info("Stopping server...")
        httpd.server_close()


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8000
    run_server(port)
