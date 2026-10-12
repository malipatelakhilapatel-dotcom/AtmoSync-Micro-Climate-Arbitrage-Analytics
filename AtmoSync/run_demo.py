"""
AtmoSync — Integrated End-to-End Decision Intelligence Demo Runner
Simulates real-time IoT fleet telematics, models biological degradation kinetics,
evaluates reachable terminal markets, and outputs live Spoilage Arbitrage recommendations.
Can be run 100% locally with zero external cloud dependencies.
"""

import json
import os
import sys
import time
from typing import List, Dict, Any
from tabulate import tabulate

# Ensure UTF-8 output encoding across Windows / Linux / macOS terminals
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# Add project root to sys.path
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from simulator.iot_simulator import IoTFleetSimulator, SensorTelemetryEvent
from simulator.config import COMMODITY_SPECS, MARKETS_CONFIG, DEFAULT_ROUTES


def evaluate_container_analytics(
    telemetry_history: List[SensorTelemetryEvent],
    prices_catalog: Dict[str, Dict[str, float]],
    markets_catalog: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:
    """Applies the analytical transformations corresponding to dbt intermediate and marts models."""
    latest = telemetry_history[-1]
    cid = latest.container_id
    comm = latest.commodity
    specs = COMMODITY_SPECS[comm]

    # 1. 3-Period Moving Averages
    recent = telemetry_history[-3:] if len(telemetry_history) >= 3 else telemetry_history
    avg_temp = sum(e.temperature_c for e in recent) / len(recent)
    avg_hum = sum(e.humidity_pct for e in recent) / len(recent)

    # 2. Count anomalous events in trailing 5 readings
    window_5 = telemetry_history[-5:] if len(telemetry_history) >= 5 else telemetry_history
    anom_count = sum(
        1 for e in window_5
        if (e.temperature_c > specs["temp_max"] or e.temperature_c < specs["temp_min"]
            or e.humidity_pct > specs["humidity_max"] or e.vibration_g > specs["vibration_max"])
    )

    # 3. Spoilage Risk Score Formulation (0 - 100)
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

    # 5. Route & Candidate Market Evaluation
    curr_dest = latest.destination
    curr_price = prices_catalog.get(curr_dest, {}).get(comm, specs["typical_market_price_inr"])

    # Look up scheduled route transit time
    curr_transit_hours = 8.5 if cid == "CONT_001" else 4.5
    for r in DEFAULT_ROUTES:
        if r["origin"] == latest.origin and r["destination"] == curr_dest:
            curr_transit_hours = r["transit_hours"] if cid != "CONT_001" else 8.5
            break

    # If transit hours > time to spoilage, dock rejection write-off is 80%
    if curr_transit_hours > tts_hours:
        curr_loss_pct = 0.80
    else:
        curr_loss_pct = min(0.70, (spoilage_score / 100.0) * 0.40)

    curr_exp_value = round(latest.quantity_kg * curr_price * (1.0 - curr_loss_pct), 2)
    potential_loss = round(latest.quantity_kg * curr_price * curr_loss_pct, 2)

    # Evaluate Candidate Reroute Markets
    candidates = []
    for mkt_name, mkt_meta in markets_catalog.items():
        if mkt_name in (latest.origin, curr_dest):
            continue

        cand_price = prices_catalog.get(mkt_name, {}).get(comm, specs["typical_market_price_inr"])

        # Determine distance & transit time
        cand_dist = 145.0 if mkt_name == "Pune" else 550.0
        cand_transit = 2.8 if mkt_name == "Pune" else 10.5
        cand_toll = 600.0 if mkt_name == "Pune" else 2100.0

        is_viable = cand_transit <= tts_hours

        # Spoilage loss at candidate arrival
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

    # Sort candidates by net arbitrage descending with viability priority
    candidates.sort(key=lambda c: (c["is_viable"], c["net_arbitrage"]), reverse=True)
    best_candidate = candidates[0] if candidates else None

    action = "MAINTAIN_ORIGINAL_ROUTE"
    if best_candidate and best_candidate["net_arbitrage"] > 0 and best_candidate["is_viable"] and risk_level in ("High", "Critical"):
        action = "EXECUTE_REROUTE"
    elif best_candidate and best_candidate["net_arbitrage"] > 0:
        action = "OPPORTUNITY_IDENTIFIED"

    return {
        "container_id": cid,
        "shipment_id": latest.shipment_id,
        "commodity": comm,
        "quantity_kg": latest.quantity_kg,
        "temperature_c": latest.temperature_c,
        "humidity_pct": latest.humidity_pct,
        "vibration_g": latest.vibration_g,
        "latitude": latest.latitude,
        "longitude": latest.longitude,
        "rolling_avg_temp": round(avg_temp, 2),
        "spoilage_risk_score": spoilage_score,
        "risk_level": risk_level,
        "time_to_spoilage_hours": tts_hours,
        "current_destination": curr_dest,
        "current_transit_hours": curr_transit_hours,
        "current_price": curr_price,
        "current_expected_value": curr_exp_value,
        "potential_loss": potential_loss,
        "recommended_destination": best_candidate["candidate_market"] if best_candidate else "N/A",
        "recommended_price": best_candidate["candidate_price"] if best_candidate else 0.0,
        "recommended_transit_hours": best_candidate["transit_hours"] if best_candidate else 0.0,
        "reroute_cost": best_candidate["reroute_cost"] if best_candidate else 0.0,
        "reroute_expected_value": best_candidate["reroute_expected_value"] if best_candidate else 0.0,
        "spoilage_avoided": best_candidate["spoilage_avoided"] if best_candidate else 0.0,
        "spoilage_arbitrage_inr": best_candidate["net_arbitrage"] if best_candidate else 0.0,
        "action": action,
    }


def run_demo():
    print("\n" + "=" * 95)
    print("           ATMOSYNC: MICRO-CLIMATE ARBITRAGE ANALYTICS -- LIVE DEMO RUNNER")
    print("=" * 95)
    print("Initializing simulated fleet across Indian logistics freight corridors...\n")

    # Wholesale terminal spot prices
    prices_catalog = {
        "Mumbai": {"Avocado": 195.0, "Banana": 45.0, "Tomato": 38.0, "Mango": 125.0, "Apple": 155.0},
        "Pune": {"Avocado": 185.0, "Banana": 42.0, "Tomato": 34.0, "Mango": 115.0, "Apple": 150.0},
        "Hyderabad": {"Avocado": 175.0, "Banana": 40.0, "Tomato": 32.0, "Mango": 110.0, "Apple": 148.0},
        "Bengaluru": {"Avocado": 190.0, "Banana": 44.0, "Tomato": 36.0, "Mango": 120.0, "Apple": 152.0},
        "Chennai": {"Avocado": 180.0, "Banana": 43.0, "Tomato": 35.0, "Mango": 118.0, "Apple": 150.0},
        "Delhi": {"Avocado": 215.0, "Banana": 52.0, "Tomato": 44.0, "Mango": 145.0, "Apple": 138.0},
    }

    fleet_sim = IoTFleetSimulator(num_containers=5, force_demo_scenario=True)
    history: Dict[str, List[SensorTelemetryEvent]] = {c.container_id: [] for c in fleet_sim.containers}

    print("[STREAMING INGESTION] Simulating real-time sensor cycles via Apache Kafka...")
    num_cycles = 10

    # Accelerate CONT_001 thermal drift for clear demo demonstration
    cont1_state = next(c for c in fleet_sim.containers if c.container_id == "CONT_001")
    cont1_state.drift_rate_temp = 0.95
    cont1_state.drift_rate_humidity = 0.50

    for cycle in range(1, num_cycles + 1):
        readings = fleet_sim.generate_fleet_readings()
        for event in readings:
            history[event.container_id].append(event)
            if event.container_id == "CONT_001":
                status = "OPTIMAL" if event.temperature_c <= 8.0 else ("WARNING" if event.temperature_c <= 12.0 else "CRITICAL RISK")
                print(
                    f"  [Cycle {cycle:02d}/{num_cycles}] CONT_001 (Avocado) -> Temp: {event.temperature_c:5.1f}°C | "
                    f"Hum: {event.humidity_pct:5.1f}% | Lat/Lon: ({event.latitude}, {event.longitude}) [{status}]"
                )
        time.sleep(0.15)

    print("\n[DBT TRANSFORMATION] Computing biological spoilage kinetics & multi-market arbitrage...\n")

    analytics_results = []
    for cid, events in history.items():
        res = evaluate_container_analytics(events, prices_catalog, MARKETS_CONFIG)
        analytics_results.append(res)

    # 1. Container Health Summary Table
    health_table_data = [
        [
            r["container_id"],
            r["commodity"],
            f"{r['temperature_c']}°C",
            f"{r['humidity_pct']}%",
            f"{r['vibration_g']}g",
            f"{r['spoilage_risk_score']}/100",
            r["risk_level"],
            f"{r['time_to_spoilage_hours']} hrs",
            r["action"],
        ]
        for r in analytics_results
    ]

    print("-" * 95)
    print("FLEET TELEMETRY & SPOILAGE RISK HEALTH STATUS (fct_container_health)")
    print("-" * 95)
    print(
        tabulate(
            health_table_data,
            headers=["Container", "Commodity", "Temp", "Humidity", "Vib", "Risk Score", "Risk Tier", "TTS Remaining", "Action"],
            tablefmt="pretty",
        )
    )

    # 2. Spoilage Arbitrage Recommendation Table
    arbitrage_table_data = [
        [
            r["container_id"],
            r["commodity"],
            r["current_destination"],
            f"INR {r['current_expected_value']:,.0f}",
            f"INR {r['potential_loss']:,.0f}",
            r["recommended_destination"],
            f"INR {r['reroute_expected_value']:,.0f}",
            f"INR {r['reroute_cost']:,.0f}",
            f"INR {r['spoilage_arbitrage_inr']:+,.0f}",
            r["action"],
        ]
        for r in analytics_results
    ]

    print("\n" + "-" * 95)
    print("SPOILAGE ARBITRAGE DECISION MATRIX (fct_spoilage_arbitrage)")
    print("-" * 95)
    print(
        tabulate(
            arbitrage_table_data,
            headers=[
                "Container", "Commodity", "Orig Dest", "Status Quo Exp",
                "Potential Loss", "Rec Market", "Reroute Exp", "Diversion Cost",
                "Net Arbitrage", "Directive"
            ],
            tablefmt="pretty",
        )
    )

    # Detailed Highlight on Flagship Case
    flagship = next(r for r in analytics_results if r["container_id"] == "CONT_001")
    print("\n" + "=" * 95)
    print(f"               FLAGSHIP ARBITRAGE CASE: CONTAINER {flagship['container_id']} (AVOCADOS)")
    print("=" * 95)
    print(f"* Incident: Compressor thermal breach detected ({flagship['temperature_c']}°C).")
    print(f"* Spoilage Risk Score: {flagship['spoilage_risk_score']}/100 [{flagship['risk_level'].upper()} RISK]")
    print(f"* Projected Shelf Life (TTS): {flagship['time_to_spoilage_hours']} Hours remaining.")
    print(f"* Scheduled Route (Mumbai): Requires {flagship['current_transit_hours']} Hours -> Produce will arrive degraded.")
    print(f"  - Status Quo Produce Loss: INR {flagship['potential_loss']:,.2f}")
    print(f"  - Net Expected Outcome at Mumbai: INR {flagship['current_expected_value']:,.2f}")
    print(f"* Reroute Opportunity ({flagship['recommended_destination']}): Reachable in {flagship['recommended_transit_hours']} Hours.")
    print(f"  - Wholesale Spot Price: INR {flagship['recommended_price']}/kg")
    print(f"  - Gross Produce Value at Pune: INR {flagship['reroute_expected_value']:,.2f}")
    print(f"  - Freight Diversion Surcharge: -INR {flagship['reroute_cost']:,.2f}")
    print(f"  - NET ARBITRAGE BENEFIT: INR {flagship['spoilage_arbitrage_inr']:+,.2f} SAVINGS")
    print(f">>> OPERATIONAL DIRECTIVE: {flagship['action']} <<<")
    print("=" * 95 + "\n")

    # Persist demo artifact
    os.makedirs("data", exist_ok=True)
    with open("data/demo_arbitrage_results.json", "w", encoding="utf-8") as f:
        json.dump(analytics_results, f, indent=2)
    print("Demo evaluation results persisted to data/demo_arbitrage_results.json\n")


if __name__ == "__main__":
    run_demo()
