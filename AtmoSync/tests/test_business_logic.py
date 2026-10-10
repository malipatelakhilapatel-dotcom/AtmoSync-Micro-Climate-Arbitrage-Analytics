"""
Unit Tests: Spoilage Kinetics & Arbitrage Formulation
Maintained by: Member 2 (Snowflake & dbt)
"""

import os
import sys
import math
import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from simulator.config import COMMODITY_SPECS

def compute_spoilage_risk(
    commodity: str,
    temp_c: float,
    humidity_pct: float,
    vibration_g: float,
    compressor_status: str
) -> float:
    """Python implementation of dbt int_spoilage_risk.sql formula."""
    spec = COMMODITY_SPECS[commodity]

    # Excursion
    temp_excursion = max(0.0, temp_c - spec["optimal_temp_max"], spec["optimal_temp_min"] - temp_c)
    penalty_temp = min(50.0, temp_excursion * 8.5)

    is_humidity_alert = (humidity_pct < spec["min_acceptable_humidity"] or humidity_pct > spec["max_acceptable_humidity"])
    penalty_humidity = 20.0 if is_humidity_alert else 0.0

    penalty_vib = min(20.0, max(0.0, (vibration_g - 0.15) * 60.0))

    penalty_comp = 25.0 if compressor_status == "FAILURE" else 12.0 if compressor_status == "DEGRADED" else 0.0

    raw_score = penalty_temp + penalty_humidity + penalty_vib + penalty_comp
    return round(min(100.0, max(0.0, raw_score)), 1)

def compute_time_to_spoilage(commodity: str, risk_score: float) -> float:
    """Python implementation of dbt Arrhenius shelf-life collapse formula."""
    baseline_hours = COMMODITY_SPECS[commodity]["baseline_shelf_life_hours"]
    decay_factor = max(0.01, 1.0 - (risk_score / 100.0))
    hours = baseline_hours * (decay_factor ** 1.85)
    return round(max(1.0, hours), 1)

def test_nominal_avocado_conditions():
    """Verify nominal conditions produce Low Risk score (< 25)."""
    score = compute_spoilage_risk(
        commodity="Avocado",
        temp_c=5.5,
        humidity_pct=87.0,
        vibration_g=0.08,
        compressor_status="NORMAL"
    )
    assert score < 25.0
    ttl = compute_time_to_spoilage("Avocado", score)
    assert ttl > 100.0  # Safe shelf life remaining

def test_critical_avocado_deterioration():
    """Verify severe temperature excursion + compressor failure produces Critical Risk (> 75)."""
    score = compute_spoilage_risk(
        commodity="Avocado",
        temp_c=14.8,  # > 7.0 optimal max, > 12.0 acceptable max
        humidity_pct=97.0,  # Condensation
        vibration_g=0.28,  # Road shock
        compressor_status="FAILURE"
    )
    assert score > 75.0
    ttl = compute_time_to_spoilage("Avocado", score)
    assert ttl < 15.0  # Shelf life collapses dramatically to single digits

def test_arbitrage_financial_advantage():
    """Verify reroute to closer market yields positive Spoilage Arbitrage."""
    quantity_kg = 12000
    mumbai_price = 180.0
    pune_price = 195.0

    # Under severe deterioration, reaching Mumbai in 3.5h causes 40% spoilage
    mumbai_spoilage_rate = 0.40
    mumbai_freight = 165.0 * 0.015 * quantity_kg
    mumbai_nrv = (quantity_kg * mumbai_price * (1.0 - mumbai_spoilage_rate)) - mumbai_freight

    # Rerouting to Pune takes only 2.0h with only 12% spoilage
    pune_spoilage_rate = 0.12
    pune_freight = 90.0 * 0.015 * quantity_kg
    pune_nrv = (quantity_kg * pune_price * (1.0 - pune_spoilage_rate)) - pune_freight

    arbitrage = pune_nrv - mumbai_nrv
    assert arbitrage > 0.0  # Positive arbitrage opportunity detected
    assert arbitrage > 10000.0  # Substantial financial benefit
