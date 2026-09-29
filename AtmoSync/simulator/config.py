"""
AtmoSync IoT Simulator Configuration
Defines commodity profiles, transit routes, and container fleet attributes.
"""

from typing import Dict, Any, List

# -------------------------------------------------------------------------
# Commodity Profiles
# -------------------------------------------------------------------------
# Each perishable commodity has specific optimal micro-climate conditions.
# Exceeding max_safe_temp or safe humidity thresholds accelerates spoilage.
COMMODITY_PROFILES: Dict[str, Dict[str, Any]] = {
    "avocado": {
        "commodity": "avocado",
        "ideal_temp_c": 6.0,
        "min_safe_temp_c": 4.5,
        "max_safe_temp_c": 8.5,
        "ideal_humidity_pct": 85.0,
        "min_safe_humidity_pct": 80.0,
        "max_safe_humidity_pct": 90.0,
        "base_shelf_life_days": 21,
        "base_price_usd_per_kg": 4.80,
        "spoilage_sensitivity": 1.4,  # High sensitivity to temperature spikes
    },
    "banana": {
        "commodity": "banana",
        "ideal_temp_c": 13.5,
        "min_safe_temp_c": 13.0,
        "max_safe_temp_c": 14.5,
        "ideal_humidity_pct": 90.0,
        "min_safe_humidity_pct": 85.0,
        "max_safe_humidity_pct": 95.0,
        "base_shelf_life_days": 18,
        "base_price_usd_per_kg": 1.95,
        "spoilage_sensitivity": 1.8,  # Extremely sensitive to chilling injury & heat
    },
    "tomato": {
        "commodity": "tomato",
        "ideal_temp_c": 11.0,
        "min_safe_temp_c": 10.0,
        "max_safe_temp_c": 13.0,
        "ideal_humidity_pct": 90.0,
        "min_safe_humidity_pct": 85.0,
        "max_safe_humidity_pct": 92.0,
        "base_shelf_life_days": 14,
        "base_price_usd_per_kg": 2.40,
        "spoilage_sensitivity": 1.2,
    },
    "mango": {
        "commodity": "mango",
        "ideal_temp_c": 12.0,
        "min_safe_temp_c": 10.5,
        "max_safe_temp_c": 13.5,
        "ideal_humidity_pct": 88.0,
        "min_safe_humidity_pct": 85.0,
        "max_safe_humidity_pct": 92.0,
        "base_shelf_life_days": 20,
        "base_price_usd_per_kg": 3.75,
        "spoilage_sensitivity": 1.3,
    },
}

# -------------------------------------------------------------------------
# Shipping Routes & Waypoints
# -------------------------------------------------------------------------
ROUTES: Dict[str, Dict[str, Any]] = {
    "NBO_BOM": {
        "route_id": "ROUTE_01",
        "origin": "Nairobi",
        "destination": "Mumbai",
        "origin_coords": {"lat": -1.2921, "lon": 36.8219},
        "dest_coords": {"lat": 18.9438, "lon": 72.8354},
        "distance_km": 4520,
        "transit_duration_hours": 168,  # ~7 days
    },
    "GYE_RTM": {
        "route_id": "ROUTE_02",
        "origin": "Guayaquil",
        "destination": "Rotterdam",
        "origin_coords": {"lat": -2.1894, "lon": -79.8891},
        "dest_coords": {"lat": 51.9244, "lon": 4.4777},
        "distance_km": 10100,
        "transit_duration_hours": 336,  # ~14 days
    },
    "ALM_FRA": {
        "route_id": "ROUTE_03",
        "origin": "Almeria",
        "destination": "Frankfurt",
        "origin_coords": {"lat": 36.8381, "lon": -2.4597},
        "dest_coords": {"lat": 50.1109, "lon": 8.6821},
        "distance_km": 2150,
        "transit_duration_hours": 48,  # ~2 days overland
    },
    "MNL_TYO": {
        "route_id": "ROUTE_04",
        "origin": "Manila",
        "destination": "Tokyo",
        "origin_coords": {"lat": 14.5995, "lon": 120.9842},
        "dest_coords": {"lat": 35.6762, "lon": 139.6503},
        "distance_km": 3050,
        "transit_duration_hours": 96,  # ~4 days maritime
    },
}

# -------------------------------------------------------------------------
# Active Container Fleet Setup
# -------------------------------------------------------------------------
# Setup multiple containers with designated routes and intentional health profiles
# so we can test both nominal operations and anomalies.
CONTAINER_FLEET: List[Dict[str, Any]] = [
    {
        "container_id": "CONT_001",
        "commodity": "avocado",
        "route_key": "NBO_BOM",
        "behavior": "nominal",  # Healthy, optimal reefer conditions
    },
    {
        "container_id": "CONT_002",
        "commodity": "avocado",
        "route_key": "NBO_BOM",
        "behavior": "cooling_failure",  # Reefer malfunction -> temperature creeps up
    },
    {
        "container_id": "CONT_003",
        "commodity": "banana",
        "route_key": "GYE_RTM",
        "behavior": "humidity_spike",  # Condensation / humidity buildup
    },
    {
        "container_id": "CONT_004",
        "commodity": "tomato",
        "route_key": "ALM_FRA",
        "behavior": "rough_transit",  # High vibration events from rough road/sea
    },
    {
        "container_id": "CONT_005",
        "commodity": "mango",
        "route_key": "MNL_TYO",
        "behavior": "nominal",  # Healthy control container
    },
]
