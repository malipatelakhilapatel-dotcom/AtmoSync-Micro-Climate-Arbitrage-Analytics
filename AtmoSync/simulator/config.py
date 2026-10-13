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

# -------------------------------------------------------------------------
# Spoilage Kinetics & Market Wholesale Configuration (INR Metric)
# -------------------------------------------------------------------------
COMMODITY_SPECS: Dict[str, Dict[str, Any]] = {
    "Avocado": {
        "temp_opt": 6.0,
        "temp_min": 4.0,
        "temp_max": 8.0,
        "temp_critical": 12.0,
        "optimal_temp_min": 4.5,
        "optimal_temp_max": 7.5,
        "humidity_min": 80.0,
        "humidity_max": 95.0,
        "min_acceptable_humidity": 80.0,
        "max_acceptable_humidity": 95.0,
        "vibration_max": 0.25,
        "base_shelf_life_hours": 120.0,
        "baseline_shelf_life_hours": 120.0,
        "typical_market_price_inr": 185.0,
    },
    "Banana": {
        "temp_opt": 14.0,
        "temp_min": 13.0,
        "temp_max": 15.0,
        "temp_critical": 18.0,
        "optimal_temp_min": 13.0,
        "optimal_temp_max": 15.0,
        "humidity_min": 85.0,
        "humidity_max": 95.0,
        "min_acceptable_humidity": 85.0,
        "max_acceptable_humidity": 95.0,
        "vibration_max": 0.20,
        "base_shelf_life_hours": 96.0,
        "baseline_shelf_life_hours": 96.0,
        "typical_market_price_inr": 48.0,
    },
    "Tomato": {
        "temp_opt": 12.0,
        "temp_min": 10.0,
        "temp_max": 14.0,
        "temp_critical": 18.0,
        "optimal_temp_min": 10.0,
        "optimal_temp_max": 14.0,
        "humidity_min": 85.0,
        "humidity_max": 95.0,
        "min_acceptable_humidity": 85.0,
        "max_acceptable_humidity": 95.0,
        "vibration_max": 0.25,
        "base_shelf_life_hours": 144.0,
        "baseline_shelf_life_hours": 144.0,
        "typical_market_price_inr": 32.0,
    },
    "Mango": {
        "temp_opt": 12.5,
        "temp_min": 10.0,
        "temp_max": 14.0,
        "temp_critical": 17.0,
        "optimal_temp_min": 10.0,
        "optimal_temp_max": 14.0,
        "humidity_min": 85.0,
        "humidity_max": 95.0,
        "min_acceptable_humidity": 85.0,
        "max_acceptable_humidity": 95.0,
        "vibration_max": 0.20,
        "base_shelf_life_hours": 168.0,
        "baseline_shelf_life_hours": 168.0,
        "typical_market_price_inr": 95.0,
    },
    "Apple": {
        "temp_opt": 2.0,
        "temp_min": 0.0,
        "temp_max": 4.0,
        "temp_critical": 8.0,
        "optimal_temp_min": 0.0,
        "optimal_temp_max": 4.0,
        "humidity_min": 88.0,
        "humidity_max": 96.0,
        "min_acceptable_humidity": 88.0,
        "max_acceptable_humidity": 96.0,
        "vibration_max": 0.30,
        "base_shelf_life_hours": 720.0,
        "baseline_shelf_life_hours": 720.0,
        "typical_market_price_inr": 155.0,
    },
}

MARKETS_CONFIG: Dict[str, Dict[str, Any]] = {
    "Nashik": {"lat": 19.9975, "lon": 73.7898, "latitude": 19.9975, "longitude": 73.7898, "state": "Maharashtra"},
    "Mumbai": {"lat": 19.0760, "lon": 72.8777, "latitude": 19.0760, "longitude": 72.8777, "state": "Maharashtra"},
    "Pune": {"lat": 18.5204, "lon": 73.8567, "latitude": 18.5204, "longitude": 73.8567, "state": "Maharashtra"},
    "Hyderabad": {"lat": 17.3850, "lon": 78.4867, "latitude": 17.3850, "longitude": 78.4867, "state": "Telangana"},
    "Bengaluru": {"lat": 12.9716, "lon": 77.5946, "latitude": 12.9716, "longitude": 77.5946, "state": "Karnataka"},
    "Chennai": {"lat": 13.0827, "lon": 80.2707, "latitude": 13.0827, "longitude": 80.2707, "state": "Tamil Nadu"},
    "Delhi": {"lat": 28.7041, "lon": 77.1025, "latitude": 28.7041, "longitude": 77.1025, "state": "Delhi NCR"},
}

DEFAULT_ROUTES: List[Dict[str, Any]] = [
    {"origin": "Nashik", "destination": "Mumbai", "distance_km": 168.0, "transit_hours": 4.5, "toll_inr": 450.0},
    {"origin": "Nashik", "destination": "Pune", "distance_km": 208.0, "transit_hours": 5.0, "toll_inr": 500.0},
    {"origin": "Nashik", "destination": "Delhi", "distance_km": 1250.0, "transit_hours": 24.0, "toll_inr": 3200.0},
    {"origin": "Hyderabad", "destination": "Bengaluru", "distance_km": 570.0, "transit_hours": 10.0, "toll_inr": 1400.0},
    {"origin": "Bengaluru", "destination": "Chennai", "distance_km": 345.0, "transit_hours": 7.0, "toll_inr": 900.0},
]

DEFAULT_CORRIDORS = DEFAULT_ROUTES

