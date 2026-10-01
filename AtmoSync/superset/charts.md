# AtmoSync — Superset Chart Definitions & SQL Queries

**Module Owner:** Member 3 — Superset & Documentation

---

## 1. Top KPI Metrics SQL Queries

### KPI 1: Total Active Containers
```sql
SELECT COUNT(DISTINCT container_id) AS total_active_containers
FROM ANALYTICS.FCT_SPOILAGE_ARBITRAGE;
```

### KPI 2: Containers At Risk
```sql
SELECT COUNT(DISTINCT container_id) AS containers_at_risk
FROM ANALYTICS.FCT_SPOILAGE_ARBITRAGE
WHERE risk_category IN ('High Risk', 'Critical Risk');
```

### KPI 3: Critical Containers
```sql
SELECT COUNT(DISTINCT container_id) AS critical_containers
FROM ANALYTICS.FCT_SPOILAGE_ARBITRAGE
WHERE risk_category = 'Critical Risk';
```

### KPI 4: Average Container Temperature
```sql
SELECT ROUND(AVG(temperature_c), 1) AS avg_temperature_c
FROM ANALYTICS.FCT_CONTAINER_HEALTH;
```

### KPI 5: Average Container Humidity
```sql
SELECT ROUND(AVG(humidity_pct), 1) AS avg_humidity_pct
FROM ANALYTICS.FCT_CONTAINER_HEALTH;
```

### KPI 6: Total Potential Spoilage Arbitrage
```sql
SELECT ROUND(SUM(spoilage_arbitrage_inr), 0) AS total_arbitrage_opportunity_inr
FROM ANALYTICS.FCT_SPOILAGE_ARBITRAGE
WHERE is_reroute_recommended = TRUE;
```

### KPI 7: Total Cargo Value Preserved / Loss Avoided
```sql
SELECT ROUND(SUM(spoilage_avoided_value_inr), 0) AS total_loss_avoided_inr
FROM ANALYTICS.FCT_SPOILAGE_ARBITRAGE
WHERE is_reroute_recommended = TRUE;
```

---

## 2. Visualization Charts SQL Queries

### Chart 1: Container Health Geographic Scatter Map
- **Chart Type**: Deck.gl Scatterplot
```sql
SELECT
    container_id,
    commodity,
    latitude,
    longitude,
    risk_category,
    spoilage_risk_score,
    time_to_spoilage_hours,
    current_destination,
    recommended_destination,
    spoilage_arbitrage_inr
FROM ANALYTICS.FCT_SPOILAGE_ARBITRAGE;
```

### Chart 2: Micro-Climate Temperature Trends
- **Chart Type**: Time-series Line Chart
```sql
SELECT
    telemetry_timestamp,
    container_id,
    commodity,
    temperature_c
FROM ANALYTICS.FCT_CONTAINER_HEALTH
ORDER BY telemetry_timestamp ASC;
```

### Chart 3: Micro-Climate Relative Humidity Trends
- **Chart Type**: Time-series Line Chart
```sql
SELECT
    telemetry_timestamp,
    container_id,
    commodity,
    humidity_pct
FROM ANALYTICS.FCT_CONTAINER_HEALTH
ORDER BY telemetry_timestamp ASC;
```

### Chart 4: Spoilage Risk Leaderboard
- **Chart Type**: Horizontal Bar Chart
```sql
SELECT
    container_id || ' (' || commodity || ')' AS container_label,
    spoilage_risk_score,
    risk_category,
    time_to_spoilage_hours
FROM ANALYTICS.FCT_SPOILAGE_ARBITRAGE
ORDER BY spoilage_risk_score DESC;
```

### Chart 5: Real-Time Spoilage Arbitrage Opportunity Matrix
- **Chart Type**: Table with Conditional Formatting
```sql
SELECT
    container_id AS "Container",
    commodity AS "Cargo",
    risk_category AS "Risk State",
    time_to_spoilage_hours AS "Shelf Life (Hrs)",
    current_destination AS "Current Dest",
    current_expected_value_inr AS "Current NRV (₹)",
    recommended_destination AS "Recommended Dest",
    reroute_expected_value_inr AS "Reroute NRV (₹)",
    spoilage_arbitrage_inr AS "Arbitrage Benefit (₹)",
    recommendation_reason AS "Trader Recommendation"
FROM ANALYTICS.FCT_SPOILAGE_ARBITRAGE
ORDER BY spoilage_arbitrage_inr DESC;
```

### Chart 6: Multi-Market Revenue Realization Comparison
- **Chart Type**: Grouped Bar Chart
```sql
SELECT
    container_id,
    current_expected_value_inr AS "Current Planned Destination",
    reroute_expected_value_inr AS "Alternative Rerouted Destination"
FROM ANALYTICS.FCT_SPOILAGE_ARBITRAGE;
```

### Chart 7: Logistics Corridor Route Performance & Transit Vulnerability
- **Chart Type**: Scatter / Bubble Plot
```sql
SELECT
    current_destination AS destination,
    distance_km,
    estimated_transit_hours,
    ROUND(projected_spoilage_rate * 100, 1) AS projected_spoilage_pct,
    quantity_kg
FROM INTERMEDIATE.INT_MARKET_OPTIONS;
```