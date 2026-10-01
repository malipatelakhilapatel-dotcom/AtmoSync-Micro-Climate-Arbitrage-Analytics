# AtmoSync — Apache Superset Dashboard Architecture & Design

**Module Owner:** Member 3 — Superset & Documentation

---

## Executive Overview

The **AtmoSync Micro-Climate Arbitrage Console** provides commodity trading desks, cold-chain operations managers, and logistics dispatchers with real-time situational awareness. It translates raw temperature, humidity, and vibration oscillations into actionable financial rerouting interventions before irreversible cargo spoilage occurs.

---

## 1. Top KPI Ribbon (Metrics Row)

Placed horizontally across the top of the dashboard for instant situational awareness:

| KPI Card | Formula / Source | Visual Format | Target Threshold |
|---|---|---|---|
| **Total Active Containers** | `COUNT(DISTINCT container_id) FROM fct_spoilage_arbitrage` | Big Number | Active Fleet Size |
| **Containers At Risk** | `COUNT(DISTINCT container_id) WHERE risk_category IN ('High Risk', 'Critical Risk')` | Big Number (Amber) | Alert if > 0 |
| **Critical Containers** | `COUNT(DISTINCT container_id) WHERE risk_category = 'Critical Risk'` | Big Number (Red Pulse) | Immediate Action |
| **Average Temperature** | `AVG(temperature_c) FROM fct_container_health` | Numeric Gauge (°C) | Expected Envelope |
| **Average Humidity** | `AVG(humidity_pct) FROM fct_container_health` | Numeric Gauge (%) | Expected Envelope |
| **Total Potential Arbitrage** | `SUM(spoilage_arbitrage_inr) WHERE is_reroute_recommended = TRUE` | Big Number Currency (₹) | Opportunity Value |
| **Potential Loss Avoided** | `SUM(spoilage_avoided_value_inr) WHERE is_reroute_recommended = TRUE` | Big Number Currency (₹) | Preserved Capital |

---

## 2. Core Visualizations & Layout Grid

```
+----------------------------------------------------------------------------------------------------+
|  [Filter Bar: Container ID | Commodity | Origin | Destination | Risk Level | Date/Time Range]      |
+----------------------------------------------------------------------------------------------------+
| [KPI 1: Fleet] [KPI 2: At Risk] [KPI 3: Critical] [KPI 4: Temp] [KPI 5: Hum] [KPI 6: Arbitrage ₹] |
+---------------------------------------------------+------------------------------------------------+
| Chart 1: Container Health Geographic Map          | Chart 4: Spoilage Risk Leaderboard             |
| (Interactive Deck.gl / Scatterplot of fleet)      | (Ranked bar chart: 0-100 risk score by unit)   |
+---------------------------------------------------+------------------------------------------------+
| Chart 2: Micro-Climate Temperature Trends         | Chart 3: Micro-Climate Humidity Trends         |
| (Multi-line time series by container)             | (Multi-line time series with critical bands)   |
+---------------------------------------------------+------------------------------------------------+
| Chart 5: Real-Time Spoilage Arbitrage Matrix                                                       |
| (Table: Container | Risk | Time Left | Current Dest | Recommended Dest | Arbitrage ₹ | Action)      |
+---------------------------------------------------+------------------------------------------------+
| Chart 6: Multi-Market Revenue Realization         | Chart 7: Logistics Corridor Quality Impact     |
| (Grouped bar chart: Current vs Alternative NRV)   | (Route performance: transit vs spoilage rate)  |
+---------------------------------------------------+------------------------------------------------+
```

---

## 3. Detailed Chart Specifications

### Chart 1: Container Health Geographic Map
- **Chart Type**: Deck.gl Scatterplot / Mapbox View
- **Datasource**: `ANALYTICS.FCT_SPOILAGE_ARBITRAGE`
- **Longitude / Latitude**: `longitude`, `latitude`
- **Color Category**: `risk_category`
  - Green (`#10B981`): Low Risk
  - Yellow (`#F59E0B`): Moderate Risk
  - Orange (`#F97316`): High Risk
  - Red (`#EF4444`): Critical Risk
- **Tooltip**: Container ID, Commodity, Current Temp, Remaining Hours to Spoilage, Recommended Destination.

### Chart 2: Micro-Climate Temperature Trends
- **Chart Type**: Time-series Line Chart
- **Datasource**: `ANALYTICS.FCT_CONTAINER_HEALTH`
- **X-Axis**: `telemetry_timestamp`
- **Y-Axis**: `temperature_c`
- **Group By**: `container_id`
- **Annotation Lines**: Upper safe threshold (12°C for Avocado, 18°C for Banana/Tomato).

### Chart 3: Micro-Climate Humidity Trends
- **Chart Type**: Time-series Line Chart
- **Datasource**: `ANALYTICS.FCT_CONTAINER_HEALTH`
- **X-Axis**: `telemetry_timestamp`
- **Y-Axis**: `humidity_pct`
- **Group By**: `container_id`
- **Annotation Lines**: 95% upper limit (high rot risk).

### Chart 4: Spoilage Risk Leaderboard
- **Chart Type**: Horizontal Bar Chart
- **Datasource**: `ANALYTICS.FCT_SPOILAGE_ARBITRAGE`
- **X-Axis**: `spoilage_risk_score` (0–100)
- **Y-Axis**: `container_id`
- **Color Metric**: `spoilage_risk_score` gradient from `#10B981` to `#EF4444`.

### Chart 5: Real-Time Spoilage Arbitrage Opportunity Matrix
- **Chart Type**: Interactive Grid Table
- **Datasource**: `ANALYTICS.FCT_SPOILAGE_ARBITRAGE`
- **Columns**:
  1. `container_id`
  2. `commodity`
  3. `risk_category`
  4. `time_to_spoilage_hours`
  5. `current_destination`
  6. `current_expected_value_inr`
  7. `recommended_destination`
  8. `reroute_expected_value_inr`
  9. `spoilage_arbitrage_inr` (Highlighted in Green when > 0)
  10. `recommendation_reason`

### Chart 6: Multi-Market Revenue Realization Comparison
- **Chart Type**: Grouped Bar Chart
- **Datasource**: `ANALYTICS.FCT_SPOILAGE_ARBITRAGE`
- **Series 1**: `current_expected_value_inr`
- **Series 2**: `reroute_expected_value_inr`
- **Dimension**: `container_id`

### Chart 7: Logistics Corridor Quality Impact
- **Chart Type**: Bubble Chart / Scatter
- **Datasource**: `INTERMEDIATE.INT_MARKET_OPTIONS`
- **X-Axis**: `distance_km`
- **Y-Axis**: `estimated_transit_hours`
- **Bubble Size**: `quantity_kg`
- **Color**: `projected_spoilage_rate`

---

## 4. Interactive Filters

The dashboard incorporates cross-filtering across:
1. **Container ID**: Multi-select dropdown
2. **Commodity**: Multi-select (`Avocado`, `Banana`, `Tomato`, `Mango`, `Apple`)
3. **Origin**: Select (`Nashik`, `Pune`, `Nagpur`, `Hyderabad`)
4. **Destination**: Select (`Mumbai`, `Pune`, `Hyderabad`, `Bengaluru`, `Delhi`, `Chennai`)
5. **Risk Level**: Multi-select (`Low Risk`, `Moderate Risk`, `High Risk`, `Critical Risk`)
6. **Date & Time Range**: Relative time filter (Last 1 hour, Last 6 hours, Last 24 hours)