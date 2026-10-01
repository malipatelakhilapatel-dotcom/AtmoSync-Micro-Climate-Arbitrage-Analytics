AtmoSync — Reproducible End-to-End Demo Walkthrough
Scenario Overview: Salvaging Container CONT_001
This walkthrough demonstrates the exact business scenario required for portfolio and interview presentations:
Consignment: 12,000 kg of premium Hass Avocados ($Q = 12,000$).
Carrier / Container: CONT_001 (Consignment SHIP_101).
Original Route: Nashik Packhouse to Mumbai APMC Terminal Market ($167$ km, $4.5$ hours transit).
The Event: En route near Lonavala, the reefer container compressor suffers a coolant leak.
Micro-Climate Impact:
Internal temperature climbs from $6.2^\circ\text{C}$ to $14.8^\circ\text{C}$ (Safe maximum: $8.0^\circ\text{C}$).
Relative humidity rises to $93.2%$.
Detection & Valuation:
Telematics streamed through Kafka.
Spoilage risk index surges to $84.6$ / $100$ (Critical).
Remaining shelf life ($TTS$) collapses from $120$ hours to $7.2$ hours.
Transit to Mumbai still requires $9.0$ hours (including city port clearance); cargo will spoil en route ($80%$ write-off penalty).
Closer alternative market Pune Agricultural Terminal is only $2.8$ hours away.
Rerouting captures ₹182.00/kg spot rate and saves ₹1,507,420 in net salvage value!
Step-by-Step Reproduction Instructions
Step 1: Run the Integrated Demo Runner
Run the standalone demo script from the project root:
python run_demo.py
Step 2: Observe Telematics Progression in Terminal
The simulator initializes the fleet and steps CONT_001 through thermal degradation:
[SIMULATOR] Step 1: CONT_001 | Temp: 6.2°C  | Status: Optimal
[SIMULATOR] Step 2: CONT_001 | Temp: 8.5°C  | Status: Thermal Alert Triggered
[SIMULATOR] Step 3: CONT_001 | Temp: 11.4°C | Status: High Risk Warning
[SIMULATOR] Step 4: CONT_001 | Temp: 14.8°C | Status: CRITICAL RISK DETECTED
Step 3: Inspect the Financial Arbitrage Comparison Output
The decision engine calculates the multi-market options:
========================================================================================================
                                   ATMOSYNC DECISION INTELLIGENCE ENGINE
========================================================================================================
[ALERT] Container CONT_001 [Avocado] entering CRITICAL RISK (Score: 84.6/100)!
        Current Location: Latitude 18.924, Longitude 73.812 (Near Lonavala)
        Temperature: 14.8°C (Safe Max: 8.0°C) | Humidity: 93.2% | Vibration: 0.38g
        Remaining Time to Spoilage: 7.2 Hours | Est. Transit to Mumbai: 9.0 Hours

--------------------------------------------------------------------------------------------------------
SPOILAGE ARBITRAGE REROUTING EVALUATION:
--------------------------------------------------------------------------------------------------------
* Status Quo (Mumbai):
  - Expected Revenue: ₹1,560,000 | Expected Spoilage Loss: 75% (-₹1,170,000)
  - Net Expected Outcome: ₹390,000

* Optimal Reroute (Pune Agricultural Terminal):
  - Transit Time: 2.8 Hours (Safely within 7.2-hour spoilage window!)
  - Alternative Market Price: ₹185.00 / kg | Gross Value: ₹2,220,000
  - Reroute Transport Surcharge: ₹24,500 | Spoilage Loss at Arrival: 12% (-₹266,400)
  - Net Expected Outcome: ₹1,929,100

>>> SPOILAGE ARBITRAGE VALUE: +₹1,539,100 NET GAIN (RECOMMENDATION: EXECUTE REROUTE TO PUNE) <<<
========================================================================================================
Step 4: Verify in Apache Superset / Tableau
When opened in Superset:
CONT_001 marker flashes red on the Container Health Map.
Temperature Trend displays the steep hockey-stick curve upwards.
Spoilage Arbitrage Opportunities Table flags:
Container: CONT_001
Current Destination: Mumbai
Recommended Destination: Pune
Spoilage Arbitrage: +₹1,539,100
Action: EXECUTE_REROUTE