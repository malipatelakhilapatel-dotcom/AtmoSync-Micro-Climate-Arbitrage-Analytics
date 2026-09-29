# AtmoSync — IoT Telemetry Simulator

## Overview
The **IoT Telemetry Simulator** simulates real-time micro-climate sensors mounted inside refrigerated shipping containers ("reefers") transporting high-value agricultural commodities across Indian logistics routes.

## Monitored Parameters
- **Temperature (°C)**: Thermal sensor with ±0.05°C precision.
- **Relative Humidity (%RH)**: Moisture sensor tracking condensation risk.
- **Vibration (g)**: Tri-axial accelerometer capturing road surface shocks and rough transit.
- **GPS Coordinates**: In-transit latitude and longitude interpolating from origin terminal to scheduled destination.

## Failure Modes Simulated
1. **NORMAL**: Telemetry stays within commodity biological comfort zones with natural white-noise oscillations.
2. **GRADUAL_WARMING**: Simulates a slow refrigerant gas leak or thermal insulation failure (+0.3°C to +0.8°C/interval).
3. **SUDDEN_SPIKE**: Simulates reefer power generator failure or container doors forced open in transit.
4. **HIGH_HUMIDITY**: Condensation runaway triggering mold proliferation.
5. **EXCESSIVE_VIBRATION**: Potholes and rough roadway transit exceeding cellular bruising thresholds.
6. **COMPOUND_FAILURE**: Concurrent thermal runaway and humidity saturation.

## Running the Simulator

### Basic Generation (10 iterations, 3 containers):
```bash
python iot_simulator.py --num-containers 3 --iterations 10 --interval 1.5
```

### Continuous Fleet Streaming:
```bash
python iot_simulator.py --num-containers 5 --interval 2.0 --iterations 0
```

### Export Golden Test Telemetry:
```bash
python iot_simulator.py --export-sample ../data/sample_sensor_data.json
```
