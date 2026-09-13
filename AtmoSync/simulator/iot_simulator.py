import random
import time
import json
from datetime import datetime, timezone
CONTAINERS = [
    {
        "container_id": "CONT001",
        "commodity": "Avocado"
    },
    {
        "container_id": "CONT002",
        "commodity": "Mango"
    },
    {
        "container_id": "CONT003",
        "commodity": "Tomato"
    },
    {
        "container_id": "CONT004",
        "commodity": "Banana"
    },
    {
        "container_id": "CONT005",
        "commodity": "Apple"
    }
]
def generate_telemetry(container):
    """
    Generate one sensor reading for a shipping container.
    """
    temperature = round(random.uniform(22, 30), 2)
    humidity = round(random.uniform(60, 85), 2)
    vibration = round(random.uniform(0.10, 0.50), 2)
    if random.random() < 0.20:
        temperature = round(random.uniform(31, 36), 2)
        humidity = round(random.uniform(86, 95), 2)
        vibration = round(random.uniform(0.50, 0.90), 2)
    telemetry = {
        "container_id": container["container_id"],
        "commodity": container["commodity"],
        "temperature": temperature,
        "humidity": humidity,
        "vibration": vibration,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }
    return telemetry
def main():
    print("=" * 60)
    print("        AtmoSync IoT Simulator")
    print("=" * 60)
    print("Generating container telemetry...")
    print("Press Ctrl+C to stop.\n")
    try:
        while True:
            container = random.choice(CONTAINERS)
            telemetry = generate_telemetry(container)
            json_data = json.dumps(telemetry)
            print(json_data)
            time.sleep(2)
    except KeyboardInterrupt:
        print("\n")
        print("AtmoSync IoT Simulator stopped.")
if __name__ == "__main__":
    main()