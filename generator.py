import boto3
import json
import random
import time
from datetime import datetime

# Initialize Kinesis client
kinesis = boto3.client('kinesis', region_name='us-east-1')  # Change region if needed

def generate_sensor_data():
    return {
        "sensor_id": random.randint(1, 4),
        "temperature": random.uniform(20.0, 30.0),
        "humidity": random.uniform(30.0, 50.0),
        "timestamp": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    }

while True:
    data = generate_sensor_data()
    print("Sending:", data)
    kinesis.put_record(
        StreamName="sensor-stream",  # Make sure this matches your stream name
        Data=json.dumps(data),
        PartitionKey=str(data["sensor_id"])
    )
    time.sleep(5)
