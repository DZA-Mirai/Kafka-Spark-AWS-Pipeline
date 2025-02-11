import os
import time
import uuid
from datetime import datetime, timedelta
import random

from confluent_kafka import SerializingProducer
import simplejson as json

# Define GPS coordinates (find them in Google)
LONDON_COORDINATES = {"latitude": 51.5074, "longitude": -0.1278}
BIRMINGHAM_COORDINATES = {"latitude": 52.4862, "longitude": -1.8904}

# Calculate how much latitude/longitude changes per step (100 steps in total)
LATITUDE_INCREMENT = (BIRMINGHAM_COORDINATES['latitude'] - LONDON_COORDINATES['latitude']) / 100
LONGITUDE_INCREMENT = (BIRMINGHAM_COORDINATES['longitude'] - LONDON_COORDINATES['longitude']) / 100

# Retrieve Kafka configuration from environment variables, with default values
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
VEHICLE_TOPIC = os.getenv('VEHICLE_TOPIC', 'vehicle_data')
GPS_TOPIC = os.getenv('GPS_TOPIC', 'gps_data')
TRAFFIC_TOPIC = os.getenv('TRAFFIC_TOPIC', 'traffic_data')
WEATHER_TOPIC = os.getenv('WEATHER_TOPIC', 'weather_data')
EMERGENCY_TOPIC = os.getenv('EMERGENCY_TOPIC', 'emergency_data')

random.seed(42)     # Seed the random generator for reproducibility
start_time = datetime.now()
start_location = LONDON_COORDINATES.copy()


def get_next_time():
    """ Generate the next timestamp by adding a random delay (30-60 seconds). """
    global start_time
    start_time += timedelta(seconds=random.randint(30, 60))  # update frequency
    return start_time


def generate_gps_data(dev_id, timestamp, vehicle_type='private'):
    """ Simulates GPS data for a vehicle. """
    return {
        'id': uuid.uuid4(),
        'deviceId': dev_id,
        'timestamp': timestamp,
        'speed': random.uniform(0, 40),   # km/h
        'direction': 'North-East',
        'vehicle_type': vehicle_type
    }


def generate_traffic_camera_data(dev_id, timestamp, location, camera_id):

    return {
        'id': uuid.uuid4(),
        'device_id': dev_id,
        'camera_id': camera_id,
        'location': location,
        'timestamp': timestamp,
        'snapshot': 'Base64EncodedString'   # Placeholder for image data
    }


def generate_weather_data(dev_id, timestamp, location):
    return {
        'id': uuid.uuid4(),
        'deviceId': dev_id,
        'location': location,
        'timestamp': timestamp,
        'temperature': random.uniform(-5, 26),      # Temperature in Celsius
        'weatherCondition': random.choice(['Sunny', 'Cloudy', 'Rain', 'Snow']),
        'precipitation': random.uniform(0, 25),
        'windSpeed': random.uniform(0, 100),
        'humidity': random.randint(0, 100),   # percentage
        'airQualityIndex': random.uniform(0, 200)   # AQI score (IQAir)
    }

# Function to generate emergency incident data, such as accidents or fires
def generate_emergency_incident_data(dev_id, timestamp, location):
    return {
        'id': uuid.uuid4(),
        'deviceId': dev_id,
        'incidentId': uuid.uuid4(),
        'type': random.choice(['Accident', 'Fire', 'Medical', 'Police', 'None']),
        'timestamp': timestamp,
        'location': location,
        'status': random.choice(['Active', 'Resolved']),
        'description': 'Description of the incident'
    }


def simulate_vehicle_movement():
    global start_location

    # move towards birmingham
    start_location['latitude'] += LATITUDE_INCREMENT
    start_location['longitude'] += LONGITUDE_INCREMENT

    # Add slight randomness to simulate real-world driving
    start_location['latitude'] += random.uniform(-0.0005, 0.0005)
    start_location['longitude'] += random.uniform(-0.0005, 0.0005)

    return start_location

# Function to generate vehicle telemetry data
def generate_vehicle_data(dev_id):
    location = simulate_vehicle_movement()
    return {
        'id': uuid.uuid4(),
        'deviceId': dev_id,
        'timestamp': get_next_time().isoformat(),
        'location': (location['latitude'], location['longitude']),
        'speed': random.uniform(10, 40),
        'direction': 'North-East',
        'make': 'Chevrolet',
        'model': 'Matiz',
        'year': 2003,
        'fuelType': 'Gas'
    }


def json_serializer(obj):
    if isinstance(obj, uuid.UUID):
        return str(obj)
    raise TypeError(f'Object of type {obj.__class__.__name__} is not JSON serializable')

# Callback function to confirm message delivery
def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

# Function to produce data to Kafka topics
def produce_data_to_kafka(prod, topic, data):
    producer.produce(
        topic,
        key=str(data['id']),
        value=json.dumps(data, default=json_serializer).encode('utf-8'),
        on_delivery=delivery_report
    )

    producer.flush()        # Ensure messages are sent immediately

# Main function to simulate the vehicle journey and send data to Kafka
def simulate_journey(prod, device_id):
    while True:
        vehicle_data = generate_vehicle_data(device_id)
        gps_data = generate_gps_data(device_id, vehicle_data['timestamp'])
        traffic_camera_data = generate_traffic_camera_data(device_id, vehicle_data['timestamp'], vehicle_data['location'], camera_id='Nikon-Cam-534')
        weather_data = generate_weather_data(device_id, vehicle_data['timestamp'], vehicle_data['location'])
        emergency_incident_data = generate_emergency_incident_data(device_id, vehicle_data['timestamp'], vehicle_data['location'])

        # Check if the vehicle has reached Birmingham and stop simulation
        if (vehicle_data['location'][0] >= BIRMINGHAM_COORDINATES['latitude']
            and vehicle_data['location'][1] <= BIRMINGHAM_COORDINATES["longitude"]):
            print('Vehicle has reached Birmingham. Simulation ending...')
            break

        # Send generated data to corresponding Kafka topics
        produce_data_to_kafka(prod, VEHICLE_TOPIC, vehicle_data)
        produce_data_to_kafka(prod, GPS_TOPIC, gps_data)
        produce_data_to_kafka(prod, TRAFFIC_TOPIC, traffic_camera_data)
        produce_data_to_kafka(prod, WEATHER_TOPIC, weather_data)
        produce_data_to_kafka(prod, EMERGENCY_TOPIC, emergency_incident_data)

        time.sleep(3)   # Delay between data transmissions


if __name__ == "__main__":
    producer_config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'error_cb': lambda err: print(f'Kafka error: {err}')
    }
    producer = SerializingProducer(producer_config)

    try:
        simulate_journey(producer, 'Vehicle-Chevrolet-Matiz-1')

    except KeyboardInterrupt:
        print('Simulation ended by the user')
    except Exception as e:
        print(f'Unexpected error occurred: {e}')
