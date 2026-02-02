"""
Test script to produce sample bird observation messages to Kafka.
Use this to test the step2_consume_kafka.py consumer.

Usage:
    python test_kafka_producer.py [num_messages]
"""

import json
import random
import sys
from datetime import datetime, timezone, timedelta

from confluent_kafka import Producer

# Add parent directory to path for imports
sys.path.insert(0, '..')
from config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC


# Sample data for generating realistic bird observations
SAMPLE_TAXON_KEYS = [
    "2474953",  # Parus major (Great Tit)
    "2474989",  # Cyanistes caeruleus (Blue Tit)
    "2490384",  # Turdus merula (Common Blackbird)
    "2490719",  # Erithacus rubecula (European Robin)
    "2481139",  # Passer domesticus (House Sparrow)
    "2479916",  # Fringilla coelebs (Common Chaffinch)
    "2481987",  # Corvus corone (Carrion Crow)
    "2481020",  # Pica pica (Eurasian Magpie)
]

HABITATS = ["forest", "urban", "wetland", "grassland", "coastal", "mountain"]
MIGRATION_STATUSES = ["resident", "partial_migrant", "summer_visitor", "winter_visitor", "passage_migrant"]
FLIGHT_PATTERNS = ["direct", "undulating", "soaring", "hovering", "gliding"]


def delivery_callback(err, msg):
    """Callback for message delivery reports."""
    if err:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")


def generate_random_observation() -> dict:
    """Generate a random bird observation with varying biological properties."""
    # Random location in Croatia (roughly)
    lat = random.uniform(42.5, 46.5)
    lng = random.uniform(13.5, 19.5)
    
    # Random timestamp in the last 30 days
    days_ago = random.randint(0, 30)
    hours_ago = random.randint(0, 23)
    observed_at = datetime.now(timezone.utc) - timedelta(days=days_ago, hours=hours_ago)
    
    observation = {
        "taxon_key": random.choice(SAMPLE_TAXON_KEYS),
        "location": {
            "latitude": round(lat, 6),
            "longitude": round(lng, 6)
        },
        "observed_at": observed_at.isoformat(),
    }
    
    # Add varying biological properties (flexible schema demonstration)
    biological_data = {}
    
    # Randomly include different properties
    if random.random() > 0.3:
        biological_data["habitat"] = random.choice(HABITATS)
    
    if random.random() > 0.5:
        biological_data["migration_status"] = random.choice(MIGRATION_STATUSES)
    
    if random.random() > 0.6:
        biological_data["flight_pattern"] = random.choice(FLIGHT_PATTERNS)
    
    if random.random() > 0.7:
        biological_data["body_size_cm"] = round(random.uniform(10, 50), 1)
    
    if random.random() > 0.7:
        biological_data["estimated_weight_g"] = round(random.uniform(10, 500), 1)
    
    if random.random() > 0.8:
        biological_data["group_size"] = random.randint(1, 50)
    
    if random.random() > 0.8:
        biological_data["is_breeding"] = random.choice([True, False])
    
    if random.random() > 0.9:
        biological_data["notes"] = random.choice([
            "Singing from treetop",
            "Feeding at bird table",
            "Nest building observed",
            "Juvenile bird",
            "Carrying food to nest"
        ])
    
    observation["biological_data"] = biological_data
    
    return observation


def produce_messages(num_messages: int = 10):
    """Produce sample messages to Kafka."""
    producer = Producer({
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'client.id': 'test-producer'
    })
    
    print(f"Producing {num_messages} sample messages to {KAFKA_TOPIC}...")
    print(f"Broker: {KAFKA_BOOTSTRAP_SERVERS}")
    print("-" * 50)
    
    for i in range(num_messages):
        observation = generate_random_observation()
        message = json.dumps(observation)
        
        producer.produce(
            topic=KAFKA_TOPIC,
            value=message.encode('utf-8'),
            callback=delivery_callback
        )
        
        print(f"[{i+1}/{num_messages}] Produced: taxon_key={observation['taxon_key']}, "
              f"location=({observation['location']['latitude']:.2f}, {observation['location']['longitude']:.2f}), "
              f"bio_props={list(observation['biological_data'].keys())}")
        
        # Flush every 10 messages
        if (i + 1) % 10 == 0:
            producer.flush()
    
    # Final flush
    producer.flush()
    
    print("-" * 50)
    print(f"Done! Produced {num_messages} messages to topic '{KAFKA_TOPIC}'")


def main():
    num_messages = 10  # Default
    
    if len(sys.argv) > 1:
        try:
            num_messages = int(sys.argv[1])
        except ValueError:
            print(f"Invalid number of messages: {sys.argv[1]}")
            print("Usage: python test_kafka_producer.py [num_messages]")
            sys.exit(1)
    
    produce_messages(num_messages)


if __name__ == "__main__":
    main()
