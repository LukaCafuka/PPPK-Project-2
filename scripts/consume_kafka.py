import json
import logging
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from confluent_kafka import Consumer, KafkaError, KafkaException

# Add parent directory to path for imports
sys.path.insert(0, '..')
from config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, KAFKA_GROUP_ID
from utils.mongo_client import insert_observations_batch, get_observations_count

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def create_consumer() -> Consumer:
    config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': KAFKA_GROUP_ID,
        'auto.offset.reset': 'earliest',  # Read from beginning on first run
        'enable.auto.commit': False,       # Manual commit after processing
        'session.timeout.ms': 30000,
        'max.poll.interval.ms': 300000,
    }
    
    consumer = Consumer(config)
    logger.info(f"Created Kafka consumer for broker: {KAFKA_BOOTSTRAP_SERVERS}")
    return consumer


def parse_message(msg_value: bytes) -> Optional[Dict[str, Any]]:
    try:
        data = json.loads(msg_value.decode('utf-8'))
        return data
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse message as JSON: {e}")
        return None
    except Exception as e:
        logger.error(f"Error parsing message: {e}")
        return None


def transform_observation(raw_data: Dict[str, Any]) -> Dict[str, Any]:
    # Extract core required fields
    observation = {
        "taxon_key": raw_data.get("taxon_key") or raw_data.get("taxonKey") or raw_data.get("species_id"),
        "source": "kafka",
    }
    
    # Extract location (handle different formats)
    location = raw_data.get("location", {})
    if isinstance(location, dict):
        observation["location"] = {
            "latitude": location.get("latitude") or location.get("lat", 0),
            "longitude": location.get("longitude") or location.get("lng") or location.get("lon", 0)
        }
    elif "latitude" in raw_data and "longitude" in raw_data:
        observation["location"] = {
            "latitude": raw_data.get("latitude", 0),
            "longitude": raw_data.get("longitude", 0)
        }
    elif "lat" in raw_data and "lng" in raw_data:
        observation["location"] = {
            "latitude": raw_data.get("lat", 0),
            "longitude": raw_data.get("lng", 0)
        }
    else:
        observation["location"] = {"latitude": 0, "longitude": 0}
    
    # Extract timestamp
    observed_at = raw_data.get("observed_at") or raw_data.get("observedAt") or raw_data.get("timestamp")
    if observed_at:
        if isinstance(observed_at, str):
            try:
                observation["observed_at"] = datetime.fromisoformat(observed_at.replace('Z', '+00:00'))
            except ValueError:
                observation["observed_at"] = datetime.now(timezone.utc)
        else:
            observation["observed_at"] = observed_at
    else:
        observation["observed_at"] = datetime.now(timezone.utc)
    
    # Extract biological data (flexible schema - take everything else)
    # Known fields to exclude from biological_data
    excluded_fields = {
        "taxon_key", "taxonKey", "species_id",
        "location", "latitude", "longitude", "lat", "lng", "lon",
        "observed_at", "observedAt", "timestamp"
    }
    
    biological_data = raw_data.get("biological_data", {})
    if isinstance(biological_data, dict):
        observation["biological_data"] = biological_data
    else:
        # If no explicit biological_data, extract remaining fields
        observation["biological_data"] = {
            k: v for k, v in raw_data.items() 
            if k not in excluded_fields and not k.startswith("_")
        }
    
    return observation


def consume_all_messages(consumer: Consumer, batch_size: int = 100) -> int:
    consumer.subscribe([KAFKA_TOPIC])
    logger.info(f"Subscribed to topic: {KAFKA_TOPIC}")
    
    total_processed = 0
    batch: List[Dict[str, Any]] = []
    empty_polls = 0
    max_empty_polls = 3  # Stop after 3 consecutive empty polls
    
    try:
        while empty_polls < max_empty_polls:
            msg = consumer.poll(timeout=5.0)
            
            if msg is None:
                empty_polls += 1
                logger.debug(f"No message received (empty poll {empty_polls}/{max_empty_polls})")
                continue
            
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.info(f"Reached end of partition {msg.partition()}")
                    empty_polls += 1
                    continue
                else:
                    raise KafkaException(msg.error())
            
            # Reset empty poll counter on successful message
            empty_polls = 0
            
            # Parse and transform the message
            raw_data = parse_message(msg.value())
            if raw_data is None:
                logger.warning(f"Skipping unparseable message at offset {msg.offset()}")
                continue
            
            observation = transform_observation(raw_data)
            batch.append(observation)
            
            # Process batch when it reaches the batch size
            if len(batch) >= batch_size:
                inserted_ids = insert_observations_batch(batch)
                total_processed += len(inserted_ids)
                logger.info(f"Inserted batch of {len(inserted_ids)} observations (total: {total_processed})")
                batch = []
                
                # Commit offsets after successful batch insert
                consumer.commit(asynchronous=False)
        
        # Insert remaining messages in the final batch
        if batch:
            inserted_ids = insert_observations_batch(batch)
            total_processed += len(inserted_ids)
            logger.info(f"Inserted final batch of {len(inserted_ids)} observations (total: {total_processed})")
            consumer.commit(asynchronous=False)
    
    except KeyboardInterrupt:
        logger.info("Consumer interrupted by user")
    except Exception as e:
        logger.error(f"Error consuming messages: {e}")
        raise
    finally:
        consumer.close()
        logger.info("Consumer closed")
    
    return total_processed


def main():
    """Main entry point for the Kafka consumer script."""
    logger.info("=" * 60)
    logger.info("Consume Kafka Bird Observations")
    logger.info("=" * 60)
    
    # Get initial count
    initial_count = get_observations_count()
    logger.info(f"Initial observations count in MongoDB: {initial_count}")
    
    # Create consumer and process messages
    consumer = create_consumer()
    
    try:
        processed = consume_all_messages(consumer)
        
        # Get final count
        final_count = get_observations_count()
        
        logger.info("=" * 60)
        logger.info(f"Processing complete!")
        logger.info(f"Messages processed: {processed}")
        logger.info(f"Observations in database: {final_count} (added {final_count - initial_count})")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"Failed to consume messages: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
