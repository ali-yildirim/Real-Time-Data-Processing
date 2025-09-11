from kafka.errors import NoBrokersAvailable, TopicAlreadyExistsError
from kafka.admin import KafkaAdminClient, NewTopic
from kafka import KafkaProducer
import logging
import requests
import json
import time
import os
import sys


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

MAX_RETRIES = 5
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC')
BOOTSTRAP_SERVERS = os.getenv('BOOTSTRAP_SERVERS')

if not KAFKA_TOPIC: 
    logger.critical("Error: KAFKA_TOPIC environment variable must be set. Exiting...")
    sys.exit(1)

if not BOOTSTRAP_SERVERS:
    logger.critical("Error: BOOTSTRAP_SERVERS environment variable must be set. Exiting...")
    sys.exit(1)

def create_admin(server):
    try:
        return KafkaAdminClient(bootstrap_servers=server)
    except Exception as e:
        logger.critical(f"Failed to create admin client: {e}. Exiting...")
        sys.exit(1)

def create_topic(topic_name, admin):
    topic = NewTopic(name=topic_name, num_partitions=32, replication_factor=1)
    try:
        admin.create_topics([topic])
        logger.info(f"Topic '{topic_name}' created.")
    except TopicAlreadyExistsError:
        logger.warning(f"Topic '{topic_name}' already exists.")
    except Exception as e:
        logger.warning(f"Failed to create topic: {e}")
        sys.exit(1)

def create_producer(server):
    return KafkaProducer(
        bootstrap_servers=[server],
        retries=5,
        retry_backoff_ms=1000,
        request_timeout_ms=30000,
        value_serializer=lambda x: json.dumps(x).encode('utf-8'),
        key_serializer=str.encode
    )

producer = None
admin = None

try:
    # Create producer
    for attempt in range(MAX_RETRIES):
        try:
            producer = create_producer(BOOTSTRAP_SERVERS)
            logger.info("Kafka Producer has been created successfully.")
            break
        except NoBrokersAvailable as e:
            if attempt < MAX_RETRIES - 1:
                time.sleep(2)
            else:
                logger.warning("Exhausted all retries. Failed to connect to Kafka broker.")
                raise

    admin = create_admin(BOOTSTRAP_SERVERS)
    create_topic(KAFKA_TOPIC, admin)

    logger.info("Data ingestion is starting...")
    while True:
        try:
            number_of_messages = 99
            url = f"https://random-data-api.com/api/v2/users?size={number_of_messages}"
            response = requests.get(url)
            response.raise_for_status() 
            data = response.json()
            logger.info("The data has been fetched successfully.")
            
            for i in range(number_of_messages):
                producer.send(KAFKA_TOPIC, value=data[i], key=str(i%32))
            producer.flush()  
            logger.info("Data ingestion has been completed.")
            
            time.sleep(60)
        except requests.RequestException as e:
            logger.warning(f"Failed to fetch data: {e}")
            time.sleep(60) 
        except Exception as e:
            logger.warning(f"Unexpected error: {e}")
            time.sleep(60)  
finally:
    if producer:
        producer.close()
    if admin:
        admin.close()
