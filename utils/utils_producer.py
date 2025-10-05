"""This file contain functions to be used in
a producer"""

#################
#IMPORTS
#################
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from kafka import KafkaProducer
import utils.utils_config as uc
from utils.utils_logger import logger
import json

#################
#FUNCTIONS
#################

# This functions creates a kafka topic on a specified broker
def create_kafka_topic(partitions:int=5,
                       replication_factor:int=1):
    
    broker = uc.get_bootstrapserver_address()
    
    topic_name = uc.get_kafka_topic()
    
    admin_client = KafkaAdminClient(bootstrap_servers = broker,
                                    client_id ='mobile_money_admin' )
    
    topic = NewTopic(
        name = topic_name,
        num_partitions = partitions,
        replication_factor=replication_factor
    )

    try:
        admin_client.create_topics(new_topics=[topic],validate_only=False)
        logger.info(f"Topic {topic_name} has been created successfully with {partitions} partitions.")
    except TopicAlreadyExistsError:
        logger.info(f"Topic {topic_name} already exists.")

    admin_client.close()

# This function creates a producer instance
def create_kafka_producer():
    broker = uc.get_bootstrapserver_address()
    producer = KafkaProducer(bootstrap_servers=broker,
                             key_serializer=lambda k:json.dumps(k).encode('utf-8') if k else None,
                             value_serializer=lambda v:json.dumps(v).encode('utf-8') if v else None)
    return producer
    logger.info(f"Kafka producer connected to broker:{broker}")
