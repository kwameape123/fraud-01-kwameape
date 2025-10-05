"""This file contains functions that help config the various 
tools that will be used in this project."""

###########################
# IMPORTS
###########################
import os
from dotenv import load_dotenv
from utils.utils_logger import logger
import pathlib
from kafka import KafkaAdminClient
from kafka.errors import KafkaError, NoBrokersAvailable

###########################
# LOAD ENVIRONMENT VARIABLES
###########################
load_dotenv()

############################
# KAFKA CONFIGURATIONS
############################

def get_bootstrapserver_address()-> str:
    address = os.getenv('KAFKA_BROKER_ADDRESS','localhost:9092')
    address = str(address)
    return address

def get_kafka_topic()->str:
    topic = os.getenv('PROJECT_TOPIC','mobile_money')
    topic = str(topic)
    return topic

def kafka_consumer_group_id()->str:
    group_id = os.getenv('PROJECT_CONSUMER_GROUP_ID','mobile_money_consumer_group_id')
    group_id = str(group_id)
    return group_id

def kafka_client_id()->str:
    client_id = os.getenv('PROJECT_CLIENT_ID','mobile_money_client')
    client_id = str(client_id)
    return client_id

def get_message_interval_in_seconds()->int:
    message_interval_in_seconds = os.getenv('MESSAGE_INTERVAL',5)
    message_interval_in_seconds = int(message_interval_in_seconds)
    return message_interval_in_seconds

def verify_kafka_service()-> bool:
    address = get_bootstrapserver_address()
    try:
        admin_client = KafkaAdminClient(bootstrap_servers=address)
        topics = admin_client.list_topics()
        logger.info(f"Kafka is running! Topics available:{topics}")
        return True
    except NoBrokersAvailable:
        logger.warning(f"No kafka broker available")
        return False
    except KafkaError as e:
        logger.warning(f"Failed to establish kafka conncection:{e}")
        return False
    except Exception as e:
        logger.warning(f"Unidentified error:{e}")
        return False




############################
# DATA FILE CONFIGURATIONS
############################

def get_base_data_path() -> pathlib.Path:
    """Fetch BASE_DATA_DIR from environment or use default."""
    project_root = pathlib.Path(__file__).parent.parent
    data_dir = project_root / os.getenv("BASE_DATA_DIR", "data")
    logger.info(f"BASE_DATA_DIR: {data_dir}")
    return data_dir

def get_live_data_path() -> pathlib.Path:
    """Fetch LIVE_DATA_FILE_NAME from environment or use default."""
    live_data_path = get_base_data_path() / os.getenv(
        "LIVE_DATA_FILE_NAME", "mobile_money_transaction_live.json"
    )
    logger.info(f"LIVE_DATA_PATH: {live_data_path}")
    return live_data_path

DATA_FILE = get_base_data_path().joinpath("mobile_money_log.csv")
logger.info(f"Data file: {DATA_FILE}")

JSON_FILE = get_live_data_path()


def get_postgres_config():
    pg_config= {
        "dbname": os.getenv("POSTGRES_DB"),
        "user": os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD"),
        "host": os.getenv("POSTGRES_HOST"),
        "port": int(os.getenv("POSTGRES", 5432))
    }
    return pg_config



