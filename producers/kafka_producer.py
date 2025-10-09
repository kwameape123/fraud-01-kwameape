
"""
Producer that reads mobile money transaction logs from CSV
and writes messages to a JSON file for testing.
"""

import csv
import json
import sys
import time
from datetime import datetime, timedelta
import utils.utils_config as uc
import utils.utils_producer as up
from utils.utils_logger import logger
from kafka.errors import KafkaError


def generate_message():
    """
    Generator that reads CSV rows and yields JSON messages
    with a timestamp per row.
    """
    try:
        with open(uc.DATA_FILE, "r", encoding="utf-8") as csv_file:
            reader = csv.DictReader(csv_file)

            # Start timestamp for transactions
            current_time = datetime(2024, 1, 1, 0, 0, 0)
            time_increment = timedelta(seconds=10)  # increment per row

            for row in reader:
                # Build message dictionary
                try:
                    message = {
                        "transaction_time": current_time.isoformat(),
                        "type_transaction": row.get("type"),
                        "amount": float(row.get("amount", 0)),
                        "nameOrig": row.get("nameOrig"),
                        "oldbalanceOrg": float(row.get("oldbalanceOrg", 0)),
                        "newbalanceOrig": float(row.get("newbalanceOrig", 0)),
                        "nameDest": row.get("nameDest"),
                        "oldbalanceDest": float(row.get("oldbalanceDest", 0)),
                        "newbalanceDest": float(row.get("newbalanceDest", 0)),
                        "isFraud": int(row.get("isFraud", 0)),
                        "isFlaggedFraud": int(row.get("isFlaggedFraud", 0))
                    }
                except ValueError as ve:
                    logger.warning(f"Skipping row due to conversion error: {row} | {ve}")
                    continue

                yield message
                current_time += time_increment

    except FileNotFoundError:
        logger.error(f"File not found: {uc.DATA_FILE}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error in message generation: {e}")
        sys.exit(3)


def main(fast_mode=True):
    """
    Main producer loop writing CSV rows to Kafka Topic.
    """
    logger.info("Starting kafka producer")
    uc.verify_kafka_service()
    producer = up.create_kafka_producer()
    kafka_topic = uc.get_kafka_topic()


    if not uc.DATA_FILE.exists():
        logger.error(f"{uc.DATA_FILE} does not exist")
        sys.exit(1)

    try:
        for message in generate_message():
            producer.send(kafka_topic,message)
            logger.info(f"{message}")
            if not fast_mode:
                time.sleep(uc.get_message_interval_in_seconds())
            
        producer.flush()
        logger.info("All messages sent successfully")

    except KafkaError as e:
        logger.error(f"Kafka error while sending message:{e}")
    except KeyboardInterrupt:
        logger.warning("Producer interrupted by user")
    except Exception as e:
        logger.error(f"Unidentified error: {e}")
    finally:
        producer.close()
        logger.info(f"Kafka producer closed")


if __name__ == "__main__":
    main()
