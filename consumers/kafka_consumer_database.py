"""kafka_consumer_database.py
This python module reads data from kafka topic and 
then push a refined version of the transaction to a database in postgreSQL.
Fiels sent to database include;
1. Type of transaction.
2. Transaction amount.
3. An indicator if the transaction was fraud.
4. Transaction time."""
################
# IMPORTS
#################
import json
import os
from datetime import datetime
import psycopg2
from kafka import KafkaConsumer
from utils.utils_logger import logger
import utils.utils_config as uc

##################
# DATABASE SETUP
##################

pg_config = uc.get_postgres_config()

TABLE_CREATION_QUERY = """
CREATE TABLE IF NOT EXISTS transactions (
    id SERIAL PRIMARY KEY,
    type_transaction TEXT,
    amount NUMERIC,
    is_fraud BOOLEAN,
    transaction_time TIMESTAMP
)
"""
#####################################
# INITIALIZE DATABASE
#####################################
def init_db():
    conn = psycopg2.connect(**pg_config)
    with conn:
        with conn.cursor() as cur:
            cur.execute(TABLE_CREATION_QUERY)
    return conn

#####################################
# PROCESS MESSAGE
#####################################

def process_message(conn, message: str):
    try:
        message_dict = json.loads(message)
        tx_type = message_dict.get("type_transaction", "unknown")
        amount = float(message_dict.get("amount", 0))
        is_fraud = bool(message_dict.get("isFraud", 0))
        tx_time_str = message_dict.get("transaction_time")
        tx_time = datetime.fromisoformat(tx_time_str) if tx_time_str else None

        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO transactions (type_transaction, amount, is_fraud, transaction_time)
                VALUES (%s, %s, %s, %s)
                """,
                (tx_type, amount, is_fraud, tx_time)
            )
        conn.commit()

        logger.info(f"Inserted transaction: {tx_type}, amount: {amount}, fraud: {is_fraud}, time: {tx_time}")

    except json.JSONDecodeError:
        logger.error(f"Invalid JSON: {message}")
    except Exception as e:
        logger.error(f"Error processing message: {e}")

#####################################
# MAIN LOOP
#####################################

def main():
    logger.info("Starting Kafka transaction consumer with PostgreSQL storage")
    conn = init_db()

    kafka_bootstrap_servers = uc.get_bootstrapserver_address()
    kafka_topic = uc.get_kafka_topic()
    kafka_group = uc.kafka_consumer_group_id()

    consumer = KafkaConsumer(
        kafka_topic,
        bootstrap_servers=kafka_bootstrap_servers,
        group_id=kafka_group,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda m: m.decode("utf-8")
    )

    try:
        for message in consumer:
            process_message(conn, message.value)

    except KeyboardInterrupt:
        logger.info("Consumer stopped by user")
    finally:
        consumer.close()
        conn.close()

#####################################
# MODULE EXECUTION POINT
#####################################

if __name__ == "__main__":
    main()
