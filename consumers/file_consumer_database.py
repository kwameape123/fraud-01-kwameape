"""file_consumer_database.py
This python module reads data from mobile_money_transactions_live.json and 
then push a refined version of the transaction to a database in postgreSQL.
Fiels sent to database include;
1. Type of transaction.
2. Transaction amount.
3. An indicator if the transaction was fraud.
4. Transaction time."""

###############
# IMPORTS
###############
import json
import os
import time
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_values
from utils.utils_logger import logger
import utils.utils_config as uc

#####################################
# DATABASE SETUP
#####################################

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
########################
# INITIALIZE THE DATABASE
#########################
def init_db():
    conn = psycopg2.connect(**pg_config)
    with conn:
        with conn.cursor() as cur:
            cur.execute(TABLE_CREATION_QUERY)
    return conn

#####################################
# PROCESS MESSAGE FUNCTION
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
        logger.info(f"Inserted transaction: {tx_type}, {amount}, {is_fraud}, {tx_time}")
    except json.JSONDecodeError:
        logger.error(f"Invalid JSON: {message}")
    except Exception as e:
        logger.error(f"Error processing message: {e}")

#####################################
# NON-BLOCKING TAIL-LIKE FUNCTION
#####################################

def tail_f_nonblocking(file):
    """
    function that reads new lines from a file without blocking indefinitely.
    """
    file.seek(0, os.SEEK_END)  # Start at the end for new messages
    while True:
        message = file.readline()
        if message:
            yield message
        else:
            time.sleep(0.5)  # wait time before checking again

#####################################
# MAIN LOOP OR MAIN FUNCTION
#####################################

def main():
    logger.info("Starting transaction consumer with PostgreSQL storage")

    if not uc.JSON_FILE.exists():
        logger.error(f"{uc.JSON_FILE} does not exist")
        return

    conn = init_db()

    try:
        with open(uc.JSON_FILE, "r", encoding="utf-8") as f:
            logger.info("Consumer ready, processing existing messages first...")

            # Process any existing messages
            for message in f:
                if message.strip():
                    process_message(conn, message)

            logger.info("Now monitoring for new messages...")

            # Tail for new messages without blocking indefinitely
            for message in tail_f_nonblocking(f):
                if message.strip():
                    process_message(conn, message)

    except KeyboardInterrupt:
        logger.info("Consumer interrupted by user")
    except Exception as e:
        logger.error(f"Error in consumer: {e}")
    finally:
        conn.close()
        logger.info("Database connection closed.")

#####################################
# MODULE EXECUTION POINT
#####################################

if __name__ == "__main__":
    main()
