import json
import os
import time
from datetime import datetime

import psycopg2
from psycopg2.extras import execute_values
from utils.utils_logger import logger
import utils.utils_config as uc

#####################################
# Database Setup
#####################################

pg_config = uc.get_postgres_config()

# Initialize table if it doesn't exist
TABLE_CREATION_QUERY = """
CREATE TABLE IF NOT EXISTS transactions (
    id SERIAL PRIMARY KEY,
    type_transaction TEXT,
    amount NUMERIC,
    is_fraud BOOLEAN,
    transaction_time TIMESTAMP
)
"""

def init_db():
    conn = psycopg2.connect(**pg_config)
    with conn:
        with conn.cursor() as cur:
            cur.execute(TABLE_CREATION_QUERY)
    return conn

#####################################
# Process Message
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

    except json.JSONDecodeError:
        logger.error(f"Invalid JSON: {message}")
    except Exception as e:
        logger.error(f"Error processing message: {e}")

#####################################
# Tail-like function for real-time reading
#####################################

def tail_f(file):
    file.seek(0, os.SEEK_END)
    while True:
        line = file.readline()
        if not line:
            time.sleep(0.5)
            continue
        yield line

#####################################
# Main Loop
#####################################

def main():
    logger.info("Starting transaction consumer with PostgreSQL storage")

    if not uc.JSON_FILE.exists():
        logger.error(f"{uc.JSON_FILE} does not exist")
        return

    conn = init_db()

    try:
        with open(uc.JSON_FILE, "r", encoding="utf-8") as f:
            logger.info("Consumer ready, waiting for new messages...")

            # Process existing lines
            for line in f:
                if line.strip():
                    process_message(conn, line)

            # Tail for new messages
            for line in tail_f(f):
                if line.strip():
                    process_message(conn, line)

    except KeyboardInterrupt:
        logger.info("Consumer stopped by user")
    finally:
        conn.close()

#####################################
# Entry Point
#####################################

if __name__ == "__main__":
    main()
