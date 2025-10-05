"""
This producer generates messages from mobile money transaction logs
and writes them to a JSON file.
"""

#################
# IMPORTS
#################
import csv
import json
import random
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import utils.utils_config as uc
from utils.utils_logger import logger

#################
# FUNCTIONS
#################

def generate_message():
    """
    Generator function to read CSV and yield JSON-serializable messages
    with random transaction timestamps.
    """
    try:
        with open(uc.DATA_FILE, "r", encoding="utf-8") as csv_file:
            reader = csv.DictReader(csv_file)

            # Random timestamp mapping (1–743 steps)
            start = datetime(2024, 1, 1)
            mapping = {i: start + timedelta(seconds=random.randint(0, 100000)) for i in range(1, 744)}

            for row in reader:
                try:
                    step_key = int(row["step"])
                except (ValueError, KeyError):
                    logger.warning(f"Invalid or missing step in row: {row}")
                    continue

                ts = mapping.get(step_key)
                if ts is None:
                    logger.warning(f"Step {step_key} not in timestamp mapping, skipping row")
                    continue

                # Build message dictionary
                try:
                    message = {
                        "step": step_key,
                        "transaction_time": ts,
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
                    logger.warning(f"Error converting row to numeric types: {row} | {ve}")
                    continue

                yield message

    except FileNotFoundError:
        logger.error(f"File not found: {uc.DATA_FILE}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error in message generation: {e}")
        sys.exit(3)


#################
# MAIN FUNCTION
#################

def main(fast_mode=False):
    logger.info("Starting file producer")

    if not uc.DATA_FILE.exists():
        logger.error(f"{uc.DATA_FILE} does not exist")
        sys.exit(1)

    try:
        with open(uc.JSON_FILE, "w", encoding="utf-8") as json_file:
            for message in generate_message():
                logger.info(f"{message}")
                json.dump(message, json_file, default=str)
                json_file.write("\n")
                
                if not fast_mode:
                    time.sleep(uc.get_message_interval_in_seconds())

    except KeyboardInterrupt:
        logger.warning("Producer interrupted by user")
    except Exception as e:
        logger.error(f"Error in message production: {e}")


#################
# ENTRY POINT
#################

if __name__ == "__main__":
    # Set fast_mode=True for testing without sleep delays
    main(fast_mode=False)
