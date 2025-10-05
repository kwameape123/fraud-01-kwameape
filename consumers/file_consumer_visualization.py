"""
Consumer that reads JSON messages from the producer in real-time
and updates live visualizations:
1. Transaction counts by type
2. Fraud counts
3. Hourly transaction amounts and revenue (2%)
"""

import json
import os
import time
from collections import defaultdict
from datetime import datetime

import matplotlib.pyplot as plt
from utils.utils_logger import logger
import utils.utils_config as uc

#####################################
# Data Structures
#####################################

transaction_type_counts = defaultdict(int)
fraud_counts = defaultdict(int)

# Track hourly amounts and revenue
hourly_amounts = defaultdict(float)
hourly_revenue = defaultdict(float)  # 2% of hourly sum

#####################################
# Setup live plot
#####################################

plt.ion()
fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(12, 8))

def update_chart():
    """Update all charts with the latest data."""
    # --- Transaction type counts ---
    ax1.clear()
    types = list(transaction_type_counts.keys())
    counts = list(transaction_type_counts.values())
    ax1.bar(types, counts, color="blue")
    ax1.set_title("Transaction Counts by Type")
    ax1.set_ylabel("Counts")
    ax1.set_xticks(range(len(types)))
    ax1.set_xticklabels(types, rotation=45, ha="right")

    # --- Fraud counts ---
    ax2.clear()
    fraud_labels = list(fraud_counts.keys())
    fraud_values = list(fraud_counts.values())
    ax2.bar(fraud_labels, fraud_values, color="red")
    ax2.set_title("Fraud vs Non-Fraud Counts")
    ax2.set_ylabel("Counts")
    ax2.set_xticks(range(len(fraud_labels)))
    ax2.set_xticklabels(fraud_labels, rotation=0)

    # --- Hourly amounts ---
    ax3.clear()
    if hourly_amounts:
        hours = sorted(hourly_amounts.keys())
        amounts = [hourly_amounts[h] for h in hours]
        ax3.plot(hours, amounts, marker='o', label='Hourly Amount')
        ax3.set_title("Hourly Transaction Amounts")
        ax3.set_xlabel("Hour")
        ax3.set_ylabel("Amount")
        ax3.grid(True)
        ax3.legend()
        plt.setp(ax3.get_xticklabels(), rotation=45, ha="right")

    # --- Hourly revenue (2%) ---
    ax4.clear()
    if hourly_revenue:
        hours = sorted(hourly_revenue.keys())
        revenue = [hourly_revenue[h] for h in hours]
        ax4.plot(hours, revenue, marker='o', color='green', label='Hourly Revenue (2%)')
        ax4.set_title("Hourly Revenue (2%)")
        ax4.set_xlabel("Hour")
        ax4.set_ylabel("Revenue")
        ax4.grid(True)
        ax4.legend()
        plt.setp(ax4.get_xticklabels(), rotation=45, ha="right")

    plt.tight_layout()
    plt.draw()
    plt.pause(0.01)

#####################################
# Process Message
#####################################

def process_message(message: str):
    try:
        message_dict = json.loads(message)

        # --- Transaction type counts ---
        tx_type = message_dict.get("type_transaction", "unknown")
        transaction_type_counts[tx_type] += 1

        # --- Fraud counts ---
        fraud = "Fraud" if message_dict.get("isFraud", 0) else "Legit"
        fraud_counts[fraud] += 1

        # --- Hourly amounts and revenue ---
        tx_time = message_dict.get("transaction_time")
        if tx_time:
            # Parse datetime (assumes string in ISO format)
            if isinstance(tx_time, str):
                dt = datetime.fromisoformat(tx_time)
            else:
                dt = tx_time  # if already datetime object
            hour_label = dt.strftime("%Y-%m-%d %H:00")  # Hour-level granularity
            amount = float(message_dict.get("amount", 0))
            hourly_amounts[hour_label] += amount
            hourly_revenue[hour_label] = hourly_amounts[hour_label] * 0.02

        update_chart()

    except json.JSONDecodeError:
        logger.error(f"Invalid JSON: {message}")
    except Exception as e:
        logger.error(f"Error processing message: {e}")

#####################################
# Tail-like function for real-time reading
#####################################

def tail_f(file):
    """Yield new lines as they are written to the file."""
    file.seek(0, os.SEEK_END)  # Start at the end
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
    logger.info("Starting transaction consumer with hourly tracking")

    if not uc.JSON_FILE.exists():
        logger.error(f"{uc.JSON_FILE} does not exist")
        return

    with open(uc.JSON_FILE, "r", encoding="utf-8") as f:
        print("Consumer ready, waiting for new messages...")

        try:
            with open(uc.JSON_FILE, "r", encoding="utf-8") as f:
    # Process all existing messages first
                for line in f:
                    if line.strip():
                        process_message(line)
    
    # Then tail for new messages
                for line in tail_f(f):
                    if line.strip():
                        process_message(line)
        except KeyboardInterrupt:
            logger.info("Consumer stopped by user")
        finally:
            plt.ioff()
            plt.show()

#####################################
# Entry Point
#####################################

if __name__ == "__main__":
    main()