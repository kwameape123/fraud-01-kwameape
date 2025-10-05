"""
Kafka consumer for mobile money transactions.
Reads messages in real-time and updates live visualizations:
1. Transaction counts by type
2. Fraud counts
3. Hourly transaction amounts
4. Hourly revenue (2%)
"""

import json
from collections import defaultdict
from datetime import datetime
import matplotlib.pyplot as plt
from kafka import KafkaConsumer
from utils.utils_logger import logger
import utils.utils_config as uc

#####################################
# Data Structures
#####################################

transaction_type_counts = defaultdict(int)
fraud_counts = defaultdict(int)
hourly_amounts = defaultdict(float)
hourly_revenue = defaultdict(float)

#####################################
# Setup live plots
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
    ax1.set_xticklabels(types, rotation=45, ha="right")

    # --- Fraud counts ---
    ax2.clear()
    fraud_labels = list(fraud_counts.keys())
    fraud_values = list(fraud_counts.values())
    ax2.bar(fraud_labels, fraud_values, color="red")
    ax2.set_title("Fraud vs Non-Fraud Counts")
    ax2.set_ylabel("Counts")
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
        ax3.tick_params(axis='x', rotation=45)

    # --- Hourly revenue ---
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
        ax4.tick_params(axis='x', rotation=45)

    plt.tight_layout()
    plt.draw()
    plt.pause(0.01)

#####################################
# Process message
#####################################

def process_message(message_dict):
    """Process a single transaction message from Kafka."""
    try:
        # Transaction type
        tx_type = message_dict.get("type_transaction", "unknown")
        transaction_type_counts[tx_type] += 1

        # Fraud counts
        fraud = "Fraud" if message_dict.get("isFraud", 0) else "Legit"
        fraud_counts[fraud] += 1

        # Hourly amounts and revenue
        tx_time = message_dict.get("transaction_time")
        if tx_time:
            dt = datetime.fromisoformat(tx_time)
            hour_label = dt.strftime("%Y-%m-%d %H:00")
            amount = float(message_dict.get("amount", 0))
            hourly_amounts[hour_label] += amount
            hourly_revenue[hour_label] = hourly_amounts[hour_label] * 0.02

        update_chart()

    except Exception as e:
        logger.error(f"Error processing message: {e}")

#####################################
# Main Kafka consumer loop
#####################################

def main():
    logger.info("Starting Kafka transaction consumer")

    consumer = KafkaConsumer(
        uc.get_kafka_topic(),               # topic name
        bootstrap_servers=uc.KAFKA_BROKER, # e.g., 'localhost:9092'
        auto_offset_reset='latest',
        enable_auto_commit=True,
        group_id='transaction_consumer_group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    print("Kafka consumer ready, waiting for messages...")

    try:
        for msg in consumer:
            process_message(msg.value)

    except KeyboardInterrupt:
        logger.info("Consumer stopped by user")
    finally:
        plt.ioff()
        plt.show()
        consumer.close()

#####################################
# Entry point
#####################################

if __name__ == "__main__":
    main()
