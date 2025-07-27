import json
import logging
import os
import random
import time
import signal
from typing import Dict, Any, Optional
from datetime import datetime, timezone, timedelta
from random import randint

from dotenv import load_dotenv
from faker import Faker
from jsonschema import validate, ValidationError, FormatChecker
from postgresql_client import PostgresSQLClient

# Configure logging
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(module)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Faker instance for generating random data
fake = Faker()

# JSON Schema for transaction validation
TRANSACTION_SCHEMA = {
    "type": "object",
    "properties": {
        "transaction_id": {"type": "string"},
        "user_id": {"type": "number", "minimum": 1000, "maximum": 9999},
        "amount": {"type": "number", "minimum": 0.01, "maximum": 100000},
        "currency": {"type": "string", "pattern": "^[A-Z]{3}$"},
        "merchant": {"type": "string"},
        "timestamp": {"type": "string", "format": "date-time"},
        "location": {"type": "string", "pattern": "^[A-Z]{2}$"},
        "is_fraud": {"type": "integer", "minimum": 0, "maximum": 1}
    },
    "required": ["transaction_id", "user_id", "amount", "currency", "timestamp", 'is_fraud']
}


class TransactionProducer:
    def __init__(self):
        self.running = False
        self.table = "transactions"

        self.db = PostgresSQLClient(
            database=os.getenv("POSTGRES_DB"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
        )

        self.compromised_users = set(random.sample(range(1000, 9999), 50))
        self.high_risk_merchants = ['QuickCash', 'GlobalDigital', 'FastMoneyX']
        self.fraud_pattern_weights = {
            'account_takeover': 0.4,
            'card_testing': 0.3,
            'merchant_collusion': 0.2,
            'geo_anomaly': 0.1
        }

        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)
        logger.info("TransactionProducer initialized (PostgreSQL mode).")

    def validate_transaction(self, transaction: Dict[str, Any]) -> bool:
        try:
            validate(
                instance=transaction,
                schema=TRANSACTION_SCHEMA,
                format_checker=FormatChecker()
            )
            return True
        except ValidationError as e:
            logger.error(f"Invalid transaction: {e.message}")
            return False

    def generate_transaction(self) -> Optional[Dict[str, Any]]:
        transaction = {
            "transaction_id": fake.uuid4(),
            "user_id": randint(1000, 9999),
            "amount": round(fake.pyfloat(min_value=0.01, max_value=10000), 2),
            "currency": "USD",
            "merchant": fake.company(),
            "timestamp": (datetime.now(timezone.utc) +
                         timedelta(seconds=random.randint(-300, 300))).isoformat(),
            "location": fake.country_code(),
            "is_fraud": 0
        }

        is_fraud = 0
        amount = transaction['amount']
        user_id = transaction['user_id']
        merchant = transaction['merchant']

        if user_id in self.compromised_users and amount > 500:
            if random.random() < 0.3:
                is_fraud = 1
                transaction['amount'] = random.uniform(500, 5000)
                transaction['merchant'] = random.choice(self.high_risk_merchants)

        if not is_fraud and amount < 2.0:
            if user_id % 1000 == 0 and random.random() < 0.25:
                is_fraud = 1
                transaction['amount'] = round(random.uniform(0.01, 2.0), 2)
                transaction['location'] = 'US'

        if not is_fraud and merchant in self.high_risk_merchants:
            if amount > 300 and random.random() < 0.15:
                is_fraud = 1
                transaction['amount'] = random.uniform(300, 1500)

        if not is_fraud:
            if user_id % 500 == 0 and random.random() < 0.1:
                is_fraud = 1
                transaction['location'] = random.choice(['CN', 'RU', 'NG'])

        if not is_fraud and random.random() < 0.002:
            is_fraud = 1
            transaction['amount'] = random.uniform(100, 2000)

        transaction['is_fraud'] = is_fraud if random.random() < 0.985 else 0

        if self.validate_transaction(transaction):
            return transaction
        return None

    def insert_transaction(self, transaction: Dict[str, Any]) -> bool:
        query = f"""
            INSERT INTO {self.table} (
                transaction_id, user_id, amount, currency, merchant,
                timestamp, location, is_fraud
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        values = (
            transaction["transaction_id"],
            transaction["user_id"],
            transaction["amount"],
            transaction["currency"],
            transaction["merchant"],
            transaction["timestamp"],
            transaction["location"],
            transaction["is_fraud"]
        )
        try:
            conn = self.db.create_conn()
            cursor = conn.cursor()
            cursor.execute(query, values)
            conn.commit()
            cursor.close()
            conn.close()
            logger.info(f"Inserted transaction: {transaction['transaction_id']}")
            return True
        except Exception as e:
            logger.error(f"Failed to insert transaction: {e}")
            return False

    def run_continuous_production(self, interval: float = 0.0):
        self.running = True
        logger.info("Starting PostgreSQL transaction producer...")

        try:
            while self.running:
                transaction = self.generate_transaction()
                if transaction:
                    self.insert_transaction(transaction)
                    time.sleep(interval)
        finally:
            self.shutdown()

    def shutdown(self, signum=None, frame=None):
        if self.running:
            logger.info("Initiating shutdown...")
            self.running = False
            logger.info("Producer stopped")


if __name__ == "__main__":
    producer = TransactionProducer()
    producer.run_continuous_production()
