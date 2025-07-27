import os

from dotenv import load_dotenv
from postgresql_client import PostgresSQLClient

load_dotenv()


def main():
    pc = PostgresSQLClient(
        database=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )

    # Create transactions table
    create_table_query = """
        CREATE TABLE IF NOT EXISTS transactions (
            transaction_id VARCHAR PRIMARY KEY,
            user_id INT CHECK (user_id >= 1000 AND user_id <= 9999),
            amount NUMERIC CHECK (amount >= 0.01 AND amount <= 100000),
            currency CHAR(3) CHECK (currency ~ '^[A-Z]{3}$'),
            merchant TEXT,
            timestamp TIMESTAMP,
            location CHAR(2) CHECK (location ~ '^[A-Z]{2}$'),
            is_fraud INT CHECK (is_fraud IN (0, 1))
        );
    """
    try:
        pc.execute_query(create_table_query)
        print("Created 'transactions' table successfully.")
    except Exception as e:
        print(f"Failed to create table with error: {e}")


if __name__ == "__main__":
    main()
