"""
Banking dummy data generator.
Generates realistic banking data for all 4 core entities.
Run this script to regenerate sample data for testing.

Usage:
    python data/sample/generate_data.py
"""

import uuid
import random
import json
import csv
from datetime import datetime, date, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from ingestion.scripts.logger import get_logger

logger = get_logger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────

FIRST_NAMES = [
    "James", "Mary", "John", "Patricia", "Robert", "Jennifer",
    "Michael", "Linda", "William", "Barbara", "David", "Elizabeth",
    "Richard", "Susan", "Joseph", "Jessica", "Thomas", "Sarah",
    "Charles", "Karen", "Akshay", "Priya", "Raj", "Anita",
    "Carlos", "Maria", "Wei", "Mei", "Ahmed", "Fatima"
]

LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia",
    "Miller", "Davis", "Wilson", "Anderson", "Taylor", "Thomas",
    "Patel", "Shah", "Kumar", "Singh", "Chen", "Wang",
    "Martinez", "Robinson", "Clark", "Rodriguez", "Lewis", "Lee"
]

STATES = [
    "CA", "NY", "TX", "FL", "IL", "PA", "OH",
    "GA", "NC", "MI", "NJ", "VA", "WA", "AZ"
]

CITIES = {
    "CA": "Los Angeles", "NY": "New York", "TX": "Houston",
    "FL": "Miami", "IL": "Chicago", "PA": "Philadelphia",
    "OH": "Columbus", "GA": "Atlanta", "NC": "Charlotte",
    "MI": "Detroit", "NJ": "Newark", "VA": "Richmond",
    "WA": "Seattle", "AZ": "Phoenix"
}

MERCHANTS = [
    ("Amazon", "RETAIL"), ("Walmart", "RETAIL"), ("Starbucks", "FOOD"),
    ("McDonald's", "FOOD"), ("Shell", "GAS"), ("CVS", "PHARMACY"),
    ("Home Depot", "HOME"), ("Netflix", "ENTERTAINMENT"),
    ("Uber", "TRANSPORT"), ("Delta Airlines", "TRAVEL"),
    ("Whole Foods", "GROCERY"), ("Target", "RETAIL"),
    ("Apple Store", "ELECTRONICS"), ("Chase ATM", "ATM"),
    ("IRS", "GOVERNMENT"), ("Electric Company", "UTILITIES")
]

CHANNELS = ["ATM", "ONLINE", "MOBILE", "BRANCH", "POS"]

FRAUD_REASONS = [
    "Unusual transaction amount",
    "Transaction in foreign country",
    "Multiple transactions in short time",
    "Transaction outside normal hours",
    "Card not present - high value",
    "Velocity check failed",
    "Blacklisted merchant",
    "Account takeover pattern detected"
]

BRANCH_CODES = ["BR001", "BR002", "BR003", "BR004", "BR005",
                "BR006", "BR007", "BR008", "BR009", "BR010"]

OUTPUT_DIR = Path("data/sample")


# ── Helpers ───────────────────────────────────────────────────────────────────

def random_date(start_year: int = 2020, end_year: int = 2024) -> date:
    start = date(start_year, 1, 1)
    end = date(end_year, 12, 31)
    delta = end - start
    return start + timedelta(days=random.randint(0, delta.days))


def random_datetime(start_year: int = 2023, end_year: int = 2024) -> datetime:
    d = random_date(start_year, end_year)
    return datetime(
        d.year, d.month, d.day,
        random.randint(0, 23),
        random.randint(0, 59),
        random.randint(0, 59)
    )


def generate_account_number() -> str:
    """Generate masked account number."""
    return f"****{random.randint(1000, 9999)}"


def generate_reference_number() -> str:
    return f"REF{random.randint(100000000, 999999999)}"


# ── Generators ────────────────────────────────────────────────────────────────

def generate_customers(n: int = 500) -> list[dict]:
    """Generate n customer records."""
    logger.info(f"Generating {n} customer records...")
    customers = []

    for _ in range(n):
        state = random.choice(STATES)
        dob = random_date(1950, 2000)
        customer = {
            "customer_id": f"CUST{str(uuid.uuid4()).replace('-', '')[:12].upper()}",
            "first_name": random.choice(FIRST_NAMES),
            "last_name": random.choice(LAST_NAMES),
            "email": "",
            "phone": f"+1{random.randint(2000000000, 9999999999)}",
            "date_of_birth": str(dob),
            "address_line1": f"{random.randint(1, 9999)} {random.choice(LAST_NAMES)} St",
            "address_line2": None,
            "city": CITIES[state],
            "state": state,
            "zip_code": f"{random.randint(10000, 99999)}",
            "country": "US",
            "customer_since": str(random_date(2010, 2023)),
            "status": random.choices(
                ["ACTIVE", "INACTIVE", "SUSPENDED"],
                weights=[85, 10, 5]
            )[0],
            "credit_score": random.randint(580, 850),
            "created_at": str(datetime.now(timezone.utc)),
            "updated_at": str(datetime.now(timezone.utc)),
        }
        # Set email after name is generated
        customer["email"] = (
            f"{customer['first_name'].lower()}."
            f"{customer['last_name'].lower()}"
            f"{random.randint(1, 999)}@email.com"
        )
        customers.append(customer)

    logger.success(f"Generated {len(customers)} customer records")
    return customers


def generate_accounts(customers: list[dict]) -> list[dict]:
    """Generate 1-3 accounts per customer."""
    logger.info("Generating account records...")
    accounts = []
    account_types = ["CHECKING", "SAVINGS", "LOAN", "CREDIT"]

    for customer in customers:
        num_accounts = random.randint(1, 3)
        selected_types = random.sample(account_types, num_accounts)

        for acc_type in selected_types:
            if acc_type == "CHECKING":
                balance = round(random.uniform(500, 50000), 2)
                rate = 0.01
            elif acc_type == "SAVINGS":
                balance = round(random.uniform(1000, 200000), 2)
                rate = round(random.uniform(0.03, 0.05), 4)
            elif acc_type == "LOAN":
                balance = round(random.uniform(-50000, -1000), 2)
                rate = round(random.uniform(0.04, 0.12), 4)
            else:  # CREDIT
                balance = round(random.uniform(-5000, 0), 2)
                rate = round(random.uniform(0.15, 0.29), 4)

            account = {
                "account_id": f"ACC{str(uuid.uuid4()).replace('-', '')[:12].upper()}",
                "customer_id": customer["customer_id"],
                "account_number": generate_account_number(),
                "account_type": acc_type,
                "balance": balance,
                "available_balance": balance * random.uniform(0.9, 1.0),
                "currency": "USD",
                "interest_rate": rate,
                "opened_date": str(random_date(2010, 2023)),
                "status": "ACTIVE" if customer["status"] == "ACTIVE" else "INACTIVE",
                "branch_code": random.choice(BRANCH_CODES),
                "created_at": str(datetime.now(timezone.utc)),
                "updated_at": str(datetime.now(timezone.utc)),
            }
            accounts.append(account)

    logger.success(f"Generated {len(accounts)} account records")
    return accounts


def generate_transactions(accounts: list[dict], n_per_account: int = 20) -> list[dict]:
    """Generate transactions for each account."""
    logger.info("Generating transaction records...")
    transactions = []

    # Only generate transactions for CHECKING and SAVINGS accounts
    active_accounts = [
        a for a in accounts
        if a["account_type"] in ("CHECKING", "SAVINGS")
        and a["status"] == "ACTIVE"
    ]

    for account in active_accounts:
        num_txns = random.randint(5, n_per_account)
        running_balance = float(account["balance"])

        for _ in range(num_txns):
            txn_type = random.choices(
                ["DEPOSIT", "WITHDRAWAL", "TRANSFER", "PAYMENT", "FEE"],
                weights=[25, 35, 20, 15, 5]
            )[0]

            if txn_type == "DEPOSIT":
                amount = round(random.uniform(100, 5000), 2)
            elif txn_type == "FEE":
                amount = round(random.uniform(5, 35), 2) * -1
            else:
                amount = round(random.uniform(10, 2000), 2) * -1

            running_balance += amount
            merchant = random.choice(MERCHANTS) if txn_type != "DEPOSIT" else None
            txn_date = random_datetime(2023, 2024)

            transaction = {
                "transaction_id": f"TXN{str(uuid.uuid4()).replace('-', '')[:14].upper()}",
                "account_id": account["account_id"],
                "customer_id": account["customer_id"],
                "transaction_type": txn_type,
                "amount": amount,
                "currency": "USD",
                "balance_after": round(running_balance, 2),
                "description": f"{txn_type} - {merchant[0] if merchant else 'Direct Deposit'}",
                "merchant_name": merchant[0] if merchant else None,
                "merchant_category": merchant[1] if merchant else None,
                "channel": random.choice(CHANNELS),
                "status": random.choices(
                    ["COMPLETED", "PENDING", "FAILED", "REVERSED"],
                    weights=[88, 7, 3, 2]
                )[0],
                "transaction_date": str(txn_date),
                "value_date": str(txn_date.date()),
                "reference_number": generate_reference_number(),
                "created_at": str(datetime.now(timezone.utc)),
            }
            transactions.append(transaction)

    logger.success(f"Generated {len(transactions)} transaction records")
    return transactions


def generate_fraud_flags(transactions: list[dict]) -> list[dict]:
    """Flag ~2% of transactions as potentially fraudulent."""
    logger.info("Generating fraud flag records...")
    flags = []

    # Flag high-value or suspicious transactions
    candidates = [
        t for t in transactions
        if abs(float(t["amount"])) > 1000
        or t["status"] == "FAILED"
    ]

    # Flag ~15% of candidates
    flagged = random.sample(candidates, int(len(candidates) * 0.15))

    for txn in flagged:
        severity = random.choices(
            ["LOW", "MEDIUM", "HIGH", "CRITICAL"],
            weights=[40, 35, 20, 5]
        )[0]

        fraud_score = {
            "LOW": round(random.uniform(0.3, 0.5), 4),
            "MEDIUM": round(random.uniform(0.5, 0.7), 4),
            "HIGH": round(random.uniform(0.7, 0.9), 4),
            "CRITICAL": round(random.uniform(0.9, 1.0), 4),
        }[severity]

        flag = {
            "flag_id": f"FLG{str(uuid.uuid4()).replace('-', '')[:12].upper()}",
            "transaction_id": txn["transaction_id"],
            "account_id": txn["account_id"],
            "customer_id": txn["customer_id"],
            "flag_reason": random.choice(FRAUD_REASONS),
            "severity": severity,
            "fraud_score": fraud_score,
            "is_confirmed_fraud": random.choices(
                [True, False], weights=[10, 90]
            )[0],
            "flagged_at": str(datetime.now(timezone.utc)),
            "reviewed_at": None,
            "reviewed_by": None,
            "created_at": str(datetime.now(timezone.utc)),
        }
        flags.append(flag)

    logger.success(f"Generated {len(flags)} fraud flag records")
    return flags


# ── Writers ───────────────────────────────────────────────────────────────────

def write_csv(data: list[dict], filename: str) -> None:
    """Write data to CSV file."""
    if not data:
        logger.warning(f"No data to write for {filename}")
        return

    filepath = OUTPUT_DIR / filename
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)

    logger.info(f"Written {len(data)} records to {filepath}")


def write_json(data: list[dict], filename: str) -> None:
    """Write data to JSON file."""
    filepath = OUTPUT_DIR / filename
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, default=str)
    logger.info(f"Written {len(data)} records to {filepath}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    logger.info("=" * 60)
    logger.info("Banking Data Platform — Sample Data Generator")
    logger.info("=" * 60)

    # Generate all datasets
    customers = generate_customers(n=500)
    accounts = generate_accounts(customers)
    transactions = generate_transactions(accounts, n_per_account=20)
    fraud_flags = generate_fraud_flags(transactions)

    # Write CSV files (simulates source system extracts)
    write_csv(customers, "customers.csv")
    write_csv(accounts, "accounts.csv")
    write_csv(transactions, "transactions.csv")
    write_csv(fraud_flags, "fraud_flags.csv")

    # Write JSON files (simulates API payloads)
    write_json(customers[:10], "customers_sample.json")
    write_json(transactions[:10], "transactions_sample.json")

    # Summary
    logger.info("=" * 60)
    logger.info("Data Generation Complete!")
    logger.info(f"  Customers:     {len(customers):,}")
    logger.info(f"  Accounts:      {len(accounts):,}")
    logger.info(f"  Transactions:  {len(transactions):,}")
    logger.info(f"  Fraud Flags:   {len(fraud_flags):,}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()