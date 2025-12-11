#!/usr/bin/env python3
"""
Enterprise Banking - Transaction Data Generator

Generates realistic synthetic transaction data including:
- Deposits (cash, check, ACH, wire)
- Withdrawals (ATM, teller, transfer)
- Transfers (internal, external, wire)
- Payments (bill pay, P2P, merchant)
- Purchases (debit card, credit card)
- Fees and interest

Includes realistic patterns:
- Time-of-day distributions
- Day-of-week patterns
- Merchant categories
- Geographic patterns
- Fraud indicators
"""

import csv
import random
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple
import os
import hashlib

# Transaction types with realistic distributions
TRANSACTION_TYPES = {
    "Deposit": {
        "subtypes": ["Cash Deposit", "Check Deposit", "ACH Credit", "Wire Transfer In", "Mobile Deposit"],
        "amount_range": (10, 50000),
        "weight": 0.15
    },
    "Withdrawal": {
        "subtypes": ["ATM Withdrawal", "Teller Withdrawal", "ACH Debit", "Wire Transfer Out"],
        "amount_range": (20, 10000),
        "weight": 0.12
    },
    "Transfer": {
        "subtypes": ["Internal Transfer", "External Transfer", "Wire Transfer", "Zelle", "Venmo"],
        "amount_range": (1, 25000),
        "weight": 0.18
    },
    "Payment": {
        "subtypes": ["Bill Pay", "Loan Payment", "Credit Card Payment", "Utility Payment", "Insurance Payment"],
        "amount_range": (25, 5000),
        "weight": 0.15
    },
    "Purchase": {
        "subtypes": ["POS Debit", "Online Purchase", "Recurring Charge", "Subscription"],
        "amount_range": (1, 2000),
        "weight": 0.30
    },
    "Fee": {
        "subtypes": ["Monthly Fee", "ATM Fee", "Overdraft Fee", "Wire Fee", "Foreign Transaction Fee"],
        "amount_range": (2, 50),
        "weight": 0.05
    },
    "Interest": {
        "subtypes": ["Interest Earned", "Interest Charged"],
        "amount_range": (0.01, 1000),
        "weight": 0.03
    },
    "Refund": {
        "subtypes": ["Purchase Refund", "Fee Refund", "Dispute Credit"],
        "amount_range": (5, 500),
        "weight": 0.02
    }
}

# Merchant Category Codes (MCC) with realistic distributions
MERCHANT_CATEGORIES = {
    "5411": {"name": "Grocery Stores", "weight": 0.15, "avg_amount": 85},
    "5541": {"name": "Gas Stations", "weight": 0.12, "avg_amount": 45},
    "5812": {"name": "Restaurants", "weight": 0.12, "avg_amount": 35},
    "5814": {"name": "Fast Food", "weight": 0.08, "avg_amount": 12},
    "5912": {"name": "Drug Stores", "weight": 0.06, "avg_amount": 28},
    "5311": {"name": "Department Stores", "weight": 0.05, "avg_amount": 75},
    "5999": {"name": "Miscellaneous Retail", "weight": 0.05, "avg_amount": 50},
    "5942": {"name": "Book Stores", "weight": 0.02, "avg_amount": 25},
    "7832": {"name": "Movie Theaters", "weight": 0.02, "avg_amount": 30},
    "7011": {"name": "Hotels", "weight": 0.03, "avg_amount": 180},
    "4511": {"name": "Airlines", "weight": 0.02, "avg_amount": 350},
    "5732": {"name": "Electronics Stores", "weight": 0.03, "avg_amount": 150},
    "5651": {"name": "Clothing Stores", "weight": 0.04, "avg_amount": 65},
    "5691": {"name": "Mens/Womens Clothing", "weight": 0.03, "avg_amount": 85},
    "5947": {"name": "Gift Shops", "weight": 0.02, "avg_amount": 40},
    "8011": {"name": "Medical Services", "weight": 0.04, "avg_amount": 125},
    "8021": {"name": "Dental Services", "weight": 0.02, "avg_amount": 200},
    "8099": {"name": "Health Services", "weight": 0.02, "avg_amount": 100},
    "4121": {"name": "Taxi/Rideshare", "weight": 0.03, "avg_amount": 25},
    "5691": {"name": "Online Retail", "weight": 0.06, "avg_amount": 55},
}

# Common merchant names by category
MERCHANT_NAMES = {
    "5411": ["Kroger", "Safeway", "Whole Foods", "Trader Joe's", "Walmart Grocery", "Target", "Publix", "Costco", "Aldi", "H-E-B"],
    "5541": ["Shell", "Chevron", "ExxonMobil", "BP", "76", "Costco Gas", "QuikTrip", "Wawa", "Sheetz", "Circle K"],
    "5812": ["Olive Garden", "Chili's", "Applebee's", "Red Lobster", "Outback", "Texas Roadhouse", "The Cheesecake Factory", "PF Chang's"],
    "5814": ["McDonald's", "Starbucks", "Subway", "Chick-fil-A", "Taco Bell", "Wendy's", "Burger King", "Chipotle", "Dunkin'", "Panera"],
    "5912": ["CVS Pharmacy", "Walgreens", "Rite Aid", "Duane Reade"],
    "5311": ["Macy's", "Nordstrom", "JCPenney", "Kohl's", "Dillard's", "Belk"],
    "5732": ["Best Buy", "Apple Store", "Microsoft Store", "Micro Center"],
    "5651": ["Old Navy", "Gap", "H&M", "Zara", "Forever 21", "TJ Maxx", "Ross", "Marshalls"],
    "7011": ["Marriott", "Hilton", "Hyatt", "IHG", "Best Western", "Hampton Inn", "Holiday Inn", "Courtyard"],
    "4511": ["Delta Airlines", "United Airlines", "American Airlines", "Southwest", "JetBlue", "Alaska Airlines"],
    "4121": ["Uber", "Lyft", "Yellow Cab", "City Taxi"],
}

# Bill pay categories
BILL_PAY_MERCHANTS = [
    {"name": "AT&T", "type": "Telecom", "typical_amount": (50, 150)},
    {"name": "Verizon", "type": "Telecom", "typical_amount": (60, 180)},
    {"name": "T-Mobile", "type": "Telecom", "typical_amount": (40, 120)},
    {"name": "Comcast Xfinity", "type": "Cable/Internet", "typical_amount": (80, 250)},
    {"name": "Spectrum", "type": "Cable/Internet", "typical_amount": (70, 200)},
    {"name": "Duke Energy", "type": "Electric", "typical_amount": (80, 300)},
    {"name": "PG&E", "type": "Electric", "typical_amount": (100, 400)},
    {"name": "National Grid", "type": "Gas/Electric", "typical_amount": (60, 250)},
    {"name": "Water Utility", "type": "Water", "typical_amount": (30, 100)},
    {"name": "State Farm", "type": "Insurance", "typical_amount": (100, 300)},
    {"name": "GEICO", "type": "Insurance", "typical_amount": (80, 250)},
    {"name": "Progressive", "type": "Insurance", "typical_amount": (90, 280)},
    {"name": "Netflix", "type": "Subscription", "typical_amount": (10, 25)},
    {"name": "Spotify", "type": "Subscription", "typical_amount": (10, 16)},
    {"name": "Amazon Prime", "type": "Subscription", "typical_amount": (14, 15)},
    {"name": "Gym Membership", "type": "Subscription", "typical_amount": (20, 80)},
]

# Transaction status
TRANSACTION_STATUS = {
    "Completed": 0.92,
    "Pending": 0.04,
    "Failed": 0.02,
    "Reversed": 0.01,
    "Disputed": 0.01
}

# Fraud indicators
FRAUD_INDICATORS = [
    "Unusual_Location",
    "Velocity_Spike",
    "Unusual_Amount",
    "Unusual_Time",
    "New_Merchant",
    "International",
    "High_Risk_MCC",
    "Card_Not_Present",
    "Multiple_Declines",
]


def weighted_choice(choices: Dict[str, float]) -> str:
    """Select from weighted choices."""
    items = list(choices.keys())
    weights = list(choices.values())
    return random.choices(items, weights=weights)[0]


def generate_transaction_id() -> str:
    """Generate unique transaction ID."""
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    random_part = random.randint(100000, 999999)
    return f"TXN-{timestamp}-{random_part}"


def generate_reference_number() -> str:
    """Generate bank reference number."""
    return f"{random.randint(100000000000, 999999999999)}"


def generate_transaction_datetime(days_back: int = 365) -> Tuple[str, str]:
    """Generate realistic transaction datetime with time-of-day patterns."""
    # Random day within range
    days_ago = random.randint(0, days_back)
    base_date = datetime.now() - timedelta(days=days_ago)

    # Time of day distribution (weighted towards business hours and evening)
    hour_weights = [
        0.5, 0.3, 0.2, 0.2, 0.3, 0.5,  # 12am-6am (low)
        1.0, 2.0, 3.0, 3.0, 3.5, 4.0,  # 6am-12pm (morning ramp)
        4.0, 3.5, 3.0, 3.0, 3.5, 4.0,  # 12pm-6pm (afternoon)
        4.5, 4.0, 3.0, 2.0, 1.5, 1.0   # 6pm-12am (evening decline)
    ]
    hour = random.choices(range(24), weights=hour_weights)[0]
    minute = random.randint(0, 59)
    second = random.randint(0, 59)

    dt = base_date.replace(hour=hour, minute=minute, second=second)

    return dt.strftime("%Y-%m-%d"), dt.strftime("%H:%M:%S")


def generate_merchant_info(mcc: str = None) -> Dict[str, str]:
    """Generate merchant information."""
    if mcc is None:
        mcc_weights = {k: v["weight"] for k, v in MERCHANT_CATEGORIES.items()}
        mcc = weighted_choice(mcc_weights)

    category = MERCHANT_CATEGORIES.get(mcc, {"name": "Other", "avg_amount": 50})

    # Get merchant name
    if mcc in MERCHANT_NAMES:
        merchant_name = random.choice(MERCHANT_NAMES[mcc])
    else:
        merchant_name = f"{category['name']} Merchant"

    # Location
    cities = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "San Francisco", "Dallas", "Miami"]
    states = ["NY", "CA", "IL", "TX", "AZ", "CA", "TX", "FL"]
    idx = random.randint(0, len(cities) - 1)

    return {
        "merchant_name": merchant_name,
        "merchant_category_code": mcc,
        "merchant_category": category["name"],
        "merchant_city": cities[idx],
        "merchant_state": states[idx],
        "merchant_country": "USA"
    }


def generate_amount(tx_type: str, subtype: str, merchant_info: Dict = None) -> float:
    """Generate realistic transaction amount."""
    config = TRANSACTION_TYPES.get(tx_type, {"amount_range": (1, 1000)})
    min_amt, max_amt = config["amount_range"]

    if tx_type == "Purchase" and merchant_info:
        mcc = merchant_info.get("merchant_category_code", "")
        if mcc in MERCHANT_CATEGORIES:
            avg = MERCHANT_CATEGORIES[mcc]["avg_amount"]
            # Normal distribution around average
            amount = random.gauss(avg, avg * 0.4)
            amount = max(1, min(max_amt, amount))
            return round(amount, 2)

    if tx_type == "Fee":
        # Fees are usually specific amounts
        if "ATM" in subtype:
            return random.choice([2.50, 3.00, 3.50])
        elif "Overdraft" in subtype:
            return random.choice([34.00, 35.00, 36.00])
        elif "Wire" in subtype:
            return random.choice([25.00, 30.00, 35.00, 45.00])
        elif "Monthly" in subtype:
            return random.choice([0, 10.00, 12.00, 15.00, 25.00])

    if tx_type == "Payment":
        if "Utility" in subtype:
            return round(random.uniform(50, 300), 2)
        elif "Insurance" in subtype:
            return round(random.uniform(100, 400), 2)
        elif "Loan" in subtype or "Credit Card" in subtype:
            return round(random.uniform(100, 2000), 2)

    # Default: log-normal distribution for realistic spread
    mean = (min_amt + max_amt) / 4
    amount = random.lognormvariate(4, 1)
    amount = max(min_amt, min(max_amt, amount))

    # Round to realistic cents
    if amount > 100:
        return round(amount, 0)
    elif amount > 10:
        return round(amount, 2)
    else:
        return round(amount, 2)


def calculate_risk_score(
    amount: float,
    tx_type: str,
    hour: int,
    is_international: bool = False
) -> Tuple[int, List[str]]:
    """Calculate transaction risk score and flags."""
    risk_score = 0
    flags = []

    # Amount-based risk
    if amount > 5000:
        risk_score += 20
        flags.append("High_Amount")
    elif amount > 10000:
        risk_score += 40
        flags.append("Very_High_Amount")

    # Time-based risk
    if hour < 6 or hour > 23:
        risk_score += 15
        flags.append("Unusual_Time")

    # Transaction type risk
    if tx_type == "Transfer" and amount > 5000:
        risk_score += 10
    if tx_type == "Withdrawal" and amount > 3000:
        risk_score += 10

    # International
    if is_international:
        risk_score += 25
        flags.append("International")

    # Random unusual patterns (small percentage)
    if random.random() < 0.02:
        risk_score += 30
        flags.append("Velocity_Spike")

    if random.random() < 0.01:
        risk_score += 40
        flags.append("Suspicious_Pattern")

    return min(100, risk_score), flags


def generate_transaction(
    tx_id: int,
    account_id: str,
    account_type: str,
    days_back: int = 365
) -> Dict[str, Any]:
    """Generate a single transaction."""

    # Select transaction type (weight by account type)
    if account_type == "CreditCard":
        type_weights = {"Purchase": 0.60, "Payment": 0.25, "Fee": 0.05, "Interest": 0.05, "Refund": 0.05}
    elif account_type == "Checking":
        type_weights = {k: v["weight"] for k, v in TRANSACTION_TYPES.items()}
    elif account_type == "Savings":
        type_weights = {"Deposit": 0.35, "Withdrawal": 0.20, "Transfer": 0.30, "Interest": 0.10, "Fee": 0.05}
    else:
        type_weights = {"Deposit": 0.30, "Withdrawal": 0.20, "Transfer": 0.25, "Payment": 0.15, "Fee": 0.05, "Interest": 0.05}

    tx_type = weighted_choice(type_weights)
    config = TRANSACTION_TYPES[tx_type]
    subtype = random.choice(config["subtypes"])

    # Generate datetime
    date, time = generate_transaction_datetime(days_back)
    hour = int(time.split(":")[0])

    # Merchant info for purchases
    merchant_info = generate_merchant_info() if tx_type == "Purchase" else {}

    # Amount
    amount = generate_amount(tx_type, subtype, merchant_info)

    # Determine if debit or credit
    is_credit = tx_type in ["Deposit", "Refund", "Interest"] and "Charged" not in subtype
    if tx_type == "Interest" and "Charged" in subtype:
        is_credit = False

    # Risk assessment
    is_international = random.random() < 0.03
    risk_score, risk_flags = calculate_risk_score(amount, tx_type, hour, is_international)

    # Status
    status = weighted_choice(TRANSACTION_STATUS)

    # Balance after (simplified - in real system would track running balance)
    balance_after = round(random.uniform(100, 50000), 2)

    transaction = {
        "transaction_id": f"TXN-{tx_id:012d}",
        "account_id": account_id,
        "transaction_type": tx_type,
        "transaction_subtype": subtype,
        "amount": amount,
        "currency": "USD",
        "is_credit": "Y" if is_credit else "N",
        "transaction_date": date,
        "transaction_time": time,
        "post_date": date,  # Simplified - same as transaction date
        "status": status,
        "reference_number": generate_reference_number(),
        "description": f"{subtype} - {merchant_info.get('merchant_name', 'Bank Transaction')}",
        "merchant_name": merchant_info.get("merchant_name", ""),
        "merchant_category_code": merchant_info.get("merchant_category_code", ""),
        "merchant_category": merchant_info.get("merchant_category", ""),
        "merchant_city": merchant_info.get("merchant_city", ""),
        "merchant_state": merchant_info.get("merchant_state", ""),
        "merchant_country": merchant_info.get("merchant_country", "USA"),
        "is_international": "Y" if is_international else "N",
        "channel": random.choice(["Branch", "ATM", "Online", "Mobile", "Phone", "Wire"]),
        "balance_after": balance_after,
        "risk_score": risk_score,
        "risk_flags": "|".join(risk_flags) if risk_flags else "",
        "is_flagged": "Y" if risk_score > 50 else "N",
        "notes": ""
    }

    return transaction


def load_accounts(accounts_file: str) -> List[Dict[str, str]]:
    """Load account IDs and types from TSV file."""
    accounts = []
    if os.path.exists(accounts_file):
        with open(accounts_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            for row in reader:
                if row.get("status") == "Active":
                    accounts.append({
                        "account_id": row["account_id"],
                        "account_type": row["account_type"]
                    })
    return accounts


def generate_transactions(
    accounts_file: str = "../data/accounts.tsv",
    output_file: str = "../data/transactions.tsv",
    avg_transactions_per_account: int = 50,
    days_back: int = 365
) -> List[Dict[str, Any]]:
    """Generate transactions for all accounts."""

    accounts = load_accounts(accounts_file)

    if not accounts:
        print("Warning: No accounts found. Generating with placeholder IDs.")
        accounts = [{"account_id": f"ACC-{i:010d}", "account_type": "Checking"} for i in range(1, 101)]

    transactions = []
    tx_id = 1

    print(f"Generating transactions for {len(accounts)} accounts...")
    print(f"Average {avg_transactions_per_account} transactions per account over {days_back} days")

    for account in accounts:
        account_id = account["account_id"]
        account_type = account["account_type"]

        # Number of transactions varies by account type
        if account_type == "Checking":
            num_tx = int(random.gauss(avg_transactions_per_account * 1.5, avg_transactions_per_account * 0.3))
        elif account_type == "CreditCard":
            num_tx = int(random.gauss(avg_transactions_per_account * 1.2, avg_transactions_per_account * 0.3))
        elif account_type == "Savings":
            num_tx = int(random.gauss(avg_transactions_per_account * 0.3, avg_transactions_per_account * 0.1))
        else:
            num_tx = int(random.gauss(avg_transactions_per_account * 0.5, avg_transactions_per_account * 0.2))

        num_tx = max(1, num_tx)

        for _ in range(num_tx):
            tx = generate_transaction(tx_id, account_id, account_type, days_back)
            transactions.append(tx)
            tx_id += 1

        if tx_id % 10000 == 0:
            print(f"  Generated {tx_id} transactions...")

    # Sort by date/time
    transactions.sort(key=lambda x: (x["transaction_date"], x["transaction_time"]))

    # Write to TSV
    if transactions:
        fieldnames = list(transactions[0].keys())

        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter='\t')
            writer.writeheader()
            writer.writerows(transactions)

        print(f"\nSaved {len(transactions)} transactions to {output_file}")

        # Summary stats
        flagged = sum(1 for t in transactions if t["is_flagged"] == "Y")
        print(f"  Flagged transactions: {flagged} ({100*flagged/len(transactions):.2f}%)")

    return transactions


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Generate synthetic transaction data")
    parser.add_argument("--accounts", type=str, default="../data/accounts.tsv", help="Accounts TSV file")
    parser.add_argument("--output", type=str, default="../data/transactions.tsv", help="Output file")
    parser.add_argument("--avg-transactions", type=int, default=50, help="Avg transactions per account")
    parser.add_argument("--days", type=int, default=365, help="Days of history to generate")
    parser.add_argument("--seed", type=int, default=42, help="Random seed")

    args = parser.parse_args()

    random.seed(args.seed)

    generate_transactions(
        accounts_file=args.accounts,
        output_file=args.output,
        avg_transactions_per_account=args.avg_transactions,
        days_back=args.days
    )
