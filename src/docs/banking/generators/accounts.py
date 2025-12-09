#!/usr/bin/env python3
"""
Enterprise Banking - Account Data Generator

Generates realistic synthetic account data including:
- Checking accounts (personal and business)
- Savings accounts
- Money market accounts
- Certificate of Deposit (CD) accounts
- Credit card accounts
- Loan accounts (mortgage, auto, personal)
- Investment accounts

Uses realistic banking patterns, interest rates, and account behaviors.
"""

import csv
import random
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import os

# Account type configurations with realistic attributes
ACCOUNT_TYPES = {
    "Checking": {
        "subtypes": ["Personal Checking", "Business Checking", "Student Checking", "Premium Checking"],
        "min_balance": 0,
        "max_balance": 50000,
        "interest_rate": (0.0, 0.01),
        "monthly_fee": [0, 0, 0, 12, 25],  # Weighted towards free
        "overdraft_limit": [0, 500, 1000, 2500],
        "weight": 0.35
    },
    "Savings": {
        "subtypes": ["Regular Savings", "High-Yield Savings", "Money Market", "Youth Savings"],
        "min_balance": 0,
        "max_balance": 250000,
        "interest_rate": (0.01, 0.05),
        "monthly_fee": [0, 0, 5, 10],
        "min_opening_deposit": [25, 100, 500, 1000],
        "weight": 0.25
    },
    "CD": {
        "subtypes": ["6-Month CD", "12-Month CD", "24-Month CD", "60-Month CD"],
        "min_balance": 1000,
        "max_balance": 500000,
        "interest_rate": (0.03, 0.055),
        "terms_months": [6, 12, 24, 36, 60],
        "early_withdrawal_penalty": 0.10,
        "weight": 0.08
    },
    "CreditCard": {
        "subtypes": ["Standard Card", "Rewards Card", "Premium Card", "Secured Card", "Business Card"],
        "min_balance": -50000,  # Credit (negative = owed)
        "max_balance": 0,
        "credit_limit": [1000, 5000, 10000, 25000, 50000, 100000],
        "interest_rate": (0.15, 0.27),  # APR
        "annual_fee": [0, 0, 0, 95, 250, 550],
        "weight": 0.15
    },
    "Mortgage": {
        "subtypes": ["30-Year Fixed", "15-Year Fixed", "ARM 5/1", "ARM 7/1", "Jumbo Loan"],
        "min_balance": 50000,
        "max_balance": 2000000,
        "interest_rate": (0.055, 0.08),
        "term_years": [15, 30],
        "weight": 0.07
    },
    "AutoLoan": {
        "subtypes": ["New Auto Loan", "Used Auto Loan", "Auto Refinance"],
        "min_balance": 5000,
        "max_balance": 100000,
        "interest_rate": (0.045, 0.12),
        "term_months": [36, 48, 60, 72, 84],
        "weight": 0.05
    },
    "PersonalLoan": {
        "subtypes": ["Unsecured Personal", "Secured Personal", "Debt Consolidation", "Home Improvement"],
        "min_balance": 1000,
        "max_balance": 50000,
        "interest_rate": (0.06, 0.25),
        "term_months": [12, 24, 36, 48, 60],
        "weight": 0.03
    },
    "Investment": {
        "subtypes": ["Brokerage", "IRA Traditional", "IRA Roth", "401k Rollover", "Trust Account"],
        "min_balance": 0,
        "max_balance": 5000000,
        "interest_rate": (0.0, 0.0),  # Market returns, not interest
        "annual_fee_percent": (0.0, 0.01),
        "weight": 0.02
    }
}

# Account status distribution
ACCOUNT_STATUS = {
    "Active": 0.88,
    "Dormant": 0.05,
    "Closed": 0.03,
    "Frozen": 0.02,
    "Pending": 0.02
}

# Currency codes (weighted towards USD)
CURRENCIES = {
    "USD": 0.95,
    "EUR": 0.02,
    "GBP": 0.01,
    "CAD": 0.01,
    "MXN": 0.01
}


def weighted_choice(choices: Dict[str, float]) -> str:
    """Select from weighted choices."""
    items = list(choices.keys())
    weights = list(choices.values())
    return random.choices(items, weights=weights)[0]


def generate_account_number(account_type: str, sequence: int) -> str:
    """Generate a realistic account number."""
    type_prefixes = {
        "Checking": "1",
        "Savings": "2",
        "CD": "3",
        "CreditCard": "4",
        "Mortgage": "5",
        "AutoLoan": "6",
        "PersonalLoan": "7",
        "Investment": "8"
    }
    prefix = type_prefixes.get(account_type, "9")
    return f"{prefix}{random.randint(100, 999)}{sequence:08d}"


def generate_routing_number() -> str:
    """Generate a valid-format routing number."""
    # Real banks have specific routing numbers; we generate realistic-looking ones
    fed_district = random.randint(1, 12)
    bank_id = random.randint(1000, 9999)
    check = random.randint(0, 9)
    return f"0{fed_district}{bank_id}{check:01d}"


def generate_open_date(max_years_ago: int = 20) -> str:
    """Generate account opening date."""
    days_ago = random.randint(1, max_years_ago * 365)
    open_date = datetime.now() - timedelta(days=days_ago)
    return open_date.strftime("%Y-%m-%d")


def generate_balance(account_type: str, config: Dict) -> float:
    """Generate a realistic balance based on account type."""
    min_bal = config.get("min_balance", 0)
    max_bal = config.get("max_balance", 100000)

    if account_type == "CreditCard":
        # Credit cards: most people carry some balance
        if random.random() < 0.3:
            return 0.0  # Paid off
        else:
            credit_limit = random.choice(config.get("credit_limit", [5000]))
            # Most people use 20-60% of credit
            utilization = random.uniform(0.1, 0.7)
            return round(-credit_limit * utilization, 2)

    elif account_type in ["Mortgage", "AutoLoan", "PersonalLoan"]:
        # Loans: balance decreases over time
        original_amount = random.uniform(min_bal, max_bal)
        # Assume somewhere in the middle of the loan
        remaining_percent = random.uniform(0.2, 0.95)
        return round(original_amount * remaining_percent, 2)

    elif account_type == "CD":
        # CDs: typically round amounts
        base_amounts = [1000, 5000, 10000, 25000, 50000, 100000, 250000]
        return float(random.choice([b for b in base_amounts if min_bal <= b <= max_bal]))

    elif account_type == "Investment":
        # Investment accounts: wider distribution
        if random.random() < 0.3:
            return round(random.uniform(0, 10000), 2)
        elif random.random() < 0.6:
            return round(random.uniform(10000, 100000), 2)
        else:
            return round(random.uniform(100000, max_bal), 2)

    else:
        # Checking/Savings: log-normal distribution (many small, few large)
        mean = (min_bal + max_bal) / 10
        balance = random.lognormvariate(8, 1.5)
        return round(min(max(balance, min_bal), max_bal), 2)


def generate_interest_rate(account_type: str, config: Dict, subtype: str) -> float:
    """Generate appropriate interest rate."""
    rate_range = config.get("interest_rate", (0.0, 0.05))

    if account_type == "CD":
        # Longer terms = higher rates
        term_bonus = {
            "6-Month CD": 0,
            "12-Month CD": 0.005,
            "24-Month CD": 0.01,
            "60-Month CD": 0.015
        }
        base_rate = random.uniform(rate_range[0], rate_range[1])
        return round(base_rate + term_bonus.get(subtype, 0), 4)

    elif account_type == "Savings":
        # High-yield savings have better rates
        if "High-Yield" in subtype:
            return round(random.uniform(0.04, 0.05), 4)
        elif "Money Market" in subtype:
            return round(random.uniform(0.03, 0.045), 4)
        else:
            return round(random.uniform(0.001, 0.02), 4)

    elif account_type == "CreditCard":
        # Credit card APR
        if "Secured" in subtype:
            return round(random.uniform(0.18, 0.24), 4)
        elif "Premium" in subtype:
            return round(random.uniform(0.15, 0.20), 4)
        else:
            return round(random.uniform(rate_range[0], rate_range[1]), 4)

    else:
        return round(random.uniform(rate_range[0], rate_range[1]), 4)


def calculate_monthly_payment(principal: float, annual_rate: float, term_months: int) -> float:
    """Calculate monthly payment for a loan."""
    if annual_rate == 0 or term_months == 0:
        return round(principal / max(term_months, 1), 2)

    monthly_rate = annual_rate / 12
    payment = principal * (monthly_rate * (1 + monthly_rate)**term_months) / ((1 + monthly_rate)**term_months - 1)
    return round(payment, 2)


def generate_account(
    account_id: int,
    customer_id: str,
    branch_id: str,
    account_type: Optional[str] = None
) -> Dict[str, Any]:
    """Generate a single account."""

    # Select account type if not specified
    if account_type is None:
        type_weights = {k: v["weight"] for k, v in ACCOUNT_TYPES.items()}
        account_type = weighted_choice(type_weights)

    config = ACCOUNT_TYPES[account_type]
    subtype = random.choice(config["subtypes"])

    account_number = generate_account_number(account_type, account_id)
    open_date = generate_open_date(15)
    balance = generate_balance(account_type, config)
    interest_rate = generate_interest_rate(account_type, config, subtype)
    status = weighted_choice(ACCOUNT_STATUS)
    currency = weighted_choice(CURRENCIES)

    account = {
        "account_id": f"ACC-{account_id:010d}",
        "account_number": account_number,
        "customer_id": customer_id,
        "branch_id": branch_id,
        "account_type": account_type,
        "account_subtype": subtype,
        "status": status,
        "currency": currency,
        "balance": balance,
        "available_balance": balance if account_type not in ["CreditCard"] else abs(balance) + random.choice(config.get("credit_limit", [5000])),
        "interest_rate": interest_rate,
        "open_date": open_date,
        "last_activity_date": (datetime.now() - timedelta(days=random.randint(0, 90))).strftime("%Y-%m-%d"),
        "routing_number": generate_routing_number(),
    }

    # Add type-specific fields
    if account_type == "Checking":
        account["overdraft_limit"] = random.choice(config.get("overdraft_limit", [0]))
        account["monthly_fee"] = random.choice(config.get("monthly_fee", [0]))
        account["debit_card_number"] = f"4{random.randint(100000000000000, 999999999999999)}"

    elif account_type == "Savings":
        account["monthly_fee"] = random.choice(config.get("monthly_fee", [0]))
        account["withdrawal_limit"] = 6  # Federal Reg D limit
        account["min_balance_required"] = random.choice([0, 100, 500, 1000])

    elif account_type == "CD":
        term_months = random.choice(config.get("terms_months", [12]))
        maturity_date = datetime.strptime(open_date, "%Y-%m-%d") + timedelta(days=term_months * 30)
        account["term_months"] = term_months
        account["maturity_date"] = maturity_date.strftime("%Y-%m-%d")
        account["early_withdrawal_penalty"] = config.get("early_withdrawal_penalty", 0.10)
        account["auto_renew"] = random.choice(["Y", "N"])

    elif account_type == "CreditCard":
        credit_limit = random.choice(config.get("credit_limit", [5000]))
        account["credit_limit"] = credit_limit
        account["available_credit"] = credit_limit + balance  # balance is negative
        account["annual_fee"] = random.choice(config.get("annual_fee", [0]))
        account["minimum_payment"] = max(25, abs(balance) * 0.02)
        account["payment_due_date"] = random.randint(1, 28)
        account["rewards_program"] = random.choice(["None", "CashBack", "Points", "Miles"])
        account["card_number_last4"] = f"{random.randint(1000, 9999)}"

    elif account_type in ["Mortgage", "AutoLoan", "PersonalLoan"]:
        if account_type == "Mortgage":
            term_months = random.choice([180, 360])  # 15 or 30 years
            collateral = "Real Property"
        elif account_type == "AutoLoan":
            term_months = random.choice(config.get("term_months", [60]))
            collateral = f"{random.randint(2018, 2024)} {random.choice(['Toyota', 'Honda', 'Ford', 'BMW', 'Mercedes'])} {random.choice(['Sedan', 'SUV', 'Truck'])}"
        else:
            term_months = random.choice(config.get("term_months", [36]))
            collateral = "None" if "Unsecured" in subtype else "Various"

        original_balance = balance / random.uniform(0.3, 0.9)
        account["original_balance"] = round(original_balance, 2)
        account["term_months"] = term_months
        account["monthly_payment"] = calculate_monthly_payment(original_balance, interest_rate, term_months)
        account["next_payment_date"] = (datetime.now() + timedelta(days=random.randint(1, 30))).strftime("%Y-%m-%d")
        account["collateral"] = collateral
        account["escrow_balance"] = round(random.uniform(0, 5000), 2) if account_type == "Mortgage" else 0

    elif account_type == "Investment":
        account["portfolio_value"] = balance
        account["cost_basis"] = round(balance * random.uniform(0.7, 1.1), 2)
        account["ytd_return_pct"] = round(random.uniform(-0.15, 0.25), 4)
        account["advisor_id"] = f"ADV-{random.randint(1000, 9999)}" if random.random() < 0.3 else ""
        account["margin_enabled"] = random.choice(["Y", "N"])

    return account


def load_customers(customers_file: str) -> List[Dict[str, str]]:
    """Load customer IDs from TSV file."""
    customers = []
    if os.path.exists(customers_file):
        with open(customers_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            for row in reader:
                customers.append({
                    "customer_id": row["customer_id"],
                    "customer_type": row["customer_type"]
                })
    return customers


def load_branches(branches_file: str) -> List[str]:
    """Load branch IDs from TSV file."""
    branches = []
    if os.path.exists(branches_file):
        with open(branches_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            for row in reader:
                branches.append(row["branch_id"])
    return branches


def generate_accounts(
    customers_file: str = "../data/customers.tsv",
    branches_file: str = "../data/branches.tsv",
    output_file: str = "../data/accounts.tsv",
    avg_accounts_per_customer: float = 2.5
) -> List[Dict[str, Any]]:
    """Generate accounts for all customers."""

    customers = load_customers(customers_file)
    branches = load_branches(branches_file)

    if not customers:
        print("Warning: No customers found. Generating with placeholder IDs.")
        customers = [{"customer_id": f"CUST-{i:08d}", "customer_type": "Individual"} for i in range(1, 1001)]

    if not branches:
        print("Warning: No branches found. Using placeholder branch.")
        branches = ["BR-0001"]

    accounts = []
    account_id = 1

    print(f"Generating accounts for {len(customers)} customers...")

    for customer in customers:
        customer_id = customer["customer_id"]
        customer_type = customer["customer_type"]
        branch_id = random.choice(branches)

        # Determine number of accounts for this customer
        if customer_type == "Corporate":
            num_accounts = random.randint(2, 8)
            # Corporate: mostly checking and credit
            account_types = ["Checking", "Checking", "CreditCard", "Savings"]
        elif customer_type == "HighNetWorth":
            num_accounts = random.randint(3, 10)
            # HNW: diverse portfolio
            account_types = ["Checking", "Savings", "Investment", "CreditCard", "CD"]
        else:
            # Individual: Poisson-like distribution
            num_accounts = max(1, int(random.expovariate(1 / avg_accounts_per_customer)))
            num_accounts = min(num_accounts, 6)
            account_types = None  # Random

        # Generate accounts
        for i in range(num_accounts):
            acct_type = None
            if account_types:
                acct_type = random.choice(account_types)

            account = generate_account(account_id, customer_id, branch_id, acct_type)
            accounts.append(account)
            account_id += 1

    # Write to TSV
    if accounts:
        all_keys = set()
        for a in accounts:
            all_keys.update(a.keys())
        fieldnames = sorted(all_keys)

        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter='\t', extrasaction='ignore')
            writer.writeheader()
            writer.writerows(accounts)

        print(f"Saved {len(accounts)} accounts to {output_file}")

    return accounts


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Generate synthetic account data")
    parser.add_argument("--customers", type=str, default="../data/customers.tsv", help="Customers TSV file")
    parser.add_argument("--branches", type=str, default="../data/branches.tsv", help="Branches TSV file")
    parser.add_argument("--output", type=str, default="../data/accounts.tsv", help="Output file path")
    parser.add_argument("--avg-accounts", type=float, default=2.5, help="Average accounts per customer")
    parser.add_argument("--seed", type=int, default=42, help="Random seed")

    args = parser.parse_args()

    random.seed(args.seed)

    generate_accounts(
        customers_file=args.customers,
        branches_file=args.branches,
        output_file=args.output,
        avg_accounts_per_customer=args.avg_accounts
    )
