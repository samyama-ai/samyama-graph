#!/usr/bin/env python3
"""
Enterprise Banking - Relationship Data Generator

Generates realistic relationship data between entities:
- Customer-Account (OWNS)
- Customer-Branch (BANKS_AT)
- Account-Transaction (HAS_TRANSACTION)
- Customer-Customer (KNOWS, REFERRED_BY, AUTHORIZED_USER)
- Account-Account (TRANSFER_TO) based on transaction history
- Customer-Employee (MANAGED_BY, RELATIONSHIP_MANAGER)
- Employee-Branch (WORKS_AT)

These relationships form the graph edges in the banking knowledge graph.
"""

import csv
import random
from datetime import datetime, timedelta
from typing import List, Dict, Any, Set, Tuple
import os
from collections import defaultdict

# Relationship types with properties
RELATIONSHIP_TYPES = {
    "OWNS": {
        "source": "Customer",
        "target": "Account",
        "properties": ["ownership_type", "ownership_percentage", "start_date", "is_primary"]
    },
    "BANKS_AT": {
        "source": "Customer",
        "target": "Branch",
        "properties": ["relationship_type", "start_date", "is_primary_branch"]
    },
    "HAS_TRANSACTION": {
        "source": "Account",
        "target": "Transaction",
        "properties": []  # Implicit from transaction data
    },
    "TRANSFER_TO": {
        "source": "Account",
        "target": "Account",
        "properties": ["frequency", "total_amount", "first_transfer_date", "last_transfer_date"]
    },
    "KNOWS": {
        "source": "Customer",
        "target": "Customer",
        "properties": ["relationship_type", "since", "strength"]
    },
    "REFERRED_BY": {
        "source": "Customer",
        "target": "Customer",
        "properties": ["referral_date", "referral_bonus", "campaign_id"]
    },
    "AUTHORIZED_USER": {
        "source": "Customer",
        "target": "Account",
        "properties": ["authorization_level", "start_date", "spending_limit"]
    },
    "EMPLOYED_BY": {
        "source": "Customer",
        "target": "Customer",  # Corporate customer
        "properties": ["position", "department", "start_date", "is_active"]
    },
    "MANAGED_BY": {
        "source": "Customer",
        "target": "Employee",
        "properties": ["assignment_date", "relationship_tier"]
    },
    "WORKS_AT": {
        "source": "Employee",
        "target": "Branch",
        "properties": ["position", "department", "start_date", "is_manager"]
    },
    "GUARANTOR_FOR": {
        "source": "Customer",
        "target": "Account",  # Loan account
        "properties": ["guarantee_amount", "guarantee_date"]
    },
    "BENEFICIARY_OF": {
        "source": "Customer",
        "target": "Account",
        "properties": ["beneficiary_type", "percentage", "designation_date"]
    }
}

# Relationship strength categories
RELATIONSHIP_STRENGTHS = {
    "Strong": 0.3,
    "Medium": 0.5,
    "Weak": 0.2
}

# Personal relationship types between customers
PERSONAL_RELATIONSHIPS = [
    "Family",
    "Friend",
    "Colleague",
    "Business Partner",
    "Neighbor",
    "Acquaintance"
]


def weighted_choice(choices: Dict[str, float]) -> str:
    """Select from weighted choices."""
    items = list(choices.keys())
    weights = list(choices.values())
    return random.choices(items, weights=weights)[0]


def load_tsv(file_path: str) -> List[Dict[str, str]]:
    """Load data from TSV file."""
    data = []
    if os.path.exists(file_path):
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            for row in reader:
                data.append(row)
    return data


def generate_customer_account_relationships(
    customers: List[Dict],
    accounts: List[Dict]
) -> List[Dict[str, Any]]:
    """Generate OWNS relationships between customers and accounts."""
    relationships = []

    # Group accounts by customer
    customer_accounts = defaultdict(list)
    for acc in accounts:
        customer_accounts[acc.get("customer_id", "")].append(acc)

    for customer in customers:
        customer_id = customer["customer_id"]
        accts = customer_accounts.get(customer_id, [])

        for i, acc in enumerate(accts):
            rel = {
                "relationship_id": f"REL-OWN-{len(relationships)+1:08d}",
                "relationship_type": "OWNS",
                "source_type": "Customer",
                "source_id": customer_id,
                "target_type": "Account",
                "target_id": acc["account_id"],
                "ownership_type": "Primary" if i == 0 else "Secondary",
                "ownership_percentage": 100 if customer["customer_type"] != "Corporate" else random.choice([100, 50, 33]),
                "start_date": acc.get("open_date", "2020-01-01"),
                "is_primary": "Y" if i == 0 else "N",
                "status": "Active"
            }
            relationships.append(rel)

    return relationships


def generate_customer_branch_relationships(
    customers: List[Dict],
    branches: List[Dict],
    accounts: List[Dict]
) -> List[Dict[str, Any]]:
    """Generate BANKS_AT relationships between customers and branches."""
    relationships = []

    # Build mapping of account to branch
    account_branch = {acc["account_id"]: acc.get("branch_id", "") for acc in accounts}

    # Build mapping of customer to accounts
    customer_accounts = defaultdict(list)
    for acc in accounts:
        customer_accounts[acc.get("customer_id", "")].append(acc["account_id"])

    branch_ids = [b["branch_id"] for b in branches]

    for customer in customers:
        customer_id = customer["customer_id"]
        accts = customer_accounts.get(customer_id, [])

        # Get unique branches from customer's accounts
        customer_branches = set()
        for acc_id in accts:
            branch_id = account_branch.get(acc_id, "")
            if branch_id:
                customer_branches.add(branch_id)

        # If no branches from accounts, assign random one
        if not customer_branches and branch_ids:
            customer_branches.add(random.choice(branch_ids))

        for i, branch_id in enumerate(customer_branches):
            rel = {
                "relationship_id": f"REL-BNK-{len(relationships)+1:08d}",
                "relationship_type": "BANKS_AT",
                "source_type": "Customer",
                "source_id": customer_id,
                "target_type": "Branch",
                "target_id": branch_id,
                "relationship_category": "Primary" if i == 0 else "Secondary",
                "start_date": customer.get("member_since", "2020-01-01"),
                "is_primary_branch": "Y" if i == 0 else "N",
                "status": "Active"
            }
            relationships.append(rel)

    return relationships


def generate_transfer_relationships(
    transactions: List[Dict],
    accounts: List[Dict]
) -> List[Dict[str, Any]]:
    """Generate TRANSFER_TO relationships based on transaction history."""
    relationships = []

    # Get valid account IDs
    valid_accounts = {acc["account_id"] for acc in accounts}

    # Track transfers between accounts
    transfer_stats = defaultdict(lambda: {
        "count": 0,
        "total_amount": 0.0,
        "first_date": None,
        "last_date": None
    })

    # Process transactions to find transfers
    for tx in transactions:
        if tx.get("transaction_type") == "Transfer" and tx.get("status") == "Completed":
            source_acc = tx.get("account_id", "")

            # For demo purposes, randomly assign a target account
            # In real data, this would come from transfer details
            if source_acc in valid_accounts:
                # Pick a random different account as target
                potential_targets = [a for a in valid_accounts if a != source_acc]
                if potential_targets and random.random() < 0.1:  # Only 10% create relationships
                    target_acc = random.choice(potential_targets)
                    key = (source_acc, target_acc)

                    amount = float(tx.get("amount", 0))
                    date = tx.get("transaction_date", "")

                    stats = transfer_stats[key]
                    stats["count"] += 1
                    stats["total_amount"] += amount
                    if stats["first_date"] is None or date < stats["first_date"]:
                        stats["first_date"] = date
                    if stats["last_date"] is None or date > stats["last_date"]:
                        stats["last_date"] = date

    # Create relationships for accounts with multiple transfers
    for (source, target), stats in transfer_stats.items():
        if stats["count"] >= 2:  # Only if transferred more than once
            rel = {
                "relationship_id": f"REL-TRF-{len(relationships)+1:08d}",
                "relationship_type": "TRANSFER_TO",
                "source_type": "Account",
                "source_id": source,
                "target_type": "Account",
                "target_id": target,
                "frequency": stats["count"],
                "total_amount": round(stats["total_amount"], 2),
                "first_transfer_date": stats["first_date"],
                "last_transfer_date": stats["last_date"],
                "status": "Active"
            }
            relationships.append(rel)

    return relationships


def generate_customer_relationships(
    customers: List[Dict],
    num_relationships: int = 1000
) -> List[Dict[str, Any]]:
    """Generate KNOWS and REFERRED_BY relationships between customers."""
    relationships = []

    customer_ids = [c["customer_id"] for c in customers]
    customer_map = {c["customer_id"]: c for c in customers}

    if len(customer_ids) < 2:
        return relationships

    # Generate KNOWS relationships (social connections)
    for _ in range(num_relationships):
        # Pick two different customers
        c1, c2 = random.sample(customer_ids, 2)

        # Same city increases likelihood of connection
        same_city = customer_map[c1].get("city") == customer_map[c2].get("city")
        if not same_city and random.random() > 0.3:
            continue

        rel = {
            "relationship_id": f"REL-KNW-{len(relationships)+1:08d}",
            "relationship_type": "KNOWS",
            "source_type": "Customer",
            "source_id": c1,
            "target_type": "Customer",
            "target_id": c2,
            "relationship_category": random.choice(PERSONAL_RELATIONSHIPS),
            "since": (datetime.now() - timedelta(days=random.randint(30, 3650))).strftime("%Y-%m-%d"),
            "strength": weighted_choice(RELATIONSHIP_STRENGTHS),
            "status": "Active"
        }
        relationships.append(rel)

    # Generate REFERRED_BY relationships (referral program)
    num_referrals = num_relationships // 5
    for _ in range(num_referrals):
        referrer, referee = random.sample(customer_ids, 2)

        referrer_date = customer_map[referrer].get("member_since", "2020-01-01")
        referee_date = customer_map[referee].get("member_since", "2020-01-01")

        # Referrer must have joined before referee
        if referrer_date < referee_date:
            rel = {
                "relationship_id": f"REL-REF-{len(relationships)+1:08d}",
                "relationship_type": "REFERRED_BY",
                "source_type": "Customer",
                "source_id": referee,
                "target_type": "Customer",
                "target_id": referrer,
                "referral_date": referee_date,
                "referral_bonus": random.choice([50, 100, 150, 200]),
                "campaign_id": f"CAMP-{random.randint(2020, 2024)}-{random.randint(1, 12):02d}",
                "status": "Completed"
            }
            relationships.append(rel)

    return relationships


def generate_employment_relationships(
    customers: List[Dict]
) -> List[Dict[str, Any]]:
    """Generate EMPLOYED_BY relationships between individual and corporate customers."""
    relationships = []

    # Separate individual and corporate customers
    individuals = [c for c in customers if c.get("customer_type") == "Individual"]
    corporates = [c for c in customers if c.get("customer_type") == "Corporate"]

    if not corporates or not individuals:
        return relationships

    # Some individuals work for corporate customers
    num_employed = min(len(individuals) // 10, len(corporates) * 50)

    positions = [
        "Executive", "Manager", "Director", "VP", "Analyst",
        "Engineer", "Consultant", "Specialist", "Associate", "Coordinator"
    ]

    departments = [
        "Finance", "Operations", "Technology", "HR", "Marketing",
        "Sales", "Legal", "Administration", "R&D", "Customer Service"
    ]

    employed_individuals = random.sample(individuals, min(num_employed, len(individuals)))

    for ind in employed_individuals:
        corp = random.choice(corporates)

        rel = {
            "relationship_id": f"REL-EMP-{len(relationships)+1:08d}",
            "relationship_type": "EMPLOYED_BY",
            "source_type": "Customer",
            "source_id": ind["customer_id"],
            "target_type": "Customer",
            "target_id": corp["customer_id"],
            "position": random.choice(positions),
            "department": random.choice(departments),
            "start_date": (datetime.now() - timedelta(days=random.randint(30, 3650))).strftime("%Y-%m-%d"),
            "is_active": random.choice(["Y", "Y", "Y", "N"]),  # 75% active
            "status": "Active"
        }
        relationships.append(rel)

    return relationships


def generate_authorized_user_relationships(
    customers: List[Dict],
    accounts: List[Dict]
) -> List[Dict[str, Any]]:
    """Generate AUTHORIZED_USER relationships for joint accounts and credit cards."""
    relationships = []

    # Group accounts by customer
    customer_accounts = defaultdict(list)
    for acc in accounts:
        customer_accounts[acc.get("customer_id", "")].append(acc)

    customer_ids = [c["customer_id"] for c in customers]

    # For some accounts, add authorized users
    for customer in customers:
        customer_id = customer["customer_id"]
        accts = customer_accounts.get(customer_id, [])

        for acc in accts:
            # Credit cards often have authorized users
            if acc.get("account_type") == "CreditCard" and random.random() < 0.2:
                # Add an authorized user
                other_customers = [c for c in customer_ids if c != customer_id]
                if other_customers:
                    auth_user = random.choice(other_customers)
                    rel = {
                        "relationship_id": f"REL-AUT-{len(relationships)+1:08d}",
                        "relationship_type": "AUTHORIZED_USER",
                        "source_type": "Customer",
                        "source_id": auth_user,
                        "target_type": "Account",
                        "target_id": acc["account_id"],
                        "authorization_level": random.choice(["Full", "Limited", "View Only"]),
                        "start_date": acc.get("open_date", "2020-01-01"),
                        "spending_limit": random.choice([500, 1000, 2500, 5000, None]),
                        "status": "Active"
                    }
                    relationships.append(rel)

    return relationships


def generate_all_relationships(
    data_dir: str = "../data",
    output_dir: str = "../data"
) -> Dict[str, List[Dict[str, Any]]]:
    """Generate all relationship types and save to TSV files."""

    print("Loading entity data...")
    customers = load_tsv(os.path.join(data_dir, "customers.tsv"))
    accounts = load_tsv(os.path.join(data_dir, "accounts.tsv"))
    branches = load_tsv(os.path.join(data_dir, "branches.tsv"))
    transactions = load_tsv(os.path.join(data_dir, "transactions.tsv"))

    print(f"  Loaded {len(customers)} customers")
    print(f"  Loaded {len(accounts)} accounts")
    print(f"  Loaded {len(branches)} branches")
    print(f"  Loaded {len(transactions)} transactions")

    all_relationships = {}

    # Generate each relationship type
    print("\nGenerating relationships...")

    print("  Customer-Account (OWNS)...")
    owns = generate_customer_account_relationships(customers, accounts)
    all_relationships["owns"] = owns
    print(f"    Generated {len(owns)} relationships")

    print("  Customer-Branch (BANKS_AT)...")
    banks_at = generate_customer_branch_relationships(customers, branches, accounts)
    all_relationships["banks_at"] = banks_at
    print(f"    Generated {len(banks_at)} relationships")

    print("  Account-Account (TRANSFER_TO)...")
    transfers = generate_transfer_relationships(transactions, accounts)
    all_relationships["transfers"] = transfers
    print(f"    Generated {len(transfers)} relationships")

    print("  Customer-Customer (KNOWS, REFERRED_BY)...")
    social = generate_customer_relationships(customers, num_relationships=len(customers) // 5)
    all_relationships["social"] = social
    print(f"    Generated {len(social)} relationships")

    print("  Customer-Corporate (EMPLOYED_BY)...")
    employment = generate_employment_relationships(customers)
    all_relationships["employment"] = employment
    print(f"    Generated {len(employment)} relationships")

    print("  Authorized Users...")
    auth_users = generate_authorized_user_relationships(customers, accounts)
    all_relationships["authorized_users"] = auth_users
    print(f"    Generated {len(auth_users)} relationships")

    # Save each relationship type to separate TSV
    # File names match what banking_demo.rs expects for loading
    print("\nSaving relationship files...")

    # Map internal names to output file names (matching banking_demo.rs expectations)
    file_name_map = {
        "owns": "owns_account.tsv",
        "banks_at": "banks_at.tsv",
        "transfers": "transfer_to.tsv",
        "social": "knows.tsv",  # Contains both KNOWS and REFERRED_BY
        "employment": "employed_by.tsv",
        "authorized_users": "authorized_user.tsv",
    }

    for rel_name, rels in all_relationships.items():
        if rels:
            # Use mapped filename or fall back to default pattern
            filename = file_name_map.get(rel_name, f"{rel_name}.tsv")
            output_file = os.path.join(output_dir, filename)
            # Gather ALL unique keys from ALL records in this relationship type
            # Why: Different records may have different fields (e.g., referrals have bonus fields)
            # After: All fields are included in the TSV, missing values become empty strings
            all_keys = set()
            for r in rels:
                all_keys.update(r.keys())
            fieldnames = sorted(all_keys)

            with open(output_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter='\t', extrasaction='ignore')
                writer.writeheader()
                writer.writerows(rels)

            print(f"  Saved {len(rels)} {rel_name} relationships to {output_file}")

    # Also save a combined relationships file
    all_rels = []
    for rels in all_relationships.values():
        all_rels.extend(rels)

    if all_rels:
        # Get all unique keys
        all_keys = set()
        for r in all_rels:
            all_keys.update(r.keys())

        output_file = os.path.join(output_dir, "relationships_all.tsv")
        fieldnames = sorted(all_keys)

        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter='\t', extrasaction='ignore')
            writer.writeheader()
            writer.writerows(all_rels)

        print(f"\n  Saved {len(all_rels)} total relationships to {output_file}")

    return all_relationships


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Generate relationship data")
    parser.add_argument("--data-dir", type=str, default="../data", help="Input data directory")
    parser.add_argument("--output-dir", type=str, default="../data", help="Output directory")
    parser.add_argument("--seed", type=int, default=42, help="Random seed")

    args = parser.parse_args()

    random.seed(args.seed)

    generate_all_relationships(
        data_dir=args.data_dir,
        output_dir=args.output_dir
    )
