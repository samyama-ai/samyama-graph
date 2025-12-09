#!/usr/bin/env python3
"""
Enterprise Banking - Master Data Generation Script

Orchestrates the generation of all synthetic banking data:
1. Branches (generated first - other entities reference them)
2. Customers (Individual, Corporate, High-Net-Worth)
3. Accounts (linked to customers and branches)
4. Transactions (linked to accounts)
5. Relationships (graph edges between all entities)

Usage:
    python generate_all.py                    # Default: medium-sized dataset
    python generate_all.py --size small       # Small demo dataset
    python generate_all.py --size large       # Large enterprise dataset
    python generate_all.py --size enterprise  # Full enterprise scale
"""

import os
import sys
import random
import argparse
import time
from datetime import datetime

# Add generators to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from branches import generate_branches
from customers import generate_customers
from accounts import generate_accounts
from transactions import generate_transactions
from relationships import generate_all_relationships


# Dataset size configurations
DATASET_SIZES = {
    "tiny": {
        "branches": 10,
        "individual_customers": 100,
        "corporate_customers": 10,
        "hnw_customers": 5,
        "avg_accounts_per_customer": 1.5,
        "avg_transactions_per_account": 10,
        "transaction_days": 30
    },
    "small": {
        "branches": 25,
        "individual_customers": 500,
        "corporate_customers": 50,
        "hnw_customers": 25,
        "avg_accounts_per_customer": 2.0,
        "avg_transactions_per_account": 25,
        "transaction_days": 90
    },
    "medium": {
        "branches": 75,
        "individual_customers": 2500,
        "corporate_customers": 250,
        "hnw_customers": 100,
        "avg_accounts_per_customer": 2.5,
        "avg_transactions_per_account": 50,
        "transaction_days": 180
    },
    "large": {
        "branches": 150,
        "individual_customers": 10000,
        "corporate_customers": 1000,
        "hnw_customers": 500,
        "avg_accounts_per_customer": 3.0,
        "avg_transactions_per_account": 75,
        "transaction_days": 365
    },
    "enterprise": {
        "branches": 500,
        "individual_customers": 50000,
        "corporate_customers": 5000,
        "hnw_customers": 2000,
        "avg_accounts_per_customer": 3.5,
        "avg_transactions_per_account": 100,
        "transaction_days": 730  # 2 years
    }
}


def ensure_data_directory(data_dir: str):
    """Create data directory if it doesn't exist."""
    os.makedirs(data_dir, exist_ok=True)
    print(f"Data directory: {os.path.abspath(data_dir)}")


def print_banner():
    """Print generation banner."""
    print("=" * 70)
    print(" SAMYAMA GRAPH DATABASE - Enterprise Banking Data Generator")
    print("=" * 70)
    print()


def print_summary(data_dir: str, start_time: float):
    """Print generation summary."""
    elapsed = time.time() - start_time

    print()
    print("=" * 70)
    print(" GENERATION COMPLETE")
    print("=" * 70)
    print()

    # List generated files with sizes
    print("Generated files:")
    total_size = 0
    total_rows = 0

    for filename in sorted(os.listdir(data_dir)):
        if filename.endswith('.tsv'):
            filepath = os.path.join(data_dir, filename)
            size = os.path.getsize(filepath)
            total_size += size

            # Count rows
            with open(filepath, 'r') as f:
                rows = sum(1 for _ in f) - 1  # Subtract header
                total_rows += rows

            size_str = f"{size / 1024:.1f} KB" if size < 1024 * 1024 else f"{size / (1024*1024):.1f} MB"
            print(f"  {filename:<35} {rows:>10,} rows  {size_str:>10}")

    print()
    print(f"Total: {total_rows:,} rows, {total_size / (1024*1024):.1f} MB")
    print(f"Time elapsed: {elapsed:.1f} seconds")
    print()
    print(f"Data location: {os.path.abspath(data_dir)}")
    print()


def main():
    parser = argparse.ArgumentParser(
        description="Generate synthetic banking data for Samyama Graph Database",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Dataset sizes:
  tiny       - Quick test (~100 customers, ~1K transactions)
  small      - Demo/development (~500 customers, ~25K transactions)
  medium     - Testing (~2,500 customers, ~300K transactions)
  large      - Performance testing (~10K customers, ~2M transactions)
  enterprise - Full scale (~50K customers, ~15M transactions)

Examples:
  python generate_all.py --size small
  python generate_all.py --size medium --seed 12345
  python generate_all.py --output ./my_data
        """
    )

    parser.add_argument(
        "--size",
        type=str,
        choices=list(DATASET_SIZES.keys()),
        default="medium",
        help="Dataset size preset (default: medium)"
    )

    parser.add_argument(
        "--output",
        type=str,
        default="../data",
        help="Output directory for generated files (default: ../data)"
    )

    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for reproducibility (default: 42)"
    )

    parser.add_argument(
        "--skip-transactions",
        action="store_true",
        help="Skip transaction generation (faster for testing)"
    )

    args = parser.parse_args()

    # Get configuration
    config = DATASET_SIZES[args.size]
    data_dir = args.output

    # Set random seed
    random.seed(args.seed)

    # Start
    print_banner()
    print(f"Dataset size: {args.size}")
    print(f"Random seed: {args.seed}")
    print()

    start_time = time.time()

    # Create data directory
    ensure_data_directory(data_dir)
    print()

    # Step 1: Generate Branches
    print("-" * 70)
    print("Step 1/5: Generating Branches")
    print("-" * 70)
    branches = generate_branches(
        num_branches=config["branches"],
        output_file=os.path.join(data_dir, "branches.tsv")
    )
    print()

    # Step 2: Generate Customers
    print("-" * 70)
    print("Step 2/5: Generating Customers")
    print("-" * 70)
    customers = generate_customers(
        num_individual=config["individual_customers"],
        num_corporate=config["corporate_customers"],
        num_hnw=config["hnw_customers"],
        output_file=os.path.join(data_dir, "customers.tsv")
    )
    print()

    # Step 3: Generate Accounts
    print("-" * 70)
    print("Step 3/5: Generating Accounts")
    print("-" * 70)
    accounts = generate_accounts(
        customers_file=os.path.join(data_dir, "customers.tsv"),
        branches_file=os.path.join(data_dir, "branches.tsv"),
        output_file=os.path.join(data_dir, "accounts.tsv"),
        avg_accounts_per_customer=config["avg_accounts_per_customer"]
    )
    print()

    # Step 4: Generate Transactions
    if not args.skip_transactions:
        print("-" * 70)
        print("Step 4/5: Generating Transactions")
        print("-" * 70)
        transactions = generate_transactions(
            accounts_file=os.path.join(data_dir, "accounts.tsv"),
            output_file=os.path.join(data_dir, "transactions.tsv"),
            avg_transactions_per_account=config["avg_transactions_per_account"],
            days_back=config["transaction_days"]
        )
        print()
    else:
        print("-" * 70)
        print("Step 4/5: Skipping Transactions (--skip-transactions)")
        print("-" * 70)
        # Create empty transactions file
        with open(os.path.join(data_dir, "transactions.tsv"), 'w') as f:
            f.write("transaction_id\taccount_id\ttransaction_type\tamount\n")
        print()

    # Step 5: Generate Relationships
    print("-" * 70)
    print("Step 5/5: Generating Relationships")
    print("-" * 70)
    relationships = generate_all_relationships(
        data_dir=data_dir,
        output_dir=data_dir
    )
    print()

    # Print summary
    print_summary(data_dir, start_time)

    return 0


if __name__ == "__main__":
    sys.exit(main())
