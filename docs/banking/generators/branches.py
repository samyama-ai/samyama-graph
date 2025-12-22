#!/usr/bin/env python3
"""
Enterprise Banking - Branch Data Generator

Generates realistic synthetic branch data including:
- Main/headquarters branches
- Regional branches
- Local branches
- ATM-only locations
- Digital/online banking centers

Uses real US geographic data and realistic banking operations.
"""

import csv
import random
from datetime import datetime, timedelta
from typing import List, Dict, Any

# Comprehensive US city data with banking presence likelihood
US_BANKING_LOCATIONS = [
    # Major financial centers (Tier 1)
    {"city": "New York", "state": "NY", "region": "Northeast", "zip": "10001", "lat": 40.7128, "lng": -74.0060, "tier": 1, "weight": 15},
    {"city": "Los Angeles", "state": "CA", "region": "West", "zip": "90001", "lat": 34.0522, "lng": -118.2437, "tier": 1, "weight": 12},
    {"city": "Chicago", "state": "IL", "region": "Midwest", "zip": "60601", "lat": 41.8781, "lng": -87.6298, "tier": 1, "weight": 10},
    {"city": "Houston", "state": "TX", "region": "South", "zip": "77001", "lat": 29.7604, "lng": -95.3698, "tier": 1, "weight": 8},
    {"city": "San Francisco", "state": "CA", "region": "West", "zip": "94102", "lat": 37.7749, "lng": -122.4194, "tier": 1, "weight": 8},
    {"city": "Boston", "state": "MA", "region": "Northeast", "zip": "02101", "lat": 42.3601, "lng": -71.0589, "tier": 1, "weight": 7},
    {"city": "Dallas", "state": "TX", "region": "South", "zip": "75201", "lat": 32.7767, "lng": -96.7970, "tier": 1, "weight": 7},
    {"city": "Miami", "state": "FL", "region": "South", "zip": "33101", "lat": 25.7617, "lng": -80.1918, "tier": 1, "weight": 6},
    {"city": "Atlanta", "state": "GA", "region": "South", "zip": "30301", "lat": 33.7490, "lng": -84.3880, "tier": 1, "weight": 6},
    {"city": "Seattle", "state": "WA", "region": "West", "zip": "98101", "lat": 47.6062, "lng": -122.3321, "tier": 1, "weight": 5},

    # Regional centers (Tier 2)
    {"city": "Phoenix", "state": "AZ", "region": "West", "zip": "85001", "lat": 33.4484, "lng": -112.0740, "tier": 2, "weight": 4},
    {"city": "Philadelphia", "state": "PA", "region": "Northeast", "zip": "19101", "lat": 39.9526, "lng": -75.1652, "tier": 2, "weight": 4},
    {"city": "San Diego", "state": "CA", "region": "West", "zip": "92101", "lat": 32.7157, "lng": -117.1611, "tier": 2, "weight": 3},
    {"city": "Denver", "state": "CO", "region": "West", "zip": "80201", "lat": 39.7392, "lng": -104.9903, "tier": 2, "weight": 4},
    {"city": "Austin", "state": "TX", "region": "South", "zip": "78701", "lat": 30.2672, "lng": -97.7431, "tier": 2, "weight": 4},
    {"city": "Charlotte", "state": "NC", "region": "South", "zip": "28201", "lat": 35.2271, "lng": -80.8431, "tier": 2, "weight": 5},  # Major banking center
    {"city": "Minneapolis", "state": "MN", "region": "Midwest", "zip": "55401", "lat": 44.9778, "lng": -93.2650, "tier": 2, "weight": 3},
    {"city": "Portland", "state": "OR", "region": "West", "zip": "97201", "lat": 45.5152, "lng": -122.6784, "tier": 2, "weight": 3},
    {"city": "Detroit", "state": "MI", "region": "Midwest", "zip": "48201", "lat": 42.3314, "lng": -83.0458, "tier": 2, "weight": 3},
    {"city": "Nashville", "state": "TN", "region": "South", "zip": "37201", "lat": 36.1627, "lng": -86.7816, "tier": 2, "weight": 3},

    # Secondary markets (Tier 3)
    {"city": "San Jose", "state": "CA", "region": "West", "zip": "95101", "lat": 37.3382, "lng": -121.8863, "tier": 3, "weight": 3},
    {"city": "Columbus", "state": "OH", "region": "Midwest", "zip": "43201", "lat": 39.9612, "lng": -82.9988, "tier": 3, "weight": 2},
    {"city": "Indianapolis", "state": "IN", "region": "Midwest", "zip": "46201", "lat": 39.7684, "lng": -86.1581, "tier": 3, "weight": 2},
    {"city": "Jacksonville", "state": "FL", "region": "South", "zip": "32201", "lat": 30.3322, "lng": -81.6557, "tier": 3, "weight": 2},
    {"city": "San Antonio", "state": "TX", "region": "South", "zip": "78201", "lat": 29.4241, "lng": -98.4936, "tier": 3, "weight": 2},
    {"city": "Fort Worth", "state": "TX", "region": "South", "zip": "76101", "lat": 32.7555, "lng": -97.3308, "tier": 3, "weight": 2},
    {"city": "Baltimore", "state": "MD", "region": "Northeast", "zip": "21201", "lat": 39.2904, "lng": -76.6122, "tier": 3, "weight": 2},
    {"city": "Salt Lake City", "state": "UT", "region": "West", "zip": "84101", "lat": 40.7608, "lng": -111.8910, "tier": 3, "weight": 2},
    {"city": "Kansas City", "state": "MO", "region": "Midwest", "zip": "64101", "lat": 39.0997, "lng": -94.5786, "tier": 3, "weight": 2},
    {"city": "Las Vegas", "state": "NV", "region": "West", "zip": "89101", "lat": 36.1699, "lng": -115.1398, "tier": 3, "weight": 2},
    {"city": "Milwaukee", "state": "WI", "region": "Midwest", "zip": "53201", "lat": 43.0389, "lng": -87.9065, "tier": 3, "weight": 2},
    {"city": "Cleveland", "state": "OH", "region": "Midwest", "zip": "44101", "lat": 41.4993, "lng": -81.6944, "tier": 3, "weight": 2},
    {"city": "Tampa", "state": "FL", "region": "South", "zip": "33601", "lat": 27.9506, "lng": -82.4572, "tier": 3, "weight": 2},
    {"city": "Raleigh", "state": "NC", "region": "South", "zip": "27601", "lat": 35.7796, "lng": -78.6382, "tier": 3, "weight": 2},
    {"city": "Pittsburgh", "state": "PA", "region": "Northeast", "zip": "15201", "lat": 40.4406, "lng": -79.9959, "tier": 3, "weight": 2},

    # Smaller markets (Tier 4)
    {"city": "Cincinnati", "state": "OH", "region": "Midwest", "zip": "45201", "lat": 39.1031, "lng": -84.5120, "tier": 4, "weight": 1},
    {"city": "St. Louis", "state": "MO", "region": "Midwest", "zip": "63101", "lat": 38.6270, "lng": -90.1994, "tier": 4, "weight": 1},
    {"city": "Orlando", "state": "FL", "region": "South", "zip": "32801", "lat": 28.5383, "lng": -81.3792, "tier": 4, "weight": 1},
    {"city": "Sacramento", "state": "CA", "region": "West", "zip": "95814", "lat": 38.5816, "lng": -121.4944, "tier": 4, "weight": 1},
    {"city": "New Orleans", "state": "LA", "region": "South", "zip": "70112", "lat": 29.9511, "lng": -90.0715, "tier": 4, "weight": 1},
    {"city": "Tucson", "state": "AZ", "region": "West", "zip": "85701", "lat": 32.2226, "lng": -110.9747, "tier": 4, "weight": 1},
    {"city": "Honolulu", "state": "HI", "region": "West", "zip": "96801", "lat": 21.3069, "lng": -157.8583, "tier": 4, "weight": 1},
    {"city": "Albuquerque", "state": "NM", "region": "West", "zip": "87101", "lat": 35.0844, "lng": -106.6504, "tier": 4, "weight": 1},
    {"city": "Omaha", "state": "NE", "region": "Midwest", "zip": "68101", "lat": 41.2565, "lng": -95.9345, "tier": 4, "weight": 1},
]

# Branch types with characteristics
BRANCH_TYPES = {
    "Headquarters": {
        "services": ["Corporate Banking", "Wealth Management", "Commercial Lending", "Treasury Services", "Investment Banking"],
        "employees_range": (200, 1000),
        "sqft_range": (50000, 200000),
        "hours": "8:00 AM - 6:00 PM",
        "weight": 0.02
    },
    "Regional Center": {
        "services": ["Commercial Banking", "Business Banking", "Wealth Management", "Mortgage Services"],
        "employees_range": (50, 200),
        "sqft_range": (15000, 50000),
        "hours": "8:00 AM - 6:00 PM",
        "weight": 0.08
    },
    "Full Service": {
        "services": ["Personal Banking", "Business Banking", "Mortgage", "Safe Deposit", "Notary"],
        "employees_range": (15, 50),
        "sqft_range": (4000, 15000),
        "hours": "9:00 AM - 5:00 PM",
        "weight": 0.40
    },
    "Standard": {
        "services": ["Personal Banking", "Basic Business", "ATM"],
        "employees_range": (8, 20),
        "sqft_range": (2000, 5000),
        "hours": "9:00 AM - 5:00 PM",
        "weight": 0.30
    },
    "Express": {
        "services": ["Teller Services", "ATM", "Account Opening"],
        "employees_range": (3, 10),
        "sqft_range": (800, 2500),
        "hours": "9:00 AM - 6:00 PM",
        "weight": 0.12
    },
    "In-Store": {
        "services": ["Basic Teller", "ATM", "Account Services"],
        "employees_range": (2, 6),
        "sqft_range": (400, 1200),
        "hours": "10:00 AM - 7:00 PM",
        "weight": 0.05
    },
    "ATM Only": {
        "services": ["ATM"],
        "employees_range": (0, 0),
        "sqft_range": (50, 200),
        "hours": "24/7",
        "weight": 0.03
    }
}

# Street names for addresses
STREET_NAMES = [
    "Main", "Broadway", "Market", "Wall", "Commerce", "Financial", "Bank",
    "First", "Second", "Third", "Park", "Madison", "Lexington", "Fifth",
    "Center", "Central", "State", "Washington", "Lincoln", "Oak", "Maple"
]

STREET_TYPES = ["Street", "Avenue", "Boulevard", "Plaza", "Drive", "Way"]


def weighted_choice(choices: Dict[str, float]) -> str:
    """Select from weighted choices."""
    items = list(choices.keys())
    weights = list(choices.values())
    return random.choices(items, weights=weights)[0]


def generate_branch_code(location: Dict, sequence: int) -> str:
    """Generate unique branch code."""
    state = location["state"]
    return f"BR-{state}-{sequence:04d}"


def generate_branch_name(location: Dict, branch_type: str, sequence: int) -> str:
    """Generate descriptive branch name."""
    city = location["city"]

    if branch_type == "Headquarters":
        return f"{city} Corporate Headquarters"
    elif branch_type == "Regional Center":
        return f"{city} Regional Banking Center"
    elif branch_type == "In-Store":
        stores = ["Walmart", "Target", "Kroger", "Safeway", "Costco"]
        return f"{city} {random.choice(stores)} Branch"
    elif branch_type == "ATM Only":
        return f"{city} ATM Location #{sequence}"
    else:
        suffixes = ["Main", "Downtown", "Central", "Plaza", "Financial District", "Midtown", "North", "South", "East", "West"]
        return f"{city} {random.choice(suffixes)} Branch"


def generate_address(location: Dict) -> Dict[str, str]:
    """Generate realistic branch address."""
    street_num = random.randint(1, 999) * 10 + random.randint(0, 9)
    street = f"{random.choice(STREET_NAMES)} {random.choice(STREET_TYPES)}"

    # Generate realistic zip variation
    base_zip = location["zip"]
    zip_suffix = random.randint(0, 99)
    full_zip = f"{base_zip[:5]}"

    return {
        "street_address": f"{street_num} {street}",
        "city": location["city"],
        "state": location["state"],
        "zip_code": full_zip,
        "country": "USA",
        "latitude": round(location["lat"] + random.uniform(-0.05, 0.05), 6),
        "longitude": round(location["lng"] + random.uniform(-0.05, 0.05), 6)
    }


def generate_branch(branch_id: int, location: Dict, branch_type: str = None) -> Dict[str, Any]:
    """Generate a single branch."""

    if branch_type is None:
        type_weights = {k: v["weight"] for k, v in BRANCH_TYPES.items()}
        branch_type = weighted_choice(type_weights)

    config = BRANCH_TYPES[branch_type]
    address = generate_address(location)

    # Employee count
    emp_min, emp_max = config["employees_range"]
    employee_count = random.randint(emp_min, emp_max) if emp_max > 0 else 0

    # Square footage
    sqft_min, sqft_max = config["sqft_range"]
    square_footage = random.randint(sqft_min, sqft_max)

    # Opening date (older branches for established locations)
    years_ago = random.randint(1, 50) if branch_type != "ATM Only" else random.randint(1, 15)
    open_date = datetime.now() - timedelta(days=years_ago * 365 + random.randint(0, 364))

    # Manager assignment (except ATM only)
    manager_id = f"EMP-{random.randint(10000, 99999)}" if employee_count > 0 else ""

    branch = {
        "branch_id": f"BR-{branch_id:05d}",
        "branch_code": generate_branch_code(location, branch_id),
        "branch_name": generate_branch_name(location, branch_type, branch_id),
        "branch_type": branch_type,
        "tier": location["tier"],
        "region": location["region"],
        "street_address": address["street_address"],
        "city": address["city"],
        "state": address["state"],
        "zip_code": address["zip_code"],
        "country": address["country"],
        "latitude": address["latitude"],
        "longitude": address["longitude"],
        "phone": f"+1-{random.randint(200, 999)}-{random.randint(200, 999)}-{random.randint(1000, 9999)}",
        "fax": f"+1-{random.randint(200, 999)}-{random.randint(200, 999)}-{random.randint(1000, 9999)}" if branch_type not in ["ATM Only", "Express"] else "",
        "email": f"branch{branch_id}@samyamabank.com",
        "hours_weekday": config["hours"],
        "hours_saturday": "9:00 AM - 1:00 PM" if branch_type not in ["ATM Only", "In-Store"] else config["hours"],
        "hours_sunday": "Closed" if branch_type != "ATM Only" else "24/7",
        "services": "|".join(config["services"]),
        "employee_count": employee_count,
        "square_footage": square_footage,
        "manager_id": manager_id,
        "open_date": open_date.strftime("%Y-%m-%d"),
        "last_renovation": (datetime.now() - timedelta(days=random.randint(0, 3650))).strftime("%Y-%m-%d"),
        "atm_count": random.randint(1, 4) if branch_type != "ATM Only" else 1,
        "drive_through": "Y" if branch_type in ["Full Service", "Standard", "Express"] and random.random() < 0.6 else "N",
        "wheelchair_accessible": "Y" if branch_type != "ATM Only" else random.choice(["Y", "N"]),
        "safe_deposit_boxes": random.randint(50, 500) if "Safe Deposit" in config["services"] else 0,
        "status": weighted_choice({"Active": 0.95, "Temporarily Closed": 0.03, "Closed": 0.02}),
        "monthly_transactions": random.randint(5000, 50000) if branch_type not in ["ATM Only"] else random.randint(500, 5000),
        "customer_satisfaction_score": round(random.uniform(3.5, 5.0), 2)
    }

    return branch


def generate_branches(
    num_branches: int = 150,
    output_file: str = "../data/branches.tsv"
) -> List[Dict[str, Any]]:
    """Generate all branches."""

    branches = []
    branch_id = 1

    # Calculate branches per location based on weights
    total_weight = sum(loc["weight"] for loc in US_BANKING_LOCATIONS)

    print(f"Generating {num_branches} branches across {len(US_BANKING_LOCATIONS)} cities...")

    # First pass: distribute branches to locations
    branches_per_location = []
    for loc in US_BANKING_LOCATIONS:
        count = max(1, int(num_branches * loc["weight"] / total_weight))
        branches_per_location.append((loc, count))

    # Generate branches
    for location, count in branches_per_location:
        # First branch in tier 1 cities might be HQ or Regional
        for i in range(count):
            if i == 0 and location["tier"] == 1 and branch_id == 1:
                branch_type = "Headquarters"
            elif i == 0 and location["tier"] <= 2:
                branch_type = "Regional Center" if random.random() < 0.3 else None
            else:
                branch_type = None

            branch = generate_branch(branch_id, location, branch_type)
            branches.append(branch)
            branch_id += 1

            if branch_id > num_branches:
                break

        if branch_id > num_branches:
            break

    # Write to TSV
    if branches:
        fieldnames = list(branches[0].keys())

        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter='\t')
            writer.writeheader()
            writer.writerows(branches)

        print(f"Saved {len(branches)} branches to {output_file}")

        # Print summary
        type_counts = {}
        for b in branches:
            t = b["branch_type"]
            type_counts[t] = type_counts.get(t, 0) + 1

        print("\nBranch distribution:")
        for t, c in sorted(type_counts.items(), key=lambda x: -x[1]):
            print(f"  {t}: {c}")

    return branches


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Generate synthetic branch data")
    parser.add_argument("--num-branches", type=int, default=150, help="Number of branches")
    parser.add_argument("--output", type=str, default="../data/branches.tsv", help="Output file")
    parser.add_argument("--seed", type=int, default=42, help="Random seed")

    args = parser.parse_args()

    random.seed(args.seed)

    generate_branches(
        num_branches=args.num_branches,
        output_file=args.output
    )
