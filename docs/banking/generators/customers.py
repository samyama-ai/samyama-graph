#!/usr/bin/env python3
"""
Enterprise Banking - Customer Data Generator

Generates realistic synthetic customer data including:
- Individual customers (retail banking)
- Corporate customers (business banking)
- High-net-worth individuals (private banking)

Uses realistic name distributions, geographic data, and banking patterns.
"""

import csv
import random
import uuid
from datetime import datetime, timedelta
from typing import List, Dict, Any
import hashlib

# Realistic first names by frequency (US Census data approximation)
MALE_FIRST_NAMES = [
    "James", "Robert", "John", "Michael", "David", "William", "Richard", "Joseph",
    "Thomas", "Christopher", "Charles", "Daniel", "Matthew", "Anthony", "Mark",
    "Donald", "Steven", "Paul", "Andrew", "Joshua", "Kenneth", "Kevin", "Brian",
    "George", "Timothy", "Ronald", "Edward", "Jason", "Jeffrey", "Ryan", "Jacob",
    "Gary", "Nicholas", "Eric", "Jonathan", "Stephen", "Larry", "Justin", "Scott",
    "Brandon", "Benjamin", "Samuel", "Raymond", "Gregory", "Frank", "Alexander",
    "Patrick", "Jack", "Dennis", "Jerry", "Tyler", "Aaron", "Jose", "Adam", "Nathan"
]

FEMALE_FIRST_NAMES = [
    "Mary", "Patricia", "Jennifer", "Linda", "Barbara", "Elizabeth", "Susan",
    "Jessica", "Sarah", "Karen", "Lisa", "Nancy", "Betty", "Margaret", "Sandra",
    "Ashley", "Kimberly", "Emily", "Donna", "Michelle", "Dorothy", "Carol",
    "Amanda", "Melissa", "Deborah", "Stephanie", "Rebecca", "Sharon", "Laura",
    "Cynthia", "Kathleen", "Amy", "Angela", "Shirley", "Anna", "Brenda", "Pamela",
    "Emma", "Nicole", "Helen", "Samantha", "Katherine", "Christine", "Debra",
    "Rachel", "Carolyn", "Janet", "Catherine", "Maria", "Heather", "Diane", "Ruth"
]

LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
    "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson",
    "Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson",
    "White", "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson", "Walker",
    "Young", "Allen", "King", "Wright", "Scott", "Torres", "Nguyen", "Hill", "Flores",
    "Green", "Adams", "Nelson", "Baker", "Hall", "Rivera", "Campbell", "Mitchell",
    "Carter", "Roberts", "Chen", "Wang", "Kim", "Patel", "Shah", "Singh", "Kumar",
    "O'Brien", "Murphy", "Kelly", "Sullivan", "McCarthy", "Cohen", "Goldstein"
]

# Corporate name components
CORP_PREFIXES = [
    "Global", "Advanced", "Premier", "United", "National", "International",
    "Strategic", "Innovative", "Dynamic", "Pacific", "Atlantic", "Continental",
    "American", "Western", "Eastern", "Northern", "Southern", "Central", "Metro"
]

CORP_CORES = [
    "Tech", "Systems", "Solutions", "Services", "Industries", "Enterprises",
    "Holdings", "Partners", "Capital", "Ventures", "Group", "Associates",
    "Consulting", "Manufacturing", "Logistics", "Healthcare", "Energy", "Media"
]

CORP_SUFFIXES = ["Inc", "LLC", "Corp", "Ltd", "Co", "Group", "International"]

# Industry sectors with realistic distribution
INDUSTRIES = {
    "Technology": 0.18,
    "Healthcare": 0.12,
    "Financial Services": 0.10,
    "Manufacturing": 0.10,
    "Retail": 0.09,
    "Real Estate": 0.08,
    "Energy": 0.06,
    "Transportation": 0.05,
    "Construction": 0.05,
    "Professional Services": 0.05,
    "Hospitality": 0.04,
    "Agriculture": 0.03,
    "Telecommunications": 0.03,
    "Education": 0.02
}

# US Cities with realistic banking presence
US_CITIES = [
    {"city": "New York", "state": "NY", "zip_prefix": "100", "weight": 0.12},
    {"city": "Los Angeles", "state": "CA", "zip_prefix": "900", "weight": 0.08},
    {"city": "Chicago", "state": "IL", "zip_prefix": "606", "weight": 0.06},
    {"city": "Houston", "state": "TX", "zip_prefix": "770", "weight": 0.05},
    {"city": "Phoenix", "state": "AZ", "zip_prefix": "850", "weight": 0.04},
    {"city": "Philadelphia", "state": "PA", "zip_prefix": "191", "weight": 0.04},
    {"city": "San Antonio", "state": "TX", "zip_prefix": "782", "weight": 0.03},
    {"city": "San Diego", "state": "CA", "zip_prefix": "921", "weight": 0.03},
    {"city": "Dallas", "state": "TX", "zip_prefix": "752", "weight": 0.03},
    {"city": "San Jose", "state": "CA", "zip_prefix": "951", "weight": 0.03},
    {"city": "Austin", "state": "TX", "zip_prefix": "787", "weight": 0.03},
    {"city": "Jacksonville", "state": "FL", "zip_prefix": "322", "weight": 0.02},
    {"city": "Fort Worth", "state": "TX", "zip_prefix": "761", "weight": 0.02},
    {"city": "Columbus", "state": "OH", "zip_prefix": "432", "weight": 0.02},
    {"city": "Charlotte", "state": "NC", "zip_prefix": "282", "weight": 0.02},
    {"city": "San Francisco", "state": "CA", "zip_prefix": "941", "weight": 0.03},
    {"city": "Indianapolis", "state": "IN", "zip_prefix": "462", "weight": 0.02},
    {"city": "Seattle", "state": "WA", "zip_prefix": "981", "weight": 0.03},
    {"city": "Denver", "state": "CO", "zip_prefix": "802", "weight": 0.02},
    {"city": "Boston", "state": "MA", "zip_prefix": "021", "weight": 0.03},
    {"city": "Nashville", "state": "TN", "zip_prefix": "372", "weight": 0.02},
    {"city": "Detroit", "state": "MI", "zip_prefix": "482", "weight": 0.02},
    {"city": "Portland", "state": "OR", "zip_prefix": "972", "weight": 0.02},
    {"city": "Miami", "state": "FL", "zip_prefix": "331", "weight": 0.03},
    {"city": "Atlanta", "state": "GA", "zip_prefix": "303", "weight": 0.03},
]

# Street name components
STREET_NAMES = [
    "Main", "Oak", "Maple", "Cedar", "Pine", "Elm", "Washington", "Park",
    "Lake", "Hill", "River", "Forest", "Sunset", "Highland", "Valley", "Spring",
    "Church", "Mill", "Center", "Union", "Liberty", "Market", "Court", "Bridge"
]

STREET_TYPES = ["St", "Ave", "Blvd", "Dr", "Ln", "Rd", "Way", "Pl", "Ct"]


def weighted_choice(choices: Dict[str, float]) -> str:
    """Select from weighted choices."""
    items = list(choices.keys())
    weights = list(choices.values())
    return random.choices(items, weights=weights)[0]


def generate_ssn() -> str:
    """Generate a realistic (but fake) SSN format."""
    # Area numbers 001-899 (excluding 666)
    area = random.randint(1, 899)
    while area == 666:
        area = random.randint(1, 899)
    group = random.randint(1, 99)
    serial = random.randint(1, 9999)
    return f"{area:03d}-{group:02d}-{serial:04d}"


def generate_ein() -> str:
    """Generate a realistic EIN (Employer Identification Number)."""
    # EIN format: XX-XXXXXXX
    prefix = random.randint(10, 99)
    suffix = random.randint(1000000, 9999999)
    return f"{prefix}-{suffix}"


def generate_phone() -> str:
    """Generate a realistic US phone number."""
    area_codes = [212, 213, 312, 415, 617, 702, 713, 718, 310, 404, 305, 214, 972, 469, 512, 737]
    area = random.choice(area_codes)
    exchange = random.randint(200, 999)
    subscriber = random.randint(1000, 9999)
    return f"+1-{area}-{exchange}-{subscriber}"


def generate_email(first_name: str, last_name: str, domain_type: str = "personal") -> str:
    """Generate a realistic email address."""
    first = first_name.lower()
    last = last_name.lower().replace("'", "").replace(" ", "")

    if domain_type == "personal":
        domains = ["gmail.com", "yahoo.com", "outlook.com", "icloud.com", "hotmail.com", "aol.com"]
        separators = [".", "_", ""]
        sep = random.choice(separators)
        patterns = [
            f"{first}{sep}{last}",
            f"{first[0]}{sep}{last}",
            f"{first}{sep}{last[0]}",
            f"{first}{last}{random.randint(1, 99)}",
        ]
    else:
        domains = ["company.com", "corp.com", "business.com", "enterprise.com"]
        patterns = [f"{first}.{last}", f"{first[0]}{last}"]

    return f"{random.choice(patterns)}@{random.choice(domains)}"


def generate_address() -> Dict[str, str]:
    """Generate a realistic US address."""
    city_data = random.choices(US_CITIES, weights=[c["weight"] for c in US_CITIES])[0]

    street_num = random.randint(1, 9999)
    street_name = random.choice(STREET_NAMES)
    street_type = random.choice(STREET_TYPES)

    # Apartment/Suite for some addresses
    unit = ""
    if random.random() < 0.3:
        unit_types = ["Apt", "Suite", "Unit", "#"]
        unit = f" {random.choice(unit_types)} {random.randint(1, 500)}"

    zip_suffix = random.randint(10, 99)

    return {
        "street": f"{street_num} {street_name} {street_type}{unit}",
        "city": city_data["city"],
        "state": city_data["state"],
        "zip_code": f"{city_data['zip_prefix']}{zip_suffix}",
        "country": "USA"
    }


def generate_date_of_birth(min_age: int = 18, max_age: int = 85) -> str:
    """Generate a realistic date of birth."""
    today = datetime.now()
    age = random.randint(min_age, max_age)
    days_offset = random.randint(0, 364)
    dob = today - timedelta(days=age*365 + days_offset)
    return dob.strftime("%Y-%m-%d")


def generate_member_since(min_years: int = 0, max_years: int = 20) -> str:
    """Generate a realistic membership start date."""
    today = datetime.now()
    days_ago = random.randint(min_years * 365, max_years * 365)
    member_date = today - timedelta(days=days_ago)
    return member_date.strftime("%Y-%m-%d")


def calculate_risk_score(customer_type: str, kyc_status: str, account_age_days: int) -> int:
    """Calculate a realistic risk score based on customer attributes."""
    base_score = 30

    # Customer type adjustment
    if customer_type == "Individual":
        base_score += random.randint(-5, 15)
    elif customer_type == "Corporate":
        base_score += random.randint(-10, 10)
    elif customer_type == "HighNetWorth":
        base_score += random.randint(-15, 5)

    # KYC status adjustment
    if kyc_status == "Verified":
        base_score -= random.randint(5, 15)
    elif kyc_status == "Pending":
        base_score += random.randint(10, 25)
    elif kyc_status == "PendingReview":
        base_score += random.randint(20, 40)
    elif kyc_status == "Rejected":
        base_score += random.randint(40, 60)

    # Account age adjustment (newer = higher risk)
    if account_age_days < 90:
        base_score += random.randint(10, 20)
    elif account_age_days < 365:
        base_score += random.randint(0, 10)
    elif account_age_days > 1825:  # 5+ years
        base_score -= random.randint(5, 15)

    # Add some randomness
    base_score += random.randint(-5, 5)

    return max(1, min(100, base_score))


def generate_individual_customer(customer_id: int) -> Dict[str, Any]:
    """Generate a realistic individual customer."""
    gender = random.choice(["M", "F"])
    first_name = random.choice(MALE_FIRST_NAMES if gender == "M" else FEMALE_FIRST_NAMES)
    last_name = random.choice(LAST_NAMES)

    address = generate_address()
    member_since = generate_member_since(0, 15)
    member_date = datetime.strptime(member_since, "%Y-%m-%d")
    account_age_days = (datetime.now() - member_date).days

    # KYC status distribution
    kyc_weights = {"Verified": 0.85, "Pending": 0.08, "PendingReview": 0.05, "Rejected": 0.02}
    kyc_status = weighted_choice(kyc_weights)

    # Segment based on various factors
    segments = []
    segments.append("Individual")

    if random.random() < 0.15:
        segments.append("Premium")
    if random.random() < 0.05:
        segments.append("HighRisk")
    if account_age_days > 3650:  # 10+ years
        segments.append("LongTerm")

    dob = generate_date_of_birth(18, 80)

    return {
        "customer_id": f"CUST-{customer_id:08d}",
        "customer_type": "Individual",
        "first_name": first_name,
        "last_name": last_name,
        "full_name": f"{first_name} {last_name}",
        "gender": gender,
        "date_of_birth": dob,
        "ssn_last4": generate_ssn()[-4:],
        "email": generate_email(first_name, last_name, "personal"),
        "phone": generate_phone(),
        "street_address": address["street"],
        "city": address["city"],
        "state": address["state"],
        "zip_code": address["zip_code"],
        "country": address["country"],
        "kyc_status": kyc_status,
        "risk_score": calculate_risk_score("Individual", kyc_status, account_age_days),
        "member_since": member_since,
        "segments": "|".join(segments),
        "preferred_contact": random.choice(["email", "phone", "mail"]),
        "marketing_consent": random.choice(["Y", "N"]),
        "status": "Active" if kyc_status != "Rejected" else "Suspended"
    }


def generate_corporate_customer(customer_id: int) -> Dict[str, Any]:
    """Generate a realistic corporate customer."""
    # Generate company name
    use_founder_name = random.random() < 0.2
    if use_founder_name:
        founder_last = random.choice(LAST_NAMES)
        company_name = f"{founder_last} {random.choice(CORP_CORES)}"
    else:
        company_name = f"{random.choice(CORP_PREFIXES)} {random.choice(CORP_CORES)}"

    suffix = random.choice(CORP_SUFFIXES)
    full_company_name = f"{company_name} {suffix}"

    address = generate_address()
    member_since = generate_member_since(0, 25)
    member_date = datetime.strptime(member_since, "%Y-%m-%d")
    account_age_days = (datetime.now() - member_date).days

    industry = weighted_choice(INDUSTRIES)

    kyc_weights = {"Verified": 0.90, "Pending": 0.05, "PendingReview": 0.04, "Rejected": 0.01}
    kyc_status = weighted_choice(kyc_weights)

    # Company size and revenue
    size_weights = {"Small": 0.60, "Medium": 0.25, "Large": 0.12, "Enterprise": 0.03}
    company_size = weighted_choice(size_weights)

    employee_ranges = {
        "Small": (1, 50),
        "Medium": (51, 500),
        "Large": (501, 5000),
        "Enterprise": (5001, 100000)
    }
    emp_range = employee_ranges[company_size]
    employee_count = random.randint(emp_range[0], emp_range[1])

    revenue_ranges = {
        "Small": (100000, 5000000),
        "Medium": (5000000, 50000000),
        "Large": (50000000, 500000000),
        "Enterprise": (500000000, 50000000000)
    }
    rev_range = revenue_ranges[company_size]
    annual_revenue = random.randint(rev_range[0], rev_range[1])

    segments = ["Corporate"]
    if company_size in ["Large", "Enterprise"]:
        segments.append("Premium")
    if annual_revenue > 100000000:
        segments.append("KeyAccount")

    # Contact person
    contact_gender = random.choice(["M", "F"])
    contact_first = random.choice(MALE_FIRST_NAMES if contact_gender == "M" else FEMALE_FIRST_NAMES)
    contact_last = random.choice(LAST_NAMES)

    return {
        "customer_id": f"CORP-{customer_id:08d}",
        "customer_type": "Corporate",
        "first_name": contact_first,
        "last_name": contact_last,
        "full_name": full_company_name,
        "gender": "",
        "date_of_birth": "",
        "ssn_last4": "",
        "email": f"accounts@{company_name.lower().replace(' ', '')}.com",
        "phone": generate_phone(),
        "street_address": address["street"],
        "city": address["city"],
        "state": address["state"],
        "zip_code": address["zip_code"],
        "country": address["country"],
        "kyc_status": kyc_status,
        "risk_score": calculate_risk_score("Corporate", kyc_status, account_age_days),
        "member_since": member_since,
        "segments": "|".join(segments),
        "preferred_contact": "email",
        "marketing_consent": random.choice(["Y", "N"]),
        "status": "Active" if kyc_status != "Rejected" else "Suspended",
        # Corporate-specific fields
        "ein": generate_ein(),
        "industry": industry,
        "company_size": company_size,
        "employee_count": employee_count,
        "annual_revenue": annual_revenue,
        "contact_name": f"{contact_first} {contact_last}",
        "contact_title": random.choice(["CFO", "Controller", "Finance Director", "Treasury Manager", "CEO"])
    }


def generate_hnw_customer(customer_id: int) -> Dict[str, Any]:
    """Generate a High-Net-Worth individual customer."""
    customer = generate_individual_customer(customer_id)

    # Override with HNW-specific attributes
    customer["customer_id"] = f"HNW-{customer_id:08d}"
    customer["customer_type"] = "HighNetWorth"

    # HNW customers typically have better KYC
    kyc_weights = {"Verified": 0.95, "Pending": 0.03, "PendingReview": 0.02}
    customer["kyc_status"] = weighted_choice(kyc_weights)

    # Lower risk scores for HNW (more scrutiny, better compliance)
    customer["risk_score"] = random.randint(5, 35)

    # Segments
    segments = ["Individual", "HighNetWorth", "Premium", "PrivateBanking"]
    if random.random() < 0.3:
        segments.append("WealthManagement")
    customer["segments"] = "|".join(segments)

    # HNW specific
    customer["net_worth_tier"] = random.choice(["Tier1_1M_5M", "Tier2_5M_25M", "Tier3_25M_Plus"])
    customer["relationship_manager"] = f"RM-{random.randint(1000, 9999)}"

    return customer


def generate_customers(
    num_individual: int = 5000,
    num_corporate: int = 500,
    num_hnw: int = 200,
    output_file: str = "customers.tsv"
) -> List[Dict[str, Any]]:
    """Generate all customer types and save to TSV."""

    customers = []
    customer_id = 1

    print(f"Generating {num_individual} individual customers...")
    for _ in range(num_individual):
        customers.append(generate_individual_customer(customer_id))
        customer_id += 1

    print(f"Generating {num_corporate} corporate customers...")
    for _ in range(num_corporate):
        customers.append(generate_corporate_customer(customer_id))
        customer_id += 1

    print(f"Generating {num_hnw} high-net-worth customers...")
    for _ in range(num_hnw):
        customers.append(generate_hnw_customer(customer_id))
        customer_id += 1

    # Shuffle to mix customer types
    random.shuffle(customers)

    # Write to TSV
    if customers:
        # Get all unique keys
        all_keys = set()
        for c in customers:
            all_keys.update(c.keys())

        fieldnames = sorted(all_keys)

        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter='\t', extrasaction='ignore')
            writer.writeheader()
            writer.writerows(customers)

        print(f"Saved {len(customers)} customers to {output_file}")

    return customers


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Generate synthetic customer data")
    parser.add_argument("--individual", type=int, default=5000, help="Number of individual customers")
    parser.add_argument("--corporate", type=int, default=500, help="Number of corporate customers")
    parser.add_argument("--hnw", type=int, default=200, help="Number of high-net-worth customers")
    parser.add_argument("--output", type=str, default="../data/customers.tsv", help="Output file path")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for reproducibility")

    args = parser.parse_args()

    random.seed(args.seed)

    generate_customers(
        num_individual=args.individual,
        num_corporate=args.corporate,
        num_hnw=args.hnw,
        output_file=args.output
    )
