#!/usr/bin/env bash
#
# LDBC FinBench Dataset Generator for Samyama Graph Database
#
# Since LDBC FinBench does not have a widely-available SF1 CSV download like
# LDBC SNB, this script generates synthetic data matching the FinBench schema
# using the Samyama finbench_loader example.
#
# The generated data consists of pipe-delimited CSV files for 5 node types
# (Account, Person, Company, Loan, Medium) and 9 edge types (OWN, TRANSFER,
# WITHDRAW, DEPOSIT, REPAY, SIGN_IN, APPLY, INVEST, GUARANTEE).
#
# Usage:
#   ./scripts/download_finbench.sh                  # Generate to default location
#   ./scripts/download_finbench.sh /custom/data/dir  # Generate to custom location
#
# After generation, run the benchmark:
#   cargo run --release --example finbench_benchmark -- --data-dir data/finbench-sf1
#
# Or use in-memory synthetic generation (no CSV needed):
#   cargo run --release --example finbench_benchmark

set -euo pipefail

DATA_DIR="${1:-data/finbench-sf1}"

echo "================================================================"
echo "  LDBC FinBench Dataset Generator — Samyama"
echo "================================================================"
echo ""
echo "  Target directory: ${DATA_DIR}"
echo ""

# ── Check if data already exists ─────────────────────────────────────
if [ -f "${DATA_DIR}/account.csv" ] && [ -f "${DATA_DIR}/transfer.csv" ]; then
    echo "  [SKIP] FinBench data already exists at ${DATA_DIR}"
    echo ""
    echo "  Files found:"
    for f in "${DATA_DIR}"/*.csv; do
        rows=$(wc -l < "$f" | tr -d ' ')
        name=$(basename "$f")
        printf "    %-30s %s lines\n" "$name" "$rows"
    done
    echo ""
    echo "  To regenerate, remove the directory first:"
    echo "    rm -rf ${DATA_DIR}"
    echo ""
    echo "  To run the benchmark:"
    echo "    cargo run --release --example finbench_benchmark -- --data-dir ${DATA_DIR}"
    exit 0
fi

# ── Try to build and run the generator ───────────────────────────────
echo "  Checking for Rust toolchain..."
if ! command -v cargo &>/dev/null; then
    echo "  ERROR: 'cargo' not found. Install Rust via https://rustup.rs/"
    exit 1
fi

echo "  Building finbench_loader..."
echo ""

if cargo build --release --example finbench_loader 2>&1; then
    echo ""
    echo "  Generating FinBench SF1 dataset..."
    echo ""
    cargo run --release --example finbench_loader -- --generate --data-dir "${DATA_DIR}" 2>&1
    echo ""
else
    echo ""
    echo "  Build failed. Falling back to shell-based data generator..."
    echo ""

    # ── Fallback: Pure shell CSV generator ───────────────────────────
    mkdir -p "${DATA_DIR}"

    NUM_PERSONS=1000
    NUM_COMPANIES=500
    NUM_ACCOUNTS=5000
    NUM_LOANS=1000
    NUM_MEDIUMS=200
    NUM_TRANSFERS=20000
    NUM_WITHDRAWALS=5000
    NUM_DEPOSITS=2000
    NUM_REPAYMENTS=3000
    NUM_SIGN_INS=8000

    ACCOUNT_TYPES=("checking" "savings" "investment" "business")
    MEDIUM_TYPES=("phone" "tablet" "laptop" "desktop" "ATM")
    FIRST_NAMES=("Alice" "Bob" "Charlie" "Diana" "Eve" "Frank" "Grace" "Henry" "Ivy" "Jack")
    LAST_NAMES=("Smith" "Johnson" "Williams" "Brown" "Jones" "Garcia" "Miller" "Davis" "Kumar" "Singh")
    COMPANY_PREFIXES=("Alpha" "Beta" "Gamma" "Delta" "Epsilon" "Zeta" "Atlas" "Nova" "Apex" "Vertex")
    COMPANY_SUFFIXES=("Corp" "Inc" "Ltd" "Group" "Holdings" "Capital" "Technologies" "Financial" "Solutions" "Global")

    # Base timestamp: 2020-01-01
    BASE_TS=1577836800000
    # 3 years in ms
    TIME_RANGE=94608000000

    random_ts() {
        echo $(( BASE_TS + RANDOM * RANDOM % TIME_RANGE ))
    }

    echo "  Generating person.csv..."
    {
        echo "id|name|isBlocked"
        for i in $(seq 1 $NUM_PERSONS); do
            first=${FIRST_NAMES[$((RANDOM % ${#FIRST_NAMES[@]}))]}
            last=${LAST_NAMES[$((RANDOM % ${#LAST_NAMES[@]}))]}
            blocked=$(( RANDOM % 50 == 0 ? 1 : 0 ))
            echo "${i}|${first} ${last}|${blocked}"
        done
    } > "${DATA_DIR}/person.csv"

    echo "  Generating company.csv..."
    {
        echo "id|name|isBlocked"
        for i in $(seq 1 $NUM_COMPANIES); do
            prefix=${COMPANY_PREFIXES[$((RANDOM % ${#COMPANY_PREFIXES[@]}))]}
            suffix=${COMPANY_SUFFIXES[$((RANDOM % ${#COMPANY_SUFFIXES[@]}))]}
            blocked=$(( RANDOM % 100 == 0 ? 1 : 0 ))
            echo "${i}|${prefix} ${suffix}|${blocked}"
        done
    } > "${DATA_DIR}/company.csv"

    echo "  Generating account.csv..."
    {
        echo "id|createTime|isBlocked|accountType"
        for i in $(seq 1 $NUM_ACCOUNTS); do
            acct_type=${ACCOUNT_TYPES[$((RANDOM % ${#ACCOUNT_TYPES[@]}))]}
            blocked=$(( RANDOM % 33 == 0 ? 1 : 0 ))
            echo "${i}|$(random_ts)|${blocked}|${acct_type}"
        done
    } > "${DATA_DIR}/account.csv"

    echo "  Generating loan.csv..."
    {
        echo "id|loanAmount|balance"
        for i in $(seq 1 $NUM_LOANS); do
            amount=$(( 1000 + RANDOM % 499000 ))
            balance=$(( amount * (RANDOM % 100) / 100 ))
            echo "${i}|${amount}.00|${balance}.00"
        done
    } > "${DATA_DIR}/loan.csv"

    echo "  Generating medium.csv..."
    {
        echo "id|mediumType|isBlocked"
        for i in $(seq 1 $NUM_MEDIUMS); do
            mtype=${MEDIUM_TYPES[$((RANDOM % ${#MEDIUM_TYPES[@]}))]}
            blocked=$(( RANDOM % 50 == 0 ? 1 : 0 ))
            echo "${i}|${mtype}|${blocked}"
        done
    } > "${DATA_DIR}/medium.csv"

    echo "  Generating transfer.csv..."
    {
        echo "srcId|tgtId|timestamp|amount"
        for _ in $(seq 1 $NUM_TRANSFERS); do
            src=$(( RANDOM % NUM_ACCOUNTS + 1 ))
            tgt=$(( RANDOM % NUM_ACCOUNTS + 1 ))
            while [ "$tgt" -eq "$src" ]; do tgt=$(( RANDOM % NUM_ACCOUNTS + 1 )); done
            amount=$(( 10 + RANDOM % 50000 ))
            echo "${src}|${tgt}|$(random_ts)|${amount}.00"
        done
    } > "${DATA_DIR}/transfer.csv"

    echo "  Generating personOwnAccount.csv..."
    {
        echo "srcId|tgtId|timestamp"
        for i in $(seq 1 $NUM_PERSONS); do
            num_accounts=$(( RANDOM % 5 + 1 ))
            for _ in $(seq 1 $num_accounts); do
                aid=$(( RANDOM % NUM_ACCOUNTS + 1 ))
                echo "${i}|${aid}|$(random_ts)"
            done
        done
    } > "${DATA_DIR}/personOwnAccount.csv"

    echo "  Generating companyOwnAccount.csv..."
    {
        echo "srcId|tgtId|timestamp"
        for i in $(seq 1 $NUM_COMPANIES); do
            num_accounts=$(( RANDOM % 3 + 1 ))
            for _ in $(seq 1 $num_accounts); do
                aid=$(( RANDOM % NUM_ACCOUNTS + 1 ))
                echo "${i}|${aid}|$(random_ts)"
            done
        done
    } > "${DATA_DIR}/companyOwnAccount.csv"

    echo "  Generating withdraw.csv..."
    {
        echo "srcId|tgtId|timestamp|amount"
        for _ in $(seq 1 $NUM_WITHDRAWALS); do
            src=$(( RANDOM % NUM_ACCOUNTS + 1 ))
            tgt=$(( RANDOM % NUM_ACCOUNTS + 1 ))
            while [ "$tgt" -eq "$src" ]; do tgt=$(( RANDOM % NUM_ACCOUNTS + 1 )); done
            amount=$(( 50 + RANDOM % 20000 ))
            echo "${src}|${tgt}|$(random_ts)|${amount}.00"
        done
    } > "${DATA_DIR}/withdraw.csv"

    echo "  Generating deposit.csv..."
    {
        echo "srcId|tgtId|timestamp|amount"
        for _ in $(seq 1 $NUM_DEPOSITS); do
            lid=$(( RANDOM % NUM_LOANS + 1 ))
            aid=$(( RANDOM % NUM_ACCOUNTS + 1 ))
            amount=$(( 500 + RANDOM % 100000 ))
            echo "${lid}|${aid}|$(random_ts)|${amount}.00"
        done
    } > "${DATA_DIR}/deposit.csv"

    echo "  Generating repay.csv..."
    {
        echo "srcId|tgtId|timestamp|amount"
        for _ in $(seq 1 $NUM_REPAYMENTS); do
            aid=$(( RANDOM % NUM_ACCOUNTS + 1 ))
            lid=$(( RANDOM % NUM_LOANS + 1 ))
            amount=$(( 100 + RANDOM % 50000 ))
            echo "${aid}|${lid}|$(random_ts)|${amount}.00"
        done
    } > "${DATA_DIR}/repay.csv"

    echo "  Generating signIn.csv..."
    {
        echo "srcId|tgtId|timestamp"
        for _ in $(seq 1 $NUM_SIGN_INS); do
            aid=$(( RANDOM % NUM_ACCOUNTS + 1 ))
            mid=$(( RANDOM % NUM_MEDIUMS + 1 ))
            echo "${aid}|${mid}|$(random_ts)"
        done
    } > "${DATA_DIR}/signIn.csv"

    echo "  Generating personApplyLoan.csv..."
    {
        echo "srcId|tgtId|timestamp"
        for i in $(seq 1 $NUM_LOANS); do
            if [ $(( RANDOM % 10 )) -lt 6 ]; then
                pid=$(( RANDOM % NUM_PERSONS + 1 ))
                echo "${pid}|${i}|$(random_ts)"
            fi
        done
    } > "${DATA_DIR}/personApplyLoan.csv"

    echo "  Generating companyApplyLoan.csv..."
    {
        echo "srcId|tgtId|timestamp"
        for i in $(seq 1 $NUM_LOANS); do
            if [ $(( RANDOM % 10 )) -ge 6 ]; then
                cid=$(( RANDOM % NUM_COMPANIES + 1 ))
                echo "${cid}|${i}|$(random_ts)"
            fi
        done
    } > "${DATA_DIR}/companyApplyLoan.csv"

    echo "  Generating companyInvestCompany.csv..."
    {
        echo "srcId|tgtId|timestamp|ratio"
        num_investments=$(( NUM_COMPANIES / 2 ))
        for _ in $(seq 1 $num_investments); do
            src=$(( RANDOM % NUM_COMPANIES + 1 ))
            tgt=$(( RANDOM % NUM_COMPANIES + 1 ))
            while [ "$tgt" -eq "$src" ]; do tgt=$(( RANDOM % NUM_COMPANIES + 1 )); done
            ratio=$(( RANDOM % 100 ))
            echo "${src}|${tgt}|$(random_ts)|0.${ratio}"
        done
    } > "${DATA_DIR}/companyInvestCompany.csv"

    echo "  Generating personInvestCompany.csv..."
    {
        echo "srcId|tgtId|timestamp|ratio"
        num_investments=$(( NUM_COMPANIES / 4 ))
        for _ in $(seq 1 $num_investments); do
            pid=$(( RANDOM % NUM_PERSONS + 1 ))
            cid=$(( RANDOM % NUM_COMPANIES + 1 ))
            ratio=$(( RANDOM % 100 ))
            echo "${pid}|${cid}|$(random_ts)|0.${ratio}"
        done
    } > "${DATA_DIR}/personInvestCompany.csv"

    echo "  Generating companyGuaranteeCompany.csv..."
    {
        echo "srcId|tgtId|timestamp"
        num_guarantees=$(( NUM_PERSONS / 8 ))
        for _ in $(seq 1 $num_guarantees); do
            src=$(( RANDOM % NUM_COMPANIES + 1 ))
            tgt=$(( RANDOM % NUM_COMPANIES + 1 ))
            while [ "$tgt" -eq "$src" ]; do tgt=$(( RANDOM % NUM_COMPANIES + 1 )); done
            echo "${src}|${tgt}|$(random_ts)"
        done
    } > "${DATA_DIR}/companyGuaranteeCompany.csv"

    echo "  Generating personGuaranteePerson.csv..."
    {
        echo "srcId|tgtId|timestamp"
        num_guarantees=$(( NUM_PERSONS / 12 ))
        for _ in $(seq 1 $num_guarantees); do
            src=$(( RANDOM % NUM_PERSONS + 1 ))
            tgt=$(( RANDOM % NUM_PERSONS + 1 ))
            while [ "$tgt" -eq "$src" ]; do tgt=$(( RANDOM % NUM_PERSONS + 1 )); done
            echo "${src}|${tgt}|$(random_ts)"
        done
    } > "${DATA_DIR}/personGuaranteePerson.csv"

    echo ""
fi

# ── Verify ───────────────────────────────────────────────────────────
echo "================================================================"
echo "  Dataset Summary"
echo "================================================================"
echo ""

total_rows=0
for f in "${DATA_DIR}"/*.csv; do
    rows=$(( $(wc -l < "$f" | tr -d ' ') - 1 ))  # subtract header
    total_rows=$(( total_rows + rows ))
    name=$(basename "$f")
    printf "  %-34s %'8d rows\n" "$name" "$rows"
done

echo ""
printf "  %-34s %'8d total rows\n" "TOTAL" "$total_rows"
echo ""
echo "================================================================"
echo "  Generation complete!"
echo ""
echo "  Run the benchmark:"
echo "    cargo run --release --example finbench_benchmark -- --data-dir ${DATA_DIR}"
echo ""
echo "  Or use in-memory synthetic data (faster, no CSV needed):"
echo "    cargo run --release --example finbench_benchmark"
echo ""
echo "  Include write operations:"
echo "    cargo run --release --example finbench_benchmark -- --writes"
echo "================================================================"
