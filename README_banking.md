# Banking Details JSON — Documentation

## Overview
`banking_details.json` is a comprehensive banking dataset export containing account information, transaction records, audit metadata, and data-quality checks. It follows the same audit and DQ enrichment pattern as the air-quality enrichment script.

## File Structure

### Top-Level Sections

#### 1. **metadata**
- `export_date` — ISO timestamp of data export
- `export_timestamp` — Unix epoch timestamp
- `data_version` — Version of the data format
- `source_system` — Originating system (e.g., CoreBankingSystem)
- `record_count` — Total number of records
- `description` — Brief overview

#### 2. **bank_info**
- `bank_code` — Unique identifier for the bank
- `bank_name` — Full legal name
- `country` — Country of operation
- `swift_code` — SWIFT/BIC code for international transfers
- `registration_number` — Regulatory registration identifier

#### 3. **accounts** (array)
Each account record contains:
- **Core Fields:**
  - `account_id` — Unique account identifier
  - `customer_id` — Link to customer record
  - `account_holder_name` — Name of account owner
  - `account_type` — e.g., Savings, Checking, Money Market
  - `account_status` — Active or Inactive
  - `currency` — ISO 4217 currency code (USD, EUR, etc.)
  - `balance` — Current account balance (nullable)
  - `opening_date` — ISO date account was opened
  - `last_transaction_date` — Most recent transaction date

- **Audit Block:**
  - `created_at` — Account creation timestamp
  - `created_by` — User/system that created the account
  - `modified_at` — Last modification timestamp
  - `modified_by` — User/system that last modified
  - `object_id` — UUID for unique identification
  - `record_hash` — SHA-256 hash of record for integrity checks

- **DQ (Data Quality) Block:**
  - `passed` — Boolean; true if all checks passed
  - `issues` — Array of data-quality issues (empty if passed)

#### 4. **transactions** (array)
Each transaction record contains:
- **Core Fields:**
  - `transaction_id` — Unique transaction identifier
  - `account_id` — Link to account
  - `transaction_type` — Debit, Credit, Transfer, etc.
  - `amount` — Transaction amount (numeric or empty string if invalid)
  - `currency` — ISO 4217 code
  - `transaction_date` — ISO date of transaction
  - `transaction_time` — Time in HH:MM:SS format
  - `description` — Human-readable description
  - `merchant_name` — Name of merchant/counterparty
  - `merchant_category` — Category for transaction classification
  - `status` — Completed, Pending, Failed
  - `reference_number` — External reference for tracking

- **Audit Block:**
  - `recorded_at` — Timestamp transaction was recorded
  - `recorded_by` — Terminal/processor that handled the transaction
  - `verification_status` — verified, pending_verification, failed_verification
  - `object_id` — UUID for unique identification
  - `record_hash` — Hash for integrity verification

- **DQ Block:**
  - `passed` — Boolean indicating data quality
  - `issues` — Array of detected issues

#### 5. **summary**
High-level aggregates and counts:
- Account counts (total, active, inactive)
- Transaction counts and status breakdowns
- Total balances by currency
- DQ pass/fail counts for both accounts and transactions

## Data Quality Checks

The DQ blocks flag common issues:

**Account Issues:**
- `account_inactive_no_recent_transaction` — Inactive account with outdated transactions
- `balance_is_null` — Balance field is null or missing
- Missing required fields

**Transaction Issues:**
- `amount_is_empty` — Amount field is empty or invalid
- `transaction_status_pending` — Transaction is still pending (not yet verified)
- `transaction_status_failed` — Transaction failed
- Date/time parsing errors
- Amount not numeric

## Sample Data Quality Statistics

From the included sample:
- **Accounts:** 5 total; 4 passed DQ, 1 failed
- **Transactions:** 6 total; 4 passed DQ, 2 failed

## Usage

### Read the file
```bash
cat banking_details.json | jq .
```

### Filter accounts with DQ failures
```bash
cat banking_details.json | jq '.accounts[] | select(.dq.passed == false)'
```

### Get transaction summary
```bash
cat banking_details.json | jq '.summary'
```

### Extract all transactions for a specific account
```bash
cat banking_details.json | jq '.transactions[] | select(.account_id == "ACC-2024-001001")'
```

## Integration with Snowflake

To load into Snowflake:
1. Use `COPY INTO` with `file_format = (type = JSON)`
2. Or flatten arrays and load into separate tables (accounts, transactions)
3. Preserve audit and dq fields for lineage and quality tracking

## Next Steps

- **Enrich further:** Add object_id generation + audit timestamps via a Python script (similar to `enrich_aqi.py`)
- **Validate:** Run comprehensive schema and business rule validations
- **Load:** Push to Snowflake or data warehouse with metadata tracking
- **Monitor:** Track DQ metrics over time
