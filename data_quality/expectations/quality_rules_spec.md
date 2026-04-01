# Banking Data Platform — Data Quality Rules Specification

35+ rules across 4 entities — null checks, domain constraints,
business rules, referential integrity, and balance arithmetic.

---

## Rule Naming Convention
```
{LAYER}-{ENTITY}-{RULE_NUMBER} {RULE_TYPE}_{DESCRIPTION}
```

Example: `SQ-001 NULL_CHECK_CUSTOMER_ID`

Prefixes:
- `BQ` — Bronze Quality
- `SQ` — Silver Quality
- `GQ` — Gold Quality

---

## Bronze Layer Rules (BQ)

### BQ-CUSTOMERS

| Rule ID | Rule Name | Description | Severity | Action |
|---|---|---|---|---|
| BQ-001 | ROW_COUNT_NOT_ZERO | Customer file must have records | CRITICAL | Fail pipeline |
| BQ-002 | NO_NULL_CUSTOMER_ID | customer_id must not be null | CRITICAL | Reject record |
| BQ-003 | NO_NULL_EMAIL | email must not be null | HIGH | Reject record |
| BQ-004 | VALID_EMAIL_FORMAT | email must contain @ | HIGH | Reject record |
| BQ-005 | NO_NULL_STATUS | status must not be null | CRITICAL | Reject record |
| BQ-006 | SOURCE_FILE_PRESENT | _source_file must be populated | MEDIUM | Warning |
| BQ-007 | BATCH_ID_PRESENT | _batch_id must be populated | MEDIUM | Warning |

### BQ-ACCOUNTS

| Rule ID | Rule Name | Description | Severity | Action |
|---|---|---|---|---|
| BQ-008 | ROW_COUNT_NOT_ZERO | Account file must have records | CRITICAL | Fail pipeline |
| BQ-009 | NO_NULL_ACCOUNT_ID | account_id must not be null | CRITICAL | Reject record |
| BQ-010 | NO_NULL_CUSTOMER_ID | customer_id must not be null | CRITICAL | Reject record |
| BQ-011 | NO_NULL_ACCOUNT_TYPE | account_type must not be null | CRITICAL | Reject record |
| BQ-012 | NO_NULL_BALANCE | balance must not be null | HIGH | Reject record |

### BQ-TRANSACTIONS

| Rule ID | Rule Name | Description | Severity | Action |
|---|---|---|---|---|
| BQ-013 | ROW_COUNT_NOT_ZERO | Transaction file must have records | CRITICAL | Fail pipeline |
| BQ-014 | NO_NULL_TRANSACTION_ID | transaction_id must not be null | CRITICAL | Reject record |
| BQ-015 | NO_NULL_ACCOUNT_ID | account_id must not be null | CRITICAL | Reject record |
| BQ-016 | NO_NULL_AMOUNT | amount must not be null | CRITICAL | Reject record |
| BQ-017 | NO_NULL_TRANSACTION_DATE | transaction_date must not be null | CRITICAL | Reject record |
| BQ-018 | VALID_TRANSACTION_DATE | transaction_date must be parseable | HIGH | Reject record |

### BQ-FRAUD_FLAGS

| Rule ID | Rule Name | Description | Severity | Action |
|---|---|---|---|---|
| BQ-019 | ROW_COUNT_NOT_ZERO | Fraud file must have records | HIGH | Warning |
| BQ-020 | NO_NULL_FLAG_ID | flag_id must not be null | CRITICAL | Reject record |
| BQ-021 | NO_NULL_TRANSACTION_ID | transaction_id must not be null | CRITICAL | Reject record |
| BQ-022 | NO_NULL_FRAUD_SCORE | fraud_score must not be null | HIGH | Reject record |

---

## Silver Layer Rules (SQ)

### SQ-CUSTOMERS

| Rule ID | Rule Name | Description | Severity | Action |
|---|---|---|---|---|
| SQ-001 | ROW_COUNT_NOT_ZERO | Silver customers must have records | CRITICAL | Fail pipeline |
| SQ-002 | NO_NULL_CUSTOMER_ID | customer_id must not be null | CRITICAL | Reject record |
| SQ-003 | NO_NULL_EMAIL | email must not be null | HIGH | Reject record |
| SQ-004 | NO_DUPLICATE_CUSTOMER_ID | customer_id must be unique | CRITICAL | Reject duplicates |
| SQ-005 | VALID_STATUS_DOMAIN | status in ACTIVE, INACTIVE, SUSPENDED | HIGH | Reject record |
| SQ-006 | VALID_CREDIT_SCORE_RANGE | credit_score between 300 and 850 | HIGH | Reject record |
| SQ-007 | VALID_DATE_OF_BIRTH | date_of_birth must be valid date | MEDIUM | Flag record |
| SQ-008 | VALID_STATE_CODE | state must be 2-letter US code | LOW | Warning |
| SQ-009 | DQ_PASS_RATE_95 | At least 95% records must be valid | CRITICAL | Fail pipeline |

### SQ-ACCOUNTS

| Rule ID | Rule Name | Description | Severity | Action |
|---|---|---|---|---|
| SQ-010 | ROW_COUNT_NOT_ZERO | Silver accounts must have records | CRITICAL | Fail pipeline |
| SQ-011 | NO_NULL_ACCOUNT_ID | account_id must not be null | CRITICAL | Reject record |
| SQ-012 | NO_NULL_CUSTOMER_ID | customer_id must not be null | CRITICAL | Reject record |
| SQ-013 | NO_DUPLICATE_ACCOUNT_ID | account_id must be unique | CRITICAL | Reject duplicates |
| SQ-014 | VALID_ACCOUNT_TYPE | account_type in valid domain | CRITICAL | Reject record |
| SQ-015 | VALID_BALANCE_TYPE | balance must be numeric | HIGH | Reject record |

### SQ-TRANSACTIONS

| Rule ID | Rule Name | Description | Severity | Action |
|---|---|---|---|---|
| SQ-016 | ROW_COUNT_NOT_ZERO | Silver transactions must have records | CRITICAL | Fail pipeline |
| SQ-017 | NO_NULL_TRANSACTION_ID | transaction_id must not be null | CRITICAL | Reject record |
| SQ-018 | NO_NULL_AMOUNT | amount must not be null | CRITICAL | Reject record |
| SQ-019 | NO_DUPLICATE_TRANSACTION_ID | transaction_id must be unique | CRITICAL | Reject duplicates |
| SQ-020 | VALID_TRANSACTION_TYPE | transaction_type in valid domain | HIGH | Reject record |
| SQ-021 | VALID_CHANNEL | channel in ATM, ONLINE, MOBILE, BRANCH, POS | MEDIUM | Flag record |
| SQ-022 | VALID_STATUS | status in valid domain | HIGH | Reject record |
| SQ-023 | AMOUNT_NOT_ZERO | amount must be non-zero | HIGH | Reject record |

### SQ-FRAUD_FLAGS

| Rule ID | Rule Name | Description | Severity | Action |
|---|---|---|---|---|
| SQ-024 | ROW_COUNT_NOT_ZERO | Silver fraud flags must have records | HIGH | Warning |
| SQ-025 | VALID_FRAUD_SCORE_RANGE | fraud_score between 0.0 and 1.0 | HIGH | Reject record |
| SQ-026 | VALID_SEVERITY_DOMAIN | severity in LOW, MEDIUM, HIGH, CRITICAL | HIGH | Reject record |
| SQ-027 | NO_DUPLICATE_FLAG_ID | flag_id must be unique | CRITICAL | Reject duplicates |

---

## Gold Layer Rules (GQ)

### GQ-DIMENSIONS

| Rule ID | Rule Name | Description | Severity | Action |
|---|---|---|---|---|
| GQ-001 | DIM_CUSTOMERS_ROW_COUNT | DIM_CUSTOMERS must have records | CRITICAL | Fail pipeline |
| GQ-002 | DIM_ACCOUNTS_ROW_COUNT | DIM_ACCOUNTS must have records | CRITICAL | Fail pipeline |
| GQ-003 | DIM_DATE_ROW_COUNT | DIM_DATE must have 2922 rows | HIGH | Alert |
| GQ-004 | DIM_CUSTOMERS_NO_DUPES | customer_id unique in DIM_CUSTOMERS | CRITICAL | Alert |
| GQ-005 | DIM_ACCOUNTS_NO_DUPES | account_id unique in DIM_ACCOUNTS | CRITICAL | Alert |
| GQ-006 | SCD2_IS_CURRENT_FLAG | Only one current record per customer | CRITICAL | Alert |

### GQ-FACTS

| Rule ID | Rule Name | Description | Severity | Action |
|---|---|---|---|---|
| GQ-007 | FACT_TXN_ROW_COUNT | FACT_TRANSACTIONS must have records | CRITICAL | Fail pipeline |
| GQ-008 | FACT_FRAUD_ROW_COUNT | FACT_FRAUD must have records | HIGH | Warning |
| GQ-009 | FACT_TXN_ACCOUNT_KEY_RI | All transactions have valid account_key | HIGH | Alert |
| GQ-010 | FACT_TXN_CUSTOMER_KEY_RI | All transactions have valid customer_key | HIGH | Alert |
| GQ-011 | FACT_TXN_DATE_KEY_RI | All transactions have valid date_key | MEDIUM | Alert |
| GQ-012 | FACT_FRAUD_SCORE_BAND | fraud_score_band in valid domain | MEDIUM | Alert |
| GQ-013 | FACT_TXN_NO_DUPES | transaction_id unique in FACT | CRITICAL | Alert |

---

## Reject Reason Codes

| Code | Description | Example |
|---|---|---|
| `NULL_PK` | Primary key is null | customer_id is null |
| `INVALID_DOMAIN` | Value not in allowed domain | status = 'UNKNOWN' |
| `OUT_OF_RANGE` | Value outside allowed range | credit_score = 900 |
| `INVALID_FORMAT` | Value format is incorrect | email missing @ |
| `DUPLICATE_KEY` | Duplicate primary key | Same transaction_id twice |
| `INVALID_DATE` | Date cannot be parsed | date_of_birth = 'N/A' |
| `REFERENTIAL_INTEGRITY` | FK not found in parent | account_id not in ACCOUNTS |
| `ARITHMETIC_FAILURE` | Balance calculation mismatch | balance_after incorrect |
| `ZERO_AMOUNT` | Transaction amount is zero | amount = 0.00 |

---

## DQ Check Results — Latest Run

| Layer | Total Checks | Passed | Failed | Pass Rate |
|---|---|---|---|---|
| Silver | 12 | 12 | 0 | 100% |
| Gold | 7 | 7 | 0 | 100% |
| **Total** | **19** | **19** | **0** | **100%** |

Last run: 2026-03-31 10:17:32 UTC

---

## Escalation Matrix

| Severity | Response Time | Escalation |
|---|---|---|
| CRITICAL | 15 minutes | On-call engineer + data lead |
| HIGH | 1 hour | Data team |
| MEDIUM | 4 hours | Data engineer |
| LOW | Next business day | Logged only |