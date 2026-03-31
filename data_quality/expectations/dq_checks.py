"""
Data Quality Framework for Banking Data Platform.
Runs validation checks against Silver and Gold layers in Snowflake.

Checks include:
    - Row count validation
    - Null checks on critical columns
    - Referential integrity checks
    - Business rule validation
    - Duplicate detection

Usage:
    python data_quality/expectations/dq_checks.py
"""

import json
from datetime import datetime, timezone
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
import snowflake.connector
from ingestion.scripts.logger import get_logger, PipelineLogger

logger = get_logger(__name__)


# ── Enums & Data Classes ──────────────────────────────────────

class CheckStatus(str, Enum):
    PASSED  = "PASSED"
    FAILED  = "FAILED"
    WARNING = "WARNING"


class CheckSeverity(str, Enum):
    CRITICAL = "CRITICAL"
    HIGH     = "HIGH"
    MEDIUM   = "MEDIUM"
    LOW      = "LOW"


@dataclass
class DQCheck:
    """Represents a single data quality check."""
    check_name:    str
    description:   str
    layer:         str
    table:         str
    severity:      CheckSeverity
    query:         str
    expected:      str
    actual:        str        = ""
    status:        CheckStatus = CheckStatus.FAILED
    error_message: str        = ""
    executed_at:   str        = ""


@dataclass
class DQSuite:
    """Collection of DQ checks with summary reporting."""
    suite_name: str
    checks:     list[DQCheck] = field(default_factory=list)

    @property
    def total(self) -> int:
        return len(self.checks)

    @property
    def passed(self) -> int:
        return sum(1 for c in self.checks if c.status == CheckStatus.PASSED)

    @property
    def failed(self) -> int:
        return sum(1 for c in self.checks if c.status == CheckStatus.FAILED)

    @property
    def warnings(self) -> int:
        return sum(1 for c in self.checks if c.status == CheckStatus.WARNING)

    @property
    def pass_rate(self) -> float:
        return (self.passed / self.total * 100) if self.total > 0 else 0

    @property
    def critical_failures(self) -> list[DQCheck]:
        return [
            c for c in self.checks
            if c.status == CheckStatus.FAILED
            and c.severity == CheckSeverity.CRITICAL
        ]


# ── Snowflake Connection ──────────────────────────────────────

def get_snowflake_connection():
    """Create Snowflake connection from environment."""
    from dotenv import load_dotenv
    import os
    load_dotenv("configs/environments/dev.env")

    return snowflake.connector.connect(
        account   = os.getenv("SNOWFLAKE_ACCOUNT"),
        user      = os.getenv("SNOWFLAKE_USER"),
        password  = os.getenv("SNOWFLAKE_PASSWORD"),
        role      = os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
        warehouse = os.getenv("SNOWFLAKE_WAREHOUSE", "BANKING_WH"),
        database  = os.getenv("SNOWFLAKE_DATABASE", "BANKING_DB"),
    )


def run_query(conn, sql: str) -> list:
    """Execute SQL and return results."""
    cursor = conn.cursor()
    cursor.execute(sql)
    return cursor.fetchall()


# ── DQ Check Definitions ──────────────────────────────────────

def get_silver_checks() -> list[DQCheck]:
    """Define all Silver layer DQ checks."""
    return [
        # ── Row Count Checks ──────────────────────────────────
        DQCheck(
            check_name  = "silver_customers_row_count",
            description = "Silver customers must have records",
            layer       = "SILVER",
            table       = "SILVER_CUSTOMERS",
            severity    = CheckSeverity.CRITICAL,
            query       = "SELECT COUNT(*) FROM BANKING_DB.SILVER.SILVER_CUSTOMERS",
            expected    = "> 0",
        ),
        DQCheck(
            check_name  = "silver_transactions_row_count",
            description = "Silver transactions must have records",
            layer       = "SILVER",
            table       = "SILVER_TRANSACTIONS",
            severity    = CheckSeverity.CRITICAL,
            query       = "SELECT COUNT(*) FROM BANKING_DB.SILVER.SILVER_TRANSACTIONS",
            expected    = "> 0",
        ),
        DQCheck(
            check_name  = "silver_accounts_row_count",
            description = "Silver accounts must have records",
            layer       = "SILVER",
            table       = "SILVER_ACCOUNTS",
            severity    = CheckSeverity.CRITICAL,
            query       = "SELECT COUNT(*) FROM BANKING_DB.SILVER.SILVER_ACCOUNTS",
            expected    = "> 0",
        ),

        # ── Null Checks ───────────────────────────────────────
        DQCheck(
            check_name  = "silver_customers_no_null_ids",
            description = "Customer IDs must not be null",
            layer       = "SILVER",
            table       = "SILVER_CUSTOMERS",
            severity    = CheckSeverity.CRITICAL,
            query       = """
                SELECT COUNT(*) FROM BANKING_DB.SILVER.SILVER_CUSTOMERS
                WHERE customer_id IS NULL
            """,
            expected    = "= 0",
        ),
        DQCheck(
            check_name  = "silver_transactions_no_null_amounts",
            description = "Transaction amounts must not be null",
            layer       = "SILVER",
            table       = "SILVER_TRANSACTIONS",
            severity    = CheckSeverity.CRITICAL,
            query       = """
                SELECT COUNT(*) FROM BANKING_DB.SILVER.SILVER_TRANSACTIONS
                WHERE amount IS NULL
            """,
            expected    = "= 0",
        ),
        DQCheck(
            check_name  = "silver_customers_no_null_emails",
            description = "Customer emails must not be null",
            layer       = "SILVER",
            table       = "SILVER_CUSTOMERS",
            severity    = CheckSeverity.HIGH,
            query       = """
                SELECT COUNT(*) FROM BANKING_DB.SILVER.SILVER_CUSTOMERS
                WHERE email IS NULL
            """,
            expected    = "= 0",
        ),

        # ── Duplicate Checks ──────────────────────────────────
        DQCheck(
            check_name  = "silver_customers_no_duplicates",
            description = "Customer IDs must be unique",
            layer       = "SILVER",
            table       = "SILVER_CUSTOMERS",
            severity    = CheckSeverity.CRITICAL,
            query       = """
                SELECT COUNT(*) FROM (
                    SELECT customer_id, COUNT(*) AS cnt
                    FROM BANKING_DB.SILVER.SILVER_CUSTOMERS
                    GROUP BY customer_id HAVING cnt > 1
                )
            """,
            expected    = "= 0",
        ),
        DQCheck(
            check_name  = "silver_transactions_no_duplicates",
            description = "Transaction IDs must be unique",
            layer       = "SILVER",
            table       = "SILVER_TRANSACTIONS",
            severity    = CheckSeverity.CRITICAL,
            query       = """
                SELECT COUNT(*) FROM (
                    SELECT transaction_id, COUNT(*) AS cnt
                    FROM BANKING_DB.SILVER.SILVER_TRANSACTIONS
                    GROUP BY transaction_id HAVING cnt > 1
                )
            """,
            expected    = "= 0",
        ),

        # ── Business Rule Checks ──────────────────────────────
        DQCheck(
            check_name  = "silver_customers_valid_status",
            description = "Customer status must be ACTIVE/INACTIVE/SUSPENDED",
            layer       = "SILVER",
            table       = "SILVER_CUSTOMERS",
            severity    = CheckSeverity.HIGH,
            query       = """
                SELECT COUNT(*) FROM BANKING_DB.SILVER.SILVER_CUSTOMERS
                WHERE status NOT IN ('ACTIVE','INACTIVE','SUSPENDED')
            """,
            expected    = "= 0",
        ),
        DQCheck(
            check_name  = "silver_customers_valid_credit_score",
            description = "Credit scores must be between 300 and 850",
            layer       = "SILVER",
            table       = "SILVER_CUSTOMERS",
            severity    = CheckSeverity.HIGH,
            query       = """
                SELECT COUNT(*) FROM BANKING_DB.SILVER.SILVER_CUSTOMERS
                WHERE credit_score < 300 OR credit_score > 850
            """,
            expected    = "= 0",
        ),
        DQCheck(
            check_name  = "silver_fraud_valid_score_range",
            description = "Fraud scores must be between 0 and 1",
            layer       = "SILVER",
            table       = "SILVER_FRAUD_FLAGS",
            severity    = CheckSeverity.HIGH,
            query       = """
                SELECT COUNT(*) FROM BANKING_DB.SILVER.SILVER_FRAUD_FLAGS
                WHERE fraud_score < 0 OR fraud_score > 1
            """,
            expected    = "= 0",
        ),

        # ── DQ Pass Rate Check ────────────────────────────────
        DQCheck(
            check_name  = "silver_dq_pass_rate",
            description = "At least 95% of records must pass DQ validation",
            layer       = "SILVER",
            table       = "SILVER_CUSTOMERS",
            severity    = CheckSeverity.CRITICAL,
            query       = """
                SELECT
                    ROUND(
                        SUM(CASE WHEN dq_is_valid THEN 1 ELSE 0 END)
                        * 100.0 / COUNT(*), 2
                    )
                FROM BANKING_DB.SILVER.SILVER_CUSTOMERS
            """,
            expected    = ">= 95",
        ),
    ]


def get_gold_checks() -> list[DQCheck]:
    """Define all Gold layer DQ checks."""
    return [
        # ── Row Count Checks ──────────────────────────────────
        DQCheck(
            check_name  = "gold_dim_customers_row_count",
            description = "DIM_CUSTOMERS must have records",
            layer       = "GOLD",
            table       = "DIM_CUSTOMERS",
            severity    = CheckSeverity.CRITICAL,
            query       = "SELECT COUNT(*) FROM BANKING_DB.GOLD.DIM_CUSTOMERS",
            expected    = "> 0",
        ),
        DQCheck(
            check_name  = "gold_fact_transactions_row_count",
            description = "FACT_TRANSACTIONS must have records",
            layer       = "GOLD",
            table       = "FACT_TRANSACTIONS",
            severity    = CheckSeverity.CRITICAL,
            query       = "SELECT COUNT(*) FROM BANKING_DB.GOLD.FACT_TRANSACTIONS",
            expected    = "> 0",
        ),

        # ── Referential Integrity ─────────────────────────────
        DQCheck(
            check_name  = "gold_fact_txn_account_key_integrity",
            description = "All transactions must have valid account keys",
            layer       = "GOLD",
            table       = "FACT_TRANSACTIONS",
            severity    = CheckSeverity.HIGH,
            query       = """
                SELECT COUNT(*) FROM BANKING_DB.GOLD.FACT_TRANSACTIONS
                WHERE account_key IS NULL
            """,
            expected    = "= 0",
        ),
        DQCheck(
            check_name  = "gold_fact_txn_customer_key_integrity",
            description = "All transactions must have valid customer keys",
            layer       = "GOLD",
            table       = "FACT_TRANSACTIONS",
            severity    = CheckSeverity.HIGH,
            query       = """
                SELECT COUNT(*) FROM BANKING_DB.GOLD.FACT_TRANSACTIONS
                WHERE customer_key IS NULL
            """,
            expected    = "= 0",
        ),

        # ── Duplicate Checks ──────────────────────────────────
        DQCheck(
            check_name  = "gold_dim_customers_no_duplicates",
            description = "DIM_CUSTOMERS must have unique customer_ids",
            layer       = "GOLD",
            table       = "DIM_CUSTOMERS",
            severity    = CheckSeverity.CRITICAL,
            query       = """
                SELECT COUNT(*) FROM (
                    SELECT customer_id, COUNT(*) AS cnt
                    FROM BANKING_DB.GOLD.DIM_CUSTOMERS
                    GROUP BY customer_id HAVING cnt > 1
                )
            """,
            expected    = "= 0",
        ),

        # ── Business Rules ────────────────────────────────────
        DQCheck(
            check_name  = "gold_fraud_score_band_valid",
            description = "Fraud score bands must be valid values",
            layer       = "GOLD",
            table       = "FACT_FRAUD",
            severity    = CheckSeverity.MEDIUM,
            query       = """
                SELECT COUNT(*) FROM BANKING_DB.GOLD.FACT_FRAUD
                WHERE fraud_score_band NOT IN
                    ('CRITICAL','HIGH','MEDIUM','LOW')
            """,
            expected    = "= 0",
        ),
        DQCheck(
            check_name  = "gold_dim_date_row_count",
            description = "DIM_DATE must have 2922 rows (2020-2027)",
            layer       = "GOLD",
            table       = "DIM_DATE",
            severity    = CheckSeverity.HIGH,
            query       = "SELECT COUNT(*) FROM BANKING_DB.GOLD.DIM_DATE",
            expected    = "= 2922",
        ),
    ]


# ── Check Runner ──────────────────────────────────────────────

def evaluate_check(actual_value: float, expected: str) -> bool:
    """Evaluate if actual value meets expected condition."""
    expected = expected.strip()
    try:
        if expected.startswith(">= "):
            return actual_value >= float(expected[3:])
        elif expected.startswith("> "):
            return actual_value > float(expected[2:])
        elif expected.startswith("= "):
            return actual_value == float(expected[2:])
        elif expected.startswith("<= "):
            return actual_value <= float(expected[3:])
        elif expected.startswith("< "):
            return actual_value < float(expected[2:])
        else:
            return False
    except Exception:
        return False


def run_dq_suite(suite: DQSuite, conn) -> DQSuite:
    """Execute all checks in a suite and return results."""
    logger.info(f"Running DQ suite: {suite.suite_name}")
    logger.info(f"Total checks: {suite.total}")

    for check in suite.checks:
        check.executed_at = datetime.now(timezone.utc).isoformat()
        try:
            result = run_query(conn, check.query)
            actual_value = float(result[0][0]) if result else 0
            check.actual = str(actual_value)
            check.status = (
                CheckStatus.PASSED
                if evaluate_check(actual_value, check.expected)
                else CheckStatus.FAILED
            )

            if check.status == CheckStatus.PASSED:
                logger.success(
                    f"PASSED | {check.check_name} | "
                    f"Expected: {check.expected} | Actual: {check.actual}"
                )
            else:
                logger.error(
                    f"FAILED | {check.check_name} | "
                    f"Expected: {check.expected} | Actual: {check.actual} | "
                    f"Severity: {check.severity}"
                )

        except Exception as e:
            check.status = CheckStatus.FAILED
            check.error_message = str(e)
            logger.error(f"ERROR | {check.check_name} | {e}")

    return suite


def save_dq_report(suite: DQSuite, output_dir: str = "logs") -> str:
    """Save DQ results to JSON report."""
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    report_file = output_path / f"dq_report_{suite.suite_name}_{timestamp}.json"

    report = {
        "suite_name":        suite.suite_name,
        "generated_at":      datetime.now(timezone.utc).isoformat(),
        "summary": {
            "total":          suite.total,
            "passed":         suite.passed,
            "failed":         suite.failed,
            "warnings":       suite.warnings,
            "pass_rate":      f"{suite.pass_rate:.1f}%",
            "critical_failures": len(suite.critical_failures),
        },
        "checks": [
            {
                "check_name":    c.check_name,
                "description":   c.description,
                "layer":         c.layer,
                "table":         c.table,
                "severity":      c.severity,
                "expected":      c.expected,
                "actual":        c.actual,
                "status":        c.status,
                "error_message": c.error_message,
                "executed_at":   c.executed_at,
            }
            for c in suite.checks
        ],
    }

    with open(report_file, "w") as f:
        json.dump(report, f, indent=2)

    logger.info(f"DQ report saved: {report_file}")
    return str(report_file)


# ── Main ──────────────────────────────────────────────────────

def main():
    with PipelineLogger("data_quality_checks") as log:
        # Connect to Snowflake
        log.info("Connecting to Snowflake...")
        conn = get_snowflake_connection()

        # ── Silver Suite ──────────────────────────────────────
        silver_suite = DQSuite(
            suite_name = "silver_layer",
            checks     = get_silver_checks(),
        )
        silver_suite = run_dq_suite(silver_suite, conn)
        silver_report = save_dq_report(silver_suite)

        # ── Gold Suite ────────────────────────────────────────
        gold_suite = DQSuite(
            suite_name = "gold_layer",
            checks     = get_gold_checks(),
        )
        gold_suite = run_dq_suite(gold_suite, conn)
        gold_report = save_dq_report(gold_suite)

        # ── Summary ───────────────────────────────────────────
        log.info("=" * 60)
        log.info("DATA QUALITY SUMMARY")
        log.info("=" * 60)
        log.info(f"Silver Layer: {silver_suite.passed}/{silver_suite.total} passed ({silver_suite.pass_rate:.1f}%)")
        log.info(f"Gold Layer:   {gold_suite.passed}/{gold_suite.total} passed ({gold_suite.pass_rate:.1f}%)")
        log.info(f"Reports saved to: logs/")
        log.info("=" * 60)

        # Fail pipeline if critical checks fail
        all_critical_failures = (
            silver_suite.critical_failures +
            gold_suite.critical_failures
        )
        if all_critical_failures:
            failed_names = [c.check_name for c in all_critical_failures]
            raise ValueError(
                f"Critical DQ checks failed: {failed_names}"
            )

        conn.close()


if __name__ == "__main__":
    main()