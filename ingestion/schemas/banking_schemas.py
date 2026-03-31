"""
Pydantic schemas for all banking domain entities.
These act as data contracts — all incoming data must conform to these schemas.
"""

from datetime import datetime, date
from decimal import Decimal
from enum import Enum
from typing import Optional
from pydantic import BaseModel, Field, field_validator
import uuid


# ── Enums ─────────────────────────────────────────────────────────────────────

class AccountType(str, Enum):
    CHECKING = "CHECKING"
    SAVINGS = "SAVINGS"
    LOAN = "LOAN"
    CREDIT = "CREDIT"


class TransactionType(str, Enum):
    DEPOSIT = "DEPOSIT"
    WITHDRAWAL = "WITHDRAWAL"
    TRANSFER = "TRANSFER"
    PAYMENT = "PAYMENT"
    FEE = "FEE"


class TransactionStatus(str, Enum):
    PENDING = "PENDING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    REVERSED = "REVERSED"


class CustomerStatus(str, Enum):
    ACTIVE = "ACTIVE"
    INACTIVE = "INACTIVE"
    SUSPENDED = "SUSPENDED"


class FraudSeverity(str, Enum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


# ── Schemas ───────────────────────────────────────────────────────────────────

class CustomerSchema(BaseModel):
    """Schema for customer master data."""
    customer_id: str = Field(..., description="Unique customer identifier")
    first_name: str = Field(..., min_length=1, max_length=50)
    last_name: str = Field(..., min_length=1, max_length=50)
    email: str = Field(..., description="Customer email address")
    phone: str = Field(..., description="Customer phone number")
    date_of_birth: date = Field(..., description="Customer date of birth")
    address_line1: str = Field(..., max_length=100)
    address_line2: Optional[str] = Field(None, max_length=100)
    city: str = Field(..., max_length=50)
    state: str = Field(..., max_length=2)
    zip_code: str = Field(..., max_length=10)
    country: str = Field(default="US", max_length=2)
    customer_since: date = Field(..., description="Date customer joined")
    status: CustomerStatus = Field(default=CustomerStatus.ACTIVE)
    credit_score: int = Field(..., ge=300, le=850)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    @field_validator("email")
    @classmethod
    def validate_email(cls, v: str) -> str:
        if "@" not in v:
            raise ValueError("Invalid email address")
        return v.lower()


class AccountSchema(BaseModel):
    """Schema for bank account data."""
    account_id: str = Field(..., description="Unique account identifier")
    customer_id: str = Field(..., description="Associated customer ID")
    account_number: str = Field(..., description="Masked account number")
    account_type: AccountType = Field(..., description="Type of account")
    balance: Decimal = Field(..., description="Current account balance")
    available_balance: Decimal = Field(..., description="Available balance")
    currency: str = Field(default="USD", max_length=3)
    interest_rate: Decimal = Field(..., ge=0, description="Annual interest rate")
    opened_date: date = Field(..., description="Account opening date")
    status: str = Field(default="ACTIVE")
    branch_code: str = Field(..., description="Branch identifier")
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)


class TransactionSchema(BaseModel):
    """Schema for financial transaction data."""
    transaction_id: str = Field(..., description="Unique transaction identifier")
    account_id: str = Field(..., description="Associated account ID")
    customer_id: str = Field(..., description="Associated customer ID")
    transaction_type: TransactionType = Field(..., description="Type of transaction")
    amount: Decimal = Field(..., description="Transaction amount")
    currency: str = Field(default="USD", max_length=3)
    balance_after: Decimal = Field(..., description="Account balance after transaction")
    description: str = Field(..., max_length=200)
    merchant_name: Optional[str] = Field(None, max_length=100)
    merchant_category: Optional[str] = Field(None, max_length=50)
    channel: str = Field(..., description="Transaction channel e.g. ATM, ONLINE")
    status: TransactionStatus = Field(default=TransactionStatus.COMPLETED)
    transaction_date: datetime = Field(..., description="Transaction timestamp")
    value_date: date = Field(..., description="Value/settlement date")
    reference_number: str = Field(..., description="External reference number")
    created_at: datetime = Field(default_factory=datetime.utcnow)


class FraudFlagSchema(BaseModel):
    """Schema for fraud detection flags."""
    flag_id: str = Field(..., description="Unique flag identifier")
    transaction_id: str = Field(..., description="Flagged transaction ID")
    account_id: str = Field(..., description="Associated account ID")
    customer_id: str = Field(..., description="Associated customer ID")
    flag_reason: str = Field(..., description="Reason for fraud flag")
    severity: FraudSeverity = Field(..., description="Flag severity level")
    fraud_score: float = Field(..., ge=0.0, le=1.0, description="ML fraud score 0-1")
    is_confirmed_fraud: bool = Field(default=False)
    flagged_at: datetime = Field(default_factory=datetime.utcnow)
    reviewed_at: Optional[datetime] = Field(None)
    reviewed_by: Optional[str] = Field(None)
    created_at: datetime = Field(default_factory=datetime.utcnow)