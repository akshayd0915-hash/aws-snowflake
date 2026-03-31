# Banking Data Platform — Regulatory Compliance Notes

---

## BSA / AML (Bank Secrecy Act / Anti-Money Laundering)

### Requirements
- Customer identification and KYC status must be historically tracked
- Suspicious transaction patterns must be flagged and reviewed
- Transaction monitoring thresholds must be enforced

### Implementation
| Requirement | Implementation |
|---|---|
| KYC history | SCD Type 2 on `DIM_CUSTOMERS` — tracks status changes |
| Transaction monitoring | `FACT_FRAUD` — ML fraud scores, severity ranking |
| Suspicious activity | `SILVER_FRAUD_FLAGS` — flag reasons, review SLA tracking |
| AML reporting | Gold layer queries on `FACT_TRANSACTIONS` + `FACT_FRAUD` |

---

## GLBA (Gramm-Leach-Bliley Act)

### Requirements
- Customer financial information must be protected
- PII must be classified and access controlled
- Data sharing must be governed

### Implementation
| Requirement | Implementation |
|---|---|
| PII protection | Role-based access — `BANKING_ANALYST_ROLE` read Gold only |
| Data classification | PII fields documented in data governance spec |
| Access control | Snowflake RBAC — pipeline vs analyst roles |
| Masked data | Account numbers masked `****XXXX` at ingestion |

---

## SOX (Sarbanes-Oxley Act)

### Requirements
- Complete audit trail of all data changes
- Immutable financial records
- Pipeline run documentation

### Implementation
| Requirement | Implementation |
|---|---|
| Audit trail | `AUDIT.PIPELINE_RUN_LOG` — every pipeline run logged |
| Immutable records | Bronze layer is append-only — never modified |
| Reject records | `ERROR.REJECT_RECORDS` — no silent data drops |
| Run documentation | Upload manifest JSON saved per pipeline run |
| Change history | Git commit history — full code change audit trail |

---

## PCI-DSS (Payment Card Industry Data Security Standard)

### Requirements
- Cardholder data must be protected
- Account numbers must never be stored in plain text
- Access to financial data must be restricted

### Implementation
| Requirement | Implementation |
|---|---|
| Masked account numbers | `****XXXX` format enforced at ingestion schema level |
| No raw card data | Card numbers never stored — reference numbers only |
| Access restriction | Snowflake RBAC enforced at schema level |
| Encrypted storage | S3 AES256 server-side encryption on all files |
| Encrypted transit | Snowflake TLS for all connections |

---

## Summary Compliance Matrix

| Control | BSA/AML | GLBA | SOX | PCI-DSS |
|---|---|---|---|---|
| KYC status history (SCD2) | ✅ | — | — | — |
| PII classification | — | ✅ | — | — |
| Role-based access | ✅ | ✅ | — | ✅ |
| Immutable Bronze layer | — | — | ✅ | — |
| Pipeline audit log | ✅ | — | ✅ | — |
| Reject record tracking | — | — | ✅ | — |
| Masked account numbers | — | ✅ | — | ✅ |
| Encrypted storage (AES256) | — | ✅ | — | ✅ |
| Git audit trail | — | — | ✅ | — |