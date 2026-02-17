"""
Unit Tests — Data Quality Checks
==================================
Tests for schema validation and data quality checker.
"""

import json
import pytest
import pandas as pd
from pathlib import Path

# Adjust import path for the project
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from data_quality.validators.schema_validator import SchemaValidator
from data_quality.validators.data_checks import DataQualityChecker


# ────────────────────────────────────────────────────────────
# Schema Validator Tests
# ────────────────────────────────────────────────────────────

class TestSchemaValidator:
    """Test JSON schema validation."""

    @pytest.fixture
    def validator(self):
        return SchemaValidator()

    def test_valid_clickstream_event(self, validator):
        record = {
            "event_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
            "event_type": "page_view",
            "event_timestamp": "2025-01-15T10:30:00Z",
            "customer_id": "CUST-000001",
            "session_id": "sess-123",
            "channel": "web"
        }
        is_valid, errors = validator.validate_record(record, "clickstream")
        assert is_valid is True
        assert len(errors) == 0

    def test_invalid_clickstream_missing_required(self, validator):
        record = {
            "event_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
            "event_type": "page_view",
            # Missing event_timestamp and customer_id
        }
        is_valid, errors = validator.validate_record(record, "clickstream")
        assert is_valid is False
        assert len(errors) > 0

    def test_invalid_clickstream_bad_event_type(self, validator):
        record = {
            "event_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
            "event_type": "invalid_type",
            "event_timestamp": "2025-01-15T10:30:00Z",
            "customer_id": "CUST-000001"
        }
        is_valid, errors = validator.validate_record(record, "clickstream")
        assert is_valid is False

    def test_valid_customer(self, validator):
        record = {
            "customer_id": "CUST-000001",
            "first_name": "John",
            "last_name": "Doe",
            "email": "john@example.com"
        }
        is_valid, errors = validator.validate_record(record, "customer")
        assert is_valid is True

    def test_valid_transaction(self, validator):
        record = {
            "transaction_id": "tx-001",
            "customer_id": "CUST-000001",
            "product_id": "PROD-0001",
            "transaction_date": "2025-01-15T10:30:00Z",
            "quantity": 2,
            "unit_price": 29.99,
            "total_amount": 59.98
        }
        is_valid, errors = validator.validate_record(record, "transaction")
        assert is_valid is True

    def test_batch_validation(self, validator):
        records = [
            {
                "customer_id": "CUST-000001",
                "first_name": "John",
                "last_name": "Doe",
                "email": "john@example.com"
            },
            {
                "customer_id": "bad-id",
                "first_name": "Jane",
                "last_name": "Doe",
                "email": "not-an-email"
            }
        ]
        result = validator.validate_batch(records, "customer")
        assert result["total_records"] == 2
        assert result["valid_records"] == 1
        assert result["invalid_records"] == 1

    def test_unknown_entity(self, validator):
        is_valid, errors = validator.validate_record({}, "nonexistent")
        assert is_valid is False
        assert "No schema found" in errors[0]


# ────────────────────────────────────────────────────────────
# Data Quality Checker Tests
# ────────────────────────────────────────────────────────────

class TestDataQualityChecker:
    """Test data quality checks."""

    @pytest.fixture
    def checker(self):
        return DataQualityChecker(thresholds={
            "null_percentage_max": 5.0,
            "duplicate_percentage_max": 1.0,
            "min_row_count": 10,
            "freshness_hours_max": 6,
        })

    @pytest.fixture
    def sample_df(self):
        return pd.DataFrame({
            "customer_id": [f"CUST-{i:06d}" for i in range(1, 101)],
            "name": [f"Customer {i}" for i in range(1, 101)],
            "amount": [i * 10.0 for i in range(1, 101)],
            "created_at": pd.date_range("2025-01-01", periods=100, freq="h"),
        })

    def test_null_check_pass(self, checker, sample_df):
        result = checker.check_nulls(sample_df, ["customer_id", "name"])
        assert result["status"] == "PASS"

    def test_null_check_fail(self, checker):
        df = pd.DataFrame({
            "id": [1, 2, None, None, None, None, 7, 8, 9, 10],
            "val": [10, 20, 30, 40, 50, 60, 70, 80, 90, 100],
        })
        result = checker.check_nulls(df, ["id"])
        assert result["status"] == "FAIL"

    def test_duplicate_check_pass(self, checker, sample_df):
        result = checker.check_duplicates(sample_df, ["customer_id"])
        assert result["status"] == "PASS"

    def test_duplicate_check_fail(self, checker):
        df = pd.DataFrame({
            "id": [1, 1, 2, 3, 4],
            "val": [10, 10, 20, 30, 40]
        })
        result = checker.check_duplicates(df, ["id"])
        assert result["status"] in ["WARN", "FAIL"]

    def test_range_check_pass(self, checker, sample_df):
        result = checker.check_ranges(sample_df, {"amount": {"min": 0, "max": 1500}})
        assert result["status"] == "PASS"

    def test_range_check_fail(self, checker, sample_df):
        result = checker.check_ranges(sample_df, {"amount": {"min": 0, "max": 50}})
        assert result["status"] == "FAIL"

    def test_row_count_pass(self, checker, sample_df):
        result = checker.check_row_count(sample_df, expected_min=10)
        assert result["status"] == "PASS"

    def test_row_count_fail(self, checker):
        df = pd.DataFrame({"a": [1, 2]})
        result = checker.check_row_count(df, expected_min=100)
        assert result["status"] == "FAIL"

    def test_referential_integrity_pass(self, checker, sample_df):
        ref = set(f"CUST-{i:06d}" for i in range(1, 200))
        result = checker.check_referential_integrity(sample_df, "customer_id", ref)
        assert result["status"] == "PASS"

    def test_referential_integrity_fail(self, checker, sample_df):
        ref = set(f"CUST-{i:06d}" for i in range(1, 50))  # Missing 50–100
        result = checker.check_referential_integrity(sample_df, "customer_id", ref)
        assert result["status"] == "FAIL"

    def test_summary(self, checker, sample_df):
        checker.check_nulls(sample_df, ["customer_id"])
        checker.check_duplicates(sample_df, ["customer_id"])
        checker.check_row_count(sample_df)
        summary = checker.get_summary()
        assert summary["overallStatus"] == "PASS"
        assert summary["totalChecks"] == 3
