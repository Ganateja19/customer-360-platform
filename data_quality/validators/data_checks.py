"""
Data Quality Checks
====================
Comprehensive data quality validation: null checks, duplicate detection,
range validation, referential integrity, row count reconciliation,
and data freshness monitoring.
"""

import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional

import pandas as pd

logger = logging.getLogger(__name__)


class DataQualityChecker:
    """
    Runs a suite of data quality checks against a pandas DataFrame.

    Parameters
    ----------
    thresholds : dict
        Quality thresholds from pipeline_config.yaml.
    """

    def __init__(self, thresholds: Dict[str, float] = None):
        self.thresholds = thresholds or {
            "null_percentage_max": 5.0,
            "duplicate_percentage_max": 1.0,
            "min_row_count": 100,
            "freshness_hours_max": 6,
        }
        self.results: List[Dict[str, Any]] = []

    def _add_result(
        self, check_name: str, status: str, details: Dict[str, Any]
    ) -> None:
        self.results.append({
            "check_name": check_name,
            "status": status,  # PASS, WARN, FAIL
            "timestamp": datetime.now(timezone.utc).isoformat(),
            **details
        })

    # ── Null Checks ──────────────────────────────────────────

    def check_nulls(
        self, df: pd.DataFrame, required_columns: List[str]
    ) -> Dict[str, Any]:
        """
        Check for null values in required columns.

        Returns FAIL if any required column exceeds the null threshold.
        """
        null_report = {}
        overall_status = "PASS"

        for col in required_columns:
            if col not in df.columns:
                null_report[col] = {"status": "FAIL", "reason": "Column missing"}
                overall_status = "FAIL"
                continue

            null_count = df[col].isnull().sum()
            null_pct = (null_count / len(df)) * 100 if len(df) > 0 else 0

            if null_pct > self.thresholds["null_percentage_max"]:
                status = "FAIL"
                overall_status = "FAIL"
            elif null_pct > 0:
                status = "WARN"
                if overall_status == "PASS":
                    overall_status = "WARN"
            else:
                status = "PASS"

            null_report[col] = {
                "null_count": int(null_count),
                "null_percentage": round(null_pct, 2),
                "threshold": self.thresholds["null_percentage_max"],
                "status": status
            }

        self._add_result("null_check", overall_status, {
            "columns_checked": len(required_columns),
            "column_results": null_report
        })
        return {"status": overall_status, "details": null_report}

    # ── Duplicate Checks ─────────────────────────────────────

    def check_duplicates(
        self, df: pd.DataFrame, key_columns: List[str]
    ) -> Dict[str, Any]:
        """
        Check for duplicate records based on key columns.
        """
        total = len(df)
        duplicates = df.duplicated(subset=key_columns, keep="first")
        dup_count = duplicates.sum()
        dup_pct = (dup_count / total) * 100 if total > 0 else 0

        if dup_pct > self.thresholds["duplicate_percentage_max"]:
            status = "FAIL"
        elif dup_pct > 0:
            status = "WARN"
        else:
            status = "PASS"

        result = {
            "total_records": total,
            "duplicate_count": int(dup_count),
            "duplicate_percentage": round(dup_pct, 2),
            "threshold": self.thresholds["duplicate_percentage_max"],
            "key_columns": key_columns,
        }

        # Sample duplicates for debugging
        if dup_count > 0:
            dup_samples = df[duplicates].head(5)[key_columns].to_dict("records")
            result["sample_duplicates"] = dup_samples

        self._add_result("duplicate_check", status, result)
        return {"status": status, "details": result}

    # ── Range Validation ─────────────────────────────────────

    def check_ranges(
        self, df: pd.DataFrame, range_rules: Dict[str, Dict[str, float]]
    ) -> Dict[str, Any]:
        """
        Validate numeric columns are within expected ranges.

        Parameters
        ----------
        range_rules : dict
            {column_name: {"min": min_val, "max": max_val}}
        """
        overall_status = "PASS"
        range_report = {}

        for col, rules in range_rules.items():
            if col not in df.columns:
                range_report[col] = {"status": "FAIL", "reason": "Column missing"}
                overall_status = "FAIL"
                continue

            violations = pd.Series(False, index=df.index)
            if "min" in rules:
                violations |= df[col] < rules["min"]
            if "max" in rules:
                violations |= df[col] > rules["max"]

            violation_count = violations.sum()
            violation_pct = (violation_count / len(df)) * 100 if len(df) > 0 else 0

            status = "FAIL" if violation_count > 0 else "PASS"
            if status == "FAIL":
                overall_status = "FAIL"

            range_report[col] = {
                "expected_range": rules,
                "violations": int(violation_count),
                "violation_percentage": round(violation_pct, 2),
                "actual_min": float(df[col].min()) if not df[col].empty else None,
                "actual_max": float(df[col].max()) if not df[col].empty else None,
                "status": status
            }

        self._add_result("range_check", overall_status, {
            "columns_checked": len(range_rules),
            "column_results": range_report
        })
        return {"status": overall_status, "details": range_report}

    # ── Row Count Reconciliation ─────────────────────────────

    def check_row_count(
        self,
        df: pd.DataFrame,
        expected_min: int = None,
        expected_max: int = None,
        previous_count: int = None,
        variance_threshold_pct: float = 50.0
    ) -> Dict[str, Any]:
        """
        Validate row count is within expected bounds.
        Also checks for suspicious variance from previous run.
        """
        actual_count = len(df)
        min_count = expected_min or self.thresholds.get("min_row_count", 0)
        status = "PASS"
        issues = []

        if actual_count < min_count:
            status = "FAIL"
            issues.append(f"Row count {actual_count} below minimum {min_count}")

        if expected_max and actual_count > expected_max:
            status = "FAIL"
            issues.append(f"Row count {actual_count} exceeds maximum {expected_max}")

        if previous_count and previous_count > 0:
            variance = abs(actual_count - previous_count) / previous_count * 100
            if variance > variance_threshold_pct:
                if status == "PASS":
                    status = "WARN"
                issues.append(
                    f"Row count variance {variance:.1f}% exceeds threshold {variance_threshold_pct}%"
                )

        result = {
            "actual_count": actual_count,
            "expected_min": min_count,
            "expected_max": expected_max,
            "previous_count": previous_count,
            "issues": issues
        }

        self._add_result("row_count_check", status, result)
        return {"status": status, "details": result}

    # ── Freshness Check ──────────────────────────────────────

    def check_freshness(
        self, df: pd.DataFrame, timestamp_column: str
    ) -> Dict[str, Any]:
        """
        Check if data is stale (latest record older than threshold).
        """
        if timestamp_column not in df.columns:
            result = {"status": "FAIL", "reason": f"Column {timestamp_column} not found"}
            self._add_result("freshness_check", "FAIL", result)
            return result

        try:
            timestamps = pd.to_datetime(df[timestamp_column])
            latest = timestamps.max()
            now = datetime.now(timezone.utc)

            if pd.isna(latest):
                status = "FAIL"
                hours_old = None
            else:
                if latest.tzinfo is None:
                    latest = latest.replace(tzinfo=timezone.utc)
                hours_old = (now - latest).total_seconds() / 3600
                max_hours = self.thresholds["freshness_hours_max"]

                if hours_old > max_hours:
                    status = "WARN"
                else:
                    status = "PASS"

            result = {
                "latest_timestamp": str(latest) if not pd.isna(latest) else None,
                "hours_since_latest": round(hours_old, 2) if hours_old else None,
                "threshold_hours": self.thresholds["freshness_hours_max"],
            }
        except Exception as e:
            status = "FAIL"
            result = {"error": str(e)}

        self._add_result("freshness_check", status, result)
        return {"status": status, "details": result}

    # ── Referential Integrity ────────────────────────────────

    def check_referential_integrity(
        self,
        df: pd.DataFrame,
        column: str,
        reference_values: set
    ) -> Dict[str, Any]:
        """
        Check that all values in a column exist in a reference set.
        Used to validate foreign keys.
        """
        if column not in df.columns:
            result = {"status": "FAIL", "reason": f"Column {column} not found"}
            self._add_result("referential_integrity", "FAIL", result)
            return result

        actual_values = set(df[column].dropna().unique())
        orphaned = actual_values - reference_values
        orphan_count = len(orphaned)

        status = "FAIL" if orphan_count > 0 else "PASS"
        result = {
            "column": column,
            "total_unique_values": len(actual_values),
            "reference_set_size": len(reference_values),
            "orphaned_count": orphan_count,
            "sample_orphans": list(orphaned)[:10] if orphaned else []
        }

        self._add_result("referential_integrity", status, result)
        return {"status": status, "details": result}

    # ── Overall Summary ──────────────────────────────────────

    def get_summary(self) -> Dict[str, Any]:
        """
        Get overall quality check summary.
        """
        statuses = [r["status"] for r in self.results]

        if "FAIL" in statuses:
            overall = "FAIL"
        elif "WARN" in statuses:
            overall = "WARN"
        else:
            overall = "PASS"

        return {
            "overallStatus": overall,
            "totalChecks": len(self.results),
            "passed": statuses.count("PASS"),
            "warnings": statuses.count("WARN"),
            "failed": statuses.count("FAIL"),
            "summary": f"{statuses.count('PASS')}/{len(statuses)} checks passed",
            "checks": self.results
        }
