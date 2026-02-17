"""
Quality Report Generator
=========================
Generates data quality reports and writes results to S3 quality logs.
"""

import json
import logging
from datetime import datetime, timezone
from typing import Dict, Any, List

import boto3

logger = logging.getLogger(__name__)


class QualityReporter:
    """
    Generates and stores data quality reports.
    """

    def __init__(self, bucket: str = "c360-quality-logs-dev", region: str = "us-east-1"):
        self.bucket = bucket
        self.s3 = boto3.client("s3", region_name=region)

    def generate_report(
        self,
        entity: str,
        schema_results: Dict[str, Any],
        quality_results: Dict[str, Any],
        process_date: str
    ) -> Dict[str, Any]:
        """
        Generate a comprehensive quality report combining schema and data checks.
        """
        now = datetime.now(timezone.utc)

        report = {
            "report_id": f"{entity}-{process_date}-{now.strftime('%H%M%S')}",
            "entity": entity,
            "process_date": process_date,
            "generated_at": now.isoformat(),
            "schema_validation": schema_results,
            "data_quality": quality_results,
            "overall_status": self._determine_overall_status(schema_results, quality_results),
        }

        return report

    def _determine_overall_status(
        self,
        schema_results: Dict[str, Any],
        quality_results: Dict[str, Any]
    ) -> str:
        """Determine overall status from combined results."""
        statuses = []
        if schema_results:
            statuses.append(schema_results.get("status", "PASS"))
        if quality_results:
            statuses.append(quality_results.get("overallStatus", "PASS"))

        if "FAIL" in statuses:
            return "FAIL"
        elif "WARN" in statuses:
            return "WARN"
        return "PASS"

    def save_report(self, report: Dict[str, Any]) -> str:
        """
        Save quality report to S3.

        Returns the S3 URI of the saved report.
        """
        entity = report["entity"]
        process_date = report["process_date"]
        now = datetime.now(timezone.utc)

        key = (
            f"quality-reports/{entity}/"
            f"year={now.strftime('%Y')}/"
            f"month={now.strftime('%m')}/"
            f"day={now.strftime('%d')}/"
            f"{report['report_id']}.json"
        )

        self.s3.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=json.dumps(report, indent=2, default=str).encode("utf-8"),
            ContentType="application/json",
            ServerSideEncryption="aws:kms"
        )

        s3_uri = f"s3://{self.bucket}/{key}"
        logger.info("Quality report saved → %s  status=%s", s3_uri, report["overall_status"])
        return s3_uri

    def save_bad_records(
        self,
        records: List[Dict[str, Any]],
        entity: str,
        process_date: str,
        reason: str
    ) -> str:
        """
        Quarantine bad records to S3 for investigation.
        """
        now = datetime.now(timezone.utc)
        key = (
            f"quarantine/{entity}/"
            f"year={now.strftime('%Y')}/"
            f"month={now.strftime('%m')}/"
            f"day={now.strftime('%d')}/"
            f"bad_records_{reason}_{now.strftime('%H%M%S')}.json"
        )

        body = "\n".join(json.dumps(r, default=str) for r in records)
        self.s3.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=body.encode("utf-8"),
            ContentType="application/json",
            ServerSideEncryption="aws:kms"
        )

        s3_uri = f"s3://{self.bucket}/{key}"
        logger.info("Quarantined %d records → %s  reason=%s", len(records), s3_uri, reason)
        return s3_uri

    def print_report(self, report: Dict[str, Any]) -> None:
        """Print a human-readable quality report to stdout."""
        print("\n" + "=" * 70)
        print(f"  DATA QUALITY REPORT: {report['entity'].upper()}")
        print(f"  Date: {report['process_date']}  |  Status: {report['overall_status']}")
        print("=" * 70)

        # Schema validation
        sv = report.get("schema_validation", {})
        if sv:
            print(f"\n  📋 Schema Validation: {sv.get('status', 'N/A')}")
            print(f"     Valid: {sv.get('valid_records', 'N/A')} / {sv.get('total_records', 'N/A')}")

        # Data quality checks
        dq = report.get("data_quality", {})
        if dq:
            print(f"\n  🔍 Data Quality: {dq.get('overallStatus', 'N/A')}")
            print(f"     Checks: {dq.get('totalChecks', 0)}  "
                  f"Passed: {dq.get('passed', 0)}  "
                  f"Warnings: {dq.get('warnings', 0)}  "
                  f"Failed: {dq.get('failed', 0)}")

            for check in dq.get("checks", []):
                icon = "✅" if check["status"] == "PASS" else "⚠️" if check["status"] == "WARN" else "❌"
                print(f"     {icon} {check['check_name']}: {check['status']}")

        print("\n" + "=" * 70)
