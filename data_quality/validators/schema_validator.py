"""
Schema Validator
=================
Validates data records against JSON schema definitions.
Used as a pre-load quality gate in the ETL pipeline.
"""

import json
import logging
from pathlib import Path
from typing import Dict, List, Any, Tuple

from jsonschema import validate, ValidationError, Draft7Validator

logger = logging.getLogger(__name__)

SCHEMAS_DIR = Path(__file__).parent.parent / "schemas"


class SchemaValidator:
    """Validates records against JSON schemas."""

    def __init__(self):
        self._schemas: Dict[str, dict] = {}
        self._validators: Dict[str, Draft7Validator] = {}
        self._load_schemas()

    def _load_schemas(self) -> None:
        """Load all JSON schema files from the schemas directory."""
        for schema_file in SCHEMAS_DIR.glob("*.json"):
            entity = schema_file.stem.replace("_schema", "")
            with open(schema_file) as f:
                schema = json.load(f)
            self._schemas[entity] = schema
            self._validators[entity] = Draft7Validator(schema)
            logger.info("Loaded schema: %s", entity)

    @property
    def available_entities(self) -> List[str]:
        """Return list of entities with loaded schemas."""
        return list(self._schemas.keys())

    def validate_record(
        self, record: Dict[str, Any], entity: str
    ) -> Tuple[bool, List[str]]:
        """
        Validate a single record against an entity schema.

        Returns
        -------
        tuple
            (is_valid, list_of_error_messages)
        """
        if entity not in self._validators:
            return False, [f"No schema found for entity: {entity}"]

        errors = []
        for error in self._validators[entity].iter_errors(record):
            errors.append(f"{error.json_path}: {error.message}")

        return len(errors) == 0, errors

    def validate_batch(
        self,
        records: List[Dict[str, Any]],
        entity: str
    ) -> Dict[str, Any]:
        """
        Validate a batch of records.

        Returns
        -------
        dict
            Validation summary with counts, error details, and sample bad records.
        """
        total = len(records)
        valid_count = 0
        invalid_count = 0
        error_details: List[Dict[str, Any]] = []

        for i, record in enumerate(records):
            is_valid, errors = self.validate_record(record, entity)
            if is_valid:
                valid_count += 1
            else:
                invalid_count += 1
                if len(error_details) < 100:  # Cap error samples
                    error_details.append({
                        "record_index": i,
                        "errors": errors,
                        "record_sample": {
                            k: v for k, v in list(record.items())[:5]
                        }
                    })

        return {
            "entity": entity,
            "total_records": total,
            "valid_records": valid_count,
            "invalid_records": invalid_count,
            "validity_percentage": round(valid_count / max(total, 1) * 100, 2),
            "status": "PASS" if invalid_count == 0 else "FAIL",
            "error_samples": error_details[:10],
        }
