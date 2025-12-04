"""
Beam transforms and helper functions for the pipeline.
"""
import json
from datetime import datetime, timezone
from typing import Any, Dict

import apache_beam as beam
from apache_beam import window


class StripTimestampedValue(beam.DoFn):
    """Strip TimestampedValue wrapper and return the raw payload."""

    def process(self, element: Any) -> Any:
        # Handle TimestampedValue from window module
        if isinstance(element, window.TimestampedValue):
            yield element.value
        else:
            yield element


class AddIngestionTimestamp(beam.DoFn):
    """Add ingestion timestamp to records for data lineage tracking"""

    def process(self, element: Dict[str, Any]) -> Any:
        output = element.copy()
        output['ingestion_timestamp'] = datetime.now(timezone.utc).isoformat()
        yield output


class FormatForGCS(beam.DoFn):
    """
    Format records for GCS output as JSON Lines (JSONL).
    
    This transform converts dict records to JSON strings suitable for
    WriteToText. Each record becomes one line in the output file.
    
    """

    def __init__(self, event_type: str):
        self.event_type = event_type

    def process(self, element: Dict[str, Any], timestamp=beam.DoFn.TimestampParam) -> Any:
        """
        Convert record to JSON string for GCS storage.
        
        Args:
            element: The record dict to format
            timestamp: Beam timestamp parameter for windowing
            
        Yields:
            JSON string representation of the record
        """
        output = element.copy()
        
        output['_event_type'] = self.event_type
        output['_processing_timestamp'] = datetime.now(timezone.utc).isoformat()
        
        yield json.dumps(output, ensure_ascii=False, default=str)


def encode_for_pubsub(element: str) -> bytes:
    """Encode string to bytes for Pub/Sub publishing."""
    if isinstance(element, bytes):
        return element
    return element.encode('utf-8')
