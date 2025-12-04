"""
Integration tests for the event processing pipeline.

Tests cover:
- Event routing to correct output tags
- End-to-end pipeline behavior with DirectRunner
- Multi-event batch processing
- Error handling and DLQ routing
"""
import json
import sys
import os
from typing import List, Any

import pytest

# Add app directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'app'))

import apache_beam as beam
from apache_beam import window
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to, is_not_empty

from src.parsing import ParseAndRoute, Tags
from src.transforms import StripTimestampedValue, AddIngestionTimestamp, FormatForGCS
from conftest import (
    create_order_event,
    create_inventory_event,
    create_activity_event,
    encode_event,
    extract_timestamped_values,
)


def has_length(expected_length: int):
    """Matcher that checks collection length."""
    def _matcher(elements: List[Any]) -> None:
        actual_length = len(list(elements))
        assert actual_length == expected_length, \
            f"Expected {expected_length} elements, got {actual_length}"
    return _matcher


def contains_field(field_name: str):
    """Matcher that checks if all elements contain a field."""
    def _matcher(elements: List[Any]) -> None:
        for element in elements:
            # Handle TimestampedValue wrappers
            if isinstance(element, window.TimestampedValue):
                element = element.value
            assert field_name in element, f"Field '{field_name}' not found in {element}"
    return _matcher


def is_empty():
    """Matcher that checks collection is empty."""
    def _matcher(elements: List[Any]) -> None:
        elements_list = list(elements)
        assert len(elements_list) == 0, f"Expected empty, got {len(elements_list)} elements"
    return _matcher


class TestEventRouting:
    """Tests for event parsing and routing."""

    def test_order_routes_to_order_tag(self, sample_order_event):
        """Order events are routed to the ORDER tag."""
        event_bytes = encode_event(sample_order_event)
        
        with TestPipeline() as p:
            output = (
                p
                | beam.Create([event_bytes])
                | beam.ParDo(ParseAndRoute()).with_outputs(
                    Tags.ORDER, Tags.INVENTORY, Tags.ACTIVITY, Tags.ERROR
                )
            )
            
            # Strip TimestampedValue for assertion
            orders = output[Tags.ORDER] | 'Strip' >> beam.ParDo(StripTimestampedValue())
            
            assert_that(orders, has_length(1), label='OrderCount')
            assert_that(orders, contains_field('order_id'), label='HasOrderId')
            assert_that(output[Tags.ERROR] | 'StripErr' >> beam.ParDo(StripTimestampedValue()), 
                       is_empty(), label='NoErrors')

    def test_inventory_routes_to_inventory_tag(self, sample_inventory_event):
        """Inventory events are routed to the INVENTORY tag."""
        event_bytes = encode_event(sample_inventory_event)
        
        with TestPipeline() as p:
            output = (
                p
                | beam.Create([event_bytes])
                | beam.ParDo(ParseAndRoute()).with_outputs(
                    Tags.ORDER, Tags.INVENTORY, Tags.ACTIVITY, Tags.ERROR
                )
            )
            
            inventory = output[Tags.INVENTORY] | 'Strip' >> beam.ParDo(StripTimestampedValue())
            
            assert_that(inventory, has_length(1), label='InventoryCount')
            assert_that(inventory, contains_field('inventory_id'), label='HasInventoryId')

    def test_activity_routes_to_activity_tag(self, sample_activity_event):
        """User activity events are routed to the ACTIVITY tag."""
        event_bytes = encode_event(sample_activity_event)
        
        with TestPipeline() as p:
            output = (
                p
                | beam.Create([event_bytes])
                | beam.ParDo(ParseAndRoute()).with_outputs(
                    Tags.ORDER, Tags.INVENTORY, Tags.ACTIVITY, Tags.ERROR
                )
            )
            
            activity = output[Tags.ACTIVITY] | 'Strip' >> beam.ParDo(StripTimestampedValue())
            
            assert_that(activity, has_length(1), label='ActivityCount')
            assert_that(activity, contains_field('user_id'), label='HasUserId')


class TestBatchProcessing:
    """Tests for processing multiple events."""

    def test_mixed_events_route_to_correct_tags(self, sample_events_batch):
        """Multiple event types are routed to their respective tags."""
        with TestPipeline() as p:
            output = (
                p
                | beam.Create(sample_events_batch)
                | beam.ParDo(ParseAndRoute()).with_outputs(
                    Tags.ORDER, Tags.INVENTORY, Tags.ACTIVITY, Tags.ERROR
                )
            )
            
            orders = output[Tags.ORDER] | 'StripOrders' >> beam.ParDo(StripTimestampedValue())
            inventory = output[Tags.INVENTORY] | 'StripInv' >> beam.ParDo(StripTimestampedValue())
            activity = output[Tags.ACTIVITY] | 'StripAct' >> beam.ParDo(StripTimestampedValue())
            errors = output[Tags.ERROR] | 'StripErr' >> beam.ParDo(StripTimestampedValue())
            
            assert_that(orders, has_length(1), label='OrderCount')
            assert_that(inventory, has_length(1), label='InventoryCount')
            assert_that(activity, has_length(1), label='ActivityCount')
            assert_that(errors, is_empty(), label='NoErrors')

    def test_large_batch_processing(self):
        """Pipeline handles large batches efficiently."""
        # Create 100 events of each type
        events = []
        for i in range(100):
            events.append(encode_event(create_order_event(order_id=f"ord-{i}")))
            events.append(encode_event(create_inventory_event(inventory_id=f"inv-{i}")))
            events.append(encode_event(create_activity_event(user_id=f"user-{i}")))
        
        with TestPipeline() as p:
            output = (
                p
                | beam.Create(events)
                | beam.ParDo(ParseAndRoute()).with_outputs(
                    Tags.ORDER, Tags.INVENTORY, Tags.ACTIVITY, Tags.ERROR
                )
            )
            
            orders = output[Tags.ORDER] | 'StripOrders' >> beam.ParDo(StripTimestampedValue())
            inventory = output[Tags.INVENTORY] | 'StripInv' >> beam.ParDo(StripTimestampedValue())
            activity = output[Tags.ACTIVITY] | 'StripAct' >> beam.ParDo(StripTimestampedValue())
            
            assert_that(orders, has_length(100), label='OrderCount')
            assert_that(inventory, has_length(100), label='InventoryCount')
            assert_that(activity, has_length(100), label='ActivityCount')


class TestErrorHandling:
    """Tests for error handling and DLQ routing."""

    def test_invalid_json_routes_to_dlq(self, invalid_json_event):
        """Invalid JSON is routed to the ERROR tag."""
        with TestPipeline() as p:
            output = (
                p
                | beam.Create([invalid_json_event])
                | beam.ParDo(ParseAndRoute()).with_outputs(
                    Tags.ORDER, Tags.INVENTORY, Tags.ACTIVITY, Tags.ERROR
                )
            )
            
            errors = output[Tags.ERROR] | 'Strip' >> beam.ParDo(StripTimestampedValue())
            orders = output[Tags.ORDER] | 'StripOrders' >> beam.ParDo(StripTimestampedValue())
            
            assert_that(errors, has_length(1), label='ErrorCount')
            assert_that(orders, is_empty(), label='NoOrders')

    def test_missing_event_type_routes_to_dlq(self, missing_event_type):
        """Events without event_type are routed to the ERROR tag."""
        with TestPipeline() as p:
            output = (
                p
                | beam.Create([missing_event_type])
                | beam.ParDo(ParseAndRoute()).with_outputs(
                    Tags.ORDER, Tags.INVENTORY, Tags.ACTIVITY, Tags.ERROR
                )
            )
            
            errors = output[Tags.ERROR] | 'Strip' >> beam.ParDo(StripTimestampedValue())
            
            def check_error_message(elements):
                for elem in elements:
                    error_data = json.loads(elem)
                    assert "event_type" in error_data.get("error_message", "").lower()
            
            assert_that(errors, has_length(1), label='ErrorCount')
            assert_that(errors, check_error_message, label='ErrorMessage')

    def test_unknown_event_type_routes_to_dlq(self, unknown_event_type):
        """Events with unknown event_type are routed to the ERROR tag."""
        with TestPipeline() as p:
            output = (
                p
                | beam.Create([unknown_event_type])
                | beam.ParDo(ParseAndRoute()).with_outputs(
                    Tags.ORDER, Tags.INVENTORY, Tags.ACTIVITY, Tags.ERROR
                )
            )
            
            errors = output[Tags.ERROR] | 'Strip' >> beam.ParDo(StripTimestampedValue())
            
            assert_that(errors, has_length(1), label='ErrorCount')

    def test_order_missing_required_field_routes_to_dlq(self, order_missing_required_field):
        """Order events missing required fields are routed to the ERROR tag."""
        with TestPipeline() as p:
            output = (
                p
                | beam.Create([order_missing_required_field])
                | beam.ParDo(ParseAndRoute()).with_outputs(
                    Tags.ORDER, Tags.INVENTORY, Tags.ACTIVITY, Tags.ERROR
                )
            )
            
            errors = output[Tags.ERROR] | 'Strip' >> beam.ParDo(StripTimestampedValue())
            orders = output[Tags.ORDER] | 'StripOrders' >> beam.ParDo(StripTimestampedValue())
            
            assert_that(errors, has_length(1), label='ErrorCount')
            assert_that(orders, is_empty(), label='NoOrders')

    def test_error_preserves_original_data(self, invalid_json_event):
        """Error payload contains the original raw data for debugging."""
        with TestPipeline() as p:
            output = (
                p
                | beam.Create([invalid_json_event])
                | beam.ParDo(ParseAndRoute()).with_outputs(
                    Tags.ORDER, Tags.INVENTORY, Tags.ACTIVITY, Tags.ERROR
                )
            )
            
            errors = output[Tags.ERROR] | 'Strip' >> beam.ParDo(StripTimestampedValue())
            
            def check_raw_data_preserved(elements):
                for elem in elements:
                    error_data = json.loads(elem)
                    assert "raw_data" in error_data
                    assert error_data["raw_data"]  # Not empty
            
            assert_that(errors, check_raw_data_preserved, label='RawDataPreserved')

    def test_partial_batch_with_errors(self):
        """Mixed batch with some invalid events processes correctly."""
        events = [
            encode_event(create_order_event(order_id="good-order")),
            b"{ invalid json }",
            encode_event(create_inventory_event(inventory_id="good-inventory")),
            json.dumps({"no_event_type": True}).encode(),  # Missing event_type
            encode_event(create_activity_event(user_id="good-user")),
        ]
        
        with TestPipeline() as p:
            output = (
                p
                | beam.Create(events)
                | beam.ParDo(ParseAndRoute()).with_outputs(
                    Tags.ORDER, Tags.INVENTORY, Tags.ACTIVITY, Tags.ERROR
                )
            )
            
            orders = output[Tags.ORDER] | 'StripOrders' >> beam.ParDo(StripTimestampedValue())
            inventory = output[Tags.INVENTORY] | 'StripInv' >> beam.ParDo(StripTimestampedValue())
            activity = output[Tags.ACTIVITY] | 'StripAct' >> beam.ParDo(StripTimestampedValue())
            errors = output[Tags.ERROR] | 'StripErr' >> beam.ParDo(StripTimestampedValue())
            
            assert_that(orders, has_length(1), label='OrderCount')
            assert_that(inventory, has_length(1), label='InventoryCount')
            assert_that(activity, has_length(1), label='ActivityCount')
            assert_that(errors, has_length(2), label='ErrorCount')  # 2 invalid events


class TestTransforms:
    """Tests for custom transforms."""

    def test_add_ingestion_timestamp(self, sample_order_event):
        """AddIngestionTimestamp adds ingestion_timestamp field."""
        with TestPipeline() as p:
            output = (
                p
                | beam.Create([sample_order_event])
                | beam.ParDo(AddIngestionTimestamp())
            )
            
            def check_timestamp(elements):
                for elem in elements:
                    assert "ingestion_timestamp" in elem
                    assert elem["ingestion_timestamp"]  # Not empty
            
            assert_that(output, check_timestamp)

    def test_format_for_gcs_produces_json_string(self, sample_order_event):
        """FormatForGCS produces valid JSON strings."""
        with TestPipeline() as p:
            output = (
                p
                | beam.Create([sample_order_event])
                | beam.ParDo(FormatForGCS('order'))
            )
            
            def check_format(elements):
                for elem in elements:
                    assert isinstance(elem, str), f"Expected string, got {type(elem)}"
                    # Should be valid JSON
                    parsed = json.loads(elem)
                    assert "order_id" in parsed
            
            assert_that(output, check_format)


class TestDataIntegrity:
    """Tests for data integrity through the pipeline."""

    def test_order_data_preserved(self, sample_order_event):
        """All order fields are preserved through processing."""
        event_bytes = encode_event(sample_order_event)
        
        with TestPipeline() as p:
            output = (
                p
                | beam.Create([event_bytes])
                | beam.ParDo(ParseAndRoute()).with_outputs(
                    Tags.ORDER, Tags.INVENTORY, Tags.ACTIVITY, Tags.ERROR
                )
            )
            
            orders = output[Tags.ORDER] | 'Strip' >> beam.ParDo(StripTimestampedValue())
            
            def check_data_integrity(elements):
                for elem in elements:
                    assert elem["order_id"] == sample_order_event["order_id"]
                    assert elem["customer_id"] == sample_order_event["customer_id"]
                    assert elem["total_amount"] == sample_order_event["total_amount"]
                    assert len(elem["items"]) == len(sample_order_event["items"])
            
            assert_that(orders, check_data_integrity)

    def test_nested_structures_preserved(self, sample_order_event):
        """Nested structures (items, shipping_address) are preserved."""
        event_bytes = encode_event(sample_order_event)
        
        with TestPipeline() as p:
            output = (
                p
                | beam.Create([event_bytes])
                | beam.ParDo(ParseAndRoute()).with_outputs(
                    Tags.ORDER, Tags.INVENTORY, Tags.ACTIVITY, Tags.ERROR
                )
            )
            
            orders = output[Tags.ORDER] | 'Strip' >> beam.ParDo(StripTimestampedValue())
            
            def check_nested(elements):
                for elem in elements:
                    # Check items array
                    assert "items" in elem
                    assert len(elem["items"]) > 0
                    assert "product_id" in elem["items"][0]
                    
                    # Check shipping_address struct
                    assert "shipping_address" in elem
                    assert "city" in elem["shipping_address"]
            
            assert_that(orders, check_nested)

    def test_metadata_preserved_in_activity(self, sample_activity_event):
        """Activity metadata struct is preserved."""
        event_bytes = encode_event(sample_activity_event)
        
        with TestPipeline() as p:
            output = (
                p
                | beam.Create([event_bytes])
                | beam.ParDo(ParseAndRoute()).with_outputs(
                    Tags.ORDER, Tags.INVENTORY, Tags.ACTIVITY, Tags.ERROR
                )
            )
            
            activity = output[Tags.ACTIVITY] | 'Strip' >> beam.ParDo(StripTimestampedValue())
            
            def check_metadata(elements):
                for elem in elements:
                    assert "metadata" in elem
                    assert "session_id" in elem["metadata"]
                    assert "platform" in elem["metadata"]
            
            assert_that(activity, check_metadata)
