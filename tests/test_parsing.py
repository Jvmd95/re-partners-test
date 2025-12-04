"""
Unit tests for the parsing module.

Tests cover:
- JSON decoding
- Event validation
- Timestamp extraction
- Error handling
"""
import json
import sys
import os
from datetime import datetime, timezone
from unittest.mock import patch

import pytest

# Add app directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'app'))

# Add tests directory to path (for importing conftest helpers)
sys.path.insert(0, os.path.dirname(__file__))

from src.parsing import (
    ParseAndRoute,
    Tags,
    VALID_ORDER_STATUSES,
    VALID_ACTIVITY_TYPES,
    VALID_INVENTORY_REASONS,
    VALID_PLATFORMS,
)
from conftest import (
    create_order_event,
    create_inventory_event,
    create_activity_event,
    encode_event,
)


class TestParseAndRouteDecoding:
    """Tests for message decoding functionality."""

    def test_decode_bytes_to_string(self):
        """Bytes input is correctly decoded to string."""
        parser = ParseAndRoute()
        result = parser._decode_message(b"test message")
        assert result == "test message"

    def test_decode_string_passthrough(self):
        """String input is passed through unchanged."""
        parser = ParseAndRoute()
        result = parser._decode_message("test message")
        assert result == "test message"

    def test_decode_utf8_characters(self):
        """UTF-8 special characters are correctly decoded."""
        parser = ParseAndRoute()
        result = parser._decode_message("Café résumé naïve".encode('utf-8'))
        assert result == "Café résumé naïve"


class TestEventValidation:
    """Tests for strict event validation logic.
    
    Design Philosophy: Fail fast on invalid data.
    Bad data goes to DLQ, not to BigQuery with warnings.
    """

    def test_validate_order_with_valid_status(self):
        """Order with valid status passes validation."""
        parser = ParseAndRoute()
        for status in VALID_ORDER_STATUSES:
            order = create_order_event(status=status)
            # Should not raise
            parser._validate_order(order)

    def test_validate_order_missing_order_id_raises(self):
        """Order without order_id raises ValueError."""
        parser = ParseAndRoute()
        order = {"event_type": "order", "customer_id": "cust-1"}
        
        with pytest.raises(ValueError, match="order_id"):
            parser._validate_order(order)

    def test_validate_order_missing_multiple_fields_raises(self):
        """Order missing multiple required fields lists them all."""
        parser = ParseAndRoute()
        order = {"event_type": "order"}  # Missing everything
        
        with pytest.raises(ValueError) as exc_info:
            parser._validate_order(order)
        
        error_msg = str(exc_info.value)
        assert "order_id" in error_msg
        assert "customer_id" in error_msg

    def test_validate_order_invalid_status_raises(self):
        """Order with invalid status raises ValueError (strict validation)."""
        parser = ParseAndRoute()
        order = create_order_event(status="invalid_status")
        
        with pytest.raises(ValueError, match="Invalid order status"):
            parser._validate_order(order)

    def test_validate_order_empty_items_raises(self):
        """Order with empty items array raises ValueError."""
        parser = ParseAndRoute()
        order = create_order_event()
        order['items'] = []
        
        with pytest.raises(ValueError, match="at least one item"):
            parser._validate_order(order)

    def test_validate_order_item_missing_fields_raises(self):
        """Order item missing required fields raises ValueError."""
        parser = ParseAndRoute()
        order = create_order_event()
        order['items'] = [{"product_id": "prod-1"}]  # Missing other fields
        
        with pytest.raises(ValueError, match="item\\[0\\] missing"):
            parser._validate_order(order)

    def test_validate_order_shipping_missing_fields_raises(self):
        """Order with incomplete shipping_address raises ValueError."""
        parser = ParseAndRoute()
        order = create_order_event()
        order['shipping_address'] = {"street": "123 Main St"}  # Missing city, country
        
        with pytest.raises(ValueError, match="shipping_address missing"):
            parser._validate_order(order)

    def test_validate_inventory_valid_event_passes(self):
        """Valid inventory event passes validation."""
        parser = ParseAndRoute()
        inventory = create_inventory_event()
        # Should not raise
        parser._validate_inventory(inventory)

    def test_validate_inventory_invalid_reason_raises(self):
        """Inventory with invalid reason raises ValueError (strict validation)."""
        parser = ParseAndRoute()
        inventory = create_inventory_event(reason="invalid_reason")
        
        with pytest.raises(ValueError, match="Invalid inventory reason"):
            parser._validate_inventory(inventory)

    def test_validate_inventory_missing_fields_raises(self):
        """Inventory missing required fields raises ValueError."""
        parser = ParseAndRoute()
        inventory = {"event_type": "inventory", "inventory_id": "inv-1"}
        
        with pytest.raises(ValueError, match="missing required fields"):
            parser._validate_inventory(inventory)

    def test_validate_inventory_invalid_quantity_type_raises(self):
        """Inventory with non-numeric quantity_change raises ValueError."""
        parser = ParseAndRoute()
        inventory = create_inventory_event()
        inventory['quantity_change'] = "not a number"
        
        with pytest.raises(ValueError, match="quantity_change must be numeric"):
            parser._validate_inventory(inventory)

    def test_validate_activity_valid_event_passes(self):
        """Valid activity event passes validation."""
        parser = ParseAndRoute()
        activity = create_activity_event()
        # Should not raise
        parser._validate_activity(activity)

    def test_validate_activity_invalid_type_raises(self):
        """Activity with invalid activity_type raises ValueError (strict validation)."""
        parser = ParseAndRoute()
        activity = create_activity_event(activity_type="invalid_activity")
        
        with pytest.raises(ValueError, match="Invalid activity_type"):
            parser._validate_activity(activity)

    def test_validate_activity_invalid_platform_raises(self):
        """Activity with invalid platform raises ValueError (strict validation)."""
        parser = ParseAndRoute()
        activity = create_activity_event(platform="invalid_platform")
        
        with pytest.raises(ValueError, match="Invalid platform"):
            parser._validate_activity(activity)

    def test_validate_activity_missing_fields_raises(self):
        """Activity missing required fields raises ValueError."""
        parser = ParseAndRoute()
        activity = {"event_type": "user_activity", "user_id": "user-1"}
        
        with pytest.raises(ValueError, match="missing required fields"):
            parser._validate_activity(activity)

    def test_validate_activity_without_metadata_passes(self):
        """Activity without optional metadata passes validation."""
        parser = ParseAndRoute()
        activity = create_activity_event()
        del activity['metadata']  # Remove optional field
        # Should not raise
        parser._validate_activity(activity)


class TestTimestampExtraction:
    """Tests for event timestamp extraction."""

    def test_extract_timestamp_from_timestamp_field(self):
        """Extracts timestamp from 'timestamp' field."""
        record = {"timestamp": "2025-01-15T10:30:00Z"}
        result = ParseAndRoute._extract_event_timestamp(record)
        
        expected = datetime(2025, 1, 15, 10, 30, 0, tzinfo=timezone.utc).timestamp()
        assert abs(result - expected) < 1  # Within 1 second

    def test_extract_timestamp_from_order_date(self):
        """Extracts timestamp from 'order_date' field."""
        record = {"order_date": "2025-01-15T10:30:00+00:00"}
        result = ParseAndRoute._extract_event_timestamp(record)
        
        expected = datetime(2025, 1, 15, 10, 30, 0, tzinfo=timezone.utc).timestamp()
        assert abs(result - expected) < 1

    def test_extract_timestamp_from_event_time(self):
        """Extracts timestamp from 'event_time' field."""
        record = {"event_time": "2025-01-15T10:30:00Z"}
        result = ParseAndRoute._extract_event_timestamp(record)
        
        expected = datetime(2025, 1, 15, 10, 30, 0, tzinfo=timezone.utc).timestamp()
        assert abs(result - expected) < 1

    def test_extract_timestamp_fallback_to_now(self):
        """Falls back to current time when no timestamp field exists."""
        record = {"some_field": "value"}
        before = datetime.now(timezone.utc).timestamp()
        result = ParseAndRoute._extract_event_timestamp(record)
        after = datetime.now(timezone.utc).timestamp()
        
        assert before <= result <= after

    def test_extract_timestamp_handles_invalid_format(self):
        """Falls back to current time on invalid timestamp format."""
        record = {"timestamp": "not-a-valid-timestamp"}
        
        with patch('src.parsing.logging.warning') as mock_warning:
            result = ParseAndRoute._extract_event_timestamp(record)
            mock_warning.assert_called_once()
        
        # Should return current time
        now = datetime.now(timezone.utc).timestamp()
        assert abs(result - now) < 5  # Within 5 seconds


class TestErrorHandling:
    """Tests for error handling and DLQ routing."""

    def test_error_payload_contains_required_fields(self):
        """Error payload contains all required fields for debugging."""
        parser = ParseAndRoute()
        original_message = b"invalid message"
        error = ValueError("Test error")
        
        results = list(parser._handle_error(original_message, error))
        
        assert len(results) == 1
        # Extract value from TaggedOutput
        error_output = results[0]
        error_json = json.loads(error_output.value.value)
        
        assert "error_message" in error_json
        assert "error_type" in error_json
        assert "raw_data" in error_json
        assert "timestamp" in error_json
        assert error_json["error_type"] == "ValueError"
        assert "Test error" in error_json["error_message"]

    def test_error_payload_truncates_long_messages(self):
        """Long error messages are handled without issues."""
        parser = ParseAndRoute()
        long_message = b"x" * 10000
        error = ValueError("Error")
        
        results = list(parser._handle_error(long_message, error))
        assert len(results) == 1


class TestConstantsAndEnums:
    """Tests for module constants and enum values."""

    def test_valid_order_statuses(self):
        """Valid order statuses are defined."""
        assert "pending" in VALID_ORDER_STATUSES
        assert "processing" in VALID_ORDER_STATUSES
        assert "shipped" in VALID_ORDER_STATUSES
        assert "delivered" in VALID_ORDER_STATUSES

    def test_valid_activity_types(self):
        """Valid activity types are defined."""
        assert "login" in VALID_ACTIVITY_TYPES
        assert "view_product" in VALID_ACTIVITY_TYPES
        assert "add_to_cart" in VALID_ACTIVITY_TYPES

    def test_valid_inventory_reasons(self):
        """Valid inventory reasons are defined."""
        assert "restock" in VALID_INVENTORY_REASONS
        assert "sale" in VALID_INVENTORY_REASONS
        assert "return" in VALID_INVENTORY_REASONS
        assert "damage" in VALID_INVENTORY_REASONS

    def test_valid_platforms(self):
        """Valid platforms are defined."""
        assert "web" in VALID_PLATFORMS
        assert "mobile" in VALID_PLATFORMS
        assert "tablet" in VALID_PLATFORMS

    def test_tags_values(self):
        """Tag constants have expected values."""
        assert Tags.ORDER == "order"
        assert Tags.INVENTORY == "inventory"
        assert Tags.ACTIVITY == "user_activity"
        assert Tags.ERROR == "error"


