"""
Pytest configuration and shared fixtures for pipeline tests.

This module provides:
- Sample event fixtures for each event type
- Helper functions for creating test data
- Common test utilities
"""
import json
import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List
from unittest.mock import MagicMock

import pytest

# Add app directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'app'))


@pytest.fixture
def sample_order_event() -> Dict[str, Any]:
    """Return a valid order event."""
    return {
        "event_type": "order",
        "order_id": "ord-12345",
        "customer_id": "cust-001",
        "order_date": "2025-01-15T10:30:00Z",
        "status": "pending",
        "total_amount": 299.99,
        "items": [
            {
                "product_id": "prod-001",
                "product_name": "Wireless Headphones",
                "quantity": 2,
                "price": 99.99
            },
            {
                "product_id": "prod-002",
                "product_name": "Phone Case",
                "quantity": 1,
                "price": 100.01
            }
        ],
        "shipping_address": {
            "street": "123 Main Street",
            "city": "Amsterdam",
            "country": "NL"
        }
    }


@pytest.fixture
def sample_inventory_event() -> Dict[str, Any]:
    """Return a valid inventory event."""
    return {
        "event_type": "inventory",
        "inventory_id": "inv-98765",
        "product_id": "prod-001",
        "warehouse_id": "wh-eu-west-1",
        "quantity_change": -5,
        "reason": "sale",
        "timestamp": "2025-01-15T10:31:00Z"
    }


@pytest.fixture
def sample_activity_event() -> Dict[str, Any]:
    """Return a valid user activity event."""
    return {
        "event_type": "user_activity",
        "user_id": "user-456",
        "activity_type": "view_product",
        "ip_address": "192.168.1.100",
        "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "timestamp": "2025-01-15T10:29:00Z",
        "metadata": {
            "session_id": "sess-abc123",
            "platform": "web"
        }
    }


@pytest.fixture
def sample_events_batch(
    sample_order_event,
    sample_inventory_event,
    sample_activity_event
) -> List[bytes]:
    """Return a batch of encoded events of different types."""
    return [
        json.dumps(sample_order_event).encode('utf-8'),
        json.dumps(sample_inventory_event).encode('utf-8'),
        json.dumps(sample_activity_event).encode('utf-8'),
    ]


@pytest.fixture
def invalid_json_event() -> bytes:
    """Return invalid JSON bytes."""
    return b"{ this is not valid json }"


@pytest.fixture
def missing_event_type() -> bytes:
    """Return event without event_type field."""
    return json.dumps({
        "order_id": "ord-123",
        "customer_id": "cust-001"
    }).encode('utf-8')


@pytest.fixture
def unknown_event_type() -> bytes:
    """Return event with unknown event_type."""
    return json.dumps({
        "event_type": "unknown_type",
        "data": "some data"
    }).encode('utf-8')


@pytest.fixture
def order_missing_required_field() -> bytes:
    """Return order event missing order_id."""
    return json.dumps({
        "event_type": "order",
        "customer_id": "cust-001",
        "status": "pending"
    }).encode('utf-8')


def create_order_event(
    order_id: str = "ord-test",
    customer_id: str = "cust-test",
    status: str = "pending",
    total_amount: float = 100.0,
    order_date: str = None
) -> Dict[str, Any]:
    """Create a customizable order event."""
    return {
        "event_type": "order",
        "order_id": order_id,
        "customer_id": customer_id,
        "order_date": order_date or datetime.now(timezone.utc).isoformat(),
        "status": status,
        "total_amount": total_amount,
        "items": [
            {
                "product_id": "prod-001",
                "product_name": "Test Product",
                "quantity": 1,
                "price": total_amount
            }
        ],
        "shipping_address": {
            "street": "Test Street",
            "city": "Test City",
            "country": "US"
        }
    }


def create_inventory_event(
    inventory_id: str = "inv-test",
    product_id: str = "prod-test",
    warehouse_id: str = "wh-test",
    quantity_change: int = 10,
    reason: str = "restock"
) -> Dict[str, Any]:
    """Create a customizable inventory event."""
    return {
        "event_type": "inventory",
        "inventory_id": inventory_id,
        "product_id": product_id,
        "warehouse_id": warehouse_id,
        "quantity_change": quantity_change,
        "reason": reason,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }


def create_activity_event(
    user_id: str = "user-test",
    activity_type: str = "login",
    platform: str = "web"
) -> Dict[str, Any]:
    """Create a customizable activity event."""
    return {
        "event_type": "user_activity",
        "user_id": user_id,
        "activity_type": activity_type,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "metadata": {
            "session_id": "sess-test",
            "platform": platform
        }
    }


def encode_event(event: Dict[str, Any]) -> bytes:
    """Encode event dict to bytes (simulating Pub/Sub message)."""
    return json.dumps(event).encode('utf-8')


def extract_timestamped_values(elements):
    """
    Extract values from TimestampedValue wrappers.
    
    Use this in test assertions when dealing with windowed outputs.
    """
    from apache_beam import window
    
    results = []
    for element in elements:
        if isinstance(element, window.TimestampedValue):
            results.append(element.value)
        else:
            results.append(element)
    return results


