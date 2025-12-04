"""
Event parsing and routing logic for the pipeline.
"""
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Generator, Optional, Union

import apache_beam as beam
from apache_beam import pvalue, window
from apache_beam.metrics import Metrics


class Tags:
    ORDER: str = 'order'
    INVENTORY: str = 'inventory'
    ACTIVITY: str = 'user_activity'
    ERROR: str = 'error'


# Valid values for enum fields (for validation)
VALID_ORDER_STATUSES = frozenset({'pending', 'processing', 'shipped', 'delivered'})
VALID_ACTIVITY_TYPES = frozenset({'login', 'logout', 'view_product', 'add_to_cart', 'remove_from_cart'})
VALID_INVENTORY_REASONS = frozenset({'restock', 'sale', 'return', 'damage'})
VALID_PLATFORMS = frozenset({'web', 'mobile', 'tablet'})


class ParseAndRoute(beam.DoFn):
    """
    Parse raw Pub/Sub messages and route to appropriate output tags.

    Pipeline:
    1. Decode bytes → string
    2. Parse JSON → dict
    3. Validate required fields
    4. Route to output tag based on event_type

    Errors are caught and routed to the ERROR tag (DLQ) with full context.

    Metrics:
        - num_orders: Count of successfully parsed order events
        - num_inventory: Count of successfully parsed inventory events
        - num_activity: Count of successfully parsed activity events
        - num_errors: Count of events routed to DLQ
    """

    def __init__(self) -> None:
        """
        Initialize metrics counters.
        """
        super().__init__()
        self.orders_count = Metrics.counter(self.__class__.__name__, 'num_orders')
        self.inventory_count = Metrics.counter(self.__class__.__name__, 'num_inventory')
        self.activity_count = Metrics.counter(self.__class__.__name__, 'num_activity')
        self.errors_count = Metrics.counter(self.__class__.__name__, 'num_errors')

    def setup(self) -> None:
        """
        One-time worker initialization (called once per worker).
        """
        # Future: Add database clients or lookup tables here if needed
        pass

    def teardown(self) -> None:
        """
        Cleanup when worker shuts down.
        """
        pass

    def process(
        self,
        element: Union[bytes, str]
    ) -> Generator[pvalue.TaggedOutput, None, None]:
        """
        Parse and route a single message.

        Args:
            element: Raw message from Pub/Sub (bytes)

        Yields:
            TaggedOutput with the parsed record or error payload
        """
        try:
            message_str = self._decode_message(element)

            record = json.loads(message_str)

            event_type = record.get('event_type')
            if not event_type:
                raise ValueError("Missing required field: event_type")

            event_timestamp = self._extract_event_timestamp(record)

            yield from self._route_event(record, event_type, event_timestamp)

        except Exception as e:
            yield from self._handle_error(element, e)

    def _decode_message(self, element: Union[bytes, str]) -> str:
        """
        Decode message to string.

        Args:
            element: Raw message (bytes or string)

        Returns:
            Decoded string
        """
        if isinstance(element, bytes):
            return element.decode('utf-8')
        return element

    def _route_event(
        self,
        record: Dict[str, Any],
        event_type: str,
        event_timestamp: float
    ) -> Generator[pvalue.TaggedOutput, None, None]:
        """
        Route event to appropriate output tag.

        Args:
            record: Parsed event record
            event_type: The event type from the record
            event_timestamp: Unix timestamp for windowing

        Yields:
            TaggedOutput for the appropriate sink

        Raises:
            ValueError: If event type is unknown or validation fails
        """
        if event_type == Tags.ORDER:
            self._validate_order(record)
            self.orders_count.inc()
            yield pvalue.TaggedOutput(
                Tags.ORDER,
                window.TimestampedValue(record, event_timestamp)
            )

        elif event_type == Tags.INVENTORY:
            self._validate_inventory(record)
            self.inventory_count.inc()
            yield pvalue.TaggedOutput(
                Tags.INVENTORY,
                window.TimestampedValue(record, event_timestamp)
            )

        elif event_type == Tags.ACTIVITY:
            self._validate_activity(record)
            self.activity_count.inc()
            yield pvalue.TaggedOutput(
                Tags.ACTIVITY,
                window.TimestampedValue(record, event_timestamp)
            )

        else:
            raise ValueError(f"Unknown event_type: {event_type}")

    def _validate_required_fields(
        self,
        record: Dict[str, Any],
        required_fields: list,
        event_name: str
    ) -> None:
        """
        Check that all required fields exist in the record.

        Args:
            record: The record to validate
            required_fields: List of field names that must be present
            event_name: Name of the event type (for error messages)

        Raises:
            ValueError: If any required field is missing
        """
        missing = [f for f in required_fields if f not in record or record[f] is None]
        if missing:
            raise ValueError(
                f"{event_name} missing required fields: {', '.join(missing)}"
            )

    def _validate_order(self, record: Dict[str, Any]) -> None:
        """
        Validate order event against schema contract.

        Checks:
        - All required top-level fields present
        - Status is a valid enum value
        - Items array is non-empty with valid structure
        - Shipping address has required fields

        Args:
            record: The order record to validate

        Raises:
            ValueError: If validation fails (event will be routed to DLQ)
        """
        required_fields = [
            'order_id', 'customer_id', 'order_date', 'status',
            'total_amount', 'items', 'shipping_address'
        ]
        self._validate_required_fields(record, required_fields, 'Order')

        status = record['status']
        if status not in VALID_ORDER_STATUSES:
            raise ValueError(
                f"Invalid order status '{status}'. "
                f"Must be one of: {', '.join(sorted(VALID_ORDER_STATUSES))}"
            )

        items = record['items']
        if not isinstance(items, list) or len(items) == 0:
            raise ValueError("Order must have at least one item")

        item_required = ['product_id', 'product_name', 'quantity', 'price']
        for i, item in enumerate(items):
            missing = [f for f in item_required if f not in item]
            if missing:
                raise ValueError(
                    f"Order item[{i}] missing required fields: {', '.join(missing)}"
                )

        shipping = record['shipping_address']
        if not isinstance(shipping, dict):
            raise ValueError("shipping_address must be an object")

        shipping_required = ['street', 'city', 'country']
        missing = [f for f in shipping_required if f not in shipping]
        if missing:
            raise ValueError(
                f"shipping_address missing required fields: {', '.join(missing)}"
            )

    def _validate_inventory(self, record: Dict[str, Any]) -> None:
        """
        Validate inventory event against schema contract.

        Checks:
        - All required fields present
        - Reason is a valid enum value
        - quantity_change is numeric

        Args:
            record: The inventory record to validate

        Raises:
            ValueError: If validation fails (event will be routed to DLQ)
        """
        required_fields = [
            'inventory_id', 'product_id', 'warehouse_id',
            'quantity_change', 'reason', 'timestamp'
        ]
        self._validate_required_fields(record, required_fields, 'Inventory')

        reason = record['reason']
        if reason not in VALID_INVENTORY_REASONS:
            raise ValueError(
                f"Invalid inventory reason '{reason}'. "
                f"Must be one of: {', '.join(sorted(VALID_INVENTORY_REASONS))}"
            )

        qty = record['quantity_change']
        if not isinstance(qty, (int, float)):
            raise ValueError(
                f"quantity_change must be numeric, got {type(qty).__name__}"
            )

    def _validate_activity(self, record: Dict[str, Any]) -> None:
        """
        Validate user activity event against schema contract.

        Checks:
        - All required fields present
        - activity_type is a valid enum value
        - If metadata.platform exists, it must be valid

        Args:
            record: The activity record to validate

        Raises:
            ValueError: If validation fails (event will be routed to DLQ)
        """
        required_fields = ['user_id', 'activity_type', 'timestamp']
        self._validate_required_fields(record, required_fields, 'Activity')

        activity_type = record['activity_type']
        if activity_type not in VALID_ACTIVITY_TYPES:
            raise ValueError(
                f"Invalid activity_type '{activity_type}'. "
                f"Must be one of: {', '.join(sorted(VALID_ACTIVITY_TYPES))}"
            )

        metadata = record.get('metadata')
        if metadata and isinstance(metadata, dict):
            platform = metadata.get('platform')
            if platform and platform not in VALID_PLATFORMS:
                raise ValueError(
                    f"Invalid platform '{platform}' in metadata. "
                    f"Must be one of: {', '.join(sorted(VALID_PLATFORMS))}"
                )

    def _handle_error(
        self,
        element: Union[bytes, str],
        error: Exception
    ) -> Generator[pvalue.TaggedOutput, None, None]:
        """
        Handle parsing/validation errors by routing to DLQ.

        Args:
            element: Original raw message
            error: The exception that occurred

        Yields:
            TaggedOutput for the error sink
        """
        self.errors_count.inc()

        error_payload = {
            'error_message': str(error),
            'error_type': type(error).__name__,
            'raw_data': str(element),
            'timestamp': datetime.now(timezone.utc).isoformat()
        }

        logging.error(
            "Failed to parse record: %s | Error: %s",
            str(element)[:100],  # Truncate for log readability
            error
        )

        yield pvalue.TaggedOutput(
            Tags.ERROR,
            window.TimestampedValue(
                json.dumps(error_payload),
                datetime.now(timezone.utc).timestamp()
            )
        )

    @staticmethod
    def _extract_event_timestamp(record: Dict[str, Any]) -> float:
        """
        Extract event timestamp from record for proper windowing.

        Looks for timestamp in common fields:
        1. timestamp (inventory, activity)
        2. order_date (orders)
        3. event_time (generic)

        Falls back to current time if no timestamp found.

        Args:
            record: Parsed event record

        Returns:
            Unix timestamp (seconds since epoch)
        """
        timestamp_str: Optional[str] = (
            record.get('timestamp')
            or record.get('order_date')
            or record.get('event_time')
        )

        if not timestamp_str:
            return datetime.now(timezone.utc).timestamp()

        normalized = timestamp_str.replace('Z', '+00:00')

        try:
            return datetime.fromisoformat(normalized).timestamp()
        except ValueError:
            logging.warning(
                "Failed to parse event timestamp '%s'; defaulting to now",
                timestamp_str
            )
            return datetime.now(timezone.utc).timestamp()
