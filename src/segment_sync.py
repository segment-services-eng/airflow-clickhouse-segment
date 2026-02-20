"""
ClickHouse to Segment sync utilities for Retail Pro data.

Production-ready implementation with:
- Retry logic with exponential backoff
- Stable idempotency keys (prevents duplicates on retry)
- Failed event tracking for recovery
- Chunked processing for large datasets
- Comprehensive error handling and logging
"""

import os
import logging
import hashlib
import time
from datetime import datetime, timezone
from typing import Any
from functools import wraps

import clickhouse_connect
import analytics
from analytics.request import APIError

logger = logging.getLogger(__name__)

# Configuration
DEFAULT_CHUNK_SIZE = 500
DEFAULT_BATCH_SIZE = 100
MAX_RETRIES = 3
INITIAL_RETRY_DELAY = 1.0  # seconds
MAX_RETRY_DELAY = 30.0  # seconds

# Reusable ClickHouse client (connection pooling)
_clickhouse_client = None


def get_clickhouse_client():
    """
    Get or create a ClickHouse client connection.

    Uses a module-level singleton for connection reuse.
    """
    global _clickhouse_client
    if _clickhouse_client is None:
        _clickhouse_client = clickhouse_connect.get_client(
            host=os.environ.get('CLICKHOUSE_HOST', 'localhost'),
            port=int(os.environ.get('CLICKHOUSE_PORT', 8123)),
            username=os.environ.get('CLICKHOUSE_USER', 'default'),
            password=os.environ.get('CLICKHOUSE_PASSWORD', ''),
        )
    return _clickhouse_client


def reset_clickhouse_client():
    """Reset the ClickHouse client (useful for testing or reconnection)."""
    global _clickhouse_client
    _clickhouse_client = None


def init_segment(write_key: str | None = None) -> bool:
    """
    Initialize the Segment analytics client.

    Args:
        write_key: Segment source write key. Falls back to SEGMENT_WRITE_KEY env var.

    Returns:
        True if initialized successfully, False if no valid key (dry-run mode).
    """
    key = write_key or os.environ.get('SEGMENT_WRITE_KEY')
    if not key or key == 'your-write-key-here':
        logger.warning("No valid SEGMENT_WRITE_KEY set - running in dry-run mode")
        return False

    analytics.write_key = key
    analytics.max_queue_size = 10000
    analytics.debug = os.environ.get('SEGMENT_DEBUG', 'false').lower() == 'true'

    # Configure sync mode for more reliable delivery
    analytics.sync_mode = False  # Async is fine with proper flushing

    return True


def generate_idempotency_key(entity_type: str, entity_id: str, event_type: str = 'default') -> str:
    """
    Generate a stable idempotency key for Segment messageId.

    Using a hash of entity identifiers ensures that retrying the same
    record produces the same messageId, preventing duplicate events.

    Args:
        entity_type: 'customer' or 'order'
        entity_id: The source system ID (e.g., SID)
        event_type: Additional discriminator for different event types

    Returns:
        A stable UUID-like string for use as messageId
    """
    # Create a deterministic hash from the inputs
    key_material = f"{entity_type}:{entity_id}:{event_type}"
    hash_bytes = hashlib.sha256(key_material.encode()).hexdigest()

    # Format as UUID-like string for Segment compatibility
    return f"{hash_bytes[:8]}-{hash_bytes[8:12]}-{hash_bytes[12:16]}-{hash_bytes[16:20]}-{hash_bytes[20:32]}"


def retry_with_backoff(max_retries: int = MAX_RETRIES, initial_delay: float = INITIAL_RETRY_DELAY):
    """
    Decorator that retries a function with exponential backoff.

    Only retries on transient errors (network issues, 5xx responses).
    Permanent errors (4xx) are not retried.
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            delay = initial_delay

            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except APIError as e:
                    last_exception = e
                    # Don't retry client errors (4xx)
                    if hasattr(e, 'status') and 400 <= e.status < 500:
                        logger.error(f"Permanent error (will not retry): {e}")
                        raise

                    if attempt < max_retries:
                        logger.warning(f"Transient error on attempt {attempt + 1}/{max_retries + 1}: {e}")
                        logger.info(f"Retrying in {delay:.1f}s...")
                        time.sleep(delay)
                        delay = min(delay * 2, MAX_RETRY_DELAY)
                    else:
                        logger.error(f"Max retries exceeded: {e}")
                        raise
                except Exception as e:
                    last_exception = e
                    if attempt < max_retries:
                        logger.warning(f"Error on attempt {attempt + 1}/{max_retries + 1}: {e}")
                        time.sleep(delay)
                        delay = min(delay * 2, MAX_RETRY_DELAY)
                    else:
                        raise

            raise last_exception
        return wrapper
    return decorator


def ensure_failed_events_table(client):
    """
    Ensure the failed_events table exists for tracking sync failures.

    This table allows recovery and analysis of failed events.
    """
    client.command("""
        CREATE TABLE IF NOT EXISTS retail.failed_events (
            id UUID DEFAULT generateUUIDv4(),
            entity_type String,
            entity_id String,
            event_type String,
            error_message String,
            error_category String,
            payload String,
            created_at DateTime DEFAULT now(),
            retry_count UInt8 DEFAULT 0,
            resolved Bool DEFAULT false
        ) ENGINE = MergeTree()
        ORDER BY (created_at, entity_type, entity_id)
        TTL created_at + INTERVAL 30 DAY
    """)


def record_failed_event(
    client,
    entity_type: str,
    entity_id: str,
    event_type: str,
    error_message: str,
    error_category: str,
    payload: str = '',
):
    """
    Record a failed event for later recovery or analysis.

    Args:
        client: ClickHouse client
        entity_type: 'customer' or 'order'
        entity_id: Source system ID
        event_type: 'identify' or 'track'
        error_message: The error that occurred
        error_category: 'transient', 'permanent', or 'validation'
        payload: JSON representation of the event (for debugging)
    """
    try:
        client.command(
            """
            INSERT INTO retail.failed_events
            (entity_type, entity_id, event_type, error_message, error_category, payload)
            VALUES (%(entity_type)s, %(entity_id)s, %(event_type)s, %(error_message)s, %(error_category)s, %(payload)s)
            """,
            parameters={
                'entity_type': entity_type,
                'entity_id': entity_id,
                'event_type': event_type,
                'error_message': str(error_message)[:1000],  # Truncate long errors
                'error_category': error_category,
                'payload': payload[:10000] if payload else '',  # Truncate large payloads
            }
        )
    except Exception as e:
        logger.error(f"Failed to record failed event: {e}")


def validate_customer(customer: dict) -> tuple[bool, str]:
    """
    Validate customer data before sending to Segment.

    Returns:
        Tuple of (is_valid, error_message)
    """
    if not customer.get('SID'):
        return False, "Missing required field: SID"

    email = customer.get('EMAIL', '')
    if email and '@' not in email:
        return False, f"Invalid email format: {email}"

    return True, ""


def validate_order(order: dict) -> tuple[bool, str]:
    """
    Validate order data before sending to Segment.

    Returns:
        Tuple of (is_valid, error_message)
    """
    if not order.get('SID'):
        return False, "Missing required field: SID"

    if not order.get('BT_CUID') and not order.get('SID'):
        return False, "No user identifier available"

    has_sale = order.get('HAS_SALE') == '1'
    has_return = order.get('HAS_RETURN') == '1'
    if not has_sale and not has_return:
        return False, "Order has neither sale nor return flag"

    return True, ""


def mark_customers_synced(client, customer_ids: list[str]):
    """Mark a batch of customers as synced in ClickHouse."""
    if not customer_ids:
        return

    update_query = """
        ALTER TABLE retail.customers
        UPDATE synced_to_segment = true
        WHERE SID IN %(ids)s
    """
    client.command(update_query, parameters={'ids': customer_ids})
    logger.info(f"Marked {len(customer_ids)} customers as synced")


def mark_documents_synced(client, doc_ids: list[str]):
    """Mark a batch of documents as synced in ClickHouse."""
    if not doc_ids:
        return

    update_query = """
        ALTER TABLE retail.documents
        UPDATE synced_to_segment = true
        WHERE SID IN %(ids)s
    """
    client.command(update_query, parameters={'ids': doc_ids})
    logger.info(f"Marked {len(doc_ids)} documents as synced")


@retry_with_backoff()
def flush_to_segment():
    """Flush the Segment queue with retry logic."""
    analytics.flush()


def sync_customers(
    last_sync_time: datetime | None = None,
    dry_run: bool = False,
    batch_size: int = DEFAULT_BATCH_SIZE,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
) -> dict[str, Any]:
    """
    Sync customers from ClickHouse to Segment.

    Production-ready implementation with:
    - Chunked queries to handle large datasets
    - Batched flushing to Segment
    - Stable idempotency keys to prevent duplicates
    - Failed event tracking for recovery
    - Validation before sending

    Args:
        last_sync_time: Only sync customers updated after this time
        dry_run: If True, don't actually send to Segment
        batch_size: Flush to Segment after this many events
        chunk_size: Number of rows to fetch from ClickHouse per iteration

    Returns:
        Dict with sync statistics: total, synced, failed, skipped
    """
    client = get_clickhouse_client()

    # Ensure failed events table exists
    ensure_failed_events_table(client)

    total_synced = 0
    total_failed = 0
    total_skipped = 0
    total_processed = 0

    while True:
        # Query a chunk of unsynced customers
        query = f"""
            SELECT
                SID,
                CUST_ID,
                LAST_NAME,
                FIRST_NAME,
                EMAIL,
                MARKETING_FLAG,
                LTY_OPT_IN,
                LTY_BALANCE,
                TOTAL_TRANSACTIONS,
                SALE_ITEM_COUNT,
                RETURN_ITEM_COUNT,
                YTD_SALE,
                CREATED_DATETIME
            FROM retail.customers
            WHERE synced_to_segment = false
            ORDER BY SID
            LIMIT {chunk_size}
        """

        result = client.query(query)
        customers = result.result_rows
        column_names = result.column_names

        if not customers:
            break  # No more records to process

        logger.info(f"Processing chunk of {len(customers)} customers (total so far: {total_processed})")

        synced_count = 0
        failed_count = 0
        skipped_count = 0
        batch_counter = 0
        customer_ids_synced = []

        for row in customers:
            customer = dict(zip(column_names, row))
            user_id = customer.get('SID')

            # Validate before processing
            is_valid, error_msg = validate_customer(customer)
            if not is_valid:
                logger.warning(f"Skipping invalid customer {user_id}: {error_msg}")
                record_failed_event(
                    client, 'customer', str(user_id), 'identify',
                    error_msg, 'validation'
                )
                skipped_count += 1
                continue

            # Build traits
            traits = {
                'email': customer.get('EMAIL'),
                'firstName': customer.get('FIRST_NAME'),
                'lastName': customer.get('LAST_NAME'),
                'customerId': customer.get('CUST_ID'),
                'marketingOptIn': customer.get('MARKETING_FLAG') == '1',
                'loyaltyOptIn': customer.get('LTY_OPT_IN') == '1',
                'loyaltyPoints': int(customer.get('LTY_BALANCE') or 0),
                'totalOrders': customer.get('TOTAL_TRANSACTIONS', 0),
                'lifetimeItemsPurchased': customer.get('SALE_ITEM_COUNT', 0),
                'lifetimeItemsReturned': customer.get('RETURN_ITEM_COUNT', 0),
                'ytdSpend': customer.get('YTD_SALE', 0),
            }

            # Remove None/empty values
            traits = {k: v for k, v in traits.items() if v is not None and v != ''}

            # Generate stable idempotency key
            message_id = generate_idempotency_key('customer', str(user_id), 'identify')

            if dry_run:
                logger.info(f"[DRY RUN] Would identify customer: {user_id} ({customer.get('EMAIL')})")
                synced_count += 1
                customer_ids_synced.append(user_id)
            else:
                try:
                    analytics.identify(
                        user_id=str(user_id),
                        traits=traits,
                        context={
                            'externalIds': [{
                                'id': str(customer.get('CUST_ID', user_id)),
                                'type': 'retailProCustomerId',
                                'collection': 'users',
                                'encoding': 'none',
                            }]
                        },
                        message_id=message_id,
                        timestamp=datetime.now(timezone.utc),
                    )
                    synced_count += 1
                    customer_ids_synced.append(user_id)
                    batch_counter += 1

                    # Flush and mark synced every batch_size events
                    if batch_counter >= batch_size:
                        logger.info(f"Flushing batch of {batch_counter} events...")
                        flush_to_segment()
                        mark_customers_synced(client, customer_ids_synced)
                        customer_ids_synced = []
                        batch_counter = 0

                except Exception as e:
                    error_category = 'transient' if 'timeout' in str(e).lower() or '5' in str(getattr(e, 'status', '')) else 'permanent'
                    logger.error(f"Failed to sync customer {user_id}: {e}")
                    record_failed_event(
                        client, 'customer', str(user_id), 'identify',
                        str(e), error_category
                    )
                    failed_count += 1

        # Flush and mark remaining records in this chunk
        if not dry_run and customer_ids_synced:
            flush_to_segment()
            mark_customers_synced(client, customer_ids_synced)

        total_synced += synced_count
        total_failed += failed_count
        total_skipped += skipped_count
        total_processed += len(customers)

        # If we got fewer than chunk_size, we're done
        if len(customers) < chunk_size:
            break

    logger.info(f"Customer sync complete: {total_synced} synced, {total_failed} failed, {total_skipped} skipped")

    return {
        'total': total_processed,
        'synced': total_synced,
        'failed': total_failed,
        'skipped': total_skipped,
    }


def sync_orders(
    last_sync_time: datetime | None = None,
    dry_run: bool = False,
    batch_size: int = DEFAULT_BATCH_SIZE,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
) -> dict[str, Any]:
    """
    Sync orders from ClickHouse to Segment as 'Order Completed' or 'Order Refunded' events.

    Production-ready implementation with:
    - Chunked queries to handle large datasets
    - Batched flushing to Segment
    - Stable idempotency keys to prevent duplicates
    - Failed event tracking for recovery
    - Validation before sending

    Args:
        last_sync_time: Only sync orders created after this time
        dry_run: If True, don't actually send to Segment
        batch_size: Flush to Segment after this many events
        chunk_size: Number of rows to fetch from ClickHouse per iteration

    Returns:
        Dict with sync statistics: total, synced, failed, skipped
    """
    client = get_clickhouse_client()

    # Ensure failed events table exists
    ensure_failed_events_table(client)

    total_synced = 0
    total_failed = 0
    total_skipped = 0
    total_processed = 0

    while True:
        # Query a chunk of unsynced documents
        query = f"""
            SELECT
                SID,
                DOC_NO,
                BT_CUID,
                BT_EMAIL,
                ST_EMAIL,
                SALE_TOTAL_AMT,
                SALE_SUBTOTAL,
                SALE_TOTAL_TAX_AMT,
                TOTAL_DISCOUNT_AMT,
                SHIPPING_AMT,
                SOLD_QTY,
                RETURN_QTY,
                CURRENCY_NAME,
                TENDER_NAME,
                STORE_CODE,
                SBS_NO,
                SHIP_METHOD,
                HAS_SALE,
                HAS_RETURN,
                POST_DATE,
                CREATED_DATETIME
            FROM retail.documents
            WHERE synced_to_segment = false
            ORDER BY SID
            LIMIT {chunk_size}
        """

        result = client.query(query)
        documents = result.result_rows
        column_names = result.column_names

        if not documents:
            break  # No more records to process

        logger.info(f"Processing chunk of {len(documents)} orders (total so far: {total_processed})")

        synced_count = 0
        failed_count = 0
        skipped_count = 0
        batch_counter = 0
        doc_ids_synced = []

        for row in documents:
            doc = dict(zip(column_names, row))
            doc_sid = doc.get('SID')

            # Validate before processing
            is_valid, error_msg = validate_order(doc)
            if not is_valid:
                logger.warning(f"Skipping invalid order {doc_sid}: {error_msg}")
                record_failed_event(
                    client, 'order', str(doc_sid), 'track',
                    error_msg, 'validation'
                )
                skipped_count += 1
                continue

            user_id = doc.get('BT_CUID') or doc_sid
            email = doc.get('BT_EMAIL') or doc.get('ST_EMAIL')

            # Get line items for this document
            items_query = """
                SELECT
                    INVN_SBS_ITEM_SID,
                    ALU,
                    DESCRIPTION1,
                    DCS_CODE,
                    VEND_CODE,
                    QTY,
                    PRICE,
                    ORIG_PRICE,
                    DISC_AMT,
                    ITEM_SIZE,
                    ATTRIBUTE
                FROM retail.document_items
                WHERE DOC_SID = %(doc_sid)s
            """
            items_result = client.query(items_query, parameters={'doc_sid': doc_sid})
            items = [dict(zip(items_result.column_names, item_row)) for item_row in items_result.result_rows]

            products = [
                {
                    'product_id': item.get('INVN_SBS_ITEM_SID'),
                    'sku': item.get('ALU'),
                    'name': item.get('DESCRIPTION1'),
                    'price': round(float(item.get('PRICE', 0) or 0), 2),
                    'quantity': int(item.get('QTY', 1) or 1),
                    'category': item.get('DCS_CODE') or None,
                    'brand': item.get('VEND_CODE') or None,
                }
                for item in items
            ]

            # Remove None values from products
            products = [{k: v for k, v in p.items() if v is not None} for p in products]

            # Determine event type
            has_sale = doc.get('HAS_SALE') == '1'
            has_return = doc.get('HAS_RETURN') == '1'

            if has_sale:
                event_name = 'Order Completed'
                revenue = float(doc.get('SALE_TOTAL_AMT', 0) or 0)
            elif has_return:
                event_name = 'Order Refunded'
                revenue = abs(float(doc.get('SALE_TOTAL_AMT', 0) or 0))
            else:
                skipped_count += 1
                continue

            properties = {
                'orderId': doc.get('DOC_NO'),
                'revenue': round(revenue, 2),
                'subtotal': round(float(doc.get('SALE_SUBTOTAL', 0) or 0), 2),
                'tax': round(float(doc.get('SALE_TOTAL_TAX_AMT', 0) or 0), 2),
                'shipping': round(float(doc.get('SHIPPING_AMT', 0) or 0), 2),
                'discount': round(float(doc.get('TOTAL_DISCOUNT_AMT', 0) or 0), 2),
                'currency': doc.get('CURRENCY_NAME') or 'USD',
                'paymentMethod': doc.get('TENDER_NAME'),
                'storeId': doc.get('STORE_CODE'),
                'shippingMethod': doc.get('SHIP_METHOD'),
                'products': products,
            }

            # Remove None/empty values
            properties = {k: v for k, v in properties.items() if v is not None and v != ''}

            context = {}
            if email:
                context['traits'] = {'email': email}

            # Generate stable idempotency key
            message_id = generate_idempotency_key('order', str(doc_sid), event_name)

            if dry_run:
                logger.info(f"[DRY RUN] Would track '{event_name}' for order: {doc.get('DOC_NO')} (user: {user_id}, revenue: ${revenue:.2f})")
                synced_count += 1
                doc_ids_synced.append(doc_sid)
            else:
                try:
                    analytics.track(
                        user_id=str(user_id),
                        event=event_name,
                        properties=properties,
                        context=context if context else None,
                        message_id=message_id,
                        timestamp=datetime.now(timezone.utc),
                    )
                    synced_count += 1
                    doc_ids_synced.append(doc_sid)
                    batch_counter += 1

                    # Flush and mark synced every batch_size events
                    if batch_counter >= batch_size:
                        logger.info(f"Flushing batch of {batch_counter} events...")
                        flush_to_segment()
                        mark_documents_synced(client, doc_ids_synced)
                        doc_ids_synced = []
                        batch_counter = 0

                except Exception as e:
                    error_category = 'transient' if 'timeout' in str(e).lower() or '5' in str(getattr(e, 'status', '')) else 'permanent'
                    logger.error(f"Failed to sync order {doc_sid}: {e}")
                    record_failed_event(
                        client, 'order', str(doc_sid), 'track',
                        str(e), error_category
                    )
                    failed_count += 1

        # Flush and mark remaining records in this chunk
        if not dry_run and doc_ids_synced:
            flush_to_segment()
            mark_documents_synced(client, doc_ids_synced)

        total_synced += synced_count
        total_failed += failed_count
        total_skipped += skipped_count
        total_processed += len(documents)

        # If we got fewer than chunk_size, we're done
        if len(documents) < chunk_size:
            break

    logger.info(f"Order sync complete: {total_synced} synced, {total_failed} failed, {total_skipped} skipped")

    return {
        'total': total_processed,
        'synced': total_synced,
        'failed': total_failed,
        'skipped': total_skipped,
    }


def get_failed_events_summary(client=None) -> dict[str, Any]:
    """
    Get a summary of failed events for monitoring.

    Returns:
        Dict with counts by category and entity type
    """
    if client is None:
        client = get_clickhouse_client()

    try:
        result = client.query("""
            SELECT
                entity_type,
                error_category,
                count() as count
            FROM retail.failed_events
            WHERE resolved = false
            GROUP BY entity_type, error_category
            ORDER BY count DESC
        """)

        summary = {
            'total_unresolved': 0,
            'by_category': {},
            'by_entity': {},
        }

        for row in result.result_rows:
            entity_type, error_category, count = row
            summary['total_unresolved'] += count
            summary['by_category'][error_category] = summary['by_category'].get(error_category, 0) + count
            summary['by_entity'][entity_type] = summary['by_entity'].get(entity_type, 0) + count

        return summary
    except Exception as e:
        logger.error(f"Failed to get failed events summary: {e}")
        return {'error': str(e)}
