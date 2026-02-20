# Segment Python SDK Guide

This guide covers how to use the Segment Python SDK (`analytics-python`) for sending customer and event data to Segment. It's written for developers who are new to Segment.

## Table of Contents

1. [Overview](#overview)
2. [Installation](#installation)
3. [Configuration](#configuration)
4. [Core Concepts](#core-concepts)
5. [The identify() Method](#the-identify-method)
6. [The track() Method](#the-track-method)
7. [Best Practices](#best-practices)
8. [Error Handling](#error-handling)
9. [Testing](#testing)
10. [Production Checklist](#production-checklist)

---

## Overview

Segment is a Customer Data Platform (CDP) that collects, cleans, and routes your customer data to hundreds of tools. The Python SDK lets you send data to Segment from your backend services.

### Key Terminology

| Term | Description |
|------|-------------|
| **Source** | Where data comes from (your app, server, etc.) |
| **Destination** | Where data goes (analytics tools, warehouses, etc.) |
| **Write Key** | API key for a specific Source |
| **userId** | Your internal identifier for a user |
| **anonymousId** | ID for users who haven't logged in yet |
| **traits** | Attributes of a user (email, name, etc.) |
| **properties** | Attributes of an event (order total, product name, etc.) |

---

## Installation

```bash
pip install analytics-python
```

Or with Poetry/uv:
```bash
poetry add analytics-python
uv add analytics-python
```

---

## Configuration

### Basic Setup

```python
import analytics

# Set your write key (from Segment Source settings)
analytics.write_key = 'YOUR_WRITE_KEY'

# Optional: Enable debug logging
analytics.debug = True

# Optional: Increase queue size for high-volume apps
analytics.max_queue_size = 10000
```

### Configuration Options

| Option | Description | Default |
|--------|-------------|---------|
| `write_key` | Your Segment source write key | Required |
| `debug` | Log debug information | `False` |
| `max_queue_size` | Max events to queue before blocking | `10000` |
| `sync_mode` | Send events synchronously | `False` |
| `send` | Actually send events (set False for testing) | `True` |

### Environment-Based Configuration

```python
import os
import analytics

analytics.write_key = os.environ.get('SEGMENT_WRITE_KEY')
analytics.debug = os.environ.get('SEGMENT_DEBUG', 'false').lower() == 'true'
```

---

## Core Concepts

### The Two Main Methods

1. **`identify()`** - Tell Segment WHO the user is
2. **`track()`** - Tell Segment WHAT the user did

### When to Use Each

```
User signs up     → identify()    "Here's who they are"
User updates profile → identify() "Here's updated info"
User places order → track()       "Order Completed"
User views page   → track()       "Page Viewed"
User clicks button → track()      "Button Clicked"
```

### The Flush Mechanism

The SDK queues events and sends them in batches. You must call `flush()` to ensure all events are sent before your program exits.

```python
analytics.identify(user_id='123', traits={'email': 'user@example.com'})
analytics.track(user_id='123', event='Order Completed', properties={'total': 99.99})

# IMPORTANT: Always flush before exiting
analytics.flush()
```

---

## The identify() Method

Use `identify()` to associate a user with their traits (attributes).

### Basic Usage

```python
analytics.identify(
    user_id='user_123',
    traits={
        'email': 'john@example.com',
        'firstName': 'John',
        'lastName': 'Doe',
    }
)
```

### Full Example with All Options

```python
from datetime import datetime, timezone

analytics.identify(
    # Required: Your internal user ID
    user_id='user_123',

    # User attributes
    traits={
        # Standard traits (Segment recognizes these)
        'email': 'john@example.com',
        'firstName': 'John',
        'lastName': 'Doe',
        'phone': '+1-555-555-5555',
        'address': {
            'street': '123 Main St',
            'city': 'San Francisco',
            'state': 'CA',
            'postalCode': '94105',
            'country': 'US',
        },
        'createdAt': '2024-01-15T10:30:00Z',

        # Custom traits (anything you want)
        'plan': 'premium',
        'loyaltyPoints': 500,
        'lifetimeValue': 1250.00,
    },

    # Optional: Additional context
    context={
        # External IDs for identity resolution
        'externalIds': [{
            'id': 'CUST_12345',
            'type': 'customerId',
            'collection': 'users',
            'encoding': 'none',
        }],
    },

    # Optional: When this actually happened (for backfilling)
    timestamp=datetime.now(timezone.utc),

    # Optional: Unique ID for this event (for deduplication)
    message_id='unique-id-123',
)
```

### Standard Traits

Segment recognizes these trait names and handles them specially in many destinations:

| Trait | Type | Description |
|-------|------|-------------|
| `email` | String | User's email address |
| `firstName` | String | First name |
| `lastName` | String | Last name |
| `name` | String | Full name |
| `phone` | String | Phone number |
| `address` | Object | Mailing address |
| `age` | Number | User's age |
| `birthday` | Date | Birthday |
| `company` | Object | Company info |
| `createdAt` | Date | When user was created |
| `description` | String | User description |
| `gender` | String | Gender |
| `title` | String | Job title |
| `username` | String | Username |
| `website` | String | Website URL |
| `avatar` | String | Avatar image URL |

---

## The track() Method

Use `track()` to record actions users take.

### Basic Usage

```python
analytics.track(
    user_id='user_123',
    event='Order Completed',
    properties={
        'orderId': 'ORD-12345',
        'total': 99.99,
    }
)
```

### Full Example with All Options

```python
from datetime import datetime, timezone

analytics.track(
    # Required: Your internal user ID
    user_id='user_123',

    # Required: Event name (use Title Case)
    event='Order Completed',

    # Event attributes
    properties={
        # Order info
        'orderId': 'ORD-12345',
        'revenue': 99.99,      # Total revenue
        'subtotal': 89.99,     # Before tax/shipping
        'tax': 8.00,
        'shipping': 5.00,
        'discount': 10.00,
        'coupon': 'SAVE10',
        'currency': 'USD',

        # Products (following eCommerce spec)
        'products': [
            {
                'product_id': 'PROD-001',
                'sku': 'SKU-001',
                'name': 'Running Shoes',
                'price': 89.99,
                'quantity': 1,
                'category': 'Footwear',
                'brand': 'Nike',
            }
        ],

        # Custom properties
        'storeId': 'STORE-123',
        'paymentMethod': 'Credit Card',
    },

    # Optional: Additional context
    context={
        'traits': {'email': 'john@example.com'},  # Helps with identity
    },

    # Optional: When this actually happened
    timestamp=datetime.now(timezone.utc),

    # Optional: Unique ID for deduplication
    message_id='order-ORD-12345-completed',
)
```

### Segment eCommerce Spec Events

Segment has a standard spec for eCommerce events. Using these exact names ensures compatibility with all eCommerce destinations:

| Event | When to Use |
|-------|-------------|
| `Products Searched` | User searches for products |
| `Product List Viewed` | User views a category/list |
| `Product Viewed` | User views a product detail page |
| `Product Added` | User adds item to cart |
| `Product Removed` | User removes item from cart |
| `Cart Viewed` | User views their cart |
| `Checkout Started` | User begins checkout |
| `Checkout Step Completed` | User completes a checkout step |
| `Payment Info Entered` | User enters payment info |
| `Order Completed` | User completes purchase |
| `Order Refunded` | Order is refunded |

### Event Naming Conventions

✅ **Do:**
- Use Title Case: `Order Completed`
- Be specific: `Button Clicked` with properties, not `Blue Button Clicked`
- Use past tense: `Order Completed`, not `Complete Order`

❌ **Don't:**
- Use camelCase: `orderCompleted`
- Use snake_case: `order_completed`
- Include data in event name: `Order 12345 Completed`

---

## Best Practices

### 1. Always Use a Stable userId

```python
# ✅ Good: Use your internal database ID
analytics.identify(user_id='db_user_12345', traits={...})

# ❌ Bad: Using email (can change)
analytics.identify(user_id='john@example.com', traits={...})
```

### 2. Use Idempotent message_ids

Generate stable IDs so retries don't create duplicates:

```python
import hashlib

def generate_message_id(entity_type: str, entity_id: str, event: str) -> str:
    """Generate a stable, unique message ID."""
    key = f"{entity_type}:{entity_id}:{event}"
    hash_hex = hashlib.sha256(key.encode()).hexdigest()
    return f"{hash_hex[:8]}-{hash_hex[8:12]}-{hash_hex[12:16]}-{hash_hex[16:20]}-{hash_hex[20:32]}"

# Same inputs always produce same message_id
msg_id = generate_message_id('order', 'ORD-123', 'Order Completed')
analytics.track(user_id='123', event='Order Completed', message_id=msg_id, ...)
```

### 3. Batch and Flush Properly

```python
# For bulk operations, flush periodically
batch_size = 100
for i, record in enumerate(records):
    analytics.track(...)

    if (i + 1) % batch_size == 0:
        analytics.flush()

# Always flush at the end
analytics.flush()
```

### 4. Include Timestamps for Historical Data

```python
from datetime import datetime, timezone

# For backfilling historical data
analytics.track(
    user_id='123',
    event='Order Completed',
    properties={...},
    timestamp=datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc),
)
```

### 5. Use Context for Identity Resolution

```python
# Include email in context to help Segment match users
analytics.track(
    user_id='123',
    event='Order Completed',
    properties={...},
    context={
        'traits': {'email': 'john@example.com'}
    }
)
```

---

## Error Handling

### The SDK's Default Behavior

By default, the SDK:
- Queues events in memory
- Sends them asynchronously in batches
- Silently drops events if the queue is full
- Does NOT raise exceptions on API errors

### Handling Errors in Production

```python
from analytics.request import APIError
import time

def send_with_retry(func, max_retries=3):
    """Send to Segment with retry logic."""
    delay = 1.0

    for attempt in range(max_retries + 1):
        try:
            func()
            analytics.flush()
            return True
        except APIError as e:
            # Don't retry client errors (4xx)
            if hasattr(e, 'status') and 400 <= e.status < 500:
                print(f"Permanent error: {e}")
                return False

            if attempt < max_retries:
                print(f"Retry {attempt + 1}/{max_retries} after {delay}s: {e}")
                time.sleep(delay)
                delay *= 2  # Exponential backoff
            else:
                print(f"Max retries exceeded: {e}")
                return False
        except Exception as e:
            print(f"Unexpected error: {e}")
            return False

# Usage
success = send_with_retry(
    lambda: analytics.track(user_id='123', event='Test', properties={})
)
```

### Custom Error Callback

```python
def on_error(error, items):
    """Called when the SDK fails to send events."""
    print(f"Segment error: {error}")
    print(f"Failed items: {len(items)}")
    # Log to your monitoring system
    # Store failed items for retry

analytics.on_error = on_error
```

---

## Testing

### Disable Sending in Tests

```python
import analytics

# In your test setup
analytics.send = False  # Events are tracked but not sent

# Run your code
analytics.identify(user_id='test', traits={'name': 'Test User'})
analytics.track(user_id='test', event='Test Event')

# Events were processed but not sent to Segment
```

### Verify Events Were Queued

```python
# Check the queue (for testing only)
print(f"Events in queue: {analytics.default_client.queue.qsize()}")
```

### Use a Test Source

Create a separate Segment Source for testing:
1. Go to Segment → Sources → Add Source
2. Name it "Python Test" or similar
3. Use this write key in your test environment

---

## Production Checklist

Before deploying to production:

- [ ] **Write key from environment variable** - Never hardcode
- [ ] **Retry logic implemented** - Handle transient failures
- [ ] **Idempotent message IDs** - Prevent duplicates on retry
- [ ] **Proper flushing** - Call `flush()` before exit/between batches
- [ ] **Error handling** - Log and track failures
- [ ] **Rate limit awareness** - Segment allows 500 requests/second
- [ ] **Data validation** - Validate before sending
- [ ] **Timestamps for backfills** - Use actual event time, not sync time
- [ ] **Monitoring** - Track success/failure rates
- [ ] **Testing** - Test with `analytics.send = False`

---

## Quick Reference

### Identify a User

```python
analytics.identify(
    user_id='user_123',
    traits={
        'email': 'user@example.com',
        'name': 'John Doe',
    }
)
analytics.flush()
```

### Track an Event

```python
analytics.track(
    user_id='user_123',
    event='Order Completed',
    properties={
        'orderId': 'ORD-123',
        'revenue': 99.99,
    }
)
analytics.flush()
```

### Full Production Pattern

```python
import analytics
import os

# Configure
analytics.write_key = os.environ['SEGMENT_WRITE_KEY']

# Send events
try:
    analytics.identify(user_id='123', traits={'email': 'user@example.com'})
    analytics.track(user_id='123', event='Order Completed', properties={'revenue': 99.99})
    analytics.flush()
    print("Events sent successfully")
except Exception as e:
    print(f"Failed to send events: {e}")
    # Handle failure (log, retry, etc.)
```

---

## Additional Resources

- [Analytics for Python (Official Docs)](https://www.twilio.com/docs/segment/connections/sources/catalog/libraries/server/python)
- [Segment eCommerce Spec](https://segment.com/docs/connections/spec/ecommerce/v2/)
- [Segment Identify Spec](https://segment.com/docs/connections/spec/identify/)
- [Segment Track Spec](https://segment.com/docs/connections/spec/track/)
- [GitHub: analytics-python](https://github.com/segmentio/analytics-python)
