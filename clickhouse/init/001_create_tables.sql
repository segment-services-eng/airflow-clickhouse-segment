-- Retail Pro Schema for ClickHouse
-- These tables match the Retail Pro CSV export format

CREATE DATABASE IF NOT EXISTS retail;

-- Customers table (from customer.csv)
CREATE TABLE IF NOT EXISTS retail.customers (
    SID String,
    CUST_ID String,
    LAST_NAME String,
    FIRST_NAME String,
    EMAIL String,
    MARKETING_FLAG String,
    LTY_OPT_IN String,
    LTY_BALANCE String,
    TOTAL_TRANSACTIONS Int32,
    SALE_ITEM_COUNT Int32,
    RETURN_ITEM_COUNT Int32,
    YTD_SALE Float64,
    CREATED_DATETIME String,
    synced_to_segment Bool DEFAULT false
) ENGINE = MergeTree()
ORDER BY SID;

-- Documents table (from document.csv) - Orders/Transactions
CREATE TABLE IF NOT EXISTS retail.documents (
    SID String,
    DOC_NO String,
    BT_CUID String,
    BT_EMAIL String,
    ST_CUID String,
    ST_EMAIL String,
    SALE_TOTAL_AMT Float64,
    SALE_SUBTOTAL Float64,
    SALE_TOTAL_TAX_AMT Float64,
    TOTAL_DISCOUNT_AMT Float64,
    SHIPPING_AMT Float64,
    SOLD_QTY Int32,
    RETURN_QTY Int32,
    CURRENCY_NAME String,
    TENDER_NAME String,
    STORE_CODE String,
    STORE_NO String,
    SBS_NO String,
    SHIP_METHOD String,
    HAS_SALE String,
    HAS_RETURN String,
    POST_DATE String,
    CREATED_DATETIME String,
    synced_to_segment Bool DEFAULT false
) ENGINE = MergeTree()
ORDER BY SID;

-- Document Items table (from document_item.csv) - Line Items
CREATE TABLE IF NOT EXISTS retail.document_items (
    SID String,
    DOC_SID String,
    ITEM_POS Int32,
    ALU String,
    DESCRIPTION1 String,
    DCS_CODE String,
    VEND_CODE String,
    QTY Int32,
    PRICE Float64,
    ORIG_PRICE Float64,
    DISC_AMT Float64,
    TAX_AMT Float64,
    ITEM_SIZE String,
    ATTRIBUTE String,
    INVN_SBS_ITEM_SID String
) ENGINE = MergeTree()
ORDER BY (DOC_SID, SID);

-- Failed Events table - Tracks sync failures for recovery and monitoring
CREATE TABLE IF NOT EXISTS retail.failed_events (
    id UUID DEFAULT generateUUIDv4(),
    entity_type String,           -- 'customer' or 'order'
    entity_id String,             -- Source system ID (SID)
    event_type String,            -- 'identify' or 'track'
    error_message String,         -- What went wrong
    error_category String,        -- 'transient', 'permanent', or 'validation'
    payload String,               -- Original event data (for debugging)
    created_at DateTime DEFAULT now(),
    retry_count UInt8 DEFAULT 0,
    resolved Bool DEFAULT false   -- Mark true after manual resolution
) ENGINE = MergeTree()
ORDER BY (created_at, entity_type, entity_id)
TTL created_at + INTERVAL 30 DAY;  -- Auto-cleanup after 30 days
