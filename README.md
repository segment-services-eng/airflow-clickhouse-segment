# Airflow ClickHouse → Segment Integration

Production-ready Apache Airflow DAG that syncs Retail Pro data from ClickHouse to Segment. This demo shows how to build a reliable batch ETL pipeline for customer and order data.

## Quick Start

### Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (running)
- A Segment workspace with a Python source

### 1. Start the Services

```bash
cd airflow-clickhouse-segment
docker compose up -d
```

Wait ~60 seconds for initialization. Verify all services are healthy:

```bash
docker compose ps
```

You should see:
- `postgres` - Healthy
- `clickhouse` - Healthy
- `airflow-webserver` - Healthy
- `airflow-scheduler` - Running

### 2. Access Airflow

Open http://localhost:8080

- **Username:** `admin`
- **Password:** `admin`

### 3. Load Sample Data

```bash
# Install the ClickHouse client library
pip install clickhouse-connect

# Load Retail Pro CSV files
python load_retail_pro_data.py "/path/to/Retail Pro Data Files"
```

### 4. Configure Segment

In Airflow UI:
1. Go to **Admin → Variables**
2. Click **+** to add a new variable
3. Set:
   - **Key:** `SEGMENT_WRITE_KEY`
   - **Value:** Your Segment source write key

### 5. Run the Sync

Option A: **Manual trigger**
- Find `clickhouse_to_segment` DAG in the Airflow UI
- Click the **Play** button (▶) → Trigger DAG

Option B: **Wait for schedule**
- The DAG runs automatically every 15 minutes

### 6. Verify in Segment

1. Go to your Segment workspace
2. Navigate to **Sources → [Your Source] → Debugger**
3. You should see:
   - `identify` events for customers
   - `Order Completed` track events for orders

---

## Architecture

```
┌──────────────┐                          ┌─────────────┐
│  Retail Pro  │  ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ▶ │ ClickHouse  │
│   (POS)      │    (existing pipeline)   │             │
└──────────────┘                          └─────────────┘
                                                 │
                                                 │
                        Sync Pipeline (every 15 min)
                                                 │
                                                 ▼
┌────────────────────────────────────────────────────────────┐
│                         Airflow                            │
│  ┌───────────────┐  ┌───────────────┐  ┌───────────────┐  │
│  │Sync Customers │─▶│  Sync Orders  │─▶│    Report     │  │
│  │  (identify)   │  │    (track)    │  │    Results    │  │
│  └───────────────┘  └───────────────┘  └───────────────┘  │
└────────────────────────────────────────────────────────────┘
                                                 │
                                      Segment Python SDK
                                     (analytics-python)
                                                 │
                                                 ▼
                                         ┌─────────────┐
                                         │   Segment   │
                                         │ identify()  │
                                         │  track()    │
                                         └─────────────┘
```

---

## Production Features

This implementation includes production-ready features:

| Feature | Description |
|---------|-------------|
| **Chunked Processing** | Handles millions of rows without memory issues |
| **Batched Flushing** | Sends events in batches of 100 for efficiency |
| **Retry Logic** | Exponential backoff for transient failures |
| **Idempotency** | Stable message IDs prevent duplicates on retry |
| **Failed Event Tracking** | Records failures for monitoring and recovery |
| **Validation** | Validates data before sending to Segment |
| **Sync Flags** | Tracks what's been synced to avoid duplicates |

---

## Project Structure

```
airflow-clickhouse-segment/
├── docker-compose.yml              # Airflow + ClickHouse stack
├── load_retail_pro_data.py         # CSV → ClickHouse loader
├── dags/
│   └── clickhouse_to_segment_dag.py    # Airflow DAG definition
├── src/
│   └── segment_sync.py             # Core sync logic (production-ready)
├── clickhouse/
│   └── init/
│       └── 001_create_tables.sql   # Database schema
├── docs/
│   └── SEGMENT_SDK_GUIDE.md        # Segment Python SDK documentation
├── logs/                           # Airflow logs (gitignored)
└── plugins/                        # Custom Airflow plugins
```

---

## Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `SEGMENT_WRITE_KEY` | Segment source write key | Required |
| `SEGMENT_DEBUG` | Enable debug logging | `false` |
| `CLICKHOUSE_HOST` | ClickHouse hostname | `clickhouse` |
| `CLICKHOUSE_PORT` | ClickHouse HTTP port | `8123` |

### DAG Configuration

Edit `dags/clickhouse_to_segment_dag.py` to customize:

```python
schedule_interval='*/15 * * * *'  # Every 15 minutes
```

### Sync Configuration

Edit constants in `src/segment_sync.py`:

```python
DEFAULT_CHUNK_SIZE = 500      # Rows per ClickHouse query
DEFAULT_BATCH_SIZE = 100      # Events per Segment flush
MAX_RETRIES = 3               # Retry attempts for failures
INITIAL_RETRY_DELAY = 1.0     # Starting delay (seconds)
```

---

## Monitoring

### Check Sync Status

```bash
# View recent DAG runs
docker compose exec airflow-webserver airflow dags list-runs -d clickhouse_to_segment

# Check task logs
docker compose exec airflow-webserver cat /opt/airflow/logs/dag_id=clickhouse_to_segment/run_id=<run_id>/task_id=sync_customers/attempt=1.log
```

### Check Failed Events

```sql
-- Connect to ClickHouse
docker compose exec clickhouse clickhouse-client

-- View unresolved failures
SELECT entity_type, error_category, count()
FROM retail.failed_events
WHERE resolved = false
GROUP BY entity_type, error_category;

-- View recent failures with details
SELECT * FROM retail.failed_events
ORDER BY created_at DESC
LIMIT 10;
```

### Check Sync Flags

```sql
-- How many records are pending sync?
SELECT synced_to_segment, count() FROM retail.customers GROUP BY synced_to_segment;
SELECT synced_to_segment, count() FROM retail.documents GROUP BY synced_to_segment;
```

---

## Common Operations

### Reset and Re-sync All Data

```bash
docker compose exec clickhouse clickhouse-client --query "
ALTER TABLE retail.customers UPDATE synced_to_segment = false WHERE 1=1
"
docker compose exec clickhouse clickhouse-client --query "
ALTER TABLE retail.documents UPDATE synced_to_segment = false WHERE 1=1
"
```

### Add New Data

```bash
# Reload from CSVs
python load_retail_pro_data.py "/path/to/new/csvs"
```

The next DAG run will automatically pick up new records.

### Mark Failed Events as Resolved

```sql
ALTER TABLE retail.failed_events
UPDATE resolved = true
WHERE entity_id = 'PROBLEM_ID';
```

---

## Stopping the Demo

```bash
# Stop containers (keeps data)
docker compose down

# Stop and remove all data
docker compose down -v
```

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| Airflow UI not loading | Run `docker compose restart airflow-webserver` |
| "No valid SEGMENT_WRITE_KEY" | Add the variable in Airflow UI: Admin → Variables |
| 0 records to sync | All records already synced - reset flags if needed |
| Events not in Segment | Check write key matches the Source you're viewing |
| ClickHouse connection error | Verify `docker compose ps` shows healthy status |

### View Detailed Logs

```bash
# Airflow scheduler
docker compose logs airflow-scheduler -f

# Airflow webserver
docker compose logs airflow-webserver -f

# ClickHouse
docker compose logs clickhouse -f
```

---

## Next Steps

- See [SEGMENT_SDK_GUIDE.md](docs/SEGMENT_SDK_GUIDE.md) for Segment Python SDK documentation
- Customize the sync logic in `src/segment_sync.py` for your data model
- Add alerting (Slack, email) in the `report_results_task`

---

## License

MIT
