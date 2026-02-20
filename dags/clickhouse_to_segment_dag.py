"""
Airflow DAG: ClickHouse to Segment Sync

Production-ready DAG that syncs Retail Pro data from ClickHouse to Segment:
1. Syncs customers as identify() calls
2. Syncs orders as 'Order Completed' track() calls
3. Reports sync results with failed event monitoring

Schedule: Every 15 minutes (configurable)
"""

import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))


def sync_customers_task(**context):
    """
    Task to sync customers from ClickHouse to Segment.

    Sends identify() calls for all unsynced customers.
    """
    from segment_sync import sync_customers, init_segment

    # Get write key from Airflow Variable
    write_key = Variable.get('SEGMENT_WRITE_KEY', default_var=None)

    # Initialize Segment
    segment_enabled = init_segment(write_key=write_key)

    # Get last sync time from Airflow Variables (optional)
    last_sync_str = Variable.get('last_customer_sync', default_var=None)
    last_sync_time = datetime.fromisoformat(last_sync_str) if last_sync_str else None

    # Run sync
    result = sync_customers(
        last_sync_time=last_sync_time,
        dry_run=not segment_enabled,
        batch_size=100,
        chunk_size=500,
    )

    # Update last sync time
    Variable.set('last_customer_sync', datetime.utcnow().isoformat())

    # Push result to XCom for downstream tasks
    context['task_instance'].xcom_push(key='customer_sync_result', value=result)

    print(f"Customer sync complete: {result}")
    return result


def sync_orders_task(**context):
    """
    Task to sync orders from ClickHouse to Segment.

    Sends track('Order Completed') calls for all unsynced orders.
    """
    from segment_sync import sync_orders, init_segment

    # Get write key from Airflow Variable
    write_key = Variable.get('SEGMENT_WRITE_KEY', default_var=None)

    # Initialize Segment
    segment_enabled = init_segment(write_key=write_key)

    # Get last sync time from Airflow Variables (optional)
    last_sync_str = Variable.get('last_order_sync', default_var=None)
    last_sync_time = datetime.fromisoformat(last_sync_str) if last_sync_str else None

    # Run sync
    result = sync_orders(
        last_sync_time=last_sync_time,
        dry_run=not segment_enabled,
        batch_size=100,
        chunk_size=500,
    )

    # Update last sync time
    Variable.set('last_order_sync', datetime.utcnow().isoformat())

    # Push result to XCom for downstream tasks
    context['task_instance'].xcom_push(key='order_sync_result', value=result)

    print(f"Order sync complete: {result}")
    return result


def report_results_task(**context):
    """
    Task to report sync results and check for failed events.

    This is where you would add alerting (Slack, email, etc.).
    """
    from segment_sync import get_failed_events_summary, get_clickhouse_client

    ti = context['task_instance']

    customer_result = ti.xcom_pull(task_ids='sync_customers', key='customer_sync_result') or {}
    order_result = ti.xcom_pull(task_ids='sync_orders', key='order_sync_result') or {}

    # Get failed events summary
    failed_summary = get_failed_events_summary()

    print("=" * 60)
    print("SYNC SUMMARY")
    print("=" * 60)
    print(f"Customers:")
    print(f"  - Synced:  {customer_result.get('synced', 0)}")
    print(f"  - Failed:  {customer_result.get('failed', 0)}")
    print(f"  - Skipped: {customer_result.get('skipped', 0)}")
    print()
    print(f"Orders:")
    print(f"  - Synced:  {order_result.get('synced', 0)}")
    print(f"  - Failed:  {order_result.get('failed', 0)}")
    print(f"  - Skipped: {order_result.get('skipped', 0)}")
    print()
    print(f"Failed Events (unresolved): {failed_summary.get('total_unresolved', 0)}")
    if failed_summary.get('by_category'):
        print(f"  By category: {failed_summary['by_category']}")
    if failed_summary.get('by_entity'):
        print(f"  By entity:   {failed_summary['by_entity']}")
    print("=" * 60)

    # Calculate totals
    total_synced = customer_result.get('synced', 0) + order_result.get('synced', 0)
    total_failed = customer_result.get('failed', 0) + order_result.get('failed', 0)
    total_skipped = customer_result.get('skipped', 0) + order_result.get('skipped', 0)

    # Alert on failures
    if total_failed > 0:
        print(f"⚠️  WARNING: {total_failed} records failed to sync")
        # TODO: Add Slack/email notification here
        # send_slack_alert(f"Segment sync had {total_failed} failures")

    if failed_summary.get('total_unresolved', 0) > 100:
        print(f"🚨 ALERT: {failed_summary['total_unresolved']} unresolved failed events!")
        # TODO: Add escalation here

    return {
        'customers': customer_result,
        'orders': order_result,
        'total_synced': total_synced,
        'total_failed': total_failed,
        'total_skipped': total_skipped,
        'failed_events': failed_summary,
    }


# DAG default arguments
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
}

# Create the DAG
with DAG(
    dag_id='clickhouse_to_segment',
    default_args=default_args,
    description='Sync customer and order data from ClickHouse to Segment',
    schedule_interval='*/15 * * * *',  # Every 15 minutes
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['segment', 'clickhouse', 'etl', 'production'],
) as dag:

    # Task 1: Sync customers (identify calls)
    sync_customers = PythonOperator(
        task_id='sync_customers',
        python_callable=sync_customers_task,
        provide_context=True,
    )

    # Task 2: Sync orders (track calls) - depends on customers being synced first
    sync_orders = PythonOperator(
        task_id='sync_orders',
        python_callable=sync_orders_task,
        provide_context=True,
    )

    # Task 3: Report results and check for failures
    report_results = PythonOperator(
        task_id='report_results',
        python_callable=report_results_task,
        provide_context=True,
    )

    # Define task dependencies
    # Customers must sync first (for identity resolution), then orders, then report
    sync_customers >> sync_orders >> report_results
