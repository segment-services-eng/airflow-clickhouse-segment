#!/usr/bin/env python3
"""Load Retail Pro CSV files into ClickHouse."""

import csv
import sys
from pathlib import Path

import clickhouse_connect


def load_customers(client, data_dir: Path):
    """Load customer.csv into ClickHouse."""
    filepath = data_dir / "customer.csv"

    with open(filepath, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        rows = []
        for row in reader:
            rows.append({
                "SID": row.get("SID", ""),
                "CUST_ID": row.get("CUST_ID", ""),
                "LAST_NAME": row.get("LAST_NAME", ""),
                "FIRST_NAME": row.get("FIRST_NAME", ""),
                "EMAIL": row.get("EMAIL", ""),
                "MARKETING_FLAG": row.get("MARKETING_FLAG", "0"),
                "LTY_OPT_IN": row.get("LTY_OPT_IN", "0"),
                "LTY_BALANCE": row.get("LTY_BALANCE", "0"),
                "TOTAL_TRANSACTIONS": int(row.get("TOTAL_TRANSACTIONS", 0) or 0),
                "SALE_ITEM_COUNT": int(row.get("SALE_ITEM_COUNT", 0) or 0),
                "RETURN_ITEM_COUNT": int(row.get("RETURN_ITEM_COUNT", 0) or 0),
                "YTD_SALE": float(row.get("YTD_SALE", 0) or 0),
                "CREATED_DATETIME": row.get("CREATED_DATETIME", ""),
            })

    if rows:
        columns = list(rows[0].keys())
        data = [[row[col] for col in columns] for row in rows]
        client.insert("retail.customers", data, column_names=columns)
        print(f"Loaded {len(rows)} customers")


def load_documents(client, data_dir: Path):
    """Load document.csv into ClickHouse."""
    filepath = data_dir / "document.csv"

    with open(filepath, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        rows = []
        for row in reader:
            rows.append({
                "SID": row.get("SID", ""),
                "DOC_NO": row.get("DOC_NO", ""),
                "BT_CUID": row.get("BT_CUID", ""),
                "BT_EMAIL": row.get("BT_EMAIL", ""),
                "ST_CUID": row.get("ST_CUID", ""),
                "ST_EMAIL": row.get("ST_EMAIL", ""),
                "SALE_TOTAL_AMT": float(row.get("SALE_TOTAL_AMT", 0) or 0),
                "SALE_SUBTOTAL": float(row.get("SALE_SUBTOTAL", 0) or 0),
                "SALE_TOTAL_TAX_AMT": float(row.get("SALE_TOTAL_TAX_AMT", 0) or 0),
                "TOTAL_DISCOUNT_AMT": float(row.get("TOTAL_DISCOUNT_AMT", 0) or 0),
                "SHIPPING_AMT": float(row.get("SHIPPING_AMT", 0) or 0),
                "SOLD_QTY": int(row.get("SOLD_QTY", 0) or 0),
                "RETURN_QTY": int(row.get("RETURN_QTY", 0) or 0),
                "CURRENCY_NAME": row.get("CURRENCY_NAME", ""),
                "TENDER_NAME": row.get("TENDER_NAME", ""),
                "STORE_CODE": row.get("STORE_CODE", ""),
                "STORE_NO": row.get("STORE_NO", ""),
                "SBS_NO": row.get("SBS_NO", ""),
                "SHIP_METHOD": row.get("SHIP_METHOD", ""),
                "HAS_SALE": row.get("HAS_SALE", "0"),
                "HAS_RETURN": row.get("HAS_RETURN", "0"),
                "POST_DATE": row.get("POST_DATE", ""),
                "CREATED_DATETIME": row.get("CREATED_DATETIME", ""),
            })

    if rows:
        columns = list(rows[0].keys())
        data = [[row[col] for col in columns] for row in rows]
        client.insert("retail.documents", data, column_names=columns)
        print(f"Loaded {len(rows)} documents")


def load_document_items(client, data_dir: Path):
    """Load document_item.csv into ClickHouse."""
    filepath = data_dir / "document_item.csv"

    with open(filepath, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        rows = []
        for row in reader:
            rows.append({
                "SID": row.get("SID", ""),
                "DOC_SID": row.get("DOC_SID", ""),
                "ITEM_POS": int(row.get("ITEM_POS", 0) or 0),
                "ALU": row.get("ALU", ""),
                "DESCRIPTION1": row.get("DESCRIPTION1", ""),
                "DCS_CODE": row.get("DCS_CODE", ""),
                "VEND_CODE": row.get("VEND_CODE", ""),
                "QTY": int(row.get("QTY", 1) or 1),
                "PRICE": float(row.get("PRICE", 0) or 0),
                "ORIG_PRICE": float(row.get("ORIG_PRICE", 0) or 0),
                "DISC_AMT": float(row.get("DISC_AMT", 0) or 0),
                "TAX_AMT": float(row.get("TAX_AMT", 0) or 0),
                "ITEM_SIZE": row.get("ITEM_SIZE", ""),
                "ATTRIBUTE": row.get("ATTRIBUTE", ""),
                "INVN_SBS_ITEM_SID": row.get("INVN_SBS_ITEM_SID", ""),
            })

    if rows:
        columns = list(rows[0].keys())
        data = [[row[col] for col in columns] for row in rows]
        client.insert("retail.document_items", data, column_names=columns)
        print(f"Loaded {len(rows)} document items")


def main():
    data_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(".")

    client = clickhouse_connect.get_client(
        host="localhost",
        port=8123,
        username="default",
        password="",
    )

    print(f"Loading data from {data_dir}")
    load_customers(client, data_dir)
    load_documents(client, data_dir)
    load_document_items(client, data_dir)
    print("Done!")


if __name__ == "__main__":
    main()
