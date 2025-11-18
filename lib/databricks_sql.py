import os
import socket
import time
import pandas as pd
import requests
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

@st.cache_data(show_spinner=False)
def run_sql(query: str):
    """Execute SQL via Databricks SQL Statements REST API and return a DataFrame."""
    # Read Databricks config from environment (same names used in assignmentTwo.py)
    DATABRICKS_SERVER_HOSTNAME = os.getenv("DATABRICKS_SERVER_HOSTNAME")
    DATABRICKS_HOST_URL = os.getenv("DATABRICKS_HOST_URL") or (f"https://{DATABRICKS_SERVER_HOSTNAME}" if DATABRICKS_SERVER_HOSTNAME else None)
    DATABRICKS_WAREHOUSE_ID = os.getenv("DATABRICKS_WAREHOUSE_ID")
    DATABRICKS_PERSONAL_ACCESS_TOKEN = os.getenv("DATABRICKS_PERSONAL_ACCESS_TOKEN")

    # Quick network precheck to avoid long hangs on unreachable host
    if DATABRICKS_SERVER_HOSTNAME:
        try:
            sock = socket.create_connection((DATABRICKS_SERVER_HOSTNAME, 443), timeout=5)
            sock.close()
        except Exception as e:
            raise RuntimeError(f"Network error reaching {DATABRICKS_SERVER_HOSTNAME}: {e}")

    if not (DATABRICKS_HOST_URL and DATABRICKS_WAREHOUSE_ID and DATABRICKS_PERSONAL_ACCESS_TOKEN):
        raise RuntimeError(
            "Missing Databricks REST API configuration. Ensure DATABRICKS_HOST_URL (or DATABRICKS_SERVER_HOSTNAME), "
            "DATABRICKS_WAREHOUSE_ID, and DATABRICKS_PERSONAL_ACCESS_TOKEN are set."
        )

    url = f"{DATABRICKS_HOST_URL.rstrip('/')}/api/2.0/sql/statements"
    headers = {
        "Authorization": f"Bearer {DATABRICKS_PERSONAL_ACCESS_TOKEN}",
        "Content-Type": "application/json",
    }
    payload = {
        "warehouse_id": DATABRICKS_WAREHOUSE_ID,
        "statement": query,
        "wait_timeout": "30s",
    }

    t0 = time.time()
    resp = requests.post(url, headers=headers, json=payload, timeout=60)
    if resp.status_code != 200:
        raise RuntimeError(f"Error submitting statement: {resp.status_code} - {resp.text}")
    body = resp.json()

    # If result is immediately available
    if body.get("status", {}).get("state") in ("SUCCEEDED",) and body.get("result"):
        result = body["result"]
    else:
        # Poll for completion
        stmt_id = body.get("statement_id") or body.get("statement", {}).get("statement_id") or body.get("id")
        if not stmt_id:
            # Some responses include result directly
            if body.get("result"):
                result = body["result"]
            else:
                raise RuntimeError("Did not receive a statement_id or result from Databricks API.")
        else:
            poll_url = f"{url}/{stmt_id}"
            deadline = time.time() + 120  # up to 2 minutes
            last_state = None
            while time.time() < deadline:
                r = requests.get(poll_url, headers=headers, timeout=30)
                if r.status_code != 200:
                    raise RuntimeError(f"Polling error: {r.status_code} - {r.text}")
                data = r.json()
                state = data.get("status", {}).get("state")
                if state != last_state:
                    last_state = state
                if state in ("SUCCEEDED",):
                    result = data.get("result")
                    break
                if state in ("FAILED", "CANCELED"):
                    raise RuntimeError(f"Statement {state.lower()}: {data}")
                time.sleep(1.0)
            else:
                raise RuntimeError("Timed out waiting for statement to complete.")


    print(result)
    
    
    if not result:
        raise RuntimeError("No result returned from Databricks API.")

    # Parse columns and rows
    rows = result.get("data_array", [])
    
    # Manual column names matching schema (22 columns, no duplicates)
    cols = [
        "time", "latitude", "longitude", "depth", "mag", "magType",
        "nst", "gap", "dmin", "rms", "net", "id", "updated", "place", "type",
        "horizontalError", "depthError", "magError", "magNst", "status",
        "locationSource", "magSource"
    ]

    out_df = pd.DataFrame(rows, columns=cols)
    out_df.attrs["_query_ms"] = int((time.time() - t0) * 1000)
    return out_df

@st.cache_data(show_spinner=False)
def run_food_sql(query: str):
    """Execute SQL via Databricks SQL Statements REST API and return a DataFrame."""
    # Read Databricks config from environment (same names used in assignmentTwo.py)
    DATABRICKS_SERVER_HOSTNAME = os.getenv("DATABRICKS_SERVER_HOSTNAME")
    DATABRICKS_HOST_URL = os.getenv("DATABRICKS_HOST_URL") or (f"https://{DATABRICKS_SERVER_HOSTNAME}" if DATABRICKS_SERVER_HOSTNAME else None)
    DATABRICKS_WAREHOUSE_ID = os.getenv("DATABRICKS_WAREHOUSE_ID")
    DATABRICKS_PERSONAL_ACCESS_TOKEN = os.getenv("DATABRICKS_PERSONAL_ACCESS_TOKEN")

    # Quick network precheck to avoid long hangs on unreachable host
    if DATABRICKS_SERVER_HOSTNAME:
        try:
            sock = socket.create_connection((DATABRICKS_SERVER_HOSTNAME, 443), timeout=5)
            sock.close()
        except Exception as e:
            raise RuntimeError(f"Network error reaching {DATABRICKS_SERVER_HOSTNAME}: {e}")

    if not (DATABRICKS_HOST_URL and DATABRICKS_WAREHOUSE_ID and DATABRICKS_PERSONAL_ACCESS_TOKEN):
        raise RuntimeError(
            "Missing Databricks REST API configuration. Ensure DATABRICKS_HOST_URL (or DATABRICKS_SERVER_HOSTNAME), "
            "DATABRICKS_WAREHOUSE_ID, and DATABRICKS_PERSONAL_ACCESS_TOKEN are set."
        )

    url = f"{DATABRICKS_HOST_URL.rstrip('/')}/api/2.0/sql/statements"
    headers = {
        "Authorization": f"Bearer {DATABRICKS_PERSONAL_ACCESS_TOKEN}",
        "Content-Type": "application/json",
    }
    payload = {
        "warehouse_id": DATABRICKS_WAREHOUSE_ID,
        "statement": query,
        "wait_timeout": "30s",
    }

    t0 = time.time()
    resp = requests.post(url, headers=headers, json=payload, timeout=60)
    if resp.status_code != 200:
        raise RuntimeError(f"Error submitting statement: {resp.status_code} - {resp.text}")
    body = resp.json()

    # If result is immediately available
    if body.get("status", {}).get("state") in ("SUCCEEDED",) and body.get("result"):
        result = body["result"]
    else:
        # Poll for completion
        stmt_id = body.get("statement_id") or body.get("statement", {}).get("statement_id") or body.get("id")
        if not stmt_id:
            # Some responses include result directly
            if body.get("result"):
                result = body["result"]
            else:
                raise RuntimeError("Did not receive a statement_id or result from Databricks API.")
        else:
            poll_url = f"{url}/{stmt_id}"
            deadline = time.time() + 120  # up to 2 minutes
            last_state = None
            while time.time() < deadline:
                r = requests.get(poll_url, headers=headers, timeout=30)
                if r.status_code != 200:
                    raise RuntimeError(f"Polling error: {r.status_code} - {r.text}")
                data = r.json()
                state = data.get("status", {}).get("state")
                if state != last_state:
                    last_state = state
                if state in ("SUCCEEDED",):
                    result = data.get("result")
                    break
                if state in ("FAILED", "CANCELED"):
                    raise RuntimeError(f"Statement {state.lower()}: {data}")
                time.sleep(1.0)
            else:
                raise RuntimeError("Timed out waiting for statement to complete.")


    print(result)
    
    
    if not result:
        raise RuntimeError("No result returned from Databricks API.")

    # Parse columns and rows
    rows = result.get("data_array", [])
    
    # Manual column names matching schema (22 columns, no duplicates)
    cols = [
        "Amount","Food","Category"
    ]

    out_df = pd.DataFrame(rows, columns=cols)
    out_df.attrs["_query_ms"] = int((time.time() - t0) * 1000)
    return out_df
