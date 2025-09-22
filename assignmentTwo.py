
# streamlit_app.py
import os
import io
import socket
import time
import pandas as pd
import streamlit as st
import requests

# Try to import geopy for geocoding, fall back to manual mapping if not available
try:
    from geopy.geocoders import Nominatim
    GEOPY_AVAILABLE = True
except ImportError:
    GEOPY_AVAILABLE = False

# Optional: load .env if present
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

DATABRICKS_SERVER_HOSTNAME = os.getenv("DATABRICKS_SERVER_HOSTNAME")
DATABRICKS_HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH")
DATABRICKS_PERSONAL_ACCESS_TOKEN = os.getenv("DATABRICKS_PERSONAL_ACCESS_TOKEN")
# REST API specific
DATABRICKS_HOST_URL = os.getenv("DATABRICKS_HOST_URL") or (
    f"https://{DATABRICKS_SERVER_HOSTNAME}" if DATABRICKS_SERVER_HOSTNAME else None
)
DATABRICKS_WAREHOUSE_ID = os.getenv("DATABRICKS_WAREHOUSE_ID")

# ---- App Config ----
st.set_page_config(page_title="Earthquake Data Explorer", layout="wide")
st.title("Earthquake Data Explorer")
st.caption("Explore and analyze earthquake data from the Databricks earthquake_data table.")

# Shared state
df = None
source_label = None

@st.cache_data(show_spinner=False)
def run_sql(query: str):
    """Execute SQL via Databricks SQL Statements REST API and return a DataFrame."""
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

    if not result:
        raise RuntimeError("No result returned from Databricks API.")

    # Parse columns and rows
    rows = result.get("data_array", [])
    # Try to extract column names from manifest schema
    cols = []
    manifest = result.get("manifest") or {}
    schema = manifest.get("schema") or {}
    columns_meta = schema.get("columns") or []
    if columns_meta:
        cols = [c.get("name", f"col{i}") for i, c in enumerate(columns_meta)]
    elif result.get("external_links"):  # fallback not used here
        cols = [f"col{i}" for i in range(len(rows[0]) if rows else 0)]
    else:
        cols = [f"col{i}" for i in range(len(rows[0]) if rows else 0)]

    out_df = pd.DataFrame(rows, columns=cols)
    out_df.attrs["_query_ms"] = int((time.time() - t0) * 1000)
    return out_df

# ---- Load Data ----
# Check if Databricks credentials are available for REST API usage
if not (DATABRICKS_PERSONAL_ACCESS_TOKEN and (DATABRICKS_HOST_URL or DATABRICKS_SERVER_HOSTNAME) and DATABRICKS_WAREHOUSE_ID):
    st.error(
        "Databricks REST API credentials not found. Please set DATABRICKS_PERSONAL_ACCESS_TOKEN, "
        "DATABRICKS_WAREHOUSE_ID, and either DATABRICKS_HOST_URL or DATABRICKS_SERVER_HOSTNAME in your environment."
    )
    st.stop()

st.sidebar.subheader("Earthquake Data Settings")
table_fqn = "hive_metastore.default.earthquake_data"  # Hardcoded table name
limit = st.sidebar.number_input("Preview row limit", min_value=10, max_value=200000, value=5000, step=1000)
# Auto-load data on app start
try:
    df = run_sql(f"SELECT * FROM {table_fqn} LIMIT {int(limit)}")
    source_label = f"Earthquake data table: `{table_fqn}`"
except Exception as e:
    st.error(f"Failed to query earthquake data. {e}")
    st.stop()

if df is None:
    st.error("Failed to load earthquake data.")
    st.stop()

# Initialize session state
if 'show_results' not in st.session_state:
    st.session_state.show_results = False
if 'query_params' not in st.session_state:
    st.session_state.query_params = {}

# Navigation
if st.session_state.show_results:
    if st.button("← Back to Query Form"):
        st.session_state.show_results = False
        st.rerun()
    st.header("Query Results")
else:
    st.header("Build Your Earthquake Data Query")

if not st.session_state.show_results:
    # STEP 1: QUERY FORM
    with st.form("earthquake_query_form"):
        # Helper functions
        def _num_range(col: str, default=(0.0, 10.0)):
            try:
                ser = pd.to_numeric(df[col], errors="coerce")
                cmin, cmax = float(ser.min()), float(ser.max())
                return (cmin, cmax) if not (pd.isna(cmin) or pd.isna(cmax)) else default
            except:
                return default
        
        def _uniqs(col: str, max_vals: int = 50):
            try:
                return sorted(df[col].dropna().astype(str).unique().tolist())[:max_vals]
            except:
                return []
        
        # Try to get default dates from data, otherwise use reasonable defaults
        default_start = None
        default_end = None
        
        if "time" in df.columns:
            try:
                tser = pd.to_datetime(df["time"], errors="coerce")
                tmin, tmax = tser.min(), tser.max()
                if not pd.isna(tmin) and not pd.isna(tmax):
                    default_start = tmin.date()
                    default_end = tmax.date()
            except:
                pass
        
        # If we couldn't get dates from data, use reasonable defaults
        if default_start is None:
            default_start = pd.to_datetime("2020-01-01").date()
        if default_end is None:
            default_end = pd.to_datetime("2025-12-31").date()
        
        # Form fields in a clean layout
        col1, col2 = st.columns(2)
        with col1:
            time_min = st.date_input("Start date", value=default_start)
        with col2:
            time_max = st.date_input("End date", value=default_end)
        
        col1, col2 = st.columns(2)
        with col1:
            mag_min = st.selectbox("Minimum magnitude", options=[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10], index=0)
        with col2:
            mag_max = st.selectbox("Maximum magnitude", options=[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10], index=10)
        
        # Pre-defined location list
        preset_locations = {
            "(none)": None,
            "Dallas, TX": (32.8, -96.8),
            "Arlington, TX": (32.7357, -97.1081),
            "Houston, TX": (29.7604, -95.3698),
            "San Antonio, TX": (29.4241, -98.4936),
            "Los Angeles, CA": (34.0522, -118.2437),
            "San Diego, CA": (32.7157, -117.1611),
            "San Francisco, CA": (37.7749, -122.4194),
            "New York, NY": (40.7128, -74.0060),
            "Chicago, IL": (41.8781, -87.6298),
            "Philadelphia, PA": (39.9526, -75.1652),
            "Phoenix, AZ": (33.4484, -112.0740),
            "Anchorage, AK": (61.0, -150.0),
        }
        
        col1, col2 = st.columns(2)
        with col1:
            selected_location = st.selectbox("Location", options=list(preset_locations.keys()), index=0)
        with col2:
            radius_km = st.number_input("Within (km)", min_value=1, max_value=10000, value=100, step=10)
        
        # Get coordinates for selected location
        latitude = longitude = None
        if selected_location != "(none)":
            latitude, longitude = preset_locations[selected_location]
        
        # Submit
        if st.form_submit_button("Run Query", type="primary"):
            st.session_state.query_params = {
                'time_min': time_min, 'time_max': time_max,
                'mag_min': mag_min, 'mag_max': mag_max,
                'latitude': latitude, 'longitude': longitude, 'radius_km': radius_km,
                'selected_location': selected_location
            }
            st.session_state.show_results = True
            st.rerun()
else:
    # STEP 2: RESULTS
    params = st.session_state.query_params
    
    # Query options on results page
    st.subheader("Query Options")
    col1, col2, col3 = st.columns(3)
    with col1:
        sort_by = st.selectbox("Sort by", ["time", "mag", "depth"], index=1)
    with col2:
        sort_order = st.selectbox("Order", ["DESC", "ASC"])
    with col3:
        max_rows = st.number_input("Max rows", 10, 10000, 1000)
    
    def distance_km_sql(lat_col: str = "latitude", lon_col: str = "longitude", lat: float = 0.0, lon: float = 0.0) -> str:
        return (
            "2 * asin(sqrt("
            f"pow(sin((radians({lat}) - radians({lat_col})) / 2), 2) + "
            f"cos(radians({lat_col})) * cos(radians({lat})) * pow(sin((radians({lon}) - radians({lon_col})) / 2), 2)"
            ")) * 6371"
        )
    
    def build_where_from_params():
        conds = []
        
        # Time filter
        if params.get('time_min') and params.get('time_max'):
            s = pd.to_datetime(params['time_min']).strftime('%Y-%m-%d')
            e = pd.to_datetime(params['time_max']).strftime('%Y-%m-%d')
            conds.append(f"time >= to_timestamp('{s}') AND time < to_timestamp('{e}') + INTERVAL 1 DAY")
        
        # Magnitude filter (range)
        mag_min = params.get('mag_min', 0)
        mag_max = params.get('mag_max', 10)
        if mag_min > 0 or mag_max < 10:
            conds.append(f"mag >= {mag_min} AND mag <= {mag_max}")
        
        # Location radius filter
        if (params.get('latitude') is not None and params.get('longitude') is not None and params.get('radius_km')):
            lat = params['latitude']
            lon = params['longitude']
            radius = params['radius_km']
            dist_expr = distance_km_sql(lat=f"{lat:.6f}", lon=f"{lon:.6f}")
            conds.append(f"{dist_expr} <= {radius}")
        
        return (" WHERE " + " AND ".join(conds)) if conds else ""
    
    # Build query
    where_clause = build_where_from_params()
    q = f"SELECT * FROM {table_fqn}{where_clause} ORDER BY {sort_by} {sort_order} NULLS LAST LIMIT {max_rows}"
    
    # Execute query and show results
    try:
        filtered_df = run_sql(q)
        
        # Summary metrics
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Records", f"{len(filtered_df):,}")
        with col2:
            if "mag" in filtered_df.columns and len(filtered_df) > 0:
                st.metric("Avg Magnitude", f"{filtered_df['mag'].mean():.2f}")
        with col3:
            if "mag" in filtered_df.columns and len(filtered_df) > 0:
                st.metric("Max Magnitude", f"{filtered_df['mag'].max():.2f}")
        with col4:
            if "depth" in filtered_df.columns and len(filtered_df) > 0:
                st.metric("Avg Depth (km)", f"{filtered_df['depth'].mean():.1f}")
        
        # Show query details
        with st.expander("Query Details"):
            st.code(q)
        
        # Data table
        st.dataframe(filtered_df, use_container_width=True, height=600)
        
        # Download
        if len(filtered_df) > 0:
            csv_data = filtered_df.to_csv(index=False).encode('utf-8')
            st.download_button(
                "Download CSV",
                data=csv_data,
                file_name="earthquake_results.csv",
                mime="text/csv"
            )
    except Exception as e:
        st.error(f"Query failed: {e}")
        st.code(f"Attempted query: {q}")

