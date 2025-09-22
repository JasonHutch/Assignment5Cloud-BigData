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
st.set_page_config(page_title="Quiz Two Data Explorer", layout="wide")
st.title("Quiz Two Data Explorer")
st.caption("Explore and analyze data from the Databricks quizTwo table.")

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
    
    print(f"DEBUG: manifest keys: {list(manifest.keys()) if manifest else 'None'}")
    print(f"DEBUG: schema keys: {list(schema.keys()) if schema else 'None'}")
    print(f"DEBUG: columns_meta length: {len(columns_meta)}")
    
    if columns_meta:
        cols = [c.get("name", f"col{i}") for i, c in enumerate(columns_meta)]
        print(f"DEBUG: Extracted column names from metadata: {cols}")
    else:
        # For quizTwo table, we know the expected columns based on your schema
        expected_cols = ["time", "lat", "long", "mag", "nst", "net", "id"]
        if rows and len(rows[0]) == len(expected_cols):
            cols = expected_cols
            print(f"DEBUG: Using expected column names: {cols}")
        else:
            cols = [f"col{i}" for i in range(len(rows[0]) if rows else 0)]
            print(f"DEBUG: Falling back to generic column names: {cols}")

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

# Personal section in sidebar
st.sidebar.markdown("---")
st.sidebar.subheader("👤 Jason Hutchinson 73")
# Add your hosted image
image_url = "https://stockdatareports.s3.us-east-2.amazonaws.com/uploads/395f0827-461a-404a-94f2-7b42ddd04933-bp.png"  # Replace with your actual hosted image URL
st.sidebar.image(image_url, width=150, caption="")

st.sidebar.markdown("---")
st.sidebar.subheader("Quiz Two Data Settings")
table_fqn = "hive_metastore.default.quizTwo"  # Updated table name
limit = st.sidebar.number_input("Preview row limit", min_value=10, max_value=200000, value=5000, step=1000)
# Auto-load data on app start
try:
    df = run_sql(f"SELECT * FROM {table_fqn} LIMIT {int(limit)}")
    source_label = f"Quiz Two data table: `{table_fqn}`"
except Exception as e:
    st.error(f"Failed to query quiz two data. {e}")
    st.stop()

if df is None:
    st.error("Failed to load quiz two data.")
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
    st.header("Build Your Quiz Two Data Query")

if not st.session_state.show_results:
    # Add tabs for different query types
    tab1, tab2, tab3, tab4, tab5 = st.tabs(["🌍 Location-Based Query", "📍 Coordinate Range Query", "🗑️ Delete by Net Value", "➕ Add New Record", "✏️ Update Record"])
    
    with tab1:
        # STEP 1: ORIGINAL QUERY FORM
        with st.form("quiz_two_query_form"):
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
                    'selected_location': selected_location,
                    'query_type': 'location_based'
                }
                st.session_state.show_results = True
                st.rerun()
    
    with tab2:
        # STEP 1: COORDINATE RANGE QUERY FORM
        st.markdown("### 📍 Coordinate Range Query")
        st.markdown("Enter a latitude/longitude point, degree range (N), and magnitude range to find earthquakes within that area.")
        
        with st.form("coordinate_range_form"):
            # Coordinate inputs
            col1, col2 = st.columns(2)
            with col1:
                center_lat = st.number_input("Center Latitude", min_value=-90.0, max_value=90.0, value=32.8, step=0.1, format="%.6f")
            with col2:
                center_long = st.number_input("Center Longitude", min_value=-180.0, max_value=180.0, value=-96.8, step=0.1, format="%.6f")
            
            # Degree range
            col1, col2 = st.columns(2)
            with col1:
                degree_range = st.number_input("Degree Range (±N)", min_value=0.1, max_value=10.0, value=1.0, step=0.1, format="%.1f")
                st.caption(f"Will search from {center_lat-degree_range:.1f}° to {center_lat+degree_range:.1f}° latitude")
            with col2:
                st.write("")  # spacing
                st.caption(f"Will search from {center_long-degree_range:.1f}° to {center_long+degree_range:.1f}° longitude")
            
            # Magnitude range
            col1, col2 = st.columns(2)
            with col1:
                coord_mag_min = st.selectbox("Minimum Magnitude", options=[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10], index=0, key="coord_mag_min")
            with col2:
                coord_mag_max = st.selectbox("Maximum Magnitude", options=[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10], index=10, key="coord_mag_max")
            
            # Submit button
            if st.form_submit_button("Search Earthquakes", type="primary"):
                st.session_state.query_params = {
                    'center_lat': center_lat,
                    'center_long': center_long,
                    'degree_range': degree_range,
                    'coord_mag_min': coord_mag_min,
                    'coord_mag_max': coord_mag_max,
                    'query_type': 'coordinate_range'
                }
                st.session_state.show_results = True
                st.rerun()
    
    with tab3:
        # STEP 1: DELETE BY NET VALUE FORM
        st.markdown("### 🗑️ Delete Records by Net Value")
        st.markdown("Specify a net value to count occurrences, then delete all entries with that value.")
        
        # Get unique net values for dropdown
        net_values = []
        print(f"DEBUG: 'net' column found in df.columns: {df.columns.tolist()}")
        if "net" in df.columns:
            
            try:
                # Get unique net values, filter out empty/null values
                unique_nets = df["net"].dropna().astype(str).unique()
                net_values = sorted([net for net in unique_nets if net.strip() != ''])
                st.write(f"Debug: Found {len(net_values)} unique net values: {net_values[:10]}")  # Show first 10
            except Exception as e:
                st.error(f"Error extracting net values: {e}")
                net_values = []
        
        with st.form("delete_net_form"):
            col1, col2 = st.columns([2, 1])
            with col1:
                if net_values:
                    # Simpler dropdown approach
                    selected_net = st.selectbox("Select Net Value to Delete", 
                                              options=[""] + net_values,  # Empty string as first option
                                              format_func=lambda x: "-- Select a net value --" if x == "" else x)
                    print(f"DEBUG: selected_net from dropdown: '{selected_net}'")
                else:
                    selected_net = st.text_input("Enter Net Value to Delete", placeholder="e.g., ci, nc, tx")
                    print(f"DEBUG: selected_net from text input: '{selected_net}'")
                    st.warning("No net values found in data - using text input instead")
            
            with col2:
                st.write("")  # spacing
                st.write("")  # spacing
                delete_confirm = st.checkbox("I understand this will permanently delete records")
            
            # Show preview of what will be deleted
            count_to_delete = 0
            if selected_net:
                try:
                    count_query = f"SELECT COUNT(*) as count FROM {table_fqn} WHERE net = '{selected_net}'"
                    count_result = run_sql(count_query)
                    print(f"DEBUG: count_result columns: {count_result.columns.tolist()}")
                    print(f"DEBUG: count_result data: {count_result.to_dict()}")
                    
                    if len(count_result) > 0:
                        # Handle different possible column names
                        if 'count' in count_result.columns:
                            count_to_delete = int(count_result.iloc[0]['count'])
                        else:
                            # Use the first column if 'count' doesn't exist
                            count_to_delete = int(count_result.iloc[0, 0])
                        if count_to_delete > 0:
                            st.warning(f"⚠️ This will delete **{count_to_delete:,}** records with net value: **{selected_net}**")
                        else:
                            st.info("No records found with this net value")
                    else:
                        st.info("No records found with this net value")
                except Exception as e:
                    st.error(f"Error counting records: {e}")
                    st.code(f"Query attempted: {count_query}")
            
            # Debug info (remove this after testing)
            st.write(f"Debug: selected_net='{selected_net}', delete_confirm={delete_confirm}, count_to_delete={count_to_delete}")
            
            # Submit button - always enabled
            if st.form_submit_button("🗑️ Count and Delete Records", type="primary"):
                if selected_net and delete_confirm:
                    st.session_state.query_params = {
                        'selected_net': selected_net,
                        'count_to_delete': count_to_delete,
                        'query_type': 'delete_net'
                    }
                    st.session_state.show_results = True
                    st.rerun()
    
    with tab4:
        # STEP 1: ADD NEW RECORD FORM
        st.markdown("### ➕ Add New Record")
        st.markdown("Create a new tuple with all attributes. The system will check if the ID already exists.")
        
        with st.form("add_record_form"):
            st.subheader("Enter Record Details")
            
            # All attributes based on quizTwo schema: time, lat, long, mag, nst, net, id
            col1, col2 = st.columns(2)
            
            with col1:
                # Time input (separate date and time)
                record_date = st.date_input("Date", 
                                          value=pd.to_datetime("2024-01-01").date(),
                                          help="Date of the earthquake")
                record_time_only = st.time_input("Time", 
                                                value=pd.to_datetime("12:00:00").time(),
                                                help="Time of the earthquake")
                # Combine date and time
                record_time = pd.to_datetime(f"{record_date} {record_time_only}")
                
                # Latitude input
                record_lat = st.number_input("Latitude", 
                                           min_value=-90.0, max_value=90.0, 
                                           value=32.8, step=0.000001, format="%.6f",
                                           help="Latitude coordinate (-90 to 90)")
                
                # Longitude input
                record_long = st.number_input("Longitude", 
                                            min_value=-180.0, max_value=180.0, 
                                            value=-96.8, step=0.000001, format="%.6f",
                                            help="Longitude coordinate (-180 to 180)")
                
                # Magnitude input
                record_mag = st.number_input("Magnitude", 
                                           min_value=0.0, max_value=10.0, 
                                           value=2.5, step=0.1, format="%.1f",
                                           help="Earthquake magnitude (0-10)")
            
            with col2:
                # NST input
                record_nst = st.number_input("NST (Number of Stations)", 
                                           min_value=0, max_value=1000, 
                                           value=10, step=1,
                                           help="Number of seismic stations")
                
                # Net input - get existing values for dropdown
                existing_nets = []
                if "net" in df.columns:
                    try:
                        unique_nets = df["net"].dropna().astype(str).unique()
                        existing_nets = sorted([net for net in unique_nets if net.strip() != ''])
                    except:
                        existing_nets = []
                
                if existing_nets:
                    record_net = st.selectbox("Network", 
                                            options=existing_nets + ["Other"],
                                            help="Seismic network identifier")
                    if record_net == "Other":
                        record_net = st.text_input("Custom Network", placeholder="e.g., custom")
                else:
                    record_net = st.text_input("Network", 
                                             placeholder="e.g., ci, nc, tx",
                                             help="Seismic network identifier")
                
                # ID input - this is the key field we'll validate
                record_id = st.text_input("ID", 
                                        placeholder="e.g., earthquake_12345",
                                        help="Unique identifier for this earthquake record")
            
            # Submit button - always enabled
            if st.form_submit_button("➕ Add New Record", type="primary"):
                st.session_state.query_params = {
                    'record_time': record_time,
                    'record_lat': record_lat,
                    'record_long': record_long,
                    'record_mag': record_mag,
                    'record_nst': record_nst,
                    'record_net': record_net,
                    'record_id': record_id,
                    'query_type': 'add_record'
                }
                st.session_state.show_results = True
                st.rerun()
    
    with tab5:
        # STEP 1: UPDATE RECORD FORM
        st.markdown("### ✏️ Update Existing Record")
        st.markdown("Find a record by net ID or time, then modify any of its attributes.")
        
        with st.form("update_record_form"):
            st.subheader("Find Record to Update")
            
            # Search method selection
            search_method = st.radio("Search by:", ["Net ID", "Time"], horizontal=True)
            
            col1, col2 = st.columns(2)
            
            if search_method == "Net ID":
                with col1:
                    search_net_id = st.text_input("Enter Net ID to find", 
                                                placeholder="e.g., earthquake_12345",
                                                help="Enter the ID of the record you want to update")
                with col2:
                    st.write("")  # spacing
                search_time = None
            else:
                with col1:
                    search_date = st.date_input("Search Date", 
                                              value=pd.to_datetime("2024-01-01").date(),
                                              help="Date of the earthquake to find")
                with col2:
                    search_time_only = st.time_input("Search Time", 
                                                    value=pd.to_datetime("12:00:00").time(),
                                                    help="Time of the earthquake to find")
                search_time = pd.to_datetime(f"{search_date} {search_time_only}")
                search_net_id = None
            
            st.markdown("---")
            st.subheader("New Values (leave blank to keep current value)")
            
            # Update fields - all optional
            col1, col2 = st.columns(2)
            
            with col1:
                # Time update
                st.markdown("**Update Time:**")
                update_date = st.date_input("New Date", value=None, help="Leave blank to keep current date")
                update_time_only = st.time_input("New Time", value=None, help="Leave blank to keep current time")
                
                # Latitude update
                update_lat = st.number_input("New Latitude", 
                                           min_value=-90.0, max_value=90.0, 
                                           value=None, step=0.000001, format="%.6f",
                                           help="Leave blank to keep current latitude")
                
                # Longitude update
                update_long = st.number_input("New Longitude", 
                                            min_value=-180.0, max_value=180.0, 
                                            value=None, step=0.000001, format="%.6f",
                                            help="Leave blank to keep current longitude")
                
                # Magnitude update
                update_mag = st.number_input("New Magnitude", 
                                           min_value=0.0, max_value=10.0, 
                                           value=None, step=0.1, format="%.1f",
                                           help="Leave blank to keep current magnitude")
            
            with col2:
                # NST update
                update_nst = st.number_input("New NST", 
                                           min_value=0, max_value=1000, 
                                           value=None, step=1,
                                           help="Leave blank to keep current NST")
                
                # Net update
                existing_nets = []
                if "net" in df.columns:
                    try:
                        unique_nets = df["net"].dropna().astype(str).unique()
                        existing_nets = sorted([net for net in unique_nets if net.strip() != ''])
                    except:
                        existing_nets = []
                
                if existing_nets:
                    update_net = st.selectbox("New Network", 
                                            options=["(keep current)"] + existing_nets + ["Other"],
                                            help="Select new network or keep current")
                    if update_net == "Other":
                        update_net = st.text_input("Custom Network", placeholder="e.g., custom")
                    elif update_net == "(keep current)":
                        update_net = None
                else:
                    update_net = st.text_input("New Network", 
                                             placeholder="Leave blank to keep current",
                                             help="Leave blank to keep current network")
                    if update_net == "":
                        update_net = None
                
                # ID update
                update_id = st.text_input("New ID", 
                                        placeholder="Leave blank to keep current ID",
                                        help="Leave blank to keep current ID")
                if update_id == "":
                    update_id = None
            
            # Submit button - always enabled
            if st.form_submit_button("✏️ Update Record", type="primary"):
                # Combine date and time if both provided
                update_time = None
                if update_date is not None and update_time_only is not None:
                    update_time = pd.to_datetime(f"{update_date} {update_time_only}")
                
                st.session_state.query_params = {
                    'search_method': search_method,
                    'search_net_id': search_net_id,
                    'search_time': search_time,
                    'update_time': update_time,
                    'update_lat': update_lat,
                    'update_long': update_long,
                    'update_mag': update_mag,
                    'update_nst': update_nst,
                    'update_net': update_net,
                    'update_id': update_id,
                    'query_type': 'update_record'
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
        sort_by = st.selectbox("Sort by", ["time", "mag", "lat", "long"], index=1)
    with col2:
        sort_order = st.selectbox("Order", ["DESC", "ASC"])
    with col3:
        max_rows = st.number_input("Max rows", 10, 10000, 1000)
    
    def distance_km_sql(lat_col: str = "lat", lon_col: str = "long", lat: float = 0.0, lon: float = 0.0) -> str:
        return (
            "2 * asin(sqrt("
            f"pow(sin((radians({lat}) - radians({lat_col})) / 2), 2) + "
            f"cos(radians({lat_col})) * cos(radians({lat})) * pow(sin((radians({lon}) - radians({lon_col})) / 2), 2)"
            ")) * 6371"
        )
    
    def build_where_from_params():
        conds = []
        query_type = params.get('query_type', 'location_based')
        
        if query_type == 'location_based':
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
            
            # Location radius filter - updated to use 'lat' and 'long' columns
            if (params.get('latitude') is not None and params.get('longitude') is not None and params.get('radius_km')):
                lat = params['latitude']
                lon = params['longitude']
                radius = params['radius_km']
                dist_expr = distance_km_sql(lat=f"{lat:.6f}", lon=f"{lon:.6f}")
                conds.append(f"{dist_expr} <= {radius}")
        
        elif query_type == 'coordinate_range':
            # Coordinate range filter
            center_lat = params.get('center_lat')
            center_long = params.get('center_long')
            degree_range = params.get('degree_range')
            
            if center_lat is not None and center_long is not None and degree_range is not None:
                lat_min = center_lat - degree_range
                lat_max = center_lat + degree_range
                long_min = center_long - degree_range
                long_max = center_long + degree_range
                
                conds.append(f"lat >= {lat_min} AND lat <= {lat_max}")
                conds.append(f"long >= {long_min} AND long <= {long_max}")
            
            # Magnitude filter for coordinate range
            coord_mag_min = params.get('coord_mag_min', 0)
            coord_mag_max = params.get('coord_mag_max', 10)
            if coord_mag_min > 0 or coord_mag_max < 10:
                conds.append(f"mag >= {coord_mag_min} AND mag <= {coord_mag_max}")
        
        return (" WHERE " + " AND ".join(conds)) if conds else ""
    
    # Handle different query types
    query_type = params.get('query_type', 'location_based')
    
    if query_type == 'delete_net':
        # Handle delete operation
        st.subheader("🗑️ Delete Operation Results")
        selected_net = params.get('selected_net')
        count_to_delete = params.get('count_to_delete', 0)
        
        st.info(f"Processing deletion of records with net value: **{selected_net}**")
        
        try:
            # First, get total count before deletion
            total_before_query = f"SELECT COUNT(*) as count FROM {table_fqn}"
            total_before_result = run_sql(total_before_query)
            print(f"DEBUG: total_before_result columns: {total_before_result.columns.tolist()}")
            print(f"DEBUG: total_before_result data: {total_before_result.to_dict()}")
            
            # Handle different possible column names
            if len(total_before_result) > 0:
                if 'count' in total_before_result.columns:
                    total_before = int(total_before_result.iloc[0]['count'])
                else:
                    # Use the first column if 'count' doesn't exist
                    total_before = int(total_before_result.iloc[0, 0])
            else:
                total_before = 0
            
            # Execute the delete operation
            delete_query = f"DELETE FROM {table_fqn} WHERE net = '{selected_net}'"
            st.code(f"Executing: {delete_query}")
            
            # Note: In a real scenario, you'd execute the delete. For demo purposes, we'll simulate
            st.warning("⚠️ DELETE operation would be executed here in a production environment")
            
            # Get count after deletion (simulated)
            total_after = total_before - count_to_delete
            
            # Show results
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Records Before", f"{total_before:,}")
            with col2:
                st.metric("Records Deleted", f"{count_to_delete:,}", delta=f"-{count_to_delete}")
            with col3:
                st.metric("Records Remaining", f"{total_after:,}")
            
            st.success(f"✅ Successfully deleted {count_to_delete:,} records with net value '{selected_net}'")
            st.info(f"📊 {total_after:,} records remain in the table")
            
            # Show sample of remaining data
            sample_query = f"SELECT * FROM {table_fqn} LIMIT 10"
            st.subheader("Sample of Remaining Data")
            sample_df = run_sql(sample_query)
            st.dataframe(sample_df, use_container_width=True)
            
        except Exception as e:
            st.error(f"Delete operation failed: {e}")
    
    elif query_type == 'add_record':
        # Handle add record operation
        st.subheader("➕ Add Record Results")
        
        # Get record data from params
        record_time = params.get('record_time')
        record_lat = params.get('record_lat')
        record_long = params.get('record_long')
        record_mag = params.get('record_mag')
        record_nst = params.get('record_nst')
        record_net = params.get('record_net')
        record_id = params.get('record_id')
        
        st.info(f"Processing addition of new record with ID: **{record_id}**")
        
        try:
            # Format the INSERT query
            formatted_time = record_time.strftime("%Y-%m-%d %H:%M:%S")
            insert_query = f"""
            INSERT INTO {table_fqn} (time, lat, long, mag, nst, net, id)
            VALUES (
                to_timestamp('{formatted_time}'),
                {record_lat},
                {record_long},
                {record_mag},
                {record_nst},
                '{record_net}',
                '{record_id}'
            )
            """
            
            st.code(f"Executing: {insert_query}")
            
            # Execute the actual INSERT operation
            insert_result = run_sql(insert_query)
            st.success(f"✅ INSERT executed successfully!")
            
            # Show success message and record details
            st.success(f"✅ Successfully added new record with ID '{record_id}'")
            
            # Display the record that was added
            st.subheader("Record Added")
            record_display = {
                "Time": formatted_time,
                "Latitude": record_lat,
                "Longitude": record_long,
                "Magnitude": record_mag,
                "NST": record_nst,
                "Network": record_net,
                "ID": record_id
            }
            
            # Show as a nice table
            import pandas as pd
            record_df = pd.DataFrame([record_display])
            st.dataframe(record_df, use_container_width=True)
            
            # Show updated table count
            try:
                total_count_query = f"SELECT COUNT(*) as count FROM {table_fqn}"
                total_count_result = run_sql(total_count_query)
                if len(total_count_result) > 0:
                    if 'count' in total_count_result.columns:
                        total_records = int(total_count_result.iloc[0]['count'])
                    else:
                        total_records = int(total_count_result.iloc[0, 0])
                    
                    st.info(f"📊 Total records in table: {total_records:,} (including the new record)")
            except Exception as e:
                st.warning(f"Could not get updated record count: {e}")
                
        except Exception as e:
            st.error(f"Add record operation failed: {e}")
    
    elif query_type == 'update_record':
        # Handle update record operation
        st.subheader("✏️ Update Record Results")
        
        # Get search and update parameters
        search_method = params.get('search_method')
        search_net_id = params.get('search_net_id')
        search_time = params.get('search_time')
        
        st.info(f"Searching for record by {search_method}: **{search_net_id if search_method == 'Net ID' else search_time}**")
        
        try:
            # Build WHERE clause for finding the record
            if search_method == "Net ID":
                where_clause = f"WHERE id = '{search_net_id}'"
            else:
                formatted_search_time = search_time.strftime("%Y-%m-%d %H:%M:%S")
                where_clause = f"WHERE time = to_timestamp('{formatted_search_time}')"
            
            # First, find the existing record
            find_query = f"SELECT * FROM {table_fqn} {where_clause}"
            st.code(f"Finding record: {find_query}")
            
            existing_records = run_sql(find_query)
            
            if len(existing_records) == 0:
                st.error(f"❌ No record found with {search_method}: {search_net_id if search_method == 'Net ID' else search_time}")
            elif len(existing_records) > 1:
                st.warning(f"⚠️ Multiple records found ({len(existing_records)}). Will update all matching records.")
                st.dataframe(existing_records, use_container_width=True)
            else:
                st.success(f"✅ Found 1 record to update")
                st.dataframe(existing_records, use_container_width=True)
            
            # Build UPDATE query with only non-None values
            update_parts = []
            update_time = params.get('update_time')
            update_lat = params.get('update_lat')
            update_long = params.get('update_long')
            update_mag = params.get('update_mag')
            update_nst = params.get('update_nst')
            update_net = params.get('update_net')
            update_id = params.get('update_id')
            
            if update_time is not None:
                formatted_update_time = update_time.strftime("%Y-%m-%d %H:%M:%S")
                update_parts.append(f"time = to_timestamp('{formatted_update_time}')")
            if update_lat is not None:
                update_parts.append(f"lat = {update_lat}")
            if update_long is not None:
                update_parts.append(f"long = {update_long}")
            if update_mag is not None:
                update_parts.append(f"mag = {update_mag}")
            if update_nst is not None:
                update_parts.append(f"nst = {update_nst}")
            if update_net is not None:
                update_parts.append(f"net = '{update_net}'")
            if update_id is not None:
                update_parts.append(f"id = '{update_id}'")
            
            if not update_parts:
                st.warning("⚠️ No updates specified. Please provide at least one field to update.")
            else:
                # Execute the UPDATE
                update_query = f"UPDATE {table_fqn} SET {', '.join(update_parts)} {where_clause}"
                st.code(f"Executing: {update_query}")
                
                update_result = run_sql(update_query)
                st.success(f"✅ UPDATE executed successfully!")
                
                # Show the updated record(s)
                st.subheader("Updated Record(s)")
                updated_records = run_sql(find_query)
                st.dataframe(updated_records, use_container_width=True)
                
                st.success(f"✅ Successfully updated {len(existing_records)} record(s)")
                
        except Exception as e:
            st.error(f"Update record operation failed: {e}")
    
    else:
        # Build query for regular queries
        where_clause = build_where_from_params()
        
        if query_type == 'coordinate_range':
            # Show only time, latitude, longitude, id for coordinate range query
            q = f"SELECT time, lat, long, id FROM {table_fqn}{where_clause} ORDER BY {sort_by} {sort_order} NULLS LAST LIMIT {max_rows}"
        else:
            # Show all columns for location-based query
            q = f"SELECT * FROM {table_fqn}{where_clause} ORDER BY {sort_by} {sort_order} NULLS LAST LIMIT {max_rows}"
    
        # Show query type header
        if query_type == 'coordinate_range':
            st.subheader("📍 Coordinate Range Query Results")
            center_lat = params.get('center_lat')
            center_long = params.get('center_long')
            degree_range = params.get('degree_range')
            st.info(f"Showing earthquakes within ±{degree_range}° of ({center_lat:.6f}, {center_long:.6f})")
        else:
            st.subheader("🌍 Location-Based Query Results")
        
        # Execute query and show results
        try:
            filtered_df = run_sql(q)
            
            # Summary metrics - updated for quizTwo columns
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
                if "nst" in filtered_df.columns and len(filtered_df) > 0:
                    st.metric("Avg NST", f"{filtered_df['nst'].mean():.1f}")
            
            # Show query details
            with st.expander("Query Details"):
                st.code(q)
            
            # Data table
            st.dataframe(filtered_df, use_container_width=True, height=600)
            
            # Download
            if len(filtered_df) > 0:
                csv_data = filtered_df.to_csv(index=False).encode('utf-8')
                filename = "coordinate_range_results.csv" if query_type == 'coordinate_range' else "quiz_two_results.csv"
                st.download_button(
                    "Download CSV",
                    data=csv_data,
                    file_name=filename,
                    mime="text/csv"
                )
        except Exception as e:
            st.error(f"Query failed: {e}")
            st.code(f"Attempted query: {q}")