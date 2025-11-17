# streamlit_app.py
import os
import io
import socket
import time
import pandas as pd
import streamlit as st
import requests

from dotenv import load_dotenv
from components.data_form import DataForm
from lib.helpers import build_where_from_params, distance_km_sql
from lib.databricks_sql import run_sql
from components.visualizations import avg_magnitude_per_net,avg_magnitude_per_net_pie,avg_magnitude_per_net_scatter

load_dotenv()
DATABRICKS_SERVER_HOSTNAME = os.getenv("DATABRICKS_SERVER_HOSTNAME")
DATABRICKS_HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH")
DATABRICKS_PERSONAL_ACCESS_TOKEN = os.getenv("DATABRICKS_PERSONAL_ACCESS_TOKEN")
DATABRICKS_HOST_URL = os.getenv("DATABRICKS_HOST_URL") or (f"https://{DATABRICKS_SERVER_HOSTNAME}" if DATABRICKS_SERVER_HOSTNAME else None)
DATABRICKS_WAREHOUSE_ID = os.getenv("DATABRICKS_WAREHOUSE_ID")
TABLE_NAME = "workspace.default.earthquakes"
limit = 100
df = None
source_label = None

# ---- App Config ----
st.set_page_config(page_title="Earthquake Data Explorer", layout="wide")
st.title("Earthquake Data Explorer")
st.caption("Explore and analyze earthquake data from the Databricks earthquake_data table.")

# Auto-load data on app start
try:
    df = run_sql(f"SELECT * FROM {TABLE_NAME} LIMIT {int(limit)}")
except Exception as e:
    st.error(f"Failed to query earthquake data. {e}")
    st.stop()

# ---- Init session state ----
if 'show_results' not in st.session_state:
    st.session_state.show_results = False
if 'query_params' not in st.session_state:
    st.session_state.query_params = {}

# ---- Init navigation ----
if st.session_state.show_results:
    if st.button("← Back to Query Form"):
        st.session_state.show_results = False
        st.rerun()
    st.header("Query Results")
else:
    st.header("Build Your Earthquake Data Query")

if not st.session_state.show_results:
    # ---- Render form ----
    DataForm(df)
else:
    # ---- Render Results ----
    params = st.session_state.query_params
    
    # Query options on results page
    col1, col2, col3 = st.columns(3)
    with col1:
        sort_by = st.selectbox("Sort by", ["time", "mag", "depth"], index=1)
    with col2:
        sort_order = st.selectbox("Order", ["DESC", "ASC"])
    with col3:
        max_rows = st.number_input("Max rows", 10, 10000, 1000)
    
    # Build query
    where_clause = build_where_from_params(params)
    q = f"SELECT * FROM {TABLE_NAME}{where_clause} ORDER BY {sort_by} {sort_order} NULLS LAST LIMIT {max_rows}"
    
    # Execute query and show results
    try:
        filtered_df = run_sql(q)
        
        # Show query details
        with st.expander("Query Details"):
            st.code(q)
        
        # # Visualizations
        st.subheader("Visualizations")
        viz_col1, viz_col2, viz_col3 = st.columns(3)
        
        with viz_col1:
            avg_magnitude_per_net(filtered_df)
        with viz_col2:
            avg_magnitude_per_net_pie(filtered_df)
        with viz_col3:
            avg_magnitude_per_net_scatter(filtered_df)
        
        # Data table
        st.dataframe(filtered_df, use_container_width=True, height=600)
        
    except Exception as e:
        st.error(f"Query failed: {e}")
        st.code(f"Attempted query: {q}")

