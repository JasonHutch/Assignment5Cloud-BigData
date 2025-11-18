# streamlit_app.py
import os
import io
import socket
import time
import pandas as pd
import streamlit as st
import requests

from dotenv import load_dotenv
from components.data_form import FoodDataForm
from lib.helpers import build_food_query
from lib.databricks_sql import run_food_sql
from components.visualizations import food_amount_bar_chart, food_amount_pie_chart
load_dotenv()
DATABRICKS_SERVER_HOSTNAME = os.getenv("DATABRICKS_SERVER_HOSTNAME")
DATABRICKS_HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH")
DATABRICKS_PERSONAL_ACCESS_TOKEN = os.getenv("DATABRICKS_PERSONAL_ACCESS_TOKEN")
DATABRICKS_HOST_URL = os.getenv("DATABRICKS_HOST_URL") or (f"https://{DATABRICKS_SERVER_HOSTNAME}" if DATABRICKS_SERVER_HOSTNAME else None)
DATABRICKS_WAREHOUSE_ID = os.getenv("DATABRICKS_WAREHOUSE_ID")
TABLE_NAME = "workspace.default.sample_food"
limit = 100
df = None
source_label = None

# ---- App Config ----
st.set_page_config(page_title="Sample Food Data Explorer", layout="wide")
st.title("Food Data Explorer")
st.caption("Explore and analyze different food.")

# Auto-load data on app start
try:
    df = run_food_sql(f"SELECT * FROM {TABLE_NAME} LIMIT {int(limit)}")
except Exception as e:
    st.error(f"Failed to query food data. {e}")
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
    st.header("Build Your Food Query")

if not st.session_state.show_results:
    # ---- Render form ----
    FoodDataForm(df)
else:
    # ---- Render Results ----
    params = st.session_state.query_params
    
    # Build query
    where_clause = build_food_query(params)
    q = f"SELECT * FROM {TABLE_NAME}{where_clause}"
    
    # Execute query and show results
    try:
        filtered_df = run_food_sql(q)
        
        # Show query details
        with st.expander("Query Details"):
            st.code(q)
        
        # # # Visualizations
        st.subheader("Visualizations")
        viz_col1, viz_col2 = st.columns(2)
        
        with viz_col1:
            food_amount_bar_chart(filtered_df)
        with viz_col2:
            food_amount_pie_chart(filtered_df)
        # with viz_col3:
        #     avg_magnitude_per_net_scatter(filtered_df)
        
        # Data table
        st.dataframe(filtered_df, use_container_width=True, height=600)
        
    except Exception as e:
        st.error(f"Query failed: {e}")
        st.code(f"Attempted query: {q}")

