import streamlit as st
import pandas as pd

def EarthquakeDataForm(df):
    with st.form("earthquake_query_form"):
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



def FoodDataForm(df):
    with st.form("food_query_form"):
        # Form fields in a clean layout
        col1,= st.columns(1)
        with col1:
            amt_range = st.slider(
                "Select a price range",
                min_value=0,
                max_value=50,
                value=(10,25)
            )
        
        # Submit
        if st.form_submit_button("Run Food Query", type="primary"):
            st.session_state.query_params = {
                'amt_range':amt_range
            }
            st.session_state.show_results = True
            st.rerun()
    