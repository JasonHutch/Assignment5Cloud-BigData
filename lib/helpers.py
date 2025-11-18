import pandas as pd

def build_where_from_params(params):
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

def distance_km_sql(lat_col: str = "latitude", lon_col: str = "longitude", lat: float = 0.0, lon: float = 0.0) -> str:
    return (
        "2 * asin(sqrt("
        f"pow(sin((radians({lat}) - radians({lat_col})) / 2), 2) + "
        f"cos(radians({lat_col})) * cos(radians({lat})) * pow(sin((radians({lon}) - radians({lon_col})) / 2), 2)"
        ")) * 6371"
    )
    
    
def build_food_query(params):
    conds = []
    
    min, max = params.get("amt_range")
    
    if params.get("amt_range"):
        conds.append(f"Amount >= {min} AND Amount <= {max}")
        
    return (" WHERE " + " AND ".join(conds)) if conds else ""
    
    