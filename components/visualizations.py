import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt

def avg_magnitude_per_net(df: pd.DataFrame, title: str = "Average Magnitude per Net"):
    """Render a bar chart showing average magnitude per net category."""
    if "net" not in df.columns or "mag" not in df.columns or len(df) == 0:
        st.warning("Required columns 'net' or 'mag' not found or no data available.")
        return
    # Ensure mag is numeric
    df = df.copy()
    df["mag"] = pd.to_numeric(df["mag"], errors="coerce")
    agg_df = df.groupby("net")["mag"].mean().sort_values(ascending=False)
    st.subheader(title)
    st.bar_chart(agg_df, use_container_width=True)

def avg_magnitude_per_net_pie(df: pd.DataFrame, title: str = "Average Magnitude per Net (Pie Chart)"):
    """Render a pie chart showing average magnitude per net category using matplotlib."""
    if "net" not in df.columns or "mag" not in df.columns or len(df) == 0:
        st.warning("Required columns 'net' or 'mag' not found or no data available.")
        return
    df = df.copy()
    df["mag"] = pd.to_numeric(df["mag"], errors="coerce")
    agg_df = df.groupby("net")["mag"].mean()
    st.subheader(title)
    fig, ax = plt.subplots()
    ax.pie(agg_df, labels=agg_df.index, autopct='%1.1f%%', startangle=90)
    ax.axis('equal')
    st.pyplot(fig)

def avg_magnitude_per_net_scatter(df: pd.DataFrame, title: str = "Average Magnitude per Net (Scatter Plot)"):
    """Render a scatter plot showing average magnitude per net category."""
    if "net" not in df.columns or "mag" not in df.columns or len(df) == 0:
        st.warning("Required columns 'net' or 'mag' not found or no data available.")
        return
    df = df.copy()
    df["mag"] = pd.to_numeric(df["mag"], errors="coerce")
    agg_df = df.groupby("net")["mag"].mean().reset_index()
    st.subheader(title)
    st.scatter_chart(agg_df, x="net", y="mag", use_container_width=True)

def food_amount_bar_chart(df: pd.DataFrame, title: str = "Amount by Food"):
    """Render a bar chart showing amount per food item."""
    if "Food" not in df.columns or "Amount" not in df.columns or len(df) == 0:
        st.warning("Required columns 'Food' or 'Amount' not found or no data available.")
        return
    df = df.copy()
    df["Amount"] = pd.to_numeric(df["Amount"], errors="coerce")
    agg_df = df.groupby("Food")["Amount"].sum().sort_values(ascending=False)
    st.subheader(title)
    st.bar_chart(agg_df, use_container_width=True)

def food_amount_pie_chart(df: pd.DataFrame, title: str = "Amount by Food (Pie Chart)"):
    """Render a pie chart showing amount distribution by food item."""
    if "Food" not in df.columns or "Amount" not in df.columns or len(df) == 0:
        st.warning("Required columns 'Food' or 'Amount' not found or no data available.")
        return
    df = df.copy()
    df["Amount"] = pd.to_numeric(df["Amount"], errors="coerce")
    agg_df = df.groupby("Food")["Amount"].sum()
    st.subheader(title)
    fig, ax = plt.subplots()
    ax.pie(agg_df, labels=agg_df.index, autopct='%1.1f%%', startangle=90)
    ax.axis('equal')
    st.pyplot(fig)

def coordinate_scatter_plot(coordinates: list, title: str = "Coordinate Scatter Plot"):
    """Render a scatter plot from coordinate pairs with custom colors."""
    if not coordinates or len(coordinates) == 0:
        st.warning("No coordinates provided.")
        return
    
    st.subheader(title)
    fig, ax = plt.subplots()
    
    for coord in coordinates:
        x = coord.get("x", 0)
        y = coord.get("y", 0)
        color = coord.get("color", "#1f77b4")
        ax.scatter(x, y, s=100, color=color, edgecolors='black', linewidth=1)
    
    ax.set_xlabel("X")
    ax.set_ylabel("Y")
    ax.grid(True, alpha=0.3)
    st.pyplot(fig)


