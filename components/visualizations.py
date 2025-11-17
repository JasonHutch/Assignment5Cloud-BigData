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