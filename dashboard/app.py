"""
    Olist Ecommerce Dashboard]
"""


import pandas_gbq as pd_gbq
import plotly.graph_objects as go
import streamlit as st
from google.oauth2.service_account import Credentials

tables = [
    "olist_data.fct_avg_delivery_time",
    "olist_data.fct_orders_by_state",
    "olist_data.fct_sales_by_catgory",
]
columns = ["avg_delivery_time_model", "total_orders", "total_sales"]
titles = ["Average Delivery Time", "Orders by State", "Sales by Category"]
extra_info = ["Days", "customer_state", "product_category_name"]
st.title("Olist Ecommerce Dashboard")
# Create API client.
credentials = Credentials.from_service_account_info(st.secrets["gcp_service_account"])


@st.cache_data(ttl=600)
def run_query(query):
    return pd_gbq.read_gbq(query, credentials=credentials)


cols = st.columns(len(tables), gap="large", vertical_alignment="bottom")
for idx, (table, column, title, info) in enumerate(
    zip(tables, columns, titles, extra_info)
):
    query = f"SELECT * FROM {table}"
    df = run_query(query)
    output = df.to_dict(orient="records")
    print(output)
    fig = go.Figure(
        go.Indicator(
            mode="number",
            align="center",
            value=output[0].get(column),
            title={"text": f"{title} ({output[0].get(info, info)})"},
        )
    )
    print(fig.__dict__)
    cols[idx].plotly_chart(fig, use_container_width=False)
