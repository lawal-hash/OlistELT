import pandas_gbq as pd_gbq
import plotly.graph_objects as go
import streamlit as st
from google.oauth2.service_account import Credentials
from plotly.subplots import make_subplots

tables = [
    "olist_data.fct_avg_delivery_time",
    "olist_data.fct_orders_by_state",
    "olist_data.fct_sales_by_category",
]
columns = ["avg_delivery_time_model", "total_orders", "total_sales"]
titles = ["Average Delivery Time", "Orders by State", "Sales by Category"]
prefixes = ["", "", "R$"]
suffixes = ["", "QTY", ""]
extra_info = ["Days", "customer_state", "product_category"]
st.markdown(
    "<h1 style='text-align: center; color: black; font-family: Times New Roman;'>Olist Ecommerce Dashboard</h1>",
    unsafe_allow_html=True,
)
# Create API client.
credentials = Credentials.from_service_account_info(st.secrets["gcp_service_account"])


@st.cache_data(ttl=1)
def run_query(query):
    return pd_gbq.read_gbq(query, credentials=credentials)


# cols = st.columns(len(tables), gap="large", vertical_alignment="bottom")
fig = make_subplots(
    rows=1, cols=3, specs=[[{"type": "domain"}, {"type": "domain"}, {"type": "domain"}]]
)
for idx, (table, column, title, info,prefix, suffix) in enumerate(
    zip(tables, columns, titles, extra_info,prefixes, suffixes)
):
    query = f"SELECT * FROM {table}  "
    df = run_query(query)
    output = df.to_dict(orient="records")
    #formated_prefix = f"<span style='font-size:35px; color:white;'>{prefix}</span>"
    formated_prefix = f"<span style='font-size:40px; color:white; text-align:right; display:block; font-family:Times New Roman;'>{prefix}</span>"
    formated_suffix = f"<span style='font-size:12px; color:gray;  font-family:Times New Roman;'>{suffix}</span>"

    fig.add_trace(
        go.Indicator(
            mode="number",
            align="center",
            value=output[0].get(column),
            title={
                "text": f"{title} ({output[0].get(info, info)})",
                "font": {"size": 14},
            },
            number={
                "prefix": formated_prefix,
                "suffix": formated_suffix,
                "font": {"size": 50, "color": "black", "family": "Times New Roman"}
            },
        ),
        row=1,
        col=idx + 1,
    )

fig.update_layout(
    margin=dict(t=50, b=50, l=0, r=40),
    height=200,
)

st.plotly_chart(fig, use_container_width=True)


def format_number(x):
    if x >= 1000 and x < 1000000:
        return f"{x/1000:.2f}K"
    elif x >= 1000000:
        return f"{x/1000000:.3f}M"
    return str(x)


int_tables = [
    "olist_data_intermediate.int_orders_by_state",
    "olist_data_intermediate.int_sales_by_category",
]
int_columns = ["total_orders", "total_sales"]

cols = st.columns(len(int_tables), gap="large", vertical_alignment="bottom")
for idx, (table, col, title) in enumerate(zip(int_tables, int_columns, titles[1:])):
    query = f"with query  as (select  rank() over (order by {col} desc) as rank, * from {table})select * from query where rank <= 5"
    df = run_query(query)
    sorted_df = df.sort_values(by="rank", ascending=True)
    df_reset = sorted_df.reset_index(drop=True)
    styled_df = df_reset.style.format(formatter={col: format_number}).set_properties(
        **{"background-color": "lightblue", "color": "black", "border-color": "white"}
    )

    title = f"Top 5 {title}"
    cols[idx].markdown(
        f"<h4 style='font-size:16px; font-family:\"Times New Roman\; text-align: center;'>{title}</h4>",
        unsafe_allow_html=True,
    )

    cols[idx].dataframe(styled_df, height=200, width=1350, hide_index=True)
