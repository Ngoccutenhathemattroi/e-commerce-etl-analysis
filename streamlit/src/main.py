import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from mlxtend.frequent_patterns import apriori, association_rules
from mlxtend.preprocessing import TransactionEncoder
import psycopg2
from dotenv import load_dotenv
import os
import warnings

warnings.filterwarnings('ignore')

# Load environment variables
load_dotenv()

# PostgreSQL configuration
PSQL_CONFIG = {
    "host": os.getenv("POSTGRES_HOST"),
    "port": os.getenv("POSTGRES_PORT"),
    "database": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD")
}


def init_connection(config):
    return psycopg2.connect(
        database=config['database'],
        user=config['user'],
        password=config['password'],
        host=config['host'],
        port=config['port']
    )

def extract_data(config, table_name):
    conn = init_connection(config)
    try:
        query = f'SELECT * FROM ecom.{table_name}'
        print(f"Executing query: {query}")
        df = pd.read_sql(query, conn)
        print(f"Data extracted from {table_name} with shape: {df.shape}")
        return df
    except Exception as e:
        print(f"Error extracting data from {table_name}:", e)
        raise
    finally:
        if conn:
            conn.close()
            print("PostgreSQL connection closed")


# Set Streamlit page configuration
st.set_page_config(page_title="EDA Dashboard", page_icon=":bar_chart:", layout="wide")

# Load data from PostgreSQL
df_transactions = extract_data(PSQL_CONFIG, 'warehouse_transactions_with_order_items')

df_sales = extract_data(PSQL_CONFIG, 'warehouse_monthly_product_sales_summary')

# Preprocess the Apriori data
df_transactions['list_of_products'] = df_transactions['list_of_products'].apply(lambda x: x.split(','))
records = df_transactions['list_of_products'].tolist()
te = TransactionEncoder()
te_ary = te.fit(records).transform(records)
df_apriori = pd.DataFrame(te_ary, columns=te.columns_)
frequent_itemsets = apriori(df_apriori, min_support=0.05, use_colnames=True)
default_categories = set(item for itemset in frequent_itemsets['itemsets'] for item in itemset)
# Round support values to 2 decimal places
frequent_itemsets['support'] = frequent_itemsets['support'].round(3)

# Convert frozenset to string for visualization
frequent_itemsets['itemsets'] = frequent_itemsets['itemsets'].apply(lambda x: ', '.join(list(x)))
#####################################################################################

# Rename columns for better readability
df_sales.rename(columns={
    'sales_month': 'Sales Month',
    'product_category': 'Product Category',
    'total_sales_value': 'Total Sales Value',
    'total_products_sold': 'Total Products Sold'
}, inplace=True)
# Convert 'Sales Month' to datetime format
df_sales['Sales Month'] = pd.to_datetime(df_sales['Sales Month'])
# Sidebar Filters
default_categories = set(item for itemset in frequent_itemsets['itemsets'] for item in itemset)
top_10_categories = df_sales.groupby('Product Category')['Total Sales Value'].sum().nlargest(10).index.tolist()
st.sidebar.header("Filters")
selected_category = st.sidebar.multiselect(
    'Select Category', 
    options=df_sales['Product Category'].unique(), 
    default=top_10_categories
)
date_range = st.sidebar.date_input('Select Date Range', [df_sales['Sales Month'].min(), df_sales['Sales Month'].max()])

# Apply Filters
filtered_df = df_sales[(df_sales['Product Category'].isin(selected_category)) & 
                       (df_sales['Sales Month'].between(pd.to_datetime(date_range[0]), pd.to_datetime(date_range[1])))]


# Plotly table for Apriori frequent itemsets
def apriori_table(data):
    fig = go.Figure(data=[go.Table(
        header=dict(values=list(data.columns), align='left'),
        cells=dict(values=[data[col] for col in data.columns],align='left'))])
    st.plotly_chart(fig, use_container_width=True)


# # Treemap Chart via on category
# def treemap_chart(data):
#     # Get top 10 categories by Total Sales Value
#     top_categories = data.groupby('Product Category')['Total Sales Value'].sum().nlargest(10).index
#     data = data[data['Product Category'].isin(top_categories)]
#     fig = px.treemap(data, path=['Product Category'], values='Total Sales Value', 
#                     template='presentation', color='Total Sales Value', 
#                     color_discrete_sequence=px.colors.sequential.Blues)
#     fig.update_traces(
#         textinfo="label+text+value",
#         texttemplate='%{label}<br>Sales Value: $%{value:,}',
#         textfont_size=15
#     )
#     st.plotly_chart(fig, use_container_width=True)

# Pie Chart via on category
def pie_chart(data):
    # Get top 10 categories by Total Sales Value
    top_categories = data.groupby('Product Category')['Total Sales Value'].sum().nlargest(10).index
    data = data[data['Product Category'].isin(top_categories)]
    fig = px.pie(data, names='Product Category', values='Total Sales Value', 
                template='ggplot2', color_discrete_sequence=px.colors.qualitative.T10)
    st.plotly_chart(fig, use_container_width=True)

# Area Chart via on category
def area_chart(data):
    fig = px.area(data, x='Sales Month', y='Total Sales Value', color='Product Category', 
                template='simple_white', color_discrete_sequence=px.colors.qualitative.T10)
    st.plotly_chart(fig, use_container_width=True)

# Box Plot via on category
def box_plot(data):
    fig = px.box(data, x='Product Category', y='Total Sales Value', color='Product Category', 
                template='plotly_dark', color_discrete_sequence=px.colors.qualitative.T10)
    st.plotly_chart(fig, use_container_width=True)

############################################################################################################

# Layout in Streamlit
st.title("Ecommerce EDA Dashboard🛒")

# Calculate total sales value, count of products, and average sales value
total_sales = df_sales["Total Sales Value"].sum()
average_sales = df_sales["Total Sales Value"].mean()
df_churn = extract_data(PSQL_CONFIG,'warehouse_customer_churn')
average_score = round(df_churn['average_review_score'].mean(), 1)
star_rating = ':star:' * int(round(average_score, 1))

left_column, right_column = st.columns(2)
with left_column:
    st.markdown("<h3 style='color: #FF69B4;'>Frequent Itemsets:</h3>", unsafe_allow_html=True)
    apriori_table(frequent_itemsets)
with right_column:
    # total sales value
    st.markdown("<h3 style='color: #FF69B4;'>Total Sales Value:</h3>", unsafe_allow_html=True)
    st.subheader(f"US $ {round(total_sales)}")
    # average score of reviews
    st.markdown("<h3 style='color: #FF69B4;'>Average score:</h3>", unsafe_allow_html=True)
    st.subheader(f"{average_score} {star_rating}")
    # average sales value
    st.markdown("<h3 style='color: #FF69B4;'>Average Sales Value:</h3>", unsafe_allow_html=True)
    st.subheader(f"US $ {round(average_sales)}")

col1, col2 = st.columns(2)
with col1:
    st.markdown("<h3 style='color: #FF69B4;'>Total Sales Value Over Time by Month:</h3>", unsafe_allow_html=True)
    sales_by_month = filtered_df.groupby("Sales Month")["Total Sales Value"].sum().reset_index()
    fig_line = px.line(sales_by_month, x="Sales Month", y="Total Sales Value",
                       markers=True,
                       template="plotly_white",
                       color_discrete_sequence=['#E377C2'])
    st.plotly_chart(fig_line, use_container_width=True)

    st.divider()
with col2:
    st.markdown("<h3 style='color: #FF69B4;'>Total Sales Value Over Time By Category:</h3>", unsafe_allow_html=True)
    area_chart(filtered_df)
col3, col4 = st.columns(2)
with col3:
    st.markdown("<h3 style='color: #FF69B4;'>Total Sales Value by Category:</h3>", unsafe_allow_html=True)
    pie_chart(filtered_df)
with col4:
    st.markdown("<h3 style='color: #FF69B4;'>Distribution of Total Sales Value by Category:</h3>", unsafe_allow_html=True)
    box_plot(filtered_df)


# bar chart for top 3 categories with highest total sale values
top_10_categories = df_sales.groupby('Product Category')['Total Sales Value'].sum().nlargest(10).reset_index()
st.markdown("<h3 style='color: #FF69B4;'>Top 10 Categories With Highest Total Sale Values</h3>", unsafe_allow_html=True)
fig = px.bar(top_10_categories, x='Product Category', y='Total Sales Value', 
             color='Product Category', 
             template='plotly_white', 
             color_discrete_sequence=px.colors.qualitative.T10)
st.plotly_chart(fig, use_container_width=True)


