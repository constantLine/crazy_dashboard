import datetime
import streamlit as st
import pandas as pd
import psycopg2

# Streamlit selector for schema
schema = st.selectbox('Select schema', ('small', 'prod'))

# Establishing connection
conn = psycopg2.connect('user=app password=verysecretpassword host=localhost dbname=appdb', options=f'-c search_path={schema}')

# Function to execute query and return DataFrame
def execute_query(query):
    return pd.read_sql_query(query, conn)

# Load categories
categories_df = execute_query('SELECT category_id, name FROM categories')
categories = categories_df.set_index('category_id')['name'].to_dict()
st.write("Categories loaded:", categories)

# Load products
products_df = execute_query('''
    SELECT pos.product_id, pc.category_id, pos.product_count
    FROM products_on_shelves pos
    JOIN product_categories pc ON pos.product_id = pc.product_id
    GROUP BY pos.product_id, pc.category_id, pos.shelve_id, pos.product_count;
''')
st.write("Get product checkpoint passed len=", len(products_df))
st.write(products_df.head(15))

# Load checks
checks_df = execute_query('''
    SELECT c.issue_date, cp.product_id, cp.product_count
    FROM checks c
    JOIN product_check_positions cp ON c.check_id = cp.check_id;
''')
st.write("Checks checkpoint passed len=", len(checks_df))
st.write(checks_df.head(15))

# Load supplies
supplies_df = execute_query('''
    SELECT s.finish_date, sp.product_id, sp.product_count
    FROM supplies s
    JOIN supplies_products sp ON s.supply_id = sp.supply_id;
''')
st.write("Supplies checkpoint passed len=", len(supplies_df))
st.write(supplies_df.head(15))

# Load external supplies
external_supplies_df = execute_query('''
    SELECT es.finish_date, esp.product_id, esp.product_count
    FROM external_supplies es
    JOIN external_supplies_products esp ON es.ext_supply_id = esp.ext_supply_id;
''')
st.write("External_supplies checkpoint passed len=", len(external_supplies_df))
st.write(external_supplies_df.head(15))

# Combine all events into a single DataFrame
events_df = pd.concat([
    checks_df.rename(columns={'issue_date': 'date', 'product_count': 'change'}).assign(type='check'),
    supplies_df.rename(columns={'finish_date': 'date', 'product_count': 'change'}).assign(type='supply'),
    external_supplies_df.rename(columns={'finish_date': 'date', 'product_count': 'change'}).assign(type='external_supply').assign(change=lambda x: -x['change'])
])

st.write("Combined events dataframe checkpoint passed len=", len(events_df))

# Create a DataFrame for the time series of each category
events_df['date'] = pd.to_datetime(events_df['date'])
events_df = events_df.sort_values('date')
graph_data = {category: [0] for category in categories.values()}
date_index = [events_df['date'].min() - pd.Timedelta(days=1)]

# Calculate initial stock levels
initial_stock = products_df.groupby('category_id')['product_count'].sum()
for category_id, count in initial_stock.items():
    graph_data[categories[category_id]][0] = count

# Create time series data
for date, group in events_df.groupby('date'):
    date_index.append(date)
    day_changes = group.groupby('product_id')['change'].sum()
    for product_id, change in day_changes.items():
        category_name = categories[products_df.loc[products_df['product_id'] == product_id, 'category_id'].values[0]]
        graph_data[category_name].append(graph_data[category_name][-1] + change)
    for category in graph_data:
        if len(graph_data[category]) < len(date_index):
            graph_data[category].append(graph_data[category][-1])

# Ensure all categories have values for all dates
for category in graph_data:
    while len(graph_data[category]) < len(date_index):
        graph_data[category].append(graph_data[category][-1])

# Convert to DataFrame
data = pd.DataFrame(data=graph_data, index=pd.to_datetime(date_index))
st.area_chart(data)
