import datetime
import streamlit as st
import pandas as pd
import psycopg2

schema = st.selectbox('Select schema', ('small', 'prod'))

conn = psycopg2.connect('user=app password=verysecretpassword host=localhost dbname=appdb', options=f'-c search_path={schema}',)

def execute_query(query, params=None):
    with conn.cursor() as cur:
        cur.execute(query, params)
        return cur.fetchall()

def get_data_dict(query):
    data = execute_query(query)
    return {row[0]: row[1] for row in data}

categories = get_data_dict('SELECT category_id, name FROM categories')
st.write("Categories loaded:", categories)
products = execute_query('''
    SELECT p.product_id, p.name, pc.category_id, c.name, COALESCE(pos.count, 0)
    FROM products p
    JOIN product_categories pc ON p.product_id = pc.product_id
    JOIN categories c ON pc.category_id = c.category_id
    LEFT JOIN products_on_shelves pos ON p.product_id = pos.product_id
    GROUP BY p.product_id, p.name, pc.category_id, c.name, pos.shelve_id;
''')
st.write("Get product checkpoint passed len=", len(products))
st.write(products[:10])


checks = execute_query('''
    SELECT c.check_id, c.issue_date, cp.product_id, cp.product_count
    FROM checks c
    JOIN product_check_positions cp ON c.check_id = cp.check_id;
''')
st.write("Checks checkpoint passed len=", len(checks))
st.write(checks[:10])

supplies = execute_query('''
    SELECT s.supply_id, s.finish_date, sp.product_id, sp.product_count
    FROM supplies s
    JOIN supplies_products sp ON s.supply_id = sp.supply_id;
''')
st.write("Supplies checkpoint passed len=", len(supplies))
st.write(supplies[:10])

external_supplies = execute_query('''
    SELECT es.ext_supply_id, es.finish_date, esp.product_id, esp.product_count
    FROM external_supplies es
    JOIN external_supplies_products esp ON es.ext_supply_id = esp.ext_supply_id;
''')
st.write("External_supplies checkpoint passed len=", len(external_supplies))
st.write(external_supplies[:10])

events = {}

for check in checks:
    check_id, issue_date, product_id, product_count = check
    if issue_date not in events:
        events[issue_date] = []
    events[issue_date].append({'type': 'check', 'product_id': product_id, 'product_count': product_count})

st.write("Events check checkpoint passed len=", len(events.items()))
st.write(events.items())

for supply in supplies:
    supply_id, finish_date, product_id, product_count = supply
    if finish_date not in events:
        events[finish_date] = []
    events[finish_date].append({'type': 'supply', 'product_id': product_id, 'product_count': product_count})

st.write("Events supply checkpoint passed len=", len(events.items()))
st.write(events.items())

for external_supply in external_supplies:
    external_supply_id, finish_date, product_id, product_count = external_supply
    if finish_date not in events:
        events[finish_date] = []
    events[finish_date].append({'type': 'external_supply', 'product_id': product_id, 'product_count': product_count})

st.write("Events external checkpoint passed len=", len(events.items()))
st.write(events.items())

index = [x.strftime('%Y-%m-%d') for x in sorted(events.keys())]
st.write("Indexies 1 =", index)

graph_data = {category_name: [0] * (len(index) + 1) for category_name in categories.values()}

for product_id, name, category_id, category_name, count in products:
    graph_data[category_name][-1] += count

st.write("Graphdata 1 =", graph_data)

first = list(sorted(events.keys()))[0]

events[first - datetime.timedelta(days=1)] = []
index = [(first - datetime.timedelta(days=1)).strftime('%Y-%m-%d')] + index

st.write("Events proxy =", events.items())

diffs = {}
offset = 0

for event_date in sorted(events.keys()):
    st.write("\nevent_dates = ", event_date)
    if offset != 0:
        for category in graph_data:
            graph_data[category][-1-offset] = graph_data[category][-1-(offset-1)] + diffs.get(category, 0)
    diffs.clear()
    day_events = events[event_date]
    for event in day_events:
        category = categories[[pc[2] for pc in products if pc[0] == event['product_id']][0]]
        st.write("\ncategories = ", category)

        if category not in diffs:
            diffs[category] = 0
        if event['type'] == 'check':
            diffs[category] += event['product_count']
        elif event['type'] == 'external_supply':
            diffs[category] -= event['product_count']
    offset += 1
    st.write("\ndiffs = ", diffs)

st.write("Indexies 2 =", index)
st.write("Graphdata 2 =", graph_data)

data = pd.DataFrame(data=graph_data, index=index)
st.area_chart(data)
