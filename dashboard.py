import datetime

import streamlit as st
import pandas as pd
import psycopg2

schema = st.selectbox('Select schema', ('small', 'prod'))

conn = psycopg2.connect('user=app password=verysecretpassword host=localhost dbname=appdb', options=f'-c search_path={schema}',)

locations = {}
store_locations = {}
warehouses = {}
warehouse_locations = {}
warehouse_shelves = {}
products_on_shelves = {}
product_check_positions = []
product_categories = {}
categories = {}
products = {}
checks = {}
checks_stores = {}
shelves = {}
stores = {}
supplies = {}
supplies_warehouses = []
supplies_stores = {}
external_supplies = {}
external_supplies_warehouses = []
supplies_products = []
external_supplies_products = []

with conn as txn:
    cur = conn.cursor()
    cur.execute('select * from locations')
    for row in cur.fetchall():
        locations[row[0]] = row[1]
    cur.execute('select * from store_locations')
    for row in cur.fetchall():
        store_locations[row[0]] = row[1]
    cur.execute('select * from warehouses')
    for row in cur.fetchall():
        warehouses[row[0]] = {'name': row[1]}
    cur.execute('select * from warehouse_locations')
    for row in cur.fetchall():
        warehouse_locations[row[0]] = row[1]
    cur.execute('select * from warehouse_shelves')
    for row in cur.fetchall():
        warehouse_shelves[row[0]] = row[1]
    cur.execute('select * from products_on_shelves')
    for row in cur.fetchall():
        products_on_shelves[row[2]] = {'count': row[1], 'product_id': row[0]}
    cur.execute('select * from product_check_positions')
    for row in cur.fetchall():
        product_check_positions.append((row[0], row[1], row[2]))
    cur.execute('select * from product_categories')
    for row in cur.fetchall():
        product_categories[row[0]] = row[1]
    cur.execute('select * from categories')
    for row in cur.fetchall():
        categories[row[0]] = row[1]
    cur.execute('select * from products')
    for row in cur.fetchall():
        products[row[0]] = {'name': row[1]}
    cur.execute('select * from checks')
    for row in cur.fetchall():
        checks[row[0]] = {'issue_date': row[1]}
    cur.execute('select * from checks_stores')
    for row in cur.fetchall():
        checks_stores[row[0]] = row[1]
    cur.execute('select * from shelves')
    for row in cur.fetchall():
        shelves[row[0]] = row[1]
    cur.execute('select * from stores')
    for row in cur.fetchall():
        stores[row[0]] = {'name': row[1]}
    cur.execute('select * from supplies')
    for row in cur.fetchall():
        supplies[row[0]] = {'finish_date': row[1]}
    cur.execute('select * from supplies_stores')
    for row in cur.fetchall():
        supplies_stores[row[0]] = row[1]
    cur.execute('select * from external_supplies')
    for row in cur.fetchall():
        external_supplies[row[0]] = {'finish_date': row[1]}
    cur.execute('select * from supplies_warehouses')
    for row in cur.fetchall():
        supplies_warehouses.append((row[0], row[1], row[2]))
    cur.execute('select * from external_supplies_warehouses')
    for row in cur.fetchall():
        external_supplies_warehouses.append((row[0], row[1], row[2]))
    cur.execute('select * from external_supplies_products')
    for row in cur.fetchall():
        external_supplies_products.append((row[0], row[1], row[2]))
    cur.execute('select * from supplies_products')
    for row in cur.fetchall():
        supplies_products.append((row[0], row[1], row[2]))

for store, opts in stores.items():
    for location_id, store_id in store_locations.items():
        if store_id == store:
            opts['location'] = locations[location_id]

for warehouse, opts in warehouses.items():
    for location_id, warehouse_id in warehouse_locations.items():
        if warehouse_id == warehouse:
            opts['location'] = locations[location_id]

for warehouse, opts in warehouses.items():
    for shelve_id, warehouse_id in warehouse_shelves.items():
        if warehouse_id == warehouse:
            if 'shelves' not in opts:
                opts['shelves'] = []
            opts['shelves'].append(shelve_id)

for product, opts in products.items():
    for shelve, data in products_on_shelves.items():
        if product == data['product_id']:
            if 'shelves' not in opts:
                opts['shelves'] = []
            opts['shelves'].append({'shelve_id': shelve, 'count': data['count']})

for product, opts in products.items():
    category_id = product_categories[product]
    category_name = categories[category_id]
    opts['category'] = category_name

for check, opts in checks.items():
    opts['store_id'] = checks_stores[check]
    opts['positions'] = []
    for position in product_check_positions:
        if position[0] == check:
            opts['positions'].append({'product_count': position[1], 'product_id': position[2]})

for supply, opts in supplies.items():
    opts['store_id'] = supplies_stores[supply]
    opts['shelves'] = []
    opts['positions'] = []
    for shelve in supplies_warehouses:
        if shelve[0] == supply:
            opts['shelves'].append({'warehouse_id': shelve[1], 'shelve_id': shelve[2]})
    for position in supplies_products:
        if position[0] == supply:
            opts['positions'].append({'product_id': position[1], 'product_count': position[2]})

for supply, opts in external_supplies.items():
    opts['shelves'] = []
    opts['positions'] = []
    for shelve in external_supplies_warehouses:
        if shelve[0] == supply:
            opts['shelves'].append({'warehouse_id': shelve[1], 'shelve_id': shelve[2]})
    for position in external_supplies_products:
        if position[0] == supply:
            opts['positions'].append({'product_id': position[1], 'product_count': position[2]})

events = {}

st.write("Get product checkpoint passed len=", len(products))
st.write(products)

st.write("Checks checkpoint passed len=", len(checks))
st.write(checks[:10])

st.write("Supplies checkpoint passed len=", len(supplies))
st.write(supplies[:10])

st.write("External_supplies checkpoint passed len=", len(external_supplies))
st.write(external_supplies[:10])


for check, opts in checks.items():
    if opts['issue_date'] not in events:
        events[opts['issue_date']] = []
    events[opts['issue_date']].append({'type': 'check', 'data': {'check_id': check, **opts}})

st.write("Events check checkpoint passed len=", len(events.items()))
st.write(events.items())

for supply, opts in supplies.items():
    if opts['finish_date'] not in events:
        events[opts['finish_date']] = []
    events[opts['finish_date']].append({'type': 'supply', 'data': {'supply_id': supply, **opts}})

st.write("Events supply checkpoint passed len=", len(events.items()))
st.write(events.items())

for supply, opts in external_supplies.items():
    if opts['finish_date'] not in events:
        events[opts['finish_date']] = []
    events[opts['finish_date']].append({'type': 'external_supply', 'data': {'supply_id': supply, **opts}})

st.write("Events external checkpoint passed len=", len(events.items()))
st.write(events.items())

index = [x.strftime('%Y-%m-%d') for x in sorted(events.keys())]
st.write("Indexies 1 =", index)
graph_data = {x: [0] * (len(index) + 1) for x in categories.values()}

for category_name in graph_data:
    cat_sum = 0
    for product in products.values():
        if product['category'] == category_name:
            for shelve in product.get('shelves', []):
                cat_sum += shelve['count']

    graph_data[category_name][-1] = cat_sum

st.write("Graphdata 1 =", graph_data)

first = list(sorted(events.keys()))[0]

events[first - datetime.timedelta(days=1)] = []
index = [(first - datetime.timedelta(days=1)).strftime('%Y-%m-%d')] + index

st.write("Events proxy =", events.items())

diffs = {}
offset = 0

for event_date in reversed(sorted(events.keys())):
    st.write("\nevent_date = ", event_date)
    if offset != 0:
        for category in graph_data:
            graph_data[category][-1-offset] = graph_data[category][-1-(offset-1)] + diffs.get(category, 0)
    diffs.clear()
    day_events = events[event_date]
    st.write("day_events = ", day_events)

    for event in day_events:
        if event['type'] == 'check':
            for position in event['data']['positions']:
                category = products[position['product_id']]['category']
                if category not in diffs:
                    diffs[category] = 0
                diffs[category] += position['product_count']
        elif event['type'] == 'external_supply':
            for position in event['data']['positions']:
                category = products[position['product_id']]['category']
                if category not in diffs:
                    diffs[category] = 0
                diffs[category] -= position['product_count']
    offset += 1
    st.write("\ndiffs = ", diffs)

st.write("Indexies 2 =", index)
st.write("Graphdata 2 =", graph_data)

data = pd.DataFrame(data=graph_data, index=index)
st.area_chart(data)
