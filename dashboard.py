import datetime
import streamlit as st
import pandas as pd
import psycopg2

# Выбор схемы базы данных
schema = st.selectbox('Select schema', ('small', 'prod'))

# Установка соединения с базой данных с использованием выбранной схемы
conn = psycopg2.connect('user=app password=verysecretpassword host=localhost dbname=appdb', options=f'-c search_path={schema}')

def execute_query(query, params=None):
    with conn.cursor() as cur:
        cur.execute(query, params)
        return cur.fetchall()

def get_data_dict(query):
    data = execute_query(query)
    return {row[0]: row[1:] for row in data}

# Запросы для получения данных
categories = get_data_dict('SELECT id, category_name FROM categories')

def get_product_info():
    query = '''
    SELECT p.id, p.name, pc.category_id, c.category_name, pos.shelve_id, pos.count
    FROM products p
    JOIN product_categories pc ON p.id = pc.product_id
    JOIN categories c ON pc.category_id = c.id
    LEFT JOIN products_on_shelves pos ON p.id = pos.product_id
    '''
    data = execute_query(query)
    products = {}
    for row in data:
        product_id, name, category_id, category_name, shelve_id, count = row
        if product_id not in products:
            products[product_id] = {
                'name': name,
                'category': category_name,
                'shelves': []
            }
        if shelve_id is not None:
            products[product_id]['shelves'].append({'shelve_id': shelve_id, 'count': count})
    return products

def get_checks():
    query = '''
    SELECT c.id, c.issue_date, cp.product_id, cp.product_count, cs.store_id
    FROM checks c
    JOIN checks_stores cs ON c.id = cs.check_id
    JOIN product_check_positions cp ON c.id = cp.check_id
    '''
    data = execute_query(query)
    checks = {}
    for row in data:
        check_id, issue_date, product_id, product_count, store_id = row
        if check_id not in checks:
            checks[check_id] = {
                'issue_date': issue_date,
                'store_id': store_id,
                'positions': []
            }
        checks[check_id]['positions'].append({
            'product_id': product_id,
            'product_count': product_count
        })
    return checks

def get_supplies(query):
    data = execute_query(query)
    supplies = {}
    for row in data:
        supply_id, finish_date, store_id, warehouse_id, shelve_id, product_id, product_count = row
        if supply_id not in supplies:
            supplies[supply_id] = {
                'finish_date': finish_date,
                'store_id': store_id,
                'shelves': [],
                'positions': []
            }
        if warehouse_id is not None and shelve_id is not None:
            supplies[supply_id]['shelves'].append({
                'warehouse_id': warehouse_id,
                'shelve_id': shelve_id
            })
        if product_id is not None and product_count is not None:
            supplies[supply_id]['positions'].append({
                'product_id': product_id,
                'product_count': product_count
            })
    return supplies

products = get_product_info()
checks = get_checks()
supplies = get_supplies('''
    SELECT s.id, s.finish_date, ss.store_id, sw.warehouse_id, sw.shelve_id, sp.product_id, sp.product_count
    FROM supplies s
    LEFT JOIN supplies_stores ss ON s.id = ss.supply_id
    LEFT JOIN supplies_warehouses sw ON s.id = sw.supply_id
    LEFT JOIN supplies_products sp ON s.id = sp.supply_id
''')
external_supplies = get_supplies('''
    SELECT es.id, es.finish_date, NULL, esw.warehouse_id, esw.shelve_id, esp.product_id, esp.product_count
    FROM external_supplies es
    LEFT JOIN external_supplies_warehouses esw ON es.id = esw.supply_id
    LEFT JOIN external_supplies_products esp ON es.id = esp.supply_id
''')

# Создание событий для графика
events = {}
for check_id, opts in checks.items():
    events.setdefault(opts['issue_date'], []).append({'type': 'check', 'data': {'check_id': check_id, **opts}})
for supply_id, opts in supplies.items():
    events.setdefault(opts['finish_date'], []).append({'type': 'supply', 'data': {'supply_id': supply_id, **opts}})
for supply_id, opts in external_supplies.items():
    events.setdefault(opts['finish_date'], []).append({'type': 'external_supply', 'data': {'supply_id': supply_id, **opts}})

# Создание индекса для графика
index = [x.strftime('%Y-%m-%d') for x in sorted(events.keys())]

# Инициализация данных для графика
graph_data = {x: [0] * (len(index) + 1) for x in categories.values()}

# Подсчет начального количества товаров в каждой категории
for category_name in graph_data:
    cat_sum = 0
    for product in products.values():
        if product['category'] == category_name:
            for shelve in product.get('shelves', []):
                cat_sum += shelve['count']
    graph_data[category_name][-1] = cat_sum

# Добавление пустого дня перед первым событием
first_date = sorted(events.keys())[0]
prev_date = first_date - datetime.timedelta(days=1)
events[prev_date] = []
index = [prev_date.strftime('%Y-%m-%d')] + index

# Расчет изменений количества товаров по категориям
diffs = {}
offset = 0
for event_date in reversed(sorted(events.keys())):
    if offset != 0:
        for category in graph_data:
            graph_data[category][-1-offset] = graph_data[category][-1-(offset-1)] + diffs.get(category, 0)
    diffs.clear()
    day_events = events[event_date]
    for event in day_events:
        if event['type'] == 'check':
            for position in event['data']['positions']:
                category = products[position['product_id']]['category']
                diffs[category] = diffs.get(category, 0) + position['product_count']
        elif event['type'] == 'external_supply':
            for position in event['data']['positions']:
                category = products[position['product_id']]['category']
                diffs[category] = diffs.get(category, 0) - position['product_count']
    offset += 1

# Создание DataFrame и отображение графика
data = pd.DataFrame(data=graph_data, index=index)
st.area_chart(data)

