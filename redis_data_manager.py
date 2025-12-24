import redis
import pandas as pd
import json
import time
import random
from faker import Faker
import uuid
import os

# --- 配置 ---
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
REDIS_DB = 0
FAKE_PRODUCT_COUNT = 1000
FAKE_USER_COUNT = 100
FAKE_ORDER_COUNT = 500
ONLINE_RETAIL_DATA_PATH = 'data.csv' # Online Retail 数据集路径

# Redis 客户端 (这里不直接连接，而是通过函数传入或在调用时创建)
def get_redis_client():
    """获取 Redis 客户端实例"""
    try:
        r = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)
        r.ping()
        return r
    except redis.exceptions.ConnectionError as e:
        print(f"无法连接到 Redis 服务器: {e}")
        return None

# 初始化 Faker
fake = Faker('zh_CN')

# --- 任务一：数据收集与清洗 ---

def generate_synthetic_product_data(count):
    products = []
    categories = ['电子产品', '服装鞋帽', '家居百货', '图书音像', '美妆个护', '食品饮料']
    for _ in range(count):
        product_id = str(uuid.uuid4())
        name = f"{fake.word()} {fake.color_name()} {fake.catch_phrase()}"
        description = fake.text(max_nb_chars=200)
        category = random.choice(categories)
        price = round(random.uniform(10.0, 10000.0), 2)
        stock = random.randint(0, 500)
        created_at = fake.date_time_this_year().isoformat()
        products.append({
            'product_id': product_id,
            'name': name,
            'description': description,
            'category': category,
            'price': price,
            'stock': stock,
            'created_at': created_at
        })
    return products

def generate_synthetic_user_data(count):
    users = []
    for _ in range(count):
        user_id = str(uuid.uuid4())
        username = fake.user_name()
        email = fake.email()
        registration_date = fake.date_time_this_decade().isoformat()
        last_login = fake.date_time_this_year().isoformat()
        users.append({
            'user_id': user_id,
            'username': username,
            'email': email,
            'registration_date': registration_date,
            'last_login': last_login
        })
    return users

def generate_synthetic_order_data(product_ids, user_ids, count):
    orders = []
    statuses = ['待付款', '已付款', '已发货', '已完成', '已取消']

    if not product_ids:
        print("警告：没有可用的商品ID来生成订单。")
        return []
    if not user_ids:
        print("警告：没有可用的用户ID来生成订单。")
        return []

    for _ in range(count):
        order_id = str(uuid.uuid4())
        user_id = random.choice(user_ids)
        product_id = random.choice(product_ids)
        quantity = random.randint(1, 5)
        unit_price = round(random.uniform(10.0, 500.0), 2)
        order_date = fake.date_time_this_year().isoformat()
        status = random.choice(statuses)
        orders.append({
            'order_id': order_id,
            'user_id': user_id,
            'product_id': product_id, # 注意：这里是单个商品ID，与Online Retail的多商品订单有差异
            'quantity': quantity,
            'unit_price': unit_price,
            'order_date': order_date,
            'status': status
        })
    return orders


def collect_and_clean_data():
    """
    模拟数据收集和清洗过程（使用 Faker 生成数据）。
    """
    products_raw = generate_synthetic_product_data(FAKE_PRODUCT_COUNT)
    products_df = pd.DataFrame(products_raw)

    users_raw = generate_synthetic_user_data(FAKE_USER_COUNT)
    users_df = pd.DataFrame(users_raw)

    product_ids_list = products_df['product_id'].tolist()
    user_ids_list = users_df['user_id'].tolist()
    orders_raw = generate_synthetic_order_data(product_ids_list, user_ids_list, FAKE_ORDER_COUNT)
    orders_df = pd.DataFrame(orders_raw)

    # 数据清洗示例
    products_df.loc[:, 'description'] = products_df['description'].fillna('暂无描述')
    products_df.loc[:, 'price'] = pd.to_numeric(products_df['price'], errors='coerce')
    products_df.loc[:, 'stock'] = pd.to_numeric(products_df['stock'], errors='coerce')
    products_df.dropna(subset=['price', 'stock'], inplace=True)
    products_df.loc[:, 'category'] = products_df['category'].str.strip().str.lower()

    users_df.loc[:, 'registration_date'] = pd.to_datetime(users_df['registration_date'], errors='coerce')
    users_df.loc[:, 'last_login'] = pd.to_datetime(users_df['last_login'], errors='coerce')
    users_df.dropna(subset=['registration_date', 'last_login'], inplace=True)

    orders_df.loc[:, 'quantity'] = pd.to_numeric(orders_df['quantity'], errors='coerce')
    orders_df.loc[:, 'unit_price'] = pd.to_numeric(orders_df['unit_price'], errors='coerce')
    orders_df.loc[:, 'order_date'] = pd.to_datetime(orders_df['order_date'], errors='coerce')
    orders_df.dropna(subset=['quantity', 'unit_price', 'order_date'], inplace=True)
    orders_df.loc[:, 'status'] = orders_df['status'].str.strip().str.lower()

    return products_df, users_df, orders_df


def load_online_retail_data_to_dfs(fake_fill_missing=True):
    """
    从 Online Retail 数据集 (data.csv) 加载、清洗并转换为 products_df, users_df, orders_df。
    """
    if not os.path.exists(ONLINE_RETAIL_DATA_PATH):
        raise FileNotFoundError(f"未找到 {ONLINE_RETAIL_DATA_PATH} 文件。请确保已下载并放置在项目根目录。")

    df = pd.read_csv(ONLINE_RETAIL_DATA_PATH, encoding='ISO-8859-1')

    # --- 数据清洗 ---
    # 移除没有 CustomerID 的行 (匿名用户)
    df.dropna(subset=['CustomerID'], inplace=True)
    df['CustomerID'] = df['CustomerID'].astype(int).astype(str) # 确保 CustomerID 是字符串

    # 移除没有 Description 的行
    df.dropna(subset=['Description'], inplace=True)

    # 移除 Quantity 或 UnitPrice 为负数的行 (退货或无效数据)
    df = df[df['Quantity'] > 0]
    df = df[df['UnitPrice'] > 0]

    # 转换日期格式
    df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'])

    # --- 提取 Products DataFrame ---
    products_df = df[['StockCode', 'Description', 'UnitPrice']].copy()
    products_df.rename(columns={'StockCode': 'product_id', 'Description': 'name'}, inplace=True)
    # 对于每个商品，取其平均价格作为商品价格
    products_df = products_df.groupby(['product_id', 'name'])['UnitPrice'].mean().reset_index()
    products_df.rename(columns={'UnitPrice': 'price'}, inplace=True)
    
    # 补充缺失字段
    if fake_fill_missing:
        products_df['description'] = products_df['name'].apply(lambda x: fake.text(max_nb_chars=100))
        categories = ['电子产品', '服装鞋帽', '家居百货', '图书音像', '美妆个护', '食品饮料']
        products_df['category'] = products_df['product_id'].apply(lambda x: random.choice(categories))
        products_df['stock'] = products_df['product_id'].apply(lambda x: random.randint(50, 500)) # 随机生成库存
        products_df['created_at'] = products_df['product_id'].apply(lambda x: fake.date_time_this_year().isoformat())
    else:
        products_df['description'] = ""
        products_df['category'] = "未知"
        products_df['stock'] = 0
        products_df['created_at'] = pd.Timestamp.now().isoformat()


    # --- 提取 Users DataFrame ---
    users_df = df[['CustomerID']].copy().drop_duplicates()
    users_df.rename(columns={'CustomerID': 'user_id'}, inplace=True)
    # 补充缺失字段
    if fake_fill_missing:
        users_df['username'] = users_df['user_id'].apply(lambda x: fake.user_name())
        users_df['email'] = users_df['user_id'].apply(lambda x: fake.email())
        users_df['registration_date'] = users_df['user_id'].apply(lambda x: fake.date_time_this_decade().isoformat())
        users_df['last_login'] = users_df['user_id'].apply(lambda x: fake.date_time_this_year().isoformat())
    else:
        users_df['username'] = "匿名用户"
        users_df['email'] = ""
        users_df['registration_date'] = pd.Timestamp.now().isoformat()
        users_df['last_login'] = pd.Timestamp.now().isoformat()


    # --- 提取 Orders DataFrame (更准确的多商品订单处理) ---
    # 聚合每个 InvoiceNo 作为一个订单
    orders_grouped = df.groupby('InvoiceNo')

    orders_data = []
    for invoice_no, group in orders_grouped:
        # 排除 InvoiceNo 中包含 'C' 的退货订单
        if 'C' in str(invoice_no):
            continue

        order_id = str(invoice_no) # 使用 InvoiceNo 作为 order_id
        user_id = group['CustomerID'].iloc[0]
        order_date = group['InvoiceDate'].iloc[0].isoformat()
        total_amount = (group['Quantity'] * group['UnitPrice']).sum()
        country = group['Country'].iloc[0] # 订单国家

        # 订单状态随机生成，因为原始数据没有
        status = random.choice(['已付款', '已发货', '已完成'])

        orders_data.append({
            'order_id': order_id,
            'user_id': user_id,
            'order_date': order_date,
            'total_amount': total_amount,
            'country': country,
            'status': status,
            'items': group[['StockCode', 'Description', 'Quantity', 'UnitPrice']].to_dict(orient='records')
        })
    orders_df = pd.DataFrame(orders_data)

    return products_df, users_df, orders_df

# --- 任务二：数据存储与管理 ---

def store_data_in_redis(r_client, products_df, users_df, orders_df, flush_db=True):
    """
    将清洗后的数据存储到 Redis。
    此版本已更新，可以处理 orders_df 中包含 'items' 列表的情况（来自 Online Retail）。
    """
    if not r_client:
        return "Redis 连接失败，无法存储数据。", False

    if flush_db:
        r_client.flushdb() # 清空当前数据库，谨慎操作！

    pipe = r_client.pipeline()

    # 1. 存储商品数据
    for index, row in products_df.iterrows():
        product_id = row['product_id']
        category = row['category']
        for key, value in row.to_dict().items():
            pipe.hset(f"product:{product_id}", key, str(value))
        pipe.sadd(f"category:{category}:products", product_id)
        pipe.sadd("product:all_ids", product_id)
        pipe.zadd("product:prices", {product_id: float(row['price'])})
    pipe.execute()

    # 2. 存储用户数据
    pipe = r_client.pipeline()
    for index, row in users_df.iterrows():
        user_id = row['user_id']
        for key, value in row.to_dict().items():
            pipe.hset(f"user:{user_id}", key, str(value))
        pipe.sadd("user:all_ids", user_id)
    pipe.execute()

    # 3. 存储订单数据 (已更新以处理多商品订单)
    pipe = r_client.pipeline()
    for index, row in orders_df.iterrows():
        order_id = row['order_id']
        user_id = row['user_id']
        
        # 存储订单总览信息 (Hash)
        order_details = row.drop('items').to_dict() # 排除 items 字段
        for key, value in order_details.items():
            pipe.hset(f"order:{order_id}", key, str(value))
        
        # 存储订单商品明细 (List of Hashes)
        # 每个订单项生成一个唯一 ID，并存储其详情
        for item_idx, item in enumerate(row['items']):
            item_id = f"{order_id}:{item_idx}" # 订单ID:索引 作为订单项ID
            # 修复：兼容旧版 Redis 的 HSET 命令，逐个字段设置
            for key, value in item.items():
                pipe.hset(f"order_item:{item_id}", key, str(value))
            pipe.rpush(f"order:{order_id}:items", item_id) # 将订单项ID添加到订单的items列表中
            # 统计商品销售记录 (针对每个销售的商品)
            # 注意：Online Retail 的 StockCode 可能包含非 UUID 格式，这里直接使用 StockCode
            pipe.lpush(f"product:{item['StockCode']}:sales", item_id) # 记录订单项ID

        # 存储用户订单历史 (List，按时间顺序，新订单在前)
        pipe.lpush(f"user:{user_id}:orders", order_id)
        pipe.sadd("order:all_ids", order_id) # 维护所有订单ID的集合
    pipe.execute()
    return "数据存储完成。", True

# --- 任务三：数据处理与分析 ---

def analyze_data_from_redis(r_client):
    """
    从 Redis 中获取数据并进行分析，返回结果字典。
    此版本已更新，以适应 Online Retail 数据集可能带来的多商品订单结构。
    """
    if not r_client:
        return {"error": "Redis 连接失败，无法进行分析。"}

    results = {}

    all_product_ids = list(r_client.smembers("product:all_ids"))
    results['total_products'] = len(all_product_ids)

    all_categories = set()
    # 维护一个所有分类的 Set 更好，这里为了兼容 KEYS * 的演示
    for key in r_client.keys("category:*:products"):
        category_name = key.split(':')[1]
        all_categories.add(category_name)
    results['total_categories'] = len(all_categories)

    # 热门分类 (商品数量)
    category_product_counts = {}
    for category in all_categories:
        count = r_client.scard(f"category:{category}:products")
        category_product_counts[category] = count
    results['top_categories'] = sorted(category_product_counts.items(), key=lambda item: item[1], reverse=True)[:5]

    # 价格最高的 N 个商品
    top_price_product_ids = r_client.zrevrange("product:prices", 0, 4)
    pipe = r_client.pipeline()
    for pid in top_price_product_ids:
        pipe.hgetall(f"product:{pid}")
    top_products_details = pipe.execute()
    results['top_priced_products'] = []
    for details in top_products_details:
        if details:
            results['top_priced_products'].append({
                'name': details.get('name', 'N/A'),
                'price': details.get('price', 'N/A')
            })

    # 低库存预警
    low_stock_products = []
    pipe = r_client.pipeline()
    for pid in all_product_ids:
        pipe.hget(f"product:{pid}", "stock")
    stocks = pipe.execute()

    for i, stock_str in enumerate(stocks):
        stock = int(stock_str) if stock_str else 0
        if stock < 10:
            product_id = all_product_ids[i]
            product_name = r_client.hget(f"product:{product_id}", "name")
            low_stock_products.append({'id': product_id, 'name': product_name, 'stock': stock})
    results['low_stock_products'] = low_stock_products

    # 最近登录用户
    all_user_ids = list(r_client.smembers("user:all_ids"))
    results['total_users'] = len(all_user_ids)
    user_logins = []
    pipe = r_client.pipeline()
    for uid in all_user_ids:
        pipe.hgetall(f"user:{uid}")
    user_details_list = pipe.execute()

    for details in user_details_list:
        if details and 'last_login' in details and 'username' in details:
            user_logins.append({'username': details['username'], 'last_login': details['last_login']})

    user_logins_sorted = sorted([u for u in user_logins if u['last_login']], key=lambda x: x['last_login'], reverse=True)
    results['recent_logins'] = user_logins_sorted[:5]

    # 新增：热门销售商品 (基于 order_item 数量)
    product_sales_counts = {}
    pipe = r_client.pipeline()
    for pid in all_product_ids:
        pipe.llen(f"product:{pid}:sales")
    sales_lengths = pipe.execute()

    for i, length in enumerate(sales_lengths):
        product_sales_counts[all_product_ids[i]] = length

    sorted_sales = sorted(product_sales_counts.items(), key=lambda item: item[1], reverse=True)[:5]
    results['top_selling_products'] = []
    for pid, count in sorted_sales:
        product_name = r_client.hget(f"product:{pid}", "name")
        results['top_selling_products'].append({'name': product_name, 'sales_count': count})


    return results

# --- 任务四：数据优化与应用 (保留部分用于演示，但主要通过GUI交互) ---

def get_product_details_cached(r_client, product_id):
    """从 Redis 获取商品详情，并利用 Redis 缓存。"""
    if not r_client:
        return None

    cache_key = f"cache:product_details:{product_id}"
    cached_data = r_client.get(cache_key)

    if cached_data:
        return json.loads(cached_data)
    else:
        details = r_client.hgetall(f"product:{product_id}")
        if details:
            r_client.setex(cache_key, 60, json.dumps(details)) # 缓存 60 秒
        return details

def get_order_details_with_items(r_client, order_id):
    """获取订单详情，包括其所有商品明细"""
    if not r_client:
        return None, None

    order_overview = r_client.hgetall(f"order:{order_id}")
    if not order_overview:
        return None, None

    item_ids = r_client.lrange(f"order:{order_id}:items", 0, -1)
    order_items = []
    if item_ids:
        pipe = r_client.pipeline()
        for item_id in item_ids:
            pipe.hgetall(f"order_item:{item_id}")
        item_details_list = pipe.execute()
        for item_detail in item_details_list:
            if item_detail:
                order_items.append(item_detail)
    
    return order_overview, order_items

def update_product_stock(r_client, product_id, change_amount):
    """更新商品库存"""
    if not r_client:
        return None

    current_stock = r_client.hget(f"product:{product_id}", "stock")
    if current_stock is None:
        return None # 商品不存在

    current_stock = int(current_stock)
    new_stock = r_client.hincrby(f"product:{product_id}", "stock", change_amount)
    return new_stock