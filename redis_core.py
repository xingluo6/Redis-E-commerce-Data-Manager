import redis
import pandas as pd
import json
import time
import random
from faker import Faker
import uuid
import os
import math # 用于分页
from datetime import datetime # 用于用户添加时的日期格式

# --- 配置 ---
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
REDIS_DB = 0
FAKE_PRODUCT_COUNT = 1000
FAKE_USER_COUNT = 100
FAKE_ORDER_COUNT = 500
ONLINE_RETAIL_DATA_PATH = 'data.csv'

# --- Redis 客户端管理 ---
_redis_client_instance = None

def get_redis_client():
    """
    获取 Redis 客户端实例。如果尚未创建，则尝试创建并连接。
    """
    global _redis_client_instance
    if _redis_client_instance is None:
        try:
            _redis_client_instance = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)
            _redis_client_instance.ping()
            print("Redis client connected successfully.")
        except redis.exceptions.ConnectionError as e:
            print(f"Failed to connect to Redis server: {e}")
            _redis_client_instance = None # 连接失败，重置为 None
    return _redis_client_instance

# --- Faker 初始化 ---
fake = Faker('zh_CN')

# --- 数据生成与清洗 (Faker) ---

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

def collect_and_clean_faker_data():
    """
    模拟数据收集和清洗过程（使用 Faker 生成数据）。
    """
    products_df = pd.DataFrame(generate_synthetic_product_data(FAKE_PRODUCT_COUNT))
    users_df = pd.DataFrame(generate_synthetic_user_data(FAKE_USER_COUNT))

    product_ids_list = products_df['product_id'].tolist()
    user_ids_list = users_df['user_id'].tolist()
    orders_df = pd.DataFrame(generate_synthetic_order_data(product_ids_list, user_ids_list, FAKE_ORDER_COUNT))

    # 数据清洗示例
    products_df.loc[:, 'description'] = products_df['description'].fillna('暂无描述')
    products_df.loc[:, 'price'] = pd.to_numeric(products_df['price'], errors='coerce')
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

# --- 数据生成与清洗 (Online Retail) ---

def load_and_clean_online_retail_data(fake_fill_missing=True):
    """
    从 Online Retail 数据集 (data.csv) 加载、清洗并转换为 products_df, users_df, orders_df。
    """
    if not os.path.exists(ONLINE_RETAIL_DATA_PATH):
        raise FileNotFoundError(f"未找到 {ONLINE_RETAIL_DATA_PATH} 文件。请确保已下载并放置在项目根目录。")

    df = pd.read_csv(ONLINE_RETAIL_DATA_PATH, encoding='ISO-8859-1')

    # --- 数据清洗 ---
    df.dropna(subset=['CustomerID'], inplace=True)
    df['CustomerID'] = df['CustomerID'].astype(int).astype(str)

    df.dropna(subset=['Description'], inplace=True)

    df = df[df['Quantity'] > 0]
    df = df[df['UnitPrice'] > 0]

    df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'])

    # --- 提取 Products DataFrame ---
    products_df = df[['StockCode', 'Description', 'UnitPrice']].copy()
    products_df.rename(columns={'StockCode': 'product_id', 'Description': 'name'}, inplace=True)
    products_df = products_df.groupby(['product_id', 'name'])['UnitPrice'].mean().reset_index()
    products_df.rename(columns={'UnitPrice': 'price'}, inplace=True)
    
    # 补充缺失字段
    if fake_fill_missing:
        products_df['description'] = products_df['name'].apply(lambda x: fake.text(max_nb_chars=100))
        categories = ['电子产品', '服装鞋帽', '家居百货', '图书音像', '美妆个护', '食品饮料']
        products_df['category'] = products_df['product_id'].apply(lambda x: random.choice(categories))
        products_df['stock'] = products_df['product_id'].apply(lambda x: random.randint(50, 500))
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


    # --- 提取 Orders DataFrame (多商品订单处理) ---
    orders_grouped = df.groupby('InvoiceNo')

    orders_data = []
    for invoice_no, group in orders_grouped:
        if 'C' in str(invoice_no): # 排除退货订单
            continue

        order_id = str(invoice_no)
        user_id = group['CustomerID'].iloc[0]
        order_date = group['InvoiceDate'].iloc[0].isoformat()
        total_amount = (group['Quantity'] * group['UnitPrice']).sum()
        country = group['Country'].iloc[0]

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

# --- 数据存储与管理 (Redis) ---

def store_data_in_redis(products_df, users_df, orders_df, flush_db=True):
    """
    将清洗后的数据存储到 Redis。
    """
    r_client = get_redis_client()
    if not r_client:
        return "Redis 连接失败，无法存储数据。", False

    if flush_db:
        r_client.flushdb()

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

    # 3. 存储订单数据
    pipe = r_client.pipeline()
    for index, row in orders_df.iterrows():
        order_id = row['order_id']
        user_id = row['user_id']
        
        order_details = row.drop('items').to_dict()
        for key, value in order_details.items():
            pipe.hset(f"order:{order_id}", key, str(value))
        
        for item_idx, item in enumerate(row['items']):
            item_id = f"{order_id}:{item_idx}"
            for key, value in item.items():
                pipe.hset(f"order_item:{item_id}", key, str(value))
            pipe.rpush(f"order:{order_id}:items", item_id)
            pipe.lpush(f"product:{item['StockCode']}:sales", item_id)

        pipe.lpush(f"user:{user_id}:orders", order_id)
        pipe.sadd("order:all_ids", order_id)
    pipe.execute()
    return "数据存储完成。", True

def flush_redis_db():
    """清空 Redis 数据库"""
    r_client = get_redis_client()
    if not r_client:
        return "Redis 连接失败，无法清空数据库。", False
    try:
        r_client.flushdb()
        return "Redis 数据库已清空。", True
    except Exception as e:
        return f"清空 Redis 数据库失败: {e}", False

# --- 数据查询 (添加分页和搜索功能) ---

def get_all_products(page=1, page_size=20, search_query=""):
    r_client = get_redis_client()
    if not r_client: return [], 0 # 返回空列表和总数0

    all_product_ids = list(r_client.smembers("product:all_ids"))
    
    # 获取所有商品的完整详情，以便在 Python 端进行过滤
    all_products_details = []
    if all_product_ids:
        pipe = r_client.pipeline()
        for pid in all_product_ids:
            pipe.hgetall(f"product:{pid}")
        fetched_details = pipe.execute()
        for details in fetched_details:
            if details:
                all_products_details.append(details)

    # 在 Python 端进行搜索过滤
    filtered_products = []
    if search_query:
        search_query_lower = search_query.lower()
        for p in all_products_details:
            # 搜索名称、描述、分类
            if (search_query_lower in p.get('name', '').lower() or
                search_query_lower in p.get('description', '').lower() or
                search_query_lower in p.get('category', '').lower()):
                filtered_products.append(p)
    else:
        filtered_products = all_products_details

    total_items = len(filtered_products)

    # 根据过滤后的结果进行分页
    start_index = (page - 1) * page_size
    end_index = start_index + page_size
    
    current_page_products = filtered_products[start_index:end_index]

    products_formatted = []
    for details in current_page_products:
        if details:
            details['price'] = float(details.get('price', 0))
            details['stock'] = int(details.get('stock', 0))
            products_formatted.append(details)
            
    return products_formatted, total_items # 返回当前页数据和总数

def get_product_details(product_id):
    r_client = get_redis_client()
    if not r_client: return None
    cache_key = f"cache:product_details:{product_id}"
    cached_data = r_client.get(cache_key)
    if cached_data:
        return json.loads(cached_data)
    else:
        details = r_client.hgetall(f"product:{product_id}")
        if details:
            r_client.setex(cache_key, 60, json.dumps(details))
        return details

# --- CRUD: 商品 ---
def add_product(product_data):
    r_client = get_redis_client()
    if not r_client: return "Redis 连接失败。", False
    product_id = str(uuid.uuid4())
    try:
        pipe = r_client.pipeline()
        for key, value in product_data.items():
            pipe.hset(f"product:{product_id}", key, str(value))
        pipe.hset(f"product:{product_id}", "product_id", product_id) # 确保ID也存储
        pipe.sadd("product:all_ids", product_id)
        pipe.sadd(f"category:{product_data['category']}:products", product_id)
        pipe.zadd("product:prices", {product_id: float(product_data['price'])})
        pipe.execute()
        return "商品添加成功。", True
    except Exception as e:
        return f"商品添加失败: {e}", False

def update_product_details(product_id, data):
    r_client = get_redis_client()
    if not r_client: return "Redis 连接失败。", False
    try:
        pipe = r_client.pipeline()
        # 获取旧的分类，如果分类改变，需要更新 category:*:products set
        old_category = r_client.hget(f"product:{product_id}", "category")

        for key, value in data.items():
            pipe.hset(f"product:{product_id}", key, str(value))
        
        if 'price' in data:
            pipe.zadd("product:prices", {product_id: float(data['price'])})
        
        if 'category' in data and old_category and old_category != data['category']:
            pipe.srem(f"category:{old_category}:products", product_id)
            pipe.sadd(f"category:{data['category']}:products", product_id)

        pipe.execute()
        # 清除缓存
        r_client.delete(f"cache:product_details:{product_id}")
        return "商品更新成功。", True
    except Exception as e:
        return f"更新商品失败: {e}", False

def delete_product(product_id):
    r_client = get_redis_client()
    if not r_client: return "Redis 连接失败。", False
    try:
        # 获取商品信息以便清理相关数据
        product_details = r_client.hgetall(f"product:{product_id}")
        if not product_details:
            return "商品不存在。", False
        
        category = product_details.get('category')

        pipe = r_client.pipeline()
        pipe.delete(f"product:{product_id}") # 删除商品详情Hash
        pipe.srem("product:all_ids", product_id) # 从所有商品ID集合中移除
        pipe.zrem("product:prices", product_id) # 从价格Sorted Set中移除
        if category:
            pipe.srem(f"category:{category}:products", product_id) # 从分类Set中移除

        # 还需要清理与该商品相关的订单项和销售记录
        # 这是一个复杂的操作，因为 product:{pid}:sales 存储的是 order_item_id
        # 简化处理：删除该商品的销售记录列表
        pipe.delete(f"product:{product_id}:sales")

        pipe.execute()
        return "商品删除成功。", True
    except Exception as e:
        return f"商品删除失败: {e}", False


def get_product_categories():
    """获取所有商品分类列表"""
    r_client = get_redis_client()
    if not r_client: return []

    # 获取所有以 "category:" 开头且以 ":products" 结尾的 key
    category_keys = r_client.keys("category:*:products")
    categories = []
    for key in category_keys:
        category_name = key.split(':')[1]
        categories.append(category_name)
    return sorted(list(set(categories)))  # 返回去重并排序后的分类列表


# --- CRUD: 用户 ---
def get_all_users(page=1, page_size=20, search_query=""):
    r_client = get_redis_client()
    if not r_client: return [], 0

    all_user_ids = list(r_client.smembers("user:all_ids"))
    
    all_users_details = []
    if all_user_ids:
        pipe = r_client.pipeline()
        for uid in all_user_ids:
            pipe.hgetall(f"user:{uid}")
        fetched_details = pipe.execute()
        for details in fetched_details:
            if details:
                all_users_details.append(details)

    filtered_users = []
    if search_query:
        search_query_lower = search_query.lower()
        for u in all_users_details:
            # 搜索用户名、邮箱
            if (search_query_lower in u.get('username', '').lower() or
                search_query_lower in u.get('email', '').lower()):
                filtered_users.append(u)
    else:
        filtered_users = all_users_details

    total_items = len(filtered_users)

    start_index = (page - 1) * page_size
    end_index = start_index + page_size
    
    current_page_users = filtered_users[start_index:end_index]

    users_formatted = []
    for details in current_page_users:
        users_formatted.append(details)

    return users_formatted, total_items

def get_user_details(user_id):
    r_client = get_redis_client()
    if not r_client: return None, []
    user = r_client.hgetall(f"user:{user_id}")
    user_orders = r_client.lrange(f"user:{user_id}:orders", 0, -1)
    return user, user_orders

def add_user(user_data):
    r_client = get_redis_client()
    if not r_client: return "Redis 连接失败。", False
    user_id = str(uuid.uuid4())
    try:
        pipe = r_client.pipeline()
        for key, value in user_data.items():
            pipe.hset(f"user:{user_id}", key, str(value))
        pipe.hset(f"user:{user_id}", "user_id", user_id) # 确保ID也存储
        pipe.sadd("user:all_ids", user_id)
        pipe.execute()
        return "用户添加成功。", True
    except Exception as e:
        return f"用户添加失败: {e}", False

def update_user_details(user_id, data):
    r_client = get_redis_client()
    if not r_client: return "Redis 连接失败。", False
    try:
        pipe = r_client.pipeline()
        for key, value in data.items():
            pipe.hset(f"user:{user_id}", key, str(value))
        pipe.execute()
        return "用户更新成功。", True
    except Exception as e:
        return f"用户更新失败: {e}", False

def delete_user(user_id):
    r_client = get_redis_client()
    if not r_client: return "Redis 连接失败。", False
    try:
        pipe = r_client.pipeline()
        pipe.delete(f"user:{user_id}") # 删除用户详情Hash
        pipe.srem("user:all_ids", user_id) # 从所有用户ID集合中移除
        pipe.delete(f"user:{user_id}:orders") # 删除用户订单历史List
        # 注意：这里没有删除用户创建的订单本身，这通常需要更复杂的业务逻辑来处理级联删除或标记
        pipe.execute()
        return "用户删除成功。", True
    except Exception as e:
        return f"用户删除失败: {e}", False

# --- CRUD: 订单 ---
def get_all_orders(page=1, page_size=20, search_query=""):
    r_client = get_redis_client()
    if not r_client: return [], 0

    all_order_ids = list(r_client.smembers("order:all_ids"))
    
    all_orders_details = []
    if all_order_ids:
        pipe = r_client.pipeline()
        for oid in all_order_ids:
            pipe.hgetall(f"order:{oid}")
        fetched_details = pipe.execute()
        for details in fetched_details:
            if details:
                all_orders_details.append(details)

    filtered_orders = []
    if search_query:
        search_query_lower = search_query.lower()
        for o in all_orders_details:
            # 搜索订单ID、用户ID、国家、状态
            if (search_query_lower in o.get('order_id', '').lower() or
                search_query_lower in o.get('user_id', '').lower() or
                search_query_lower in o.get('country', '').lower() or
                search_query_lower in o.get('status', '').lower()):
                filtered_orders.append(o)
    else:
        filtered_orders = all_orders_details

    total_items = len(filtered_orders)

    start_index = (page - 1) * page_size
    end_index = start_index + page_size
    
    current_page_orders = filtered_orders[start_index:end_index]

    orders_formatted = []
    for details in current_page_orders:
        if details:
            details['total_amount'] = float(details.get('total_amount', 0))
            orders_formatted.append(details)
    return orders_formatted, total_items

def get_order_details_with_items(order_id):
    r_client = get_redis_client()
    if not r_client: return None, None
    order_overview = r_client.hgetall(f"order:{order_id}")
    if not order_overview: return None, None

    item_ids = r_client.lrange(f"order:{order_id}:items", 0, -1)
    order_items = []
    if item_ids:
        pipe = r_client.pipeline()
        for item_id in item_ids:
            pipe.hgetall(f"order_item:{item_id}")
        item_details_list = pipe.execute()
        for item_detail in item_details_list:
            if item_detail:
                item_detail['Quantity'] = int(item_detail.get('Quantity', 0))
                item_detail['UnitPrice'] = float(item_detail.get('UnitPrice', 0))
                item_detail['total_price'] = item_detail['Quantity'] * item_detail['UnitPrice']
                order_items.append(item_detail)
    
    return order_overview, order_items

def update_order_status(order_id, new_status):
    r_client = get_redis_client()
    if not r_client: return "Redis 连接失败。", False
    try:
        r_client.hset(f"order:{order_id}", "status", new_status)
        return "订单状态更新成功。", True
    except Exception as e:
        return f"订单状态更新失败: {e}", False

def delete_order(order_id):
    r_client = get_redis_client()
    if not r_client: return "Redis 连接失败。", False
    try:
        # 获取订单信息以便清理相关数据
        order_details = r_client.hgetall(f"order:{order_id}")
        if not order_details:
            return "订单不存在。", False
        
        user_id = order_details.get('user_id')
        item_ids = r_client.lrange(f"order:{order_id}:items", 0, -1)

        pipe = r_client.pipeline()
        pipe.delete(f"order:{order_id}") # 删除订单详情Hash
        pipe.srem("order:all_ids", order_id) # 从所有订单ID集合中移除
        if user_id:
            pipe.lrem(f"user:{user_id}:orders", 0, order_id) # 从用户订单历史中移除

        # 删除所有订单项
        if item_ids:
            for item_id in item_ids:
                pipe.delete(f"order_item:{item_id}")
                # 还需要从 product:{StockCode}:sales 列表中移除对应的 item_id
                # 这个操作比较复杂，因为需要知道 item_id 对应的 StockCode
                # 简化处理：不从 product:{StockCode}:sales 中移除，因为LLEN只用于统计
                # 实际生产中，可能需要维护一个 set 记录每个商品包含哪些订单项，以便快速移除
            pipe.delete(f"order:{order_id}:items") # 删除订单项列表

        pipe.execute()
        return "订单删除成功。", True
    except Exception as e:
        return f"订单删除失败: {e}", False

# --- 数据分析 ---

def analyze_data_from_redis():
    """
    从 Redis 中获取数据并进行分析，返回结果字典。
    """
    r_client = get_redis_client()
    if not r_client:
        return {"error": "Redis 连接失败，无法进行分析。"}

    results = {}

    all_product_ids = list(r_client.smembers("product:all_ids"))
    results['total_products'] = len(all_product_ids)

    all_categories = set()
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

    # 热门销售商品 (基于 order_item 数量)
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

    # 月销售额趋势
    all_order_ids = list(r_client.smembers("order:all_ids"))
    monthly_sales = {} # { "YYYY-MM": total_amount }

    if all_order_ids:
        pipe = r_client.pipeline()
        for oid in all_order_ids:
            pipe.hgetall(f"order:{oid}")
        all_orders_details = pipe.execute()

        for order_detail in all_orders_details:
            if order_detail and 'order_date' in order_detail and 'total_amount' in order_detail:
                try:
                    order_date = datetime.fromisoformat(order_detail['order_date'])
                    month_key = order_date.strftime('%Y-%m')
                    amount = float(order_detail['total_amount'])
                    monthly_sales[month_key] = monthly_sales.get(month_key, 0.0) + amount
                except (ValueError, TypeError):
                    # 忽略无效日期或金额
                    pass
    
    # 按月份排序
    sorted_monthly_sales = sorted(monthly_sales.items())
    results['monthly_sales_trend'] = sorted_monthly_sales

    return results