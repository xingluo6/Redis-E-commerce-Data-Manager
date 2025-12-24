from flask import Flask, render_template, request, redirect, url_for, flash, g
import redis
import json
import pandas as pd # 仅用于类型检查，实际数据处理在 redis_data_manager 中
import time # 用于模拟耗时操作或计时

# 导入你的数据管理模块
import redis_data_manager as rdm

app = Flask(__name__)
app.secret_key = 'your_secret_key_for_flash_messages' # 用于 flash 消息，生产环境请使用更复杂的密钥

# Redis 连接配置
REDIS_HOST = rdm.REDIS_HOST
REDIS_PORT = rdm.REDIS_PORT
REDIS_DB = rdm.REDIS_DB

# 在每个请求之前连接 Redis
@app.before_request
def before_request():
    if not hasattr(g, 'redis_db'):
        g.redis_db = rdm.get_redis_client()
        if not g.redis_db:
            flash("无法连接到 Redis 服务器，请检查配置和服务器状态。", "danger")

# 在每个请求之后关闭 Redis 连接 (如果使用连接池则不需要关闭)
@app.teardown_request
def teardown_request(exception):
    redis_db = getattr(g, 'redis_db', None)
    # 在 StrictRedis 中，通常不需要显式关闭，连接会在请求结束后自动释放
    # 如果你使用的是连接池，则连接会被返回到池中
    # pass


# --- 路由定义 ---

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/data_management', methods=['GET', 'POST'])
def data_management():
    if request.method == 'POST':
        if not g.redis_db:
            flash("Redis 连接失败，无法执行操作。", "danger")
            return redirect(url_for('data_management'))

        action = request.form.get('action')
        message = ""
        success = False

        if action == 'generate_and_store_faker': # 修改为Faker数据生成
            try:
                products_df, users_df, orders_df = rdm.collect_and_clean_data()
                if not products_df.empty and not users_df.empty and not orders_df.empty:
                    message, success = rdm.store_data_in_redis(g.redis_db, products_df, users_df, orders_df, flush_db=True)
                else:
                    message = "Faker 数据生成或清洗后为空，未存储到 Redis。"
                    success = False
            except Exception as e:
                message = f"Faker 数据生成或存储过程中发生错误: {e}"
                success = False
        elif action == 'load_and_store_online_retail': # 新增Online Retail数据加载
            try:
                # 获取复选框状态
                fake_fill_missing = request.form.get('fake_fill_missing') == 'true'
                products_df, users_df, orders_df = rdm.load_online_retail_data_to_dfs(fake_fill_missing=fake_fill_missing)
                if not products_df.empty and not users_df.empty and not orders_df.empty:
                    message, success = rdm.store_data_in_redis(g.redis_db, products_df, users_df, orders_df, flush_db=True)
                else:
                    message = "Online Retail 数据加载或清洗后为空，未存储到 Redis。"
                    success = False
            except FileNotFoundError as e:
                message = f"错误：{e} 请确保 data.csv 文件存在。"
                success = False
            except Exception as e:
                message = f"Online Retail 数据加载或存储过程中发生错误: {e}"
                success = False
        elif action == 'flush_db':
            try:
                g.redis_db.flushdb()
                message = "Redis 数据库已清空。"
                success = True
            except Exception as e:
                message = f"清空 Redis 数据库失败: {e}"
                success = False
        else:
            message = "未知操作。"
            success = False

        flash(message, "success" if success else "danger")
        return redirect(url_for('data_management'))

    return render_template('data_management.html')

@app.route('/products')
def list_products():
    if not g.redis_db:
        return render_template('products.html', products=[], error="无法连接到 Redis。")

    all_product_ids = list(g.redis_db.smembers("product:all_ids"))
    products = []
    if all_product_ids:
        pipe = g.redis_db.pipeline()
        for pid in all_product_ids:
            pipe.hgetall(f"product:{pid}")
        product_details_list = pipe.execute()

        for details in product_details_list:
            if details:
                # 确保价格和库存是数字类型，方便显示和排序
                details['price'] = float(details.get('price', 0))
                details['stock'] = int(details.get('stock', 0))
                products.append(details)
    return render_template('products.html', products=products)

@app.route('/product/<product_id>')
def product_detail(product_id):
    if not g.redis_db:
        flash("Redis 连接失败。", "danger")
        return redirect(url_for('list_products'))

    product = rdm.get_product_details_cached(g.redis_db, product_id)
    if product:
        product['price'] = float(product.get('price', 0))
        product['stock'] = int(product.get('stock', 0))
        return render_template('product_detail.html', product=product)
    flash("商品未找到。", "warning")
    return redirect(url_for('list_products'))

@app.route('/product/edit/<product_id>', methods=['GET', 'POST'])
def edit_product(product_id):
    if not g.redis_db:
        flash("Redis 连接失败。", "danger")
        return redirect(url_for('list_products'))

    if request.method == 'POST':
        try:
            new_name = request.form['name']
            new_description = request.form['description']
            new_category = request.form['category']
            new_price = float(request.form['price'])
            new_stock = int(request.form['stock'])

            pipe = g.redis_db.pipeline()
            pipe.hset(f"product:{product_id}", "name", new_name)
            pipe.hset(f"product:{product_id}", "description", new_description)
            pipe.hset(f"product:{product_id}", "category", new_category)
            pipe.hset(f"product:{product_id}", "price", str(new_price)) # Redis存储为字符串
            pipe.hset(f"product:{product_id}", "stock", str(new_stock)) # Redis存储为字符串
            pipe.zadd("product:prices", {product_id: new_price}) # 更新Sorted Set中的价格
            pipe.execute()

            flash(f"商品 {product_id} 更新成功！", "success")
            return redirect(url_for('product_detail', product_id=product_id))
        except ValueError:
            flash("价格或库存输入无效，请输入数字。", "danger")
        except Exception as e:
            flash(f"更新商品失败: {e}", "danger")
        return redirect(url_for('edit_product', product_id=product_id))
    else:
        product = g.redis_db.hgetall(f"product:{product_id}")
        if product:
            product['price'] = float(product.get('price', 0))
            product['stock'] = int(product.get('stock', 0))
            return render_template('edit_product.html', product=product)
        flash("商品未找到。", "warning")
        return redirect(url_for('list_products'))

@app.route('/users')
def list_users():
    if not g.redis_db:
        return render_template('users.html', users=[], error="无法连接到 Redis。")

    all_user_ids = list(g.redis_db.smembers("user:all_ids"))
    users = []
    if all_user_ids:
        pipe = g.redis_db.pipeline()
        for uid in all_user_ids:
            pipe.hgetall(f"user:{uid}")
        user_details_list = pipe.execute()

        for details in user_details_list:
            if details:
                users.append(details)
    return render_template('users.html', users=users)

@app.route('/user/<user_id>')
def user_detail(user_id):
    if not g.redis_db:
        flash("Redis 连接失败。", "danger")
        return redirect(url_for('list_users'))

    user = g.redis_db.hgetall(f"user:{user_id}")
    if user:
        # 获取用户的订单历史
        user_orders = g.redis_db.lrange(f"user:{user_id}:orders", 0, -1)
        # 订单ID可能来自 Online Retail 的 InvoiceNo，或者 Faker 的 UUID
        # 为了显示友好，我们只显示前8位
        display_orders = [{'id': order_id, 'display_id': order_id[:8] + '...' if len(order_id) > 10 else order_id} for order_id in user_orders]
        return render_template('user_detail.html', user=user, user_orders=display_orders)
    flash("用户未找到。", "warning")
    return redirect(url_for('list_users'))

@app.route('/orders')
def list_orders():
    if not g.redis_db:
        return render_template('orders.html', orders=[], error="无法连接到 Redis。")

    # 获取所有订单ID (现在我们维护了 order:all_ids)
    all_order_ids = list(g.redis_db.smembers("order:all_ids"))

    orders = []
    if all_order_ids:
        pipe = g.redis_db.pipeline()
        for oid in all_order_ids:
            pipe.hgetall(f"order:{oid}")
        order_details_list = pipe.execute()

        for details in order_details_list:
            if details:
                # 确保数字类型转换
                details['total_amount'] = float(details.get('total_amount', 0))
                # 订单可能没有 quantity 和 unit_price 字段，因为 Online Retail 是多商品订单
                # 这里只显示订单总览信息
                orders.append(details)
    return render_template('orders.html', orders=orders)

@app.route('/order/<order_id>')
def order_detail(order_id):
    if not g.redis_db:
        flash("Redis 连接失败。", "danger")
        return redirect(url_for('list_orders'))

    order, order_items = rdm.get_order_details_with_items(g.redis_db, order_id)
    if order:
        order['total_amount'] = float(order.get('total_amount', 0))
        # 获取商品和用户详情
        user_id = order.get('user_id')
        username = g.redis_db.hget(f"user:{user_id}", "username") if user_id else "N/A"

        # 处理订单项，确保数字类型
        for item in order_items:
            item['Quantity'] = int(item.get('Quantity', 0))
            item['UnitPrice'] = float(item.get('UnitPrice', 0))
            item['total_price'] = item['Quantity'] * item['UnitPrice'] # 计算单项总价

        return render_template('order_detail.html', order=order, username=username, order_items=order_items)
    flash("订单未找到。", "warning")
    return redirect(url_for('list_orders'))

@app.route('/analysis')
def analysis():
    if not g.redis_db:
        return render_template('analysis.html', analysis_results={"error": "无法连接到 Redis。"})

    analysis_results = rdm.analyze_data_from_redis(g.redis_db)
    return render_template('analysis.html', analysis_results=analysis_results)

if __name__ == '__main__':
    app.run(debug=True)