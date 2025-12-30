from flask import Flask, render_template, request, redirect, url_for, flash, g
import redis_core as rc # 导入核心逻辑模块
import json
import pandas as pd
import plotly.graph_objects as go
from plotly.offline import plot
import plotly.express as px
from datetime import datetime
import math # <-- 修复：添加导入 math 模块

app = Flask(__name__)
app.secret_key = 'your_web_secret_key_for_flash_messages' # 用于 flash 消息，生产环境请使用更复杂的密钥

# 在每个请求之前连接 Redis
@app.before_request
def before_request():
    g.redis_db = rc.get_redis_client()
    if not g.redis_db:
        flash("无法连接到 Redis 服务器，请检查配置和服务器状态。", "danger")

# --- 辅助函数：处理分页和搜索参数 ---
def get_pagination_params(request):
    page = request.args.get('page', 1, type=int)
    page_size = request.args.get('page_size', 20, type=int)
    search_query = request.args.get('search_query', '', type=str)
    
    # 确保 page_size 在合理范围内
    if not (10 <= page_size <= 100): # 限制每页大小
        page_size = 20
    
    return page, page_size, search_query

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

        if action == 'generate_and_store_faker':
            try:
                products_df, users_df, orders_df = rc.collect_and_clean_faker_data()
                if not products_df.empty and not users_df.empty and not orders_df.empty:
                    message, success = rc.store_data_in_redis(products_df, users_df, orders_df, flush_db=True)
                else:
                    message = "Faker 数据生成或清洗后为空，未存储到 Redis。"
                    success = False
            except Exception as e:
                message = f"Faker 数据生成或存储过程中发生错误: {e}"
                success = False
        elif action == 'load_and_store_online_retail':
            try:
                fake_fill_missing = request.form.get('fake_fill_missing') == 'true'
                products_df, users_df, orders_df = rc.load_and_clean_online_retail_data(fake_fill_missing=fake_fill_missing)
                if not products_df.empty and not users_df.empty and not orders_df.empty:
                    message, success = rc.store_data_in_redis(products_df, users_df, orders_df, flush_db=True)
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
            message, success = rc.flush_redis_db()
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
    
    page, page_size, search_query = get_pagination_params(request)
    products, total_items = rc.get_all_products(page, page_size, search_query)
    
    total_pages = math.ceil(total_items / page_size) if total_items > 0 else 1

    return render_template('products.html', 
                           products=products, 
                           current_page=page, 
                           total_pages=total_pages, 
                           page_size=page_size, 
                           search_query=search_query)

@app.route('/product/<product_id>')
def product_detail(product_id):
    if not g.redis_db:
        flash("Redis 连接失败。", "danger")
        return redirect(url_for('list_products'))
    product = rc.get_product_details(product_id)
    if product:
        product['price'] = float(product.get('price', 0))
        product['stock'] = int(product.get('stock', 0))
        return render_template('product_detail.html', product=product)
    flash("商品未找到。", "warning")
    return redirect(url_for('list_products'))

@app.route('/product/add', methods=['GET', 'POST'])
def add_product():
    if not g.redis_db:
        flash("Redis 连接失败。", "danger")
        return redirect(url_for('list_products'))
    
    categories = rc.get_product_categories() # 获取所有分类用于下拉选择

    if request.method == 'POST':
        try:
            product_data = {
                "name": request.form['name'],
                "description": request.form['description'],
                "category": request.form['category'],
                "price": float(request.form['price']),
                "stock": int(request.form['stock']),
                "created_at": datetime.now().isoformat()
            }
            message, success = rc.add_product(product_data)
            flash(message, "success" if success else "danger")
            if success:
                return redirect(url_for('list_products'))
        except ValueError:
            flash("价格或库存输入无效，请输入数字。", "danger")
        except Exception as e:
            flash(f"添加商品失败: {e}", "danger")
        return render_template('add_edit_product.html', title="添加商品", categories=categories)
    
    return render_template('add_edit_product.html', title="添加商品", categories=categories)


@app.route('/product/edit/<product_id>', methods=['GET', 'POST'])
def edit_product(product_id):
    if not g.redis_db:
        flash("Redis 连接失败。", "danger")
        return redirect(url_for('list_products'))
    
    categories = rc.get_product_categories() # 获取所有分类用于下拉选择

    product = rc.get_product_details(product_id)
    if not product:
        flash("商品未找到。", "warning")
        return redirect(url_for('list_products'))

    if request.method == 'POST':
        try:
            updated_data = {
                "name": request.form['name'],
                "description": request.form['description'],
                "category": request.form['category'],
                "price": float(request.form['price']),
                "stock": int(request.form['stock'])
            }
            message, success = rc.update_product_details(product_id, updated_data)
            flash(message, "success" if success else "danger")
            if success:
                return redirect(url_for('product_detail', product_id=product_id))
        except ValueError:
            flash("价格或库存输入无效，请输入数字。", "danger")
        except Exception as e:
            flash(f"更新商品失败: {e}", "danger")
        return render_template('add_edit_product.html', title="编辑商品", product=product, categories=categories)
    
    return render_template('add_edit_product.html', title="编辑商品", product=product, categories=categories)

@app.route('/product/delete/<product_id>', methods=['POST'])
def delete_product(product_id):
    if not g.redis_db:
        flash("Redis 连接失败。", "danger")
        return redirect(url_for('list_products'))
    
    message, success = rc.delete_product(product_id)
    flash(message, "success" if success else "danger")
    return redirect(url_for('list_products'))


@app.route('/users')
def list_users():
    if not g.redis_db:
        return render_template('users.html', users=[], error="无法连接到 Redis。")
    
    page, page_size, search_query = get_pagination_params(request)
    users, total_items = rc.get_all_users(page, page_size, search_query)

    total_pages = math.ceil(total_items / page_size) if total_items > 0 else 1

    return render_template('users.html', 
                           users=users, 
                           current_page=page, 
                           total_pages=total_pages, 
                           page_size=page_size, 
                           search_query=search_query)

@app.route('/user/<user_id>')
def user_detail(user_id):
    if not g.redis_db:
        flash("Redis 连接失败。", "danger")
        return redirect(url_for('list_users'))

    user, user_orders = rc.get_user_details(user_id)
    if user:
        # 订单ID可能来自 Online Retail 的 InvoiceNo，或者 Faker 的 UUID
        # 为了显示友好，我们只显示前8位
        display_orders = [{'id': order_id, 'display_id': order_id[:8] + '...' if len(order_id) > 10 else order_id} for order_id in user_orders]
        return render_template('user_detail.html', user=user, user_orders=display_orders)
    flash("用户未找到。", "warning")
    return redirect(url_for('list_users'))

@app.route('/user/add', methods=['GET', 'POST'])
def add_user():
    if not g.redis_db:
        flash("Redis 连接失败。", "danger")
        return redirect(url_for('list_users'))
    
    if request.method == 'POST':
        try:
            user_data = {
                "username": request.form['username'],
                "email": request.form['email'],
                "registration_date": datetime.now().isoformat(),
                "last_login": datetime.now().isoformat()
            }
            message, success = rc.add_user(user_data)
            flash(message, "success" if success else "danger")
            if success:
                return redirect(url_for('list_users'))
        except Exception as e:
            flash(f"添加用户失败: {e}", "danger")
        return render_template('add_edit_user.html', title="添加用户")
    
    return render_template('add_edit_user.html', title="添加用户")

@app.route('/user/edit/<user_id>', methods=['GET', 'POST'])
def edit_user(user_id):
    if not g.redis_db:
        flash("Redis 连接失败。", "danger")
        return redirect(url_for('list_users'))
    
    user, _ = rc.get_user_details(user_id)
    if not user:
        flash("用户未找到。", "warning")
        return redirect(url_for('list_users'))

    if request.method == 'POST':
        try:
            updated_data = {
                "username": request.form['username'],
                "email": request.form['email']
                # registration_date 和 last_login 不允许从 Web 端编辑
            }
            message, success = rc.update_user_details(user_id, updated_data)
            flash(message, "success" if success else "danger")
            if success:
                return redirect(url_for('user_detail', user_id=user_id))
        except Exception as e:
            flash(f"更新用户失败: {e}", "danger")
        return render_template('add_edit_user.html', title="编辑用户", user=user)
    
    return render_template('add_edit_user.html', title="编辑用户", user=user)

@app.route('/user/delete/<user_id>', methods=['POST'])
def delete_user(user_id):
    if not g.redis_db:
        flash("Redis 连接失败。", "danger")
        return redirect(url_for('list_users'))
    
    message, success = rc.delete_user(user_id)
    flash(message, "success" if success else "danger")
    return redirect(url_for('list_users'))


@app.route('/orders')
def list_orders():
    if not g.redis_db:
        return render_template('orders.html', orders=[], error="无法连接到 Redis。")
    
    page, page_size, search_query = get_pagination_params(request)
    orders, total_items = rc.get_all_orders(page, page_size, search_query)

    total_pages = math.ceil(total_items / page_size) if total_items > 0 else 1

    return render_template('orders.html', 
                           orders=orders, 
                           current_page=page, 
                           total_pages=total_pages, 
                           page_size=page_size, 
                           search_query=search_query)

@app.route('/order/<order_id>')
def order_detail(order_id):
    if not g.redis_db:
        flash("Redis 连接失败。", "danger")
        return redirect(url_for('list_orders'))

    order, order_items = rc.get_order_details_with_items(order_id)
    if order:
        order['total_amount'] = float(order.get('total_amount', 0))
        user_id = order.get('user_id')
        username = g.redis_db.hget(f"user:{user_id}", "username") if user_id else "N/A"

        for item in order_items:
            item['Quantity'] = int(item.get('Quantity', 0))
            item['UnitPrice'] = float(item.get('UnitPrice', 0))
            item['total_price'] = item['Quantity'] * item['UnitPrice']

        return render_template('order_detail.html', order=order, username=username, order_items=order_items)
    flash("订单未找到。", "warning")
    return redirect(url_for('list_orders'))

@app.route('/order/edit_status/<order_id>', methods=['GET', 'POST'])
def edit_order_status(order_id):
    if not g.redis_db:
        flash("Redis 连接失败。", "danger")
        return redirect(url_for('list_orders'))
    
    order, _ = rc.get_order_details_with_items(order_id)
    if not order:
        flash("订单未找到。", "warning")
        return redirect(url_for('list_orders'))

    if request.method == 'POST':
        try:
            new_status = request.form['status']
            message, success = rc.update_order_status(order_id, new_status)
            flash(message, "success" if success else "danger")
            if success:
                return redirect(url_for('order_detail', order_id=order_id))
        except Exception as e:
            flash(f"更新订单状态失败: {e}", "danger")
        return render_template('edit_order_status.html', order=order, current_status=order.get('status', '未知'))
    
    return render_template('edit_order_status.html', order=order, current_status=order.get('status', '未知'))

@app.route('/order/delete/<order_id>', methods=['POST'])
def delete_order(order_id):
    if not g.redis_db:
        flash("Redis 连接失败。", "danger")
        return redirect(url_for('list_orders'))
    
    message, success = rc.delete_order(order_id)
    flash(message, "success" if success else "danger")
    return redirect(url_for('list_orders'))


@app.route('/analysis')
def analysis():
    if not g.redis_db:
        flash("Redis 连接失败。", "danger")
        return render_template('analysis.html', analysis_results={"error": "无法连接到 Redis。"})

    analysis_results = rc.analyze_data_from_redis()
    
    if "error" in analysis_results:
        flash(f"分析失败: {analysis_results['error']}", "danger")
        return render_template('analysis.html', analysis_results=analysis_results)

    # --- 生成 Plotly 图表 ---
    charts = {}

    # 1. 商品分类分布图 (饼图)
    categories = [item[0] for item in analysis_results.get('top_categories', [])]
    counts = [item[1] for item in analysis_results.get('top_categories', [])]
    if categories:
        fig_category = px.pie(names=categories, values=counts, title='商品分类分布')
        charts['category_distribution'] = plot(fig_category, output_type='div', include_plotlyjs=False)
    else:
        charts['category_distribution'] = "<p>无分类数据。</p>"

    # 2. 热门销售商品 (柱状图)
    top_selling_names = [item['name'] for item in analysis_results.get('top_selling_products', [])]
    top_selling_counts = [item['sales_count'] for item in analysis_results.get('top_selling_products', [])]
    if top_selling_names:
        fig_selling = px.bar(x=top_selling_names, y=top_selling_counts, title='热门销售商品', labels={'x':'商品名称', 'y':'销售数量'})
        charts['top_selling_products'] = plot(fig_selling, output_type='div', include_plotlyjs=False)
    else:
        charts['top_selling_products'] = "<p>无销售数据。</p>"

    # 3. 月销售额趋势 (折线图)
    monthly_sales_data = analysis_results.get('monthly_sales_trend', [])
    if monthly_sales_data:
        months = [item[0] for item in monthly_sales_data]
        sales = [item[1] for item in monthly_sales_data]
        fig_monthly = px.line(x=months, y=sales, title='月销售额趋势', labels={'x':'月份', 'y':'销售额'})
        charts['monthly_sales_trend'] = plot(fig_monthly, output_type='div', include_plotlyjs=False)
    else:
        charts['monthly_sales_trend'] = "<p>无月销售额数据。</p>"

    return render_template('analysis.html', analysis_results=analysis_results, charts=charts)

if __name__ == '__main__':
    app.run(debug=True) # debug=True 会在代码修改时自动重启服务器，并显示详细错误信息