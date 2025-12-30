import sys
import os
import threading
import time
import math
import json
import uuid
from datetime import datetime # 用于用户添加时的日期格式

# 导入Qt模块
from PyQt5.QtWidgets import (QApplication, QMainWindow, QVBoxLayout,
                             QHBoxLayout, QPushButton, QLabel,
                             QComboBox, QFileDialog, QWidget,
                             QTabWidget, QGroupBox, QMessageBox,
                             QTextEdit, QSpinBox, QDoubleSpinBox,
                             QFormLayout, QProgressBar, QLineEdit, QCheckBox,
                             QTableWidget, QTableWidgetItem, QHeaderView,
                             QDialog, QDialogButtonBox, QSizePolicy)
from PyQt5.QtCore import Qt, QTimer, pyqtSignal, QObject
from PyQt5.QtGui import QPixmap, QImage, QFont, QIcon, QPalette, QBrush, QColor

# 导入Matplotlib相关
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.figure import Figure
import matplotlib.pyplot as plt
import numpy as np # 用于Matplotlib示例数据

import redis_core as rc # 导入核心逻辑模块

# 设置matplotlib支持中文显示
plt.rcParams['font.sans-serif'] = ['SimHei', 'Microsoft YaHei', 'DejaVu Sans']
plt.rcParams['axes.unicode_minus'] = False

# --- 辅助类：Worker 线程 (用于在后台执行耗时操作) ---
class Worker(QObject):
    finished = pyqtSignal(object) # 信号，用于将结果传回主线程
    error = pyqtSignal(str) # 信号，用于报告错误
    progress = pyqtSignal(str) # 信号，用于报告进度

    def __init__(self, func, *args, **kwargs):
        super().__init__()
        self.func = func
        self.args = args
        self.kwargs = kwargs

    def run(self):
        try:
            result = self.func(*self.args, **self.kwargs)
            self.finished.emit(result)
        except Exception as e:
            self.error.emit(str(e))

# --- Matplotlib画布 ---
class MplCanvas(FigureCanvas):
    def __init__(self, parent=None, width=5, height=4, dpi=100):
        self.fig = Figure(figsize=(width, height), dpi=dpi)
        self.axes = self.fig.add_subplot(111) # 默认创建一个子图，方便初始化样式
        super().__init__(self.fig)
        self.setParent(parent)
        self.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        self.updateGeometry()

        # 默认背景和文本颜色，现在改为白色背景，黑色文字
        self.set_plot_style(bg_color='white', text_color='black', grid_color='#cccccc')

    def set_plot_style(self, bg_color='white', text_color='black', grid_color='#cccccc'):
        """设置matplotlib样式以匹配应用主题"""
        # 设置 Figure 的背景色
        self.fig.patch.set_facecolor(bg_color)
        # 设置 Axes 的背景色
        self.axes.set_facecolor(bg_color)

        # 设置刻度、标签、标题颜色
        self.axes.tick_params(colors=text_color)
        self.axes.xaxis.label.set_color(text_color)
        self.axes.yaxis.label.set_color(text_color)
        self.axes.title.set_color(text_color)
        
        # 设置边框颜色
        self.axes.spines['bottom'].set_color(text_color)
        self.axes.spines['top'].set_color(text_color)
        self.axes.spines['left'].set_color(text_color)
        self.axes.spines['right'].set_color(text_color)
        
        # 设置网格颜色和透明度
        self.axes.grid(True, alpha=0.3, color=grid_color)
        
        # 更新legend的颜色
        if self.axes.legend_:
            for text in self.axes.legend_.get_texts():
                text.set_color(text_color)
        
        self.fig.tight_layout() # 调整布局以避免标签重叠
        self.draw_idle() # 重新绘制画布


# --- 辅助对话框类 ---
class AddEditProductDialog(QDialog):
    def __init__(self, parent=None, title="商品信息", product_data=None):
        super().__init__(parent)
        self.setWindowTitle(title)
        self.setModal(True) # 设置为模态对话框
        self.setGeometry(100, 100, 450, 550)

        self.product_data = product_data
        self.result = None

        main_layout = QVBoxLayout(self)
        form_layout = QFormLayout()

        self.entries = {}
        
        # 字段定义
        fields = [
            ("名称", "name", QLineEdit),
            ("描述", "description", QTextEdit),
            ("分类", "category", QComboBox), # 更改为QComboBox
            ("价格", "price", QDoubleSpinBox),
            ("库存", "stock", QSpinBox)
        ]
        
        # 获取所有分类
        self.categories = rc.get_product_categories()
        if not self.categories:
            self.categories = ["默认分类"] # 至少有一个分类

        for label_text, key, widget_type in fields:
            label = QLabel(label_text + ":")
            if widget_type == QLineEdit:
                entry = QLineEdit()
                if product_data and key in product_data:
                    entry.setText(str(product_data[key]))
            elif widget_type == QTextEdit:
                entry = QTextEdit()
                if product_data and key in product_data:
                    entry.setText(str(product_data[key]))
            elif widget_type == QDoubleSpinBox:
                entry = QDoubleSpinBox()
                entry.setRange(0.01, 1000000.00)
                entry.setSingleStep(0.01)
                if product_data and key in product_data:
                    entry.setValue(float(product_data[key]))
            elif widget_type == QSpinBox:
                entry = QSpinBox()
                entry.setRange(0, 100000)
                if product_data and key in product_data:
                    entry.setValue(int(product_data[key]))
            elif widget_type == QComboBox: # 处理QComboBox
                entry = QComboBox()
                entry.addItems(self.categories)
                if product_data and key in product_data:
                    current_cat = str(product_data[key])
                    if current_cat not in self.categories:
                        entry.addItem(current_cat) # 如果当前商品的分类不在列表中，添加它
                    entry.setCurrentText(current_cat)
                
                # 添加一个QLineEdit用于输入新分类
                new_cat_layout = QHBoxLayout()
                new_cat_layout.addWidget(entry)
                new_cat_input = QLineEdit()
                new_cat_input.setPlaceholderText("输入新分类 (如果列表中没有)")
                # new_cat_input.textChanged.connect(lambda text, cb=entry: self._update_category_combobox(text, cb)) # 不需要实时更新
                new_cat_layout.addWidget(new_cat_input)
                form_layout.addRow(label, new_cat_layout)
                self.entries[key] = entry # 存储QComboBox
                self.entries[key + "_new_input"] = new_cat_input # 存储QLineEdit
                continue # 跳过下面的form_layout.addRow(label, entry)
            
            self.entries[key] = entry
            form_layout.addRow(label, entry)
        
        main_layout.addLayout(form_layout)

        # 按钮
        button_box = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        button_box.accepted.connect(self.accept)
        button_box.rejected.connect(self.reject)
        main_layout.addWidget(button_box)
    
    def accept(self):
        data = {}
        try:
            data["name"] = self.entries["name"].text()
            data["description"] = self.entries["description"].toPlainText()
            
            # 获取分类，优先使用新输入的，否则使用下拉框选中的
            new_cat_text = self.entries["category_new_input"].text().strip()
            if new_cat_text:
                data["category"] = new_cat_text
            else:
                data["category"] = self.entries["category"].currentText()

            data["price"] = self.entries["price"].value()
            data["stock"] = self.entries["stock"].value()

            if not self.product_data: # 添加模式
                data["created_at"] = datetime.now().isoformat()
            
            self.result = data
            super().accept()
        except ValueError:
            QMessageBox.critical(self, "输入错误", "价格和库存必须是有效数字。")
        except Exception as e:
            QMessageBox.critical(self, "错误", f"保存失败: {e}")

class AddEditUserDialog(QDialog):
    def __init__(self, parent=None, title="用户信息", user_data=None):
        super().__init__(parent)
        self.setWindowTitle(title)
        self.setModal(True)
        self.setGeometry(100, 100, 450, 400)

        self.user_data = user_data
        self.result = None

        main_layout = QVBoxLayout(self)
        form_layout = QFormLayout()

        self.entries = {}

        fields = [
            ("用户名", "username", QLineEdit),
            ("邮箱", "email", QLineEdit),
            ("注册日期", "registration_date", QLineEdit),
            ("最后登录", "last_login", QLineEdit)
        ]

        for label_text, key, widget_type in fields:
            label = QLabel(label_text + ":")
            entry = QLineEdit()
            if user_data and key in user_data:
                entry.setText(str(user_data[key]))
            
            if key in ["registration_date", "last_login"] and user_data:
                entry.setReadOnly(True) # 这些字段通常不可直接编辑
            
            self.entries[key] = entry
            form_layout.addRow(label, entry)
        
        main_layout.addLayout(form_layout)

        button_box = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        button_box.accepted.connect(self.accept)
        button_box.rejected.connect(self.reject)
        main_layout.addWidget(button_box)

    def accept(self):
        data = {}
        try:
            data["username"] = self.entries["username"].text()
            data["email"] = self.entries["email"].text()
            
            if not self.user_data: # 添加模式
                data["registration_date"] = datetime.now().isoformat()
                data["last_login"] = datetime.now().isoformat()
            
            self.result = data
            super().accept()
        except Exception as e:
            QMessageBox.critical(self, "错误", f"保存失败: {e}")

class EditOrderStatusDialog(QDialog):
    def __init__(self, parent=None, title="编辑订单状态", current_status="未知"):
        super().__init__(parent)
        self.setWindowTitle(title)
        self.setModal(True)
        self.setGeometry(100, 100, 350, 200)

        self.result = None
        self.current_status = current_status

        main_layout = QVBoxLayout(self)
        
        main_layout.addWidget(QLabel("当前状态: " + current_status))
        
        self.status_options = ['待付款', '已付款', '已发货', '已完成', '已取消']
        self.status_combobox = QComboBox()
        self.status_combobox.addItems(self.status_options)
        
        if current_status in self.status_options:
            self.status_combobox.setCurrentText(current_status)
        else:
            self.status_combobox.setCurrentIndex(0)

        main_layout.addWidget(self.status_combobox)

        button_box = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        button_box.accepted.connect(self.accept)
        button_box.rejected.connect(self.reject)
        main_layout.addWidget(button_box)

    def accept(self):
        self.result = self.status_combobox.currentText()
        super().accept()

class DetailDisplayDialog(QDialog):
    def __init__(self, parent=None, title="详情", data=None):
        super().__init__(parent)
        self.setWindowTitle(title)
        self.setModal(True)
        self.setGeometry(100, 100, 600, 400) # 调整大小

        main_layout = QVBoxLayout(self)
        
        self.text_display = QTextEdit()
        self.text_display.setReadOnly(True)
        self.text_display.setFont(QFont("Monospace", 10)) # 等宽字体方便查看JSON
        
        if data:
            self.text_display.setText(json.dumps(data, indent=2, ensure_ascii=False))
        else:
            self.text_display.setText("无数据可显示。")
        
        main_layout.addWidget(self.text_display)

        close_button = QPushButton("关闭")
        close_button.clicked.connect(self.accept)
        main_layout.addWidget(close_button)


# --- 主应用窗口 ---
class REDMApp(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Redis 电商数据管理系统 (REDM) - 桌面版")
        self.setGeometry(100, 100, 1200, 800) # 初始窗口大小

        self.redis_client = None
        self.current_workers = [] # 用于管理正在运行的线程

        # --- 分页状态 ---
        self.page_size = 20 # 默认每页显示数量
        self.products_current_page = 1
        self.products_total_pages = 1
        self.products_search_query = "" # 新增搜索查询
        self.users_current_page = 1
        self.users_total_pages = 1
        self.users_search_query = "" # 新增搜索查询
        self.orders_current_page = 1
        self.orders_total_pages = 1
        self.orders_search_query = "" # 新增搜索查询

        self.selected_product_id = None
        self.selected_user_id = None
        self.selected_order_id = None

        self.init_ui()
        self.test_redis_connection() # 启动时测试连接

    def init_ui(self):
        main_widget = QWidget()
        self.setCentralWidget(main_widget)
        main_layout = QVBoxLayout(main_widget)

        # --- 顶部 Redis 状态栏 ---
        redis_status_group = QGroupBox("Redis 连接状态")
        redis_status_layout = QHBoxLayout(redis_status_group)
        self.redis_status_label = QLabel("Redis 状态: 未连接")
        self.redis_status_label.setFont(QFont("Arial", 12, QFont.Bold))
        self.redis_status_label.setStyleSheet("color: red;") # 默认红色
        redis_status_layout.addWidget(self.redis_status_label)
        self.test_redis_button = QPushButton("测试/连接 Redis")
        self.test_redis_button.clicked.connect(self.test_redis_connection)
        redis_status_layout.addWidget(self.test_redis_button)
        main_layout.addWidget(redis_status_group)

        # --- Tab 控件 ---
        self.tab_widget = QTabWidget()
        main_layout.addWidget(self.tab_widget)

        self.create_data_management_tab()
        self.create_products_tab()
        self.create_users_tab()
        self.create_orders_tab()
        self.create_analysis_tab()

        # 确保所有按钮都已创建后再调用
        self.set_all_buttons_state(False)

    def set_all_buttons_state(self, enabled):
        """统一设置所有操作按钮的启用/禁用状态"""
        buttons = [
            self.test_redis_button,
            self.faker_button, self.online_retail_button, self.flush_db_button,
            self.refresh_products_button, self.add_product_button,
            self.products_prev_button, self.products_next_button, self.products_search_input, self.products_search_button, self.products_page_jump_input, self.products_page_jump_button,
            self.refresh_users_button, self.add_user_button,
            self.users_prev_button, self.users_next_button, self.users_search_input, self.users_search_button, self.users_page_jump_input, self.users_page_jump_button,
            self.refresh_orders_button,
            self.orders_prev_button, self.orders_next_button, self.orders_search_input, self.orders_search_button, self.orders_page_jump_input, self.orders_page_jump_button,
            self.run_analysis_button
        ]
        for btn in buttons:
            btn.setEnabled(enabled)
        
        # QCheckBox 的启用/禁用
        self.fake_fill_checkbox.setEnabled(enabled)

        # CRUD 按钮的启用状态还需要根据是否有选中项来判断
        self.update_crud_button_states()


    def update_crud_button_states(self):
        """根据是否有选中项来启用/禁用 CRUD 按钮"""
        # 只有在主按钮启用时才更新 CRUD 按钮
        # 避免在 set_all_buttons_state(False) 期间覆盖
        if self.refresh_products_button.isEnabled():
            self.edit_product_button.setEnabled(self.selected_product_id is not None)
            self.delete_product_button.setEnabled(self.selected_product_id is not None)
        if self.refresh_users_button.isEnabled():
            self.edit_user_button.setEnabled(self.selected_user_id is not None)
            self.delete_user_button.setEnabled(self.selected_user_id is not None)
        if self.refresh_orders_button.isEnabled():
            self.edit_order_button.setEnabled(self.selected_order_id is not None)
            self.delete_order_button.setEnabled(self.selected_order_id is not None)

    def run_in_thread(self, func, callback_finished=None, callback_error=None, callback_progress=None, *args, **kwargs):
        """
        在单独的线程中运行一个函数，并在主线程中处理结果。
        :param func: 要在子线程中运行的函数。
        :param callback_finished: 线程完成时在主线程中调用的回调函数 (接收 func 的返回值)。
        :param callback_error: 线程发生错误时在主线程中调用的回调函数 (接收错误信息)。
        :param callback_progress: 线程报告进度时在主线程中调用的回调函数 (接收进度信息)。
        """
        self.set_all_buttons_state(False) # 禁用所有按钮

        worker = Worker(func, *args, **kwargs)
        thread = threading.Thread(target=worker.run)
        self.current_workers.append(thread) # 跟踪线程

        if callback_finished:
            worker.finished.connect(callback_finished)
        if callback_error:
            worker.error.connect(callback_error)
        if callback_progress:
            worker.progress.connect(callback_progress)
        
        # 线程结束后，重新启用按钮
        worker.finished.connect(lambda: self.set_all_buttons_state(True))
        worker.error.connect(lambda: self.set_all_buttons_state(True))

        thread.start()
        return thread # 返回线程对象，如果需要进一步管理

    def update_log(self, widget, message, clear_first=False):
        """更新日志输出框"""
        if clear_first:
            widget.clear()
        widget.append(f"[{datetime.now().strftime('%H:%M:%S')}] {message}")

    def test_redis_connection(self):
        self.redis_client = rc.get_redis_client()
        if self.redis_client:
            self.redis_status_label.setText("Redis 状态: 已连接!")
            self.redis_status_label.setStyleSheet("color: green;")
            QMessageBox.information(self, "Redis 连接", "成功连接到 Redis!")
            self.set_all_buttons_state(True) # 连接成功后启用所有按钮
        else:
            self.redis_status_label.setText("Redis 状态: 未连接!")
            self.redis_status_label.setStyleSheet("color: red;")
            QMessageBox.critical(self, "Redis 连接", "无法连接到 Redis。请确保 Redis 服务器正在运行。")
            self.set_all_buttons_state(False) # 连接失败则禁用

    # --- 数据管理 Tab ---
    def create_data_management_tab(self):
        tab = QWidget()
        layout = QVBoxLayout(tab)

        # Redis 连接组
        redis_group = QGroupBox("Redis 操作")
        redis_layout = QFormLayout(redis_group)
        
        self.faker_button = QPushButton("生成并存储 Faker 模拟数据")
        self.faker_button.clicked.connect(self.generate_faker_data)
        redis_layout.addRow(self.faker_button)

        self.fake_fill_checkbox = QCheckBox("自动补足缺失信息 (Online Retail)")
        self.fake_fill_checkbox.setChecked(True)
        redis_layout.addRow(self.fake_fill_checkbox)

        self.online_retail_button = QPushButton("加载并存储 Online Retail 数据")
        self.online_retail_button.clicked.connect(self.load_online_retail_data)
        redis_layout.addRow(self.online_retail_button)

        self.flush_db_button = QPushButton("清空 Redis 数据库 (危险操作!)")
        self.flush_db_button.setStyleSheet("background-color: red; color: white;")
        self.flush_db_button.clicked.connect(self.flush_redis_db)
        redis_layout.addRow(self.flush_db_button)

        layout.addWidget(redis_group)

        # 操作日志组
        log_group = QGroupBox("操作日志")
        log_layout = QVBoxLayout(log_group)
        self.data_mgmt_output = QTextEdit()
        self.data_mgmt_output.setReadOnly(True)
        log_layout.addWidget(self.data_mgmt_output)
        layout.addWidget(log_group)

        self.tab_widget.addTab(tab, "数据管理")

    def generate_faker_data(self):
        if not self.redis_client:
            QMessageBox.critical(self, "错误", "Redis 未连接。请先连接。")
            return
        self.update_log(self.data_mgmt_output, "正在生成并存储 Faker 数据...", clear_first=True)
        self.run_in_thread(rc.collect_and_clean_faker_data, self._handle_data_storage_result, self._handle_thread_error, is_faker=True)

    def _handle_data_storage_result(self, data_tuple, is_faker=False):
        if isinstance(data_tuple, tuple) and len(data_tuple) == 3 and all(isinstance(df, pd.DataFrame) for df in data_tuple):
            products_df, users_df, orders_df = data_tuple
            self.update_log(self.data_mgmt_output, "数据清洗完成，正在存储到 Redis...")
            msg, success = rc.store_data_in_redis(products_df, users_df, orders_df, flush_db=True)
        else:
            msg, success = data_tuple # 可能是错误信息
        
        self.update_log(self.data_mgmt_output, f"{msg} (成功: {success})")
        if success:
            QMessageBox.information(self, "数据管理", f"数据操作完成: {msg}")
            # 刷新所有列表
            self.products_current_page = 1
            self.users_current_page = 1
            self.orders_current_page = 1
            self.refresh_products(auto_select_tab=False)
            self.refresh_users(auto_select_tab=False)
            self.refresh_orders(auto_select_tab=False)
        else:
            QMessageBox.critical(self, "数据管理", f"数据操作失败: {msg}")

    def load_online_retail_data(self):
        if not self.redis_client:
            QMessageBox.critical(self, "错误", "Redis 未连接。请先连接。")
            return
        self.update_log(self.data_mgmt_output, "正在加载并存储 Online Retail 数据...", clear_first=True)
        fake_fill = self.fake_fill_checkbox.isChecked() # QCheckBox 的状态用 isChecked()
        self.run_in_thread(rc.load_and_clean_online_retail_data, self._handle_data_storage_result, self._handle_thread_error, fake_fill_missing=fake_fill)

    def flush_redis_db(self):
        if not self.redis_client:
            QMessageBox.critical(self, "错误", "Redis 未连接。请先连接。")
            return
        reply = QMessageBox.question(self, "确认清空数据库", "您确定要清空 Redis 数据库吗？此操作不可逆！",
                                     QMessageBox.Yes | QMessageBox.No, QMessageBox.No)
        if reply == QMessageBox.Yes:
            self.update_log(self.data_mgmt_output, "正在清空 Redis 数据库...", clear_first=True)
            self.run_in_thread(rc.flush_redis_db, self._handle_flush_result, self._handle_thread_error)

    def _handle_flush_result(self, result):
        msg, success = result
        self.update_log(self.data_mgmt_output, f"{msg} (成功: {success})")
        if success:
            QMessageBox.information(self, "数据管理", "Redis 数据库已清空!")
            # 清空数据后，刷新所有列表，重置分页
            self.products_current_page = 1
            self.products_search_query = ""
            self.users_current_page = 1
            self.users_search_query = ""
            self.orders_current_page = 1
            self.orders_search_query = ""
            self.refresh_products(auto_select_tab=False)
            self.refresh_users(auto_select_tab=False)
            self.refresh_orders(auto_select_tab=False)
        else:
            QMessageBox.critical(self, "数据管理", f"清空数据库失败: {msg}")

    def _handle_thread_error(self, error_msg):
        QMessageBox.critical(self, "线程错误", f"后台操作发生错误: {error_msg}")
        self.update_log(self.data_mgmt_output, f"错误: {error_msg}")


    # --- 商品 Tab ---
    def create_products_tab(self):
        tab = QWidget()
        layout = QVBoxLayout(tab)

        # 顶部操作栏 (刷新、添加、编辑、删除、搜索、分页)
        top_bar_layout = QHBoxLayout()
        self.refresh_products_button = QPushButton("刷新商品列表")
        self.refresh_products_button.clicked.connect(lambda: self.refresh_products(auto_select_tab=False))
        top_bar_layout.addWidget(self.refresh_products_button)

        self.add_product_button = QPushButton("添加商品")
        self.add_product_button.clicked.connect(self.add_product_dialog)
        top_bar_layout.addWidget(self.add_product_button)

        self.edit_product_button = QPushButton("编辑选中商品")
        self.edit_product_button.clicked.connect(self.edit_product_dialog)
        self.edit_product_button.setEnabled(False)
        top_bar_layout.addWidget(self.edit_product_button)

        self.delete_product_button = QPushButton("删除选中商品")
        self.delete_product_button.setStyleSheet("background-color: red; color: white;")
        self.delete_product_button.clicked.connect(self.delete_product)
        self.delete_product_button.setEnabled(False)
        top_bar_layout.addWidget(self.delete_product_button)

        top_bar_layout.addStretch() # 填充空白

        # 搜索框和搜索按钮
        self.products_search_input = QLineEdit()
        self.products_search_input.setPlaceholderText("搜索商品名称/描述/分类...")
        self.products_search_input.returnPressed.connect(lambda: self.search_items('products')) # 回车键触发搜索
        top_bar_layout.addWidget(self.products_search_input)
        self.products_search_button = QPushButton("搜索")
        self.products_search_button.clicked.connect(lambda: self.search_items('products'))
        top_bar_layout.addWidget(self.products_search_button)

        # 分页控件
        pagination_layout = QHBoxLayout()
        self.products_prev_button = QPushButton("上一页")
        self.products_prev_button.clicked.connect(lambda: self.change_page('products', -1))
        pagination_layout.addWidget(self.products_prev_button)
        self.products_page_label = QLabel("页码: 1/1")
        pagination_layout.addWidget(self.products_page_label)
        self.products_next_button = QPushButton("下一页")
        self.products_next_button.clicked.connect(lambda: self.change_page('products', 1))
        pagination_layout.addWidget(self.products_next_button)
        
        # 页码跳转输入框和按钮
        self.products_page_jump_input = QLineEdit()
        self.products_page_jump_input.setPlaceholderText("跳转到页")
        self.products_page_jump_input.setFixedWidth(80)
        self.products_page_jump_input.returnPressed.connect(lambda: self.jump_to_page('products'))
        pagination_layout.addWidget(self.products_page_jump_input)
        self.products_page_jump_button = QPushButton("跳转")
        self.products_page_jump_button.clicked.connect(lambda: self.jump_to_page('products'))
        pagination_layout.addWidget(self.products_page_jump_button)

        top_bar_layout.addLayout(pagination_layout)

        layout.addLayout(top_bar_layout)

        # 商品列表 (使用 QTableWidget)
        self.products_table = QTableWidget()
        self.products_table.setSelectionBehavior(QTableWidget.SelectRows) # 整行选中
        self.products_table.setSelectionMode(QTableWidget.SingleSelection) # 单行选中
        self.products_table.itemSelectionChanged.connect(self._on_product_selection_changed)
        self.products_table.doubleClicked.connect(lambda: self.edit_product_dialog()) # 双击编辑

        layout.addWidget(self.products_table)
        self.tab_widget.addTab(tab, "商品")

    def _on_product_selection_changed(self):
        selected_items = self.products_table.selectedItems()
        if selected_items:
            # 获取选中行的第一列（ID）
            self.selected_product_id = selected_items[0].data(Qt.UserRole) # 从UserData获取完整ID
        else:
            self.selected_product_id = None
        self.update_crud_button_states()

    def refresh_products(self, auto_select_tab=True):
        if not self.redis_client:
            QMessageBox.critical(self, "错误", "Redis 未连接。请先连接。")
            return
        if auto_select_tab:
            self.tab_widget.setCurrentWidget(self.tab_widget.findChild(QWidget, "商品")) # 切换到商品Tab

        self.selected_product_id = None
        self.update_crud_button_states()

        self.run_in_thread(
            lambda: rc.get_all_products(self.products_current_page, self.page_size, self.products_search_query),
            self._display_products,
            self._handle_thread_error
        )

    def _display_products(self, result):
        products, total_items = result
        self.products_total_pages = math.ceil(total_items / self.page_size) if total_items > 0 else 1
        self.products_page_label.setText(f"页码: {self.products_current_page}/{self.products_total_pages}")

        headers = ['ID', '名称', '分类', '价格', '库存']
        self.products_table.setColumnCount(len(headers))
        self.products_table.setHorizontalHeaderLabels(headers)
        self.products_table.setRowCount(len(products))

        for row_idx, p in enumerate(products):
            product_id = p.get('product_id', '')
            display_id = product_id[:8] + '...' if len(product_id) > 10 else product_id
            name = p.get('name', '')
            category = p.get('category', '')
            price = f"{p.get('price', 0):.2f}"
            stock = str(p.get('stock', ''))

            item_id = QTableWidgetItem(display_id)
            item_id.setData(Qt.UserRole, product_id) # 存储完整ID
            self.products_table.setItem(row_idx, 0, item_id)
            self.products_table.setItem(row_idx, 1, QTableWidgetItem(name))
            self.products_table.setItem(row_idx, 2, QTableWidgetItem(category))
            self.products_table.setItem(row_idx, 3, QTableWidgetItem(price))
            self.products_table.setItem(row_idx, 4, QTableWidgetItem(stock))
        
        self.products_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch) # 自动拉伸列宽
        self.products_table.resizeRowsToContents() # 调整行高以适应内容

    def add_product_dialog(self):
        if not self.redis_client:
            QMessageBox.critical(self, "错误", "Redis 未连接。请先连接。")
            return
        dialog = AddEditProductDialog(self, title="添加新商品")
        if dialog.exec_() == QDialog.Accepted:
            product_data = dialog.result
            self.run_in_thread(lambda: rc.add_product(product_data), self._handle_add_product_result, self._handle_thread_error)

    def _handle_add_product_result(self, result):
        msg, success = result
        if success:
            QMessageBox.information(self, "添加商品", msg)
            self.refresh_products(auto_select_tab=False)
        else:
            QMessageBox.critical(self, "添加商品", msg)

    def edit_product_dialog(self):
        if not self.redis_client:
            QMessageBox.critical(self, "错误", "Redis 未连接。请先连接。")
            return
        if not self.selected_product_id:
            QMessageBox.warning(self, "编辑商品", "请先从列表中选择一个商品。")
            return
        
        product_details = rc.get_product_details(self.selected_product_id)
        if not product_details:
            QMessageBox.critical(self, "错误", "未找到选中商品的详情。")
            return

        dialog = AddEditProductDialog(self, title="编辑商品", product_data=product_details)
        if dialog.exec_() == QDialog.Accepted:
            updated_data = dialog.result
            self.run_in_thread(lambda: rc.update_product_details(self.selected_product_id, updated_data), self._handle_edit_product_result, self._handle_thread_error)

    def _handle_edit_product_result(self, result):
        msg, success = result
        if success:
            QMessageBox.information(self, "编辑商品", msg)
            self.refresh_products(auto_select_tab=False)
        else:
            QMessageBox.critical(self, "编辑商品", msg)

    def delete_product(self):
        if not self.redis_client:
            QMessageBox.critical(self, "错误", "Redis 未连接。请先连接。")
            return
        if not self.selected_product_id:
            QMessageBox.warning(self, "删除商品", "请先从列表中选择一个商品。")
            return
        
        reply = QMessageBox.question(self, "确认删除", f"您确定要删除商品 ID: {self.selected_product_id[:8]}... 吗？此操作不可逆！",
                                     QMessageBox.Yes | QMessageBox.No, QMessageBox.No)
        if reply == QMessageBox.Yes:
            self.run_in_thread(lambda: rc.delete_product(self.selected_product_id), self._handle_delete_product_result, self._handle_thread_error)

    def _handle_delete_product_result(self, result):
        msg, success = result
        if success:
            QMessageBox.information(self, "删除商品", msg)
            self.selected_product_id = None
            self.refresh_products(auto_select_tab=False)
        else:
            QMessageBox.critical(self, "删除商品", msg)

    # --- 用户 Tab ---
    def create_users_tab(self):
        tab = QWidget()
        layout = QVBoxLayout(tab)

        # 顶部操作栏 (刷新、添加、编辑、删除、搜索、分页)
        top_bar_layout = QHBoxLayout()
        self.refresh_users_button = QPushButton("刷新用户列表")
        self.refresh_users_button.clicked.connect(lambda: self.refresh_users(auto_select_tab=False))
        top_bar_layout.addWidget(self.refresh_users_button)

        self.add_user_button = QPushButton("添加用户")
        self.add_user_button.clicked.connect(self.add_user_dialog)
        top_bar_layout.addWidget(self.add_user_button)

        self.edit_user_button = QPushButton("编辑选中用户")
        self.edit_user_button.clicked.connect(self.edit_user_dialog)
        self.edit_user_button.setEnabled(False)
        top_bar_layout.addWidget(self.edit_user_button)

        self.delete_user_button = QPushButton("删除选中用户")
        self.delete_user_button.setStyleSheet("background-color: red; color: white;")
        self.delete_user_button.clicked.connect(self.delete_user)
        self.delete_user_button.setEnabled(False)
        top_bar_layout.addWidget(self.delete_user_button)

        top_bar_layout.addStretch()

        # 搜索框和搜索按钮
        self.users_search_input = QLineEdit()
        self.users_search_input.setPlaceholderText("搜索用户名/邮箱...")
        self.users_search_input.returnPressed.connect(lambda: self.search_items('users')) # 回车键触发搜索
        top_bar_layout.addWidget(self.users_search_input)
        self.users_search_button = QPushButton("搜索")
        self.users_search_button.clicked.connect(lambda: self.search_items('users'))
        top_bar_layout.addWidget(self.users_search_button)

        # 分页控件
        pagination_layout = QHBoxLayout()
        self.users_prev_button = QPushButton("上一页")
        self.users_prev_button.clicked.connect(lambda: self.change_page('users', -1))
        pagination_layout.addWidget(self.users_prev_button)
        self.users_page_label = QLabel("页码: 1/1")
        pagination_layout.addWidget(self.users_page_label)
        self.users_next_button = QPushButton("下一页")
        self.users_next_button.clicked.connect(lambda: self.change_page('users', 1))
        pagination_layout.addWidget(self.users_next_button)
        
        # 页码跳转输入框和按钮
        self.users_page_jump_input = QLineEdit()
        self.users_page_jump_input.setPlaceholderText("跳转到页")
        self.users_page_jump_input.setFixedWidth(80)
        self.users_page_jump_input.returnPressed.connect(lambda: self.jump_to_page('users'))
        pagination_layout.addWidget(self.users_page_jump_input)
        self.users_page_jump_button = QPushButton("跳转")
        self.users_page_jump_button.clicked.connect(lambda: self.jump_to_page('users'))
        pagination_layout.addWidget(self.users_page_jump_button)

        top_bar_layout.addLayout(pagination_layout)

        layout.addLayout(top_bar_layout)

        # 用户列表 (使用 QTableWidget)
        self.users_table = QTableWidget()
        self.users_table.setSelectionBehavior(QTableWidget.SelectRows)
        self.users_table.setSelectionMode(QTableWidget.SingleSelection)
        self.users_table.itemSelectionChanged.connect(self._on_user_selection_changed)
        self.users_table.doubleClicked.connect(lambda: self.edit_user_dialog()) # 双击编辑

        layout.addWidget(self.users_table)
        self.tab_widget.addTab(tab, "用户")

    def _on_user_selection_changed(self):
        selected_items = self.users_table.selectedItems()
        if selected_items:
            self.selected_user_id = selected_items[0].data(Qt.UserRole)
        else:
            self.selected_user_id = None
        self.update_crud_button_states()

    def refresh_users(self, auto_select_tab=True):
        if not self.redis_client:
            QMessageBox.critical(self, "错误", "Redis 未连接。请先连接。")
            return
        if auto_select_tab:
            self.tab_widget.setCurrentWidget(self.tab_widget.findChild(QWidget, "用户"))

        self.selected_user_id = None
        self.update_crud_button_states()

        self.run_in_thread(
            lambda: rc.get_all_users(self.users_current_page, self.page_size, self.users_search_query),
            self._display_users,
            self._handle_thread_error
        )

    def _display_users(self, result):
        users, total_items = result
        self.users_total_pages = math.ceil(total_items / self.page_size) if total_items > 0 else 1
        self.users_page_label.setText(f"页码: {self.users_current_page}/{self.users_total_pages}")

        headers = ['ID', '用户名', '邮箱', '注册日期', '最后登录']
        self.users_table.setColumnCount(len(headers))
        self.users_table.setHorizontalHeaderLabels(headers)
        self.users_table.setRowCount(len(users))

        for row_idx, u in enumerate(users):
            user_id = u.get('user_id', '')
            display_id = user_id[:8] + '...' if len(user_id) > 10 else user_id
            username = u.get('username', '')
            email = u.get('email', '')
            reg_date = u.get('registration_date', '')
            last_login = u.get('last_login', '')

            item_id = QTableWidgetItem(display_id)
            item_id.setData(Qt.UserRole, user_id)
            self.users_table.setItem(row_idx, 0, item_id)
            self.users_table.setItem(row_idx, 1, QTableWidgetItem(username))
            self.users_table.setItem(row_idx, 2, QTableWidgetItem(email))
            self.users_table.setItem(row_idx, 3, QTableWidgetItem(reg_date))
            self.users_table.setItem(row_idx, 4, QTableWidgetItem(last_login))
        
        self.users_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.users_table.resizeRowsToContents()

    def add_user_dialog(self):
        if not self.redis_client:
            QMessageBox.critical(self, "错误", "Redis 未连接。请先连接。")
            return
        dialog = AddEditUserDialog(self, title="添加新用户")
        if dialog.exec_() == QDialog.Accepted:
            user_data = dialog.result
            self.run_in_thread(lambda: rc.add_user(user_data), self._handle_add_user_result, self._handle_thread_error)

    def _handle_add_user_result(self, result):
        msg, success = result
        if success:
            QMessageBox.information(self, "添加用户", msg)
            self.refresh_users(auto_select_tab=False)
        else:
            QMessageBox.critical(self, "添加用户", msg)

    def edit_user_dialog(self):
        if not self.redis_client:
            QMessageBox.critical(self, "错误", "Redis 未连接。请先连接。")
            return
        if not self.selected_user_id:
            QMessageBox.warning(self, "编辑用户", "请先从列表中选择一个用户。")
            return
        
        user_details, _ = rc.get_user_details(self.selected_user_id)
        if not user_details:
            QMessageBox.critical(self, "错误", "未找到选中用户的详情。")
            return

        dialog = AddEditUserDialog(self, title="编辑用户", user_data=user_details)
        if dialog.exec_() == QDialog.Accepted:
            updated_data = dialog.result
            self.run_in_thread(lambda: rc.update_user_details(self.selected_user_id, updated_data), self._handle_edit_user_result, self._handle_thread_error)

    def _handle_edit_user_result(self, result):
        msg, success = result
        if success:
            QMessageBox.information(self, "编辑用户", msg)
            self.refresh_users(auto_select_tab=False)
        else:
            QMessageBox.critical(self, "编辑用户", msg)

    def delete_user(self):
        if not self.redis_client:
            QMessageBox.critical(self, "错误", "Redis 未连接。请先连接。")
            return
        if not self.selected_user_id:
            QMessageBox.warning(self, "删除用户", "请先从列表中选择一个用户。")
            return
        
        reply = QMessageBox.question(self, "确认删除", f"您确定要删除用户 ID: {self.selected_user_id[:8]}... 吗？此操作不可逆！",
                                     QMessageBox.Yes | QMessageBox.No, QMessageBox.No)
        if reply == QMessageBox.Yes:
            self.run_in_thread(lambda: rc.delete_user(self.selected_user_id), self._handle_delete_user_result, self._handle_thread_error)

    def _handle_delete_user_result(self, result):
        msg, success = result
        if success:
            QMessageBox.information(self, "删除用户", msg)
            self.selected_user_id = None
            self.refresh_users(auto_select_tab=False)
        else:
            QMessageBox.critical(self, "删除用户", msg)

    # --- 订单 Tab ---
    def create_orders_tab(self):
        tab = QWidget()
        layout = QVBoxLayout(tab)

        # 顶部操作栏 (刷新、编辑、删除、搜索、分页)
        top_bar_layout = QHBoxLayout()
        self.refresh_orders_button = QPushButton("刷新订单列表")
        self.refresh_orders_button.clicked.connect(lambda: self.refresh_orders(auto_select_tab=False))
        top_bar_layout.addWidget(self.refresh_orders_button)

        self.edit_order_button = QPushButton("编辑选中订单状态")
        self.edit_order_button.clicked.connect(self.edit_order_status_dialog)
        self.edit_order_button.setEnabled(False)
        top_bar_layout.addWidget(self.edit_order_button)

        self.delete_order_button = QPushButton("删除选中订单")
        self.delete_order_button.setStyleSheet("background-color: red; color: white;")
        self.delete_order_button.clicked.connect(self.delete_order)
        self.delete_order_button.setEnabled(False)
        top_bar_layout.addWidget(self.delete_order_button)

        top_bar_layout.addStretch()

        # 搜索框和搜索按钮
        self.orders_search_input = QLineEdit()
        self.orders_search_input.setPlaceholderText("搜索订单ID/用户ID/国家/状态...")
        self.orders_search_input.returnPressed.connect(lambda: self.search_items('orders')) # 回车键触发搜索
        top_bar_layout.addWidget(self.orders_search_input)
        self.orders_search_button = QPushButton("搜索")
        self.orders_search_button.clicked.connect(lambda: self.search_items('orders'))
        top_bar_layout.addWidget(self.orders_search_button)

        # 分页控件
        pagination_layout = QHBoxLayout()
        self.orders_prev_button = QPushButton("上一页")
        self.orders_prev_button.clicked.connect(lambda: self.change_page('orders', -1))
        pagination_layout.addWidget(self.orders_prev_button)
        self.orders_page_label = QLabel("页码: 1/1")
        pagination_layout.addWidget(self.orders_page_label)
        self.orders_next_button = QPushButton("下一页")
        self.orders_next_button.clicked.connect(lambda: self.change_page('orders', 1))
        pagination_layout.addWidget(self.orders_next_button)
        
        # 页码跳转输入框和按钮
        self.orders_page_jump_input = QLineEdit()
        self.orders_page_jump_input.setPlaceholderText("跳转到页")
        self.orders_page_jump_input.setFixedWidth(80)
        self.orders_page_jump_input.returnPressed.connect(lambda: self.jump_to_page('orders'))
        pagination_layout.addWidget(self.orders_page_jump_input)
        self.orders_page_jump_button = QPushButton("跳转")
        self.orders_page_jump_button.clicked.connect(lambda: self.jump_to_page('orders'))
        pagination_layout.addWidget(self.orders_page_jump_button)

        top_bar_layout.addLayout(pagination_layout)

        layout.addLayout(top_bar_layout)

        # 订单列表 (使用 QTableWidget)
        self.orders_table = QTableWidget()
        self.orders_table.setSelectionBehavior(QTableWidget.SelectRows)
        self.orders_table.setSelectionMode(QTableWidget.SingleSelection)
        self.orders_table.itemSelectionChanged.connect(self._on_order_selection_changed)
        self.orders_table.doubleClicked.connect(lambda: self.show_order_detail_popup()) # 双击查看详情

        layout.addWidget(self.orders_table)
        self.tab_widget.addTab(tab, "订单")

    def _on_order_selection_changed(self):
        selected_items = self.orders_table.selectedItems()
        if selected_items:
            self.selected_order_id = selected_items[0].data(Qt.UserRole)
        else:
            self.selected_order_id = None
        self.update_crud_button_states()

    def refresh_orders(self, auto_select_tab=True):
        if not self.redis_client:
            QMessageBox.critical(self, "错误", "Redis 未连接。请先连接。")
            return
        if auto_select_tab:
            self.tab_widget.setCurrentWidget(self.tab_widget.findChild(QWidget, "订单"))

        self.selected_order_id = None
        self.update_crud_button_states()

        self.run_in_thread(
            lambda: rc.get_all_orders(self.orders_current_page, self.page_size, self.orders_search_query),
            self._display_orders,
            self._handle_thread_error
        )

    def _display_orders(self, result):
        orders, total_items = result
        self.orders_total_pages = math.ceil(total_items / self.page_size) if total_items > 0 else 1
        self.orders_page_label.setText(f"页码: {self.orders_current_page}/{self.orders_total_pages}")

        headers = ['订单ID', '用户ID', '总金额', '国家', '订单日期', '状态']
        self.orders_table.setColumnCount(len(headers))
        self.orders_table.setHorizontalHeaderLabels(headers)
        self.orders_table.setRowCount(len(orders))

        for row_idx, o in enumerate(orders):
            order_id = o.get('order_id', '')
            display_id = order_id[:8] + '...' if len(order_id) > 10 else order_id
            user_id = o.get('user_id', '')
            display_user_id = user_id[:8] + '...' if len(user_id) > 10 else user_id
            total_amount = f"{o.get('total_amount', 0):.2f}"
            country = o.get('country', '')
            order_date = o.get('order_date', '')
            status = o.get('status', '')

            item_id = QTableWidgetItem(display_id)
            item_id.setData(Qt.UserRole, order_id)
            self.orders_table.setItem(row_idx, 0, item_id)
            self.orders_table.setItem(row_idx, 1, QTableWidgetItem(display_user_id))
            self.orders_table.setItem(row_idx, 2, QTableWidgetItem(total_amount))
            self.orders_table.setItem(row_idx, 3, QTableWidgetItem(country))
            self.orders_table.setItem(row_idx, 4, QTableWidgetItem(order_date))
            self.orders_table.setItem(row_idx, 5, QTableWidgetItem(status))
        
        self.orders_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.orders_table.resizeRowsToContents()

    def show_order_detail_popup(self): # 双击表格行或点击按钮触发
        if not self.redis_client:
            QMessageBox.critical(self, "错误", "Redis 未连接。请先连接。")
            return
        if not self.selected_order_id:
            QMessageBox.warning(self, "订单详情", "请先从列表中选择一个订单。")
            return
        
        order_details, order_items = rc.get_order_details_with_items(self.selected_order_id)
        if order_details:
            detail_str = f"订单ID: {order_details.get('order_id')}\n" \
                         f"用户ID: {order_details.get('user_id')}\n" \
                         f"总金额: {float(order_details.get('total_amount', 0)):.2f}\n" \
                         f"国家: {order_details.get('country')}\n" \
                         f"订单日期: {order_details.get('order_date')}\n" \
                         f"状态: {order_details.get('status')}\n\n"
            
            items_str = "订单商品明细:\n"
            if order_items:
                for item in order_items:
                    items_str += f"  - 商品编码: {item.get('StockCode')}, 描述: {item.get('Description')}, 数量: {item.get('Quantity')}, 单价: {item.get('UnitPrice'):.2f}, 总价: {item.get('total_price'):.2f}\n"
            else:
                items_str += "  - 此订单无商品明细。"
            
            dialog = DetailDisplayDialog(self, title=f"订单详情 - {self.selected_order_id[:8]}...", data=None) # 传入None，手动设置文本
            dialog.text_display.setText(detail_str + items_str)
            dialog.exec_()
        else:
            QMessageBox.critical(self, "错误", "未找到订单详情。")

    def edit_order_status_dialog(self):
        if not self.redis_client:
            QMessageBox.critical(self, "错误", "Redis 未连接。请先连接。")
            return
        if not self.selected_order_id:
            QMessageBox.warning(self, "编辑订单", "请先从列表中选择一个订单。")
            return
        
        order_details, _ = rc.get_order_details_with_items(self.selected_order_id)
        if not order_details:
            QMessageBox.critical(self, "错误", "未找到选中订单的详情。")
            return

        dialog = EditOrderStatusDialog(self, title="编辑订单状态", current_status=order_details.get('status', '未知'))
        if dialog.exec_() == QDialog.Accepted:
            new_status = dialog.result
            self.run_in_thread(lambda: rc.update_order_status(self.selected_order_id, new_status), self._handle_edit_order_result, self._handle_thread_error)

    def _handle_edit_order_result(self, result):
        msg, success = result
        if success:
            QMessageBox.information(self, "编辑订单", msg)
            self.refresh_orders(auto_select_tab=False)
        else:
            QMessageBox.critical(self, "编辑订单", msg)

    def delete_order(self):
        if not self.redis_client:
            QMessageBox.critical(self, "错误", "Redis 未连接。请先连接。")
            return
        if not self.selected_order_id:
            QMessageBox.warning(self, "删除订单", "请先从列表中选择一个订单。")
            return
        
        reply = QMessageBox.question(self, "确认删除", f"您确定要删除订单 ID: {self.selected_order_id[:8]}... 吗？此操作不可逆！",
                                     QMessageBox.Yes | QMessageBox.No, QMessageBox.No)
        if reply == QMessageBox.Yes:
            self.run_in_thread(lambda: rc.delete_order(self.selected_order_id), self._handle_delete_order_result, self._handle_thread_error)

    def _handle_delete_order_result(self, result):
        msg, success = result
        if success:
            QMessageBox.information(self, "删除订单", msg)
            self.selected_order_id = None
            self.refresh_orders(auto_select_tab=False)
        else:
            QMessageBox.critical(self, "删除订单", msg)

    # --- 数据分析 Tab ---
    def create_analysis_tab(self):
        tab = QWidget()
        main_layout = QVBoxLayout(tab)

        analysis_group = QGroupBox("数据分析")
        analysis_group_layout = QVBoxLayout(analysis_group)

        self.run_analysis_button = QPushButton("运行数据分析")
        self.run_analysis_button.clicked.connect(self.run_analysis)
        analysis_group_layout.addWidget(self.run_analysis_button)

        # 在分析Tab内部创建子TabWidget
        self.analysis_sub_tab_widget = QTabWidget()
        analysis_group_layout.addWidget(self.analysis_sub_tab_widget)

        # 文字报告子标签页
        text_report_tab = QWidget()
        text_report_layout = QVBoxLayout(text_report_tab)
        self.analysis_output = QTextEdit()
        self.analysis_output.setReadOnly(True)
        self.analysis_output.setFont(QFont("Monospace", 10))
        text_report_layout.addWidget(self.analysis_output)
        self.analysis_sub_tab_widget.addTab(text_report_tab, "文字报告")

        # 可视化图表子标签页
        charts_tab = QWidget()
        charts_layout = QVBoxLayout(charts_tab)
        
        # 使用 QHBoxLayout 放置多个图表
        charts_grid_layout = QHBoxLayout() # 使用网格布局更灵活
        
        # 1. 热门分类 (饼图)
        self.category_canvas = MplCanvas(self, width=5, height=4)
        charts_grid_layout.addWidget(self.category_canvas)

        # 2. 热门销售商品 (柱状图)
        self.top_selling_canvas = MplCanvas(self, width=5, height=4)
        charts_grid_layout.addWidget(self.top_selling_canvas)

        charts_layout.addLayout(charts_grid_layout)

        # 3. 月销售额趋势 (折线图)
        self.monthly_sales_canvas = MplCanvas(self, width=10, height=5) # 尺寸更大
        charts_layout.addWidget(self.monthly_sales_canvas)

        self.analysis_sub_tab_widget.addTab(charts_tab, "可视化图表")

        main_layout.addWidget(analysis_group)
        self.tab_widget.addTab(tab, "数据分析")

    def run_analysis(self):
        if not self.redis_client:
            QMessageBox.critical(self, "错误", "Redis 未连接。请先连接。")
            return
        self.update_log(self.analysis_output, "正在运行数据分析...", clear_first=True)
        self.run_in_thread(rc.analyze_data_from_redis, self._display_analysis_results, self._handle_thread_error)

    def _display_analysis_results(self, analysis_results):
        if "error" in analysis_results:
            self.update_log(self.analysis_output, f"错误: {analysis_results['error']}")
            QMessageBox.critical(self, "分析错误", analysis_results['error'])
        else:
            output_str = f"--- 分析结果 ({datetime.now().strftime('%Y-%m-%d %H:%M:%S')}) ---\n"
            output_str += f"总商品数: {analysis_results.get('total_products', 0)}\n"
            output_str += f"总分类数: {analysis_results.get('total_categories', 0)}\n"
            output_str += "\n热门分类 (按商品数量):\n"
            for cat, count in analysis_results.get('top_categories', []):
                output_str += f"  - {cat.capitalize()}: {count} 种商品\n"
            output_str += "\n价格最高的 5 个商品:\n"
            for prod in analysis_results.get('top_priced_products', []):
                output_str += f"  - 名称: {prod.get('name', 'N/A')}, 价格: {prod.get('price', 'N/A')}\n"
            output_str += "\n低库存商品预警 (库存 < 10):\n"
            if analysis_results.get('low_stock_products'):
                for prod in analysis_results['low_stock_products']: # 修复此处
                    output_str += f"  - ID: {prod['id'][:8]}..., 名称: {prod['name']}, 库存: {prod['stock']}\n"
            else:
                output_str += "  - 无低库存商品。\n"
            output_str += "\n热门销售商品:\n"
            for prod in analysis_results.get('top_selling_products', []):
                output_str += f"  - 名称: {prod.get('name', 'N/A')}, 销售数量: {prod.get('sales_count', 0)}\n"
            output_str += "\n最近登录的 5 个用户:\n"
            for user in analysis_results.get('recent_logins', []):
                output_str += f"  - 用户名: {user.get('username', 'N/A')}, 最后登录: {user.get('last_login', 'N/A')}\n"

            self.update_log(self.analysis_output, output_str, clear_first=True)
            QMessageBox.information(self, "数据分析", "分析完成!")

            # 绘图
            self._plot_analysis_results(analysis_results)

    def _plot_analysis_results(self, analysis_results):
        # 1. 热门分类 (饼图)
        self.category_canvas.axes.clear()
        categories = [item[0] for item in analysis_results.get('top_categories', [])]
        counts = [item[1] for item in analysis_results.get('top_categories', [])]
        if categories:
            ax = self.category_canvas.axes
            ax.pie(counts, labels=categories, autopct='%1.1f%%', startangle=90, textprops={'color': 'black'}) # 文字黑色
            ax.set_title('商品分类分布', color='black') # 标题黑色
        else:
            self.category_canvas.axes.text(0.5, 0.5, '无分类数据', horizontalalignment='center', verticalalignment='center', transform=self.category_canvas.axes.transAxes, color='black') # 文字黑色
        self.category_canvas.set_plot_style(bg_color='white', text_color='black', grid_color='#cccccc') # 背景白色，文字黑色
        self.category_canvas.draw()


        # 2. 热门销售商品 (柱状图)
        self.top_selling_canvas.axes.clear()
        top_selling_names = [item['name'] for item in analysis_results.get('top_selling_products', [])]
        top_selling_counts = [item['sales_count'] for item in analysis_results.get('top_selling_products', [])]
        if top_selling_names:
            ax = self.top_selling_canvas.axes
            ax.bar(top_selling_names, top_selling_counts, color='skyblue')
            ax.set_title('热门销售商品', color='black') # 标题黑色
            ax.set_ylabel('销售数量', color='black') # Y轴标签黑色
            ax.tick_params(axis='x', rotation=45, labelsize=8, colors='black') # X轴刻度文字黑色
            ax.tick_params(axis='y', colors='black') # Y轴刻度文字黑色
        else:
            self.top_selling_canvas.axes.text(0.5, 0.5, '无销售数据', horizontalalignment='center', verticalalignment='center', transform=self.top_selling_canvas.axes.transAxes, color='black') # 文字黑色
        self.top_selling_canvas.set_plot_style(bg_color='white', text_color='black', grid_color='#cccccc') # 背景白色，文字黑色
        self.top_selling_canvas.draw()


        # 3. 月销售额趋势 (折线图)
        self.monthly_sales_canvas.axes.clear()
        monthly_sales_data = analysis_results.get('monthly_sales_trend', [])
        if monthly_sales_data:
            months = [item[0] for item in monthly_sales_data]
            sales = [item[1] for item in monthly_sales_data]
            ax = self.monthly_sales_canvas.axes
            ax.plot(months, sales, marker='o', color='lightcoral')
            ax.set_title('月销售额趋势', color='black') # 标题黑色
            ax.set_xlabel('月份', color='black') # X轴标签黑色
            ax.set_ylabel('销售额', color='black') # Y轴标签黑色
            ax.tick_params(axis='x', rotation=45, labelsize=8, colors='black') # X轴刻度文字黑色
            ax.tick_params(axis='y', colors='black') # Y轴刻度文字黑色
        else:
            self.monthly_sales_canvas.axes.text(0.5, 0.5, '无月销售额数据', horizontalalignment='center', verticalalignment='center', transform=self.monthly_sales_canvas.axes.transAxes, color='black') # 文字黑色
        self.monthly_sales_canvas.set_plot_style(bg_color='white', text_color='black', grid_color='#cccccc') # 背景白色，文字黑色
        self.monthly_sales_canvas.draw()


    def change_page(self, item_type, direction):
        if item_type == 'products':
            new_page = self.products_current_page + direction
            if 1 <= new_page <= self.products_total_pages:
                self.products_current_page = new_page
                self.refresh_products(auto_select_tab=False)
        elif item_type == 'users':
            new_page = self.users_current_page + direction
            if 1 <= new_page <= self.users_total_pages:
                self.users_current_page = new_page
                self.refresh_users(auto_select_tab=False)
        elif item_type == 'orders':
            new_page = self.orders_current_page + direction
            if 1 <= new_page <= self.orders_total_pages:
                self.orders_current_page = new_page
                self.refresh_orders(auto_select_tab=False)

    def search_items(self, item_type):
        if item_type == 'products':
            self.products_search_query = self.products_search_input.text()
            self.products_current_page = 1 # 搜索时重置到第一页
            self.refresh_products(auto_select_tab=False)
        elif item_type == 'users':
            self.users_search_query = self.users_search_input.text()
            self.users_current_page = 1
            self.refresh_users(auto_select_tab=False)
        elif item_type == 'orders':
            self.orders_search_query = self.orders_search_input.text()
            self.orders_current_page = 1
            self.refresh_orders(auto_select_tab=False)

    def jump_to_page(self, item_type):
        try:
            if item_type == 'products':
                page_str = self.products_page_jump_input.text()
                new_page = int(page_str)
                if 1 <= new_page <= self.products_total_pages:
                    self.products_current_page = new_page
                    self.refresh_products(auto_select_tab=False)
                else:
                    QMessageBox.warning(self, "页码跳转", f"请输入有效的页码 (1 到 {self.products_total_pages})。")
            elif item_type == 'users':
                page_str = self.users_page_jump_input.text()
                new_page = int(page_str)
                if 1 <= new_page <= self.users_total_pages:
                    self.users_current_page = new_page
                    self.refresh_users(auto_select_tab=False)
                else:
                    QMessageBox.warning(self, "页码跳转", f"请输入有效的页码 (1 到 {self.users_total_pages})。")
            elif item_type == 'orders':
                page_str = self.orders_page_jump_input.text()
                new_page = int(page_str)
                if 1 <= new_page <= self.orders_total_pages:
                    self.orders_current_page = new_page
                    self.refresh_orders(auto_select_tab=False)
                else:
                    QMessageBox.warning(self, "页码跳转", f"请输入有效的页码 (1 到 {self.orders_total_pages})。")
        except ValueError:
            QMessageBox.warning(self, "页码跳转", "请输入一个有效的数字作为页码。")


if __name__ == "__main__":
    app = QApplication(sys.argv)
    app.setStyle('Fusion') # 推荐使用Fusion风格，因为它在不同平台上表现一致且支持QSS
    
    window = REDMApp()
    window.show()
    sys.exit(app.exec_())