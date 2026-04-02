import pandas as pd
import pyodbc
import os
from math import ceil

# =========================
# 📁 路徑設定
# =========================
BASE_DIR = os.path.dirname(__file__)
DATA_DIR = os.path.join(BASE_DIR, "dataset")
data_file = os.path.join(DATA_DIR, "ecommerce_dataset_+1m.csv")

# =========================
# 🔗 SQL 連線
# =========================
server = 'linpeichunhappy.database.windows.net'
database = 'project'
username = 'missa'
password = 'Cc12345678'
driver = '{ODBC Driver 18 for SQL Server}'

conn = pyodbc.connect(
    f'DRIVER={driver};'
    f'SERVER={server};'
    f'DATABASE={database};'
    f'UID={username};'
    f'PWD={password};'
    'Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'
)
cursor = conn.cursor()
print("✅ SQL 連線成功")

# =========================
# 📂 CSV 安全讀取
# =========================
chunksize = 100000
chunks = pd.read_csv(data_file, dtype=str, chunksize=chunksize)
df = pd.concat(chunks, ignore_index=True)
df.columns = df.columns.str.strip().str.lower()
print(f"✅ 原始資料筆數: {len(df)}")

# =========================
# 🧼 資料清洗
# =========================
def clean_dataframe(df):
    df = df.copy()
    for col in df.columns:
        df[col] = df[col].astype(str).str.strip()
    df = df.replace({'nan': None, 'None': None})
    return df

df = clean_dataframe(df)

# =========================
# 🏗 拆表（35個欄位）
# =========================
df_customers = df[['customer_id', 'customer_name', 'gender', 'age', 
                   'customer_segment', 'country', 'city', 
                   'total_orders_by_customer', 'account_creation_date']].drop_duplicates(subset=['customer_id'])

df_products = df[['product_id', 'product_name', 'category', 'sub_category', 
                  'brand', 'product_rating_avg', 'product_reviews_count', 
                  'stock_quantity', 'unit_price_usd', 'cost_usd']].drop_duplicates(subset=['product_id'])

df_orders = df[['order_id', 'order_date', 'return_reason', 'customer_id']].drop_duplicates(subset=['order_id'])

df_order_items = df[['order_id', 'product_id', 'quantity', 'unit_price_usd', 
                     'discount_percent', 'total_price_usd', 'cost_usd', 'profit_margin_percent']].copy()
df_order_items['order_item_id'] = range(1, len(df_order_items)+1)

df_payments = df[['order_id', 'payment_method']].drop_duplicates(subset=['order_id']).copy()
df_payments['payment_id'] = range(1, len(df_payments)+1)

df_shipping = df[['order_id', 'shipping_method', 'shipping_cost_usd', 
                  'delivery_days', 'shipping_country']].drop_duplicates(subset=['order_id']).copy()
df_shipping['shipping_id'] = range(1, len(df_shipping)+1)

df_promotions = df[['order_id', 'coupon_used', 'coupon_code', 'campaign_source']].drop_duplicates(subset=['order_id']).copy()
df_promotions['promotion_id'] = range(1, len(df_promotions)+1)

df_sessions = df[['order_id', 'traffic_source']].drop_duplicates(subset=['order_id']).copy()
df_sessions['session_id'] = range(1, len(df_sessions)+1)

# =========================
# 🏷 表格字典 & 主鍵
# =========================
tables = {
    "customers": df_customers,
    "products": df_products,
    "orders": df_orders,
    "order_items": df_order_items,
    "payments": df_payments,
    "shipping": df_shipping,
    "promotions": df_promotions,
    "sessions": df_sessions
}

primary_keys = {
    "customers": "customer_id",
    "products": "product_id",
    "orders": "order_id",
    "order_items": "order_item_id",
    "payments": "payment_id",
    "shipping": "shipping_id",
    "promotions": "promotion_id",
    "sessions": "session_id"
}

# =========================
# 💾 輸出 CSV
# =========================
OUTPUT_DIR = os.path.join(BASE_DIR, "output")
os.makedirs(OUTPUT_DIR, exist_ok=True)

for table_name, df_table in tables.items():
    csv_path = os.path.join(OUTPUT_DIR, f"{table_name}.csv")
    df_table.to_csv(csv_path, index=False)
    print(f"✅ 已輸出 {table_name} CSV: {csv_path}")

# =========================
# 🔢 型別對應（更新後）
# =========================
type_map = {
    "customer_id": "VARCHAR(50)", "customer_name": "VARCHAR(255)", "gender": "VARCHAR(10)", "age": "INT",
    "customer_segment": "VARCHAR(50)", "country": "VARCHAR(50)", "city": "VARCHAR(50)",
    "total_orders_by_customer": "INT", "account_creation_date": "DATETIME",
    "product_id": "VARCHAR(50)", "product_name": "VARCHAR(255)", "category": "VARCHAR(50)", "sub_category": "VARCHAR(50)",
    "brand": "VARCHAR(50)", "product_rating_avg": "DECIMAL(3,2)", "product_reviews_count": "INT", "stock_quantity": "INT",
    "unit_price_usd": "DECIMAL(18,2)", "cost_usd": "DECIMAL(18,2)", "profit_margin_percent": "DECIMAL(5,2)",
    "order_id": "VARCHAR(50)", "order_date": "DATETIME", "return_reason": "VARCHAR(255)", 
    "quantity": "INT", "discount_percent": "DECIMAL(5,2)", "total_price_usd": "DECIMAL(18,2)",
    "payment_method": "VARCHAR(50)", "shipping_method": "VARCHAR(50)", "shipping_cost_usd": "DECIMAL(18,2)",
    "delivery_days": "INT", "shipping_country": "VARCHAR(50)", 
    "coupon_used": "BIT", "coupon_code": "VARCHAR(50)", "campaign_source": "VARCHAR(50)", 
    "traffic_source": "VARCHAR(50)",
    "order_item_id": "INT", "payment_id": "INT", "shipping_id": "INT", "promotion_id": "INT", "session_id": "INT"
}

# =========================
# 🛑 刪除舊表
# =========================
drop_order = ["sessions", "promotions", "shipping", "payments", "order_items", "orders", "products", "customers"]
for table in drop_order:
    cursor.execute(f"IF OBJECT_ID('{table}', 'U') IS NOT NULL DROP TABLE [{table}]")
conn.commit()
print("✅ 舊表已刪除（如存在）")

# =========================
# 🏗 建表函數
# =========================
def create_table(table_name, df, pk=None):
    columns = []
    for col in df.columns:
        sql_type = type_map.get(col, "VARCHAR(255)")
        columns.append(f"[{col}] {sql_type}")
    if pk:
        columns.append(f"PRIMARY KEY ([{pk}])")
    columns_sql = ",\n".join(columns)
    sql = f"CREATE TABLE [{table_name}] (\n{columns_sql}\n)"
    cursor.execute(sql)
    conn.commit()
    print(f"✅ 建立資料表: {table_name}")

# =========================
# ⚡ 批次寫入函數（覆蓋進度顯示）
# =========================
def insert_dataframe(df, table_name, batch_size=1000):
    df = clean_dataframe(df)
    columns = ",".join(f"[{col}]" for col in df.columns)
    placeholders = ",".join("?" for _ in df.columns)
    sql = f"INSERT INTO [{table_name}] ({columns}) VALUES ({placeholders})"
    total_rows = len(df)
    batches = ceil(total_rows / batch_size)
    cursor.fast_executemany = True
    rows_written = 0

    for i in range(batches):
        batch_df = df.iloc[i*batch_size:(i+1)*batch_size]
        data = []
        for row in batch_df.itertuples(index=False, name=None):
            new_row = []
            for val, col in zip(row, batch_df.columns):
                if val is None or val == '':
                    new_row.append(None)
                else:
                    col_type = type_map.get(col, "VARCHAR(255)")
                    try:
                        if col_type == "INT":
                            new_row.append(int(float(val)))
                        elif col_type == "BIT":
                            new_row.append(1 if str(val).strip() in ['1','True','true'] else 0)
                        elif "DECIMAL" in col_type:
                            new_row.append(float(val))
                        elif col_type == "DATETIME":
                            new_row.append(pd.to_datetime(val))
                        else:
                            new_row.append(str(val))
                    except Exception:
                        if col_type == "INT":
                            new_row.append(float(val))
                        else:
                            new_row.append(str(val))
            data.append(tuple(new_row))
        try:
            cursor.executemany(sql, data)
            conn.commit()
            rows_written += len(batch_df)
            percent = (rows_written / total_rows) * 100
            # 同一行顯示進度
            print(f"⏳ {table_name}: {rows_written}/{total_rows} 筆 ({percent:.2f}%)", end='\r')
        except pyodbc.IntegrityError:
            print(f"⚠️ 批次 {i+1}/{batches} 有重複主鍵，已跳過")
    # 完成後換行
    print(f"✅ 寫入完成: {table_name}                              ")

# =========================
# 🔗 FK 建立
# =========================
def create_foreign_key(child_table, child_col, parent_table, parent_col):
    fk_name = f"FK_{child_table}_{child_col}"
    sql = f"""
    ALTER TABLE [{child_table}]
    ADD CONSTRAINT [{fk_name}]
    FOREIGN KEY ([{child_col}])
    REFERENCES [{parent_table}]([{parent_col}])
    """
    cursor.execute(sql)
    conn.commit()
    print(f"🔗 FK 建立: {child_table}.{child_col} → {parent_table}.{parent_col}")

# =========================
# 🚀 建表 + 寫入
# =========================
for table_name, df_table in tables.items():
    create_table(table_name, df_table, primary_keys.get(table_name))

for table_name, df_table in tables.items():
    insert_dataframe(df_table, table_name)

# =========================
# 🔗 建立 FK
# =========================
create_foreign_key("orders", "customer_id", "customers", "customer_id")
create_foreign_key("order_items", "order_id", "orders", "order_id")
create_foreign_key("order_items", "product_id", "products", "product_id")
create_foreign_key("payments", "order_id", "orders", "order_id")
create_foreign_key("shipping", "order_id", "orders", "order_id")
create_foreign_key("promotions", "order_id", "orders", "order_id")
create_foreign_key("sessions", "order_id", "orders", "order_id")

print("🎉 ETL 全流程完成！")

# =========================
# 📊 報表輸出（缺值 & 統計）
# =========================
for table_name, df_table in tables.items():
    missing_df = df_table.isnull().sum().reset_index()
    missing_df.columns = ['欄位', '缺值數量']
    missing_df['缺值比例'] = missing_df['缺值數量'] / len(df_table)
    csv_path = os.path.join(OUTPUT_DIR, f"{table_name}_missing.csv")
    missing_df.to_csv(csv_path, index=False)
    print(f"✅ 欄位缺值報告已輸出: {csv_path}")

missing_report_list = []
for table_name, df_table in tables.items():
    total_cells = df_table.size
    total_missing = df_table.isnull().sum().sum()
    missing_report_list.append({
        '表格': table_name,
        '總欄位數': df_table.shape[1],
        '總筆數': len(df_table),
        '缺值筆數': total_missing,
        '缺值比例': total_missing / total_cells
    })
missing_report_df = pd.DataFrame(missing_report_list)
csv_path = os.path.join(OUTPUT_DIR, "missing_report.csv")
missing_report_df.to_csv(csv_path, index=False)
print(f"✅ 缺值摘要報告已輸出: {csv_path}")

for table_name, df_table in tables.items():
    numeric_cols = df_table.select_dtypes(include=['number']).columns
    if len(numeric_cols) == 0:
        continue
    stats_df = df_table[numeric_cols].describe().transpose().reset_index()
    stats_df.rename(columns={'index': '欄位'}, inplace=True)
    csv_path = os.path.join(OUTPUT_DIR, f"{table_name}_stats.csv")
    stats_df.to_csv(csv_path, index=False)
    print(f"✅ 欄位統計報告已輸出: {csv_path}")

table_stats_list = []
for table_name, df_table in tables.items():
    table_stats_list.append({
        '表格': table_name,
        '總筆數': len(df_table),
        '欄位數量': df_table.shape[1],
        '缺值總數': df_table.isnull().sum().sum(),
        '欄位列表': ', '.join(df_table.columns)
    })
table_stats_df = pd.DataFrame(table_stats_list)
csv_path = os.path.join(OUTPUT_DIR, "table_stats.csv")
table_stats_df.to_csv(csv_path, index=False)
print(f"✅ 表格概況已輸出: {csv_path}")