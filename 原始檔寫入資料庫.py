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
# 📂 CSV 讀取
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
# 🔢 型別簡單對應
# =========================
type_map = {col: "VARCHAR(255)" for col in df.columns}  # 全部當字串型別，可視需要調整

# =========================
# 🏗 建表（可選 PK）
# =========================
table_name = "ecommerce_raw"
# 刪除舊表（如存在）
cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE [{table_name}]")
conn.commit()

columns_sql = ",\n".join([f"[{col}] {typ}" for col, typ in type_map.items()])
sql = f"CREATE TABLE [{table_name}] (\n{columns_sql}\n)"
cursor.execute(sql)
conn.commit()
print(f"✅ 建立資料表: {table_name}")

# =========================
# ⚡ 分批寫入
# =========================
batch_size = 1000
total_rows = len(df)
batches = ceil(total_rows / batch_size)
cursor.fast_executemany = True
rows_written = 0

columns = ",".join(f"[{col}]" for col in df.columns)
placeholders = ",".join("?" for _ in df.columns)
sql_insert = f"INSERT INTO [{table_name}] ({columns}) VALUES ({placeholders})"

for i in range(batches):
    batch_df = df.iloc[i*batch_size:(i+1)*batch_size]
    data = [tuple(row) for row in batch_df.fillna('').values]
    cursor.executemany(sql_insert, data)
    conn.commit()
    rows_written += len(batch_df)
    percent = (rows_written / total_rows) * 100
    print(f"⏳ {table_name}: {rows_written}/{total_rows} 筆 ({percent:.2f}%)", end='\r')

print(f"\n✅ 全部寫入完成: {table_name}")