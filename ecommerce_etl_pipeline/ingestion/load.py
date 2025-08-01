import pandas as pd
from sqlalchemy import create_engine
import os

csv_path = "data/processed/user_cart_products.csv"
df = pd.read_csv(csv_path)

DB_USER = "ecommerce1_user"
DB_PASSWORD = "321Admin"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "ecommerce1"

conn_str = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(conn_str)

table_name = "user_cart_products"

df.to_sql(table_name, engine, schema="my_schema", if_exists="append", index=False)


print(f"Loaded {len(df)} rows into '{table_name}' table in database '{DB_NAME}'")
