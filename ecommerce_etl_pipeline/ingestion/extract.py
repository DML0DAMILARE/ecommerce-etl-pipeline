import os
import requests
import pandas as pd
from datetime import datetime

# ✅ Ensure folder exists
os.makedirs("data/raw", exist_ok=True)

def fetch_fakestore_data(endpoint):
    url = f"https://fakestoreapi.com/{endpoint}"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    
    df = pd.json_normalize(data)
    df['fetched_at'] = datetime.utcnow()
    return df

if __name__ == "__main__":
    products_df = fetch_fakestore_data("products")
    users_df = fetch_fakestore_data("users")
    carts_df = fetch_fakestore_data("carts")

    products_df.to_csv("data/raw/products.csv", index=False)
    users_df.to_csv("data/raw/users.csv", index=False)
    carts_df.to_csv("data/raw/carts.csv", index=False)

    print("✅ Data fetched and saved to data/raw/")
