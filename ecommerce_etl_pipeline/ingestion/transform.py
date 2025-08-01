import pandas as pd
import os
from datetime import datetime

os.makedirs("data/processed", exist_ok=True)

def transform_all_data():
    users = pd.read_csv("data/raw/users.csv")
    products = pd.read_csv("data/raw/products.csv")
    carts = pd.read_csv("data/raw/carts.csv")

    products.rename(columns={"id": "product_id"}, inplace=True)
    users.rename(columns={"id": "user_id"}, inplace=True)
    carts.rename(columns={"id": "cart_id"}, inplace=True)

    carts['products'] = carts['products'].apply(eval)

    exploded_rows = []
    for _, row in carts.iterrows():
        for item in row['products']:
            exploded_rows.append({
                "cart_id": row["cart_id"],
                "user_id": row["userId"],
                "date": row["date"],
                "product_id": item["productId"],
                "quantity": item["quantity"]
            })
    cart_products_df = pd.DataFrame(exploded_rows)

    enriched = cart_products_df.merge(products, on="product_id", how="left")

    enriched = enriched.merge(users, on="user_id", how="left", suffixes=('', '_user'))

    enriched['total_price'] = enriched['price'] * enriched['quantity']
    enriched['transformed_at'] = datetime.utcnow()

    return enriched

if __name__ == "__main__":
    df = transform_all_data()
    df.to_csv("data/processed/user_cart_products.csv", index=False)
    print(" Transformed & joined data saved to data/processed/user_cart_products.csv")
