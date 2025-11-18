import pandas as pd
import os

# --- CONFIGURATION ---
INPUT_FILE = "retail_data_delta.csv"  # Your file name
OUTPUT_FOLDER = "processed_data"

if not os.path.exists(OUTPUT_FOLDER):
    os.makedirs(OUTPUT_FOLDER)

print(f"Reading {INPUT_FILE}...")

# 1. Load the raw data
# We use 'header=None' if your file has no column names, based on your error message it looks headless.
# If it HAS headers, remove 'header=None' and use the actual names.
# Below I am mapping the columns based on the snippet you shared.
column_names = [
    "transaction_id", "customer_id", "customer_name", "email", "phone", 
    "address", "city", "state", "zip", "country", "age", "gender", 
    "income", "loyalty_status", "date", "year", "month", "time", 
    "quantity", "unit_price", "total_amount", "category", "brand", 
    "product_type", "shipping_speed", "shipping_method", "payment", 
    "status", "store_id", "product_name"
]

# NOTE: Adjust 'names' if your CSV actually has a header row
df = pd.read_csv(INPUT_FILE, names=column_names, low_memory=False)

print("Data loaded. Splitting into entities...")

# --- 2. CREATE CUSTOMERS TABLE ---
# Extract unique customers and rename columns to match the PDF schema [cite: 52]
customers = df[[
    "customer_id", "customer_name", "email", "phone", "loyalty_status"
]].drop_duplicates(subset=["customer_id"])

# Split name into First Name (Rough approximation)
customers["first_name"] = customers["customer_name"].apply(lambda x: x.split(" ")[0] if isinstance(x, str) else "Unknown")
customers = customers[["customer_id", "first_name", "email", "loyalty_status", "phone"]]
customers.columns = ["customer_id", "first_name", "email", "loyalty_status", "customer_phone"]

# Calculate 'total_loyalty_points' (Placeholder - you calculate this in Use Case 1)
customers["total_loyalty_points"] = 0 

customers.to_csv(f"{OUTPUT_FOLDER}/customer_details.csv", index=False)
print(f"✔ Created customer_details.csv ({len(customers)} rows)")

# --- 3. CREATE PRODUCTS TABLE ---
# Extract unique products [cite: 49]
products = df[["product_name", "category", "unit_price"]].drop_duplicates(subset=["product_name"])
products["product_id"] = range(1, len(products) + 1) # Generate ID
products["current_stock_level"] = 100 # Mock value for inventory
products.columns = ["product_name", "category", "price", "product_id", "current_stock_level"]

# Reorder to match schema
products = products[["product_id", "product_name", "category", "price", "current_stock_level"]]

products.to_csv(f"{OUTPUT_FOLDER}/products.csv", index=False)
print(f"✔ Created products.csv ({len(products)} rows)")

# --- 4. CREATE SALES HEADER TABLE ---
# [cite: 58]
sales = df[["transaction_id", "customer_id", "store_id", "date", "total_amount", "phone"]]
sales.columns = ["transaction_id", "customer_id", "store_id", "transaction_date", "total_amount", "customer_phone"]

sales.to_csv(f"{OUTPUT_FOLDER}/store_sales_header.csv", index=False)
print(f"✔ Created store_sales_header.csv ({len(sales)} rows)")

print("\nProcessing Complete. Files are in the 'processed_data' folder.")