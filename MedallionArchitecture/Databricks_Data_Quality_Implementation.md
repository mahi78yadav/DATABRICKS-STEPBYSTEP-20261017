# Data Quality Implementation — Azure Databricks Retail Lakehouse

## 1. Objective

Add a data-quality notebook to your `DATABRICKS-STEPBYSTEP-20261017` project. It validates Silver-layer data for `customers`, `products`, `stores`, and `orders`.

Reference notebook: [05_Data_Quality.py](https://github.com/mahi78yadav/azure-databricks-retail-lakehouse/blob/main/notebooks/05_Data_Quality.py)

**Important project adaptation:** This guide uses your catalog `retailuat` and Unity Catalog table names. It does not use the reference repository's storage account paths or hard-coded storage keys. Do not add `spark.conf.set("fs.azure.account.key...")`; use your existing Unity Catalog credentials and external locations.

## 2. Prerequisites

Confirm these Silver tables exist:

- `retailuat.silver.customers`
- `retailuat.silver.products`
- `retailuat.silver.stores`
- `retailuat.silver.orders`

The `orders` table is expected to contain `order_id`, `customer_id`, `product_id`, `store_id`, `order_date`, `quantity`, `order_status`, and `payment_method`. Update the code if your schema uses different names.

## 3. Create the notebook

1. In Databricks, create a Python notebook named `05_Data_Quality`.
2. Select your existing Serverless compute.
3. Paste the complete code below into one Python cell and run it.
4. Review the check results and final status.

## 4. Complete notebook code

```python
from pyspark.sql import functions as F

# Step 1: Load Silver tables using Unity Catalog
customers = spark.table("retailuat.silver.customers")
products  = spark.table("retailuat.silver.products")
stores    = spark.table("retailuat.silver.stores")
orders    = spark.table("retailuat.silver.orders")

print("Silver tables loaded successfully")

# Validate required columns before running checks
required_columns = {
    "orders": [
        "order_id", "customer_id", "product_id", "store_id",
        "order_date", "quantity", "order_status", "payment_method"
    ],
    "customers": ["customer_id"],
    "products": ["product_id"],
    "stores": ["store_id"]
}
dataframes = {
    "orders": orders, "customers": customers,
    "products": products, "stores": stores
}
for table_name, expected in required_columns.items():
    missing = sorted(set(expected) - set(dataframes[table_name].columns))
    if missing:
        raise ValueError(
            f"{table_name} is missing columns {missing}. "
            f"Available columns: {dataframes[table_name].columns}"
        )

# Step 2: Check NULLs in critical order columns
critical_columns = [
    "order_id", "customer_id", "product_id", "store_id",
    "order_date", "quantity", "order_status", "payment_method"
]
null_check = orders.agg(*[
    F.sum(F.col(c).isNull().cast("long")).alias(f"null_{c}")
    for c in critical_columns
])
display(null_check)
null_counts = null_check.first().asDict()
null_order_id_count = null_counts.get("null_order_id", 0) or 0

# Step 3: Duplicate or NULL order IDs
duplicate_orders = (
    orders.groupBy("order_id").count()
          .filter((F.col("count") > 1) | F.col("order_id").isNull())
)
duplicate_order_count = duplicate_orders.count()
print("Duplicate/null order-ID groups:", duplicate_order_count)
display(duplicate_orders)

# Step 4: Invalid quantities
invalid_quantity = orders.filter(
    F.col("quantity").isNull() | (F.col("quantity") <= 0)
)
invalid_quantity_count = invalid_quantity.count()
print("Invalid quantity records:", invalid_quantity_count)
display(invalid_quantity)

# Step 5: Invalid order status values — adjust to match your data
valid_statuses = ["COMPLETED", "CANCELLED", "PENDING"]
invalid_status = orders.filter(
    F.col("order_status").isNull()
    | ~F.upper(F.trim(F.col("order_status"))).isin(valid_statuses)
)
invalid_status_count = invalid_status.count()
print("Invalid order-status records:", invalid_status_count)
display(invalid_status)

# Step 6: Invalid payment methods — adjust to match your data
valid_payment_methods = ["UPI", "CREDIT CARD", "DEBIT CARD", "CASH"]
invalid_payment = orders.filter(
    F.col("payment_method").isNull()
    | ~F.upper(F.trim(F.col("payment_method"))).isin(valid_payment_methods)
)
invalid_payment_count = invalid_payment.count()
print("Invalid payment-method records:", invalid_payment_count)
display(invalid_payment)

# Step 7: Invalid customer references
invalid_customers = orders.join(
    customers.select("customer_id").distinct(),
    on="customer_id", how="left_anti"
)
invalid_customer_count = invalid_customers.count()
print("Orders with invalid customer IDs:", invalid_customer_count)
display(invalid_customers)

# Step 8: Invalid product references
invalid_products = orders.join(
    products.select("product_id").distinct(),
    on="product_id", how="left_anti"
)
invalid_product_count = invalid_products.count()
print("Orders with invalid product IDs:", invalid_product_count)
display(invalid_products)

# Step 9: Invalid store references
invalid_stores = orders.join(
    stores.select("store_id").distinct(),
    on="store_id", how="left_anti"
)
invalid_store_count = invalid_stores.count()
print("Orders with invalid store IDs:", invalid_store_count)
display(invalid_stores)

# Step 10: Future-dated orders
future_orders = orders.filter(F.col("order_date") > F.current_date())
future_order_count = future_orders.count()
print("Future-dated orders:", future_order_count)
display(future_orders)

# Step 11: Summary
total_orders = orders.count()
summary_df = spark.createDataFrame([
    ("Total orders", total_orders),
    ("Null order IDs", null_order_id_count),
    ("Duplicate/null order-ID groups", duplicate_order_count),
    ("Invalid quantities", invalid_quantity_count),
    ("Invalid statuses", invalid_status_count),
    ("Invalid payment methods", invalid_payment_count),
    ("Invalid customer references", invalid_customer_count),
    ("Invalid product references", invalid_product_count),
    ("Invalid store references", invalid_store_count),
    ("Future-dated orders", future_order_count)
], ["quality_check", "issue_count"])
display(summary_df)

quality_passed = all(count == 0 for count in [
    null_order_id_count, duplicate_order_count, invalid_quantity_count,
    invalid_status_count, invalid_payment_count, invalid_customer_count,
    invalid_product_count, invalid_store_count, future_order_count
])
print("DATA QUALITY STATUS:", "PASS" if quality_passed else "FAIL")

# Step 12: Add quality flags and split valid/rejected orders
orders_flagged = (
    orders
    .withColumn("dq_null_order_id", F.col("order_id").isNull())
    .withColumn(
        "dq_invalid_quantity",
        F.col("quantity").isNull() | (F.col("quantity") <= 0)
    )
    .withColumn(
        "dq_invalid_status",
        F.col("order_status").isNull()
        | ~F.upper(F.trim(F.col("order_status"))).isin(valid_statuses)
    )
    .withColumn(
        "dq_invalid_payment",
        F.col("payment_method").isNull()
        | ~F.upper(F.trim(F.col("payment_method"))).isin(valid_payment_methods)
    )
    .withColumn("dq_future_order_date", F.col("order_date") > F.current_date())
    .join(
        customers.select("customer_id").distinct()
                 .withColumn("dq_customer_found", F.lit(True)),
        on="customer_id", how="left"
    )
    .join(
        products.select("product_id").distinct()
                .withColumn("dq_product_found", F.lit(True)),
        on="product_id", how="left"
    )
    .join(
        stores.select("store_id").distinct()
              .withColumn("dq_store_found", F.lit(True)),
        on="store_id", how="left"
    )
    .fillna({
        "dq_customer_found": False,
        "dq_product_found": False,
        "dq_store_found": False
    })
    .withColumn(
        "dq_is_valid",
        ~(
            F.col("dq_null_order_id")
            | F.col("dq_invalid_quantity")
            | F.col("dq_invalid_status")
            | F.col("dq_invalid_payment")
            | F.col("dq_future_order_date")
            | ~F.col("dq_customer_found")
            | ~F.col("dq_product_found")
            | ~F.col("dq_store_found")
        )
    )
)

valid_orders = orders_flagged.filter(F.col("dq_is_valid"))
rejected_orders = orders_flagged.filter(~F.col("dq_is_valid"))

valid_count = valid_orders.count()
rejected_count = rejected_orders.count()
print("Total orders:", total_orders)
print("Valid orders:", valid_count)
print("Rejected orders:", rejected_count)
print("Valid + rejected:", valid_count + rejected_count)
print(
    "RECONCILIATION STATUS:",
    "PASS" if total_orders == valid_count + rejected_count else "FAIL"
)
display(rejected_orders)

# Step 13: Save outputs as separate Unity Catalog Delta tables.
# This does not overwrite the original Silver orders table.
valid_orders.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable("retailuat.silver.validated_orders")

rejected_orders.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable("retailuat.silver.rejected_orders")

# Step 14: Verify saved outputs
validated_count = spark.table("retailuat.silver.validated_orders").count()
rejected_saved_count = spark.table("retailuat.silver.rejected_orders").count()
print("Validated orders in Silver:", validated_count)
print("Rejected orders in Silver:", rejected_saved_count)
print(
    "SAVED-OUTPUT RECONCILIATION:",
    "PASS" if total_orders == validated_count + rejected_saved_count else "FAIL"
)
```

## 5. Important notes

- **Status/payment values:** Change `valid_statuses` and `valid_payment_methods` if your CSV files use different values. Otherwise, valid records could be incorrectly rejected.
- **Duplicate IDs:** The notebook reports duplicate order IDs. It does not automatically remove duplicates; review them and decide on a deduplication rule.
- **Table names:** If your Silver tables are stored as Delta paths but are not registered in Unity Catalog, register them first or update the four `spark.table(...)` lines to match your actual registered tables.
- **Output behavior:** Each run overwrites only `retailuat.silver.validated_orders` and `retailuat.silver.rejected_orders`. It does not overwrite `retailuat.silver.orders`.
- **Permissions:** Your user/compute needs permission to read the Silver tables and create/modify the two output tables.

## 6. Validation checklist

- [ ] All four Silver tables load successfully.
- [ ] Every data-quality check runs and displays results.
- [ ] `DATA QUALITY STATUS` is reviewed.
- [ ] `RECONCILIATION STATUS` is `PASS`.
- [ ] `retailuat.silver.validated_orders` and `retailuat.silver.rejected_orders` exist.
- [ ] Saved output counts reconcile to the total number of orders.
- [ ] Commit the notebook and this guide to your GitHub repository after a successful run.
