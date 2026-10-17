# Silver Layer Implementation

## 1. Objective

Cleanse, standardize, deduplicate, and validate Bronze data before writing it to the Silver Delta layer.

## 2. Read Bronze Tables

**Cell 1 — Load the four Bronze tables from Unity Catalog.**

```python
catalog = "retailuat"

customers_bronze = spark.table(f"{catalog}.bronze.customers")
products_bronze  = spark.table(f"{catalog}.bronze.products")
stores_bronze    = spark.table(f"{catalog}.bronze.stores")
orders_bronze    = spark.table(f"{catalog}.bronze.orders")
```

## 3. Clean Customers

**Cell 2 — Trim text columns, standardize customer segments, and remove duplicate customer IDs.**

```python
from pyspark.sql.functions import col, trim, upper

customers_silver = (
    customers_bronze
    .withColumn("customer_id", trim(col("customer_id")))
    .withColumn("customer_name", trim(col("customer_name")))
    .withColumn("city", trim(col("city")))
    .withColumn("state", trim(col("state")))
    .withColumn("customer_segment", upper(trim(col("customer_segment"))))
    .filter(col("customer_id").isNotNull() & (col("customer_id") != ""))
    .dropDuplicates(["customer_id"])
)
```

## 4. Clean Products

**Cell 3 — Standardize product text, cast unit price, and remove duplicate product IDs.**

```python
products_silver = (
    products_bronze
    .withColumn("product_id", trim(col("product_id")))
    .withColumn("product_name", trim(col("product_name")))
    .withColumn("category", upper(trim(col("category"))))
    .withColumn("subcategory", trim(col("subcategory")))
    .withColumn("unit_price", col("unit_price").cast("decimal(12,2)"))
    .filter(col("product_id").isNotNull() & (col("product_id") != ""))
    .dropDuplicates(["product_id"])
)
```

## 5. Clean Stores

**Cell 4 — Standardize store text and region, and remove duplicate store IDs.**

```python
stores_silver = (
    stores_bronze
    .withColumn("store_id", trim(col("store_id")))
    .withColumn("store_name", trim(col("store_name")))
    .withColumn("city", trim(col("city")))
    .withColumn("state", trim(col("state")))
    .withColumn("region", upper(trim(col("region"))))
    .filter(col("store_id").isNotNull() & (col("store_id") != ""))
    .dropDuplicates(["store_id"])
)
```

## 6. Clean Orders

**Cell 5 — Standardize order text, cast quantity, and remove duplicate order IDs.**

```python
orders_silver = (
    orders_bronze
    .withColumn("order_id", trim(col("order_id")))
    .withColumn("customer_id", trim(col("customer_id")))
    .withColumn("product_id", trim(col("product_id")))
    .withColumn("store_id", trim(col("store_id")))
    .withColumn("order_status", upper(trim(col("order_status"))))
    .withColumn("payment_method", upper(trim(col("payment_method"))))
    .withColumn("quantity", col("quantity").cast("integer"))
    .filter(col("order_id").isNotNull() & (col("order_id") != ""))
    .dropDuplicates(["order_id"])
)
```

## 7. Write Silver Delta Tables

**Cell 6 — Write all cleaned datasets to the existing Silver container and register their Unity Catalog tables.**

```python
silver_path = (
    "abfss://silver@stdatabricksretailuat.dfs.core.windows.net"
)

silver_dataframes = {
    "customers": customers_silver,
    "products": products_silver,
    "stores": stores_silver,
    "orders": orders_silver
}

for entity, df in silver_dataframes.items():
    path = f"{silver_path}/{entity}/"

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(path)
    )

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS retailuat.silver.{entity}
        USING DELTA
        LOCATION '{path}'
    """)

    print(f"Silver table completed: retailuat.silver.{entity}")
```

## 8. Validate Silver Tables

**Cell 7 — Display record counts and sample records from all Silver tables.**

```python
for entity in silver_dataframes:
    table_name = f"retailuat.silver.{entity}"
    df = spark.table(table_name)

    print(f"{table_name}: {df.count()} records")
    display(df.limit(10))
```

## 9. Important Notes

- Storage account: `stdatabricksretailuat`.
- Source layer: `retailuat.bronze`.
- Target layer: `retailuat.silver`.
- Ensure the Silver external locations, Unity Catalog privileges, and ADLS permissions are configured.
- This implementation performs a full overwrite, not incremental processing.
- `CREATE TABLE IF NOT EXISTS` does not update an existing table's location. Verify existing table locations before running.
- Confirm source column names match the notebook before execution.

**Next Phase:** Build the Gold layer for business-ready retail analytics.
