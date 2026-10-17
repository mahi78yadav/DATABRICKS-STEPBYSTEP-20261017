# Gold Layer Implementation

## 1. Objective

Create a business-ready retail dataset by joining Silver tables, calculating sales metrics, and storing the results in the Gold layer.

### Cell 1 — Load Silver tables from Unity Catalog

```python
catalog = "retailuat"

customers_silver = spark.table(f"{catalog}.silver.customers")
products_silver = spark.table(f"{catalog}.silver.products")
orders_silver = spark.table(f"{catalog}.silver.orders")
stores_silver = spark.table(f"{catalog}.silver.stores")

print("Silver tables loaded successfully")
```

### Cell 2 — Inspect Silver table schemas

```python
customers_silver.printSchema()
products_silver.printSchema()
orders_silver.printSchema()
stores_silver.printSchema()
```

### Cell 3 — Join Silver tables to create the Gold sales dataset

```python
from pyspark.sql.functions import col

gold_sales = (
    orders_silver.alias("o")
    .join(customers_silver.alias("c"), col("o.customer_id") == col("c.customer_id"), "left")
    .join(products_silver.alias("p"), col("o.product_id") == col("p.product_id"), "left")
    .join(stores_silver.alias("s"), col("o.store_id") == col("s.store_id"), "left")
    .select(
        col("o.order_id"),
        col("o.order_date"),
        col("o.customer_id"),
        col("c.customer_name"),
        col("c.customer_segment"),
        col("o.product_id"),
        col("p.product_name"),
        col("p.category"),
        col("p.subcategory"),
        col("p.unit_price"),
        col("o.quantity"),
        col("o.order_status"),
        col("o.payment_method"),
        col("o.store_id"),
        col("s.store_name"),
        col("s.city").alias("store_city"),
        col("s.state").alias("store_state"),
        col("s.region"),
        (col("o.quantity") * col("p.unit_price")).alias("sales_amount")
    )
)

display(gold_sales)
```

### Cell 4 — Validate Gold data and record counts

```python
print("Gold row count:", gold_sales.count())
gold_sales.printSchema()

print("Null customer names:", gold_sales.filter(col("customer_name").isNull()).count())
print("Null product names:", gold_sales.filter(col("product_name").isNull()).count())
print("Null store names:", gold_sales.filter(col("store_name").isNull()).count())
```

### Cell 5 — Write Gold data to the Gold container

```python
gold_path = (
    "abfss://gold@stdatabricksretailuat.dfs.core.windows.net/sales/"
)

(
    gold_sales.write
    .format("delta")
    .mode("overwrite")
    .save(gold_path)
)

print("Gold data written successfully")
```

### Cell 6 — Register the Gold table in Unity Catalog

```python
spark.sql("""
    CREATE TABLE IF NOT EXISTS retailuat.gold.sales
    USING DELTA
    LOCATION 'abfss://gold@stdatabricksretailuat.dfs.core.windows.net/sales/'
""")
```

### Cell 7 — Validate the Gold Delta table

```python
gold_sales_check = spark.table("retailuat.gold.sales")

print("Gold sales row count:", gold_sales_check.count())
display(gold_sales_check.limit(10))
```

### Cell 8 — Calculate business metrics

```python
from pyspark.sql.functions import sum, count, avg, round

gold_metrics = gold_sales_check.agg(
    count("order_id").alias("total_orders"),
    sum("quantity").alias("total_quantity"),
    round(sum(col("sales_amount")), 2).alias("total_sales"),
    round(avg(col("unit_price")), 2).alias("average_unit_price")
)

display(gold_metrics)
```

## 2. Important Notes

- Source: `retailuat.silver`.
- Target: `retailuat.gold.sales`.
- Storage account: `stdatabricksretailuat`.
- Gold storage path: `gold/sales/`.
- Ensure the Gold external location and required Unity Catalog permissions are configured.
- This implementation overwrites the existing Gold data on each run.
- If `retailuat.gold.sales` already exists, verify its location before running the registration cell.
- Confirm the Silver column names match the notebook before execution.

## 3. Completion Criteria

- [ ] Silver tables loaded successfully.
- [ ] Gold sales dataset created by joining all four entities.
- [ ] Null checks and row counts validated.
- [ ] Gold Delta data written to ADLS.
- [ ] `retailuat.gold.sales` registered and queried successfully.
- [ ] Business metrics calculated.

