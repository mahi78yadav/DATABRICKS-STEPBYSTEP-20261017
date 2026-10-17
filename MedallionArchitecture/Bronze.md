# Bronze Layer Ingestion

### Cell 1 — Define source and Bronze paths

```python
storage_account = "stdatabricksretailuat"

source_path = f"abfss://source@{storage_account}.dfs.core.windows.net/"
bronze_path = f"abfss://bronze@{storage_account}.dfs.core.windows.net/"

catalog_name = "retailuat"
```

### Cell 2 — Define entities

```python
entities = ["customers", "products", "stores", "orders"]
```

### Cell 3 — Read source CSV files and write them as Bronze Delta tables

```python
for entity in entities:
    source_df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(f"{source_path}{entity}/")
    )

    target_path = f"{bronze_path}{entity}/"

    (
        source_df.write
        .format("delta")
        .mode("overwrite")
        .save(target_path)
    )

    print(f"{entity}: Bronze ingestion completed")
```

### Cell 4 — Register Bronze Delta tables in Unity Catalog

```python
for entity in entities:
    target_path = f"{bronze_path}{entity}/"

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS
        {catalog_name}.bronze.{entity}
        USING DELTA
        LOCATION '{target_path}'
    """)

    print(f"{catalog_name}.bronze.{entity}: registered")
```

### Cell 5 — Validate Bronze tables and record counts

```python
for entity in entities:
    table_name = f"{catalog_name}.bronze.{entity}"

    print(f"{table_name}: {spark.table(table_name).count()} records")
    display(spark.table(table_name).limit(10))
```

