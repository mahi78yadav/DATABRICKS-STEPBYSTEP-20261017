# Auto Loader Ingestion

### Cell 1 — Configure storage paths and entity names

```python
storage_account = "stdatabricksretailuat"

source_base = (
    f"abfss://source@{storage_account}.dfs.core.windows.net/"
)

bronze_base = (
    f"abfss://bronze@{storage_account}.dfs.core.windows.net/"
)

catalog_name = "retailuat"

entities = ["customers", "products", "stores", "orders"]
```

### Cell 2 — Define Auto Loader ingestion for all entities

```python
def ingest_with_autoloader(entity):
    source_path = f"{source_base}{entity}/"
    target_path = f"{bronze_base}{entity}_autoloader/"

    schema_location = (
        f"{bronze_base}autoloader_metadata/{entity}/schema/"
    )

    checkpoint_path = (
        f"{bronze_base}autoloader_metadata/{entity}/checkpoint/"
    )

    print(f"Starting Auto Loader: {entity}")

    stream_df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", schema_location)
        .option("header", "true")
        .load(source_path)
    )

    query = (
        stream_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .trigger(availableNow=True)
        .start(target_path)
    )

    query.awaitTermination()

    print(f"Auto Loader completed: {entity}")
```

### Cell 3 — Execute ingestion for all entities

```python
for entity in entities:
    ingest_with_autoloader(entity)
```

### Cell 4 — Register Auto Loader tables in Unity Catalog

```python
for entity in entities:
    target_path = f"{bronze_base}{entity}_autoloader/"

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS
        {catalog_name}.bronze.{entity}_autoloader
        USING DELTA
        LOCATION '{target_path}'
    """)

    print(f"Registered: {catalog_name}.bronze.{entity}_autoloader")
```

### Cell 5 — Validate Auto Loader tables

```python
for entity in entities:
    table_name = f"{catalog_name}.bronze.{entity}_autoloader"
    df = spark.table(table_name)

    print(f"{table_name}: {df.count()} records")
    display(df.limit(10))
```

### Important Notes

- **Source:** `source/customers/`, `source/products/`, `source/stores/`, `source/orders/`.
- **Bronze outputs:** `bronze/customers_autoloader/`, `bronze/products_autoloader/`, `bronze/stores_autoloader/`, `bronze/orders_autoloader/`.
- Each entity has its own schema and checkpoint paths under `bronze/autoloader_metadata/`.
- Ensure the Unity Catalog external location and permissions cover the output and metadata paths.
- `availableNow=True` processes currently available new files and then stops.
- Retain checkpoints between runs so previously processed files are not processed again.
- This creates separate Auto Loader tables. Your current Silver notebook reads the original Bronze tables, so update it later if you want Silver to consume these Auto Loader outputs.
- Run the notebook using compatible Databricks compute with Structured Streaming and Auto Loader support.

