# Auto Loader Ingestion

### Cell 1 — Configure source, Bronze, schema, and checkpoint paths

```python
storage_account = "stdatabricksretailuat"

source_path = (
    f"abfss://source@{storage_account}.dfs.core.windows.net/orders/"
)

bronze_path = (
    f"abfss://bronze@{storage_account}.dfs.core.windows.net/orders_autoloader/"
)

schema_location = (
    f"abfss://bronze@{storage_account}.dfs.core.windows.net/"
    "autoloader_metadata/orders/schema/"
)

checkpoint_path = (
    f"abfss://bronze@{storage_account}.dfs.core.windows.net/"
    "autoloader_metadata/orders/checkpoint/"
)
```

### Cell 2 — Read new CSV files using Auto Loader

```python
orders_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaLocation", schema_location)
    .option("header", "true")
    .load(source_path)
)
```

### Cell 3 — Incrementally write new records to Bronze Delta

```python
query = (
    orders_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)
    .start(bronze_path)
)

query.awaitTermination()

print("Auto Loader ingestion completed")
```

### Cell 4 — Register the Auto Loader output as a Unity Catalog table

```python
spark.sql("""
    CREATE TABLE IF NOT EXISTS retailuat.bronze.orders_autoloader
    USING DELTA
    LOCATION
    'abfss://bronze@stdatabricksretailuat.dfs.core.windows.net/orders_autoloader/'
""")
```

### Cell 5 — Validate ingested data

```python
df = spark.table("retailuat.bronze.orders_autoloader")

print("Auto Loader record count:", df.count())
display(df.limit(10))
```

### Important Notes

- Source: `source/orders/`.
- Auto Loader output: `bronze/orders_autoloader/`.
- Schema and checkpoint metadata are stored separately under `bronze/autoloader_metadata/orders/`.
- Configure Unity Catalog external locations and permissions to cover these paths before running.
- `availableNow=True` processes available new files and stops. Schedule the notebook as a Job for recurring ingestion.
- Keep the checkpoint between runs; do not delete it during normal operation.
- This example handles the `orders` entity. Use separate output and checkpoint paths for other entities.
