# Auto Loader Implementation

## 1. Objective

Incrementally ingest CSV files for customers, products, stores, and orders from ADLS Gen2 into separate Bronze Delta tables using Databricks Auto Loader.

## 2. Prerequisites

- Storage account: `stdatabricksretailuat`
- Catalog: `retailuat`
- Schemas: `source`, `bronze`
- Containers: `source`, `bronze`
- Storage credential: `cred-adls-retail-uat`
- External locations: source and Bronze locations, plus `ext_bronze_autoloader_metadata`
- Databricks compute supporting Unity Catalog, Auto Loader, and Unity Catalog Volumes.

## 3. Create Metadata Volume

**Run in Databricks SQL Editor.**

```sql
CREATE VOLUME IF NOT EXISTS retailuat.bronze.autoloader_metadata;
```

## 4. Configure Auto Loader

### Cell 1 — Define source, Bronze, metadata paths, and entities

```python
storage_account = "stdatabricksretailuat"

source_base = (
    f"abfss://source@{storage_account}.dfs.core.windows.net/"
)

bronze_base = (
    f"abfss://bronze@{storage_account}.dfs.core.windows.net/"
)

metadata_base = "/Volumes/retailuat/bronze/autoloader_metadata"

catalog_name = "retailuat"
entities = ["customers", "products", "stores", "orders"]
```

### Cell 2 — Define the ingestion function

```python
def ingest_with_autoloader(entity):
    source_path = f"{source_base}{entity}/"
    target_path = f"{bronze_base}{entity}_autoloader/"

    schema_location = f"{metadata_base}/{entity}/schema"
    checkpoint_path = f"{metadata_base}/{entity}/checkpoint"

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

    print(f"Completed: {entity}")
```

### Cell 3 — Execute Auto Loader for all entities

```python
for entity in entities:
    ingest_with_autoloader(entity)
```

### Cell 4 — Register Bronze Auto Loader tables

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

### Cell 5 — Validate tables and record counts

```python
for entity in entities:
    table_name = f"{catalog_name}.bronze.{entity}_autoloader"
    df = spark.table(table_name)

    print(f"{table_name}: {df.count()} records")
    display(df.limit(10))
```

## 5. Expected Output

| Source folder | Bronze table |
|---|---|
| `source/customers/` | `retailuat.bronze.customers_autoloader` |
| `source/products/` | `retailuat.bronze.products_autoloader` |
| `source/stores/` | `retailuat.bronze.stores_autoloader` |
| `source/orders/` | `retailuat.bronze.orders_autoloader` |

## 6. Important Notes

- Keep the schema and checkpoint paths between executions.
- `availableNow=True` processes available new files and stops.
- New files are processed on the next run; schedule a Databricks Job for recurring execution.
- This creates separate Auto Loader tables and does not overwrite your existing Bronze tables.
- Grant the required volume permissions and ensure source and Bronze storage access works through your configured Unity Catalog setup.
- Do not configure `fs.azure.account.key` when using managed identity.
- If direct `abfss://` source or target access still raises an authentication error, fix the compute's storage access configuration before rerunning.

## 7. Completion Criteria

- [ ] Metadata volume created.
- [ ] All four entities ingested successfully.
- [ ] Four Auto Loader tables registered.
- [ ] Record counts and sample data validated.
- [ ] Checkpoints retained for subsequent incremental runs.

