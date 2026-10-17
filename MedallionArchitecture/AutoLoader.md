# Auto Loader Ingestion — Azure Databricks

## 1. Objective

Implement Databricks Auto Loader to incrementally ingest new CSV files from Azure Data Lake Storage Gen2 (ADLS Gen2) into Bronze Delta tables.

Auto Loader processes new files, maintains schema and checkpoint metadata, and prevents already-processed files from being reprocessed during normal subsequent runs.

## 2. Configuration

| Setting | Value |
|---|---|
| Storage Account | `stdatabricksretailuat` |
| Catalog | `retailuat` |
| Source Container | `source` |
| Bronze Container | `bronze` |
| Entities | `customers`, `products`, `stores`, `orders` |
| Input Format | CSV |
| Output Format | Delta |
| Processing Mode | `availableNow=True` |

## 3. Prerequisites

- ADLS Gen2 storage account and source files are available.
- Unity Catalog storage credential `cred-adls-retail-uat` is configured.
- External locations and permissions allow access to the required source and Bronze paths.
- The Databricks compute environment supports the required storage access.
- The Auto Loader output folders are separate from existing Bronze Delta folders.

## 4. Configure paths and entities

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

**Important:** The metadata volume must exist and be accessible. Create the required volume and grant permissions before running the notebook. Do not replace this path with an Azure storage key.

## 5. Define the Auto Loader ingestion function

```python
def ingest_with_autoloader(entity):

    source_path = f"{source_base}{entity}/"

    # Separate output from existing Bronze Delta data
    target_path = f"{bronze_base}{entity}_autoloader/"

    schema_location = (
        f"{metadata_base}/{entity}/schema"
    )

    checkpoint_path = (
        f"{metadata_base}/{entity}/checkpoint"
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

## 6. Execute ingestion for all entities

Run this cell only after verifying the source, metadata, and output paths and confirming the required permissions.

```python
for entity in entities:
    ingest_with_autoloader(entity)
```

The notebook processes the four entities sequentially. `availableNow=True` processes available new files and then stops.

## 7. Register the output as Unity Catalog tables

After successful ingestion, register each Delta output folder as a table.

```sql
CREATE TABLE IF NOT EXISTS retailuat.bronze.customers_autoloader
USING DELTA
LOCATION 'abfss://bronze@stdatabricksretailuat.dfs.core.windows.net/customers_autoloader/';

CREATE TABLE IF NOT EXISTS retailuat.bronze.products_autoloader
USING DELTA
LOCATION 'abfss://bronze@stdatabricksretailuat.dfs.core.windows.net/products_autoloader/';

CREATE TABLE IF NOT EXISTS retailuat.bronze.stores_autoloader
USING DELTA
LOCATION 'abfss://bronze@stdatabricksretailuat.dfs.core.windows.net/stores_autoloader/';

CREATE TABLE IF NOT EXISTS retailuat.bronze.orders_autoloader
USING DELTA
LOCATION 'abfss://bronze@stdatabricksretailuat.dfs.core.windows.net/orders_autoloader/';
```

Ensure Unity Catalog external locations and privileges cover these four output folders before running the SQL. Alternatively, configure the output locations under an already-authorized external location.

## 8. Validate ingestion

Run in a Python cell:

```python
for entity in entities:
    table_name = f"retailuat.bronze.{entity}_autoloader"

    print(f"{table_name}: {spark.table(table_name).count()} rows")
```

To preview records:

```python
display(
    spark.table("retailuat.bronze.orders_autoloader").limit(10)
)
```

Repeat for other entities as required.

## 9. Incremental processing

1. Run the notebook for the initial batch of files.
2. Add a new CSV file to the appropriate source entity folder.
3. Run the notebook again.
4. Auto Loader uses the existing checkpoint to identify newly discovered files.
5. Validate the corresponding output table.

Keep the schema and checkpoint paths unchanged between runs. Do not delete them during normal operation.

## 10. Important considerations

- Auto Loader processes files incrementally; it does not automatically transform data into Silver or Gold.
- Silver and Gold transformations are implemented as separate pipeline steps.
- `availableNow=True` is suitable for batch-style incremental runs. Schedule the notebook as a Databricks Job for recurring ingestion.
- The initial load and subsequent incremental runs should be validated using row counts and sample records.
- Do not claim successful execution until the notebook cells finish without errors and the output tables have been validated.

## 11. Expected architecture

```text
ADLS Gen2 — source
        |
        | CSV files
        v
Databricks Auto Loader
        |
        | Schema and checkpoint metadata
        v
ADLS Gen2 — Bronze Auto Loader output
        |
        v
Unity Catalog Bronze tables
        |
        v
Silver transformations
        |
        v
Gold business tables
```
