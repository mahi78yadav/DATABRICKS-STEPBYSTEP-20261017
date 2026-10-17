# Azure Databricks Auto Loader Ingestion — UAT

## 1. Objective

Implement Auto Loader to incrementally ingest CSV files from the ADLS Gen2 `source` container into Delta tables in the `bronze` container.

**Entities:** `customers`, `products`, `stores`, `orders`

## 2. Prerequisites

Confirm the following resources are available:

| Resource | Name |
|---|---|
| Databricks workspace | `dbw-retail-lakehouse-uat` |
| Storage account | `stdatabricksretailuat` |
| Catalog | `retailuat` |
| Bronze schema | `retailuat.bronze` |
| Storage credential | `cred-adls-retail-uat` |

Unity Catalog external locations must provide access to the required source, Bronze, and metadata paths. Do not configure storage account keys in Spark.

## 3. Source and Target Paths

| Entity | Source folder | Bronze target |
|---|---|---|
| Customers | `source/customers/` | `bronze/customers/` |
| Products | `source/products/` | `bronze/products/` |
| Stores | `source/stores/` | `bronze/stores/` |
| Orders | `source/orders/` | `bronze/orders/` |

Source files are stored in the corresponding folders in ADLS Gen2.

## 4. Configure Paths and Entities

Create a Python cell in the **Auto Loader Ingestion** notebook.

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

Run the cell.

## 5. Validate Source Access

Run this test before starting ingestion.

```python
display(
    dbutils.fs.ls(source_base + "customers/")
)
```

Confirm that `customers.csv` is listed.

## 6. Define the Auto Loader Function

Create the next Python cell and run it.

```python
def ingest_with_autoloader(entity):

    source_path = f"{source_base}{entity}/"
    target_path = f"{bronze_base}{entity}/"

    schema_location = f"{metadata_base}/{entity}/schema"
    checkpoint_path = f"{metadata_base}/{entity}/checkpoint"

    print(f"Starting Auto Loader: {entity}")
    print(f"Source: {source_path}")
    print(f"Target: {target_path}")

    stream_df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("cloudFiles.schemaLocation", schema_location)
        .option("header", "true")
        .load(source_path)
    )

    # Match the existing Bronze Delta table schema.
    # Rescued values are not retained by this implementation.
    stream_df = stream_df.drop("_rescued_data")

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

### Important configuration notes

- `schema_location` stores Auto Loader schema information.
- `checkpoint_path` stores streaming progress.
- `availableNow=True` processes available input and then stops.
- The output targets the existing Bronze Delta folders.
- The `_rescued_data` column is removed to match the existing Bronze schema. Values contained only in that column are discarded.
- Keep existing checkpoint folders when rerunning ingestion. Do not delete them to troubleshoot without first reviewing the ingestion state.

## 7. Test One Entity First

Run the following cell:

```python
ingest_with_autoloader("customers")
```

Expected successful output:

```text
Starting Auto Loader: customers
Source: abfss://source@stdatabricksretailuat.dfs.core.windows.net/customers/
Target: abfss://bronze@stdatabricksretailuat.dfs.core.windows.net/customers/
Auto Loader completed: customers
```

A successful run confirms that the `customers` stream completed. It does not by itself prove that new records were inserted.

## 8. Run All Entities

After confirming each target schema is compatible and the checkpoint state is correct, run the following cell.

```python
for entity in entities:
    ingest_with_autoloader(entity)
```

Review the output for each entity and confirm that all four runs complete without errors.

**Duplicate-data precaution:** These writes use append mode. If an entity's existing Bronze data was loaded by another process, Auto Loader may append duplicate records. Use a consistent ingestion process and checkpoint for each entity; do not blindly rerun with new checkpoints.

## 9. Validate Bronze Data

Run the following cell to check the existing Delta data.

```python
for entity in entities:
    print(f"Bronze entity: {entity}")

    display(
        spark.read.format("delta")
        .load(f"{bronze_base}{entity}/")
        .limit(10)
    )
```

Review the displayed rows and confirm the expected columns and records are present.

## 10. Final Checklist

- [ ] Source CSV files are present.
- [ ] Unity Catalog access is configured.
- [ ] Metadata volume is available.
- [ ] Auto Loader function runs successfully.
- [ ] `customers` test succeeds.
- [ ] `products`, `stores`, and `orders` are processed.
- [ ] Bronze Delta data is validated.
- [ ] Checkpoints and schema information are retained.
- [ ] No unintended duplicate records are introduced.

**Result:** Auto Loader ingestion is implemented using ADLS Gen2, Unity Catalog access, schema tracking, streaming checkpoints, and Delta Lake Bronze targets.
