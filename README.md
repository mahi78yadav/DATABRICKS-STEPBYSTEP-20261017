# DATABRICKS-STEPBYSTEP-20261017

# Azure Databricks Retail Lakehouse Project

## 1. Project Overview

Build a retail sales analytics solution using Azure Databricks and ADLS Gen2, following the Medallion Architecture: Source → Bronze → Silver → Gold.

### Business Analysis Scope

- Product Performance
- Store Performance
- Customer Purchase Analysis
- Sales Data Analysis
- Overall Business Metrics

## 2. Azure Resource Details

| Resource | Name |
|---|---|
| Resource Group | `rg-databricks-retail-uat` |
| Storage Account | `stdatabricksretailuat` |
| Access Connector | `ac-databricks-retail-uat` |
| Azure Databricks Workspace | `dbw-retail-lakehouse-uat` |

## 3. Create ADLS Gen2 Storage

1. Create resource group `rg-databricks-retail-uat`.
2. Create storage account `stdatabricksretailuat`.
3. Enable **Hierarchical namespace**.
4. Create the following containers:
   - `source`
   - `bronze`
   - `silver`
   - `gold`

## 4. Create Folders and Upload Source Files

Create the following folders inside both the `source` and `bronze` containers:

```text
customers/
products/
stores/
orders/
```

Download the source files from GitHub:

[Retail Source Files](https://github.com/mahi78yadav/DATABRICKS-STEPBYSTEP-20261017/tree/main/data)

Upload the corresponding files into the appropriate folders in the `source` container.

## 5. Create Access Connector and Assign Roles

1. Create an Azure Databricks Access Connector named `ac-databricks-retail-uat`.
2. Enable its managed identity.
3. Assign the following roles to the Access Connector's managed identity:
   - Storage Blob Data Contributor
   - Storage Account Contributor
   - EventGrid EventSubscription Contributor
   - Storage Queue Data Contributor
4. Verify role assignments and their scopes.

**Note:** Storage Account Contributor, Event Grid, and Storage Queue permissions may be required for specific file-event or resource-management scenarios. Assign roles according to the implementation requirements.

## 6. Create Azure Databricks Workspace

1. Open the Azure portal.
2. Search for **Azure Databricks** and select **Create**.
3. Select the subscription and resource group `rg-databricks-retail-uat`.
4. Enter workspace name `dbw-retail-lakehouse-uat`.
5. Select a compatible Azure region and the required pricing tier.
6. Review the configuration and create the workspace.
7. After deployment completes, select **Launch Workspace**.

## 7. Expected Outcome

- Resource group and ADLS Gen2 storage account created.
- Four containers created.
- Customer, product, store, and orders folders created in Source and Bronze.
- Source files uploaded from GitHub.
- Access Connector created and required roles assigned.
- Azure Databricks workspace created and launched.

**Next Phase:** Configure Unity Catalog, create the storage credential and external locations, and begin Bronze ingestion.

---

# Azure Databricks — Unity Catalog and Bronze Ingestion

## 1. Prerequisites

- Azure Databricks workspace: `dbw-retail-lakehouse-uat`
- Storage account: `stdatabricksretailuat`
- Access Connector: `ac-databricks-retail-uat`
- ADLS Gen2 containers: `source`, `bronze`, `silver`, `gold`
- Source folders: `customers`, `products`, `stores`, `orders`
- Unity Catalog enabled with required privileges
- Databricks SQL Warehouse available

## 2. Create Storage Credential

1. Open the Azure portal.
2. Open `ac-databricks-retail-uat`.
3. Copy its full **Resource ID** from Properties.
4. Open Databricks SQL Editor and select a SQL Warehouse.
5. Replace `<ACCESS_CONNECTOR_RESOURCE_ID>` in the script below.

```sql
CREATE STORAGE CREDENTIAL `cred-adls-retail-uat`
WITH IDENTITY = 'ManagedIdentity'
ON '<ACCESS_CONNECTOR_RESOURCE_ID>'
COMMENT 'Managed identity credential for retail ADLS Gen2';
```

## Step 2.1: Create SQL Warehouse

Create a SQL Warehouse in the Azure Databricks workspace to execute SQL scripts for the Retail Lakehouse project.

**Workspace:** `dbw-retail-lakehouse-uat`

### Configuration

| Setting | Value |
|---|---|
| SQL Warehouse Name | `retailuat` |
| Warehouse Type | Pro (or available suitable type) |
| Cluster Size | 2X-Small |
| Auto Stop | 10 minutes |
| Minimum Clusters | 1 |
| Maximum Clusters | 1 |

### Steps

1. Open the Azure Databricks workspace `dbw-retail-lakehouse-uat`.
2. Select **SQL Warehouses** from the left navigation menu.
3. Click **Create SQL warehouse**.
4. Enter the configuration values listed above.
5. Click **Create** and wait until the warehouse status is **Running**.
6. Open **SQL Editor** and select the `retailuat` SQL Warehouse.

### Purpose

   The SQL Warehouse will be used to execute SQL scripts for:
   
   - Creating the Unity Catalog catalog and schemas.
   - Creating storage credentials and external locations.
   - Registering source tables.
   - Creating Bronze Delta tables.
   - Validating table data and row counts.
   
   **Note:** A SQL Warehouse is different from a traditional Databricks compute cluster. Start with a small warehouse for this learning project and stop it when you finish working to control costs.



## 3. Create Catalog and Schemas

```sql
CREATE CATALOG IF NOT EXISTS retailuat;

CREATE SCHEMA IF NOT EXISTS retailuat.source;
CREATE SCHEMA IF NOT EXISTS retailuat.bronze;
CREATE SCHEMA IF NOT EXISTS retailuat.silver;
CREATE SCHEMA IF NOT EXISTS retailuat.gold;
```

## 4. Create External Locations

### 4.1 Source Locations

```sql
CREATE EXTERNAL LOCATION IF NOT EXISTS ext_source_customers
URL 'abfss://source@stdatabricksretailuat.dfs.core.windows.net/customers/'
WITH (STORAGE CREDENTIAL `cred-adls-retail-uat`);

CREATE EXTERNAL LOCATION IF NOT EXISTS ext_source_products
URL 'abfss://source@stdatabricksretailuat.dfs.core.windows.net/products/'
WITH (STORAGE CREDENTIAL `cred-adls-retail-uat`);

CREATE EXTERNAL LOCATION IF NOT EXISTS ext_source_stores
URL 'abfss://source@stdatabricksretailuat.dfs.core.windows.net/stores/'
WITH (STORAGE CREDENTIAL `cred-adls-retail-uat`);

CREATE EXTERNAL LOCATION IF NOT EXISTS ext_source_orders
URL 'abfss://source@stdatabricksretailuat.dfs.core.windows.net/orders/'
WITH (STORAGE CREDENTIAL `cred-adls-retail-uat`);
```

### 4.2 Bronze Locations

```sql
CREATE EXTERNAL LOCATION IF NOT EXISTS ext_bronze_customers
URL 'abfss://bronze@stdatabricksretailuat.dfs.core.windows.net/customers/'
WITH (STORAGE CREDENTIAL `cred-adls-retail-uat`);

CREATE EXTERNAL LOCATION IF NOT EXISTS ext_bronze_products
URL 'abfss://bronze@stdatabricksretailuat.dfs.core.windows.net/products/'
WITH (STORAGE CREDENTIAL `cred-adls-retail-uat`);

CREATE EXTERNAL LOCATION IF NOT EXISTS ext_bronze_stores
URL 'abfss://bronze@stdatabricksretailuat.dfs.core.windows.net/stores/'
WITH (STORAGE CREDENTIAL `cred-adls-retail-uat`);

CREATE EXTERNAL LOCATION IF NOT EXISTS ext_bronze_orders
URL 'abfss://bronze@stdatabricksretailuat.dfs.core.windows.net/orders/'
WITH (STORAGE CREDENTIAL `cred-adls-retail-uat`);
```

## 5. Create Source Tables

**Assumption:** Source files are CSV files with headers. Adjust the format and options if the actual files differ.

```sql
CREATE TABLE IF NOT EXISTS retailuat.source.customers
USING CSV
OPTIONS ('header' = 'true', 'inferSchema' = 'true')
LOCATION 'abfss://source@stdatabricksretailuat.dfs.core.windows.net/customers/';

CREATE TABLE IF NOT EXISTS retailuat.source.products
USING CSV
OPTIONS ('header' = 'true', 'inferSchema' = 'true')
LOCATION 'abfss://source@stdatabricksretailuat.dfs.core.windows.net/products/';

CREATE TABLE IF NOT EXISTS retailuat.source.stores
USING CSV
OPTIONS ('header' = 'true', 'inferSchema' = 'true')
LOCATION 'abfss://source@stdatabricksretailuat.dfs.core.windows.net/stores/';

CREATE TABLE IF NOT EXISTS retailuat.source.orders
USING CSV
OPTIONS ('header' = 'true', 'inferSchema' = 'true')
LOCATION 'abfss://source@stdatabricksretailuat.dfs.core.windows.net/orders/';
```

## 6. Begin Bronze Ingestion

Copy source data into Delta tables in the Bronze layer.

```sql
CREATE TABLE IF NOT EXISTS retailuat.bronze.customers
USING DELTA
LOCATION 'abfss://bronze@stdatabricksretailuat.dfs.core.windows.net/customers/'
AS SELECT * FROM retailuat.source.customers;

CREATE TABLE IF NOT EXISTS retailuat.bronze.products
USING DELTA
LOCATION 'abfss://bronze@stdatabricksretailuat.dfs.core.windows.net/products/'
AS SELECT * FROM retailuat.source.products;

CREATE TABLE IF NOT EXISTS retailuat.bronze.stores
USING DELTA
LOCATION 'abfss://bronze@stdatabricksretailuat.dfs.core.windows.net/stores/'
AS SELECT * FROM retailuat.source.stores;

CREATE TABLE IF NOT EXISTS retailuat.bronze.orders
USING DELTA
LOCATION 'abfss://bronze@stdatabricksretailuat.dfs.core.windows.net/orders/'
AS SELECT * FROM retailuat.source.orders;
```

## 7. Validate Tables and Row Counts

```sql
SHOW TABLES IN retailuat.source;
SHOW TABLES IN retailuat.bronze;

SELECT COUNT(*) FROM retailuat.bronze.customers;
SELECT COUNT(*) FROM retailuat.bronze.products;
SELECT COUNT(*) FROM retailuat.bronze.stores;
SELECT COUNT(*) FROM retailuat.bronze.orders;
```

## 8. Important Notes

- Replace the Access Connector Resource ID placeholder before running the credential command.
- The executing identity needs the required Unity Catalog privileges and Azure storage permissions.
- External locations provide governed access to storage paths; they do not create tables automatically.
- Source tables register existing source files. Bronze tables store the data in Delta format.
- `CREATE TABLE IF NOT EXISTS` does not refresh existing tables when executed again.
- The above Bronze ingestion is an initial load, not an incremental ingestion process.
- Configure separate external locations for Silver and Gold when those layers are implemented.

## 9. Implementing Bronze Layer

Refer to the detailed notebook implementation and execution steps:

[Bronze Layer Implementation](MedallionArchitecture/Bronze.md)

## 10. Implementing Bronze Layer

Refer to the detailed notebook implementation and execution steps:

[Silver Layer Implementation](MedallionArchitecture/Silver.md)

## 11. Implementing Gold Layer

[Gold Layer Implementation](MedallionArchitecture/Gold.md)
