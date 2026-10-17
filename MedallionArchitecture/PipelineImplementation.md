# Azure Databricks Retail Lakehouse — Pipeline Implementation

## 1. Objective

Create a Databricks Job to automate the Retail Lakehouse data pipeline using the existing notebooks.

The pipeline processes four entities: `customers`, `products`, `stores`, and `orders`.

## 2. Pipeline Architecture

```text
ADLS Gen2 — Source Container
            |
            v
Bronze Layer Ingestion
(Initial Load — One Time)
            |
            v
Auto Loader Ingestion
(Incremental File Processing)
            |
            v
Silver Layer Implementation
            |
            v
Data_Quality
            |
            v
Gold Layer Implementation
            |
            v
Power BI Reporting
```

**Important:** The initial Bronze load is a one-time initialization step. The recurring job starts with Auto Loader after the initial Bronze data and tables are ready.

## 3. Existing Notebooks

| Notebook | Purpose |
|---|---|
| `Bronze Layer Ingestion` | Loads initial CSV files into Bronze Delta storage and registers Bronze tables |
| `Auto Loader Ingestion` | Incrementally processes new source files |
| `Silver Layer Implementation` | Cleans and transforms Bronze data into Silver |
| `Data_Quality` | Validates Silver data and captures rejected records |
| `Gold Layer Implementation` | Creates or refreshes Gold business tables |

Use the existing notebooks. Do not create duplicate notebooks for these steps.

## 4. Prerequisites

Before creating the recurring job, verify:

- [ ] Source files exist in the ADLS Gen2 `source` container.
- [ ] The initial Bronze ingestion has completed successfully.
- [ ] Bronze Delta tables exist for all four entities.
- [ ] Auto Loader works with persistent checkpoint and schema-tracking locations.
- [ ] Silver, Data Quality, and Gold notebooks run successfully in the required order.
- [ ] Unity Catalog tables and storage permissions are configured correctly.

## 5. Step 1 — Complete Initial Bronze Ingestion

Run `Bronze Layer Ingestion` manually, if the initial load has not already been completed.

Verify that these tables exist:

```text
retailuat.bronze.customers
retailuat.bronze.products
retailuat.bronze.stores
retailuat.bronze.orders
```

**Important:** The current Bronze notebook uses `.mode("overwrite")`. Do not include it in the recurring job because each execution could overwrite existing Bronze data.

## 6. Step 2 — Create the Databricks Job

1. Open your Azure Databricks workspace.
2. Navigate to **Jobs & Pipelines**.
3. Click **Create** and select **Job**.
4. Enter the job name:

   `JOB_Retail_Lakehouse_Pipeline`

5. Create the first task using the existing notebook `Auto Loader Ingestion`.

Configure the task:

| Setting | Value |
|---|---|
| Task name | `task_autoloader_ingestion` |
| Task type | Notebook |
| Notebook | `Auto Loader Ingestion` |
| Compute | Suitable job compute available in your workspace |

Save the task.

## 7. Step 3 — Add Silver Transformation

1. Click **Add task**.
2. Select **Notebook**.
3. Enter the task name `task_silver_transformation`.
4. Select `Silver Layer Implementation`.
5. Under **Depends on**, select `task_autoloader_ingestion`.
6. Set the dependency condition to **All succeeded**.
7. Save the task.

## 8. Step 4 — Add Data Quality Validation

1. Click **Add task**.
2. Select **Notebook**.
3. Enter the task name `task_data_quality`.
4. Select `Data_Quality`.
5. Under **Depends on**, select `task_silver_transformation`.
6. Set the dependency condition to **All succeeded**.
7. Save the task.

Ensure the Data Quality notebook returns a failed status when a critical quality gate is breached. If it only records rejected records and finishes successfully, add an explicit quality-gate check before Gold.

## 9. Step 5 — Add Gold Transformation

1. Click **Add task**.
2. Select **Notebook**.
3. Enter the task name `task_gold_transformation`.
4. Select `Gold Layer Implementation`.
5. Under **Depends on**, select `task_data_quality`.
6. Set the dependency condition to **All succeeded**.
7. Save the task.

## 10. Step 6 — Verify Task Dependencies

The final recurring job should have the following dependency structure:

```text
task_autoloader_ingestion
            |
            v
task_silver_transformation
            |
            v
task_data_quality
            |
            v
task_gold_transformation
```

The initial `Bronze Layer Ingestion` notebook is intentionally excluded from this recurring job.

## 11. Step 7 — Validate Auto Loader

Before running the complete job, verify that the Auto Loader notebook:

- Uses the correct source paths for all four entities.
- Writes data to the intended Bronze Delta targets.
- Uses persistent checkpoint and schema-tracking locations.
- Handles already-processed files without unintended duplicate ingestion.
- Finishes successfully when used in a scheduled workflow.

For a scheduled workflow, the Auto Loader notebook must process available files and terminate. A continuously running stream can prevent downstream dependent tasks from starting.

**Important:** Do not delete checkpoints to resolve ordinary rerun issues. Checkpoint state is required to track processed files.

## 12. Step 8 — Run the Job Manually

1. Open `JOB_Retail_Lakehouse_Pipeline`.
2. Click **Run now**.
3. Open the latest run.
4. Verify each task's status and logs.
5. Investigate and correct any failed task before proceeding.

Do not configure a recurring schedule until the complete job has passed the manual test.

## 13. Step 9 — Validate the Output Tables

Run the following SQL in a Databricks SQL editor or notebook.

### Bronze tables

```sql
SELECT COUNT(*) FROM retailuat.bronze.customers;
SELECT COUNT(*) FROM retailuat.bronze.products;
SELECT COUNT(*) FROM retailuat.bronze.stores;
SELECT COUNT(*) FROM retailuat.bronze.orders;
```

### Silver tables

```sql
SELECT COUNT(*) FROM retailuat.silver.customers;
SELECT COUNT(*) FROM retailuat.silver.products;
SELECT COUNT(*) FROM retailuat.silver.stores;
SELECT COUNT(*) FROM retailuat.silver.orders;
```

### Gold tables

```sql
SHOW TABLES IN retailuat.gold;
```

Query the actual Gold tables and confirm that the business results are correct.

If your Data Quality notebook creates these tables, also validate:

```sql
SELECT COUNT(*) FROM retailuat.silver.validated_orders;
SELECT COUNT(*) FROM retailuat.silver.rejected_orders;
```

Confirm that row counts, rejected records, and reconciliation results match your expectations.

## 14. Step 10 — Test Incremental Processing

1. Record the current table row counts.
2. Add a small number of new source files or records.
3. Run the job again.
4. Verify that Auto Loader processes the new data.
5. Confirm that existing data is not unintentionally duplicated.
6. Validate the Silver, Data Quality, and Gold results.
7. Repeat the test to verify that rerunning the job without new files does not introduce duplicates.

## 15. Step 11 — Configure the Schedule

After successful manual and incremental tests:

1. Open the job's schedule or trigger settings.
2. Configure the required execution frequency and time zone.
3. Save and enable the schedule.
4. Verify that a scheduled run completes successfully.

Set the schedule according to the expected source-file arrival pattern. Avoid unnecessarily frequent runs to control compute costs.

## 16. Final Completion Checklist

- [ ] Initial Bronze load completed successfully.
- [ ] Auto Loader processes incremental files successfully.
- [ ] Checkpoints and schema tracking persist between runs.
- [ ] Silver transformation completes successfully.
- [ ] Data Quality validation produces the expected results.
- [ ] Gold business tables are refreshed successfully.
- [ ] All job dependencies are correct.
- [ ] Incremental reruns do not introduce unintended duplicates.
- [ ] Manual execution succeeds end to end.
- [ ] Scheduled execution succeeds, if scheduling is required.
- [ ] Final pipeline documentation is committed to GitHub.

## 17. Official Documentation

- [Create your first Databricks Job](https://learn.microsoft.com/en-us/azure/databricks/jobs/jobs-quickstart)
- [Configure Databricks Jobs](https://learn.microsoft.com/en-us/azure/databricks/jobs/configure-job)
- [Auto Loader overview](https://learn.microsoft.com/en-us/azure/databricks/ingestion/cloud-object-storage/auto-loader/)
- [Control task execution flow](https://learn.microsoft.com/en-us/azure/databricks/jobs/control-flow)
