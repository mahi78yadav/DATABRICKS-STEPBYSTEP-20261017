# Azure Databricks Retail Lakehouse — Pipeline Implementation Guide

## 1. Project Configuration

| Item | Value |
|---|---|
| Resource Group | `rg-databricks-retail-uat` |
| Storage Account | `stdatabricksretailuat` |
| Databricks Workspace | `dbw-retail-lakehouse-uat` |
| Catalog | `retailuat` |
| Entities | `customers`, `products`, `stores`, `orders` |
| Source Schema | `retailuat.source` |
| Bronze Schema | `retailuat.bronze` |
| Silver Schema | `retailuat.silver` |
| Gold Schema | `retailuat.gold` |
| Pipeline/Job Name | `JOB_Retail_Lakehouse_Pipeline` |

## 2. Pipeline Architecture

```text
Azure Data Lake Storage Gen2
        |
        v
   Source Files
        |
        v
  Bronze Ingestion
        |
        v
  Silver Transformation
        |
        v
  Data Quality Validation
        |
        v
  Gold Business Tables
        |
        v
  Power BI Reporting
```

## 3. Prerequisites

Before creating the job, verify the following:

- [ ] Azure Databricks workspace is accessible.
- [ ] Unity Catalog catalog `retailuat` exists.
- [ ] Schemas `source`, `bronze`, `silver`, and `gold` exist.
- [ ] Storage credential and external locations are configured.
- [ ] Source files for all four entities are available.
- [ ] Bronze ingestion notebooks run successfully.
- [ ] Silver transformation notebooks run successfully.
- [ ] Data quality notebook runs successfully.
- [ ] Gold transformation notebooks run successfully.
- [ ] All notebook paths and table names have been verified.

**Important:** Use your actual notebook names and paths. Do not create job tasks pointing to notebooks that have not been tested.

## 4. Prepare the Notebooks

Organize the notebooks logically in your Databricks workspace or Git folder.

| Task | Suggested task name | Notebook purpose |
|---|---|---|
| 1 | `task_bronze_ingestion` | Load source data into Bronze |
| 2 | `task_silver_transformation` | Clean and transform Bronze data |
| 3 | `task_data_quality` | Validate Silver data and capture rejected records |
| 4 | `task_gold_transformation` | Create or refresh Gold business tables |

If your implementation uses separate notebooks for individual entities, run and validate those notebooks first. You can initially create one task per layer if each notebook processes all four entities.

## 5. Create the Databricks Job

1. Open Azure Databricks workspace `dbw-retail-lakehouse-uat`.
2. Select **Jobs & Pipelines** from the left navigation.
3. Click **Create** and select **Job**.
4. Enter the job name:

   `JOB_Retail_Lakehouse_Pipeline`

5. Configure the first notebook task:
   - **Task name:** `task_bronze_ingestion`
   - **Type:** Notebook
   - **Source:** Workspace or Git folder, depending on where the notebook is stored
   - **Path:** Select your verified Bronze ingestion notebook
   - **Compute:** Select an available, suitable job compute option
6. Click **Save task**.

## 6. Add the Remaining Tasks

### Task 2 — Silver Transformation

1. Click **Add task**.
2. Select **Notebook**.
3. Set the task name to `task_silver_transformation`.
4. Select the Silver transformation notebook.
5. Configure the compute option.
6. Under **Depends on**, select `task_bronze_ingestion`.
7. Set the dependency condition to **All succeeded**.
8. Save the task.

### Task 3 — Data Quality Validation

1. Click **Add task**.
2. Select **Notebook**.
3. Set the task name to `task_data_quality`.
4. Select the Data Quality notebook.
5. Set **Depends on** to `task_silver_transformation`.
6. Set the dependency condition to **All succeeded**.
7. Save the task.

### Task 4 — Gold Transformation

1. Click **Add task**.
2. Select **Notebook**.
3. Set the task name to `task_gold_transformation`.
4. Select the Gold transformation notebook.
5. Set **Depends on** to `task_data_quality`.
6. Set the dependency condition to **All succeeded**.
7. Save the task.

**Important:** Use the sequence above only if the Data Quality notebook is designed to fail the run when unacceptable data-quality conditions are detected. If it merely records rejected rows and returns success, configure an explicit quality-gate check before Gold so that Gold does not refresh when your defined quality threshold is breached.

## 7. Verify the Task Dependency Graph

The expected task sequence is:

```text
task_bronze_ingestion
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

Confirm that each downstream task depends on the correct upstream task and that the dependency condition is **All succeeded**.

## 8. Configure Job Settings

1. Open the job settings.
2. Configure the job to use an appropriate compute option supported by your workspace.
3. Prefer job compute or serverless job compute when available and suitable.
4. Configure task retries where appropriate.
5. Enable failure notifications if supported and available.
6. Set a maximum concurrent run count of `1` initially to avoid overlapping executions.
7. Save the job configuration.

Avoid hardcoding storage account keys, passwords, or other secrets in notebooks or job parameters.

## 9. Run the Pipeline Manually

1. Open `JOB_Retail_Lakehouse_Pipeline`.
2. Click **Run now**.
3. Open the **Runs** tab.
4. Select the latest run.
5. Inspect each task's status and output.
6. If a task fails, open its logs, correct the root cause, and rerun the affected task or job.

Do not schedule the pipeline until the complete job has passed a manual end-to-end run.

## 10. Validate Bronze, Silver, and Gold

Run the following SQL queries in a SQL editor or notebook with access to the catalog.

### Bronze validation

```sql
SELECT COUNT(*) AS row_count
FROM retailuat.bronze.customers;

SELECT COUNT(*) AS row_count
FROM retailuat.bronze.products;

SELECT COUNT(*) AS row_count
FROM retailuat.bronze.stores;

SELECT COUNT(*) AS row_count
FROM retailuat.bronze.orders;
```

### Silver validation

```sql
SELECT COUNT(*) AS row_count
FROM retailuat.silver.customers;

SELECT COUNT(*) AS row_count
FROM retailuat.silver.products;

SELECT COUNT(*) AS row_count
FROM retailuat.silver.stores;

SELECT COUNT(*) AS row_count
FROM retailuat.silver.orders;
```

### Data quality validation

```sql
SELECT COUNT(*) AS valid_order_count
FROM retailuat.silver.validated_orders;

SELECT COUNT(*) AS rejected_order_count
FROM retailuat.silver.rejected_orders;
```

Run the last two queries only if these tables are created by your Data Quality notebook.

### Gold validation

Replace the example table names below with your actual Gold tables.

```sql
SHOW TABLES IN retailuat.gold;
```

Then query the Gold tables and confirm that the output matches your expected business results.

**Validation checklist:**

- [ ] All expected source entities are processed.
- [ ] Bronze tables contain the expected data.
- [ ] Silver tables contain cleaned and transformed data.
- [ ] Data quality results and rejected records are available.
- [ ] Gold tables contain the expected business data.
- [ ] Row counts and reconciliation checks are acceptable.
- [ ] No unexpected duplicate records are introduced by reruns.

## 11. Test Incremental Processing and Reruns

1. Record the current row counts in Bronze, Silver, and Gold.
2. Add a small, known set of new source records.
3. Run the job again.
4. Confirm that new records are processed according to your ingestion design.
5. Verify that previously processed records are not duplicated.
6. Check the Silver and Gold results.
7. Confirm that the pipeline can be rerun safely after a failure.

If the Bronze notebook uses Auto Loader, ensure its checkpoint and schema-tracking locations persist between runs. Do not delete or recreate checkpoints simply to make a rerun succeed.

## 12. Configure the Schedule

After a successful manual run:

1. Open the job's schedule or trigger settings.
2. Enable a schedule appropriate for your source-data arrival pattern.
3. Choose the required time zone.
4. Save and enable the schedule.
5. Verify that the next scheduled run starts and finishes successfully.

For initial testing, a manual run is recommended. Use a frequent schedule only when required; unnecessary runs can increase compute costs.

## 13. Final Success Criteria

Mark the pipeline implementation as complete only after all the following conditions are met:

- [ ] The complete job runs successfully from beginning to end.
- [ ] All task dependencies are configured correctly.
- [ ] Bronze, Silver, and Gold tables are populated as expected.
- [ ] Data quality checks produce the expected results.
- [ ] Incremental reruns do not introduce unintended duplicates.
- [ ] Failed tasks can be investigated and rerun.
- [ ] The schedule works, if scheduling is required.
- [ ] The notebook paths, architecture, and execution steps are documented in GitHub.

## 14. Official Documentation

- [Create your first workflow with Lakeflow Jobs](https://learn.microsoft.com/en-us/azure/databricks/jobs/jobs-quickstart)
- [Configure and edit Lakeflow Jobs](https://learn.microsoft.com/en-us/azure/databricks/jobs/configure-job)
- [Configure notebook tasks](https://learn.microsoft.com/en-us/azure/databricks/jobs/notebook)
- [Control task execution flow](https://learn.microsoft.com/en-us/azure/databricks/jobs/control-flow)
