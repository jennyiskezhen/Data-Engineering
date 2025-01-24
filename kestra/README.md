## Workflow Orchestration Kestra

#### 1. Enable and disable saving output files in Kestra
```
- id: purge_files
  type: io.kestra.plugin.core.storage.PurgeCurrentExecutionFiles
  description: use the disabled property below
  disabled: false
``` 


#### 2. Inputs and Variables in Kestra
`{{inputs.taxi}}` directly render the dynamic variable.
`{{render(vars.file)}}` requires the `render()` method.

#### 3. Table and staging-table for Postgres
Since the datasets are split by month, I used a staging-table to get the data for each month, then merged it to the main table. The contents of the staging-table change according to the month.

#### 4. Table, external-table and temp-table for BigQuery in GCP
External-table is used to get the original data by month, temp-table added unique id using MD5 hash  to prevent repeated data and filename. Temp-table is then merged to the main table.


#### 5. Configure timezone in the Kestra schedule trigger
```
triggers:
  - id: scheduler
    type: io.kestra.plugin.core.trigger.Schedule
    cron: "0 9 1 * *" #at 9am on the first day of every month
    timezone: America/New_York
```
