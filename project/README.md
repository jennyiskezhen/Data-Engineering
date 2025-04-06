## Project - Manufacturing defects

### Problem description

This project explores the factors influencing product defect rates in a manufacturing environment. Each record in the dataset represents various influencing factors for predicting high (1) or low (0) defect occurrences in production processes.

### Cloud
The project is developed in GCP and IaC tool (i.e., Terraform) is used to create the GCS bucket (ny-taxi-jenny-manufacturing) and BigQuery DWH dataset (manufacture)

### Data ingestion 
Batch processing is used for dataset ingestion. Data is ingested to GCS using the workflow orchestration tool Kestra. 

### Data warehouse

Data table is clustered based on the Delivery Delay feature since it is used for ordering and sorting the data.

### Dashboard
A dashboard is created to show the interactions between the influencing factors and the production defects: <https://lookerstudio.google.com/s/uYjlnmPLsiw>

Viewers can control the data range by selecting the data associated with either low or high defects. The dashboard shows four charts:

- a pie chart showing the distribution of low and high defects. There are more data associated with high defect ratio than low defect ratio. 
- a bar chart showing the record counts associated with maintenance hours per week of the record for low and high defects, respectively. There is no clear relation between maintenance hours and the number of high/low defects within the first 10 hours of maintenance. 
- a scatter plot  showing the relationship between  Defect rate and supplier quality. There is a clean  trend of lower defect rate related to higher supplier quality. To a smaller extend, a lower defect rate is related to a higher delivery delay. 
- a treemap showing the record number for different maintenance hours. There is no clear pattern. 

### Instructions for reproducing the results
To reproduce the dashboard results, please follow the steps below:

1. Use Terraform to create the resources on GCP
2. In kestra, use the plugin for scripts.shell.commands and Kaggel CLI method to download the dataset. Then use the plugin for gcp.gcs.Upload to ingest the data to GCS.
3. In BigQuery, create an external table from the gcs path of the ingested dataset. Create an optimized materialized dataset using data clustering from the external table. 
4. In Looker Studio, connect the datasource to the dataset in BigQuery, then create charts to explore the various relationships between features and the product  defect rates.  