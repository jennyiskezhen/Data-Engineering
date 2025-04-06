-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `ny-taxi-jenny.manufacture.external_man_data`
OPTIONS (
  format = 'csv',
  uris = ['gs://ny-taxi-jenny-manufacturing/manufacturing_defect_dataset.csv']
);

-- Create a table from the external table
create or replace table ny-taxi-jenny.manufacture.man_data_raw as
select *
from ny-taxi-jenny.manufacture.external_man_data;

-- partition and filter
create or replace table ny-taxi-jenny.manufacture.man_data_opt 
cluster by 
  DeliveryDelay as
select 
MD5(CONCAT(
              COALESCE(CAST(ProductionVolume AS STRING), ""),
              COALESCE(CAST(ProductionCost AS STRING), ""),
              COALESCE(CAST(SupplierQuality AS STRING), ""),
              COALESCE(CAST(QualityScore AS STRING), ""),
              COALESCE(CAST(StockoutRate AS STRING), "")
            )) AS id,
*
from ny-taxi-jenny.manufacture.external_man_data
;