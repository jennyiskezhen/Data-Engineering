{{
    config(
        materialized='view'
    )
}}

with mandata as 
(
  select *,
    -- add order by
    row_number() over(order by DeliveryDelay,ProductionVolume, ProductionCost) as rn
  from {{ source('staging','man_data_opt') }}
)
select
    -- id
    {{ dbt_utils.generate_surrogate_key(['ProductionVolume', 'ProductionCost', 'SupplierQuality']) }} as manid,
    
    -- integer
    cast(ProductionVolume as integer) as ProductionVolume,
    cast(DeliveryDelay as integer) as DeliveryDelay,
    cast(MaintenanceHours as integer) as MaintenanceHours,
    cast(SafetyIncidents as integer) as SafetyIncidents,
    
    -- numeric
    cast(ProductionCost as numeric) as ProductionCost,
    cast(SupplierQuality as numeric) as SupplierQuality,
    cast(DefectRate as numeric) as DefectRate,
    cast(QualityScore as numeric) as QualityScore,
    cast(DowntimePercentage as numeric) as DowntimePercentage,
    cast(InventoryTurnover as numeric) as InventoryTurnover,
    cast(StockoutRate as numeric) as StockoutRate,
    cast(WorkerProductivity as numeric) as WorkerProductivity,
    cast(EnergyConsumption as numeric) as EnergyConsumption,
    cast(EnergyEfficiency as numeric) as EnergyEfficiency,
    cast(AdditiveProcessTime as numeric) as AdditiveProcessTime,
    cast(AdditiveMaterialCost as numeric) as AdditiveMaterialCost,
    cast(DefectStatus as numeric) as DefectStatus,

from mandata
where rn = 1