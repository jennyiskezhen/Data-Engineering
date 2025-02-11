## NY taxi ride from three different services

In this projects, taxi ride data are obtained from TLC website <https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page> for year 2019. The three taxi ride services are:

- Yellow taxi
- Green taxi
- For-hire vehicles (fhv)

The datasets used are:

- Yellow taxi data
- Green taxi data
- fhv data
- Pick-up and Drop-off location lookups

Raw data were first uploaded to BigQuery using Kestra backfill, then the datasets were processed and joined using dbt. The final dataset for analysis is maintained by CI process for data transformation updates and a recurring schedule for model deployment. 


