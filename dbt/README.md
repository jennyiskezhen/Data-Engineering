## dbt

First create the folder in GitHub, then initialize the project in dbt

The production and deployment environment are in the same GCP project database, as well as the source data.
When connecting BigQuery dataset to the dbt project, it is important to keep the net dataset location (under `Optional Settings`) the same as the source data. There are three datasets being used:

- source data
- development data (created by dbt)
- production/deployment data (created by dbt)

 
`Dbt build` - to build the dbt model <br>
`Dbt deps` - to install the packages <br>
`Det docs generate` - to generation documentation <br>

```
dbt build --select +fact_trips+ --vars '{'is_test_run': 'false'}'
```

Where `+fact_trips+` means both parents and children of the fact_trips model

