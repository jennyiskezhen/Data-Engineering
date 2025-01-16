import pandas as pd
from sqlalchemy import create_engine

df = pd.read_csv('green_tripdata_2019-10.csv', nrows = 10)
print(df.columns)

engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')

df_zones = pd.read_csv('taxi_zone_lookup.csv')
df_zones.to_sql(name = 'zones', con = engine, if_exists = 'append')