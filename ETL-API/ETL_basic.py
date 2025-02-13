import duckdb

# 1. Create a connection to an in-memory DuckDB database
conn = duckdb.connect("ny_taxi_manual.db")

# 2. Create the rides Table
# Since our dataset has nested structures, we must manually flatten it before inserting data.
conn.execute("""
CREATE TABLE IF NOT EXISTS rides (
    record_hash TEXT PRIMARY KEY,
    vendor_name TEXT,
    pickup_time TIMESTAMP,
    dropoff_time TIMESTAMP,
    start_lon DOUBLE,
    start_lat DOUBLE,
    end_lon DOUBLE,
    end_lat DOUBLE
);
""")

# 3. Insert Data Manually
# Since JSON data has nested fields, we need to extract and transform them before inserting them into DuckDB.
data = [
    {
        "vendor_name": "VTS",
        "record_hash": "b00361a396177a9cb410ff61f20015ad",
        "time": {
            "pickup": "2009-06-14 23:23:00",
            "dropoff": "2009-06-14 23:48:00"
        },
        "coordinates": {
            "start": {"lon": -73.787442, "lat": 40.641525},
            "end": {"lon": -73.980072, "lat": 40.742963}
        }
    }
]

# Prepare data for insertion
flattened_data = [
    (
        ride["record_hash"],
        ride["vendor_name"],
        ride["time"]["pickup"],
        ride["time"]["dropoff"],
        ride["coordinates"]["start"]["lon"],
        ride["coordinates"]["start"]["lat"],
        ride["coordinates"]["end"]["lon"],
        ride["coordinates"]["end"]["lat"]
    )
    for ride in data
]

# Insert into DuckDB
conn.executemany("""
INSERT INTO rides (record_hash, vendor_name, pickup_time, dropoff_time, start_lon, start_lat, end_lon, end_lat)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)
""", flattened_data)

print("Data successfully loaded into DuckDB!")


# 4. Query Data in DuckDB
# Now that the data is loaded, we can query it using DuckDB’s SQL engine.
df = conn.execute("SELECT * FROM rides").df()

conn.close()
df