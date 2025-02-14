## Data ETL from APIs

REST APIs (Representational State Transfer Application Programming Interfaces) allow you to extract structured data using HTTP requests (GET, POST, PUT, DELETE). 

Challenges with REST APIs are:

- Rate limits: many APIs limit the number of requests one can make in a certain time frame
- Authentication: many APIs require an API key or token to access data
- Pagination: many APIs return data in chunks (or pages). To retrieve all the data, one needs to make multiple requests for all the pages until the last one
- Limited Memory requires usage control

### Extracting data from APIs using the `requests` library
In python, one can use the `requests` library to extract the data from APIs. This includes creating a generator that requests data page by page.

This method has low throughput, since data transfer is limited by API constraints such as rate limits and response time. 

### Loading data to DuckDB database
A basic pipeline requires:

- Setting up a database connection
- Creating tables and defining schemas manually
- Handling schema changes manually
- Writing queries to insert/update data

### dlt - data load tool
The ETL tool dlt is an open-source Python library for more efficient ETL processes.

dlt has the following advantages:

- Built-in REST API support
- Automatic pagination handling
- Manages rate limits and retries
- Streaming support: extracts and processes data without loading everything into memory
- Works with normalization and loading automatically in a single pipeline
- Incremental Loading: load only new or changed data



