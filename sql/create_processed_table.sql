CREATE EXTERNAL TABLE retail_orders_processed (
  row_id INT,
  order_id STRING,
  order_date DATE,
  ship_date DATE,
  ship_mode STRING,
  customer_id STRING,
  segment STRING,
  country STRING,
  city STRING,
  state STRING,
  region STRING,
  product_id STRING,
  category STRING,
  sub_category STRING,
  product_name STRING,
  sales DOUBLE,
  quantity INT,
  discount DOUBLE,
  profit DOUBLE,
  cost_estimate DOUBLE,
  discount_amount DOUBLE
)
STORED AS PARQUET
LOCATION 'path';
