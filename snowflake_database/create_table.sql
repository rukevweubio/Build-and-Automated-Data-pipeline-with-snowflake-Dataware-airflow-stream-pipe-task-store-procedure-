 select  current_database();
 // create new database 
 create or replace database customer_database;
 create or replace schema customer_schema;

 // create file format 
 create or replace file format my_csv_file_format
 type ='csv'
 field_delimiter =','
 skip_header=  1;

 // create internal stage 
 create or replace stage my_csv_stage
 file_format =(format_name=my_csv_file_format);


// create a varainet table
 create or replace table ny_new_table
 (data variant);

 // create pipe to load data automaticallyCREATE OR REPLACE PIPE mypipe
 
CREATE OR REPLACE PIPE mypipe
  AS
  COPY INTO customer
  FROM @my_csv_stage/ncustomer.csv
  FILE_FORMAT = (TYPE = 'CSV', FIELD_OPTIONALLY_ENCLOSED_BY = '"', SKIP_HEADER = 1);

// create a task to load the data into table customer
  // create atask to automate the process 
  CREATE OR REPLACE TASK load_customer_data_task
  WAREHOUSE = Compute_wh  -- Specify the warehouse to use
  SCHEDULE = '1 Minute'  -- This runs every hour, adjust as needed
AS
  -- Trigger the pipe to load data from the stage into the table
  ALTER PIPE mypipe REFRESH;


  // load the data inot snowfalke with snowsql cli
 list@my_csv_stage;

 //create table to check the datastructure of the file  


  SELECT *
  FROM TABLE(
    INFER_SCHEMA(
      LOCATION=>'@my_csv_stage/Products.csv.gz'
      , FILE_FORMAT=>'my_csv_file_format'
      )
    );

CREATE OR REPLACE TABLE Products
AS
  SELECT 
    $1 AS ProductKey,
    $2 AS ProductName,
    $3 AS Brand,
    $4 AS Color,
    REPLACE($5, '$', '') AS UnitCostUSD,
    REPLACE($6, '$', '') AS UnitPriceUSD,
    $7 AS SubcategoryKey,
    $8 AS Subcategory,
    $9 AS CategoryKey,
    $10 AS Category
  FROM @my_csv_stage/Products.csv.gz

;
// CREATE CUSTOMER TABLE 

  CREATE OR REPLACE TABLE CUSTOMER 
AS 

 SELECT 
    $1 AS CustomerKey,
    $2 AS Gender,
    $3 AS Name,
    $4 AS City,
    $5 AS "State Code",
    $6 AS State,
    $7 AS "Zip Code",
    $8 AS Country,
    $9 AS Continent,
    TO_DATE($10, 'MM/DD/YYYY') AS Birthday
FROM @my_csv_stage/ncustomer.csv;
---FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

// select to make sure the tbale is created 
 select * from customer;

  // create the sales table 
  CREATE OR REPLACE TABLE ORDERS
  AS 
SELECT 
    $1 AS OrderNumber,
    $2 AS LineItem,
    TO_DATE($3, 'MM/DD/YYYY') AS OrderDate,
    TO_DATE($4, 'MM/DD/YYYY') AS DeliveryDate,
    $5 AS CustomerKey,
    $6 AS StoreKey,
    $7 AS ProductKey,
    $8 AS Quantity,
    $9 AS CurrencyCode
FROM @my_csv_stage/Sales.csv;

// create the 
CREATE OR REPLACE  TABLE STORE 
AS
SELECT 
    $1 AS StoreKey,
    $2 AS Country,
    $3 AS State,
    $4 AS SquareMeters,
    TO_DATE($5, 'MM/DD/YYYY') AS OpenDate  -- Assuming Open Date is in MM/DD/YYYY format
FROM @my_csv_stage/Stores.csv;
