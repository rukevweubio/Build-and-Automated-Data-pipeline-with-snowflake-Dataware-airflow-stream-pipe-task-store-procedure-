// use database 
use database customer_database;
use schema customer_schema;

// create pipe to load data into tbale 
CREATE OR REPLACE PIPE mypipe_product
  
  AS
  COPY INTO PRODUCTS
  FROM @my_csv_stage/Products.csv
  FILE_FORMAT = (TYPE = 'CSV', FIELD_OPTIONALLY_ENCLOSED_BY = '"', SKIP_HEADER = 1);


  // pipe to load data into orders
  CREATE OR REPLACE PIPE mypipe_ORDER
  AS
  COPY INTO ORDERS
  FROM @my_csv_stage/Sales.csv
  FILE_FORMAT = (TYPE = 'CSV', FIELD_OPTIONALLY_ENCLOSED_BY = '"', SKIP_HEADER = 1);

  //create pipe to load data into store 
    CREATE OR REPLACE PIPE mypipe_store
  AS
  COPY INTO STORE
  FROM @my_csv_stage/Stores.csv
  FILE_FORMAT = (TYPE = 'CSV', FIELD_OPTIONALLY_ENCLOSED_BY = '"', SKIP_HEADER = 1);

  // create piep to load data to customer table 
    CREATE OR REPLACE PIPE mypipe_customer
  AS
  COPY INTO customer
  FROM @my_csv_stage/ncustomer.csv
  FILE_FORMAT = (TYPE = 'CSV', FIELD_OPTIONALLY_ENCLOSED_BY = '"', SKIP_HEADER = 1);

  // create atask to automate the process to load the data into the table 
  CREATE OR REPLACE PROCEDURE load_product_data_task_proc()
  RETURNS STRING
  LANGUAGE SQL
AS
DECLARE
  result STRING;
BEGIN
  -- Create the task to refresh the product pipe every minute
  EXECUTE IMMEDIATE '
    CREATE OR REPLACE TASK load_product_data_task
    WAREHOUSE = Compute_wh
    SCHEDULE = "1 MINUTE"  -- Every minute
    AS
      ALTER PIPE mypipe_product REFRESH;
  ';

  -- Return a success message
  RETURN 'Product data pipe task is set up and scheduled successfully.';
END;




CREATE OR REPLACE PROCEDURE load_store_data_task_proc()
  RETURNS STRING
  LANGUAGE SQL
AS
DECLARE
  result STRING;
BEGIN
  -- Create the task to refresh the store pipe every minute
  EXECUTE IMMEDIATE'
    CREATE OR REPLACE TASK load_store_data_task
    WAREHOUSE = Compute_wh
    SCHEDULE = "1 MINUTE"  -- Every minute
    AS
      ALTER PIPE mypipe_store REFRESH;
  ';

  -- Return a success message
  RETURN 'Store data pipe task is set up and scheduled successfully.';
END;


CREATE OR REPLACE PROCEDURE load_orders_data_task_proc()
  RETURNS STRING
  LANGUAGE SQL
AS
DECLARE
  result STRING;
BEGIN
  -- Create the task to refresh the orders pipe every minute
  EXECUTE IMMEDIATE '
    CREATE OR REPLACE TASK load_orders_data_task
    WAREHOUSE = Compute_wh
    SCHEDULE = "1 MINUTE"  -- Every minute
    AS
      ALTER PIPE mypipe_orders REFRESH;
  ';

  -- Return a success message
  RETURN 'Orders data pipe task is set up and scheduled successfully.';
END;




CREATE OR REPLACE PROCEDURE load_product_data_task_proc()
  RETURNS STRING
  LANGUAGE SQL
AS
DECLARE
  result STRING;
BEGIN
  -- Create the task to refresh the product pipe every minute
  EXECUTE IMMEDIATE '
    CREATE OR REPLACE TASK load_product_data_task
    WAREHOUSE = Compute_wh
    SCHEDULE = "1 MINUTE"  -- Every minute
    AS
      ALTER PIPE mypipe_product REFRESH;
  ';

  -- Return a success message
  RETURN 'Product data pipe task is set up and scheduled successfully.';
END;

call load_product_data_task_proc()

// create a task that call all the store procedure 
 create or replace task task_calling_pipe_load_table
 warehouse=compute_wh
 schedule ='1 MINUTE' 
 AS

CALL load_product_data_task_proc()
 