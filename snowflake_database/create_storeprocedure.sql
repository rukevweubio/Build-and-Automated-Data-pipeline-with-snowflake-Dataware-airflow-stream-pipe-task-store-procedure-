use database customer_database;
// use schema 
use schema customer_schema;

// create pipe to load data into table from stage 
CREATE OR REPLACE PROCEDURE load_pipes_into_tables()
  RETURNS STRING
  LANGUAGE SQL
AS
DECLARE
  result STRING;
BEGIN
  -- Resume the pipes before refreshing them
  EXECUTE IMMEDIATE 'ALTER PIPE mypipe_product SET PIPE_EXECUTION_PAUSED = FALSE;';
  EXECUTE IMMEDIATE 'ALTER PIPE mypipe_order SET PIPE_EXECUTION_PAUSED = FALSE;';
  EXECUTE IMMEDIATE 'ALTER PIPE mypipe_store SET PIPE_EXECUTION_PAUSED = FALSE;';
  EXECUTE IMMEDIATE 'ALTER PIPE mypipe_customer SET PIPE_EXECUTION_PAUSED = FALSE;';
  
  -- Refresh the pipes to load data from the stage into the tables
  EXECUTE IMMEDIATE 'ALTER PIPE mypipe_product REFRESH;';
  EXECUTE IMMEDIATE 'ALTER PIPE mypipe_order REFRESH;';
  EXECUTE IMMEDIATE 'ALTER PIPE mypipe_store REFRESH;';
  EXECUTE IMMEDIATE 'ALTER PIPE mypipe_customer REFRESH;';
  
  -- Return a success message
  RETURN 'Pipes refreshed successfully!';
END;


// TASK TO CALL ALL THE STORE PROCEDURE 
CREATE OR REPLACE TASK task_calling_pipe_load_table
  WAREHOUSE = Compute_wh
  SCHEDULE = '1 MINUTE'  -- Every minute
AS
 CALL load_pipes_into_tables()
;