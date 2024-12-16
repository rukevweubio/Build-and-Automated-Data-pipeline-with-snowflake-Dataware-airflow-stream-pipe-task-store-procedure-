## Project Overview:
This project  focuses on automating an ETL pipeline using Snowflake to streamline data processing, transformation, and loading.
It addresses the challenge of manually handling large datasets, ensuring timely and accurate data updates.
By utilizing Snowflake's Streams, Tasks, Pipes, and SnowSQL, the pipeline will automate real-time data ingestion, transformation, and loading, significantly
improving operational efficiency and data accuracy. Currently, the data processing workflows are manual, time-consuming, and prone to errors,
highlighting the need for an automated system that ensures real-time data updates and transformation with minimal intervention. The solution 
automates the entire ETL process, from data extraction to transformation and loading, using Snowflake’s
native capabilities. This ensures seamless, error-free data flow, increases operational efficiency, and provides stakeholders with up-to-date insights.
### Project objective
The project aims to automate data extraction, transformation, and loading processes to improve data accuracy, ensure real-time updates, and minimize manual
intervention. It seeks to enhance scalability to handle growing data volumes efficiently. The expected outcomes include a
fully automated ETL pipeline in Snowflake, real-time data availability, and improved decision-making from timely and accurate data. Success will be
measured by seamless data integration into Snowflake, automated transformation tasks, accurate data loading, and real-time monitoring and error handling using Snowflake's native features.
### Project features
The project features core functionalities such as real-time data ingestion using Snowflake Pipes for continuous loading,
change data capture with Streams to detect and update changes in source tables, and automated data transformation through 
Snowflake Tasks, reducing manual SQL executions. It also includes efficient querying, transformation, and reporting via SnowSQL.
Innovative aspects include the integration of Streams and Tasks, which ensures real-time updates and automated transformations,
maintaining an up-to-date pipeline without manual intervention. The pipeline is designed for scalability and flexibility,
leveraging Snowflake’s cloud-native architecture to handle increasing data volumes. Additionally, built-in error handling 
and monitoring features ensure reliability by automatically managing failures and retries.
### Project Tech Stack
- Snowflake
- snowflake stream : Real-time change data capture (CDC) for tracking changes in source tables.
- Snowflake Tasks:  Automation of SQL-based transformation jobs based on defined schedules or events
- Snowflake Pipes: Continuous data loading into Snowflake from external sources.
- SnowSQL:Command-line client for executing queries, automating transformation processes, and managing Snowflake resources.
- SQL (Snowflake SQL):Used for writing and executing SQL queries for data manipulation and transformation
### How to Set Up the Tech Stack
- Snowflake Setup:create a Snowflake account if not already done and Set up a Snowflake warehouse, database, and schema to manage the ETL pipeline
```
CREATE WAREHOUSE my_warehouse;
CREATE DATABASE my_database;
CREATE SCHEMA my_schema;
````
- Install SnowSQL:Download and install SnowSQL from Snowflake’s official site [snowflake.com](snowflake.com)
- Configure SnowSQL Connection:Configure SnowSQL to connect to your Snowflake account
```
snowsql -a <account_identifier> -u <username> -r <role> -w <warehouse> -d <database> -s <schema>
```
- Set Up Snowflake Pipes for Data Ingestion:Define the external stage and pipe
``` CREATE STAGE my_stage
 FILE_FORMAT = (TYPE = 'CSV');
```
```
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

```
- Set Up Snowflake Streams for Change Data Capture (CDC):Create a stream to track changes in a source table

 ``` 
  CREATE OR REPLACE STREAM my_stream ON TABLE my_source_table;
```
-  Snowflake Tasks for Automation:Create a task to automate transformation
```
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
```
- create store procedure to automate the process of the pipeline
  ```// create task for DIm_data
CREATE OR REPLACE TASK my_store_procedure_dim_date
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = '2 MINUTE'  
  
  WHEN SYSTEM$STREAM_HAS_DATA('UBIO_DEMO.UBIO_SCHEMA.DIM_DATE_STREAM_2') 
  AS
    MERGE INTO UBIO_DEMO.UBIO_SCHEMA."Dim_date" AS Dim_date
    USING (
        SELECT 
            "Date_id",
            "Date_of_Purchase",
            "DAY_NAME",
            "MONTH_NAME",
            "Year",
            "Quarter",
            "Hour_of_Purchase",
            METADATA$ACTION,
            METADATA$ISUPDATE
        FROM UBIO_DEMO.UBIO_SCHEMA.DIM_DATE_STREAM_2
    ) AS stream
    ON Dim_date."Date_id" = stream."Date_id"

    -- Update when matched and action is 'INSERT' and isupdate is false
    WHEN MATCHED AND stream.METADATA$ACTION = 'INSERT' AND stream.METADATA$ISUPDATE = FALSE THEN
        UPDATE SET 
            Dim_date."Date_of_Purchase" = stream."Date_of_Purchase",
            Dim_date."DAY_NAME" = stream."DAY_NAME",
            Dim_date."MONTH_NAME" = stream."MONTH_NAME",
            Dim_date."Year" = stream."Year",
            Dim_date."Quarter" = stream."Quarter",
            Dim_date."Hour_of_Purchase" = stream."Hour_of_Purchase"

    -- Insert when not matched and action is 'INSERT'
    WHEN NOT MATCHED AND stream.METADATA$ACTION = 'INSERT' THEN
        INSERT ("Date_id", "Date_of_Purchase", "DAY_NAME", "MONTH_NAME", "Year", "Quarter", "Hour_of_Purchase")
        VALUES (
            stream."Date_id", 
            stream."Date_of_Purchase", 
            stream."DAY_NAME", 
            stream."MONTH_NAME", 
            stream."Year", 
            stream."Quarter", 
            stream."Hour_of_Purchase"
        );
```
- create table in the raw stage and using the copy command to load data into table
```// create a table  and load the data from the stage 
 create or replace table uk_railway_company
as 
select 
    $1 as "Transaction ID",
    $2 as "Date of Purchase",
    $3 as "Time of Purchase",
    $4 as "Purchase Type",
    $5 as "Payment Method",
    $6 as "Railcard",
    $7 as "Ticket Class",
    $8 as "Ticket Type",
    $9 as "Price",
    $10 as "Departure Station",
    $11 as "Arrival Destination",
    $12 as "Date of Journey",
    $13 as "Departure Time",
    $14 as "Arrival Time",
    $15 as "Actual Arrival Time",
    $16 as "Journey Status",
    $17 as "Reason for Delay",
    $18 as "Refund Request"
from @uk_internal_stage;
// check if the table is created 
select * from uk_railway_company
```
### Data Sources
- Structured Data: The primary data used in the pipeline is structured data, such as transactional data, sales records, customer information, and financial data.
- Flat Files:the Data type flat files (CSV, json)
- Format:csv.
### Data Flow
The data flow through this ETL pipeline is designed to automate the process of ingesting, transforming, and loading data into Snowflake.
The process involves loading a CSV file into Snowflake using SnowSQL, where the file is initially staged. With the help of a Snowflake Pipe,
you automate the process of loading the data from the stage into the target table. A task is used to trigger the pipe and automate the loading 
process. The loading operation is written as a stored procedure, which is wrapped in the task. Each time data is loaded into the stage,
the task calls the pipe to load the data into the target table, ensuring an automated, continuous data flow
```CREATE OR REPLACE TASK my_store_procedure_task2
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = '1 MINUTE'  -- Runs every minute
  AS
  BEGIN
      CALL stream_on_table3();
      CALL stream_on_table();
      CALL stream_on_table2();
  END;
```
- stream:
After loading the file into the table, a Snowflake Stream is created on the table to capture any changes, including inserts, updates, and deletes. The Stream tracks changes in the table that occur between ETL operations, enabling real-time detection of data changes for efficient monitoring and processing.
```CREATE OR REPLACE PROCEDURE my_update()
RETURNS text
LANGUAGE SQL
AS
$$
DECLARE
    sql_statement text;
BEGIN
    -- Prepare the dynamic SQL for merge statement
    sql_statement := '
    MERGE INTO UBIO_DEMO.UBIO_SCHEMA."Dim_railcard" AS Dim_railcard
    USING (
        SELECT 
            RAILCARD_ID,
            RAILCARD,
            METADATA$ACTION,
            METADATA$ISUPDATE
        FROM UBIO_DEMO.UBIO_SCHEMA.DIM_RAILCARD_STREAM
    ) AS stream
    ON Dim_railcard."RAILCARD_ID" = stream."RAILCARD_ID"
    
    -- Update when matched and action is insert and isupdate is false
    WHEN MATCHED AND stream.METADATA$ACTION = ''INSERT'' AND stream.METADATA$ISUPDATE = FALSE THEN
        UPDATE SET 
            Dim_railcard."RAILCARD" = stream."RAILCARD"
    
    -- Delete when matched and action is delete
    WHEN MATCHED AND stream.METADATA$ACTION = ''DELETE'' THEN
        DELETE
    
    -- Insert when not matched and action is insert
    WHEN NOT MATCHED AND stream.METADATA$ACTION = ''INSERT'' THEN
        INSERT ("RAILCARD_ID", "RAILCARD")
        VALUES (stream."RAILCARD_ID", stream."RAILCARD");
    ';

    -- Execute the dynamic SQL
    EXECUTE IMMEDIATE :sql_statement;

    -- Return success message
    RETURN 'Procedure executed successfully';
END;
$$;

call my_update()
```
snowflake trasformation 
- Data transformation is automated using Snowflake Tasks, which are scheduled to run periodically or in 
response to data changes. These tasks execute SQL transformations on raw data in staging tables, 
applying business logic, data cleaning, aggregation, and calculations to generate the required output.
Once data is transformed, it is loaded into the target tables within Snowflake for analysis and reporting. 
As transformation tasks run, the data in the target tables is updated in near real-time,
ensuring it is readily available for querying by business users or reporting tools.
```// create  station tabl
create or replace table Dim_Station
as
select 
    my_sq.nextval as Station_ID,
    "Departure Station" as Dp_Station,
    "Arrival Destination" as Arr_station
from    
    UK_DEMO.UK_SCHEMA.UK_RAILWAY_COMPANY;
select * from  Dim_Station

// create  the journey table 
create or replace table   Dim_Journeys
as
SELECT
    my_sq.nextval as Journey_ID,
"Date of Journey" as Date_of_Journey,
"Departure Time" as Departure_Time,
"Arrival Time" as Arrival_Time,
"Actual Arrival Time" as Actual_Arrival_Time,
"Journey Status" as Journey_Status
from    
    UK_DEMO.UK_SCHEMA.UK_RAILWAY_COMPANY;

//  create the refund table 
create or replace table Dim_Refunds
as 
SELECT 
    my_sq.nextval as Refund_ID,
    "Reason for Delay" as Reason_for_Delay,
    "Refund Request" as Refund_Request
from    
    UK_DEMO.UK_SCHEMA.UK_RAILWAY_COMPANY;

//create  table Dim_Date
create or replace table Dim_Date
as
 select 
    my_sq.nextval as Date_ID,
    "Date of Purchase" as Date_of_Purchase,
    dayname(to_date("Date of Purchase")) as day_name ,
    monthname(to_date("Date of Purchase")) as month_name,
     year(to_date("Date of Purchase")) as Year,
     QUARTER(to_date("Date of Purchase")) as Quanter,
    HOUR("Time of Purchase"::time) as Hour_of_Purchase
   
from    
    UK_DEMO.UK_SCHEMA.UK_RAILWAY_COMPANY;

 select * from Dim_Date

 // create the time table 
 create or replace table  Dim_Time
 as
select 
    my_sq.nextval as  Time_ID,
    HOUR("Time of Purchase"::time) as Hour_of_Purchase,
    Minute("Time of Purchase"::time) as minute_of_Purchase,
    second("Time of Purchase"::time) as second_of_Purchase
from    
    UK_DEMO.UK_SCHEMA.UK_RAILWAY_COMPANY;

//create the fact table 
create or replace table Dim_Date_Departure
as
 select 
    my_sq.nextval as Date_ID_Departure,
    "Date of Purchase" as Date_of_Purchase,
    dayname(to_date("Date of Journey")) as day_name_journey ,
    monthname(to_date("Date of Journey")) as month_name_journey,
     year(to_date("Date of Journey")) as Year_journey,
     QUARTER(to_date("Date of Journey")) as Quanter,
    HOUR("Departure Time"::time) as Departure_Time
   
from    
    UK_DEMO.UK_SCHEMA.UK_RAILWAY_COMPANY;
 ```




### project  Architecture
- **External Data Sources**: Structured and unstructured data (APIs, databases, CSV, JSON) are sourced from external systems for processing.
- **Snowflake**: Central data warehouse for scalable storage and querying, where data is processed and stored.
- **Snowflake Stream**: Captures changes in source tables (inserts, updates, deletes) to enable real-time data updates via change data capture (CDC).
- **Snowflake Tasks**: Automates SQL-based data transformations, scheduled or triggered by Streams or custom schedules.
- **Data Transformation**: SQL queries apply business logic, data cleaning, aggregation, and enrichment to the raw data.
- **Target Tables**: Transformed data is loaded into target tables for analysis and reporting, typically in a normalized format.
- **SnowSQL / BI Tools**: SnowSQL enables command-line querying, while BI tools like Tableau or Power BI visualize the data and generate reports.
![project  Architecture](https://github.com/rukevweubio/Build-Automated-Data-pipeline-with-snowflake-task-stream-and-store-procedure-/blob/main/Data%20architecture.jpeg)


