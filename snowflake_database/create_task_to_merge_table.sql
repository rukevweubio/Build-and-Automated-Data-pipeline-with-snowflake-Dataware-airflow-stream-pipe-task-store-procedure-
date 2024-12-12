use database customer_database;
 use schema customer_schema;

 // create task for merge the customer table 
 CREATE OR REPLACE TASK CURATED_MERGE_TASK_CUSTOMER
WAREHOUSE = COMPUTE_WH
SCHEDULE = '1 MINUTE'
WHEN SYSTEM$STREAM_HAS_DATA('CUSTOMER_DATABASE.CUSTOMER_SCHEMA.MY_CUSTOMER_STREAM')
as
MERGE INTO CUSTOMER_DATABASE.CURATE_ZONE.CUSTOMER_CURA AS CURATED
USING (
    SELECT
        CUSTOMERKEY,
        GENDER,
        SPLIT(NAME, ' ')[0]::STRING AS FIRST_NAME,  -- Extract first name
        SPLIT(NAME, ' ')[1]::STRING AS LAST_NAME,   -- Extract last name
        CITY,
        "State Code" AS STATE_CODE,
        STATE,
        "Zip Code" AS ZIP_CODE,
        COUNTRY,
        CONTINENT,
        BIRTHDAY,
        CONCAT("Zip Code", ' ', "State Code") AS STATE_ZIP_CODE,
        METADATA$ACTION,
        METADATA$ISUPDATE
    FROM CUSTOMER_DATABASE.CUSTOMER_SCHEMA.MY_CUSTOMER_STREAM
) AS STREAM
ON CURATED.CUSTOMERKEY = STREAM.CUSTOMERKEY
WHEN MATCHED
AND STREAM.METADATA$ACTION = 'INSERT'
AND STREAM.METADATA$ISUPDATE = FALSE
THEN UPDATE SET
    GENDER = STREAM.GENDER,
    FIRST_NAME = SPLIT(STREAM.NAME, ' ')[0]::STRING,
    LAST_NAME = SPLIT(STREAM.NAME, ' ')[1]::STRING,
    CITY = STREAM.CITY,
    STATE_CODE = STREAM.STATE_CODE,
    STATE = STREAM.STATE,
    ZIP_CODE = STREAM.ZIP_CODE,
    COUNTRY = STREAM.COUNTRY,
    CONTINENT = STREAM.CONTINENT,
    BIRTHDAY = STREAM.BIRTHDAY,
    STATE_ZIP_CODE = CONCAT(STREAM.ZIP_CODE, ' ', STREAM.STATE_CODE)
WHEN NOT MATCHED
AND STREAM.METADATA$ACTION = 'INSERT' 
THEN INSERT (
    CUSTOMERKEY,
    GENDER,
    FIRST_NAME,
    LAST_NAME,
    CITY,
    STATE_CODE,
    STATE,
    ZIP_CODE,
    COUNTRY,
    CONTINENT,
    BIRTHDAY,
    STATE_ZIP_CODE
) VALUES (
    STREAM.CUSTOMERKEY,
    STREAM.GENDER,
    SPLIT(STREAM.NAME, ' ')[0]::STRING,
    SPLIT(STREAM.NAME, ' ')[1]::STRING,
    STREAM.CITY,
    STREAM.STATE_CODE,
    STREAM.STATE,
    STREAM.ZIP_CODE,
    STREAM.COUNTRY,
    STREAM.CONTINENT,
    STREAM.BIRTHDAY,
    CONCAT(STREAM.ZIP_CODE, ' ', STREAM.STATE_CODE)
);
// create a task to merge the product table 

CREATE OR REPLACE TASK CURATED_MERGE_TASK_PRODUCT
WAREHOUSE = COMPUTE_WH
SCHEDULE = '3 MINUTE'
WHEN SYSTEM$STREAM_HAS_DATA('CUSTOMER_DATABASE.CUSTOMER_SCHEMA.MY_PRODUCT_STREAM')
AS
MERGE INTO CUSTOMER_DATABASE.CURATE_ZONE.PRODUCTS_CURA AS CURATED
USING (
    SELECT
        PRODUCTKEY,
        PRODUCTNAME,
        BRAND,
        COLOR,
        UNITCOSTUSD,
        UNITPRICEUSD,
        SUBCATEGORYKEY,
        CATEGORYKEY,
        METADATA$ACTION,
        METADATA$ISUPDATE
    FROM CUSTOMER_DATABASE.CUSTOMER_SCHEMA.MY_PRODUCT_STREAM
) AS STREAM
ON CURATED.PRODUCTKEY = STREAM.PRODUCTKEY
WHEN MATCHED
AND STREAM.METADATA$ACTION = 'insert'
AND STREAM.METADATA$ISUPDATE = False
THEN UPDATE SET
    PRODUCTNAME = STREAM.PRODUCTNAME,
    BRAND = STREAM.BRAND,
    COLOR = STREAM.COLOR,
    UNITCOSTUSD = STREAM.UNITCOSTUSD,
    UNITPRICEUSD = STREAM.UNITPRICEUSD,
    SUBCATEGORYKEY = STREAM.SUBCATEGORYKEY,
    CATEGORYKEY = STREAM.CATEGORYKEY
WHEN NOT MATCHED
AND STREAM.METADATA$ACTION = 'INSERT' 
and STREAM.METADATA$ISUPDATE = True
THEN INSERT (
    PRODUCTKEY,
    PRODUCTNAME,
    BRAND,
    COLOR,
    UNITCOSTUSD,
    UNITPRICEUSD,
    SUBCATEGORYKEY,
    CATEGORYKEY
) VALUES (
    STREAM.PRODUCTKEY,
    STREAM.PRODUCTNAME,
    STREAM.BRAND,
    STREAM.COLOR,
    STREAM.UNITCOSTUSD,
    STREAM.UNITPRICEUSD,
    STREAM.SUBCATEGORYKEY,
    STREAM.CATEGORYKEY
);
// create the merge table for the orders 
CREATE OR REPLACE TASK CURATED_MERGE_TASK_ORDER
WAREHOUSE = COMPUTE_WH
SCHEDULE = '4 MINUTE'
WHEN SYSTEM$STREAM_HAS_DATA('CUSTOMER_DATABASE.CUSTOMER_SCHEMA.MY_ORDER_STREAM')
AS
MERGE INTO CUSTOMER_DATABASE.CURATE_ZONE.ORDERS_CURA AS CURATED
USING (
    SELECT
        ORDERNUMBER,
        LINEITEM,
        ORDERDATE,
        DELIVERYDATE,
        CUSTOMERKEY,
        STOREKEY,
        PRODUCTKEY,
        QUANTITY,
        CURRENCYCODE,
        METADATA$ACTION,
        METADATA$ISUPDATE
    FROM CUSTOMER_DATABASE.CUSTOMER_SCHEMA.MY_ORDER_STREAM
) AS STREAM
ON CURATED.PRODUCTKEY = STREAM.PRODUCTKEY
   AND CURATED.CUSTOMERKEY = STREAM.CUSTOMERKEY
WHEN MATCHED
AND STREAM.METADATA$ACTION = 'insert'
AND STREAM.METADATA$ISUPDATE =  false
THEN UPDATE SET
    CURATED.ORDERNUMBER = STREAM.ORDERNUMBER,
    CURATED.LINEITEM = STREAM.LINEITEM,
    CURATED.ORDERDATE = STREAM.ORDERDATE,
    CURATED.DELIVERYDATE = STREAM.DELIVERYDATE,
    CURATED.CUSTOMERKEY = STREAM.CUSTOMERKEY,
    CURATED.STOREKEY = STREAM.STOREKEY,
    CURATED.QUANTITY = STREAM.QUANTITY,
    CURATED.CURRENCYCODE = STREAM.CURRENCYCODE
WHEN NOT MATCHED
AND STREAM.METADATA$ACTION = 'INSERT'
THEN INSERT (
    ORDERNUMBER,
    LINEITEM,
    ORDERDATE,
    DELIVERYDATE,
    CUSTOMERKEY,
    STOREKEY,
    PRODUCTKEY,
    QUANTITY,
    CURRENCYCODE
) VALUES (
    STREAM.ORDERNUMBER,
    STREAM.LINEITEM,
    STREAM.ORDERDATE,
    STREAM.DELIVERYDATE,
    STREAM.CUSTOMERKEY,
    STREAM.STOREKEY,
    STREAM.PRODUCTKEY,
    STREAM.QUANTITY,
    STREAM.CURRENCYCODE
);
// create a merge table for the category 
// product table 
CREATE OR REPLACE TASK CURATED_MERGE_TASK_CATEGORY
WAREHOUSE = COMPUTE_WH
SCHEDULE = '5 MINUTE'
WHEN SYSTEM$STREAM_HAS_DATA('CUSTOMER_DATABASE.CUSTOMER_SCHEMA.MY_PRODUCT_STREAM')
AS
MERGE INTO CUSTOMER_DATABASE.CURATE_ZONE.CATEGORY_CURA AS CURATED
USING (
    SELECT
        CATEGORYKEY,
        CATEGORY,
        METADATA$ACTION,
        METADATA$ISUPDATE
    FROM CUSTOMER_DATABASE.CUSTOMER_SCHEMA.MY_PRODUCT_STREAM
) AS STREAM
ON CURATED.CATEGORYKEY = STREAM.CATEGORYKEY
WHEN MATCHED
AND STREAM.METADATA$ACTION = 'insert'
AND STREAM.METADATA$ISUPDATE = false
THEN UPDATE SET
    CURATED.CATEGORYKEY = STREAM.CATEGORYKEY,
    CURATED.CATEGORY = STREAM.CATEGORY
WHEN NOT MATCHED
AND STREAM.METADATA$ACTION = 'INSERT'
THEN INSERT (
    CATEGORYKEY,
    CATEGORY
) VALUES (
    STREAM.CATEGORYKEY,
    STREAM.CATEGORY
);
// create tabel for category 
// product table 
CREATE OR REPLACE TASK CURATED_MERGE_TASK_SUBCATEGORY
WAREHOUSE = COMPUTE_WH
SCHEDULE = '6 MINUTE'
WHEN SYSTEM$STREAM_HAS_DATA('CUSTOMER_DATABASE.CUSTOMER_SCHEMA.MY_PRODUCT_STREAM')
AS
MERGE INTO CUSTOMER_DATABASE.CURATE_ZONE.SUBCATEGORY AS CURATED
USING (
    SELECT
       
        SUBCATEGORYKEY
        SUBCATEGORY,
        METADATA$ACTION,
        METADATA$ISUPDATE
    FROM CUSTOMER_DATABASE.CUSTOMER_SCHEMA.MY_PRODUCT_STREAM
) AS STREAM
ON CURATED.SUBCATEGORYKEY= STREAM.SUBCATEGORYKEY
WHEN MATCHED
AND STREAM.METADATA$ACTION = 'insert'
AND STREAM.METADATA$ISUPDATE = false
THEN UPDATE SET
    CURATED.SUBCATEGORYKEY = STREAM.SUBCATEGORYKEY,
    CURATED.SUBCATEGORY = STREAM.SUBCATEGORY
WHEN NOT MATCHED
AND STREAM.METADATA$ACTION = 'INSERT'
THEN INSERT (
    SUBCATEGORYKEY,
   SUBCATEGORY
) VALUES (
    STREAM.SUBCATEGORYKEY,
    STREAM.SUBCATEGORY
);
// CREATE MEGER FOR THE STORE TABLE 
CREATE OR REPLACE TASK CURATED_MERGE_TASK_STORE
WAREHOUSE = COMPUTE_WH
SCHEDULE = '7 MINUTE'
WHEN SYSTEM$STREAM_HAS_DATA('CUSTOMER_DATABASE.CUSTOMER_SCHEMA.MY_STORE_STREAM')
AS
MERGE INTO CUSTOMER_DATABASE.CURATE_ZONE.STORE_CURA AS CURATED
USING (
    SELECT
        STOREKEY,
        COUNTRY,
        STATE,
        SQUAREMETERS,
        OPENDATE,
        METADATA$ACTION,
        METADATA$ISUPDATE
    FROM CUSTOMER_DATABASE.CUSTOMER_SCHEMA.MY_STORE_STREAM
) AS STREAM
ON CURATED.STOREKEY = STREAM.STOREKEY
WHEN MATCHED
AND STREAM.METADATA$ACTION = 'INSERT'
AND STREAM.METADATA$ISUPDATE = FALSE
THEN UPDATE SET
    CURATED.STOREKEY = STREAM.STOREKEY,
    CURATED.COUNTRY = STREAM.COUNTRY,
    CURATED.STATE = STREAM.STATE,
    CURATED.SQUAREMETERS = STREAM.SQUAREMETERS,
    CURATED.OPENDATE = STREAM.OPENDATE
WHEN NOT MATCHED
AND STREAM.METADATA$ACTION = 'INSERT'
THEN INSERT (
    STOREKEY,
    COUNTRY,
    STATE,
    SQUAREMETERS,
    OPENDATE
) VALUES (
    STREAM.STOREKEY,
    STREAM.COUNTRY,
    STREAM.STATE,
    STREAM.SQUAREMETERS,
    STREAM.OPENDATE
);
CREATE OR REPLACE TASK PARENT_TASK
WAREHOUSE = COMPUTE_WH
SCHEDULE = '10 MINUTE' -- Adjust the schedule as needed
AS
BEGIN
    -- Activate each task
    EXECUTE IMMEDIATE 'ALTER TASK CURATED_MERGE_TASK_CUSTOMER RESUME';
    EXECUTE IMMEDIATE 'ALTER TASK CURATED_MERGE_TASK_PRODUCT RESUME';
    EXECUTE IMMEDIATE 'ALTER TASK CURATED_MERGE_TASK_ORDER RESUME';
    EXECUTE IMMEDIATE 'ALTER TASK CURATED_MERGE_TASK_CATEGORY RESUME';
    EXECUTE IMMEDIATE 'ALTER TASK CURATED_MERGE_TASK_SUBCATEGORY RESUME';
    EXECUTE IMMEDIATE 'ALTER TASK CURATED_MERGE_TASK_STORE RESUME';
END;