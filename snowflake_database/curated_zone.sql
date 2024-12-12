use database customer_database;

// create a curates_zone schema 
 create  schema curate_zone;


CREATE OR REPLACE TABLE PRODUCTS
AS 
SELECT 
    PRODUCTKEY,
    PRODUCTNAME,
    BRAND,
    COLOR,
    REPLACE(REPLACE(UNITCOSTUSD, '$', ''), ',', '')::FLOAT AS UNITCOSTUSD,  -- Remove '$' and ',' then convert to FLOAT
    REPLACE(REPLACE(UNITPRICEUSD, '$', ''), ',', '')::FLOAT AS UNITPRICEUSD,  -- Remove '$' and ',' then convert to FLOAT
    SUBCATEGORYKEY,
    ---SUBCATEGORY,
    CATEGORYKEY,
   --- CATEGORY
FROM 
    customer_database.customer_schema.products;


// CREATE THE CATEGORY AND THE SUBCATEGORY TABLE TO NORMALIZE THE TABLE



CREATE OR REPLACE TABLE SUBCATEGORY
AS 
SELECT
    SUBCATEGORYKEY,
    SUBCATEGORY,
    --CATEGORYKEY,
   --- CATEGORY
FROM 
    customer_database.customer_schema.products;
use schema curated_zone

// create table on the curated zone to track and update the data from the lansing zone 
create or replace table customer_trasf
as 

SELECT 
    CUSTOMERKEY,
    GENDER,
    SPLIT(NAME, ' ')[0]::STRING AS first_name,  -- Extract first name
    SPLIT(NAME, ' ')[1]::STRING AS last_name,    -- Extract last name
    CITY,
    "State Code",
    STATE,
    "Zip Code",
    COUNTRY,
    CONTINENT,
    BIRTHDAY,
    CONCAT("Zip Code", ' ', "State Code") AS state_zip_code  -- Concatenate "Zip Code" and "Code"
FROM 
    customer_database.customer_schema.customer;

select * from customer_trasf

CREATE OR REPLACE TABLE PRODUCTS
AS 
SELECT 
    PRODUCTKEY,
    PRODUCTNAME,
    BRAND,
    COLOR,
    REPLACE(REPLACE(UNITCOSTUSD, '$', ''), ',', '')::FLOAT AS UNITCOSTUSD,  -- Remove '$' and ',' then convert to FLOAT
    REPLACE(REPLACE(UNITPRICEUSD, '$', ''), ',', '')::FLOAT AS UNITPRICEUSD,  -- Remove '$' and ',' then convert to FLOAT
    SUBCATEGORYKEY,
    ---SUBCATEGORY,
    CATEGORYKEY,
   --- CATEGORY
FROM 
    customer_database.customer_schema.products;  

 // CREATE THE SUBCATEGORY TABLE 
 CREATE OR REPLACE TABLE CATEGORY
AS 
SELECT
    
    CATEGORYKEY,
   CATEGORY
FROM 
    customer_database.customer_schema.products;

     TRUNCATE TABLE PRODUCTS

// create the orders table in the curated zone
 CREATE OR REPLACE TABLE ORFERS_CURA
 as
 select 

ORDERNUMBER,
LINEITEM,
ORDERDATE,
DELIVERYDATE,
CUSTOMERKEY,
STOREKEY,
PRODUCTKEY,
QUANTITY,
CURRENCYCODE
from customer_database.customer_schema.orders

// create the sales table in the curated zone 
 CREATE OR REPLACE TABLE STORE_CURA
 as
 select 
    STOREKEY,
    COUNTRY,
    STATE,
    SQUAREMETERS,
    OPENDATE,
from customer_database.customer_schema.store