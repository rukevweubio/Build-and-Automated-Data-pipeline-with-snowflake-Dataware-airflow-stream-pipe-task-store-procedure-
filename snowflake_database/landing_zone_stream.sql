use database customer_database;
// use schema customerschema 
use schema customer_schema;

// create stream on the table to track the change make on the table 
 create or replace stream my_customer_stream on table customer;
 create or replace stream my_product_stream on table products;
 create or replace stream my_order_stream on table orders;
 create or replace stream my_store_stream on table store;