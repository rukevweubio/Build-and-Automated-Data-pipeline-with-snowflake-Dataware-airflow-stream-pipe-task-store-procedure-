// createa stream to capture the chnage in the curated zone layer 
// crearte a stream that capture the curated zone 

use database customer_database;
use schema curate_zone;

create or replace stream curated_stream_customer on  table customer_cura;
create or replace stream curated_stream_orders on  table orders_cura;
create or replace stream curated_stream_products on  table products_cura;
create or replace stream curated_stream_orders on  table orders_cura;
create or replace stream curated_stream_store on  table store_cura;
create or replace stream curated_stream_category on  table category_cura;
create or replace stream curated_stream_subcategory on  table subcategory;