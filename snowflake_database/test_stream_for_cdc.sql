
// test the stream for the customer table 

INSERT INTO CUSTOMER_DATABASE.CUSTOMER_SCHEMA.CUSTOMER (
    CUSTOMERKEY,
    GENDER,
    NAME,
    CITY,
    "State Code",
    STATE,
    "Zip Code",
    COUNTRY,
    CONTINENT,
    BIRTHDAY
) VALUES (
    12345,
    'Male',
    'John Doe',
    'New York',
    'NY',
    'New York',
    '10001',
    'United States',
    'North America',
    '1985-07-15'
);
INSERT INTO CUSTOMER_DATABASE.CUSTOMER_SCHEMA.CUSTOMER (
    CUSTOMERKEY,
    GENDER,
    NAME,
    CITY,
    "State Code",
    STATE,
    "Zip Code",
    COUNTRY,
    CONTINENT,
    BIRTHDAY
) VALUES
    (12345, 'Male', 'John Doe', 'New York', 'NY', 'New York', '10001', 'United States', 'North America', '1985-07-15'),
    (67890, 'Female', 'Jane Smith', 'Los Angeles', 'CA', 'California', '90001', 'United States', 'North America', '1990-03-22');

 select * from my_customer_stream