-- Table Modifications for orders_table

-- 1. Modify column types to align with proper data types
ALTER TABLE orders_table
    ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID;

ALTER TABLE orders_table
    ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID;

ALTER TABLE orders_table
    ALTER COLUMN card_number TYPE VARCHAR(19);

ALTER TABLE orders_table
    ALTER COLUMN store_code TYPE VARCHAR(12);

ALTER TABLE orders_table
    ALTER COLUMN product_code TYPE VARCHAR(11);

-- Table Modifications for dim_users

-- 2. Modify column types for user details table
ALTER TABLE dim_users
    ALTER COLUMN first_name TYPE VARCHAR(255);

ALTER TABLE dim_users
    ALTER COLUMN last_name TYPE VARCHAR(255);

ALTER TABLE dim_users
    ALTER COLUMN date_of_birth TYPE DATE USING date_of_birth::DATE;

ALTER TABLE dim_users
    ALTER COLUMN join_date TYPE DATE USING join_date::DATE;

ALTER TABLE dim_users
    ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID;

-- Modify country_code length in dim_users
ALTER TABLE dim_users
    ALTER COLUMN country_code TYPE VARCHAR(3);

-- Table Modifications for dim_store_details

-- 3. Update store details table
UPDATE dim_store_details
    SET lat = COALESCE(lat, latitude);

ALTER TABLE dim_store_details
    DROP COLUMN latitude;

ALTER TABLE dim_store_details
    RENAME COLUMN lat TO latitude;

ALTER TABLE dim_store_details
    ALTER COLUMN longitude TYPE NUMERIC USING longitude::NUMERIC;

ALTER TABLE dim_store_details
    ALTER COLUMN latitude TYPE NUMERIC USING latitude::NUMERIC;

ALTER TABLE dim_store_details
    ALTER COLUMN locality TYPE VARCHAR(255);

ALTER TABLE dim_store_details
    ALTER COLUMN store_code TYPE VARCHAR(11);

ALTER TABLE dim_store_details
    ALTER COLUMN staff_numbers TYPE SMALLINT USING staff_numbers::SMALLINT;

ALTER TABLE dim_store_details
    ALTER COLUMN opening_date TYPE DATE USING opening_date::DATE;

ALTER TABLE dim_store_details
    ALTER COLUMN store_type TYPE VARCHAR(255);

ALTER TABLE dim_store_details
    ALTER COLUMN country_code TYPE VARCHAR(2);

ALTER TABLE dim_store_details
    ALTER COLUMN continent TYPE VARCHAR(255);

-- Table Modifications for dim_products

-- 4. Modify product details table and remove unwanted characters
UPDATE dim_products
    SET product_price = REGEXP_REPLACE(product_price, '[£]', '');

ALTER TABLE dim_products
    ADD COLUMN weight_class VARCHAR(20);

UPDATE dim_products
    SET weight_class = CASE
        WHEN weight < 2 THEN 'Light'
        WHEN weight >=2 AND weight < 40 THEN 'Mid_Sized'
        WHEN weight >= 40 AND weight < 140 THEN 'Heavy'
        WHEN weight >= 140 THEN 'Truck_Required'
        ELSE NULL
    END;

-- Modify columns in dim_products
ALTER TABLE dim_products
    ALTER COLUMN product_price TYPE NUMERIC USING product_price::NUMERIC;

ALTER TABLE dim_products
    ALTER COLUMN weight TYPE NUMERIC USING weight::NUMERIC;

ALTER TABLE dim_products
    ALTER COLUMN "EAN" TYPE VARCHAR(17);

ALTER TABLE dim_products
    ALTER COLUMN product_code TYPE VARCHAR(11);

ALTER TABLE dim_products
    ALTER COLUMN uuid TYPE UUID USING uuid::UUID;

ALTER TABLE dim_products
    ALTER COLUMN still_available TYPE BOOL USING still_available::BOOL;

ALTER TABLE dim_products
    ALTER COLUMN weight_class TYPE VARCHAR(14);

-- Table Modifications for dim_card_details

-- 5. Modify card details table
ALTER TABLE dim_card_details
    ALTER COLUMN card_number TYPE VARCHAR(22);

ALTER TABLE dim_card_details
    ALTER COLUMN expiry_date TYPE VARCHAR(5);

ALTER TABLE dim_card_details
    ALTER COLUMN date_payment_confirmed TYPE DATE USING date_payment_confirmed::DATE;

-- Adding Primary Keys to Dimension Tables

-- 6. Add primary keys to dimension tables
ALTER TABLE dim_date_times
    ADD PRIMARY KEY (date_uuid);

ALTER TABLE dim_users
    ADD PRIMARY KEY (user_uuid);

ALTER TABLE dim_card_details
    ADD PRIMARY KEY (card_number);

ALTER TABLE dim_store_details
    ADD PRIMARY KEY (store_code);

ALTER TABLE dim_products
    ADD PRIMARY KEY (product_code);

-- Adding Foreign Keys to orders_table

-- 7. Establish foreign key relationships in orders_table
ALTER TABLE orders_table
    ADD CONSTRAINT fk_date_uuid
    FOREIGN KEY(date_uuid) REFERENCES dim_date_times(date_uuid);

ALTER TABLE orders_table
    ADD CONSTRAINT fk_user_uuid
    FOREIGN KEY (user_uuid) REFERENCES dim_users(user_uuid);

ALTER TABLE orders_table
    ADD CONSTRAINT fk_card_number
    FOREIGN KEY (card_number) REFERENCES dim_card_details(card_number);

ALTER TABLE orders_table
    ADD CONSTRAINT fk_store_code
    FOREIGN KEY (store_code) REFERENCES dim_store_details(store_code);

ALTER TABLE orders_table
    ADD CONSTRAINT fk_product_code
    FOREIGN KEY (product_code) REFERENCES dim_products(product_code);
