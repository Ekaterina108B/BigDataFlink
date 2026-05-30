DROP TABLE IF EXISTS fact_sales CASCADE;
DROP TABLE IF EXISTS dim_product CASCADE;
DROP TABLE IF EXISTS dim_customer CASCADE;
DROP TABLE IF EXISTS dim_seller CASCADE;
DROP TABLE IF EXISTS dim_store CASCADE;
DROP TABLE IF EXISTS dim_supplier CASCADE;

CREATE TABLE dim_supplier (
    supplier_name    VARCHAR(200) NOT NULL,
    supplier_city    VARCHAR(100) NOT NULL,
    supplier_contact VARCHAR(200),
    supplier_email   VARCHAR(200),
    supplier_phone   VARCHAR(50),
    supplier_address VARCHAR(300),
    supplier_country VARCHAR(100),
    PRIMARY KEY (supplier_name, supplier_city)
);

CREATE TABLE dim_customer (
    customer_id   INTEGER PRIMARY KEY,
    first_name    VARCHAR(100),
    last_name     VARCHAR(100),
    age           INTEGER,
    email         VARCHAR(200),
    country       VARCHAR(100),
    postal_code   VARCHAR(50),
    pet_type      VARCHAR(50),
    pet_name      VARCHAR(100),
    pet_breed     VARCHAR(100)
);

CREATE TABLE dim_seller (
    seller_id    INTEGER PRIMARY KEY,
    first_name   VARCHAR(100),
    last_name    VARCHAR(100),
    email        VARCHAR(200),
    country      VARCHAR(100),
    postal_code  VARCHAR(50)
);

CREATE TABLE dim_product (
    product_id    INTEGER PRIMARY KEY,
    name          VARCHAR(200),
    category      VARCHAR(100),
    price         NUMERIC(10,2),
    quantity      INTEGER,
    weight        NUMERIC(10,2),
    color         VARCHAR(50),
    size          VARCHAR(50),
    brand         VARCHAR(100),
    material      VARCHAR(100),
    description   TEXT,
    rating        NUMERIC(3,1),
    reviews       INTEGER,
    release_date  DATE,
    expiry_date   DATE,
    supplier_name VARCHAR(200),
    supplier_city VARCHAR(100)
);

CREATE TABLE dim_store (
    store_name    VARCHAR(200) NOT NULL,
    store_city    VARCHAR(100) NOT NULL,
    store_location VARCHAR(200),
    store_state   VARCHAR(100),
    store_country VARCHAR(100),
    store_phone   VARCHAR(50),
    store_email   VARCHAR(200),
    PRIMARY KEY (store_name, store_city)
);

CREATE TABLE fact_sales (
    sale_id          INTEGER PRIMARY KEY,
    customer_id      INTEGER,
    seller_id        INTEGER,
    product_id       INTEGER,
    store_name       VARCHAR(200),
    store_city       VARCHAR(100),
    sale_date        DATE,
    sale_quantity    INTEGER,
    sale_total_price NUMERIC(12,2)
);
