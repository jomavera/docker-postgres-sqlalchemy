DROP TABLE IF EXISTS device;
CREATE TABLE IF NOT EXISTS device
(
    id INT PRIMARY KEY,
    type INT,
    store_id INT
);
DROP TABLE IF EXISTS store;
CREATE TABLE IF NOT EXISTS store
(
    id INT PRIMARY KEY,
    name varchar(100),
    address varchar(255),
    city varchar(50),
    country varchar(50),
    created_at timestamp,
    typology varchar(50),
    customer_id int
);
DROP TABLE IF EXISTS transaction;
CREATE TABLE IF NOT EXISTS transaction
(
    id INT PRIMARY KEY,
    device_id INT,
    product_name varchar(100),
    product_sku bigint,
    category_name varchar(50),
    amount int,
    status varchar(50),
    card_number bigint,
    cvv int,
    created_at timestamp,
    happened_at timestamp
);

CREATE TABLE IF NOT EXISTS uf (
    value_date date PRIMARY KEY,
    value decimal
);