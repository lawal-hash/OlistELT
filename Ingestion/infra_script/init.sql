--CREATE DATABASE ecommerce;
CREATE SCHEMA IF NOT EXISTS olist;

CREATE TABLE IF NOT EXISTS  olist.geolocation (
    geolocation_zip_code_prefix int8 PRIMARY KEY,
    geolocation_lat float4 NULL,
    geolocation_lng float4 NULL,
    geolocation_city varchar(255) NULL,
    geolocation_state varchar(15) NULL
);

COPY olist.geolocation  (
geolocation_zip_code_prefix ,
geolocation_lat ,
geolocation_lng,
geolocation_city,
geolocation_state 
)
FROM '/data/olist_geolocation_dataset.csv' DELIMITER ',' CSV HEADER;

CREATE TABLE  IF NOT EXISTS olist.customers (
    customer_id uuid PRIMARY KEY,
    customer_unique_id uuid REFERENCES olist.geolocation(geolocation_zip_code_prefix) ON DELETE CASCADE,
    customer_zip_code_prefix int4 NULL,
    customer_city varchar(255) NULL,
    customer_state varchar(50) NULL

);

COPY olist.customers  (
customer_id ,
customer_unique_id ,
customer_zip_code_prefix ,
customer_city ,
customer_state

)
FROM '/data/olist_customers_dataset.csv' DELIMITER ',' CSV HEADER;

CREATE TABLE IF NOT EXISTS olist.sellers (
    seller_id uuid PRIMARY KEY,
    seller_zip_code_prefix int4 REFERENCES olist.geolocation(geolocation_zip_code_prefix) ON DELETE CASCADE,
    seller_city varchar(255) NULL,
    seller_state varchar(50) NULL
);

COPY olist.sellers  (
seller_id,
seller_zip_code_prefix ,
seller_city ,
seller_state  

)
FROM '/data/olist_sellers_dataset.csv' DELIMITER ',' CSV HEADER;

CREATE TABLE IF NOT EXISTS olist.orders (
    order_id uuid PRIMARY KEY,
    customer_id uuid REFERENCES olist.customers ON DELETE CASCADE,
    order_status varchar(50) NULL,
    order_purchase_timestamp timestamp NULL,
    order_approved_at timestamp NULL,
    order_delivered_carrier_date timestamp NULL,
    order_delivered_customer_date timestamp NULL,
    order_estimated_delivery_date timestamp NULL
    
);

COPY olist.orders  (
order_id ,
customer_id ,
order_status,
order_purchase_timestamp ,
order_approved_at ,
order_delivered_carrier_date ,
order_delivered_customer_date ,
order_estimated_delivery_date 

)
FROM '/data/olist_orders_dataset.csv' DELIMITER ',' CSV HEADER;

CREATE TABLE IF NOT EXISTS olist.products (
    product_id uuid PRIMARY KEY,
    product_category_name varchar(255) NULL,
    product_name_lenght int4 NULL,
    product_description_lenght int4 NULL,
    product_photos_qty int4 NULL,
    product_weight_g int4 NULL,
    product_length_cm int4 NULL,
    product_height_cm int4 NULL,
    product_width_cm int4 NULL
);


COPY olist.products  (
product_id ,
product_category_name ,
product_name_lenght ,
product_description_lenght ,
product_photos_qty ,
product_weight_g ,
product_length_cm ,
product_height_cm,
product_width_cm 

)
FROM '/data/olist_products_dataset.csv' DELIMITER ',' CSV HEADER;


CREATE TABLE IF NOT EXISTS olist.order_payments (
    order_id uuid REFERENCES olist.orders ON DELETE CASCADE,
    payment_sequential int4 NULL,
    payment_type varchar(50) NULL,
    payment_installments int4 NULL,
    payment_value float4 NULL
);

COPY olist.order_payments  (
order_id,
payment_sequential ,
payment_type,
payment_installments ,
payment_value 
)
FROM '/data/olist_order_payments_dataset.csv' DELIMITER ',' CSV HEADER;


CREATE TABLE IF NOT EXISTS olist.order_reviews (
    review_id varchar(255) PRIMARY KEY,
    order_id uuid REFERENCES olist.orders ON DELETE CASCADE,
    review_score int4 NULL,
    review_comment_title varchar(255) NULL,
    review_comment_message varchar(512) NULL,
    review_creation_date varchar(255) NULL,
    review_answer_timestamp timestamp NULL
);

COPY olist.order_reviews  (
review_id,
order_id,
review_score ,
review_comment_title,
review_comment_message,
review_creation_date ,
review_answer_timestamp 

)
FROM '/data/olist_order_reviews_dataset.csv' DELIMITER ',' CSV HEADER;






CREATE TABLE IF NOT EXISTS olist.order_items (
    order_id uuid REFERENCES olist.orders ON DELETE CASCADE,
    order_item_id int4 PRIMARY KEY,
    product_id uuid REFERENCES olist.products ON DELETE CASCADE,
    seller_id uuid REFERENCES olist.sellers ON DELETE CASCADE,
    shipping_limit_date timestamp NULL,
    price numeric(8, 3) NULL,
    freight_value numeric(8, 3) NULL, 
  
);

COPY olist.order_items  (
order_id ,
order_item_id ,
product_id ,
seller_id,
shipping_limit_date ,
price ,
freight_value 
)
FROM '/data/olist_order_items_dataset.csv' DELIMITER ',' CSV HEADER;