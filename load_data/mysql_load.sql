LOAD DATA LOCAL INFILE
'/tmp/brazilian-ecommerce/olist_products_dataset.csv' INTO TABLE
olist_products_dataset FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

LOAD DATA LOCAL INFILE
'/tmp/brazilian-ecommerce/olist_orders_dataset.csv' INTO TABLE
olist_orders_dataset FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

LOAD DATA LOCAL INFILE
'/tmp/brazilian-ecommerce/olist_order_items_dataset.csv' INTO TABLE
olist_order_items_dataset FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

LOAD DATA LOCAL INFILE
'/tmp/brazilian-ecommerce/olist_order_payments_dataset.csv' INTO TABLE
olist_order_payments_dataset FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

LOAD DATA LOCAL INFILE
'/tmp/brazilian-ecommerce/product_category_name_translation.csv' INTO TABLE
product_category_name_translation FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

LOAD DATA LOCAL INFILE '/tmp/brazilian-ecommerce/olist_order_reviews_dataset.csv' 
INTO TABLE olist_order_reviews_dataset 
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n' 
IGNORE 1 ROWS 
(review_id, order_id, review_score, review_comment_title, review_comment_message, review_creation_date, review_answer_timestamp)
SET 
    review_id = NULLIF(review_id, ''),
    order_id = NULLIF(order_id, ''),
    review_score = NULLIF(review_score, ''),
    review_comment_title = NULLIF(review_comment_title, ''),
    review_comment_message = NULLIF(review_comment_message, ''),
    review_creation_date = NULLIF(review_creation_date, ''),
    review_answer_timestamp = NULLIF(review_answer_timestamp, '');
