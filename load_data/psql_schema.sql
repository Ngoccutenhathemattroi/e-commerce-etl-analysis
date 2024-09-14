CREATE SCHEMA IF NOT EXISTS ecom;

CREATE TABLE IF NOT EXISTS ecom.transactions_with_order_items (
    order_id VARCHAR(255) NOT NULL,
    list_of_products TEXT NOT NULL
);


CREATE TABLE IF NOT EXISTS ecom.monthly_product_sales_summary (
    sales_month VARCHAR(255) NOT NULL,
    product_id VARCHAR(255) NOT NULL,
    total_sales_value FLOAT NOT NULL,
    total_products_sold FLOAT NOT NULL
);

CREATE TABLE IF NOT EXISTS ecom.customer_review_summary (
    customer_id VARCHAR(255) NOT NULL,
    total_reviews INT NOT NULL,
    average_review_score FLOAT NOT NULL
);

CREATE TABLE IF NOT EXISTS ecom.customer_churn (
    customer_id VARCHAR(255) NOT NULL,
    total_orders INT NOT NULL,
    total_spent FLOAT NOT NULL,
    average_review_score FLOAT NOT NULL,
    last_purchase_timestamp TIMESTAMP NOT NULL,
    days_since_last_purchase INT NOT NULL,
    churn INT NOT NULL
);


