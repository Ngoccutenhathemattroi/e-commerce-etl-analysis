from dagster import Definitions
import os
from dotenv import load_dotenv
from .assets.bronze_layer import all_assets as bronze_assets
from .assets.silver_layer import (
    silver_olist_products,
    silver_olist_orders,
    silver_olist_reviews,
    silver_olist_customers,
    silver_olist_products_sales,
    silver_customer_last_purchase
)
from .assets.gold_layer import (
    gold_transactions_with_order_items,
    gold_monthly_product_sales_summary,
    gold_customer_review_summary,
    gold_customer_churn
)
from .assets.warehouse_layer import (
    warehouse_transactions_with_order_items,
    warehouse_monthly_product_sales_summary,
    warehouse_customer_review_summary,
    warehouse_customer_churn
)
from .resources.mysql_io_manager import MySQLIOManager
from .resources.minio_io_manager import MinIOIOManager
from .resources.psql_io_manager import PostgreSQLIOManager


load_dotenv()


MYSQL_CONFIG = {
    "host": os.getenv("MYSQL_HOST"),
    "port": os.getenv("MYSQL_PORT"),
    "database": os.getenv("MYSQL_DATABASE"),
    "user": os.getenv("MYSQL_USER"),
    "password": os.getenv("MYSQL_PASSWORD")
}


MINIO_CONFIG = {
    "endpoint_url": os.getenv("MINIO_ENDPOINT"),
    "bucket": os.getenv("DATALAKE_BUCKET"),
    "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID"),
    "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY")
}


PSQL_CONFIG = {
    "host": os.getenv("POSTGRES_HOST"),
    "port": os.getenv("POSTGRES_PORT"),
    "database": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD")
}


silver_assets = [
    silver_olist_products,
    silver_olist_orders,
    silver_olist_reviews,
    silver_olist_customers,
    silver_olist_products_sales,
    silver_customer_last_purchase
]


gold_assets = [
    gold_transactions_with_order_items,
    gold_monthly_product_sales_summary,
    gold_customer_review_summary,
    gold_customer_churn
]


warehouse_assets = [
    warehouse_transactions_with_order_items,
    warehouse_monthly_product_sales_summary,
    warehouse_customer_review_summary,
    warehouse_customer_churn
]


all_assets = bronze_assets + silver_assets + gold_assets + warehouse_assets


defs = Definitions(
    assets=all_assets,
    resources={
        "mysql_io_manager": MySQLIOManager(MYSQL_CONFIG),
        "minio_io_manager": MinIOIOManager(MINIO_CONFIG),
        "psql_io_manager": PostgreSQLIOManager(PSQL_CONFIG),
    }
)
