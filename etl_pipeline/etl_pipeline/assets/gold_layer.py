import pandas as pd
from dagster import asset, Output, AssetIn
import pandasql as psql

@asset(
    description="Transaction with ordered items",
    ins={
        "silver_olist_products": AssetIn(key_prefix=["silver", "ecom"]),
        "silver_olist_orders": AssetIn(key_prefix=["silver", "ecom"]),
    },
    io_manager_key="minio_io_manager",
    required_resource_keys={"minio_io_manager"},
    key_prefix=["gold", "ecom"],
    group_name="gold",
    compute_kind="Pandas"
)
def gold_transactions_with_order_items(context, silver_olist_products: pd.DataFrame, silver_olist_orders: pd.DataFrame) -> Output[pd.DataFrame]:
    query = """
    WITH order_items AS (
        SELECT 
            soo.order_id,
            sop.product_category_name_english
        FROM
            silver_olist_orders soo
        JOIN
            silver_olist_products sop
        ON 
            soo.product_id = sop.product_id
    )
    SELECT
        order_id,
        GROUP_CONCAT(product_category_name_english) AS list_of_products
    FROM 
        order_items
    GROUP BY 
        order_id
    ORDER BY
        order_id
    """
    transaction_summary = psql.sqldf(query, locals())

    transaction_summary['list_of_products'] = transaction_summary['list_of_products'].str.strip()

    context.resources.minio_io_manager.handle_output(context, transaction_summary)

    context.log.info(f"Data extracted with shape: {transaction_summary.shape}")
    return Output(
        transaction_summary,
        metadata={
            "table": "gold_transactions_with_order_items",
            "rows": len(transaction_summary),
            "columns": list(transaction_summary.columns)
        }
    )

@asset(
    description="Monthly product sales summary",
    ins={
        "silver_olist_products_sales": AssetIn(key_prefix=["silver", "ecom"]),
        "silver_olist_products": AssetIn(key_prefix=["silver","ecom"]),
    },
    io_manager_key="minio_io_manager",
    required_resource_keys={"minio_io_manager"},
    key_prefix=["gold", "ecom"],
    group_name="gold",
    compute_kind="Pandas"
)
def gold_monthly_product_sales_summary(context, silver_olist_products_sales: pd.DataFrame, silver_olist_products: pd.DataFrame) -> Output[pd.DataFrame]:
    query = """
    WITH monthly_sales AS (
        SELECT
            strftime('%Y-%m', s.order_purchase_timestamp) AS sales_month,
            p.product_category_name_english AS product_category,
            SUM(s.total_sales_value) AS total_sales_value,
            SUM(s.price) AS total_products_sold
        FROM silver_olist_products_sales s
        JOIN silver_olist_products p ON s.product_id = p.product_id
        GROUP BY sales_month, product_category
    )
    SELECT 
        sales_month,
        product_category,
        total_sales_value,
        total_products_sold
    FROM monthly_sales
    ORDER BY sales_month, product_category
    """
    monthly_sales_summary = psql.sqldf(query, locals())

    monthly_sales_summary['product_category'] = monthly_sales_summary['product_category'].str.strip()

    context.resources.minio_io_manager.handle_output(context, monthly_sales_summary)

    context.log.info(f"Data extracted with shape: {monthly_sales_summary.shape}")
    return Output(
        monthly_sales_summary,
        metadata={
            "table": "gold_monthly_product_sales_summary",
            "rows": len(monthly_sales_summary),
            "columns": list(monthly_sales_summary.columns)
        }
    )

@asset(
    description="Customer review summary",
    ins={
        "silver_olist_reviews": AssetIn(key_prefix=["silver", "ecom"]),
        "silver_olist_orders": AssetIn(key_prefix=["silver", "ecom"])
    },
    io_manager_key="minio_io_manager",
    required_resource_keys={"minio_io_manager"},
    key_prefix=["gold", "ecom"],
    group_name="gold",
    compute_kind="Pandas"
)
def gold_customer_review_summary(context, silver_olist_reviews: pd.DataFrame, silver_olist_orders: pd.DataFrame) -> Output[pd.DataFrame]:
    query = """
    WITH review_summary AS (
        SELECT
            DISTINCT soo.customer_id,
            COUNT(DISTINCT soo.order_id) AS total_orders,
            COUNT(sor.review_id) AS total_reviews,
            AVG(sor.score) AS average_review_score,
            SUM(soo.payment_value) AS total_spent
        FROM silver_olist_orders soo
        JOIN silver_olist_reviews sor ON sor.order_id = soo.order_id
        GROUP BY soo.customer_id
    )
    SELECT 
        rs.customer_id,
        rs.total_orders,
        rs.total_reviews,
        rs.average_review_score,
        rs.total_spent
    FROM review_summary rs
    ORDER BY rs.customer_id
    """

    review_summary_df = psql.sqldf(query, locals())
    review_summary_df['average_review_score'] = review_summary_df['average_review_score'].fillna(0)
    review_summary_df['customer_id'] = review_summary_df['customer_id'].str.strip('"')

    context.resources.minio_io_manager.handle_output(context, review_summary_df)

    context.log.info(f"Data extracted with shape: {review_summary_df.shape}")
    return Output(
        review_summary_df,
        metadata={
            "table": "gold_customer_review_summary",
            "rows": len(review_summary_df),
            "columns": list(review_summary_df.columns)
        }
    )
@asset(
    description="Customer churn prediction",
    ins={
        "gold_customer_review_summary": AssetIn(key_prefix=["gold", "ecom"]),
        "silver_customer_last_purchase": AssetIn(key_prefix=["silver", "ecom"]),
    },
    io_manager_key="minio_io_manager",
    required_resource_keys={"minio_io_manager"},
    key_prefix=["gold", "ecom"],
    group_name="gold",
    compute_kind="Pandas"
)
def gold_customer_churn(context, gold_customer_review_summary: pd.DataFrame, silver_customer_last_purchase: pd.DataFrame) -> Output[pd.DataFrame]:
    silver_customer_last_purchase['last_purchase_timestamp'] = pd.to_datetime(silver_customer_last_purchase['last_purchase_timestamp'])
    query = """
    WITH last_order_date AS (
        SELECT 
            MAX(last_purchase_timestamp) AS last_date
        FROM silver_customer_last_purchase
    )
    SELECT
        c.customer_id,
        c.total_orders,
        c.total_spent,
        c.average_review_score,
        l.last_purchase_timestamp,
        CAST((julianday(o.last_date) - julianday(l.last_purchase_timestamp)) AS INTEGER) AS days_since_last_purchase,
        CASE
            WHEN (julianday(o.last_date) - julianday(l.last_purchase_timestamp)) > 180 THEN 1
            ELSE 0
        END AS churn
    FROM
        gold_customer_review_summary c
    JOIN
        silver_customer_last_purchase l ON c.customer_id = l.customer_id
    CROSS JOIN last_order_date o
    """
    churn_df = psql.sqldf(query, locals())

    context.log.info(f"Data extracted with shape: {churn_df.shape}")

    return Output(
        churn_df,
        metadata={
            "table": "gold_customer_churn",
            "rows": len(churn_df),
            "columns": list(churn_df.columns)
        }
    )