import pandas as pd
from dagster import Output, AssetIn, AssetOut, multi_asset

@multi_asset(
    ins={
        "gold_transactions_with_order_items": AssetIn(
            key_prefix=["gold", "ecom"],
        )
    },
    outs={
        "warehouse_transactions_with_order_items": AssetOut(
            io_manager_key="psql_io_manager",
            key_prefix=["warehouse","ecom"],
            metadata={
                "columns": ["order_id", "list_of_products"]
            },
        ),
    },
    compute_kind="PostgresSQL",
    group_name="warehouse"
)
def warehouse_transactions_with_order_items(gold_transactions_with_order_items: pd.DataFrame) -> Output[pd.DataFrame]:
    return Output(
            gold_transactions_with_order_items,
            metadata={
                "schema": "ecom",
                "table": "transactions_with_order_items",
                "record_count": len(gold_transactions_with_order_items),
            }
    )

@multi_asset(
    ins={
        "gold_monthly_product_sales_summary": AssetIn(
            key_prefix=["gold", "ecom"],
        )
    },
    outs={
        "warehouse_monthly_product_sales_summary": AssetOut(
            io_manager_key="psql_io_manager",
            key_prefix=["warehouse", "ecom"],
            metadata={
                "columns": ["sales_month", "product_category", "total_sales_value", "total_products_sold"]
            },
        ),
    },
    compute_kind="PostgresSQL",
    group_name="warehouse"
)
def warehouse_monthly_product_sales_summary(gold_monthly_product_sales_summary: pd.DataFrame) -> Output[pd.DataFrame]:
    return Output(
            gold_monthly_product_sales_summary,
            metadata={
                "schema": "ecom",
                "table": "monthly_product_sales_summary",
                "records_count": len(gold_monthly_product_sales_summary),
                "columns": list(gold_monthly_product_sales_summary.columns)
            }
    )

@multi_asset(
    ins={
        "gold_customer_review_summary": AssetIn(
            key_prefix=["gold", "ecom"],
        )
    },
    outs={
        "warehouse_customer_review_summary": AssetOut(
            io_manager_key="psql_io_manager",
            key_prefix=["warehouse", "ecom"],
            metadata={
                "columns": ["customer_id", "total_orders","total_reviews", "average_review_score","total_spent"]
            },
        ),
    },
    compute_kind="PostgresSQL",
    group_name="warehouse"
)
def warehouse_customer_review_summary(gold_customer_review_summary: pd.DataFrame) -> Output[pd.DataFrame]:
    return Output(
            gold_customer_review_summary,
            metadata={
                "schema": "ecom",
                "table": "warehouse_customer_review_summary",
                "records_count": len(gold_customer_review_summary),
                "columns": list(gold_customer_review_summary.columns)
            }
    )

@multi_asset(
    ins={
        "gold_customer_churn": AssetIn(
            key_prefix=["gold","ecom"],
        )
    },
    outs={
        "warehouse_customer_churn": AssetOut(
            io_manager_key="psql_io_manager",
            key_prefix=["warehouse", "ecom"],
            metadata={
                "columns": ["customer_id", "total_orders", "total_spent", "average_review_score", "last_purchase_timestamp", "days_since_last_purchase", "churn"]
            },
        ),
    },
    compute_kind="PostgresSQL",
    group_name="warehouse"
)
def warehouse_customer_churn(gold_customer_churn: pd.DataFrame) -> Output[pd.DataFrame]:
    return Output(
            gold_customer_churn,
            metadata={
                "schema": "ecom",
                "table": "warehouse_customer_churn",
                "records_count": len(gold_customer_churn),
                "columns": list(gold_customer_churn.columns)
            }
    )
