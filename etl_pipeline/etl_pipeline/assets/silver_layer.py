from dagster import asset, Output, AssetIn
import pandas as pd

@asset(
    description="Information related to products",
    ins={
        "olist_products_dataset_asset": AssetIn(
            key_prefix=["bronze", "ecom"]
        ),
        "product_category_name_translation_asset": AssetIn(
            key_prefix=["bronze", "ecom"]
        )
    },
    io_manager_key="minio_io_manager",
    required_resource_keys={"minio_io_manager"},
    key_prefix=["silver", "ecom"],
    group_name="silver",
    compute_kind="Pandas"
)
def silver_olist_products(context, olist_products_dataset_asset: pd.DataFrame, product_category_name_translation_asset: pd.DataFrame) -> Output[pd.DataFrame]:
    merged_df = pd.merge(
        olist_products_dataset_asset,
        product_category_name_translation_asset,
        left_on="product_category_name",
        right_on="product_category_name"
    )[["product_id", "product_category_name_english"]]

    

    context.log.info(f"Data extracted with shape: {merged_df.shape}")
    return Output(
        merged_df,
        metadata={
            "table": "silver_olist_products",
            "rows": len(merged_df),
            "columns": list(merged_df.columns)
        }
    )

@asset(
    description="Information related to ordered items",
    ins={
        "olist_order_items_dataset_asset": AssetIn(
            key_prefix=["bronze", "ecom"]
        ),
        "olist_orders_dataset_asset": AssetIn(
            key_prefix=["bronze", "ecom"]
        ),
        "olist_order_payments_dataset_asset": AssetIn(
            key_prefix=["bronze", "ecom"]
        )
    },
    io_manager_key="minio_io_manager",
    required_resource_keys={"minio_io_manager"},
    key_prefix=["silver", "ecom"],
    group_name="silver",
    compute_kind="Pandas"
)
def silver_olist_orders(context, olist_orders_dataset_asset: pd.DataFrame, olist_order_items_dataset_asset: pd.DataFrame, olist_order_payments_dataset_asset: pd.DataFrame) -> Output[pd.DataFrame]:
    merged_df = pd.merge(
        olist_order_items_dataset_asset,
        olist_orders_dataset_asset,
        on="order_id"
    )
    merged_df = pd.merge(
        merged_df,
        olist_order_payments_dataset_asset,
        on="order_id"
    )[["order_id", "customer_id", "order_purchase_timestamp", "product_id", "payment_value", "order_status"]]

    context.log.info(f"Data extracted with shape: {merged_df.shape}")
    return Output(
        merged_df,
        metadata={
            "table": "silver_olist_orders",
            "rows": len(merged_df),
            "columns": list(merged_df.columns)
        }
    )

@asset(
    description="Information related to ordered items",
    ins={
        "olist_order_reviews_dataset_asset": AssetIn(
            key_prefix=["bronze", "ecom"]
        ),
    },
    io_manager_key="minio_io_manager",
    required_resource_keys={"minio_io_manager"},
    key_prefix=["silver", "ecom"],
    group_name="silver",
    compute_kind="Pandas"
)
def silver_olist_reviews(context, olist_order_reviews_dataset_asset: pd.DataFrame) -> Output[pd.DataFrame]:
    reviews = olist_order_reviews_dataset_asset.copy()
    reviews.drop(columns=['review_creation_date', 'review_answer_timestamp'], inplace=True)
    reviews = reviews.dropna(subset=['review_comment_message'])
    reviews.drop_duplicates(inplace=True)
    reviews.rename(columns={'review_score': 'score', 'review_comment_title': 'title', 'review_comment_message': 'comment'}, inplace=True)
    context.log.info(f"Data extracted with shape: {reviews.shape}")
    return Output(
        reviews,
        metadata={
            "table": "silver_olist_reviews",
            "rows": len(reviews),
            "columns": list(reviews.columns)
        }
    )

@asset(
    description="Dimension table for customers",
    ins={
        "olist_orders_dataset_asset": AssetIn(key_prefix=["bronze", "ecom"])
    },
    io_manager_key="minio_io_manager",
    required_resource_keys={"minio_io_manager"},
    key_prefix=["silver", "ecom"],
    group_name="silver",
    compute_kind="Pandas"
)
def silver_olist_customers(context, olist_orders_dataset_asset: pd.DataFrame) -> Output[pd.DataFrame]:
    customers_df = olist_orders_dataset_asset[['customer_id']].drop_duplicates().reset_index(drop=True)

    context.log.info(f"Data extracted with shape: {customers_df.shape}")
    return Output(
        customers_df,
        metadata={
            "table": "silver_olist_customers",
            "rows": len(customers_df),
            "columns": list(customers_df.columns)
        }
    )

@asset(
    description="Fact table for product sales",
    ins={
        "olist_order_items_dataset_asset": AssetIn(key_prefix=["bronze", "ecom"]),
        "olist_products_dataset_asset": AssetIn(key_prefix=["bronze", "ecom"]),
        "olist_orders_dataset_asset": AssetIn(key_prefix=["bronze", "ecom"]),
        "product_category_name_translation_asset": AssetIn(key_prefix=["bronze", "ecom"])
    },
    io_manager_key="minio_io_manager",
    required_resource_keys={"minio_io_manager"},
    key_prefix=["silver", "ecom"],
    group_name="silver",
    compute_kind="Pandas"
)
def silver_olist_products_sales(context, olist_order_items_dataset_asset: pd.DataFrame, olist_products_dataset_asset: pd.DataFrame, olist_orders_dataset_asset: pd.DataFrame, product_category_name_translation_asset: pd.DataFrame) -> Output[pd.DataFrame]:
    product_sales_df = pd.merge(
        olist_order_items_dataset_asset,
        olist_products_dataset_asset,
        on="product_id"
    )
    product_sales_df = pd.merge(
        product_sales_df,
        product_category_name_translation_asset,
        on="product_category_name"
    )
    product_sales_df = pd.merge(
        product_sales_df,
        olist_orders_dataset_asset,
        on="order_id"
    )[['order_id', 'product_id', 'product_category_name_english', 'price', 'freight_value', 'order_purchase_timestamp', 'order_status']]
    
    product_sales_df['total_sales_value'] = product_sales_df['price'] + product_sales_df['freight_value']

    product_sales_df['product_id'] = product_sales_df['product_id'].str.strip('"')
    
    context.log.info(f"Data extracted with shape: {product_sales_df.shape}")
    return Output(
        product_sales_df,
        metadata={
            "table": "silver_olist_products_sales",
            "rows": len(product_sales_df),
            "columns": list(product_sales_df.columns)
        }
    )

@asset(
    description="Recorded of the last purchase of customers",
    ins={
        "olist_orders_dataset_asset": AssetIn(
            key_prefix=["bronze", "ecom"],
        )
    },
    io_manager_key="minio_io_manager",
    required_resource_keys={"minio_io_manager"},
    key_prefix=["silver", "ecom"],
    group_name="silver",
    compute_kind="Pandas"
)
def silver_customer_last_purchase(context, olist_orders_dataset_asset: pd.DataFrame) -> Output[pd.DataFrame]:
    last_purchase_df = olist_orders_dataset_asset.copy()
    last_purchase_df['order_purchase_timestamp'] = pd.to_datetime(last_purchase_df['order_purchase_timestamp'])
    last_purchase_df = last_purchase_df.groupby('customer_id')['order_purchase_timestamp'].max().reset_index()
    last_purchase_df.rename(columns={'order_purchase_timestamp': 'last_purchase_timestamp'}, inplace=True)
    context.log.info(f"Data extracted with shape: {last_purchase_df.shape}")
    
    return Output(
        last_purchase_df,
        metadata={
            "table": "silver_customer_last_purchase",
            "rows": len(last_purchase_df),
            "columns": list(last_purchase_df.columns)
        }
    )
