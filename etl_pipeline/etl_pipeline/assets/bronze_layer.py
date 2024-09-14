from dagster import asset, Output
import pandas as pd

tables = [
    "olist_order_items_dataset",
    "olist_orders_dataset",
    "olist_products_dataset",
    "product_category_name_translation",
    "olist_order_reviews_dataset",
    "olist_order_payments_dataset"
]

def create_asset(table):
    @asset(
        name=f"{table}_asset",
        io_manager_key="minio_io_manager",
        required_resource_keys={"mysql_io_manager"},
        key_prefix=["bronze", "ecom"],
        compute_kind="SQL",
        group_name="bronze"
    )
    def _asset(context) -> Output[pd.DataFrame]:
        sql_stm = f"SELECT * FROM {table}"
        pd_data = context.resources.mysql_io_manager.extract_data(sql_stm)
        context.log.info(f"Table extracted: {pd_data.shape}")
        return Output(
            pd_data,
            metadata={
                "table": table,
                "records": len(pd_data)
            }
        )
    return _asset

all_assets = [create_asset(table) for table in tables]
