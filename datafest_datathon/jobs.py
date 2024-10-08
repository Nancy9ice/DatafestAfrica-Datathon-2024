from dagster import (
    define_asset_job,
    AssetSelection,
    repository  # Import the repository decorator
)
from datafest_datathon.assets.snowflake_dbt import my_dbt_assets

# Define job to run Airbyte sync and dbt build
airbyte_dbt_sync_job = define_asset_job(
    "airbyte_mysql_to_snowflake_to_dbt",
    AssetSelection.assets(my_dbt_assets)
    .upstream()
    .required_multi_asset_neighbors(),  # all Airbyte assets linked to the same connection
)

# Define the repository
@repository
def my_repository():
    return [airbyte_dbt_sync_job]  # Return job here
