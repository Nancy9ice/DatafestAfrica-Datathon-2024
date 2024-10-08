# repository.py
from dagster import Definitions, repository
from datafest_datathon.assets.airbyte_instance import airbyte_assets
from datafest_datathon.assets.snowflake_dbt import my_dbt_assets, dbt_resource
from datafest_datathon.constants import airbyte_instance
from datafest_datathon.schedules import daily_airbyte_dbt_schedule  # Import your schedule

# Define the Dagster repository
defs = Definitions(
    assets=airbyte_assets + [my_dbt_assets],  # Combine Airbyte and dbt assets
    schedules=[daily_airbyte_dbt_schedule],  # Add schedule
    resources={"airbyte": airbyte_instance, "dbt": dbt_resource},
    )
