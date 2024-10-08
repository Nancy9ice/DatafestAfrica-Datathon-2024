from dagster import (
    ScheduleDefinition,
    Definitions,
    repository,  # Import the repository decorator
)
from datafest_datathon.assets.airbyte_instance import airbyte_assets
from datafest_datathon.assets.snowflake_dbt import my_dbt_assets
from datafest_datathon.jobs import airbyte_dbt_sync_job
from constants import airbyte_instance
from datafest_datathon.assets.snowflake_dbt import dbt_resource

# Define the Dagster definitions
@repository  # Decorate the function to indicate it's a repository
def my_repository():
    return Definitions(
        assets=airbyte_assets + [my_dbt_assets],  # Combine Airbyte assets and dbt assets
        schedules=[
            ScheduleDefinition(
                job=airbyte_dbt_sync_job,  # Schedule for Airbyte job
                cron_schedule="@daily",  # Runs the Airbyte sync daily at 12am
            ),
        ],
        jobs=[airbyte_dbt_sync_job],
        resources={"airbyte": airbyte_instance, "dbt": dbt_resource},
    )
