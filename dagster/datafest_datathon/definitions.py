from dagster import (
    ScheduleDefinition,
    Definitions,
)
from airbyte_instance import airbyte_assets
from snowflake_dbt import my_dbt_assets
from job import airbyte_dbt_sync_job
from constants import airbyte_instance
from snowflake_dbt import dbt_resource


# Define the Dagster definitions
defs = Definitions(
    assets= airbyte_assets + [my_dbt_assets],  # Combine Airbyte assets and dbt assets
    schedules=[
        ScheduleDefinition(
            job=airbyte_dbt_sync_job,  # Schedule for Airbyte job
            cron_schedule="@daily",  # Runs the Airbyte sync daily at 12am
        ),
    ],
    jobs=[airbyte_dbt_sync_job],
    resources={"airbyte": airbyte_instance, "dbt": dbt_resource},
)