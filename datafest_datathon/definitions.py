# repository.py
from dagster import Definitions, repository
from datafest_datathon.assets.airbyte_instance import airbyte_assets
from datafest_datathon.assets.snowflake_dbt import my_dbt_assets, dbt_resource
from datafest_datathon.constants import airbyte_instance
from datafest_datathon.jobs import airbyte_dbt_sync_job
from datafest_datathon.schedules import daily_airbyte_dbt_schedule  # Import your schedule

from dagster import (
    ScheduleDefinition,
    Definitions,
)

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
