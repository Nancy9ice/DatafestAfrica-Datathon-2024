from dagster import (
    ScheduleDefinition,
    Definitions,
    repository,  # Import the repository decorator
)
from dagster_airbyte import build_airbyte_assets
from datafest_datathon.assets.snowflake_dbt import my_dbt_assets
from datafest_datathon.jobs import airbyte_dbt_sync_job
from constants import airbyte_instance
from datafest_datathon.assets.snowflake_dbt import dbt_resource

# Build Airbyte assets (this will create raw tables)
airbyte_assets = build_airbyte_assets(
    connection_id="2d2df882-996e-4224-8a89-642c2461c6a3",
    destination_tables=[
        "raw_attendance_day", "raw_calendar_events", "raw_courses",
        "raw_discipline_referrals", "raw_eligibility", "raw_eligibility_activities",
        "raw_marking_periods", "raw_parent", "raw_portal_polls",
        "raw_school_gradelevels", "raw_schools", "raw_student_jamb_scores",
        "raw_student_waec_grades", "raw_students", "raw_students_courses", "raw_teachers"
    ],
    asset_key_prefix=["data_baddies_datafest"],
)

# Define the Dagster repository
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
