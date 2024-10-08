from dagster import AssetExecutionContext, asset
from dagster_airbyte import build_airbyte_assets
from dagster_dbt import DbtCliResource, dbt_assets
from datafest_datathon.constants import DBT_PROJECT_DIR
import os

# Paths to dbt project
DBT_PROJECT_DIR = 'datafest_datathon/dbt_data_baddies_datafest'

# Build Airbyte assets (this will create raw tables)
@asset(group_name="airbyte") 
def airbyte_assets():
    return build_airbyte_assets(
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

# Define the dbt CLI resource
dbt_resource = DbtCliResource(
    project_dir=(os.fspath(DBT_PROJECT_DIR)),
    profiles_dir=(os.fspath(DBT_PROJECT_DIR))
)

# Define the dbt project assets
@dbt_assets(manifest=f"{DBT_PROJECT_DIR}/target/manifest.json")
def my_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()