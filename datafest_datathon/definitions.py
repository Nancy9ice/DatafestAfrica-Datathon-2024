# repository.py
from dagster import Definitions, repository
from datafest_datathon.assets.airbyte_instance import airbyte_assets
from datafest_datathon.assets.snowflake_dbt import my_dbt_assets, dbt_resource
from datafest_datathon.ml_constants import snowflake
from datafest_datathon.constants import airbyte_instance
from datafest_datathon.jobs import airbyte_dbt_sync_job
from datafest_datathon.ml_jobs import waec_pipeline, jamb_pipeline
from datafest_datathon.assets.ml_assets import (jamb_data, train_and_predict_jamb_data, 
                                                write_jamb_predictions_to_snowflake,
                                                load_waec_data, clean_waec_data,
                                                filter_alumni_data, prepare_training_data,
                                                evaluate_waec_model, train_waec_model,
                                                predict_for_current_students, write_waec_predictions_to_snowflake)
from dagster import (
    ScheduleDefinition,
    Definitions,
)

# Define the Dagster definitions
defs = Definitions(
    assets=airbyte_assets + [my_dbt_assets] + [
        jamb_data,
        train_and_predict_jamb_data,
        write_jamb_predictions_to_snowflake,
        load_waec_data,
        clean_waec_data,
        filter_alumni_data,
        prepare_training_data,
        train_waec_model,
        evaluate_waec_model,
        predict_for_current_students,
        write_waec_predictions_to_snowflake
    ],  # Combine Airbyte assets, dbt assets, JAMB, and WAEC assets
    schedules=[
        ScheduleDefinition(
            job=airbyte_dbt_sync_job,  # Schedule for Airbyte job
            cron_schedule="@daily",  # Runs the Airbyte sync daily at 12am
        ),
        ScheduleDefinition(
            job=waec_pipeline,  # Schedule for the WAEC pipeline
            cron_schedule="0 1 * * *",  # Runs WAEC pipeline daily at 1am
        ),
        ScheduleDefinition(
            job=jamb_pipeline,  # Schedule for the JAMB pipeline
            cron_schedule="0 1 * * *",  # Runs JAMB pipeline daily at 1am
        ),
    ],
    jobs=[airbyte_dbt_sync_job, waec_pipeline, jamb_pipeline],  # Include the defined jobs
    resources={"airbyte": airbyte_instance, "dbt": dbt_resource, "snowflake": snowflake},  # Define required resources
)
