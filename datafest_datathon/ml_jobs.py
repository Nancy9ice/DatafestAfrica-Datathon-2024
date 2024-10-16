from dagster import job, graph
from datafest_datathon.assets.ml_assets import (
    load_waec_data,
    clean_waec_data,
    filter_alumni_data,
    prepare_training_data,
    train_waec_model,
    evaluate_waec_model,
    predict_for_current_students,
    jamb_data,
    train_and_predict_jamb_data,
    write_jamb_predictions_to_snowflake,
    write_waec_predictions_to_snowflake
)

from dagster import define_asset_job, AssetSelection

# Define the job for JAMB data assets
jamb_pipeline = define_asset_job(
    "jamb_pipeline_job",
    AssetSelection.assets(jamb_data, train_and_predict_jamb_data, write_jamb_predictions_to_snowflake)
    .upstream()
)

# Define the WAEC pipeline asset job
waec_pipeline = define_asset_job(
    "waec_pipeline_job",
    AssetSelection.assets(
        load_waec_data,
        clean_waec_data,
        filter_alumni_data,
        prepare_training_data,
        train_waec_model,
        evaluate_waec_model,
        predict_for_current_students,
        write_waec_predictions_to_snowflake
    ).upstream()
)