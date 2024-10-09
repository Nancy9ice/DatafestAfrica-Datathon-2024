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

@job
def jamb_pipeline():
    get_jamb_data = jamb_data()
    jamb_predicted_students = train_and_predict_jamb_data(get_jamb_data)
    write_jamb_predictions_to_snowflake(jamb_predicted_students)

@job
def waec_pipeline():
    waec_df = load_waec_data()
    cleaned_data = clean_waec_data(waec_df)
    alumni_data = filter_alumni_data(cleaned_data)
    training_data = prepare_training_data(alumni_data)
    best_model = train_waec_model(training_data)
    evaluated_model = evaluate_waec_model(best_model)
    waec_predicted_students = predict_for_current_students(evaluated_model, cleaned_data)
    write_waec_predictions_to_snowflake(waec_predicted_students)
