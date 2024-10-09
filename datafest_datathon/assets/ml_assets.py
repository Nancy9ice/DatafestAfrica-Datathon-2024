import logging
import pandas as pd
from dagster import op
from datafest_datathon.machine_learning.helper_functions import (
    load_and_clean_waec_data, filter_alumni, prepare_waec_data,
    grid_search_rf, save_model, evaluate_model, filter_current_students,
    predict_current_students, train_and_predict_jamb
)
from snowflake.connector.pandas_tools import write_pandas
from dagster_snowflake import SnowflakeResource
from dagster import MaterializeResult

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='waec_model_run.log',
    filemode='a'
)

@op
def load_waec_data(snowflake: SnowflakeResource):
    query = """
        SELECT * FROM "DATAFESTAFRICA"."CORE".waec_performance_metrics;
    """

    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query)
        waec_df = cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description]  # Get column names
        df = pd.DataFrame(waec_df, columns=column_names)  # Create DataFrame
        
        # Convert all column names to lowercase
        df.columns = [col.lower() for col in df.columns]  # Change column names to lowercase
        print(df)

    return df 

@op
def clean_waec_data(waec_df: pd.DataFrame) -> pd.DataFrame:
    logging.info("Cleaning WAEC data...")
    cleaned_data = load_and_clean_waec_data(waec_df)
    return cleaned_data

@op
def filter_alumni_data(cleaned_data):
    logging.info("Filtering alumni data...")
    alumni_data = filter_alumni(cleaned_data)
    return alumni_data

@op
def prepare_training_data(alumni_data):
    logging.info("Preparing data for training...")
    X_train, X_test, y_train, y_test = prepare_waec_data(alumni_data)
    return X_train, X_test, y_train, y_test

@op
def train_waec_model(inputs):
    X_train, X_test, y_train, y_test = inputs
    logging.info("Training the WAEC model with Grid Search...")
    best_model, _, _ = grid_search_rf(X_train, y_train)
    save_model(best_model)
    return best_model, X_test, y_test

@op
def evaluate_waec_model(inputs):
    best_model, X_test, y_test = inputs
    logging.info("Evaluating the WAEC model...")
    evaluation_results = evaluate_model(best_model, X_test, y_test)
    logging.info("Evaluation Results: %s", evaluation_results)
    return best_model

@op
def predict_for_current_students(best_model, cleaned_data):
    logging.info("Making predictions for current students...")
    current_students = filter_current_students(cleaned_data)
    predicted_current_students = predict_current_students(best_model, current_students)
    return predicted_current_students


@op
def write_waec_predictions_to_snowflake(snowflake: SnowflakeResource, predicted_students_df: pd.DataFrame):
    with snowflake.get_connection() as conn:
        table_name = "Waec_Predicted_Performance"
        success, number_chunks, rows_inserted, output = write_pandas(
            conn,
            predicted_students_df[['student_id', 'predicted_status']],
            table_name=table_name,
            auto_create_table=True,
            overwrite=True,
            quote_identifiers=False,
        )
    return MaterializeResult(
        metadata={"rows_inserted": rows_inserted},
    )


@op
def jamb_data(snowflake: SnowflakeResource):
    query = """
        SELECT * from "CORE".jamb_performance_metrics;
    """

    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query)
        jamb_df = cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description]  # Get column names
        df = pd.DataFrame(jamb_df, columns=column_names)  # Create DataFrame
        
        # Convert all column names to lowercase
        df.columns = [col.lower() for col in df.columns]  # Change column names to lowercase
        print(df)

    return df 

@op
def train_and_predict_jamb_data(jamb_data):
    logging.info("Training and predicting JAMB data...")
    predicted_results = train_and_predict_jamb(jamb_data)
    return predicted_results


@op
def write_jamb_predictions_to_snowflake(snowflake: SnowflakeResource, predicted_students_df: pd.DataFrame):
    with snowflake.get_connection() as conn:
        table_name = "Jamb_Predicted_Performance"
        success, number_chunks, rows_inserted, output = write_pandas(
            conn,
            predicted_students_df[['student_id', 'predicted_status']],
            table_name=table_name,
            auto_create_table=True,
            overwrite=True,
            quote_identifiers=False,
        )
    return MaterializeResult(
        metadata={"rows_inserted": rows_inserted},
    )