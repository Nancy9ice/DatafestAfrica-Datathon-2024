import logging
import pandas as pd
from dagster import op, asset
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

@asset
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


@asset
def clean_waec_data(load_waec_data: pd.DataFrame) -> pd.DataFrame:
    logging.info("Cleaning WAEC data...")
    cleaned_data = load_and_clean_waec_data(load_waec_data)  # Ensure this function is defined
    return cleaned_data


@asset
def filter_alumni_data(clean_waec_data: pd.DataFrame) -> pd.DataFrame:
    logging.info("Filtering alumni data...")
    alumni_data = filter_alumni(clean_waec_data)  # Ensure this function is defined
    return alumni_data


@asset
def prepare_training_data(filter_alumni_data: pd.DataFrame):
    logging.info("Preparing data for training...")
    # Prepare training data (ensure `prepare_waec_data` is defined elsewhere)
    X_train, X_test, y_train, y_test = prepare_waec_data(filter_alumni_data)  
    return X_train, X_test, y_train, y_test


@asset
def train_waec_model(prepare_training_data):
    X_train, X_test, y_train, y_test = prepare_training_data  # Unpack the tuple from `prepare_training_data`
    logging.info("Training the WAEC model with Grid Search...")
    best_model, _, _ = grid_search_rf(X_train, y_train)  # Ensure grid_search_rf is defined
    save_model(best_model)  # Ensure save_model is defined
    return best_model, X_test, y_test


@asset
def evaluate_waec_model(train_waec_model):
    best_model, X_test, y_test = train_waec_model  # Unpack the tuple from `train_waec_model`
    logging.info("Evaluating the WAEC model...")
    evaluation_results = evaluate_model(best_model, X_test, y_test)  # Ensure evaluate_model is defined
    logging.info("Evaluation Results: %s", evaluation_results)
    return best_model


@asset
def predict_for_current_students(evaluate_waec_model, clean_waec_data: pd.DataFrame) -> pd.DataFrame:
    logging.info("Making predictions for current students...")
    current_students = filter_current_students(clean_waec_data)  # Ensure filter_current_students is defined
    predicted_current_students = predict_current_students(evaluate_waec_model, current_students)  # Ensure predict_current_students is defined
    return predicted_current_students


@asset
def write_waec_predictions_to_snowflake(snowflake: SnowflakeResource, predict_for_current_students: pd.DataFrame):
    with snowflake.get_connection() as conn:
        table_name = "Waec_Predicted_Performance"
        success, number_chunks, rows_inserted, output = write_pandas(
            conn,
            predict_for_current_students[['student_id', 'predicted_waec_status']],  # Ensure DataFrame has these columns
            table_name=table_name,
            auto_create_table=True,
            overwrite=True,
            quote_identifiers=False
        )
        
    return MaterializeResult(
        metadata={"rows_inserted": rows_inserted},  # Return metadata about the inserted rows
    )


@asset
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

@asset
def train_and_predict_jamb_data(jamb_data):
    logging.info("Training and predicting JAMB data...")
    predicted_results = train_and_predict_jamb(jamb_data)
    return predicted_results


@asset
def write_jamb_predictions_to_snowflake(snowflake: SnowflakeResource, train_and_predict_jamb_data: pd.DataFrame):
    with snowflake.get_connection() as conn:
        table_name = "Jamb_Predicted_Performance"
        success, number_chunks, rows_inserted, output = write_pandas(
            conn,
            train_and_predict_jamb_data[['student_id', 'predicted_status']],  # Ensure correct columns are present
            table_name=table_name,
            auto_create_table=True,
            overwrite=True,
            quote_identifiers=False
        )
        
    return MaterializeResult(
        metadata={"rows_inserted": rows_inserted},
    )