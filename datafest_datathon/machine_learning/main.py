import logging
import pandas as pd
from db_conn import get_engine
from helper_functions import (load_and_clean_waec_data, filter_alumni, prepare_waec_data,
    grid_search_rf, save_model, evaluate_model, filter_current_students,
     predict_current_students, filter_current_students,
    load_jamb_data_from_db, train_and_predict_jamb
) 

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='waec_model_run.log',
    filemode='a'
)

logging.info("Starting WAEC model training and prediction pipeline...")
engine = get_engine()

# Define queries
alumni_query = 'SELECT * FROM "CORE".waec_performance_metrics WHERE student_class = \'Alumni\' LIMIT 1000'
current_students_query = 'SELECT * FROM "CORE".waec_performance_metrics WHERE student_status = \'Curent Student\' LIMIT 3000'

# Combine the queries with UNION ALL
waec_query = f"({alumni_query}) UNION ALL ({current_students_query})"




logging.basicConfig(level=logging.INFO)

# Load your dataset
waec_df = pd.read_sql_query(waec_query, engine)

# Process data
cleaned_data = load_and_clean_waec_data(waec_df)
alumni_data = filter_alumni(cleaned_data)
X_train, X_test, y_train, y_test = prepare_waec_data(alumni_data)

# Train and evaluate model
best_model, _, _ = grid_search_rf(X_train, y_train)
save_model(best_model)

# Evaluate on test set
evaluation_results = evaluate_model(best_model, X_test, y_test)
logging.info("Evaluation Results: %s", evaluation_results)

# Make predictions for current students
current_students = filter_current_students(cleaned_data)
predicted_current_students = predict_current_students(best_model, current_students)

# Print predictions for current students
print(predicted_current_students[['student_id', 'predicted_waec_status']].head())
  


    
# ----- JAMB Data Processing ----- #
jamb_query= 'SELECT * from "CORE".jamb_performance_metrics LIMIT 10000'
jamb = load_jamb_data_from_db (engine , jamb_query)

predicted_results = train_and_predict_jamb(jamb)

# Preview the predictions
print(predicted_results.head())

