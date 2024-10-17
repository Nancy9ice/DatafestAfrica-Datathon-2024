import logging
import pandas as pd
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, f1_score, roc_auc_score, classification_report
from sklearn.preprocessing import LabelEncoder
from imblearn.over_sampling import SMOTE
import joblib
from sqlalchemy import create_engine

# ----- Database Connection ----- #
def get_engine():
    connection_string = 'snowflake://<username>:<password>@<account>/<database>/<schema>?warehouse=<warehouse>'
    return create_engine(connection_string)

def load_waec_data_from_db(engine, waec_query):
    try:
        waec_df = pd.read_sql_query(waec_query, engine)
        if waec_df.empty:
            logging.error("Query returned no results: %s", waec_query)
            return None
        return waec_df
    except Exception as e:
        logging.error("Error loading data from database: %s", e)
        return None

# ----- WAEC Processing Functions ----- #

# Load and Clean WAEC Data
def load_and_clean_waec_data(waec_df):
    if waec_df is None:
        logging.error("WAEC data could not be loaded or is empty.")
        return None

    logging.info("WAEC data loaded successfully. Number of rows: %d", len(waec_df))

    # Data cleaning and transformation
    waec_df['offense_count'] = waec_df.groupby('student_course_id')['student_offence'].transform('count')
    waec_df.drop_duplicates(subset='student_course_id', keep='first', inplace=True)
    waec_df = numerical_data(waec_df)
    waec_df = feature_engineering(waec_df)
    # waec_df.dropna(inplace=True)
    
    logging.info("WAEC data cleaned. Number of rows after cleaning: %d", len(waec_df))
    return waec_df

# Numerical Encoding
def numerical_data(waec_df):
    waec_df['gender'] = waec_df['gender'].map({'Male': 0, 'Female': 1})
    waec_df['bus_pickup'] = waec_df['bus_pickup'].map({'No': 0, 'Yes': 1})
    waec_df['bus_dropoff'] = waec_df['bus_dropoff'].map({'No': 0, 'Yes': 1})
    waec_df['student_evaluation'] = waec_df['student_evaluation'].map({'Fail': 0, 'Pass': 1})
    waec_df['student_activity_status'] = waec_df['student_activity_status'].map({'Not active': 0, 'Active': 1})
    waec_df['parent_education'] = waec_df['parent_education'].fillna('None').map({
        'Primary': 1, 'Secondary': 2, 'Higher': 3, 'None': 0
    })
    waec_df['health_condition'] = waec_df['health_condition'].map({
        'Poor Condition': 0, 'Very Good Condition': 3, 'Average Condition': 1, 'Good Condition': 2
    })
    return waec_df

# Feature Engineering
def feature_engineering(waec_df):
    waec_df['num_courses'] = waec_df.groupby('student_id')['student_course_id'].transform('count')
    waec_df['department_focus'] = waec_df.groupby(['student_id', 'course_department'])['student_course_id'].transform('count')
    waec_df['absenteeism'] = waec_df['average_expected_student_attendance'] - waec_df['average_student_minutes_attendance']
    waec_df['attendance_rate'] = waec_df['average_student_minutes_attendance'] / waec_df['average_expected_student_attendance']
    waec_df['attendance_score_interaction'] = waec_df['attendance_rate'] * waec_df['average_student_score']
    pass_fail_mapping = {
        'A1': 'Pass', 'B2': 'Pass', 'B3': 'Pass', 
        'C4': 'Pass', 'C5': 'Pass', 'C6': 'Pass', 
        'D7': 'Fail', 'E8': 'Fail', 'F9': 'Fail'
    }
    waec_df['waec_grade'] = waec_df['waec_grade'].map(pass_fail_mapping)
    waec_df['waec_status'] = waec_df['waec_grade'].map({'Fail': 0, 'Pass': 1})
    return waec_df

# Filter Alumni Data
def filter_alumni(waec_df):
    alumni_df = waec_df[waec_df['student_status'].str.contains('Alumni', case=False, na=False)]
    logging.info("Number of rows in alumni_df: %d", len(alumni_df))
    return alumni_df

# Prepare WAEC Data for Training
def prepare_waec_data(alumni_df):
    numfeature = alumni_df.select_dtypes(exclude=['object'])
    numfeature_clean = numfeature.dropna(subset=['waec_status'])
    
    logging.info("Number of rows after dropping missing 'waec_status': %d", len(numfeature_clean))
    
    X = numfeature_clean.drop(columns=['waec_status', 'student_id', 'department_focus', 'student_course_id', 'course_id', 'waec_exam_year'], errors='ignore')
    y = numfeature_clean['waec_status']
    
    if X.empty or y.empty:
        logging.error("Data preparation failed: 'X' or 'y' is empty. X shape: %s, y length: %d", X.shape, len(y))
        raise ValueError("No data available for training. Check the data filtering and cleaning steps.")
    
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    logging.info("Data split successfully. Training set size: %d, Test set size: %d", len(X_train), len(X_test))
    
    return X_train, X_test, y_train, y_test

# Train Model with Grid Search
def grid_search_rf(X_train, y_train):
    param_grid = {
        'n_estimators': [50, 100],
        'max_depth': [10, 20],
        'min_samples_split': [2, 5],
        'min_samples_leaf': [1, 2]
    }
    rf_model = RandomForestClassifier(random_state=42)
    grid_search = GridSearchCV(estimator=rf_model, param_grid=param_grid, cv=3, scoring='accuracy', n_jobs=-1, verbose=1)
    grid_search.fit(X_train, y_train)
    return grid_search.best_estimator_, grid_search.best_params_, grid_search.best_score_

# Save and Load Model
def save_model(model, filename='final_random_forest_model_waec.pkl'):
    joblib.dump(model, filename)

def load_model(filename='final_random_forest_model_waec.pkl'):
    return joblib.load(filename)

# Evaluate Model
def evaluate_model(model, X_test, y_test):
    y_pred = model.predict(X_test)
    accuracy = accuracy_score(y_test, y_pred)
    f1 = f1_score(y_test, y_pred)
    roc_auc = roc_auc_score(y_test, y_pred)
    return {'accuracy': accuracy, 'f1_score': f1, 'roc_auc': roc_auc}

# Filter Current Students
def filter_current_students(waec_df):
    current_df = waec_df[waec_df['student_status'].str.contains('Curent Student', case=False, na=False)]
    current_df.reset_index(drop=True, inplace=True)
    return current_df

# Predict for Current Students
def predict_current_students(model, current_df):
    numfeature = current_df.select_dtypes(exclude=['object'])
    X_new = numfeature.drop(columns=['waec_status', 'student_id', 'department_focus', 'student_course_id', 'course_id', 'waec_exam_year'], errors='ignore')
    current_df['predicted_waec_status'] = model.predict(X_new)
    # Map predicted values (0 -> 'Fail', 1 -> 'Pass')
    current_df['predicted_waec_status'] = current_df['predicted_waec_status'].map({0: 'Fail', 1: 'Pass'})
    return current_df





# ----- JAMB Processing Functions ----- #

def load_jamb_data_from_db(engine, jamb_query):
    try:
        jamb = pd.read_sql_query(jamb_query, engine)
        if jamb.empty:
            logging.error("Query returned no results: %s", jamb_query)
            return None
        return jamb
    except Exception as e:
        logging.error("Error loading data from database: %s", e)
        return None
    

def train_and_predict_jamb(jamb):
    # Define categorical columns
    categorical_columns = [
        'student_class', 'student_status', 'department', 'gender',
        'student_parent', 'parent_education', 'student_activity_status',
        'student_extracurricular_activity', 'student_school_performance',
        'student_offence', 'disciplinary_action_taken', 'disciplinary_teacher'
    ]

    # Encode categorical variables
    label_encoders = {}
    for col in categorical_columns:
        le = LabelEncoder()
        jamb[col] = le.fit_transform(jamb[col].astype(str))  # Convert to string to handle NaNs if necessary
        label_encoders[col] = le

    # Define the feature set X and target y
    X = jamb.drop(columns=[
        'student_id', 'jamb_score', 'jamb_exam_year', 'jamb_performance', 'student_class', 'student_status', 
        'department', 'student_parent', 'student_extracurricular_activity', 'disciplinary_action_taken', 
        'disciplinary_teacher', 'jamb_exam_year', 'student_name', 'health_condition'
    ])
    y = (jamb['jamb_performance'] == 'Pass').astype(int)  # Convert to binary: 1 if 'Pass', 0 otherwise

    # Split data into those who have and haven't written JAMB
    jamb_written = jamb.dropna(subset=['jamb_performance'])
    jamb_not_written = jamb[jamb['jamb_performance'].isna()]

    # Further split the data into train and test sets for students who have written JAMB
    X_written = X.loc[jamb_written.index]
    y_written = y.loc[jamb_written.index]

    # Split into train and test sets
    X_train, X_test, y_train, y_test = train_test_split(X_written, y_written, test_size=0.3, random_state=42, stratify=y_written)

    # Apply SMOTE to handle class imbalance in the training set
    smote = SMOTE(random_state=42)
    X_train_balanced, y_train_balanced = smote.fit_resample(X_train, y_train)

    # Train a Random Forest Classifier
    rf_model = RandomForestClassifier(random_state=42, class_weight={0: 1, 1: 10})
    rf_model.fit(X_train_balanced, y_train_balanced)

    # Evaluate the model on the test set
    y_pred_test = rf_model.predict(X_test)
    accuracy = accuracy_score(y_test, y_pred_test)
    report = classification_report(y_test, y_pred_test)
    
    print("Test Set Accuracy:", accuracy)
    print("Classification Report:\n", report)

    # Make predictions on students who haven't written JAMB
    X_not_written = X.loc[jamb_not_written.index]
    jamb_predictions = rf_model.predict(X_not_written)

    # Attach predictions to the original data for students who haven't written JAMB
    jamb_not_written['predicted_status'] = jamb_predictions
    # Map predicted values (0 -> 'Fail', 1 -> 'Pass')
    jamb_not_written['predicted_status'] = jamb_not_written['predicted_status'].map({0: 'Fail', 1: 'Pass'})

    # Return the predictions
    return jamb_not_written[['student_id', 'predicted_status']]