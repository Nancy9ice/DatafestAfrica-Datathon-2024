import os
from dotenv import load_dotenv
from sqlalchemy import create_engine

# Load environment variables from .env file
load_dotenv()

# Retrieve credentials and connection details from environment variables
user = os.getenv('SNOWFLAKE_USER')
password = os.getenv('SNOWFLAKE_PASSWORD')
account = os.getenv('SNOWFLAKE_ACCOUNT')
database = os.getenv('SNOWFLAKE_DATABASE')
schema = os.getenv('SNOWFLAKE_SCHEMA')
warehouse = os.getenv('SNOWFLAKE_WAREHOUSE')

# Create the connection string
connection_string = connection_string = f'snowflake://{user}:{password}@{account}/{database}/{schema}?warehouse={warehouse}'
# print(connection_string)
# connection_string = (
#    'snowflake://DATABADDIES2024:%40Databaddies2_@ya78182.eu-west-2.aws/'
#    'DATAFESTAFRICA/PUBLIC?warehouse=COMPUTE_WH'
# )
# Establish a connection using SQLAlchemy
def get_engine():
    engine = create_engine(connection_string)
    return engine

# Example usage
if __name__ == "__main__":
    engine = get_engine()