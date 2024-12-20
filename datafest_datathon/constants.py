from dotenv import load_dotenv
import os
from dagster_airbyte import AirbyteCloudResource
from dagster import EnvVar

# Load environment variables from .env file
load_dotenv()

# Path to the dbt project
DBT_PROJECT_DIR = 'dbt_data_baddies_datafest'

# Define the Airbyte resource
airbyte_instance = AirbyteCloudResource(
    client_id=EnvVar("AIRBYTE_CLIENT_ID"),
    client_secret=EnvVar("AIRBYTE_CLIENT_SECRET"),
)
