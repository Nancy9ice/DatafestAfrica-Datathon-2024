from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets
from datafest_datathon.constants import DBT_PROJECT_DIR
import os

# Paths to dbt project
DBT_PROJECT_DIR = 'datafest_datathon/dbt_data_baddies_datafest'

# Define the dbt CLI resource
dbt_resource = DbtCliResource(
    project_dir=(os.fspath(DBT_PROJECT_DIR)),
    profiles_dir=(os.fspath(DBT_PROJECT_DIR))
)

# Define the dbt project assets
@dbt_assets(manifest=f"{DBT_PROJECT_DIR}/target/manifest.json")
def my_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()