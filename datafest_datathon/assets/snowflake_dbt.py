from dagster import AssetExecutionContext, asset, OpExecutionContext
from dagster_airbyte import build_airbyte_assets
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

def run_dbt_deps(context: OpExecutionContext):
    """Run the dbt deps command to install dependencies."""
    context.log.info("Running dbt deps...")
    dbt_resource.cli(["deps"], context=context)

# Define the dbt project assets
@dbt_assets(manifest=f"{DBT_PROJECT_DIR}/target/manifest.json")
def my_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    # Run dbt deps before building assets
    run_dbt_deps(context)
    
    yield from dbt.cli(["build"], context=context).stream()
