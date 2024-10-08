from dagster_dbt import DbtCliResource, dbt_assets
from dagster import AssetExecutionContext
from constants import DBT_PROJECT_DIR

# Define the dbt CLI resource
dbt_resource = DbtCliResource(
    project_dir=DBT_PROJECT_DIR,
    profiles_dir=DBT_PROJECT_DIR
)

# Define the dbt project assets
@dbt_assets(
    manifest=f"{DBT_PROJECT_DIR}/target/manifest.json"
)
def my_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()