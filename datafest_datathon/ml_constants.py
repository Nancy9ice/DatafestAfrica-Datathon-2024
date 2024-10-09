from dagster_snowflake import SnowflakeResource
from dagster import EnvVar

snowflake = SnowflakeResource(
    account=EnvVar("SNOWFLAKE_ACCOUNT"),  # required
    user=EnvVar("SNOWFLAKE_USER"),  # required
    password=EnvVar("SNOWFLAKE_PASSWORD"),  # password or private key required
    warehouse="COMPUTE_WH",
    database="DATAFESTAFRICA",
    schema="CORE",
    role="ACCOUNTADMIN",
)