# DatafestAfrica Datathon 2024

This is a project built by the Data Baddies team (Olaminike Olaiya and Nancy Amandi) for the DatafestAfrica Datathon 2024. This project aimed to solve the problem of a secondary school as regards predicting the scores of their future exams. The scope of the project and the tools involved includes:

- Data Gathering: Rosariosis Software and MySQL database

- Data Ingestion: Airbyte

- Data Storage: Snowflake

- Data Preparation and Modeling: dbt

- Machine Learning: Python

- Reporting and Visualization: Metabase

- Pipeline Orchestration: Dagster

This Github project contains codes on dbt, Dagster and Python (machine learning). 

The dbt and machine learning projects are wrapped in the dagster orchestration code.

## Project Documentation

You can access the detailed [project documentation here](https://nancy9ice.github.io/DatafestAfrica-Datathon-2024)

## How to Build this Project

Clone the github project

```bash
git clone https://github.com/Nancy9ice/DatafestAfrica-Datathon-2024.git
```

You can choose to build this project in Dagster Cloud or Dagster CLI. 

## Building the project on Dagster Cloud.

To build the project on dagster cloud, do the following:

- Push the cloned repo to your github

- Create the required environment variables in your Dagster Cloud UI

- Add the repo as your Dagster cloud location

- Launch the dagster run

## Building the project on Dagster CLI

To build the project in the Dagster CLI, do the following after cloning the github repo:

- Create a virtual environment

```bash
python -m venv venv_name
```

- Activate the virtual environment

- Install the project requirements

```bash
pip install -r requirements.txt
```

- Change Directory

```bash
cd datafest_datathon
```

- Build the project module

```bash
pip install -e ".[dev]"
```

- Change Directory to root folder

```bash
cd ..
```

- Don't forget to add the python executable path of your environment to path
```bash
export PYTHONPATH="<venv full path>/lib/$PYTHONVERSION/site-packages:$PYTHONPATH"
```

- Ensure you have the following environment variables defined in a .env file in the root folder
```bash
SNOWFLAKE_ACCOUNT
SNOWFLAKE_PASSWORD
SNOWFLAKE_USER
SNOWFLAKE_DATABASE
SNOWFLAKE_SCHEMA
SNOWFLAKE_WAREHOUSE
AIRBYTE_CLIENT_ID
AIRBYTE_CLIENT_SECRET
```

- Launch your Dagster environment

```bash
dagster dev -w workspace.yaml
```