# CHECK MY WORK [HERE](./doc/Doc.md)


## DNA Engineering Data Assignment

Build an ETL pipeline using Apache Airflow.

`Apache Airflow` is an open-source platform for developing, scheduling, and monitoring batch-oriented workflows.
Airflow’s extensible Python framework enables you to build workflows connecting with virtually any technology. A web interface helps manage the state of your workflows. Airflow is deployable in many ways, varying from a single process on your laptop to a distributed setup to support even the biggest workflows.

Airflow Docs: https://airflow.apache.org/docs/

## Table of content

- [Prerequisites](#prerequisites)
- [Project Setup](#project-setup)
- [Airflow Setup](#airflow-setup)
- [Before we begin](#before-we-begin)
- [Description](#description)
- [Assignment](#assignment)


## Prerequisites
- Python 3.8 or higher

- Create and activate a virtual environment

### Virtual Environment

#### Creation

```bash
python -m venv venv
```

#### Activation

On Linux

```bash
source venv/bin/activate
```

On Windows
```bash
venv\Scripts\activate
```

#### Deactivation

```bash
deactivate
```

## Project Setup

Export AIRFLOW_HOME before installing dependencies

```bash
export AIRFLOW_HOME="your_desired_airflow_location"
```

Install dependencies

```bash
pip install -r requirements.txt
```

## Airflow Setup

Show Info about Airflow Env
```bash
airflow info
```

Display Airflow cheat sheet
```bash
airflow cheat-sheet
```

**Set load_examples to False in airflow.cfg if you don't want to load tutorial dags and examples, before you execute the next command**


Migrate airflow database

```bash
airflow db migrate
```

Create an Admin user

```bash
airflow users create \
    --username admin \
    --firstname first_name_example \
    --lastname last_name_example \
    --role Admin \
    --email your_email@example.com
```

Start all components

```bash
airflow standalone
```

- Access Airflow UI at: http://localhost:8080, and enter your login information


## Before we begin
- In this assignment, you will be asked to write, refactor, and test code.
- Make sure you respect clean code guidelines.
- Read the assignment carefully.


## Description
- You are invited to build an ETL pipline using Ariflow in this assignment.
- Data Location: `source/jobs.csv`

**Data description**

Your target data is located in the context column.
It's a json data that needs to be cleaned, transformed and saved to an sqlite database


**Provided by default:**
- Pipline structure with necessary tasks under `dags/etl.py`.
- SQL Query for tables creation.
- The blueprint task functions that needs to be completed.

## Assignment

### 1. Code Refactoring

The code of the etl is grouped into one Python (`dags/etl.py`) script with makes it long, unoptimized, hard to read, hard to maintain, and hard to upgrade.

Your job is to:

- Rewrite the code while respecting clean code guidelines.
- Refactor the script and dissociate the tasks, and domains.

### 2. ETL Tasks
Fill in the necessary code for tasks: Extract, Transform, Load.


#### Extract job

Read the Dataframe from `source/jobs.csv`, extract the context column data, and save each item to `staging/extracted` as a text file.

#### Transform job

Read the extracted text files from `staging/extracted` as json, clean the job description, transform the schema, and save each item to `staging/transformed` as json file.

The desired schema from the transform job:

```json
{
    "job": {
        "title": "job_title",
        "industry": "job_industry",
        "description": "job_description",
        "employment_type": "job_employment_type",
        "date_posted": "job_date_posted",
    },
    "company": {
        "name": "company_name",
        "link": "company_linkedin_link",
    },
    "education": {
        "required_credential": "job_required_credential",
    },
    "experience": {
        "months_of_experience": "job_months_of_experience",
        "seniority_level": "seniority_level",
    },
    "salary": {
        "currency": "salary_currency",
        "min_value": "salary_min_value",
        "max_value": "salary_max_value",
        "unit": "salary_unit",
    },
    "location": {
        "country": "country",
        "locality": "locality",
        "region": "region",
        "postal_code": "postal_code",
        "street_address": "street_address",
        "latitude": "latitude",
        "longitude": "longitude",
    },
}
```

#### Load job

Read the transformed data from `staging/transformed`, and save it to the sqlite database.


### 3. Unit Testing

As mentioned previously, your code should be unit tested.

Hints: Use pytest for your unit tests as well as mocks for external services.
