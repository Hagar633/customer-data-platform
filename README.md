# Customer Data Platform

An end-to-end data engineering project for ingesting customer, product, order, and order-item data, validating data quality, capturing database changes, and building business-ready dimensional models.

The project combines batch ingestion with a streaming Change Data Capture (CDC) pipeline using PostgreSQL, Debezium, Apache Kafka, and Apache Flink.

## Architecture

```text
CSV / Parquet Data
        |
        v
Python Ingestion
        |
        v
PostgreSQL RAW Layer
        |
        +------> Audit and Ingestion Metadata
        |
        v
Debezium CDC Connector
        |
        v
Apache Kafka
        |
        v
Python CDC Validation
        |
        v
PostgreSQL STAGING Layer
        |
        v
Debezium / Kafka / Apache Flink
        |
        v
PostgreSQL BUSINESS Layer
        |
        v
Analytics Marts / Power BI
```

The project supports batch ingestion and is being developed toward continuous end-to-end streaming processing.

## Technology Stack

- **Python** — ingestion, validation, and data processing
- **PostgreSQL** — raw, staging, audit, business, and analytics schemas
- **Debezium** — PostgreSQL change data capture
- **Apache Kafka** — event streaming and topic-based communication
- **Apache Flink** — stream processing
- **SQL** — database schemas, transformations, and analytics marts
- **Apache Airflow** — workflow orchestration environment
- **dbt** — transformation project area
- **Power BI** — business intelligence and reporting

## Repository Structure

```text
customer-data-platform/
├── airflow/                 # Workflow orchestration
├── config/                  # Application and connector configuration
├── dashboard/               # Dashboard project area
├── data/                    # Local source datasets (not committed)
│   ├── raw/
│   └── clean/
├── dbt/                     # dbt project area
├── docs/                    # Project and analytics documentation
├── flink/                   # Java CDC job, Maven configuration, SQL tests
├── scripts/                 # Kafka publishing and utility scripts
├── sql/                     # Database schemas, tables, and transformations
├── src/
│   ├── ingestion/           # Source-data ingestion
│   ├── staging/             # Staging, validation, and business loading
│   ├── streaming/           # Streaming CDC validation service
│   └── validation/          # Data quality and relationship checks
├── tests/                   # Automated tests
├── .gitignore
└── README.md
```

## Data Architecture

### 1. RAW Layer

The RAW layer stores ingested source records in PostgreSQL.

Responsibilities include:
- Loading customer, product, order, and order-item datasets.
- Preserving source data for downstream processing.
- Tracking ingestion metadata and source records.
- Supporting change capture through PostgreSQL logical replication.

### 2. STAGING Layer

The STAGING layer prepares records for downstream processing.

Responsibilities include:
- Validating incoming records.
- Checking required fields and relationships between entities.
- Separating valid and invalid records.
- Recording validation outcomes and rejection reasons.
- Preparing validated data for business-layer loading.

### 3. BUSINESS Layer

The BUSINESS layer contains dimensional and fact tables designed for analytics.

The project includes historical dimension attributes using fields such as:
- `valid_from`
- `valid_to`
- `is_current`

These fields support tracking changes to dimension records over time where implemented.

### 4. AUDIT Layer

The audit schema tracks pipeline activity and data quality.

The project includes tables for:
- Pipeline runs
- Ingestion records
- Data-quality results
- Rejected records
- Change logs

### 5. Analytics Layer

SQL scripts define analytical marts and validation queries. The resulting business data can be used by reporting tools such as Power BI.

## Prerequisites

Install or configure the following:

- Git
- Docker Desktop with Docker Compose, or Docker Engine with Compose
- Python 3
- Java 21 and Maven for the Flink Java job
- PostgreSQL client tools (`psql`)

The required Docker services must share a Docker network so they can communicate through container service names.

## Local Database Configuration

The current development environment uses these PostgreSQL settings:

| Setting | Value |
|---|---|
| Database | `appdb` |
| Username | `postgres` |
| Host port | `5433` |
| Container port | `5432` |
| Host address | `localhost` |

These settings describe the local development environment. Do not use default development credentials in production.

Configure passwords and other sensitive values through environment variables or an untracked `.env` file. Never commit real credentials, API keys, or tokens.

## Running the Docker Infrastructure

The current Docker Compose file is maintained separately from this repository at:

```text
~/bigdata/streaming-stack/docker-compose.yml
```

Start the existing stack from your development machine:

```bash
docker compose \
  -f ~/bigdata/streaming-stack/docker-compose.yml \
  up -d
```

Check running containers:

```bash
docker ps
```

View the logs of a specific service:

```bash
docker logs --tail 100 postgres
docker logs --tail 100 kafka
docker logs --tail 100 debezium
docker logs --tail 100 streaming-stack-cdc-validator-1
```

The last container name applies to the current local setup; names may differ in other environments.

## Local Service Interfaces

| Service | URL |
|---|---|
| Kafka UI | http://localhost:8085 |
| Debezium Connect REST API | http://localhost:8083 |
| Debezium UI | http://localhost:8086 |
| Flink Dashboard | http://localhost:8081 |
| pgAdmin | http://localhost:5050 |

A URL being listed does not guarantee that the corresponding service is currently healthy; availability depends on the local container configuration.

## Database Setup

SQL scripts are numbered to indicate the intended setup sequence. Review the scripts before running them against an existing database, as some operations may depend on previously created objects or may not be safe to repeat.

Connect to PostgreSQL:

```bash
psql -h localhost -p 5433 -U postgres -d appdb
```

The initial schema and table scripts include:

```text
sql/01_create_schema.sql
sql/02_create_raw_tables.sql
sql/03_create_audit_tables.sql
sql/04_create_nonrepititve_raw.sql
sql/05_create_staging_tables.sql
sql/06_load_staging.sql
sql/07_create_business_tables.sql
sql/08_create_marts.sql
sql/09_validate_marts.sql
```

For example, execute a SQL file with:

```bash
psql -h localhost -p 5433 -U postgres -d appdb \
  -f sql/01_create_schema.sql
```

Continue with the remaining scripts in the appropriate dependency order for your database.

**Important:** Do not blindly rerun scripts that truncate tables, recreate objects, or load data. Check their contents first, especially when preserving existing data.

## Python Environment Setup

Create and activate a virtual environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
```

Install the dependencies required by the scripts you intend to execute.

A dependency manifest such as `requirements.txt` should be maintained to make installation reproducible. Until one is available, inspect the imports in the relevant source files and install the corresponding packages.

Configure database connection settings and source-data paths before running ingestion or validation scripts.

## Batch Data Processing

The source code under `src/` contains ingestion, staging, validation, and business-loading scripts.

The general batch workflow is:

1. Prepare the source CSV or Parquet datasets.
2. Load source records into PostgreSQL RAW tables.
3. Run staging validation.
4. Separate valid and invalid records.
5. Review rejected records and data-quality results.
6. Load valid records into the BUSINESS layer.
7. Validate business tables and analytical marts.

Review each script before execution to understand whether it appends, updates, truncates, or replaces existing data.

## Change Data Capture and Streaming

The intended streaming workflow is:

1. Insert or update a record in PostgreSQL RAW.
2. PostgreSQL logical replication makes the change available to Debezium.
3. Debezium captures the database change.
4. Apache Kafka receives the event in the configured topic.
5. The Python CDC validation service consumes RAW events.
6. Validated records are written to PostgreSQL STAGING.
7. Staging changes can be captured and published through a staging CDC connector.
8. Apache Flink consumes staging events and applies business-layer processing.

The current implementation and configuration should be checked before assuming every stage is running continuously.

### Example Kafka Topics

The local project has used topics including:

```text
customer.raw.customers
customer.raw.products
customer.raw.orders
customer.raw.order_items

customer.clean.customers
customer.clean.products
customer.clean.orders
customer.clean.order_items

customer.staging.stg_customers
```

The actual topics available depend on the connector configuration and which pipeline components have been started.

### Debezium Connector Configuration

Connector configuration files are maintained under `config/` and `src/config/`.

The local setup uses PostgreSQL logical replication, a replication slot, and a publication to capture database changes. Connector names, table filters, topic prefixes, and replication settings should match the actual database and Docker environment.

### Flink Processing

The `flink/` directory contains the Java CDC job, Maven configuration, and SQL test files.

Build the Java application from the Flink project directory:

```bash
cd flink
mvn clean package
```

This requires Java and Maven to be installed and the required dependencies to be accessible.

Configure the Kafka topic, consumer group, PostgreSQL JDBC connection, and business-layer logic before submitting the job to the Flink cluster.

## Data and Build Artifacts

Large datasets, Python virtual environments, compiled files, and generated build outputs are excluded from Git.

To run the project locally:

1. Obtain the authorized source datasets.
2. Place them under the appropriate local `data/` directory.
3. Update ingestion paths if necessary.
4. Configure database connectivity.
5. Start the required infrastructure.
6. Run the ingestion and processing steps.

Flink connector JARs and Maven build outputs should be obtained or generated locally as required by the selected versions.


## Project Status

This is an evolving data engineering project. Batch ingestion, data-quality validation, PostgreSQL data layers, audit tracking, and CDC infrastructure have been developed incrementally.

The end-to-end integration should be tested against the current local configuration before being treated as production-ready.
