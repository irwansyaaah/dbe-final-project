# Smoking Data Pipeline

This project implements a data pipeline using PostgreSQL, Kafka, and Metabase to process and visualize smoking data.

## Prerequisites

- Docker and Docker Compose
- Python 3.8+
- pip (Python package manager)

## Setup Instructions

1. Install Python dependencies:
```bash
pip install -r requirements.txt
```

2. Start the services using Docker Compose:
```bash
docker-compose up -d
```

3. Wait for all services to start (this may take a few minutes)

4. Initialize the PostgreSQL database:
```bash
# Connect to PostgreSQL container
docker exec -it postgres psql -U postgres -d smoking_db

# Create the smoking table and import data
\i /data/smoking.sql
```

5. Run the data pipeline:
```bash
python scripts/pipeline.py
```

## Accessing the Services

- **Metabase**: http://localhost:3000
  - First-time setup: Follow the setup wizard
  - Connect to PostgreSQL using:
    - Host: postgres
    - Port: 5432
    - Database: smoking_db
    - Username: postgres
    - Password: postgres

- **PostgreSQL**: localhost:5432
  - Database: smoking_db
  - Username: postgres
  - Password: postgres

- **Kafka**: localhost:9092

## Data Flow

1. Data is stored in PostgreSQL
2. The Python script reads data from PostgreSQL and streams it to Kafka
3. Metabase connects to PostgreSQL to visualize the data

## Creating Visualizations in Metabase

1. Log in to Metabase (http://localhost:3000)
2. Add a new database connection to PostgreSQL
3. Create new questions/visualizations using the smoking data
4. Create dashboards to display your visualizations

## Stopping the Services

To stop all services:
```bash
docker-compose down
```

To stop and remove all data (including volumes):
```bash
docker-compose down -v
```

---