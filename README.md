# Smart City Data Platform

A comprehensive data platform for ingesting, processing, and managing smart city data from multiple sources including IoT sensors, APIs, databases, and user-generated content.

## Features

- Multi-source data ingestion (databases, APIs, IoT streams, user events)
- Real-time streaming with Kafka
- Workflow orchestration with Apache Airflow
- Configurable data pipelines
- Scalable architecture

## Getting Started

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Configure environment variables:
```bash
cp .env.example .env
# Edit .env with your configuration
```

3. (Optional) Start services with Docker:
```bash
docker-compose up -d
```

## Project Structure

- `configs/` - Configuration files for various services
- `ingestion/` - Core data ingestion modules
- `airflow_dags/` - Airflow DAG definitions
- `tests/` - Unit and integration tests
- `data/` - Local data storage (raw and processed)

## License

MIT
