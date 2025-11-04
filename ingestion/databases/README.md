# Database Population & Ingestion

## Populating Ticket Sales Data

### Using Docker (Recommended)

```bash
# Start PostgreSQL
docker-compose up -d postgres

# Populate ticket sales data
docker-compose run --rm populate_tickets

# Check the data
docker-compose exec postgres psql -U postgres -d smart_city_db -c "SELECT COUNT(*) FROM ticket_sales;"
```

### Running Locally (Without Docker)

```bash
# Install dependencies
pip install -r requirements.txt

# Run the script
python ingestion/databases/populate_ticket_sales.py
```

## Running the Ingestion Pipeline

After populating the data, run the ingestion pipeline:

```bash
docker-compose up ingest
```

Or locally:
```bash
python ingestion/databases/postgres_ingest.py
```

## Environment Variables

The script uses these environment variables (automatically set in Docker):

- `DB_HOST`: Database host (default: `localhost`, Docker: `postgres`)
- `DB_PORT`: Database port (default: `5432`)
- `DB_NAME`: Database name (default: `smart_city_db`)
- `DB_USER`: Database user (default: `postgres`)
- `DB_PASSWORD`: Database password (default: `postgres123`)

## What the Script Does

The `populate_ticket_sales.py` script:

1. Connects to PostgreSQL database
2. Creates the `ticket_sales` table if it doesn't exist
3. Generates 1000 fake ticket sales records using Faker library with:
   - Random event names and types (Concert, Theater, Sports, etc.)
   - Random venues
   - Customer names and emails (fake data)
   - Ticket prices, quantities, and totals
   - Payment methods
   - Seat sections
4. Inserts all records into the database

The data is then ready to be extracted by `postgres_ingest.py` for further processing.
