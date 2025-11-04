from faker import Faker
import psycopg2
from datetime import datetime, timedelta
import random
import os
from dotenv import load_dotenv

load_dotenv()

fake = Faker()

# Database connection - works both locally and in Docker
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres123")
DB_HOST = os.getenv("DB_HOST", "localhost")  # Will be "postgres" in Docker
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "smart_city_db")

# Event data
EVENT_TYPES = ['Concert', 'Theater', 'Sports', 'Museum', 'Festival', 'Exhibition']
VENUES = ['City Arena', 'Municipal Theater', 'Sports Stadium', 'Art Museum', 'Convention Center']
PAYMENT_METHODS = ['credit_card', 'debit_card', 'mobile_payment', 'cash', 'online']

def generate_ticket_sales(num_records=1000):
    """Generate fake ticket sales data"""
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    cursor = conn.cursor()

    # Create table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS ticket_sales (
            ticket_id SERIAL PRIMARY KEY,
            event_name VARCHAR(255),
            event_type VARCHAR(100),
            venue VARCHAR(255),
            purchase_date TIMESTAMP,
            event_date TIMESTAMP,
            customer_name VARCHAR(255),
            customer_email VARCHAR(255),
            ticket_price DECIMAL(10, 2),
            quantity INTEGER,
            total_amount DECIMAL(10, 2),
            payment_method VARCHAR(50),
            seat_section VARCHAR(50)
        )
    """)
    conn.commit()

    print(f"Generating {num_records} ticket sales records...")

    for i in range(num_records):
        # Generate random data
        purchase_date = fake.date_time_between(start_date='-1y', end_date='now')
        event_date = purchase_date + timedelta(days=random.randint(1, 90))
        event_type = random.choice(EVENT_TYPES)

        ticket_price = round(random.uniform(15.00, 250.00), 2)
        quantity = random.randint(1, 6)
        total_amount = round(ticket_price * quantity, 2)

        # Insert data
        cursor.execute("""
            INSERT INTO ticket_sales (
                event_name, event_type, venue, purchase_date, event_date,
                customer_name, customer_email, ticket_price, quantity,
                total_amount, payment_method, seat_section
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            f"{fake.catch_phrase()} - {event_type}",
            event_type,
            random.choice(VENUES),
            purchase_date,
            event_date,
            fake.name(),
            fake.email(),
            ticket_price,
            quantity,
            total_amount,
            random.choice(PAYMENT_METHODS),
            f"Section {random.choice(['A', 'B', 'C'])}-{random.randint(1, 20)}"
        ))

        if (i + 1) % 100 == 0:
            print(f"Inserted {i + 1}/{num_records} records...")

    conn.commit()
    cursor.close()
    conn.close()

    print(f"Successfully inserted {num_records} ticket sales records!")

if __name__ == "__main__":
    generate_ticket_sales(1000)
