import psycopg2
from psycopg2.extras import execute_values
from faker import Faker
import time

# ---- Database connection details ----
DB_NAME = "employees_record"
DB_USER = "postgres"
DB_PASSWORD = "3CeWfQIX8X"
DB_HOST = "localhost"
DB_PORT = "5432"

# ---- Data generation settings ----
TOTAL_RECORDS = 100000
BATCH_SIZE = 5000  # A good batch size for performance

def generate_address_data(num_records):
    """Generates a list of tuples with fake address data."""
    fake = Faker()
    return [(fake.street_address(), fake.city(), fake.state_abbr(), fake.zipcode()) for _ in range(num_records)]

def generate_staff_data(num_records, address_ids):
    """Generates a list of tuples with fake staff data, including a foreign key."""
    fake = Faker()
    # Shuffle the address IDs to ensure random distribution
    fake.random.shuffle(address_ids)
    
    staff_data = []
    for i in range(num_records):
        first_name = fake.first_name()
        last_name = fake.last_name()
        email = f"{first_name.lower()}.{last_name.lower()}{i}@example.com"
        address_id = address_ids[i]
        staff_data.append((first_name, last_name, email, address_id))
    return staff_data

def bulk_insert():
    """Performs the bulk insert operation."""
    conn = None
    try:
        # Connect to the database
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        cur = conn.cursor()
        print("Connected to PostgreSQL database.")
        
        start_time = time.time()
        
        # --- Step 1: Insert addresses ---
        print(f"Generating {TOTAL_RECORDS} fake address records...")
        address_data = generate_address_data(TOTAL_RECORDS)
        
        print("Inserting address records in batches...")
        address_ids = []
        for i in range(0, TOTAL_RECORDS, BATCH_SIZE):
            batch = address_data[i:i + BATCH_SIZE]
            query = "INSERT INTO address (street_address, city, state, zip_code) VALUES %s RETURNING id"
            
            # Use execute_values for efficient batch insertion
            ids_batch = execute_values(cur, query, batch, fetch=True)
            address_ids.extend([item[0] for item in ids_batch])
            
        print(f"Successfully inserted {len(address_ids)} address records.")
        
        # --- Step 2: Insert staff ---
        print(f"Generating {TOTAL_RECORDS} fake staff records...")
        staff_data = generate_staff_data(TOTAL_RECORDS, address_ids)
        
        print("Inserting staff records in batches...")
        for i in range(0, TOTAL_RECORDS, BATCH_SIZE):
            batch = staff_data[i:i + BATCH_SIZE]
            query = "INSERT INTO staff (first_name, last_name, email, address_id) VALUES %s"
            execute_values(cur, query, batch)
            
        print(f"Successfully inserted {TOTAL_RECORDS} staff records.")
        
        # Commit the transaction
        conn.commit()
        end_time = time.time()
        print(f"\nAll data inserted successfully in {end_time - start_time:.2f} seconds.")

    except psycopg2.Error as e:
        print(f"Database error: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            cur.close()
            conn.close()
            print("Database connection closed.")

if __name__ == "__main__":
    bulk_insert()