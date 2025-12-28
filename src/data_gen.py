import duckdb
import random
from faker import Faker
from datetime import datetime, timedelta
import pandas as pd
from textblob import TextBlob

import os

# Configuration
# Resolves to .../reputation_analytics_demo/data/analytics.duckdb
DB_PATH = os.path.join(os.path.dirname(__file__), "../data/analytics.duckdb")
NUM_BUSINESSES = 50
MAX_REVIEWS_PER_BUSINESS = 100
START_DATE = datetime.now() - timedelta(days=365)
END_DATE = datetime.now()

fake = Faker()

def create_schema(con):
    con.execute("DROP TABLE IF EXISTS raw_responses")
    con.execute("DROP TABLE IF EXISTS raw_reviews")
    con.execute("DROP TABLE IF EXISTS raw_businesses")
    
    con.execute("""
        CREATE TABLE IF NOT EXISTS raw_businesses (
            id INTEGER PRIMARY KEY,
            name VARCHAR,
            industry VARCHAR,
            location VARCHAR,
            latitude DOUBLE,
            longitude DOUBLE
        );
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS raw_reviews (
            id INTEGER PRIMARY KEY,
            business_id INTEGER,
            rating INTEGER,
            text VARCHAR,
            sentiment_score DOUBLE,
            created_at TIMESTAMP,
            FOREIGN KEY (business_id) REFERENCES raw_businesses(id)
        );
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS raw_responses (
            id INTEGER PRIMARY KEY,
            review_id INTEGER,
            response_text VARCHAR,
            responded_at TIMESTAMP,
            FOREIGN KEY (review_id) REFERENCES raw_reviews(id)
        );
    """)

def generate_data(con):
    print("Generating businesses...")
    industries = ['Restaurant', 'Retail', 'Service', 'Hospitality', 'Automotive']
    businesses = []
    
    # Lat: 6.4 to 6.7, Lon: 3.2 to 3.5 roughly around Lagos for density
    lat_min, lat_max = 6.40, 6.70
    lon_min, lon_max = 3.20, 3.50
    
    for i in range(1, NUM_BUSINESSES + 1):
        businesses.append({
            'id': i,
            'name': fake.company(),
            'industry': random.choice(industries),
            'location': fake.city(),
            'latitude': random.uniform(lat_min, lat_max),
            'longitude': random.uniform(lon_min, lon_max)
        })
    
    con.executemany("INSERT INTO raw_businesses VALUES (?, ?, ?, ?, ?, ?)", 
                    [[b['id'], b['name'], b['industry'], b['location'], b['latitude'], b['longitude']] for b in businesses])

    print("Generating reviews...")
    reviews = []
    responses = []
    review_id_counter = 1
    response_id_counter = 1

    for business in businesses:
        num_reviews = random.randint(5, MAX_REVIEWS_PER_BUSINESS)
        # Create a "quality" profile for the business to make data realistic
        # e.g., a good business gets mostly 4-5 stars
        quality_profile = random.choice(['excellent', 'good', 'average', 'poor'])
        
        for _ in range(num_reviews):
            created_at = fake.date_time_between(start_date=START_DATE, end_date=END_DATE)
            
            if quality_profile == 'excellent':
                rating = random.choices([5, 4, 3, 2, 1], weights=[60, 30, 5, 3, 2])[0]
            elif quality_profile == 'good':
                rating = random.choices([5, 4, 3, 2, 1], weights=[30, 40, 15, 10, 5])[0]
            elif quality_profile == 'average':
                rating = random.choices([5, 4, 3, 2, 1], weights=[10, 20, 40, 20, 10])[0]
            else: # poor
                rating = random.choices([5, 4, 3, 2, 1], weights=[5, 10, 20, 35, 30])[0]

            review_text = fake.paragraph(nb_sentences=2)
            # Simple sentiment to match the rating loosely
            blob = TextBlob(review_text)
            sentiment_score = blob.sentiment.polarity
            
            if rating >= 4:
                sentiment_score = random.uniform(0.3, 0.9)
            elif rating <= 2:
                sentiment_score = random.uniform(-0.9, -0.1)
            else:
                sentiment_score = random.uniform(-0.2, 0.2)

            reviews.append([
                review_id_counter,
                business['id'],
                rating,
                review_text,
                sentiment_score,
                created_at
            ])
            
            # Response rate depends on the business "diligence"
            response_chance = random.uniform(0, 1)
            if response_chance > 0.4: 
                responded_at = created_at + timedelta(hours=random.randint(1, 72))
                if responded_at < datetime.now():
                    responses.append([
                        response_id_counter,
                        review_id_counter,
                        fake.sentence(),
                        responded_at
                    ])
                    response_id_counter += 1

            review_id_counter += 1

    print(f"Inserting {len(reviews)} reviews...")
    con.executemany("INSERT INTO raw_reviews VALUES (?, ?, ?, ?, ?, ?)", reviews)
    
    print(f"Inserting {len(responses)} responses...")
    con.executemany("INSERT INTO raw_responses VALUES (?, ?, ?, ?)", responses)

def main():
    con = duckdb.connect(DB_PATH)
    create_schema(con)
    
    count = con.execute("SELECT count(*) FROM raw_businesses").fetchone()[0]
    if count == 0:
        generate_data(con)
        print("Data generation complete.")
    else:
        print("Data already exists. Skipping generation.")
        
    con.close()

if __name__ == "__main__":
    main()
