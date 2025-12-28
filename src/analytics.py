import duckdb
import os
import math
from datetime import datetime, timedelta

DB_PATH = os.path.join(os.path.dirname(__file__), "../data/analytics.duckdb")

def get_connection():
    return duckdb.connect(DB_PATH, read_only=True) # API should be read-only mostly

def get_writable_connection():
    return duckdb.connect(DB_PATH, read_only=False)

def read_sql_file(filename):
    # Assumes run from src/
    path = os.path.join(os.path.dirname(__file__), "../database/models", filename)
    with open(path, 'r') as f:
        return f.read()

def init_analytics():
    print("Initializing analytics views...")
    con = duckdb.connect(DB_PATH) # Default read_only=False
    
    # Order matters
    files = [
        "dm_daily_stats.sql",
        "dm_business_trust_score.sql",
        "dm_industry_benchmarks.sql"
    ]
    
    for f in files:
        print(f"Executing {f}...")
        sql = read_sql_file(f)
        con.execute(sql)
    
    con.close()
    print("Analytics views initialized.")

def get_business_overview(business_id: int):
    con = get_connection()
    # Get basic info + trust score
    query = """
        SELECT 
            b.id,
            b.name,
            b.industry,
            b.location,
            ts.trust_score,
            ts.total_reviews,
            ts.weighted_rating,
            ts.response_rate,
            ib.p50_trust_score as industry_avg_trust,
            ib.p90_trust_score as industry_top_trust
        FROM raw_businesses b
        LEFT JOIN dm_business_trust_score ts ON b.id = ts.business_id
        LEFT JOIN dm_industry_benchmarks ib ON b.industry = ib.industry
        WHERE b.id = ?
    """
    result = con.execute(query, [business_id]).fetchone()
    con.close()
    
    if not result:
        return None
        
    cols = ["id", "name", "industry", "location", "trust_score", "total_reviews", "weighted_rating", "response_rate", "industry_avg_trust", "industry_top_trust"]
    return dict(zip(cols, result))

def get_business_trends(business_id: int):
    con = get_connection()
    query = """
        SELECT 
            date,
            review_count,
            daily_rating,
            daily_sentiment,
            running_avg_rating
        FROM dm_daily_stats
        WHERE business_id = ?
        ORDER BY date ASC
    """
    result = con.execute(query, [business_id]).fetchall()
    con.close()
    
    return [
        {
            "date": r[0].isoformat(),
            "review_count": r[1],
            "daily_rating": r[2],
            "daily_sentiment": r[3],
            "running_avg_rating": r[4]
        }
        for r in result
    ]

def get_industry_benchmark(business_id: int):
    # First get business industry
    con = get_connection()
    industry_res = con.execute("SELECT industry FROM raw_businesses WHERE id = ?", [business_id]).fetchone()
    if not industry_res:
        con.close()
        return None
    
    industry = industry_res[0]
    
    query = """
        SELECT * FROM dm_industry_benchmarks WHERE industry = ?
    """
    result = con.execute(query, [industry]).fetchone()
    con.close()
    
    if not result:
        return None

    cols = ["industry", "p50_rating", "p90_rating", "p50_trust_score", "p90_trust_score", "avg_response_rate"]
    return dict(zip(cols, result))

def get_sentiment_distribution(business_id: int, days: int = 60):
    con = get_connection()
    cutoff_date = datetime.now() - timedelta(days=days)
    
    query = """
        SELECT 
            CASE 
                WHEN sentiment_score > 0.2 THEN 'Positive'
                WHEN sentiment_score < -0.2 THEN 'Negative'
                ELSE 'Neutral'
            END as sentiment_category,
            COUNT(*) as count
        FROM raw_reviews
        WHERE business_id = ? 
          AND created_at >= ?
        GROUP BY 1
    """
    result = con.execute(query, [business_id, cutoff_date]).fetchall()
    con.close()
    
    dist = {"Positive": 0, "Neutral": 0, "Negative": 0}
    for r in result:
        dist[r[0]] = r[1]
    return dist

def get_performance_deltas(business_id: int):
    con = get_connection()
    
    
    query = """
    WITH periods AS (
        SELECT 
            id,
            rating,
            sentiment_score,
            CASE 
                WHEN created_at >= CURRENT_DATE - INTERVAL 30 DAY THEN 'current'
                WHEN created_at >= CURRENT_DATE - INTERVAL 60 DAY THEN 'previous'
                ELSE 'older'
            END as period
        FROM raw_reviews
        WHERE business_id = ? AND created_at >= CURRENT_DATE - INTERVAL 60 DAY
    ),
    responses AS (
        SELECT 
            r.id,
            CASE 
                WHEN r.created_at >= CURRENT_DATE - INTERVAL 30 DAY THEN 'current'
                WHEN r.created_at >= CURRENT_DATE - INTERVAL 60 DAY THEN 'previous'
            END as period,
            COUNT(resp.id) as has_response
        FROM raw_reviews r
        LEFT JOIN raw_responses resp ON r.id = resp.review_id
        WHERE r.business_id = ? AND r.created_at >= CURRENT_DATE - INTERVAL 60 DAY
        GROUP BY r.id, r.created_at
    ),
    stats AS (
        SELECT 
            p.period,
            AVG(p.rating) as avg_rating,
            SUM(CASE WHEN p.sentiment_score < -0.2 THEN 1.0 ELSE 0.0 END) / COUNT(*) as neg_sentiment_rate,
            AVG(res.has_response) as response_rate
        FROM periods p
        JOIN responses res ON p.id = res.id
        WHERE p.period IN ('current', 'previous')
        GROUP BY p.period
    )
    SELECT * FROM stats;
    """
    
    result = con.execute(query, [business_id, business_id]).fetchall()
    con.close()
    
    # Process results into dictionary
    data = {
        'current': {'rating': 0, 'neg_sentiment': 0, 'response_rate': 0},
        'previous': {'rating': 0, 'neg_sentiment': 0, 'response_rate': 0}
    }
    
    for r in result:
        period = r[0] # 'current' or 'previous'
        data[period]['rating'] = r[1] if r[1] else 0
        data[period]['neg_sentiment'] = r[2] if r[2] else 0
        data[period]['response_rate'] = r[3] if r[3] else 0
        
    return {
        'delta_rating': data['current']['rating'] - data['previous']['rating'],
        'delta_neg_sentiment': data['current']['neg_sentiment'] - data['previous']['neg_sentiment'],
        'delta_response_rate': data['current']['response_rate'] - data['previous']['response_rate']
    }

def get_rating_trend_monthly(business_id: int):
    con = get_connection()
    query = """
        SELECT 
            DATE_TRUNC('month', created_at) as month,
            AVG(rating) as avg_rating
        FROM raw_reviews
        WHERE business_id = ?
        GROUP BY 1
        ORDER BY 1 ASC
    """
    result = con.execute(query, [business_id]).fetchall()
    con.close()
    
    return [
        {"date": r[0].isoformat(), "rating": r[1]} for r in result
    ]

def get_geo_sentiment_data():
    """
    Returns aggregated metrics per business location for the map.
    """
    con = get_connection()
    query = """
        SELECT 
            b.id,
            b.name,
            b.location as city,
            b.latitude,
            b.longitude,
            COUNT(r.id) as review_count,
            AVG(r.rating) as avg_rating,
            AVG(r.sentiment_score) as net_sentiment_score,
            -- Response rate logic
            (SELECT COUNT(*) FROM raw_responses resp JOIN raw_reviews r2 ON resp.review_id = r2.id WHERE r2.business_id = b.id) * 1.0 / NULLIF(COUNT(r.id), 0) as response_rate
        FROM raw_businesses b
        LEFT JOIN raw_reviews r ON b.id = r.business_id
        GROUP BY 1, 2, 3, 4, 5
        HAVING COUNT(r.id) > 0
    """
    result = con.execute(query).fetchall()
    con.close()
    
    cols = ["location_id", "name", "city", "latitude", "longitude", "review_count", "avg_rating", "net_sentiment_score", "response_rate"]
    return [dict(zip(cols, r)) for r in result]

def get_geo_insight():
    """
    Compares top 20% review-volume locations vs bottom 50%.
    """
    data = get_geo_sentiment_data()
    if not data:
        return "Not enough data for insights."
        
    # Sort by review count desc
    data_sorted = sorted(data, key=lambda x: x['review_count'], reverse=True)
    n = len(data_sorted)
    
    top_20_count = max(1, int(n * 0.2))
    bottom_50_count = max(1, int(n * 0.5))
    
    top_20_locs = data_sorted[:top_20_count]
    bottom_50_locs = data_sorted[-bottom_50_count:]
    
    avg_sent_top = sum(d['net_sentiment_score'] for d in top_20_locs) / len(top_20_locs)
    avg_sent_bottom = sum(d['net_sentiment_score'] for d in bottom_50_locs) / len(bottom_50_locs)
    
    diff = avg_sent_top - avg_sent_bottom
    

    # Threshold 0.1 for significance
    if diff < -0.1:
        return "High-volume locations show consistently lower sentiment compared to lower-volume locations."
    elif diff > 0.1:
        return "High-volume locations show consistently higher sentiment compared to lower-volume locations."
    else:
        return "Sentiment is consistent across both high and low volume locations."
