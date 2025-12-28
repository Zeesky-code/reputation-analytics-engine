-- dm_business_trust_score.sql
CREATE OR REPLACE VIEW dm_business_trust_score AS
WITH reviews_augmented AS (
    SELECT 
        r.*,
        DATE_DIFF('day', created_at, CURRENT_TIMESTAMP) as days_ago,
        -- simple linear decay
        GREATEST(0, 1 - (DATE_DIFF('day', created_at, CURRENT_TIMESTAMP) / 730.0)) as time_weight
    FROM raw_reviews r
),
weighted_ratings AS (
    SELECT 
        business_id,
        SUM(rating * time_weight) / SUM(time_weight) as time_weighted_rating,
        COUNT(*) as total_reviews
    FROM reviews_augmented
    GROUP BY business_id
),
response_stats AS (
    SELECT 
        r.business_id,
        COUNT(resp.id) * 1.0 / COUNT(r.id) as response_rate
    FROM raw_reviews r
    LEFT JOIN raw_responses resp ON r.id = resp.review_id
    GROUP BY r.business_id
)
SELECT 
    b.id as business_id,
    b.name,
    b.industry,
    wr.total_reviews,
    ROUND(wr.time_weighted_rating, 2) as weighted_rating,
    ROUND(COALESCE(rs.response_rate, 0), 2) as response_rate,
    -- Trust Score Construction:
    -- 1. Rating Component (50%): (Weighted Rating / 5) * 100
    -- 2. Response Component (30%): Response Rate * 100
    -- 3. Volume Component (20%): Log scaled, maxing out at roughly 100 reviews. LOG10(100) = 2.
    --    Trust Score = (Rating% * 0.5) + (Response% * 0.3) + (Volume% * 0.2)
    CAST(
        ( (wr.time_weighted_rating / 5.0) * 100 * 0.5 ) +
        ( COALESCE(rs.response_rate, 0) * 100 * 0.3 ) +
        ( LEAST(LOG10(wr.total_reviews + 1) * 50, 100) * 0.2 )
    AS INTEGER) as trust_score
FROM raw_businesses b
JOIN weighted_ratings wr ON b.id = wr.business_id
LEFT JOIN response_stats rs ON b.id = rs.business_id;
