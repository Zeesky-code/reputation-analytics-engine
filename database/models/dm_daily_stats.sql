-- dm_daily_stats.sql
CREATE OR REPLACE VIEW dm_daily_stats AS
SELECT 
    business_id,
    CAST(created_at AS DATE) as date,
    COUNT(*) as review_count,
    AVG(rating) as daily_rating,
    AVG(sentiment_score) as daily_sentiment,
    SUM(COUNT(*)) OVER (PARTITION BY business_id ORDER BY CAST(created_at AS DATE)) as cumulative_reviews,
    AVG(AVG(rating)) OVER (PARTITION BY business_id ORDER BY CAST(created_at AS DATE) ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as running_avg_rating
FROM raw_reviews
GROUP BY 1, 2;
