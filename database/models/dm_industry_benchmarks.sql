-- dm_industry_benchmarks.sql
CREATE OR REPLACE VIEW dm_industry_benchmarks AS
SELECT 
    industry,
    -- Rating Percentiles
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY weighted_rating) as p50_rating,
    PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY weighted_rating) as p90_rating,
    -- Trust Score Percentiles
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY trust_score) as p50_trust_score,
    PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY trust_score) as p90_trust_score,
    AVG(response_rate) as avg_response_rate
FROM dm_business_trust_score
GROUP BY industry;
