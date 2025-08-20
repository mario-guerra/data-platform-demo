-- Orders serving view for analytical queries
-- This creates a serving table/view in PostgreSQL for real-time order analytics

-- Create the raw orders table to store data from NiFi
CREATE TABLE IF NOT EXISTS orders_raw (
    order_id VARCHAR(255) PRIMARY KEY,
    customer_id VARCHAR(255) NOT NULL,
    product VARCHAR(255) NOT NULL,
    quantity INTEGER NOT NULL,
    price DECIMAL(10,2) NOT NULL,
    currency VARCHAR(10) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    region VARCHAR(50) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_orders_raw_timestamp ON orders_raw(timestamp);
CREATE INDEX IF NOT EXISTS idx_orders_raw_customer ON orders_raw(customer_id);
CREATE INDEX IF NOT EXISTS idx_orders_raw_region ON orders_raw(region);
CREATE INDEX IF NOT EXISTS idx_orders_raw_currency ON orders_raw(currency);

-- Create a serving view for real-time analytics
CREATE OR REPLACE VIEW orders_serving_view AS
SELECT 
    -- Basic order information
    order_id,
    customer_id,
    product,
    quantity,
    price,
    currency,
    timestamp as order_timestamp,
    region,
    
    -- Calculated fields
    (quantity * price) as total_amount,
    DATE(timestamp) as order_date,
    EXTRACT(HOUR FROM timestamp) as order_hour,
    
    -- Time-based categorization
    CASE 
        WHEN EXTRACT(HOUR FROM timestamp) BETWEEN 9 AND 17 THEN 'Business Hours'
        WHEN EXTRACT(HOUR FROM timestamp) BETWEEN 18 AND 22 THEN 'Evening'
        ELSE 'Night/Early Morning'
    END as time_category,
    
    -- Price categorization
    CASE 
        WHEN price < 50 THEN 'Low Value'
        WHEN price BETWEEN 50 AND 200 THEN 'Medium Value'
        WHEN price BETWEEN 200 AND 500 THEN 'High Value'
        ELSE 'Premium'
    END as price_category,
    
    -- Currency conversion to USD (simplified example rates)
    CASE currency
        WHEN 'USD' THEN price
        WHEN 'EUR' THEN price * 1.10
        WHEN 'GBP' THEN price * 1.25
        WHEN 'JPY' THEN price * 0.007
        WHEN 'CAD' THEN price * 0.75
        ELSE price
    END as price_usd,
    
    created_at
FROM orders_raw
WHERE timestamp >= CURRENT_DATE - INTERVAL '30 days';

-- Create materialized view for daily aggregations (better performance)
CREATE MATERIALIZED VIEW IF NOT EXISTS daily_order_summary AS
SELECT 
    DATE(timestamp) as order_date,
    region,
    currency,
    COUNT(*) as total_orders,
    SUM(quantity) as total_quantity,
    SUM(quantity * price) as total_revenue,
    AVG(price) as avg_price,
    MAX(price) as max_price,
    MIN(price) as min_price,
    COUNT(DISTINCT customer_id) as unique_customers,
    COUNT(DISTINCT product) as unique_products
FROM orders_raw
GROUP BY DATE(timestamp), region, currency
ORDER BY order_date DESC, total_revenue DESC;

-- Create index on the materialized view
CREATE UNIQUE INDEX IF NOT EXISTS idx_daily_summary_unique 
ON daily_order_summary(order_date, region, currency);

-- Function to refresh the materialized view
CREATE OR REPLACE FUNCTION refresh_daily_summary()
RETURNS VOID AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY daily_order_summary;
END;
$$ LANGUAGE plpgsql;

-- Example analytical queries

-- 1. Real-time dashboard query - last 24 hours
CREATE OR REPLACE VIEW realtime_dashboard AS
SELECT 
    COUNT(*) as orders_24h,
    SUM(total_amount) as revenue_24h,
    AVG(total_amount) as avg_order_value_24h,
    COUNT(DISTINCT customer_id) as unique_customers_24h,
    COUNT(DISTINCT region) as active_regions_24h
FROM orders_serving_view
WHERE order_timestamp >= CURRENT_TIMESTAMP - INTERVAL '24 hours';

-- 2. Hourly trend analysis
CREATE OR REPLACE VIEW hourly_trends AS
SELECT 
    order_hour,
    COUNT(*) as order_count,
    SUM(total_amount) as revenue,
    AVG(total_amount) as avg_order_value
FROM orders_serving_view
WHERE order_date = CURRENT_DATE
GROUP BY order_hour
ORDER BY order_hour;

-- 3. Regional performance
CREATE OR REPLACE VIEW regional_performance AS
SELECT 
    region,
    COUNT(*) as total_orders,
    SUM(total_amount) as total_revenue,
    AVG(total_amount) as avg_order_value,
    COUNT(DISTINCT customer_id) as unique_customers
FROM orders_serving_view
WHERE order_date >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY region
ORDER BY total_revenue DESC;

-- 4. Product popularity by region
CREATE OR REPLACE VIEW product_regional_popularity AS
SELECT 
    region,
    product,
    COUNT(*) as order_count,
    SUM(quantity) as total_quantity,
    SUM(total_amount) as total_revenue,
    RANK() OVER (PARTITION BY region ORDER BY COUNT(*) DESC) as popularity_rank
FROM orders_serving_view
WHERE order_date >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY region, product
ORDER BY region, popularity_rank;

-- 5. Currency distribution
CREATE OR REPLACE VIEW currency_distribution AS
SELECT 
    currency,
    COUNT(*) as order_count,
    SUM(total_amount) as revenue_local_currency,
    SUM(price_usd * quantity) as revenue_usd,
    ROUND(COUNT(*)::DECIMAL / SUM(COUNT(*)) OVER () * 100, 2) as percentage
FROM orders_serving_view
WHERE order_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY currency
ORDER BY order_count DESC;

-- Grant permissions for read-only analytics user
-- CREATE USER analytics_user WITH PASSWORD 'analytics_password';
-- GRANT SELECT ON ALL TABLES IN SCHEMA public TO analytics_user;
-- GRANT SELECT ON ALL VIEWS IN SCHEMA public TO analytics_user;