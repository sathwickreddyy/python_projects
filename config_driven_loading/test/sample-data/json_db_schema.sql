-- Create audit table first
CREATE TABLE IF NOT EXISTS data_loading_audit (
    id SERIAL PRIMARY KEY,
    table_name VARCHAR(255) NOT NULL,
    source_type VARCHAR(50) NOT NULL,
    record_count INTEGER NOT NULL,
    duration_ms BIGINT NOT NULL,
    execution_time TIMESTAMP NOT NULL,
    successful_records INTEGER DEFAULT 0,
    error_records INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create user_profiles table for MAPPED strategy
CREATE TABLE IF NOT EXISTS user_profiles (
    id SERIAL PRIMARY KEY,
    user_id VARCHAR(50) UNIQUE NOT NULL,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    email VARCHAR(255) NOT NULL,
    phone VARCHAR(20),
    city VARCHAR(100),
    country VARCHAR(100),
    date_of_birth DATE,
    notifications_enabled BOOLEAN DEFAULT true,
    newsletter_subscribed BOOLEAN DEFAULT false,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create user_profiles_direct table for DIRECT strategy testing
CREATE TABLE IF NOT EXISTS user_profiles_direct (
    id SERIAL PRIMARY KEY,
    USER_ID VARCHAR(50),  -- Note: DIRECT strategy converts camelCase to UPPER_SNAKE_CASE
    FIRST_NAME VARCHAR(100),
    LAST_NAME VARCHAR(100),
    EMAIL VARCHAR(255),
    PHONE VARCHAR(20),
    DATE_OF_BIRTH VARCHAR(50),  -- Will be string in DIRECT mode
    NOTIFICATIONS BOOLEAN,
    NEWSLETTER BOOLEAN,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create order_details table
CREATE TABLE IF NOT EXISTS order_details (
    id SERIAL PRIMARY KEY,
    order_id VARCHAR(50) UNIQUE NOT NULL,
    customer_name VARCHAR(255) NOT NULL,
    customer_email VARCHAR(255),
    primary_product VARCHAR(255),
    primary_quantity INTEGER,
    total_amount DECIMAL(10, 2),
    shipping_city VARCHAR(100),
    shipping_zip VARCHAR(20),
    created_date TIMESTAMP,
    order_status VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_user_profiles_user_id ON user_profiles(user_id);
CREATE INDEX IF NOT EXISTS idx_user_profiles_email ON user_profiles(email);
CREATE INDEX IF NOT EXISTS idx_order_details_order_id ON order_details(order_id);
CREATE INDEX IF NOT EXISTS idx_order_details_customer_email ON order_details(customer_email);
CREATE INDEX IF NOT EXISTS idx_audit_table_name ON data_loading_audit(table_name);
CREATE INDEX IF NOT EXISTS idx_audit_execution_time ON data_loading_audit(execution_time);
