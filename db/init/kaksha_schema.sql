-- Kaksha Database Schema Initialization Script
-- Contains users and licenses tables with related functionality

-- Enable UUID extension for generating UUIDs
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- Create kaksha_users table
CREATE TABLE IF NOT EXISTS kaksha_users (
    userid VARCHAR(50) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL UNIQUE,
    lastlogin TIMESTAMP WITHOUT TIME ZONE,
    islicenseactive BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    -- Check constraints
    CONSTRAINT chk_email_format CHECK (email ~* '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}')
);

-- Create kaksha_licenses table
CREATE TABLE IF NOT EXISTS kaksha_licenses (
    license_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    userid VARCHAR(50) NOT NULL,
    license_type VARCHAR(20) NOT NULL,
    start_date DATE NOT NULL,
    expiry_date DATE NOT NULL,
    product_name VARCHAR(100) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'ACTIVE',
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    -- Check constraints
    CONSTRAINT chk_date_validity CHECK (expiry_date > start_date),
    CONSTRAINT chk_license_status CHECK (status IN ('ACTIVE', 'EXPIRED', 'SUSPENDED', 'REVOKED')),
    CONSTRAINT chk_license_type CHECK (license_type IN ('Type 1', 'Type 2', 'Type 3')),
    
    -- Foreign key constraint
    CONSTRAINT fk_kaksha_licenses_userid FOREIGN KEY (userid) 
        REFERENCES kaksha_users(userid) ON UPDATE CASCADE ON DELETE CASCADE
);

-- Create indexes for kaksha_users
CREATE INDEX IF NOT EXISTS idx_kaksha_users_email ON kaksha_users(email);
CREATE INDEX IF NOT EXISTS idx_kaksha_users_islicenseactive ON kaksha_users(islicenseactive);
CREATE INDEX IF NOT EXISTS idx_kaksha_users_lastlogin ON kaksha_users(lastlogin);

-- Create indexes for kaksha_licenses
CREATE INDEX IF NOT EXISTS idx_kaksha_licenses_expiry ON kaksha_licenses(expiry_date);
CREATE INDEX IF NOT EXISTS idx_kaksha_licenses_expiry_status ON kaksha_licenses(expiry_date, status);
CREATE INDEX IF NOT EXISTS idx_kaksha_licenses_product ON kaksha_licenses(product_name);
CREATE INDEX IF NOT EXISTS idx_kaksha_licenses_status ON kaksha_licenses(status);
CREATE INDEX IF NOT EXISTS idx_kaksha_licenses_type ON kaksha_licenses(license_type);
CREATE INDEX IF NOT EXISTS idx_kaksha_licenses_type_status ON kaksha_licenses(license_type, status);
CREATE INDEX IF NOT EXISTS idx_kaksha_licenses_userid ON kaksha_licenses(userid);
CREATE INDEX IF NOT EXISTS idx_kaksha_licenses_userid_status ON kaksha_licenses(userid, status);

-- Create updated_at trigger function
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE 'plpgsql';

-- Create function to update user license status
CREATE OR REPLACE FUNCTION update_user_license_status()
RETURNS TRIGGER AS $$
BEGIN
    -- Update user's islicenseactive status based on active licenses
    UPDATE kaksha_users 
    SET islicenseactive = EXISTS (
        SELECT 1 FROM kaksha_licenses 
        WHERE userid = NEW.userid 
        AND status = 'ACTIVE' 
        AND expiry_date > CURRENT_DATE
    )
    WHERE userid = NEW.userid;
    
    RETURN NEW;
END;
$$ LANGUAGE 'plpgsql';

-- Create triggers for updated_at
CREATE TRIGGER update_kaksha_users_updated_at 
    BEFORE UPDATE ON kaksha_users
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_kaksha_licenses_updated_at 
    BEFORE UPDATE ON kaksha_licenses
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Create trigger to update user license status
CREATE TRIGGER trg_update_user_license_status 
    AFTER INSERT OR UPDATE ON kaksha_licenses
    FOR EACH ROW EXECUTE FUNCTION update_user_license_status();

-- Insert sample users
INSERT INTO kaksha_users (userid, name, email, lastlogin, islicenseactive) VALUES
('user001', 'John Doe', 'john.doe@example.com', '2024-01-15 10:30:00', TRUE),
('user002', 'Jane Smith', 'jane.smith@example.com', '2024-01-14 14:45:00', TRUE),
('user003', 'Bob Johnson', 'bob.johnson@example.com', '2024-01-13 09:15:00', FALSE),
('user004', 'Alice Brown', 'alice.brown@example.com', '2024-01-12 16:20:00', TRUE),
('user005', 'Charlie Wilson', 'charlie.wilson@example.com', NULL, FALSE)
ON CONFLICT (userid) DO NOTHING;

-- Insert sample licenses
INSERT INTO kaksha_licenses (userid, license_type, start_date, expiry_date, product_name, status) VALUES
('user001', 'Type 1', '2024-01-01', '2024-12-31', 'Professional Suite', 'ACTIVE'),
('user002', 'Type 2', '2024-01-01', '2024-06-30', 'Analytics Pro', 'ACTIVE'),
('user003', 'Type 1', '2023-01-01', '2023-12-31', 'Basic Package', 'EXPIRED'),
('user004', 'Type 3', '2024-01-01', '2024-12-31', 'Enterprise Suite', 'ACTIVE'),
('user001', 'Type 2', '2024-02-01', '2024-11-30', 'Data Insights', 'ACTIVE')
ON CONFLICT DO NOTHING;

-- Update user license status for existing data
UPDATE kaksha_users 
SET islicenseactive = EXISTS (
    SELECT 1 FROM kaksha_licenses 
    WHERE kaksha_licenses.userid = kaksha_users.userid 
    AND status = 'ACTIVE' 
    AND expiry_date > CURRENT_DATE
);

-- Grant permissions
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO postgres;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO postgres;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO postgres;
