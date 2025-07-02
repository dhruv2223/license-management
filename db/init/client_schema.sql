-- Client Database Schema Initialization Script
-- Contains user license requests and related functionality

-- Enable UUID extension for generating UUIDs
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- Create user_licence_requests table
CREATE TABLE IF NOT EXISTS user_licence_requests (
    id BIGSERIAL PRIMARY KEY,
    userid VARCHAR(50) NOT NULL,
    request_status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    license_type VARCHAR(20),
    product_name VARCHAR(100),
    requested_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    processed_at TIMESTAMP WITHOUT TIME ZONE,
    
    -- Check constraints
    CONSTRAINT chk_license_type CHECK (license_type IN ('Type 1', 'Type 2', 'Type 3')),
    CONSTRAINT chk_request_status CHECK (request_status IN ('PENDING', 'APPROVED', 'REJECTED', 'PROCESSING', 'FAILED'))
);

-- Create indexes for user_licence_requests
CREATE INDEX IF NOT EXISTS idx_user_licence_requests_requested_at ON user_licence_requests(requested_at);
CREATE INDEX IF NOT EXISTS idx_user_licence_requests_status ON user_licence_requests(request_status);
CREATE INDEX IF NOT EXISTS idx_user_licence_requests_userid ON user_licence_requests(userid);

-- Insert sample license requests
INSERT INTO user_licence_requests (userid, request_status, license_type, product_name, requested_at) VALUES
('user019', 'PENDING', 'Type 1', 'Professional Suite', '2024-01-16 09:00:00'),
('user020', 'PENDING', 'Type 2', 'Analytics Pro', '2024-01-16 10:30:00'),
('user021', 'PROCESSING', 'Type 1', 'Professional Suite', '2024-01-15 14:45:00'),
('user033', 'PENDING', 'Type 1', 'Educational Suite', '2024-01-14 16:20:00'),
('user035', 'APPROVED', 'Type 2', 'Healthcare Analytics', '2024-01-13 11:15:00'),
('user037', 'PENDING', 'Type 2', 'Government Analytics', '2024-01-12 13:30:00'),
('user044', 'REJECTED', 'Type 3', 'Enterprise Suite', '2024-01-11 15:45:00'),
('user046', 'PENDING', 'Type 1', 'Student Suite', '2024-01-10 12:00:00'),
('user047', 'PROCESSING', 'Type 1', 'Intern Package', '2024-01-09 10:15:00')
ON CONFLICT DO NOTHING;

-- Grant permissions
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO client_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO client_user;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO client_user;
