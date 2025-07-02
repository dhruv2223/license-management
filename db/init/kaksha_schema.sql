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
-- Recently active users with licenses
('user006', 'Sarah Johnson', 'sarah.johnson@techcorp.com', '2024-01-16 08:45:00', TRUE),
('user007', 'Michael Chen', 'michael.chen@innovate.com', '2024-01-16 11:20:00', TRUE),
('user008', 'Emma Rodriguez', 'emma.rodriguez@dataflow.com', '2024-01-15 15:30:00', TRUE),
('user009', 'David Thompson', 'david.thompson@analytics.com', '2024-01-15 12:15:00', TRUE),
('user010', 'Lisa Wang', 'lisa.wang@enterprise.com', '2024-01-14 17:45:00', TRUE),

-- Users with expired or no licenses
('user011', 'Robert Miller', 'robert.miller@startup.com', '2024-01-13 10:30:00', FALSE),
('user012', 'Jennifer Davis', 'jennifer.davis@consulting.com', '2024-01-12 14:20:00', FALSE),
('user013', 'Kevin Park', 'kevin.park@freelance.com', '2024-01-11 09:15:00', FALSE),
('user014', 'Amanda Foster', 'amanda.foster@agency.com', '2024-01-10 16:45:00', FALSE),
('user015', 'James Wilson', 'james.wilson@smallbiz.com', '2024-01-09 13:30:00', FALSE),

-- Long-time inactive users
('user016', 'Maria Garcia', 'maria.garcia@oldtech.com', '2023-12-15 11:45:00', FALSE),
('user017', 'Thomas Lee', 'thomas.lee@legacy.com', '2023-11-28 14:30:00', FALSE),
('user018', 'Rachel Green', 'rachel.green@vintage.com', '2023-10-20 09:15:00', FALSE),

-- Recent signups with no login yet
('user019', 'Steven Kim', 'steven.kim@newco.com', NULL, FALSE),
('user020', 'Nicole White', 'nicole.white@fresh.com', NULL, FALSE),
('user021', 'Daniel Martinez', 'daniel.martinez@modern.com', NULL, FALSE),

-- Power users with recent activity
('user022', 'Catherine Liu', 'catherine.liu@bigdata.com', '2024-01-16 07:30:00', TRUE),
('user023', 'Andrew Scott', 'andrew.scott@cloudtech.com', '2024-01-16 13:45:00', TRUE),
('user024', 'Michelle Adams', 'michelle.adams@aitech.com', '2024-01-15 18:20:00', TRUE),
('user025', 'Christopher Taylor', 'christopher.taylor@automation.com', '2024-01-15 08:15:00', TRUE),

-- International users
('user026', 'Yuki Tanaka', 'yuki.tanaka@tokyo.co.jp', '2024-01-14 22:30:00', TRUE),
('user027', 'Hans Mueller', 'hans.mueller@berlin.de', '2024-01-14 15:45:00', TRUE),
('user028', 'Priya Sharma', 'priya.sharma@mumbai.in', '2024-01-13 04:30:00', TRUE),
('user029', 'Pierre Dubois', 'pierre.dubois@paris.fr', '2024-01-12 12:15:00', FALSE),
('user030', 'Sofia Rossi', 'sofia.rossi@milano.it', '2024-01-11 16:45:00', TRUE),

-- Educational sector users
('user031', 'Dr. Patricia Moore', 'patricia.moore@university.edu', '2024-01-14 10:00:00', TRUE),
('user032', 'Prof. Richard Clark', 'richard.clark@college.edu', '2024-01-13 14:30:00', TRUE),
('user033', 'Jessica Turner', 'jessica.turner@school.edu', '2024-01-12 11:15:00', FALSE),

-- Healthcare sector users
('user034', 'Dr. Mark Anderson', 'mark.anderson@hospital.com', '2024-01-15 07:45:00', TRUE),
('user035', 'Nurse Helen Cooper', 'helen.cooper@clinic.com', '2024-01-14 19:30:00', FALSE),

-- Government sector users
('user036', 'Agent John Roberts', 'john.roberts@gov.com', '2024-01-13 09:00:00', TRUE),
('user037', 'Mary Phillips', 'mary.phillips@city.gov', '2024-01-12 15:20:00', FALSE),

-- Startup ecosystem users
('user038', 'Alex Patel', 'alex.patel@unicorn.com', '2024-01-16 20:15:00', TRUE),
('user039', 'Zoe Campbell', 'zoe.campbell@scale.com', '2024-01-15 23:45:00', TRUE),
('user040', 'Ryan Murphy', 'ryan.murphy@growth.com', '2024-01-14 06:30:00', TRUE),

-- Senior executives
('user041', 'CEO Elizabeth Hart', 'elizabeth.hart@fortune500.com', '2024-01-15 08:00:00', TRUE),
('user042', 'CTO Benjamin Wright', 'benjamin.wright@techgiant.com', '2024-01-14 17:15:00', TRUE),
('user043', 'VP Susan Collins', 'susan.collins@enterprise.com', '2024-01-13 12:45:00', TRUE),

-- Freelancers and consultants
('user044', 'Freelancer Jake Morgan', 'jake.morgan@freelance.net', '2024-01-12 21:30:00', FALSE),
('user045', 'Consultant Anna Bell', 'anna.bell@consulting.pro', '2024-01-11 10:15:00', TRUE),

-- Recent graduates/interns
('user046', 'Graduate Sam Torres', 'sam.torres@newgrad.com', '2024-01-10 14:45:00', FALSE),
('user047', 'Intern Maya Singh', 'maya.singh@intern.com', '2024-01-09 16:20:00', FALSE),

-- Remote workers
('user048', 'Remote Worker Eric Johnson', 'eric.johnson@remote.com', '2024-01-16 03:30:00', TRUE),
('user049', 'Digital Nomad Lily Chen', 'lily.chen@nomad.com', '2024-01-15 19:45:00', TRUE),
('user050', 'Remote Manager Tom Brown', 'tom.brown@distributed.com', '2024-01-14 21:15:00', TRUE)

ON CONFLICT (userid) DO NOTHING;

-- Insert sample licenses

INSERT INTO kaksha_licenses (userid, license_type, start_date, expiry_date, product_name, status) VALUES
-- Recently active users with current licenses (EXTENDED EXPIRY DATES - 15 users after Sept 10, 2025)
('user006', 'Type 2', '2024-01-01', '2025-11-30', 'Analytics Pro', 'ACTIVE'),
('user007', 'Type 1', '2024-01-01', '2025-12-31', 'Professional Suite', 'ACTIVE'),
('user008', 'Type 3', '2024-01-01', '2025-10-15', 'Enterprise Suite', 'ACTIVE'),
('user009', 'Type 2', '2024-01-01', '2025-09-30', 'Data Insights', 'ACTIVE'),
('user010', 'Type 3', '2024-01-01', '2025-12-15', 'Enterprise Suite', 'ACTIVE'),

-- Power users with multiple licenses (EXTENDED EXPIRY DATES)
('user022', 'Type 1', '2024-01-01', '2025-11-15', 'Professional Suite', 'ACTIVE'),
('user022', 'Type 2', '2024-01-01', '2025-10-31', 'Analytics Pro', 'ACTIVE'),
('user023', 'Type 3', '2024-01-01', '2025-12-31', 'Enterprise Suite', 'ACTIVE'),
('user024', 'Type 2', '2024-01-01', '2025-09-15', 'AI Analytics', 'ACTIVE'),
('user025', 'Type 1', '2024-01-01', '2025-11-30', 'Automation Tools', 'ACTIVE'),

-- International users with active licenses (EXTENDED EXPIRY DATES)
('user026', 'Type 2', '2024-01-01', '2025-10-20', 'Analytics Pro', 'ACTIVE'),
('user027', 'Type 1', '2024-01-01', '2025-12-31', 'Professional Suite', 'ACTIVE'),
('user028', 'Type 3', '2024-01-01', '2025-09-25', 'Enterprise Suite', 'ACTIVE'),
('user030', 'Type 2', '2024-01-01', '2025-11-10', 'Analytics Pro', 'ACTIVE'),

-- Educational sector licenses (EXTENDED EXPIRY DATES)
('user031', 'Type 1', '2024-01-01', '2025-12-20', 'Educational Suite', 'ACTIVE'),

-- Remaining licenses with original 2024 expiry dates
('user032', 'Type 2', '2024-01-01', '2024-12-31', 'Research Analytics', 'ACTIVE'),
-- Healthcare sector licenses
('user034', 'Type 3', '2024-01-01', '2024-12-31', 'Healthcare Suite', 'ACTIVE'),
-- Government sector licenses
('user036', 'Type 2', '2024-01-01', '2024-12-31', 'Government Analytics', 'ACTIVE'),
-- Startup ecosystem licenses
('user038', 'Type 1', '2024-01-01', '2024-12-31', 'Startup Suite', 'ACTIVE'),
('user039', 'Type 2', '2024-01-01', '2024-12-31', 'Scale Analytics', 'ACTIVE'),
('user040', 'Type 3', '2024-01-01', '2024-12-31', 'Growth Suite', 'ACTIVE'),
-- Executive licenses
('user041', 'Type 3', '2024-01-01', '2024-12-31', 'Executive Suite', 'ACTIVE'),
('user042', 'Type 3', '2024-01-01', '2024-12-31', 'CTO Suite', 'ACTIVE'),
('user043', 'Type 2', '2024-01-01', '2024-12-31', 'VP Analytics', 'ACTIVE'),
-- Consultant license
('user045', 'Type 1', '2024-01-01', '2024-12-31', 'Consulting Tools', 'ACTIVE'),
-- Remote worker licenses
('user048', 'Type 2', '2024-01-01', '2024-12-31', 'Remote Analytics', 'ACTIVE'),
('user049', 'Type 1', '2024-01-01', '2024-12-31', 'Nomad Suite', 'ACTIVE'),
('user050', 'Type 3', '2024-01-01', '2024-12-31', 'Manager Suite', 'ACTIVE'),

-- Some expired licenses for testing
('user011', 'Type 1', '2023-01-01', '2023-12-31', 'Professional Suite', 'EXPIRED'),
('user012', 'Type 2', '2023-06-01', '2023-12-31', 'Analytics Pro', 'EXPIRED'),
('user013', 'Type 1', '2023-01-01', '2023-06-30', 'Basic Package', 'EXPIRED'),
('user016', 'Type 2', '2023-01-01', '2023-12-31', 'Analytics Pro', 'EXPIRED'),
('user017', 'Type 1', '2022-01-01', '2022-12-31', 'Professional Suite', 'EXPIRED'),

-- Some suspended licenses for testing
('user014', 'Type 2', '2024-01-01', '2024-12-31', 'Analytics Pro', 'SUSPENDED'),
('user029', 'Type 1', '2024-01-01', '2024-12-31', 'Professional Suite', 'SUSPENDED')

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
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO kaksha_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO kaksha_user;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO kaksha_user;
