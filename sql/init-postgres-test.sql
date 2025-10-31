-- =============================================================================
-- Sockudo PostgreSQL Test Database Initialization
-- =============================================================================
-- This script initializes the PostgreSQL database for testing

-- Create the sockudo_test database (if not already created by POSTGRES_DB env var)
-- Database is created by Docker environment variable, so we just need to ensure schema

-- Create apps table for testing
CREATE TABLE IF NOT EXISTS applications (
    id VARCHAR(255) NOT NULL PRIMARY KEY,
    key VARCHAR(255) NOT NULL UNIQUE,
    secret VARCHAR(255) NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,

    -- Connection limits
    max_connections INTEGER DEFAULT 1000,
    max_client_events_per_second INTEGER DEFAULT 100,
    max_read_requests_per_second INTEGER DEFAULT 100,
    max_backend_events_per_second INTEGER DEFAULT 100,

    -- Channel settings
    max_channel_name_length INTEGER DEFAULT 200,
    max_event_name_length INTEGER DEFAULT 200,
    max_event_payload_in_kb INTEGER DEFAULT 100,
    max_event_channels_at_once INTEGER DEFAULT 100,
    max_event_batch_size INTEGER DEFAULT 10,

    -- Presence settings
    max_presence_members_per_channel INTEGER DEFAULT 100,
    max_presence_member_size_in_kb INTEGER DEFAULT 2,

    -- Feature flags
    enable_client_messages BOOLEAN DEFAULT FALSE,
    enable_user_authentication BOOLEAN DEFAULT FALSE,
    enable_watchlist_events BOOLEAN DEFAULT FALSE,

    -- Webhooks (JSONB column for webhook configurations)
    webhooks JSONB DEFAULT NULL,

    -- Allowed origins (JSONB array for CORS-like origin validation)
    allowed_origins JSONB DEFAULT NULL,

    -- Timestamps
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_applications_key ON applications(key);
CREATE INDEX IF NOT EXISTS idx_applications_enabled ON applications(enabled);
CREATE INDEX IF NOT EXISTS idx_applications_created_at ON applications(created_at);

-- Insert a test application for development
INSERT INTO applications (
    id,
    key,
    secret,
    enabled,
    max_connections,
    max_client_events_per_second,
    enable_client_messages
) VALUES (
    'test-app-1',
    'test-app-key',
    'test-app-secret',
    TRUE,
    1000,
    100,
    TRUE
) ON CONFLICT (id) DO NOTHING;
