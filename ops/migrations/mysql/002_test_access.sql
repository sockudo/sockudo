-- =============================================================================
-- Sockudo MySQL test access bootstrap
-- =============================================================================
-- Optional follow-up for local/test environments after applying
-- `001_fresh_schema.sql`.

CREATE USER IF NOT EXISTS 'sockudo'@'%' IDENTIFIED BY 'sockudo123';
CREATE USER IF NOT EXISTS 'sockudo'@'localhost' IDENTIFIED BY 'sockudo123';

GRANT ALL PRIVILEGES ON sockudo.* TO 'sockudo'@'%';
GRANT ALL PRIVILEGES ON sockudo.* TO 'sockudo'@'localhost';

FLUSH PRIVILEGES;
