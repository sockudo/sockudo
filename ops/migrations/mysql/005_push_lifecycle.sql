-- Internal push lifecycle checkpoints and retirement evidence.
-- Apply before starting this binary. Current writers and cleanup use this schema automatically.
-- Binary rollback requires a compatible reader of retirement evidence; old readers must fail closed.
CREATE TABLE IF NOT EXISTS push_publish_lifecycle_scan (
    app_id VARCHAR(255) CHARACTER SET ascii COLLATE ascii_general_ci NOT NULL,
    publish_id VARCHAR(255) NOT NULL,
    scan_json TEXT NOT NULL,
    updated_at_ms BIGINT NOT NULL,
    PRIMARY KEY (app_id, publish_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
