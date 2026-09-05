const REVOCATION_SNAPSHOT_FRESHNESS: Duration = Duration::from_millis(250);
const REVOCATION_SNAPSHOT_APP_LIMIT: usize = 32;
const REVOCATION_SNAPSHOT_CACHE_BYTES: usize = 2 * 1024 * 1024;
type RevocationSnapshotSlot = Arc<AsyncMutex<Option<RevocationSnapshot>>>;

struct RevocationSnapshot {
    // Measure age from scan start, not completion, so slow storage cannot extend
    // the freshness bound seen by an existing connection.
    started: TokioInstant,
    outcome: Result<Arc<Vec<AblyRevocationRecord>>, AblyAuthError>,
}

