#!/usr/bin/env bash
# Start the docker-compose.test.yml stack and block until every service answers.
#
# Every Rust test shard needs the same live backends, so this lives in one place
# instead of being copy-pasted into each CI job.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
compose_file="${repo_root}/docker-compose.test.yml"

container_id() {
  docker ps -q -f "name=$1"
}

redis_ready() { docker exec "$(container_id redis-test)" redis-cli ping; }
redis_cluster_ready() { docker exec "$(container_id redis-cluster)" redis-cli -p 7003 ping; }
nats_1_ready() { curl -f http://localhost:18222/varz; }
nats_2_ready() { curl -f http://localhost:18223/varz; }
mysql_ready() { mysql -h 127.0.0.1 -P 13306 -u root -proot123 -e "SELECT 1"; }
scylla_ready() { docker exec "$(container_id scylladb-test)" nodetool status | grep -q "UN"; }
postgres_ready() {
  PGPASSWORD=postgres123 psql -h 127.0.0.1 -p 15432 -U postgres -d sockudo_test -c "SELECT 1"
}
dynamodb_ready() { curl -s http://localhost:18000 | grep -q 'MissingAuthenticationToken'; }

wait_for() {
  local label="$1" attempts="$2" check="$3"
  for ((i = 1; i <= attempts; i++)); do
    if "$check" >/dev/null 2>&1; then
      echo "${label} is ready!"
      return 0
    fi
    echo "Waiting for ${label}... (${i}/${attempts})"
    sleep 2
  done
  echo "timed out waiting for ${label}" >&2
  return 1
}

docker compose -f "${compose_file}" up -d

echo "Waiting for services to be ready..."
wait_for "Redis test" 60 redis_ready
wait_for "Redis cluster" 60 redis_cluster_ready
wait_for "NATS-1" 60 nats_1_ready
wait_for "NATS-2" 60 nats_2_ready
wait_for "MySQL test" 60 mysql_ready
wait_for "ScyllaDB test" 120 scylla_ready
wait_for "PostgreSQL test" 60 postgres_ready
wait_for "DynamoDB Local" 30 dynamodb_ready

echo "=== Running Docker containers ==="
docker ps
echo "=== Port bindings ==="
for name in redis-test redis-cluster nats-1 nats-2 mysql-test scylladb-test postgres-test dynamodb-test; do
  docker port "$(container_id "$name")" || true
done
