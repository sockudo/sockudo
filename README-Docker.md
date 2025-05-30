# Sockudo Docker Setup

This directory contains a complete Docker setup for running Sockudo WebSocket Server with all its dependencies.

## Quick Start

1. **Clone and prepare the environment:**
   ```bash
   git clone <sockudo-repo>
   cd sockudo
   cp .env.example .env
   ```

2. **Edit the `.env` file:**
   ```bash
   nano .env
   # Set your passwords and configuration
   ```

3. **Start the services:**
   ```bash
   docker-compose up -d
   ```

4. **Check the status:**
   ```bash
   docker-compose ps
   docker-compose logs sockudo
   ```

5. **Test the WebSocket connection:**
   ```bash
   curl http://localhost:6001/up/demo-app
   ```

## Architecture

The Docker setup includes:

- **Sockudo Server**: Main WebSocket server
- **Redis**: Cache, message broker, and rate limiting
- **Optional Services**: MySQL, Redis Cluster, Prometheus, Grafana

## Configuration

### Environment Variables

Key environment variables in `.env`:

```bash
# Security
REDIS_PASSWORD=your-secure-password
MYSQL_PASSWORD=your-mysql-password

# Application
SOCKUDO_DEFAULT_APP_KEY=your-app-key
SOCKUDO_DEFAULT_APP_SECRET=your-app-secret

# SSL (for production)
SSL_ENABLED=true
DOMAIN=your-domain.com
```

### Configuration File

The main configuration is in `config/config.json`. Key sections:

- **Drivers**: Choose between Redis, MySQL, memory-based storage
- **SSL**: Enable HTTPS for production
- **Rate Limiting**: Configure API and WebSocket rate limits
- **Metrics**: Enable Prometheus metrics collection

## Production Deployment

### SSL/HTTPS Setup

1. **Enable SSL in docker-compose.yml:**
   ```yaml
   environment:
     - SSL_ENABLED=true
     - SSL_CERT_PATH=/app/ssl/fullchain.pem
     - SSL_KEY_PATH=/app/ssl/privkey.pem
   ```

2. **Mount SSL certificates:**
   ```yaml
   volumes:
     - ./ssl:/app/ssl:ro
   ```

### High Availability

For production, uncomment the Redis Cluster configuration:

```yaml
# Uncomment redis-cluster-1, redis-cluster-2, redis-cluster-3
# Set ADAPTER_DRIVER=redis-cluster
# Set CACHE_DRIVER=redis-cluster
```

### Load Balancing

For multiple Sockudo instances, uncomment the nginx service:

```yaml
# nginx service with load balancing configuration
```

### Monitoring

Enable Prometheus and Grafana for comprehensive monitoring:

```yaml
# Uncomment prometheus and grafana services in docker-compose.yml
# Set MONITORING_ENABLED=true in .env.example
```

Access monitoring:
- Prometheus: http://localhost:9090
- Grafana: http://localhost:3000 (admin/admin123)
- Sockudo Metrics: http://localhost:9601/metrics

## Scaling Options

### Horizontal Scaling

1. **Multiple Sockudo Instances:**
   ```bash
   docker-compose up -d --scale sockudo=3
   ```

2. **Redis Cluster (High Availability):**
   ```yaml
   # Uncomment Redis cluster services
   environment:
     - ADAPTER_DRIVER=redis-cluster
     - REDIS_CLUSTER_NODES=redis-cluster-1:7000,redis-cluster-2:7001,redis-cluster-3:7002
   ```

3. **Load Balancer:**
   ```yaml
   # Uncomment nginx service for load balancing
   ```

### Vertical Scaling

Adjust resource limits in docker-compose.yml:

```yaml
deploy:
  resources:
    limits:
      memory: 1G
      cpus: '2.0'
    reservations:
      memory: 512M
      cpus: '1.0'
```

## Database Options

### MySQL (Persistent App Storage)

1. **Uncomment MySQL service in docker-compose.yml**
2. **Set environment variables:**
   ```bash
   APP_MANAGER_DRIVER=mysql
   DATABASE_MYSQL_HOST=mysql
   DATABASE_MYSQL_PASSWORD=your-password
   ```

3. **Create database schema:**
   ```sql
   CREATE TABLE applications (
     id VARCHAR(255) PRIMARY KEY,
     key VARCHAR(255) UNIQUE NOT NULL,
     secret VARCHAR(255) NOT NULL,
     enabled BOOLEAN DEFAULT TRUE,
     -- Add other columns as needed
   );
   ```

### DynamoDB (AWS)

```bash
APP_MANAGER_DRIVER=dynamodb
AWS_REGION=us-east-1
AWS_ACCESS_KEY_ID=your-key
AWS_SECRET_ACCESS_KEY=your-secret
```

## Networking

### Custom Network Configuration

The setup creates a custom bridge network `sockudo-network` with subnet `172.20.0.0/16`.

### External Access

- **WebSocket/API**: Port 6001
- **Metrics**: Port 9601
- **Redis**: Port 6379 (optional)
- **MySQL**: Port 3306 (optional)

### Firewall Configuration

For production, restrict access:

```bash
# Allow only necessary ports
ufw allow 80/tcp
ufw allow 443/tcp
ufw allow 6001/tcp
ufw deny 6379/tcp  # Redis - internal only
ufw deny 3306/tcp  # MySQL - internal only
```

## Health Checks

All services include health checks:

```bash
# Check service health
docker-compose ps

# View health check logs
docker inspect sockudo-server | grep Health -A 10
```

## Logging

### Log Configuration

Logs are configured with rotation:

```yaml
logging:
  driver: "json-file"
  options:
    max-size: "10m"
    max-file: "3"
```

### View Logs

```bash
# All services
docker-compose logs

# Specific service
docker-compose logs sockudo

# Follow logs
docker-compose logs -f sockudo

# Last 100 lines
docker-compose logs --tail=100 sockudo
```

### Log Aggregation

For production, consider using:
- ELK Stack (Elasticsearch, Logstash, Kibana)
- Fluentd
- Grafana Loki

## Backup and Recovery

### Redis Backup

```bash
# Manual backup
docker-compose exec redis redis-cli BGSAVE

# Automated backup script
./scripts/backup-redis.sh
```

### MySQL Backup

```bash
# Database dump
docker-compose exec mysql mysqldump -u sockudo -p sockudo > backup.sql

# Restore
docker-compose exec -T mysql mysql -u sockudo -p sockudo < backup.sql
```

### Configuration Backup

```bash
# Backup configuration
tar -czf sockudo-config-$(date +%Y%m%d).tar.gz config/ .env.example
```

## Troubleshooting

### Common Issues

1. **Permission Denied:**
   ```bash
   sudo chown -R $(id -u):$(id -g) logs/
   sudo chown -R $(id -u):$(id -g) config/
   ```

2. **Port Already in Use:**
   ```bash
   # Check what's using the port
   sudo lsof -i :6001
   
   # Change port in docker-compose.yml
   ports:
     - "6002:6001"
   ```

3. **Redis Connection Failed:**
   ```bash
   # Check Redis logs
   docker-compose logs redis
   
   # Test Redis connection
   docker-compose exec redis redis-cli ping
   ```

4. **Out of Memory:**
   ```bash
   # Check memory usage
   docker stats
   
   # Increase memory limits
   # Edit docker-compose.yml memory limits
   ```

### Debug Mode

Enable debug mode for troubleshooting:

```bash
# In .env.example file
DEBUG_MODE=true
LOG_LEVEL=debug

# Restart services
docker-compose restart sockudo
```

### Performance Monitoring

```bash
# Container resource usage
docker stats

# Detailed container info
docker-compose exec sockudo top

# Network connections
docker-compose exec sockudo netstat -tlnp
```

## Security Considerations

### Production Security

1. **Use strong passwords:**
   ```bash
   # Generate secure passwords
   openssl rand -base64 32
   ```

2. **Limit network exposure:**
   ```yaml
   # Remove port mappings for internal services
   # redis:
   #   ports: []  # No external access
   ```

3. **Enable SSL/TLS:**
   ```bash
   SSL_ENABLED=true
   # Use Let's Encrypt certificates
   ```

4. **Regular security updates:**
   ```bash
   # Update base images
   docker-compose pull
   docker-compose up -d
   ```

### Container Security

- Run as non-root user (configured in Dockerfile)
- Use official base images
- Regular security scanning:
  ```bash
  docker scan sockudo:latest
  ```

## Development vs Production

### Development Setup

```bash
# Use development overrides
docker-compose -f docker-compose.yml -f docker-compose.dev.yml up -d
```

### Production Setup

```bash
# Use production overrides
docker-compose -f docker-compose.yml -f docker-compose.prod.yml up -d
```

## Migration and Upgrades

### Upgrading Sockudo

```bash
# Pull latest changes
git pull origin main

# Rebuild image
docker-compose build sockudo

# Rolling update
docker-compose up -d sockudo
```

### Data Migration

```bash
# Export data before upgrade
./scripts/export-data.sh

# Import data after upgrade
./scripts/import-data.sh
```

## Support

For issues and questions:
- Check logs: `docker-compose logs`
- Review configuration: `config/config.json`
- Monitor metrics: `http://localhost:9601/metrics`
- Community support: [GitHub Issues](https://github.com/sockudo/sockudo/issues)