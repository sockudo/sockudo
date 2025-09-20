#!/bin/bash

echo "Starting Sockudo Unix Socket Test Environment..."

# Generate self-signed certificate for Nginx HTTPS
if [ ! -f /etc/ssl/certs/nginx.crt ]; then
    echo "Generating self-signed certificate for Nginx..."
    openssl req -x509 -nodes -days 365 -newkey rsa:2048 \
        -keyout /etc/ssl/private/nginx.key \
        -out /etc/ssl/certs/nginx.crt \
        -subj "/C=US/ST=State/L=City/O=Sockudo/CN=localhost"
    chmod 644 /etc/ssl/certs/nginx.crt
    chmod 600 /etc/ssl/private/nginx.key
fi

# Ensure socket directory exists with proper permissions
mkdir -p /var/run/sockudo
chmod 755 /var/run/sockudo

# Clean up any existing socket file
rm -f /var/run/sockudo/sockudo.sock

# Start supervisor to manage both processes
echo "Starting Supervisor to manage Nginx and Sockudo..."
exec /usr/bin/supervisord -c /etc/supervisor/conf.d/supervisord.conf