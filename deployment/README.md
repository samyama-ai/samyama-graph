# Deployment Configuration

Deployment configuration files for Samyama Graph Database sandbox environment.

## Files

- `api-gateway.service` - Systemd service for API Gateway
- `nginx-api-gateway.conf` - Nginx reverse proxy configuration

## Deployment Steps

### 1. Install Samyama

```bash
# Build Samyama
cargo build --release

# Copy binary
sudo cp target/release/samyama /usr/local/bin/

# Create systemd service (see samyama.service)
sudo systemctl enable samyama
sudo systemctl start samyama
```

### 2. Deploy API Gateway

```bash
# Install Node.js dependencies
cd api-gateway
npm install

# Copy systemd service
sudo cp deployment/api-gateway.service /etc/systemd/system/

# Enable and start service
sudo systemctl daemon-reload
sudo systemctl enable api-gateway
sudo systemctl start api-gateway

# Check status
sudo systemctl status api-gateway
```

### 3. Configure Nginx

```bash
# Copy nginx config
sudo cp deployment/nginx-api-gateway.conf /etc/nginx/sites-available/api-gateway

# Enable site
sudo ln -s /etc/nginx/sites-available/api-gateway /etc/nginx/sites-enabled/

# Test configuration
sudo nginx -t

# Reload nginx
sudo systemctl reload nginx
```

### 4. Setup SSL with Certbot

```bash
# Install certbot
sudo apt install certbot python3-certbot-nginx

# Obtain certificate
sudo certbot --nginx -d api.samyama.dev

# Auto-renewal is configured via systemd timer
sudo systemctl status certbot.timer
```

## Environment Variables

### API Gateway
- `PORT` - HTTP server port (default: 8080)
- `NODE_ENV` - production or development

### Samyama
Set via systemd service file or command line:
- `--host` - Bind address (default: 127.0.0.1)
- `--port` - RESP protocol port (default: 6379)

## Service Management

```bash
# Check service status
sudo systemctl status samyama
sudo systemctl status api-gateway
sudo systemctl status nginx

# View logs
sudo journalctl -u samyama -f
sudo journalctl -u api-gateway -f
sudo journalctl -u nginx -f

# Restart services
sudo systemctl restart samyama
sudo systemctl restart api-gateway
sudo systemctl reload nginx
```

## Data Loading

Load sandbox data at startup:

```bash
# Copy data files to /tmp/
scp clinical_nodes.tsv azureuser@vm:/tmp/
scp clinical_edges.tsv azureuser@vm:/tmp/
scp phegeni.tsv azureuser@vm:/tmp/
scp aact_*.tsv azureuser@vm:/tmp/

# Restart Samyama to load data
sudo systemctl restart samyama
```

## Architecture

```
Internet
    ↓
Nginx (Port 443, SSL)
    ↓
API Gateway (Port 8080, Node.js)
    ↓
Samyama (Port 6379, RESP Protocol)
```

## Security Checklist

- [ ] SSL certificate installed and auto-renewing
- [ ] Firewall configured (allow 80, 443; block 6379, 8080 from internet)
- [ ] API Gateway rate limiting enabled
- [ ] CORS configured for authorized domains only
- [ ] Write operations blocked in API Gateway
- [ ] Nginx security headers configured
- [ ] Services running as non-root user
- [ ] Regular backups configured

## Monitoring

Monitor service health:

```bash
# API Gateway health
curl https://api.samyama.dev/health

# Check graph statistics
redis-cli -p 6379 GRAPH.QUERY default "MATCH (n) RETURN count(n)"

# Check resource usage
htop
df -h
free -h
```

## Troubleshooting

**API Gateway won't start:**
```bash
sudo journalctl -u api-gateway -n 50
npm install  # Reinstall dependencies
node index.js  # Run manually to see errors
```

**Nginx 502 Bad Gateway:**
```bash
sudo systemctl status api-gateway  # Check if gateway is running
sudo nginx -t  # Test nginx config
sudo journalctl -u nginx -n 50
```

**Samyama data not loading:**
```bash
ls -la /tmp/*.tsv  # Check if data files exist
sudo journalctl -u samyama -n 100  # Check loading logs
```

## Backup and Recovery

**Backup:**
```bash
# Backup data
sudo systemctl stop samyama
tar -czf samyama-data-$(date +%Y%m%d).tar.gz ./samyama_data
sudo systemctl start samyama

# Backup configs
tar -czf configs-$(date +%Y%m%d).tar.gz \
  /etc/nginx/sites-available/api-gateway \
  /etc/systemd/system/api-gateway.service \
  /etc/systemd/system/samyama.service
```

**Restore:**
```bash
sudo systemctl stop samyama
tar -xzf samyama-data-YYYYMMDD.tar.gz
sudo systemctl start samyama
```
