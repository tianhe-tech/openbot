# Docker

## Build and Run

### 1. Create environment file

Copy `.env.example` to `.env` and configure your settings:

```bash
cp .env.example .env
```

Edit `.env` and set your API keys and credentials.

### 2. Start services

```bash
docker-compose up -d
```

This will start:
- **opencode**: OpenCode server on port 3000
- **openbot**: Gateway on port 8080

### 3. Check logs

```bash
# View all logs
docker-compose logs -f

# View specific service logs
docker-compose logs -f opencode
docker-compose logs -f openbot
```

### 4. Stop services

```bash
docker-compose down
```

## Environment Variables

### Required
- `OPENCODE_API_KEY`: API key for OpenCode server

### Optional
- `WECOM_*`: WeChat Work credentials
- `FEISHU_*`: Feishu credentials
- `DINGTALK_*`: DingTalk credentials

### Advanced
- `SERVER_ADDR`: Server address (default: :8080)
- `SERVER_READ_TIMEOUT`: Read timeout (default: 10s)
- `SERVER_WRITE_TIMEOUT`: Write timeout (default: 10s)
- `SERVER_SHUTDOWN_GRACE`: Graceful shutdown timeout (default: 15s)

## Build Only

To build the Docker image without running:

```bash
docker build -t openbot:latest .
```

## Development

For development with hot reload, use Docker Compose:

```bash
docker-compose up --build
```

## Health Checks

Both services have health checks:
- OpenCode: `http://localhost:3000/health`
- OpenBot: `http://localhost:8080/healthz`