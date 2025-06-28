# Processing Service

A Python-based Kafka consumer service that processes user activity events from the Ingestion-Service.

## Overview

This service consumes messages from the `user-activity-events` Kafka topic and processes them in real-time. It's designed to be a production-ready service with proper error handling, logging, and graceful shutdown.

## Architecture

```
Ingestion-Service (Go) → Kafka Topic → Processing-Service (Python)
     (Producer)           (user-activity-events)      (Consumer)
```

## Features

- Real-time message consumption from Kafka
- Snappy compression support for efficient message handling
- Automatic retry mechanism with configurable retry limits
- Graceful shutdown handling
- Production-ready logging with structured output
- Health checks for container orchestration
- Docker support with optimized image
- Environment-based configuration

## Quick Start

### Using Docker Compose (Recommended)

The Processing-Service is included in the main `docker-compose.yml`:

```bash
# Start all services including Processing-Service
docker-compose up -d

# View logs
docker-compose logs -f processing-service

# Stop all services
docker-compose down
```

### Manual Docker Build

```bash
# Build the image
docker build -t processing-service .

# Run the container
docker run -d \
  --name processing-service \
  --network kafka-network \
  -e KAFKA_BROKERS=kafka:29092 \
  -e KAFKA_TOPIC=user-activity-events \
  -e KAFKA_GROUP_ID=processing-service-group \
  processing-service
```

### Local Development

```bash
# Install dependencies
pip install -r requirements.txt

# Run the service
python kafka_consumer.py

# Or run with specific options
python kafka_consumer.py --read-from-beginning
```

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BROKERS` | `localhost:9092` | Kafka broker addresses (comma-separated) |
| `KAFKA_TOPIC` | `user-activity-events` | Kafka topic to consume from |
| `KAFKA_GROUP_ID` | `processing-service-group` | Consumer group ID |
| `AUTO_OFFSET_RESET` | `earliest` | Where to start reading messages |
| `ENABLE_AUTO_COMMIT` | `true` | Auto-commit offsets |
| `MAX_RETRIES` | `5` | Maximum connection retry attempts |
| `RETRY_DELAY` | `10` | Delay between retries (seconds) |
| `ENVIRONMENT` | `development` | Environment (development/production) |

### Example Environment File

```bash
# Copy and customize
cp env.example .env
```

## Message Processing

The service processes `EnrichedEvent` messages with the following structure:

```json
{
  "event_id": "uuid",
  "request_id": "uuid",
  "event_type": "page_view",
  "timestamp": "2024-01-15T10:30:45Z",
  "user_id": "user-123",
  "session_id": "session-456",
  "page_url": "https://example.com",
  "event_data": {
    "button_clicked": "submit",
    "form_data": {"field1": "value1"}
  },
  "client_info": {
    "user_agent": "Mozilla/5.0...",
    "screen_resolution": "1920x1080",
    "language": "en-US"
  },
  "service_info": {
    "service_name": "ingestion-service",
    "service_version": "1.0.0",
    "environment": "development"
  },
  "processing_info": {
    "received_at": "2024-01-15T10:30:45Z",
    "processed_at": "2024-01-15T10:30:45Z",
    "processing_ms": 5
  }
}
```

## Logging

The service uses structured logging with the following levels:
- **INFO**: Normal operation messages
- **WARNING**: Non-critical issues
- **ERROR**: Critical errors that may affect operation

Example log output:
```
2024-01-15 10:30:45 - ProcessingService - INFO - Starting Processing Service...
2024-01-15 10:30:45 - ProcessingService - INFO - Successfully connected to Kafka
2024-01-15 10:30:46 - ProcessingService - INFO - Processing event: page_view
```

## Health Checks

The service includes health checks for container orchestration:

```bash
# Check health
docker exec processing-service python -c "import sys; sys.exit(0)"
```

## Troubleshooting

### Connection Issues
- Verify Kafka is running and accessible
- Check network connectivity between services
- Ensure topic exists: `user-activity-events`

### Message Processing Issues
- Verify Snappy compression support is installed
- Check message format matches expected schema
- Review logs for specific error messages

### Performance Issues
- Monitor consumer lag in Kafka UI
- Adjust batch size and polling intervals
- Consider scaling with multiple consumer instances

## Development

### Adding New Features

1. **Message Processing Logic**: Extend the `display_message()` method in `kafka_consumer.py`
2. **Error Handling**: Add specific exception handling
3. **Configuration**: Add new environment variables as needed

### Testing

```bash
# Run with test data
python kafka_consumer.py --read-from-beginning

# Test error scenarios
docker-compose stop kafka
docker-compose logs processing-service  # Should show retry attempts
```

## Monitoring

### Key Metrics to Monitor
- **Consumer Lag**: Messages behind the latest offset
- **Error Rate**: Failed message processing attempts
- **Processing Time**: Time to process each message
- **Connection Status**: Kafka connection health

### Integration with Monitoring Tools
- **Prometheus**: Add metrics endpoint
- **Grafana**: Create dashboards for consumer metrics
- **ELK Stack**: Centralized logging and analysis 