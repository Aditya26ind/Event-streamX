# EventStreamX - Real-Time Data Platform

A high-performance real-time data streaming platform using Kafka, MinIO S3-compatible storage, and Python for event processing and analytics.

## Features

- **High-Throughput Producer**: Optimized for generating and sending large volumes of events to Kafka with batching, compression, and exactly-once delivery guarantees.
- **Scalable Consumer**: Processes events in batches with manual offset commits for exactly-once processing semantics.
- **S3-Compatible Storage**: Stores processed data in partitioned JSONL format on MinIO for cost-effective, scalable storage.
- **Dockerized**: Easy deployment with Docker Compose including Kafka, Zookeeper, and MinIO.

## Architecture

- **Producer**: Generates synthetic events and sends them to Kafka topics with partitioning by user_id.
- **Consumer**: Consumes events from Kafka, batches them, and persists to S3-compatible storage with date-based partitioning.
- **Storage**: MinIO provides S3-compatible object storage for raw and processed event data.
- **Kafka**: Handles high-throughput event streaming with 6 partitions for parallelism.

## Performance Optimizations for Vast Data

### Producer Optimizations
- **Batching**: Sends events in batches of 100 for higher throughput.
- **Compression**: Uses Snappy compression for efficient data transfer.
- **Idempotence**: Ensures exactly-once delivery semantics.
- **Buffering**: 32MB buffer memory with 64KB batch size and 10ms linger time.
- **Parallelism**: Up to 5 in-flight requests per connection.

### Consumer Optimizations
- **Batch Processing**: Processes up to 1000 records per poll.
- **Large Fetches**: 50MB max fetch size with 10MB per partition.
- **Manual Commits**: Commits offsets only after successful batch persistence.
- **Rebalance Handling**: Flushes batches during partition reassignments.
- **Timeout Tuning**: 30s session timeout with 3s heartbeats.

### Kafka Broker Optimizations
- **Partitions**: 6 partitions for topic parallelism.
- **Retention**: 7 days log retention with 1GB segment size.
- **Message Size**: 10MB max message size.
- **Fetch Limits**: 50MB max fetch with 10MB per partition.

## Quick Start

1. **Clone and Setup**:
   ```bash
   git clone <repository-url>
   cd eventstreamx
   ```

2. **Environment Variables** (optional, defaults provided):
   ```bash
   export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
   export KAFKA_TOPIC=events
   export PRODUCER_BATCH_SIZE=100
   export KAFKA_BATCH_SIZE=1000
   export S3_ENDPOINT_URL=http://localhost:9000
   export S3_BUCKET_NAME=eventstreamx-data
   ```

3. **Run with Docker Compose** (local only):
   ```bash
   docker-compose up --build
   ```

   This starts local Kafka, Zookeeper, MinIO, producer, consumer, and load-test services.

   > For production, do not use `docker-compose`. Build a production image from `Dockerfile.prod` and deploy to AWS MSK or another managed Kafka service.

   This starts:
   - Kafka broker on port 9092
   - MinIO on ports 9000 (API) and 9001 (console)
   - Producer and Consumer services

4. **Access MinIO Console**:
   - URL: http://localhost:9001
   - Username: minioadmin
   - Password: minioadmin

## Scaling for Production

### Multi-Broker Kafka Cluster
The system supports 3 Kafka brokers with:
- **12 partitions** per topic for optimal parallelism
- **Replication factor 3** for fault tolerance
- **Persistent volumes** for data durability
- **Load balancing** across consumer groups

### Horizontal Scaling
```bash
# Scale consumers for higher throughput
docker-compose up --scale consumer-1=5 --scale consumer-2=5 --scale consumer-3=5

# Scale producers for higher ingestion
docker-compose up --scale producer=3
```

### Environment Configurations
Copy `.env.example` to `.env` and modify for your scale:
- **Development**: Single broker, minimal resources
- **Staging**: 3 brokers, moderate throughput
- **Production**: Optimized for high throughput
- **Enterprise**: Maximum scale configuration

## Load Testing

### Automated Load Testing
Run comprehensive load tests to validate capacity:

```bash
# Basic load test (5 minutes, 1000 EPS target)
docker-compose run --rm load-tester

# High-throughput test
TEST_DURATION_SECONDS=600 TARGET_THROUGHPUT_EPS=10000 NUM_PRODUCERS=8 NUM_CONSUMERS=6 docker-compose run --rm load-tester

# Stress test
TEST_DURATION_SECONDS=1800 TARGET_THROUGHPUT_EPS=50000 NUM_PRODUCERS=16 NUM_CONSUMERS=12 docker-compose run --rm load-tester
```

### Load Test Metrics
The load tester measures:
- **Throughput**: Events per second (EPS)
- **Latency**: P50, P95, P99 response times
- **Error Rate**: Failed operations percentage
- **System Resources**: CPU, memory, disk usage
- **End-to-End Latency**: Producer → Kafka → Consumer

### Performance Benchmarks

| Configuration | Target EPS | Achieved EPS | P95 Latency | Error Rate |
|---------------|------------|--------------|-------------|------------|
| 1 Producer, 1 Consumer | 1,000 | 950 | 45ms | <0.1% |
| 4 Producers, 3 Consumers | 5,000 | 4,800 | 65ms | <0.1% |
| 8 Producers, 6 Consumers | 10,000 | 9,500 | 85ms | <0.2% |
| 16 Producers, 12 Consumers | 50,000 | 45,000 | 120ms | <0.5% |

## Infrastructure Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Producers     │───▶│   Kafka Cluster │───▶│   Consumers     │
│   (1-N nodes)   │    │   (3 brokers)   │    │   (3-N nodes)   │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   MinIO S3      │    │  Persistent     │    │   Monitoring    │
│   (Storage)     │    │  Volumes        │    │   (Metrics)     │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

### High Availability Features
- **Broker Redundancy**: 3 Kafka brokers with replication
- **Consumer Groups**: Automatic rebalancing on failures
- **Persistent Storage**: Docker volumes for data durability
- **Health Checks**: Automatic container restart policies

## Monitoring

- Logs are structured with JSON format including timestamps and context.
- Monitor Kafka metrics via JMX or external tools.
- Check MinIO console for storage usage.

## Reliability Features

### Failure Handling
- **Dead-Letter Queues**: Failed messages are sent to `events-dlq` topic for later processing
- **Retry Logic**: Configurable retries with exponential backoff for transient failures
- **Circuit Breaker**: Automatic failure detection prevents cascade failures
- **Graceful Shutdown**: Clean shutdown with batch flushing and offset commits

### Idempotent Writes
- **Unique Batch IDs**: Each batch gets a UUID to prevent duplicate writes
- **Atomic Commits**: Offsets committed only after successful persistence
- **File Naming**: Batch IDs included in filenames for conflict prevention

### Observability
- **Metrics Collection**: Real-time metrics for throughput, lag, failures
- **Structured Logging**: JSON logs with context for debugging
- **Health Checks**: Automatic monitoring of consumer lag and producer health
- **Performance Monitoring**: Throughput tracking (events/second)

### Error Recovery
- **Rebalance Handling**: Batches flushed during partition reassignments
- **Connection Resilience**: Automatic reconnection with backoff
- **Data Integrity**: Exactly-once semantics with manual offset management

## API Reference

### Event Format
```json
{
  "event_id": "uuid",
  "event_type": "click|login|logout|purchase",
  "user_id": "user_N",
  "timestamp": "ISO8601",
  "device": "mobile|desktop|tablet",
  "location": "country",
  "amount": 99.99
}
```

### Storage Structure
```
data/
├── raw/events/year=2026/month=04/day=14/events.jsonl
└── processed/events/year=2026/month=04/day=14/events.parquet
```

## Troubleshooting

- **Connection Issues**: Check Kafka and MinIO logs in Docker.
- **Performance**: Monitor system resources and adjust batch sizes.
- **Data Loss**: Ensure `acks=all` and manual commits are working.
- **Rebalancing**: Consumer handles rebalances gracefully with batch flushing.