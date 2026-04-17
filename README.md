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

3. **Run with Docker Compose**:
   ```bash
   docker-compose up --build
   ```

   This starts:
   - Kafka broker on port 9092
   - MinIO on ports 9000 (API) and 9001 (console)
   - Producer and Consumer services

4. **Access MinIO Console**:
   - URL: http://localhost:9001
   - Username: minioadmin
   - Password: minioadmin

## Scaling for Vast Data

### Horizontal Scaling
- **Multiple Consumers**: Scale consumer instances by running multiple containers:
  ```bash
  docker-compose up --scale consumer=3
  ```

- **Multiple Producers**: Similarly for producers if needed.

### Vertical Scaling
- **Increase Resources**: Allocate more CPU/memory to containers.
- **Kafka Cluster**: Add more Kafka brokers for higher throughput.

### Configuration Tuning
- **Batch Sizes**: Increase `PRODUCER_BATCH_SIZE` and `KAFKA_BATCH_SIZE` for higher throughput.
- **Partitions**: Increase `KAFKA_NUM_PARTITIONS` in docker-compose.yml.
- **Memory**: Increase `buffer_memory` in producer for larger buffers.

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