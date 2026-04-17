#!/usr/bin/env python3
"""
Load Testing Script for EventStreamX

This script performs comprehensive load testing of the EventStreamX platform,
measuring throughput, latency, and system stability under various loads.
"""

import asyncio
import json
import os
import statistics
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Dict, List, Optional

import aiohttp
import requests
from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer
from kafka.admin import NewTopic
from kafka.errors import KafkaError

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.event_generator import GenerateEvents


class LoadTester:
    def __init__(self):
        self.kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092,localhost:9094,localhost:9096")
        self.topic = os.getenv("KAFKA_TOPIC", "events")
        self.test_duration = int(os.getenv("TEST_DURATION_SECONDS", "300"))  # 5 minutes default
        self.target_throughput = int(os.getenv("TARGET_THROUGHPUT_EPS", "1000"))  # events per second
        self.num_producers = int(os.getenv("NUM_PRODUCERS", "4"))
        self.num_consumers = int(os.getenv("NUM_CONSUMERS", "3"))

        self.metrics = {
            "producer": {
                "events_sent": 0,
                "send_latency_ms": [],
                "errors": 0,
                "throughput_eps": 0
            },
            "consumer": {
                "events_received": 0,
                "processing_latency_ms": [],
                "errors": 0,
                "lag": 0,
                "throughput_eps": 0
            },
            "system": {
                "cpu_usage": [],
                "memory_usage": [],
                "disk_usage": []
            }
        }

        self.generator = GenerateEvents()
        self.admin_client = None
        self.producers = []
        self.consumers = []

    def setup_kafka_topic(self):
        """Create or reset the test topic with proper configuration."""
        try:
            self.admin_client = KafkaAdminClient(
                bootstrap_servers=self.kafka_servers.split(','),
                client_id='load-tester'
            )

            # Delete existing topic if it exists
            try:
                self.admin_client.delete_topics([self.topic])
                time.sleep(5)  # Wait for deletion
            except KafkaError:
                pass  # Topic might not exist

            # Create new topic with optimal settings
            topic = NewTopic(
                name=self.topic,
                num_partitions=12,
                replication_factor=3,
                topic_configs={
                    'retention.ms': str(24 * 60 * 60 * 1000),  # 24 hours
                    'segment.bytes': str(512 * 1024 * 1024),  # 512MB segments
                    'cleanup.policy': 'delete'
                }
            )

            self.admin_client.create_topics([topic])
            print(f"✓ Created topic '{self.topic}' with 12 partitions, RF=3")

        except Exception as e:
            print(f"✗ Failed to setup Kafka topic: {e}")
            raise

    def create_producers(self):
        """Create multiple producer instances."""
        for i in range(self.num_producers):
            try:
                producer = KafkaProducer(
                    bootstrap_servers=self.kafka_servers.split(','),
                    key_serializer=lambda k: k.encode("utf-8") if isinstance(k, str) else str(k).encode("utf-8"),
                    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
                    acks="all",
                    compression_type="snappy",
                    batch_size=65536,
                    linger_ms=10,
                    buffer_memory=67108864,  # 64MB
                    max_in_flight_requests_per_connection=5,
                    retries=3,
                    retry_backoff_ms=100,
                )
                self.producers.append(producer)
                print(f"✓ Created producer {i+1}/{self.num_producers}")
            except Exception as e:
                print(f"✗ Failed to create producer {i+1}: {e}")
                raise

    def create_consumers(self):
        """Create multiple consumer instances."""
        for i in range(self.num_consumers):
            try:
                consumer = KafkaConsumer(
                    self.topic,
                    group_id=f"load-test-group-{i}",
                    bootstrap_servers=self.kafka_servers.split(','),
                    auto_offset_reset="latest",
                    enable_auto_commit=True,
                    auto_commit_interval_ms=1000,
                    consumer_timeout_ms=1000,
                    max_poll_records=1000,
                    fetch_max_bytes=50 * 1024 * 1024,
                    max_partition_fetch_bytes=10 * 1024 * 1024,
                    session_timeout_ms=30000,
                    heartbeat_interval_ms=3000,
                )
                self.consumers.append(consumer)
                print(f"✓ Created consumer {i+1}/{self.num_consumers}")
            except Exception as e:
                print(f"✗ Failed to create consumer {i+1}: {e}")
                raise

    async def producer_worker(self, producer_id: int, target_eps: int):
        """Async producer worker that maintains target throughput."""
        producer = self.producers[producer_id]
        events_per_batch = max(1, target_eps // 10)  # Send in small batches
        interval = 1.0 / (target_eps / events_per_batch)

        start_time = time.time()
        sent_count = 0

        while time.time() - start_time < self.test_duration:
            batch_start = time.time()

            for _ in range(events_per_batch):
                event = self.generator.generate_event()
                send_start = time.time()

                try:
                    future = producer.send(
                        self.topic,
                        key=event["user_id"],
                        value=event
                    )
                    # Wait for send to complete
                    record_metadata = future.get(timeout=5)
                    send_latency = (time.time() - send_start) * 1000

                    self.metrics["producer"]["events_sent"] += 1
                    self.metrics["producer"]["send_latency_ms"].append(send_latency)
                    sent_count += 1

                except Exception as e:
                    self.metrics["producer"]["errors"] += 1
                    print(f"Producer {producer_id} error: {e}")

            # Maintain target rate
            batch_time = time.time() - batch_start
            if batch_time < interval:
                await asyncio.sleep(interval - batch_time)

        throughput = sent_count / self.test_duration
        print(f"Producer {producer_id}: {sent_count} events sent, {throughput:.1f} EPS")

    async def consumer_worker(self, consumer_id: int):
        """Async consumer worker that processes events."""
        consumer = self.consumers[consumer_id]
        start_time = time.time()
        received_count = 0

        while time.time() - start_time < self.test_duration:
            try:
                message_batch = consumer.poll(timeout_ms=1000, max_records=1000)

                for topic_partition, messages in message_batch.items():
                    for message in messages:
                        process_start = time.time()
                        # Simulate processing (deserialize, validate, etc.)
                        event = json.loads(message.value.decode('utf-8'))
                        process_latency = (time.time() - process_start) * 1000

                        self.metrics["consumer"]["events_received"] += 1
                        self.metrics["consumer"]["processing_latency_ms"].append(process_latency)
                        received_count += 1

            except Exception as e:
                self.metrics["consumer"]["errors"] += 1
                print(f"Consumer {consumer_id} error: {e}")

        throughput = received_count / self.test_duration
        print(f"Consumer {consumer_id}: {received_count} events received, {throughput:.1f} EPS")

    async def monitor_system_metrics(self):
        """Monitor system resource usage."""
        while True:
            try:
                # Get system metrics (simplified - in production use psutil)
                # For now, just track time-based metrics
                await asyncio.sleep(10)
            except asyncio.CancelledError:
                break

    async def run_load_test(self):
        """Run the complete load test."""
        print("🚀 Starting EventStreamX Load Test")
        print(f"Duration: {self.test_duration}s")
        print(f"Target Throughput: {self.target_throughput} EPS")
        print(f"Producers: {self.num_producers}")
        print(f"Consumers: {self.num_consumers}")
        print("-" * 50)

        # Setup
        self.setup_kafka_topic()
        self.create_producers()
        self.create_consumers()

        # Start monitoring
        monitor_task = asyncio.create_task(self.monitor_system_metrics())

        # Start producers and consumers
        producer_tasks = []
        consumer_tasks = []

        for i in range(self.num_producers):
            target_eps_per_producer = self.target_throughput // self.num_producers
            task = asyncio.create_task(self.producer_worker(i, target_eps_per_producer))
            producer_tasks.append(task)

        for i in range(self.num_consumers):
            task = asyncio.create_task(self.consumer_worker(i))
            consumer_tasks.append(task)

        # Wait for test duration
        await asyncio.sleep(self.test_duration)

        # Stop all tasks
        monitor_task.cancel()
        for task in producer_tasks + consumer_tasks:
            task.cancel()

        # Wait for cleanup
        await asyncio.gather(*producer_tasks, *consumer_tasks, return_exceptions=True)

        # Cleanup
        self.cleanup()

        # Report results
        self.report_results()

    def cleanup(self):
        """Clean up resources."""
        for producer in self.producers:
            try:
                producer.flush()
                producer.close()
            except:
                pass

        for consumer in self.consumers:
            try:
                consumer.close()
            except:
                pass

        if self.admin_client:
            try:
                self.admin_client.close()
            except:
                pass

    def report_results(self):
        """Generate comprehensive test report."""
        print("\n" + "="*60)
        print("📊 LOAD TEST RESULTS")
        print("="*60)

        # Producer metrics
        producer_events = self.metrics["producer"]["events_sent"]
        producer_errors = self.metrics["producer"]["errors"]
        if self.metrics["producer"]["send_latency_ms"]:
            producer_p50 = statistics.median(self.metrics["producer"]["send_latency_ms"])
            producer_p95 = statistics.quantiles(self.metrics["producer"]["send_latency_ms"], n=20)[18]  # 95th percentile
            producer_p99 = statistics.quantiles(self.metrics["producer"]["send_latency_ms"], n=100)[98]  # 99th percentile
        else:
            producer_p50 = producer_p95 = producer_p99 = 0

        producer_throughput = producer_events / self.test_duration

        print("\n📤 PRODUCER METRICS:")
        print(f"  Events Sent: {producer_events:,}")
        print(f"  Throughput: {producer_throughput:.1f} EPS")
        print(f"  Errors: {producer_errors}")
        print(f"  Send Latency - P50: {producer_p50:.1f}ms, P95: {producer_p95:.1f}ms, P99: {producer_p99:.1f}ms")

        # Consumer metrics
        consumer_events = self.metrics["consumer"]["events_received"]
        consumer_errors = self.metrics["consumer"]["errors"]
        if self.metrics["consumer"]["processing_latency_ms"]:
            consumer_p50 = statistics.median(self.metrics["consumer"]["processing_latency_ms"])
            consumer_p95 = statistics.quantiles(self.metrics["consumer"]["processing_latency_ms"], n=20)[18]
            consumer_p99 = statistics.quantiles(self.metrics["consumer"]["processing_latency_ms"], n=100)[98]
        else:
            consumer_p50 = consumer_p95 = consumer_p99 = 0

        consumer_throughput = consumer_events / self.test_duration

        print("\n📥 CONSUMER METRICS:")
        print(f"  Events Received: {consumer_events:,}")
        print(f"  Throughput: {consumer_throughput:.1f} EPS")
        print(f"  Errors: {consumer_errors}")
        print(f"  Processing Latency - P50: {consumer_p50:.1f}ms, P95: {consumer_p95:.1f}ms, P99: {consumer_p99:.1f}ms")

        # System health
        success_rate = (producer_events - producer_errors) / producer_events * 100 if producer_events > 0 else 0
        end_to_end_latency = producer_p95 + consumer_p95  # Rough estimate

        print("\n🏥 SYSTEM HEALTH:")
        print(f"  Success Rate: {success_rate:.2f}%")
        print(f"  Estimated End-to-End Latency (P95): {end_to_end_latency:.1f}ms")
        print(f"  Data Loss: {max(0, producer_events - consumer_events)} events")

        # Performance assessment
        print("\n🎯 PERFORMANCE ASSESSMENT:")
        if producer_throughput >= self.target_throughput * 0.9:
            print(f"  ✓ Throughput: Target met ({producer_throughput:.0f} >= {self.target_throughput * 0.9:.0f} EPS)")
        else:
            print(f"  ✗ Throughput: Target not met ({producer_throughput:.0f} < {self.target_throughput * 0.9:.0f} EPS)")

        if producer_p95 < 100:  # Less than 100ms
            print("  ✓ Latency: Acceptable (< 100ms P95)")
        else:
            print("  ⚠ Latency: High (> 100ms P95)")

        if success_rate > 99.9:
            print("  ✓ Reliability: Excellent (> 99.9% success)")
        elif success_rate > 99:
            print("  ⚠ Reliability: Good (> 99% success)")
        else:
            print("  ✗ Reliability: Poor (< 99% success)")


async def main():
    """Main entry point."""
    tester = LoadTester()
    try:
        await tester.run_load_test()
    except KeyboardInterrupt:
        print("\n🛑 Test interrupted by user")
        tester.cleanup()
    except Exception as e:
        print(f"\n💥 Test failed: {e}")
        tester.cleanup()
        raise


if __name__ == "__main__":
    asyncio.run(main())