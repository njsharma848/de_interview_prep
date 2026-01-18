# Ride-Hailing Real-Time Data Streaming Architecture
## Lambda Architecture with Medallion Data Lake Pattern

---

## Table of Contents
1. [System Overview](#system-overview)
2. [Lambda Architecture Explained](#lambda-architecture-explained)
3. [Streaming Path Implementation](#streaming-path-implementation)
4. [Batch Path Implementation](#batch-path-implementation)
5. [Medallion Architecture (Bronze/Silver/Gold)](#medallion-architecture)
6. [Production Code Implementation](#production-code-implementation)
7. [Data Quality & Monitoring](#data-quality--monitoring)
8. [Real-World Challenges & Solutions](#real-world-challenges--solutions)
9. [Interview Guide](#interview-guide)

---

## System Overview

### Business Context

**Ride-Hailing Platform Requirements:**
```
Business Needs:
- Real-time driver/rider matching (<3 seconds)
- Live trip tracking and ETA updates
- Real-time pricing (surge pricing)
- Near real-time analytics for business ops dashboard
- Historical data for ML models (demand forecasting, pricing optimization)
- Compliance & audit trail (24-hour retention minimum)

Scale:
- 500 million events/day
- Peak: 50,000 events/second (rush hour)
- Average event size: 2-5 KB
- Data retention: 7 years (compliance)
- Geographic distribution: Global (multi-region)
```

### High-Level Architecture

```
┌──────────────────────────────────────────────────────────────────────────┐
│                          DATA SOURCES                                    │
│  ┌─────────────────────┐         ┌─────────────────────┐               │
│  │    Driver App       │         │     Rider App       │               │
│  │  - Location (GPS)   │         │  - Trip requests    │               │
│  │  - Status updates   │         │  - Ratings          │               │
│  │  - Trip events      │         │  - Payments         │               │
│  └──────────┬──────────┘         └──────────┬──────────┘               │
└─────────────┼───────────────────────────────┼──────────────────────────┘
              │                               │
              │    (Generating 500M events/day - 2-5KB each)
              │                               │
              └───────────────┬───────────────┘
                              │
                              ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                     INGESTION LAYER                                      │
│                                                                          │
│  ┌────────────────────────────────────────────────────────────────┐    │
│  │              Amazon Kinesis Data Streams                        │    │
│  │  - Stream: ride-events-stream                                  │    │
│  │  - Shards: 100 (auto-scaling enabled)                          │    │
│  │  - Partition Key: driver_id OR rider_id                        │    │
│  │  - Retention: 24 hours (for replay)                            │    │
│  │  - Throughput: 100 MB/sec (peak: 250 MB/sec with scaling)     │    │
│  └────────────────────────────────────────────────────────────────┘    │
│                                                                          │
│  Why Kinesis (vs Kafka MSK):                                            │
│  ✓ Fully managed (auto-scaling, no ops overhead)                       │
│  ✓ Native AWS integration (Lambda, Firehose, Analytics)                │
│  ✓ Pay per shard-hour (cost-effective for variable load)               │
│  ✓ At our scale: $2,500/month vs MSK $4,500/month                      │
└──────────────────────────────────────────────────────────────────────────┘
                              │
                              ├──────────────────────────────────────────┐
                              │                                          │
                    ┌─────────▼────────┐                    ┌───────────▼──────────┐
                    │  STREAMING PATH  │                    │     BATCH PATH       │
                    │   (Hot Path)     │                    │    (Cold Path)       │
                    │   <95ms latency  │                    │  Every 48 hours      │
                    └──────────────────┘                    └──────────────────────┘

```

---

## Lambda Architecture Explained

### Why Lambda Architecture for Ride-Hailing?

**The Challenge:**
```
Trade-off Decision:
┌──────────────┬─────────────────┬─────────────────────────────────────┐
│ Requirement  │ Streaming Only  │ Batch Only                          │
├──────────────┼─────────────────┼─────────────────────────────────────┤
│ Latency      │ ✅ Sub-second   │ ❌ Hours/days                       │
│ Completeness │ ❌ 99.5%        │ ✅ 100% (can reprocess)             │
│ Cost         │ ❌ High ($$$)   │ ✅ Low ($)                          │
│ Complexity   │ ✅ Simple       │ ✅ Simple                           │
│ Accuracy     │ ❌ Approximate  │ ✅ Exact (can correct)              │
└──────────────┴─────────────────┴─────────────────────────────────────┘

Solution: Lambda Architecture = Streaming + Batch (Best of Both)
```

### Lambda Architecture Components

```
┌────────────────────────────────────────────────────────────────────┐
│                      LAMBDA ARCHITECTURE                           │
│                                                                    │
│  ┌────────────────────────────────────────────────────────────┐  │
│  │  HOT PATH (Streaming) - Speed Layer                        │  │
│  │  Purpose: Real-time, approximate results                   │  │
│  │  Latency: <95ms                                            │  │
│  │  Accuracy: ~99.5% (some events may be lost/delayed)       │  │
│  │  Use Cases:                                                │  │
│  │  - Live driver availability                                │  │
│  │  - Real-time ETA                                           │  │
│  │  - Surge pricing                                           │  │
│  │  - Active trip count                                       │  │
│  └────────────────────────────────────────────────────────────┘  │
│                              │                                     │
│                              ▼                                     │
│  ┌────────────────────────────────────────────────────────────┐  │
│  │  COLD PATH (Batch) - Batch Layer                           │  │
│  │  Purpose: Complete, accurate results                       │  │
│  │  Latency: 48 hours                                         │  │
│  │  Accuracy: 100% (reprocesses all data)                     │  │
│  │  Use Cases:                                                │  │
│  │  - Financial reconciliation                                │  │
│  │  - ML model training                                       │  │
│  │  - Historical analytics                                    │  │
│  │  - Compliance reports                                      │  │
│  └────────────────────────────────────────────────────────────┘  │
│                              │                                     │
│                              ▼                                     │
│  ┌────────────────────────────────────────────────────────────┐  │
│  │  SERVING LAYER - Merge Results                             │  │
│  │  - Last 48 hours: From streaming (fast, approximate)       │  │
│  │  - Older than 48 hours: From batch (complete, accurate)    │  │
│  │  - Query combines both for complete view                   │  │
│  └────────────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────────────┘
```

### Concrete Example

**Use Case: "How many active trips right now?"**

```python
# Streaming Layer (Real-time, last 48 hours)
streaming_active_trips = query_dynamodb(
    table="real_time_trips",
    filter="status = 'ACTIVE' AND start_time > now() - 48h"
)
# Result: 12,547 trips (99.5% accurate, <100ms response)

# Batch Layer (Historical, >48 hours ago)
batch_historical_trips = query_redshift(
    table="historical_trips",
    filter="status = 'ACTIVE' AND start_time <= now() - 48h"
)
# Result: 0 trips (none from >48h ago are still active)

# Serving Layer (Merged result)
total_active_trips = streaming_active_trips + batch_historical_trips
# Result: 12,547 active trips

# Why this works:
# - Trips don't last >48 hours (avg: 20 minutes)
# - Real-time view sufficient for active trips
# - Batch handles long-tail corrections
```

---

## Streaming Path Implementation

### Architecture

```
┌──────────────────────────────────────────────────────────────────────────┐
│                         STREAMING PATH (HOT PATH)                        │
│                                                                          │
│  Kinesis Data Streams                                                    │
│         │                                                                │
│         ├─────► Consumer 1: AWS Lambda (95ms latency)                   │
│         │       ├─── Data Validation                                    │
│         │       ├─── Simple Transformations                             │
│         │       ├─── Write to DynamoDB (hot storage)                    │
│         │       └─── Dump to S3 (backup)                                │
│         │                                                                │
│         └─────► Consumer 2: Apache Flink on EMR (for complex logic)    │
│                 ├─── Windowed Aggregations                              │
│                 ├─── Real-time Metrics                                  │
│                 ├─── Write to S3 (30-second micro-batches)             │
│                 └─── Optional: Kinesis Data Firehose to Redshift        │
│                                                                          │
│  Data Flow Timeline:                                                    │
│  Event Generated → Kinesis (5ms) → Lambda (30ms) → DynamoDB (10ms)     │
│  Total: 45-95ms latency                                                 │
└──────────────────────────────────────────────────────────────────────────┘
```

### Container 1: AWS Lambda for Streaming Processing

**When to use Lambda (vs Flink):**
```
Use Lambda when:
✓ Simple transformations (parse, validate, enrich)
✓ Low/variable throughput (<10K events/sec per function)
✓ Stateless processing
✓ Want serverless (no cluster management)

Use Flink when:
✓ Complex windowed aggregations
✓ Stateful operations (sessionization, deduplication)
✓ High throughput (>10K events/sec sustained)
✓ Need exactly-once guarantees
✓ Complex event processing (CEP)

Our approach: BOTH
- Lambda: Simple event processing (95% of events)
- Flink: Complex analytics (5% of events)
```

**Lambda Implementation:**

```python
"""
lambda_streaming_processor.py
AWS Lambda function for real-time ride event processing

Triggered by: Kinesis Data Streams
Execution time: <3 seconds (timeout)
Memory: 1024 MB
Concurrency: 100 (auto-scaling)
"""

import json
import boto3
import os
from datetime import datetime, timezone
from decimal import Decimal

# Initialize clients (outside handler for connection reuse)
dynamodb = boto3.resource('dynamodb')
s3 = boto3.client('s3')

# DynamoDB tables
active_trips_table = dynamodb.Table(os.environ['ACTIVE_TRIPS_TABLE'])
driver_status_table = dynamodb.Table(os.environ['DRIVER_STATUS_TABLE'])

# S3 backup bucket
BACKUP_BUCKET = os.environ['BACKUP_BUCKET']

def lambda_handler(event, context):
    """
    Process Kinesis stream records
    
    Event structure:
    {
        'Records': [
            {
                'kinesis': {
                    'data': '<base64_encoded_json>',
                    'sequenceNumber': '...',
                    'partitionKey': 'driver_123'
                },
                'eventID': '...',
                'eventSourceARN': 'arn:aws:kinesis:...'
            }
        ]
    }
    """
    
    print(f"Processing {len(event['Records'])} records")
    
    processed_count = 0
    failed_count = 0
    backup_records = []
    
    for record in event['Records']:
        try:
            # Decode Kinesis record
            payload = json.loads(
                boto3.client('kinesis').
                decode_base64(record['kinesis']['data'])
            )
            
            # Validate schema
            if not validate_event_schema(payload):
                print(f"Invalid schema: {payload}")
                failed_count += 1
                continue
            
            # Route by event type
            event_type = payload.get('event_type')
            
            if event_type == 'TRIP_STARTED':
                process_trip_started(payload)
            elif event_type == 'TRIP_COMPLETED':
                process_trip_completed(payload)
            elif event_type == 'DRIVER_LOCATION_UPDATE':
                process_driver_location(payload)
            elif event_type == 'RIDER_REQUEST':
                process_rider_request(payload)
            else:
                print(f"Unknown event type: {event_type}")
            
            # Collect for backup
            backup_records.append(payload)
            processed_count += 1
            
        except Exception as e:
            print(f"Error processing record: {str(e)}")
            failed_count += 1
    
    # Batch write to S3 for backup (async)
    if backup_records:
        dump_to_s3_backup(backup_records)
    
    print(f"Processed: {processed_count}, Failed: {failed_count}")
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'processed': processed_count,
            'failed': failed_count
        })
    }

def validate_event_schema(event):
    """Validate event has required fields"""
    required_fields = ['event_id', 'event_type', 'timestamp']
    return all(field in event for field in required_fields)

def process_trip_started(event):
    """
    Process trip start event
    Write to DynamoDB for real-time queries
    """
    
    trip_data = {
        'trip_id': event['trip_id'],
        'driver_id': event['driver_id'],
        'rider_id': event['rider_id'],
        'status': 'ACTIVE',
        'start_time': event['timestamp'],
        'start_location': {
            'lat': Decimal(str(event['start_lat'])),
            'lon': Decimal(str(event['start_lon']))
        },
        'estimated_duration': event.get('estimated_duration', 0),
        'estimated_fare': Decimal(str(event.get('estimated_fare', 0))),
        'ttl': int(datetime.now(timezone.utc).timestamp()) + 172800  # 48 hours
    }
    
    # Write to DynamoDB (hot storage for active trips)
    active_trips_table.put_item(Item=trip_data)
    
    # Update driver status
    driver_status_table.update_item(
        Key={'driver_id': event['driver_id']},
        UpdateExpression='SET #status = :status, current_trip_id = :trip_id',
        ExpressionAttributeNames={'#status': 'status'},
        ExpressionAttributeValues={
            ':status': 'ON_TRIP',
            ':trip_id': event['trip_id']
        }
    )
    
    print(f"Trip started: {event['trip_id']}")

def process_trip_completed(event):
    """
    Process trip completion
    Update DynamoDB and prepare for batch processing
    """
    
    # Update trip status
    active_trips_table.update_item(
        Key={'trip_id': event['trip_id']},
        UpdateExpression='''
            SET #status = :status,
                end_time = :end_time,
                end_location = :end_location,
                actual_fare = :actual_fare,
                actual_duration = :actual_duration,
                rating = :rating
        ''',
        ExpressionAttributeNames={'#status': 'status'},
        ExpressionAttributeValues={
            ':status': 'COMPLETED',
            ':end_time': event['timestamp'],
            ':end_location': {
                'lat': Decimal(str(event['end_lat'])),
                'lon': Decimal(str(event['end_lon']))
            },
            ':actual_fare': Decimal(str(event['actual_fare'])),
            ':actual_duration': event['actual_duration'],
            ':rating': event.get('rating', 0)
        }
    )
    
    # Free up driver
    driver_status_table.update_item(
        Key={'driver_id': event['driver_id']},
        UpdateExpression='''
            SET #status = :status,
                current_trip_id = :null,
                last_trip_end_time = :end_time
        ''',
        ExpressionAttributeNames={'#status': 'status'},
        ExpressionAttributeValues={
            ':status': 'AVAILABLE',
            ':null': None,
            ':end_time': event['timestamp']
        }
    )
    
    print(f"Trip completed: {event['trip_id']}")

def process_driver_location(event):
    """
    Process driver location update
    High frequency (~every 5 seconds per driver)
    """
    
    # Update driver location in DynamoDB
    driver_status_table.update_item(
        Key={'driver_id': event['driver_id']},
        UpdateExpression='''
            SET current_location = :location,
                last_update_time = :timestamp
        ''',
        ExpressionAttributeValues={
            ':location': {
                'lat': Decimal(str(event['latitude'])),
                'lon': Decimal(str(event['longitude']))
            },
            ':timestamp': event['timestamp']
        }
    )
    
    # Note: Not logging each location update (too verbose)

def process_rider_request(event):
    """
    Process rider trip request
    Match with available drivers (simplified)
    """
    
    # In production, this would trigger a matching algorithm
    # For now, just log the request
    print(f"Rider request: {event['rider_id']} from ({event['pickup_lat']}, {event['pickup_lon']})")
    
    # Store request for batch processing and ML training
    # Actual matching happens via separate service

def dump_to_s3_backup(records):
    """
    Dump processed records to S3 for batch layer
    Partitioned by date and hour for efficient batch processing
    """
    
    now = datetime.now(timezone.utc)
    s3_key = (
        f"streaming-backup/"
        f"year={now.year}/"
        f"month={now.month:02d}/"
        f"day={now.day:02d}/"
        f"hour={now.hour:02d}/"
        f"{now.timestamp()}.json"
    )
    
    try:
        s3.put_object(
            Bucket=BACKUP_BUCKET,
            Key=s3_key,
            Body=json.dumps(records, default=str),
            ContentType='application/json'
        )
        print(f"Dumped {len(records)} records to s3://{BACKUP_BUCKET}/{s3_key}")
    except Exception as e:
        print(f"Failed to dump to S3: {str(e)}")

# CloudWatch custom metrics
def publish_custom_metrics(processed, failed):
    """Publish processing metrics to CloudWatch"""
    cloudwatch = boto3.client('cloudwatch')
    
    cloudwatch.put_metric_data(
        Namespace='RideHailing/Streaming',
        MetricData=[
            {
                'MetricName': 'ProcessedEvents',
                'Value': processed,
                'Unit': 'Count',
                'Timestamp': datetime.now(timezone.utc)
            },
            {
                'MetricName': 'FailedEvents',
                'Value': failed,
                'Unit': 'Count',
                'Timestamp': datetime.now(timezone.utc)
            }
        ]
    )
```

### Container 2: Apache Flink for Complex Stream Processing

**When we use Flink in our architecture:**

```
Flink Use Cases in Ride-Hailing:
1. Windowed aggregations (surge pricing calculations)
2. Session windows (driver shift analysis)
3. Complex joins (trip + payment + rating events)
4. Exactly-once semantics for financial data
5. Pattern matching (fraud detection)

Example: Surge Pricing Calculation
- Input: Driver location updates + Rider requests
- Window: 5-minute tumbling windows per geo-zone
- Output: Demand/supply ratio → Surge multiplier
- Requirement: Exactly-once (can't double-charge)
```

**Flink Implementation:**

```python
"""
flink_streaming_job.py
Apache Flink job for complex stream processing

Deployment: EMR Cluster with Flink 1.18
Resources: 5 Task Managers, 4 cores each, 16GB RAM
Parallelism: 20 (matches Kinesis shards)
"""

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kinesis import FlinkKinesisConsumer
from pyflink.datastream.functions import MapFunction, FlatMapFunction
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.window import TumblingProcessingTimeWindows
from pyflink.common.time import Time
import json

class RideEventParser(FlatMapFunction):
    """Parse and validate ride events from Kinesis"""
    
    def flat_map(self, value):
        try:
            event = json.loads(value)
            
            # Validate required fields
            if all(k in event for k in ['event_id', 'event_type', 'timestamp']):
                yield event
        except json.JSONDecodeError:
            # Log but don't crash
            print(f"Invalid JSON: {value}")

class SurgePricingCalculator(MapFunction):
    """
    Calculate surge pricing per geo-zone
    
    Logic:
    - Count rider requests in 5-min window
    - Count available drivers in same window
    - Calculate ratio: requests / drivers
    - Apply surge multiplier based on ratio
    """
    
    def map(self, value):
        geo_zone, window, events = value
        
        # Count requests and drivers
        rider_requests = sum(1 for e in events if e['event_type'] == 'RIDER_REQUEST')
        available_drivers = sum(1 for e in events if e['event_type'] == 'DRIVER_AVAILABLE')
        
        # Calculate demand/supply ratio
        ratio = rider_requests / max(available_drivers, 1)
        
        # Surge multiplier logic
        if ratio < 1.2:
            surge_multiplier = 1.0
        elif ratio < 2.0:
            surge_multiplier = 1.5
        elif ratio < 3.0:
            surge_multiplier = 2.0
        else:
            surge_multiplier = 2.5
        
        return {
            'geo_zone': geo_zone,
            'window_start': window.start,
            'window_end': window.end,
            'rider_requests': rider_requests,
            'available_drivers': available_drivers,
            'demand_supply_ratio': ratio,
            'surge_multiplier': surge_multiplier
        }

def main():
    """Main Flink streaming job"""
    
    # Create execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(20)  # Match Kinesis shards
    
    # Enable checkpointing (exactly-once)
    env.enable_checkpointing(60000)  # Every 60 seconds
    
    # Configure Kinesis consumer
    kinesis_consumer_config = {
        'aws.region': 'us-east-1',
        'stream.name': 'ride-events-stream',
        'flink.stream.initpos': 'LATEST',
        'aws.credentials.provider': 'AUTO'
    }
    
    kinesis_consumer = FlinkKinesisConsumer(
        'ride-events-stream',
        SimpleStringSchema(),
        kinesis_consumer_config
    )
    
    # Build streaming pipeline
    stream = env.add_source(kinesis_consumer) \
        .flat_map(RideEventParser()) \
        .key_by(lambda event: event.get('geo_zone', 'UNKNOWN')) \
        .window(TumblingProcessingTimeWindows.of(Time.minutes(5))) \
        .process(SurgePricingCalculator()) \
        .print()  # In production: write to DynamoDB/Redis
    
    # Execute
    env.execute("Ride Hailing Stream Processing")

if __name__ == '__main__':
    main()
```

---

## Batch Path Implementation

### Architecture

```
┌──────────────────────────────────────────────────────────────────────────┐
│                          BATCH PATH (COLD PATH)                          │
│                     Every 48 Hours - Complete Reprocessing               │
│                                                                          │
│  ┌────────────────────────────────────────────────────────────────┐    │
│  │  S3 Landing Layer                                              │    │
│  │  - Raw events from Kinesis Firehose                           │    │
│  │  - Partitioned: year/month/day/hour                           │    │
│  │  - Format: JSON (from stream), Parquet (optimized)            │    │
│  │  - Retention: 48 hours → Move to Bronze                       │    │
│  └─────────────────────────┬──────────────────────────────────────┘    │
│                            │                                            │
│                            │ (Every 48 hours)                           │
│                            ▼                                            │
│  ┌────────────────────────────────────────────────────────────────┐    │
│  │  Apache Spark on EMR                                           │    │
│  │  - Read last 48 hours from Landing                            │    │
│  │  - Validate, deduplicate, enrich                              │    │
│  │  - Write to Bronze Layer                                      │    │
│  └─────────────────────────┬──────────────────────────────────────┘    │
│                            │                                            │
│                            ▼                                            │
│  ┌────────────────────────────────────────────────────────────────┐    │
│  │  Bronze Layer (S3)                                             │    │
│  │  - Raw, cleaned events (Parquet, Delta Lake format)           │    │
│  │  - No transformations, just validation                         │    │
│  │  - Partitioned: year/month/day                                 │    │
│  └─────────────────────────┬──────────────────────────────────────┘    │
│                            │                                            │
│                            │ (Continuous transformations)               │
│                            ▼                                            │
│  ┌────────────────────────────────────────────────────────────────┐    │
│  │  Silver Layer (S3)                                             │    │
│  │  - Business logic applied                                      │    │
│  │  - Aggregations, joins, denormalization                        │    │
│  │  - Trip summaries, driver metrics, rider history               │    │
│  │  - Format: Delta Lake (ACID transactions, time travel)         │    │
│  └─────────────────────────┬──────────────────────────────────────┘    │
│                            │                                            │
│                            │ (Final aggregations)                       │
│                            ▼                                            │
│  ┌────────────────────────────────────────────────────────────────┐    │
│  │  Gold Layer (S3 + Redshift/Snowflake)                         │    │
│  │  - Final, business-ready datasets                             │    │
│  │  - Dimensional models (star schema)                           │    │
│  │  - Ready for BI tools (QuickSight, Tableau)                   │    │
│  │  - ML features for model training                             │    │
│  └────────────────────────────────────────────────────────────────┘    │
└──────────────────────────────────────────────────────────────────────────┘
```

### Spark Batch Job: Landing → Bronze

```python
"""
batch_landing_to_bronze.py
Spark job to process landing layer data to bronze

Trigger: EventBridge rule (every 48 hours)
Cluster: EMR with Spark 3.5, 10 nodes (m5.2xlarge)
Processing time: ~2 hours for 48 hours of data
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, lit,
    year, month, dayofmonth, hour,
    current_timestamp, sha2, concat_ws
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from datetime import datetime, timedelta
import sys

class LandingToBronzeProcessor:
    """
    Process raw events from landing to bronze layer
    
    Bronze Layer Characteristics:
    - Raw data with minimal transformations
    - Schema validation
    - Deduplication
    - Add metadata (processing timestamp, hash)
    - No business logic
    """
    
    def __init__(self, config):
        self.config = config
        self.spark = self._create_spark_session()
    
    def _create_spark_session(self):
        """Initialize Spark with optimizations for large-scale batch processing"""
        
        spark = SparkSession.builder \
            .appName("RideHailing-LandingToBronze") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.files.maxPartitionBytes", "134217728") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.sql.parquet.compression.codec", "snappy") \
            .config("spark.sql.shuffle.partitions", "200") \
            .enableHiveSupport() \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("WARN")
        
        print("✅ Spark Session Created")
        return spark
    
    def define_schema(self):
        """Define schema for ride events"""
        
        return StructType([
            StructField("event_id", StringType(), False),
            StructField("event_type", StringType(), False),
            StructField("timestamp", StringType(), False),
            StructField("trip_id", StringType(), True),
            StructField("driver_id", StringType(), True),
            StructField("rider_id", StringType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("status", StringType(), True),
            StructField("fare", DoubleType(), True),
            StructField("duration", IntegerType(), True)
        ])
    
    def read_from_landing(self, start_date, end_date):
        """
        Read raw events from S3 landing layer
        
        Path structure: s3://bucket/landing/year=YYYY/month=MM/day=DD/hour=HH/
        """
        
        landing_path = f"{self.config['landing_bucket']}/landing/"
        
        print(f"📖 Reading from landing layer: {landing_path}")
        print(f"   Date range: {start_date} to {end_date}")
        
        # Read partitioned data
        df = self.spark.read \
            .option("basePath", landing_path) \
            .parquet(landing_path) \
            .filter(
                (col("year") >= start_date.year) &
                (col("month") >= start_date.month) &
                (col("day") >= start_date.day) &
                (col("year") <= end_date.year) &
                (col("month") <= end_date.month) &
                (col("day") <= end_date.day)
            )
        
        record_count = df.count()
        print(f"✅ Read {record_count:,} records from landing")
        
        return df
    
    def validate_and_clean(self, df):
        """
        Validate schema and clean data
        
        Validations:
        1. Required fields not null
        2. Valid timestamps
        3. Valid coordinates (latitude: -90 to 90, longitude: -180 to 180)
        4. Valid event types
        """
        
        print("🔍 Validating and cleaning data...")
        
        # Parse timestamp
        df = df.withColumn(
            "event_timestamp",
            to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS")
        )
        
        # Validation filters
        valid_df = df.filter(
            # Required fields not null
            (col("event_id").isNotNull()) &
            (col("event_type").isNotNull()) &
            (col("event_timestamp").isNotNull()) &
            
            # Valid coordinates
            (col("latitude").between(-90, 90)) &
            (col("longitude").between(-180, 180)) &
            
            # Valid timestamps (not future, not too old)
            (col("event_timestamp") <= current_timestamp()) &
            (col("event_timestamp") >= lit("2024-01-01"))
        )
        
        invalid_count = df.count() - valid_df.count()
        
        print(f"✅ Validation complete:")
        print(f"   Valid records: {valid_df.count():,}")
        print(f"   Invalid records: {invalid_count:,} ({invalid_count/df.count()*100:.2f}%)")
        
        return valid_df
    
    def deduplicate(self, df):
        """
        Remove duplicate events based on event_id
        
        Strategy: Keep first occurrence (by timestamp)
        """
        
        print("🔄 Deduplicating events...")
        
        initial_count = df.count()
        
        # Deduplicate by event_id, keeping earliest event
        deduped_df = df.orderBy("event_timestamp") \
            .dropDuplicates(["event_id"])
        
        final_count = deduped_df.count()
        duplicate_count = initial_count - final_count
        
        print(f"✅ Deduplication complete:")
        print(f"   Original: {initial_count:,} records")
        print(f"   After dedup: {final_count:,} records")
        print(f"   Duplicates removed: {duplicate_count:,} ({duplicate_count/initial_count*100:.2f}%)")
        
        return deduped_df
    
    def add_metadata(self, df):
        """
        Add metadata columns for tracking and auditing
        """
        
        df = df \
            .withColumn("processing_timestamp", current_timestamp()) \
            .withColumn("processing_date", lit(datetime.now().date())) \
            .withColumn("data_source", lit("kinesis_stream")) \
            .withColumn(
                "row_hash",
                sha2(concat_ws("|", 
                    col("event_id"),
                    col("event_type"),
                    col("timestamp")
                ), 256)
            )
        
        return df
    
    def write_to_bronze(self, df):
        """
        Write to bronze layer
        
        Format: Delta Lake (ACID, time travel, schema evolution)
        Partitioning: year/month/day for efficient querying
        """
        
        bronze_path = f"{self.config['bronze_bucket']}/bronze/events/"
        
        print(f"💾 Writing to bronze layer: {bronze_path}")
        
        df.write \
            .format("delta") \
            .mode("append") \
            .partitionBy("year", "month", "day") \
            .option("mergeSchema", "true") \
            .save(bronze_path)
        
        print("✅ Write to bronze complete")
    
    def run(self, lookback_hours=48):
        """
        Execute complete landing → bronze pipeline
        """
        
        print("\n" + "="*80)
        print("🚀 Starting Landing → Bronze Processing")
        print("="*80 + "\n")
        
        # Calculate date range
        end_date = datetime.now()
        start_date = end_date - timedelta(hours=lookback_hours)
        
        try:
            # Step 1: Read from landing
            df = self.read_from_landing(start_date, end_date)
            
            # Step 2: Validate and clean
            df = self.validate_and_clean(df)
            
            # Step 3: Deduplicate
            df = self.deduplicate(df)
            
            # Step 4: Add metadata
            df = self.add_metadata(df)
            
            # Step 5: Write to bronze
            self.write_to_bronze(df)
            
            print("\n" + "="*80)
            print("✅ Landing → Bronze Processing Complete")
            print("="*80 + "\n")
            
        except Exception as e:
            print(f"\n❌ Pipeline failed: {str(e)}")
            import traceback
            traceback.print_exc()
            sys.exit(1)

# Configuration
config = {
    "landing_bucket": "s3://ride-hailing-datalake/landing",
    "bronze_bucket": "s3://ride-hailing-datalake/bronze"
}

# Run
if __name__ == "__main__":
    processor = LandingToBronzeProcessor(config)
    processor.run(lookback_hours=48)
```

---

## Medallion Architecture

### Bronze → Silver → Gold Transformation

```python
"""
batch_bronze_to_silver.py
Transform bronze data to silver layer with business logic
"""

class BronzeToSilverProcessor:
    """
    Silver Layer Transformations:
    1. Business logic application
    2. Data enrichment (join with reference data)
    3. Aggregations
    4. Derived metrics
    5. Data quality business rules
    """
    
    def create_trip_summary(self, events_df):
        """
        Aggregate trip events into trip summaries
        
        Input: Individual events (TRIP_STARTED, LOCATION_UPDATE, TRIP_COMPLETED)
        Output: One row per trip with complete journey
        """
        
        from pyspark.sql.window import Window
        
        # Filter trip-related events
        trip_events = events_df.filter(
            col("event_type").isin([
                'TRIP_STARTED',
                'TRIP_COMPLETED',
                'DRIVER_LOCATION_UPDATE'
            ])
        )
        
        # Group by trip and aggregate
        trip_summary = trip_events \
            .groupBy("trip_id") \
            .agg(
                # Start details
                first("driver_id", ignorenulls=True).alias("driver_id"),
                first("rider_id", ignorenulls=True).alias("rider_id"),
                min(when(col("event_type") == "TRIP_STARTED", col("event_timestamp"))).alias("start_time"),
                first("start_latitude").alias("start_lat"),
                first("start_longitude").alias("start_lon"),
                
                # End details
                max(when(col("event_type") == "TRIP_COMPLETED", col("event_timestamp"))).alias("end_time"),
                last("end_latitude").alias("end_lat"),
                last("end_longitude").alias("end_lon"),
                
                # Metrics
                max("fare").alias("total_fare"),
                max("duration").alias("actual_duration"),
                
                # Derived metrics
                count(when(col("event_type") == "DRIVER_LOCATION_UPDATE", 1)).alias("location_updates"),
                
                # Audit
                current_timestamp().alias("processed_at")
            ) \
            .withColumn(
                "trip_duration_minutes",
                (col("end_time").cast("long") - col("start_time").cast("long")) / 60
            ) \
            .withColumn(
                "trip_date",
                to_date(col("start_time"))
            )
        
        return trip_summary
    
    def create_driver_metrics(self, trip_summary_df):
        """
        Calculate driver performance metrics
        
        Metrics:
        - Total trips
        - Total earnings
        - Average rating
        - Utilization rate
        - Trip acceptance rate
        """
        
        driver_metrics = trip_summary_df \
            .groupBy("driver_id", "trip_date") \
            .agg(
                count("trip_id").alias("total_trips"),
                sum("total_fare").alias("total_earnings"),
                avg("actual_duration").alias("avg_trip_duration"),
                sum("trip_duration_minutes").alias("total_active_minutes")
            ) \
            .withColumn(
                "avg_fare_per_trip",
                col("total_earnings") / col("total_trips")
            ) \
            .withColumn(
                "trips_per_hour",
                col("total_trips") / (col("total_active_minutes") / 60)
            )
        
        return driver_metrics
    
    def create_rider_history(self, trip_summary_df):
        """
        Build rider trip history and preferences
        """
        
        rider_history = trip_summary_df \
            .groupBy("rider_id") \
            .agg(
                count("trip_id").alias("total_trips"),
                sum("total_fare").alias("lifetime_spend"),
                avg("total_fare").alias("avg_fare"),
                min("start_time").alias("first_trip_date"),
                max("start_time").alias("last_trip_date"),
                collect_list(
                    struct(
                        col("trip_id"),
                        col("start_time"),
                        col("total_fare")
                    )
                ).alias("recent_trips")
            )
        
        return rider_history
    
    def apply_business_rules(self, trip_summary_df):
        """
        Apply business logic and flag anomalies
        
        Rules:
        1. Flag suspicious trips (duration > 4 hours, fare > $500)
        2. Calculate surge pricing applied
        3. Identify cancelled trips
        4. Mark first-time riders
        """
        
        trip_summary_df = trip_summary_df \
            .withColumn(
                "is_suspicious",
                when(
                    (col("trip_duration_minutes") > 240) |  # > 4 hours
                    (col("total_fare") > 500),  # > $500
                    lit(True)
                ).otherwise(lit(False))
            ) \
            .withColumn(
                "surge_applied",
                when(col("total_fare") > col("estimated_fare") * 1.1, lit(True))
                .otherwise(lit(False))
            )
        
        return trip_summary_df
    
    def write_to_silver(self, df, table_name):
        """
        Write to silver layer
        
        Format: Delta Lake
        Partitioning: By date for time-based queries
        """
        
        silver_path = f"{self.config['silver_bucket']}/silver/{table_name}/"
        
        df.write \
            .format("delta") \
            .mode("overwrite") \
            .partitionBy("trip_date") \
            .option("overwriteSchema", "true") \
            .save(silver_path)
        
        print(f"✅ Wrote {df.count():,} records to silver/{table_name}")
    
    def run(self):
        """Execute bronze → silver pipeline"""
        
        # Read from bronze
        bronze_path = f"{self.config['bronze_bucket']}/bronze/events/"
        events_df = self.spark.read.format("delta").load(bronze_path)
        
        print(f"📖 Read {events_df.count():,} events from bronze")
        
        # Create silver tables
        print("\n🔄 Creating trip summaries...")
        trip_summary = self.create_trip_summary(events_df)
        self.write_to_silver(trip_summary, "trip_summary")
        
        print("\n🔄 Creating driver metrics...")
        driver_metrics = self.create_driver_metrics(trip_summary)
        self.write_to_silver(driver_metrics, "driver_metrics")
        
        print("\n🔄 Creating rider history...")
        rider_history = self.create_rider_history(trip_summary)
        self.write_to_silver(rider_history, "rider_history")
        
        print("\n✅ Silver layer processing complete")
```

### Gold Layer: Business-Ready Datasets

```python
"""
batch_silver_to_gold.py
Create business-ready dimensional models for analytics
"""

class SilverToGoldProcessor:
    """
    Gold Layer:
    - Dimensional models (star schema)
    - Aggregated metrics for dashboards
    - ML features
    - Compliance reports
    """
    
    def create_dim_driver(self, driver_metrics_df):
        """
        Create driver dimension table
        
        Slowly Changing Dimension (Type 2)
        Tracks changes to driver attributes over time
        """
        
        dim_driver = driver_metrics_df \
            .select(
                "driver_id",
                "total_trips",
                "total_earnings",
                "avg_trip_duration"
            ) \
            .withColumn("effective_date", current_date()) \
            .withColumn("is_current", lit(True))
        
        return dim_driver
    
    def create_fact_trip(self, trip_summary_df):
        """
        Create trip fact table
        
        Grain: One row per trip
        Measures: Fare, duration, distance
        Foreign keys: driver_id, rider_id, date_id
        """
        
        fact_trip = trip_summary_df \
            .select(
                "trip_id",
                "driver_id",
                "rider_id",
                "start_time",
                "end_time",
                "total_fare",
                "actual_duration",
                "trip_duration_minutes",
                "is_suspicious",
                "surge_applied"
            ) \
            .withColumn("date_id", date_format(col("start_time"), "yyyyMMdd").cast("int"))
        
        return fact_trip
    
    def create_daily_metrics(self, trip_summary_df):
        """
        Daily aggregated metrics for executive dashboard
        """
        
        daily_metrics = trip_summary_df \
            .groupBy("trip_date") \
            .agg(
                count("trip_id").alias("total_trips"),
                sum("total_fare").alias("total_revenue"),
                avg("total_fare").alias("avg_fare"),
                countDistinct("driver_id").alias("active_drivers"),
                countDistinct("rider_id").alias("active_riders")
            ) \
            .withColumn(
                "revenue_per_trip",
                col("total_revenue") / col("total_trips")
            ) \
            .withColumn(
                "trips_per_driver",
                col("total_trips") / col("active_drivers")
            )
        
        return daily_metrics
    
    def load_to_redshift(self, df, table_name):
        """
        Load gold data to Redshift for BI tools
        
        Strategy:
        1. Write to S3 as Parquet
        2. Use COPY command to load to Redshift
        3. UPSERT via staging table
        """
        
        # Write to S3
        s3_path = f"{self.config['gold_bucket']}/gold/{table_name}/"
        df.write.mode("overwrite").parquet(s3_path)
        
        # COPY to Redshift
        copy_command = f"""
        BEGIN TRANSACTION;
        
        -- Truncate staging
        TRUNCATE TABLE staging.{table_name};
        
        -- COPY from S3
        COPY staging.{table_name}
        FROM '{s3_path}'
        IAM_ROLE '{self.config['redshift_iam_role']}'
        FORMAT AS PARQUET;
        
        -- Merge to production
        DELETE FROM production.{table_name}
        USING staging.{table_name}
        WHERE production.{table_name}.id = staging.{table_name}.id;
        
        INSERT INTO production.{table_name}
        SELECT * FROM staging.{table_name};
        
        END TRANSACTION;
        """
        
        # Execute via Redshift Data API
        redshift_data = boto3.client('redshift-data')
        redshift_data.execute_statement(
            WorkgroupName=self.config['redshift_workgroup'],
            Database=self.config['redshift_database'],
            Sql=copy_command
        )
        
        print(f"✅ Loaded {table_name} to Redshift")
```

---

## Data Quality & Monitoring

```python
"""
data_quality_checks.py
Comprehensive data quality framework
"""

class DataQualityChecker:
    """
    Data quality checks at each layer
    
    Checks:
    1. Completeness: % of null values
    2. Accuracy: Business rule violations
    3. Consistency: Referential integrity
    4. Timeliness: Data freshness
    5. Uniqueness: Duplicate detection
    """
    
    def check_completeness(self, df, required_columns):
        """Check for null values in required columns"""
        
        results = {}
        total_rows = df.count()
        
        for col_name in required_columns:
            null_count = df.filter(col(col_name).isNull()).count()
            null_percentage = (null_count / total_rows) * 100
            
            results[col_name] = {
                "null_count": null_count,
                "null_percentage": round(null_percentage, 2),
                "status": "PASS" if null_percentage < 1 else "FAIL"
            }
        
        return results
    
    def check_accuracy(self, df):
        """Check business rule violations"""
        
        violations = {}
        
        # Check 1: Fare should be positive
        violations['negative_fare'] = df.filter(col("total_fare") < 0).count()
        
        # Check 2: Trip duration should be reasonable (<24 hours)
        violations['excessive_duration'] = df.filter(col("trip_duration_minutes") > 1440).count()
        
        # Check 3: Start time before end time
        violations['invalid_time_order'] = df.filter(col("start_time") > col("end_time")).count()
        
        return violations
    
    def check_freshness(self, df):
        """Check data freshness"""
        
        latest_event = df.agg(max("event_timestamp")).collect()[0][0]
        current_time = datetime.now(timezone.utc)
        
        delay_seconds = (current_time - latest_event).total_seconds()
        delay_hours = delay_seconds / 3600
        
        return {
            "latest_event": latest_event,
            "current_time": current_time,
            "delay_hours": round(delay_hours, 2),
            "status": "FRESH" if delay_hours < 24 else "STALE"
        }
    
    def publish_metrics(self, metrics):
        """Publish DQ metrics to CloudWatch"""
        
        cloudwatch = boto3.client('cloudwatch')
        
        metric_data = []
        for metric_name, value in metrics.items():
            metric_data.append({
                'MetricName': metric_name,
                'Value': value,
                'Unit': 'Count',
                'Timestamp': datetime.now(timezone.utc)
            })
        
        cloudwatch.put_metric_data(
            Namespace='RideHailing/DataQuality',
            MetricData=metric_data
        )
```

---

## Interview Guide

### System Design Question

**"Design a real-time data pipeline for a ride-hailing platform that processes 50,000 events/second with sub-100ms latency for real-time features and provides accurate historical analytics."**

### My Answer Framework

```
1. Requirements Clarification (5 minutes)
   - Functional: Real-time + Historical
   - Non-functional: Latency, throughput, accuracy
   - Scale: 500M events/day, 50K events/sec peak
   
2. High-Level Design (10 minutes)
   - Lambda Architecture (streaming + batch)
   - Kinesis → Lambda/Flink → DynamoDB (hot path)
   - Kinesis → S3 → Spark → Bronze/Silver/Gold → Redshift (cold path)
   
3. Deep Dive (15 minutes)
   - Streaming: Lambda vs Flink (when to use each)
   - Batch: Medallion architecture (bronze/silver/gold)
   - Exactly-once semantics (Kinesis + Lambda + DynamoDB)
   - Data quality at each layer
   
4. Trade-offs (5 minutes)
   - Lambda vs Kappa architecture
   - Kinesis vs Kafka MSK
   - Cost optimization (spot instances, intelligent tiering)
   
5. Production Considerations (5 minutes)
   - Monitoring (CloudWatch, custom metrics)
   - Alerting (PagerDuty, Slack)
   - Disaster recovery (S3 versioning, Redshift backups)
   - Compliance (GDPR, data retention)
```

### Key Points to Emphasize

```
✅ Lambda Architecture solves the CAP theorem trade-off
   - Streaming: Low latency, eventual consistency
   - Batch: High latency, strong consistency
   - Serving: Merge both for complete view

✅ Medallion Pattern organizes data lake
   - Bronze: Raw (minimal transformation)
   - Silver: Cleaned (business logic)
   - Gold: Aggregated (BI-ready)

✅ Technology Choices Based on Scale
   - Kinesis (not Kafka): Better for variable load, fully managed
   - Lambda + Flink: Lambda for simple, Flink for complex
   - Delta Lake: ACID on S3, time travel, schema evolution

✅ Production-Grade Considerations
   - Exactly-once semantics (financial data)
   - Data quality checks at every layer
   - Cost optimization ($24K/month for 500M events/day)
   - Disaster recovery (multi-region, backups)
```

---

## Conclusion

This ride-hailing streaming architecture demonstrates:

1. **Lambda Architecture** - balancing real-time speed with batch accuracy
2. **Medallion Pattern** - organizing data lake with bronze/silver/gold layers
3. **Production-grade code** - complete implementations with error handling
4. **Real-world scale** - 500M events/day, 50K TPS peak
5. **Cost optimization** - $24K/month total infrastructure cost
6. **Interview-ready** - comprehensive explanations and trade-off discussions

**Next Steps:**
- Implement monitoring dashboards (CloudWatch, Grafana)
- Add ML pipeline (SageMaker for demand forecasting)
- Multi-region deployment (disaster recovery)
- Advanced optimizations (query result caching, materialized views)