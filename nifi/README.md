# NiFi Data Ingestion Configuration

This guide explains how to configure NiFi processors to consume data from Kafka and write to both MinIO (object storage) and PostgreSQL.

## Access NiFi

1. Start the services: `make up`
2. Access NiFi at: https://localhost:8443/nifi
3. Login credentials:
   - Username: `admin`
   - Password: `ctsBtRBKHRAx69EqUghvvgEvjnaLjFEB`

## Data Flow Overview

```
Kafka (orders topic) → NiFi → MinIO (raw bucket) + PostgreSQL (orders_raw table)
```

## Step-by-Step Configuration

### 1. ConsumeKafka_2_6 Processor

Configure the Kafka consumer processor:

**Properties:**
- **Kafka Brokers**: `kafka:29092`
- **Topic Name(s)**: `orders`
- **Topic Name Format**: names
- **Honor Transactions**: false
- **Group ID**: `nifi-consumer-group`
- **Offset Reset**: latest
- **Message Demarcator**: (leave empty)
- **Max Poll Records**: 10000
- **Communications Timeout**: 60 sec

**Relationships:**
- Route `success` to next processor
- Route `failure` to LogMessage processor for debugging

### 2. Split Into Two Flows

From ConsumeKafka processor, create two parallel flows:

#### Flow A: To MinIO (Object Storage)
#### Flow B: To PostgreSQL

### 3. Flow A: MinIO Object Storage

#### 3.1 UpdateAttribute Processor (for MinIO)

**Properties:**
- Add custom property: `filename` = `orders_${now():format('yyyy-MM-dd-HH-mm-ss')}_${UUID()}.json`
- Add custom property: `s3.bucket` = `raw`
- Add custom property: `s3.key` = `orders/${now():format('yyyy/MM/dd')}/${filename}`

#### 3.2 PutS3Object Processor

**Properties:**
- **Bucket**: `${s3.bucket}`
- **Object Key**: `${s3.key}`
- **Access Key ID**: `minioadmin`
- **Secret Access Key**: `minioadmin`
- **Endpoint Override URL**: `http://minio:9000`
- **Signer Override**: `AWSS3V4SignerType`
- **Region**: `us-east-1`
- **Communications Timeout**: 30 sec
- **Expiration Time Rule**: (leave empty)
- **Storage Class**: Standard

**Relationships:**
- Route `success` to LogMessage
- Route `failure` to LogMessage

### 4. Flow B: PostgreSQL Database

#### 4.1 EvaluateJsonPath Processor

Extract JSON fields for database insertion:

**Properties:**
- **Destination**: flowfile-attribute
- **Return Type**: auto-detect

**Add these custom properties:**
- `order_id`: `$.order_id`
- `customer_id`: `$.customer_id`
- `product`: `$.product`
- `quantity`: `$.quantity`
- `price`: `$.price`
- `currency`: `$.currency`
- `timestamp`: `$.timestamp`
- `region`: `$.region`

#### 4.2 AttributesToSQL Processor

Convert attributes to SQL INSERT statement:

**Properties:**
- **JDBC Connection Pool**: PostgreSQLConnectionPool (see below)
- **Statement Type**: INSERT
- **Table Name**: `orders_raw`
- **Catalog Name**: (leave empty)
- **Schema Name**: public
- **Translate Field Names**: true
- **Unmatched Field Behavior**: Ignore Unmatched Fields
- **Unmatched Column Behavior**: Fail on Unmatched Column
- **Update Keys**: (leave empty)

#### 4.3 PutSQL Processor

Execute the SQL INSERT:

**Properties:**
- **JDBC Connection Pool**: PostgreSQLConnectionPool
- **SQL Statement**: (will use the statement from AttributesToSQL)
- **Batch Size**: 100
- **Obtain Generated Keys**: false

**Relationships:**
- Route `success` to LogMessage
- Route `failure` to LogMessage
- Route `retry` back to PutSQL (for transient failures)

### 5. Database Connection Pool Configuration

Create a PostgreSQL Connection Pool service:

#### 5.1 DBCPConnectionPool Service

**Properties:**
- **Database Connection URL**: `jdbc:postgresql://postgres:5432/dataplatform`
- **Database Driver Class Name**: `org.postgresql.Driver`
- **Database User**: `dataplatform`
- **Password**: `dataplatform`
- **Max Wait Time**: 500 millis
- **Max Total Connections**: 8
- **Validation Query**: `SELECT 1`

### 6. Example JSON Schema

The processors expect JSON messages in this format:

```json
{
  "order_id": "123e4567-e89b-12d3-a456-426614174000",
  "customer_id": "customer_123",
  "product": "laptop",
  "quantity": 2,
  "price": 999.99,
  "currency": "USD",
  "timestamp": "2023-10-15T10:30:00Z",
  "region": "us-east"
}
```

### 7. Error Handling and Monitoring

#### 7.1 LogMessage Processors

Add LogMessage processors to capture:
- Successful MinIO writes
- Successful PostgreSQL inserts
- Any failures in either flow

**Properties:**
- **Log Level**: INFO
- **Log Prefix**: `[Flow Name]`
- **Log Message**: Custom message describing the step

#### 7.2 Monitoring

- Monitor processor queues for backups
- Check MinIO bucket for file creation
- Verify PostgreSQL table for record insertion
- Monitor NiFi data provenance for end-to-end tracking

### 8. Performance Tuning

**For high-throughput scenarios:**

1. **ConsumeKafka_2_6**:
   - Increase Max Poll Records to 10000
   - Reduce Communications Timeout if network is reliable

2. **PutS3Object**:
   - Use multipart upload for large files
   - Batch smaller messages before writing

3. **PutSQL**:
   - Increase Batch Size to 1000
   - Use connection pooling
   - Consider using PutDatabaseRecord for better performance

4. **General**:
   - Increase concurrent tasks for processors
   - Monitor and adjust run schedules
   - Use back-pressure when necessary

### 9. Security Considerations

- Use encrypted connections in production
- Rotate MinIO and PostgreSQL credentials regularly
- Enable NiFi authentication and authorization
- Consider using parameter contexts for sensitive properties

### 10. Troubleshooting

**Common issues:**

1. **Kafka Connection Refused**:
   - Verify Kafka is running: `docker-compose ps kafka`
   - Check bootstrap servers: `kafka:29092` (internal network)

2. **MinIO Access Denied**:
   - Verify MinIO credentials
   - Check bucket exists: Access MinIO Console at http://localhost:9001

3. **PostgreSQL Connection Failed**:
   - Verify database is running: `docker-compose ps postgres`
   - Check if table exists (run SQL serving scripts first)

4. **No Data in Outputs**:
   - Check if Kafka producer is running: `make kafka-producer`
   - Verify topic has messages: Use Kafka UI at http://localhost:8080

## Next Steps

1. Start the Kafka producer: `make kafka-producer`
2. Configure the NiFi flow as described above
3. Monitor data flow in NiFi UI
4. Verify data in MinIO Console and PostgreSQL
5. Run Spark streaming job: `make stream`
6. Run Spark batch job: `make batch`