# Azure Migration Guide

This guide provides practical instructions for migrating from the local development environment to Azure services.

## Service Mapping: Local → Azure

| Local Service | Azure Service | Purpose |
|---------------|---------------|---------|
| Kafka | Event Hubs | Streaming data ingestion |
| MinIO | ADLS Gen2 | Object storage |
| PostgreSQL | Azure Database for PostgreSQL | Relational database |
| Spark (local) | Azure Synapse / AKS | Data processing |
| MLflow (local) | Azure ML | ML experiment tracking |

## 1. Event Hubs Configuration (Kafka → Event Hubs)

### Connection Configuration

Replace Kafka bootstrap servers with Event Hubs:

```bash
# Local Kafka
KAFKA_BOOTSTRAP_SERVERS=kafka:29092

# Azure Event Hubs
KAFKA_BOOTSTRAP_SERVERS=<eventhub-namespace>.servicebus.windows.net:9093
```

### Authentication

Event Hubs requires SASL/SSL authentication:

```properties
# Kafka client configuration for Event Hubs
bootstrap.servers=<eventhub-namespace>.servicebus.windows.net:9093
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="<connection-string>";
```

### Java Producer Updates

Update `ingestion/java-producer/src/main/java/demo/App.java`:

```java
// Add Event Hubs configuration
props.put("security.protocol", "SASL_SSL");
props.put("sasl.mechanism", "PLAIN");
props.put("sasl.jaas.config", 
    "org.apache.kafka.common.security.plain.PlainLoginModule required " +
    "username=\"$ConnectionString\" " +
    "password=\"" + System.getenv("EVENTHUB_CONNECTION_STRING") + "\";");
```

### Spark Streaming Configuration

Update `spark/python/streaming_job.py`:

```python
# Event Hubs configuration for Spark
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", eventhub_servers) \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config", eventhub_jaas_config) \
    .option("subscribe", "orders") \
    .load()
```

## 2. ADLS Gen2 Configuration (MinIO → ADLS)

### Storage Path Updates

Replace MinIO S3 paths with ABFSS paths:

```bash
# Local MinIO
s3a://delta/bronze/orders

# Azure ADLS Gen2
abfss://delta@<storage-account>.dfs.core.windows.net/bronze/orders
```

### Spark Configuration for ABFSS

Update Spark jobs to use ABFSS with OAuth authentication:

```python
# ADLS Gen2 configuration for Spark
spark = SparkSession.builder \
    .config("spark.hadoop.fs.azure.account.auth.type.<storage-account>.dfs.core.windows.net", "OAuth") \
    .config("spark.hadoop.fs.azure.account.oauth.provider.type.<storage-account>.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider") \
    .config("spark.hadoop.fs.azure.account.oauth2.client.id.<storage-account>.dfs.core.windows.net", "<client-id>") \
    .config("spark.hadoop.fs.azure.account.oauth2.client.secret.<storage-account>.dfs.core.windows.net", "<client-secret>") \
    .config("spark.hadoop.fs.azure.account.oauth2.client.endpoint.<storage-account>.dfs.core.windows.net", "https://login.microsoftonline.com/<tenant-id>/oauth2/token") \
    .getOrCreate()
```

### Environment Variables

Set these environment variables for Azure authentication:

```bash
# Azure authentication
export AZURE_CLIENT_ID="<service-principal-client-id>"
export AZURE_CLIENT_SECRET="<service-principal-client-secret>"
export AZURE_TENANT_ID="<tenant-id>"
export AZURE_STORAGE_ACCOUNT="<storage-account-name>"
```

### MLflow Artifact Store

Update MLflow to use ABFSS for artifact storage:

```python
# MLflow with ABFSS
import mlflow

mlflow.set_tracking_uri("http://your-mlflow-server:5000")
mlflow.set_experiment("order_price_prediction")

# Set artifact root to ADLS
artifact_uri = "abfss://mlflow@<storage-account>.dfs.core.windows.net/"
```

## 3. Spark on Azure Options

### Option A: Azure Synapse Analytics (Recommended)

```python
# Synapse Spark configuration
spark = SparkSession.builder \
    .appName("DataPlatformETL") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# ADLS is automatically configured in Synapse
```

### Option B: AKS with Spark Operator

Deploy Spark jobs to AKS using the Spark Operator:

```yaml
# spark-job.yaml
apiVersion: "sparkoperator.k8s.io/v1beta2"
kind: SparkApplication
metadata:
  name: streaming-etl
spec:
  type: Python
  mode: cluster
  image: "your-registry/spark-python:latest"
  imagePullPolicy: Always
  mainApplicationFile: "local:///opt/spark-apps/streaming_job.py"
  sparkVersion: "3.5.0"
  deps:
    packages:
      - "io.delta:delta-core_2.12:2.4.0"
      - "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
  driver:
    cores: 1
    memory: "2g"
    serviceAccount: spark-operator-spark
  executor:
    cores: 1
    instances: 2
    memory: "2g"
```

## 4. Cost Optimization Strategies

### Storage Tiers

Configure appropriate storage tiers for different data:

```bash
# Hot tier for frequently accessed data
az storage blob set-tier --account-name <storage-account> --container-name delta --name bronze/ --tier Hot

# Cool tier for archival data
az storage blob set-tier --account-name <storage-account> --container-name raw --name archive/ --tier Cool

# Archive tier for long-term retention
az storage blob set-tier --account-name <storage-account> --container-name raw --name historical/ --tier Archive
```

### Event Hubs Scaling

```bash
# Scale Event Hubs throughput units based on load
az eventhubs namespace update \
  --resource-group <resource-group> \
  --name <eventhub-namespace> \
  --capacity 2  # Adjust based on throughput needs
```

### Auto-pause for Development

For development environments, implement auto-pause for Synapse SQL pools:

```bash
# Pause Synapse SQL pool when not in use
az synapse sql pool pause \
  --name <sql-pool> \
  --workspace-name <workspace> \
  --resource-group <resource-group>
```

## 5. Security Best Practices

### Service Principal Setup

Create a service principal for application authentication:

```bash
# Create service principal
az ad sp create-for-rbac \
  --name "data-platform-sp" \
  --role "Storage Blob Data Contributor" \
  --scopes "/subscriptions/<subscription-id>/resourceGroups/<resource-group>"

# Grant Event Hubs access
az role assignment create \
  --assignee <service-principal-id> \
  --role "Azure Event Hubs Data Receiver" \
  --scope "/subscriptions/<subscription-id>/resourceGroups/<resource-group>/providers/Microsoft.EventHub/namespaces/<namespace>"
```

### Key Vault Integration

Store secrets in Azure Key Vault:

```bash
# Store connection strings
az keyvault secret set \
  --vault-name <key-vault> \
  --name "eventhub-connection-string" \
  --value "<connection-string>"

az keyvault secret set \
  --vault-name <key-vault> \
  --name "storage-account-key" \
  --value "<storage-key>"
```

## 6. Monitoring and Alerting

### Application Insights

Add Application Insights to Spark jobs:

```python
from applicationinsights import TelemetryClient

# Initialize telemetry
tc = TelemetryClient('<instrumentation-key>')

# Track custom events
tc.track_event('StreamingJobStarted', {'environment': 'production'})
tc.track_metric('RecordsProcessed', record_count)
tc.flush()
```

### Azure Monitor Alerts

Set up cost and performance alerts:

```bash
# Create cost alert
az monitor metrics alert create \
  --name "EventHubsHighThroughput" \
  --resource-group <resource-group> \
  --scopes <eventhub-resource-id> \
  --condition "avg Platform.IncomingMessages > 1000" \
  --description "Event Hubs receiving high message volume"

# Create storage cost alert
az consumption budget create \
  --budget-name "DataPlatformBudget" \
  --amount 100 \
  --resource-group <resource-group> \
  --time-grain Monthly \
  --threshold 80
```

## 7. Migration Checklist

### Pre-Migration
- [ ] Provision Azure resources using Terraform
- [ ] Set up service principal with appropriate permissions
- [ ] Configure Azure Key Vault with secrets
- [ ] Update application configuration for Azure services

### Migration Steps
- [ ] Update Kafka clients to use Event Hubs
- [ ] Migrate MinIO data to ADLS Gen2
- [ ] Configure Spark jobs for ABFSS
- [ ] Update MLflow for Azure Blob Storage
- [ ] Test end-to-end pipeline in Azure

### Post-Migration
- [ ] Set up monitoring and alerting
- [ ] Configure cost management and budgets
- [ ] Implement backup and disaster recovery
- [ ] Document operational procedures

## 8. Troubleshooting

### Common Issues

**Event Hubs Connection Timeout**
```bash
# Check network connectivity
nslookup <eventhub-namespace>.servicebus.windows.net

# Verify connection string format
echo $EVENTHUB_CONNECTION_STRING | grep -o "EntityPath=[^;]*"
```

**ADLS Authentication Failures**
```bash
# Test ADLS access
az storage fs exists \
  --account-name <storage-account> \
  --name delta \
  --auth-mode login
```

**Spark Job Failures**
```bash
# Check Spark driver logs
kubectl logs <spark-driver-pod> -f

# Verify Delta Lake packages
spark-submit --packages io.delta:delta-core_2.12:2.4.0 --version
```

### Performance Tuning

**Event Hubs Throughput**
- Use multiple partitions for parallel processing
- Batch messages to improve throughput
- Monitor throttling metrics

**ADLS Performance**
- Use appropriate blob access tiers
- Implement data partitioning strategies
- Enable hierarchical namespace for better performance

**Spark Optimization**
- Use appropriate cluster sizes for workload
- Optimize partition sizes for Delta tables
- Enable adaptive query execution

## 9. Cost Monitoring

### Daily Cost Review
```bash
# Get daily costs for resource group
az consumption usage list \
  --start-date $(date -d "7 days ago" +%Y-%m-%d) \
  --end-date $(date +%Y-%m-%d) \
  --scope "/subscriptions/<subscription-id>/resourceGroups/<resource-group>"
```

### Resource Optimization
- Use Azure Advisor recommendations
- Implement auto-scaling for compute resources
- Regular review of unused resources
- Consider reserved instances for predictable workloads

This guide provides the foundation for migrating your local data platform to Azure while maintaining cost efficiency and operational excellence.