# Data Platform Demo

A comprehensive, Azure-first, local-first, cost-conscious data platform scaffold demonstrating modern data engineering and ML practices.

## 🏗️ Architecture Overview

```
                    📊 DATA PLATFORM ARCHITECTURE 📊

┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Data Sources  │    │   Stream Ingest  │    │  Batch Process  │
│                 │    │                  │    │                 │
│ Java Producer   │───▶│ Kafka → NiFi     │───▶│ Spark Streaming │
│ (Orders JSON)   │    │                  │    │ (Delta Bronze)  │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                                │                        │
                                ▼                        ▼
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Object Store  │    │    OLTP Store    │    │ Delta Lake ETL  │
│                 │    │                  │    │                 │
│ MinIO/ADLS Gen2 │    │   PostgreSQL     │    │ PySpark Stream  │
│ (Raw/Delta)     │    │ (orders_raw)     │    │ Scala Batch    │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                                                        │
                                                        ▼
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   ML Platform   │    │   Serving Layer  │    │  Delta Layers   │
│                 │    │                  │    │                 │
│ MLflow Server   │◀───│ SQL Views        │◀───│ Bronze/Silver/  │
│ Training/Track  │    │ Analytics APIs   │    │ Gold Tables     │
└─────────────────┘    └──────────────────┘    └─────────────────┘
```

### Data Flow Pipeline

1. **Ingestion**: Java Kafka producer → Kafka topics
2. **Stream Processing**: NiFi → MinIO (raw) + PostgreSQL 
3. **Delta Pipeline**: Spark Streaming → Bronze → Silver (windowed aggregations)
4. **Batch Processing**: Scala ETL → Gold (daily stats)
5. **ML Pipeline**: Feature engineering → sklearn training → MLflow tracking
6. **Serving**: SQL views, Delta tables, ML model endpoints

## 🚀 Quick Start

### Prerequisites

- Docker & Docker Compose
- Java 11+ (for Kafka producer)
- Python 3.10+ (for ML components)
- Make (optional, for convenience)

### 1. Start Local Environment

```bash
# Clone and navigate to repository
git clone https://github.com/mario-guerra/data-platform-demo.git
cd data-platform-demo

# Start all services
make up

# Or manually:
cp .env.example .env
docker-compose up -d
```

### 2. Generate Test Data

```bash
# Build and run Java Kafka producer
make kafka-producer

# Or manually:
cd ingestion/java-producer
mvn clean package
java -jar target/kafka-producer-1.0-SNAPSHOT.jar
```

### 3. Configure NiFi Data Flow

1. Access NiFi: https://localhost:8443/nifi
2. Login: `admin` / `ctsBtRBKHRAx69EqUghvvgEvjnaLjFEB`
3. Follow setup guide: [nifi/README.md](nifi/README.md)

### 4. Run Data Processing Jobs

```bash
# Start Spark streaming (Delta bronze/silver)
make stream

# Run Spark batch job (Delta gold)
make batch

# Train ML model
make train-ml

# Test ML serving locally
make lambda-local
```

### 5. Access Services

| Service | URL | Credentials |
|---------|-----|-------------|
| **Kafka UI** | http://localhost:8080 | - |
| **NiFi** | https://localhost:8443/nifi | admin / ctsBtRBKHRAx69EqUghvvgEvjnaLjFEB |
| **MinIO Console** | http://localhost:9001 | minioadmin / minioadmin |
| **Spark Master** | http://localhost:8080 | - |
| **MLflow** | http://localhost:5000 | - |
| **PostgreSQL** | localhost:5432 | dataplatform / dataplatform |

## ☁️ Azure Deployment

### 1. Setup GitHub Environment

Create an "azure" environment in GitHub with these secrets:
- `AZURE_CLIENT_ID`
- `AZURE_TENANT_ID` 
- `AZURE_SUBSCRIPTION_ID`

### 2. Deploy Infrastructure

1. Go to Actions → "Azure Infrastructure" 
2. Select "Run workflow"
3. Choose options:
   - **Apply**: `true` (to deploy)
   - **Enable AKS**: `false` (default - saves cost)
   - **Enable ADF**: `false` (default - saves cost)

### 3. Migration Guide

See detailed migration instructions: [azure/README.md](azure/README.md)

**Key Service Mappings:**
- Kafka → Event Hubs (Kafka-compatible)
- MinIO → ADLS Gen2 (with ABFSS)
- Local Spark → Azure Synapse/AKS

## 💰 Cost Optimization

### Local Development (Free)
- All services run in Docker containers
- No cloud costs during development

### Azure Baseline (~$16-31/month)
- **ADLS Gen2 (LRS)**: ~$5-20 (data-dependent)
- **Event Hubs Basic**: ~$11 (1 throughput unit)
- **Resource Group**: Free

### Optional Azure Services (Disabled by Default)
- **AKS Cluster**: ~$30/month (1 x Standard_B2s node)
- **Azure Data Factory**: ~$0.50 + execution costs

### Cost Monitoring
```bash
# After Azure deployment, monitor costs:
az consumption usage list --start-date $(date -d "7 days ago" +%Y-%m-%d)
```

## 🛠️ Development

### Repository Structure

```
├── ingestion/java-producer/     # Kafka producer (Java/Maven)
├── spark/python/               # Streaming jobs (PySpark) 
├── spark/scala/                # Batch ETL (Scala/SBT)
├── sql/serving/                # SQL views and schemas
├── nifi/                       # NiFi configuration guide
├── ml/training/                # ML training scripts
├── ml/serving/                 # ML serving handlers
├── infra/azure/terraform/      # Azure infrastructure
├── azure/                      # Azure migration guide
├── cs/                        # CS fundamentals (LSM-tree)
├── .github/workflows/          # CI/CD pipelines
└── docker-compose.yml         # Local environment
```

### Make Targets

```bash
make up          # Start all services
make down        # Stop all services  
make logs        # Show service logs
make kafka-producer  # Run data generator
make stream      # Run streaming job
make batch       # Run batch ETL
make train-ml    # Train ML model
make lambda-local    # Test ML serving
make test        # Run tests
make format      # Format code
make clean       # Clean up
```

### Code Quality

```bash
# Setup pre-commit hooks
pip install -r requirements-dev.txt
pre-commit install

# Run linting
black .
flake8 .
isort .
```

## 🧪 Testing

### Run All Tests
```bash
# Python tests
python -m pytest tests/ -v

# Test CS fundamentals
python tests/test_lsm_tree.py

# Test ML components
cd ml/serving && python lambda_handler.py test
```

### Integration Testing
```bash
# Validate services are healthy
make up
docker-compose ps

# Test connectivity
curl -f http://localhost:9001/minio/health/live
```

## 📚 Learning Resources

### CS Fundamentals
- **LSM Trees**: Explore `cs/lsm_tree.py` for a complete implementation
- **Tests**: Run `tests/test_lsm_tree.py` for comprehensive examples

### Data Engineering
- **Kafka Producer**: Java implementation with synthetic data generation
- **NiFi Flows**: Visual data pipeline configuration
- **Delta Lake**: Multi-layer architecture (Bronze/Silver/Gold)
- **Spark**: Both streaming (PySpark) and batch (Scala) processing

### ML Engineering  
- **MLflow**: Experiment tracking and model versioning
- **Feature Engineering**: Time-based and product features
- **Model Serving**: Lambda-style local serving with fallbacks

### Infrastructure
- **Terraform**: Azure resource provisioning
- **GitHub Actions**: CI/CD with OIDC authentication
- **Docker Compose**: Local development environment

## 🔧 Troubleshooting

### Common Issues

**Services won't start:**
```bash
# Check Docker resources
docker system df
docker system prune -f

# Restart services
make down && make up
```

**Kafka connection issues:**
```bash
# Check Kafka is ready
docker-compose exec kafka kafka-topics.sh --list --bootstrap-server localhost:29092

# Verify dual listener setup
docker-compose logs kafka | grep -i listener
```

**Spark job failures:**
```bash
# Check Spark master logs
docker-compose logs spark-master

# Verify Delta packages
docker-compose exec spark-master spark-submit --packages io.delta:delta-core_2.12:2.4.0 --version
```

**ML training issues:**
```bash
# Check MLflow connectivity
curl -f http://localhost:5000/health

# Verify MinIO access
docker-compose exec spark-master python -c "import boto3; print('✓ Boto3 available')"
```

### Network Configuration

The platform uses **dual Kafka listeners** for proper networking:
- **Host applications**: Connect to `localhost:9092`
- **Container services**: Connect to `kafka:29092`

This ensures both external tools and internal services can access Kafka correctly.

### Performance Tuning

**For high throughput:**
- Increase Kafka partitions
- Scale Spark executor memory/cores
- Tune Delta Lake file sizes
- Implement proper data partitioning

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Run the full test suite
6. Submit a pull request

### Development Workflow

```bash
# Setup development environment
git clone https://github.com/your-fork/data-platform-demo.git
cd data-platform-demo

# Install dependencies
pip install -r requirements-dev.txt
pre-commit install

# Start local environment
make up

# Make changes and test
make test
make format

# Commit and push
git add .
git commit -m "Your changes"
git push origin feature-branch
```

## 📄 License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## 🔗 Related Projects

- [Apache Kafka](https://kafka.apache.org/) - Distributed streaming platform
- [Apache Spark](https://spark.apache.org/) - Unified analytics engine  
- [Delta Lake](https://delta.io/) - Storage layer for data lakes
- [MLflow](https://mlflow.org/) - ML lifecycle management
- [Apache NiFi](https://nifi.apache.org/) - Data flow automation

---

**Built with ❤️ for the data community**

Start building your data platform today with this comprehensive scaffold that grows from local development to enterprise-scale Azure deployment!