# Data Platform Demo

A demonstration repository for modern data platform architecture and implementation patterns including data pipelines, storage solutions, processing frameworks, and analytics tools.

**IMPORTANT**: Always reference these instructions first and fallback to search or bash commands only when you encounter unexpected information that does not match the info here.

## Repository Status

**CURRENT STATE**: This repository is in initial setup phase and contains only a LICENSE file. The instructions below provide guidance for data platform development patterns and will be applicable once the platform components are implemented.

## Working Effectively

### Initial Setup and Validation
- Clone repository: `git clone https://github.com/mario-guerra/data-platform-demo.git`
- Navigate to repo: `cd data-platform-demo`
- Check status: `git --no-pager status` -- takes <1 second
- List contents: `ls -la` -- takes <1 second

### Repository Structure (Future State)
When populated, this repository will likely contain:
```
├── infrastructure/          # Infrastructure as Code (Terraform, CloudFormation)
├── pipelines/              # Data pipeline definitions (Airflow DAGs, Prefect flows)
├── data/                   # Sample datasets and schemas
├── notebooks/              # Jupyter notebooks for analysis
├── services/               # Microservices and APIs
├── docker/                 # Container definitions
├── config/                 # Configuration files
├── scripts/                # Setup and utility scripts
├── tests/                  # Test suites
└── docs/                   # Documentation
```

### Development Environment Setup (To Be Implemented)
Expected setup commands once implemented:
- **Python Environment**: `python -m venv venv && source venv/bin/activate`
- **Dependencies**: `pip install -r requirements.txt` -- Expected time: 2-5 minutes. NEVER CANCEL. Set timeout to 10+ minutes.
- **Docker Setup**: `docker-compose up -d` -- Expected time: 5-15 minutes for initial pull. NEVER CANCEL. Set timeout to 30+ minutes.

### Build and Test Processes (To Be Implemented)
Expected processes once repository is populated:
- **Linting**: `flake8 . && black --check .` -- Expected time: 30 seconds. Set timeout to 2 minutes.
- **Type Checking**: `mypy .` -- Expected time: 1-2 minutes. Set timeout to 5 minutes.
- **Unit Tests**: `pytest tests/` -- Expected time: 2-10 minutes depending on test suite. NEVER CANCEL. Set timeout to 20+ minutes.
- **Integration Tests**: `pytest tests/integration/` -- Expected time: 10-30 minutes. NEVER CANCEL. Set timeout to 60+ minutes.

### Data Platform Components (Future Implementation)
When implemented, validate these components:

#### Database Connections
- **Test Database**: `python scripts/test_db_connection.py`
- **Run Migrations**: `alembic upgrade head` -- Expected time: 1-5 minutes. Set timeout to 10 minutes.

#### Data Pipeline Testing
- **Validate Pipelines**: `airflow dags validate` -- Expected time: 30 seconds-2 minutes. Set timeout to 5 minutes.
- **Test Data Processing**: `python pipelines/test_pipeline.py` -- Expected time: 5-30 minutes. NEVER CANCEL. Set timeout to 60+ minutes.

#### Infrastructure Deployment
- **Plan Infrastructure**: `terraform plan` -- Expected time: 30 seconds-2 minutes. Set timeout to 5 minutes.
- **Apply Infrastructure**: `terraform apply` -- Expected time: 10-45 minutes. NEVER CANCEL. Set timeout to 90+ minutes.

## Validation Scenarios

### Manual Testing Requirements
After making changes, ALWAYS validate functionality through these scenarios:

#### Basic Repository Operations
1. **Git Operations**: Ensure `git status`, `git diff`, and `git log` work correctly
2. **File System**: Verify all directories and files are accessible
3. **Permissions**: Check that scripts have executable permissions where needed

#### Data Platform Functionality (When Implemented)
1. **Database Connectivity**: Test connection to all configured databases
2. **Pipeline Execution**: Run a simple end-to-end data pipeline
3. **Data Validation**: Verify data quality checks pass
4. **API Endpoints**: Test all service endpoints return expected responses
5. **Dashboard Access**: Ensure analytics dashboards load and display data correctly

#### Infrastructure Validation (When Implemented)
1. **Service Health**: Check all containerized services are running
2. **Network Connectivity**: Verify inter-service communication
3. **Resource Monitoring**: Confirm monitoring and alerting systems are functional
4. **Data Flow**: Trace data from ingestion through to final output

## Technology-Specific Guidelines

### Python Development
- Always use virtual environments: `python -m venv venv`
- Pin dependencies in requirements.txt with exact versions
- Use type hints throughout the codebase
- Follow PEP 8 style guidelines
- **Testing**: Use pytest for all testing -- includes fixtures and parametrized tests

### Docker and Containerization
- **Build Images**: `docker build -t data-platform .` -- Expected time: 5-20 minutes. NEVER CANCEL. Set timeout to 45+ minutes.
- **Compose Services**: `docker-compose up` -- Expected time: 2-10 minutes. Set timeout to 20+ minutes.
- Always use multi-stage builds for production images
- Include health checks in all service definitions

### Data Processing
- **Batch Processing**: Use Spark for large datasets -- jobs may take 30+ minutes. NEVER CANCEL.
- **Stream Processing**: Implement proper error handling and backpressure
- **Data Validation**: Always include schema validation in pipelines

### Infrastructure as Code
- **Terraform Commands**: Plan before apply, always review changes
- **State Management**: Use remote state storage for team collaboration
- **Security**: Never commit secrets, use secret management services

## Common Tasks

### Repository Management
```bash
# Check repository status
git --no-pager status

# View recent commits  
git --no-pager log --oneline -10

# Create feature branch
git checkout -b feature/new-pipeline

# Stage and commit changes
git add . && git commit -m "Add new data pipeline"
```

### Development Workflow (When Implemented)
```bash
# Activate virtual environment
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Run code quality checks
black . && flake8 . && mypy .

# Run tests
pytest tests/ -v

# Build and test locally
docker-compose up --build
```

### Data Pipeline Development (Future)
```bash
# Validate Airflow DAGs
airflow dags validate

# Test pipeline locally
python -m pipelines.example_pipeline --local

# Run data quality checks
python -m data_quality.validate_dataset --dataset example
```

## Error Handling and Troubleshooting

### Common Issues (Future Reference)
- **Database Connection Errors**: Check connection strings and credentials
- **Pipeline Failures**: Review logs in `logs/` directory
- **Docker Issues**: Clear cache with `docker system prune`
- **Permission Errors**: Ensure proper file permissions with `chmod +x scripts/*.sh`

### Debug Commands
```bash
# Check running containers
docker ps -a

# View container logs
docker-compose logs service-name

# Connect to database
psql -h localhost -U username -d database_name

# Monitor resource usage
docker stats

# Check network connectivity
docker network ls
```

## CI/CD Integration

### Pre-commit Validation
Always run these commands before committing:
```bash
# Code formatting and linting -- takes 30 seconds
black . && flake8 .

# Type checking -- takes 1-2 minutes
mypy .

# Run unit tests -- takes 2-10 minutes. NEVER CANCEL.
pytest tests/ --timeout=1200

# Build verification -- takes 5-20 minutes. NEVER CANCEL.
docker build -t test-build .
```

### Pipeline Validation
Before deploying changes:
```bash
# Validate infrastructure changes -- takes 1-5 minutes
terraform plan

# Test pipeline syntax -- takes 30 seconds
airflow dags validate

# Run integration tests -- takes 10-30 minutes. NEVER CANCEL.
pytest tests/integration/ --timeout=3600
```

## Performance Expectations

### Command Timing Guidelines
- **Git operations**: <1 second
- **File system operations**: <1 second  
- **Dependency installation**: 2-10 minutes. Set timeout to 20+ minutes.
- **Code linting/formatting**: 30 seconds-2 minutes. Set timeout to 5 minutes.
- **Unit tests**: 2-15 minutes. NEVER CANCEL. Set timeout to 30+ minutes.
- **Integration tests**: 10-60 minutes. NEVER CANCEL. Set timeout to 120+ minutes.
- **Docker builds**: 5-45 minutes. NEVER CANCEL. Set timeout to 90+ minutes.
- **Infrastructure deployment**: 10-90 minutes. NEVER CANCEL. Set timeout to 120+ minutes.
- **Data pipeline execution**: 5 minutes-several hours. NEVER CANCEL. Set appropriate timeouts based on data volume.

### Critical Timing Rules
- **NEVER CANCEL** any build, test, or deployment command
- Always set explicit timeouts with generous buffers (50%+ over expected time)
- For data processing jobs, timeout should be 2x expected processing time minimum
- Monitor resource usage during long-running operations

## Security and Best Practices

### Secret Management
- Never commit secrets to repository
- Use environment variables or secret management services
- Rotate credentials regularly
- Audit access logs

### Data Security
- Implement data encryption at rest and in transit
- Follow data retention policies
- Ensure GDPR/privacy compliance for personal data
- Validate data integrity throughout pipelines

### Code Quality
- Maintain test coverage above 80%
- Use static analysis tools
- Implement proper logging throughout services
- Follow semantic versioning for releases

## Additional Resources

### Key Files to Monitor
When repository is populated, these files are critical:
- `requirements.txt` - Python dependencies
- `docker-compose.yml` - Service orchestration
- `terraform/` - Infrastructure definitions
- `pipelines/` - Data processing workflows
- `.github/workflows/` - CI/CD pipelines

### Documentation Standards
- Update README.md for major changes
- Document API endpoints with OpenAPI/Swagger
- Maintain architecture decision records (ADRs)
- Include inline code documentation

Remember: This repository is currently in initial setup phase. Many of the commands and workflows described above will become applicable as the data platform components are implemented and the repository is populated with actual code and infrastructure.