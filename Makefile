.PHONY: help up down logs kafka-producer stream batch train-ml lambda-local test format

help: ## Show this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Targets:'
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

up: ## Start all services with Docker Compose
	@cp .env.example .env 2>/dev/null || true
	docker-compose up -d
	@echo "Services starting... Access URLs:"
	@echo "  Kafka UI: http://localhost:8080"
	@echo "  NiFi: https://localhost:8443/nifi (admin/ctsBtRBKHRAx69EqUghvvgEvjnaLjFEB)"
	@echo "  MinIO Console: http://localhost:9001 (minioadmin/minioadmin)"
	@echo "  Spark Master: http://localhost:8080"
	@echo "  MLflow: http://localhost:5000"
	@echo "  PostgreSQL: localhost:5432 (dataplatform/dataplatform)"

down: ## Stop all services
	docker-compose down

logs: ## Show logs from all services
	docker-compose logs -f

kafka-producer: ## Build and run Java Kafka producer
	cd ingestion/java-producer && mvn clean package
	java -jar ingestion/java-producer/target/kafka-producer-1.0-SNAPSHOT.jar

stream: ## Run PySpark streaming job
	docker-compose exec spark-master spark-submit \
		--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-core_2.12:2.4.0 \
		--conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
		--conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
		--conf "spark.hadoop.fs.s3a.endpoint=http://minio:9000" \
		--conf "spark.hadoop.fs.s3a.access.key=minioadmin" \
		--conf "spark.hadoop.fs.s3a.secret.key=minioadmin" \
		--conf "spark.hadoop.fs.s3a.path.style.access=true" \
		--conf "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" \
		/opt/spark-apps/python/streaming_job.py

batch: ## Build and run Scala batch job
	cd spark/scala && sbt assembly
	docker-compose exec spark-master spark-submit \
		--packages io.delta:delta-core_2.12:2.4.0 \
		--conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
		--conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
		--conf "spark.hadoop.fs.s3a.endpoint=http://minio:9000" \
		--conf "spark.hadoop.fs.s3a.access.key=minioadmin" \
		--conf "spark.hadoop.fs.s3a.secret.key=minioadmin" \
		--conf "spark.hadoop.fs.s3a.path.style.access=true" \
		--conf "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" \
		--class etl.BatchETL \
		/opt/spark-apps/scala/target/scala-2.12/batch-etl-assembly-1.0.jar

train-ml: ## Run ML training
	docker-compose exec spark-master bash -c "\
		pip install mlflow scikit-learn delta-spark pandas && \
		python /opt/spark-apps/training/train.py"

lambda-local: ## Run local Lambda-style ML serving
	cd ml/serving && python lambda_handler.py

test: ## Run tests
	python -m pytest tests/ -v

format: ## Format code
	black .
	isort .

clean: ## Clean build artifacts
	cd ingestion/java-producer && mvn clean || true
	cd spark/scala && sbt clean || true
	docker-compose down -v
	docker system prune -f