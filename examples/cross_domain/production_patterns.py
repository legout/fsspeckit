"""
Production Patterns Example

This cross-domain example demonstrates production-ready patterns and best practices
for deploying fsspeckit-based solutions in enterprise environments.

The example covers:
1. Configuration management and environment handling
2. Production deployment patterns (Docker, Kubernetes)
3. Monitoring, observability, and alerting
4. Error handling, retries, and circuit breakers
5. Performance optimization and auto-scaling
6. Security, authentication, and compliance
7. Testing strategies for production systems

This example shows how to build enterprise-grade, production-ready
data solutions using fsspeckit with operational excellence.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import List, Dict, Any, Optional, Callable, Union
from urllib.parse import urlparse

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

# Production-related imports
from fsspeckit.datasets import DuckDBParquetHandler
from fsspeckit.sql import validate_sql_query, optimize_sql_query
from fsspeckit.common import setup_structured_logging, run_parallel
from fsspeckit.storage_options import AwsStorageOptions, AzureStorageOptions, GcsStorageOptions

# Optional production dependencies
try:
    import prometheus_client as prometheus
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

try:
    from pydantic import BaseModel, validator
    PYDANTIC_AVAILABLE = True
except ImportError:
    PYDANTIC_AVAILABLE = False

try:
    import yaml
    YAML_AVAILABLE = True
except ImportError:
    YAML_AVAILABLE = False


class Environment(Enum):
    """Production environment types."""
    DEVELOPMENT = "development"
    TESTING = "testing"
    STAGING = "staging"
    PRODUCTION = "production"


class DeploymentMode(Enum):
    """Deployment modes."""
    LOCAL = "local"
    DOCKER = "docker"
    KUBERNETES = "kubernetes"
    SERVERLESS = "serverless"


@dataclass
class ProductionConfig:
    """Production-ready configuration with validation."""

    # Environment configuration
    environment: Environment
    deployment_mode: DeploymentMode

    # Infrastructure settings
    aws_region: str = "us-east-1"
    azure_region: str = "eastus"
    gcp_region: str = "us-central1"

    # Performance settings
    max_workers: int = 4
    memory_limit_mb: int = 8192
    connection_pool_size: int = 10
    request_timeout_seconds: int = 30

    # Security settings
    enable_authentication: bool = True
    enable_encryption: bool = True
    audit_logging: bool = True

    # Monitoring settings
    enable_metrics: bool = True
    enable_tracing: bool = True
    health_check_interval_seconds: int = 30

    # Retry and resilience
    max_retries: int = 3
    retry_backoff_seconds: float = 1.0
    circuit_breaker_threshold: int = 5

    # Compliance and governance
    data_retention_days: int = 365
    enable_data_lineage: bool = True
    compliance_region: str = "us"

    def __post_init__(self):
        """Validate configuration after initialization."""
        self._validate()

    def _validate(self):
        """Validate production configuration."""
        if self.environment == Environment.PRODUCTION:
            if not self.enable_authentication:
                raise ValueError("Authentication must be enabled in production")
            if not self.enable_encryption:
                raise ValueError("Encryption must be enabled in production")
            if not self.audit_logging:
                raise ValueError("Audit logging must be enabled in production")


class CircuitBreaker:
    """Circuit breaker pattern for resilience."""

    def __init__(self, failure_threshold: int = 5, timeout_seconds: float = 60.0):
        self.failure_threshold = failure_threshold
        self.timeout_seconds = timeout_seconds
        self.failure_count = 0
        self.last_failure_time = None
        self.state = "CLOSED"  # CLOSED, OPEN, HALF_OPEN

    async def call(self, func: Callable, *args, **kwargs):
        """Execute function with circuit breaker protection."""
        if self.state == "OPEN":
            if time.time() - self.last_failure_time > self.timeout_seconds:
                self.state = "HALF_OPEN"
            else:
                raise Exception("Circuit breaker is OPEN")

        try:
            result = await func(*args, **kwargs)
            if self.state == "HALF_OPEN":
                self.state = "CLOSED"
                self.failure_count = 0
            return result
        except Exception as e:
            self.failure_count += 1
            self.last_failure_time = time.time()

            if self.failure_count >= self.failure_threshold:
                self.state = "OPEN"

            raise e


class ProductionMetrics:
    """Production metrics collection and monitoring."""

    def __init__(self, config: ProductionConfig):
        self.config = config
        self.metrics = {}

        if PROMETHEUS_AVAILABLE and config.enable_metrics:
            self._setup_prometheus_metrics()

    def _setup_prometheus_metrics(self):
        """Setup Prometheus metrics."""
        self.metrics['requests_total'] = prometheus.Counter(
            'fsspeckit_requests_total',
            'Total number of requests',
            ['method', 'endpoint', 'status']
        )

        self.metrics['request_duration'] = prometheus.Histogram(
            'fsspeckit_request_duration_seconds',
            'Request duration in seconds',
            ['method', 'endpoint']
        )

        self.metrics['active_connections'] = prometheus.Gauge(
            'fsspeckit_active_connections',
            'Number of active connections'
        )

        self.metrics['data_processed_bytes'] = prometheus.Counter(
            'fsspeckit_data_processed_bytes_total',
            'Total data processed in bytes',
            ['operation', 'source']
        )

    def record_request(self, method: str, endpoint: str, status: str, duration: float):
        """Record request metrics."""
        if not self.config.enable_metrics or not PROMETHEUS_AVAILABLE:
            return

        self.metrics['requests_total'].labels(
            method=method, endpoint=endpoint, status=status
        ).inc()

        self.metrics['request_duration'].labels(
            method=method, endpoint=endpoint
        ).observe(duration)

    def record_data_processed(self, operation: str, source: str, bytes_processed: int):
        """Record data processing metrics."""
        if not self.config.enable_metrics or not PROMETHEUS_AVAILABLE:
            return

        self.metrics['data_processed_bytes'].labels(
            operation=operation, source=source
        ).inc(bytes_processed)

    def increment_active_connections(self):
        """Increment active connections gauge."""
        if self.config.enable_metrics and PROMETHEUS_AVAILABLE:
            self.metrics['active_connections'].inc()

    def decrement_active_connections(self):
        """Decrement active connections gauge."""
        if self.config.enable_metrics and PROMETHEUS_AVAILABLE:
            self.metrics['active_connections'].dec()


class ConfigurationManager:
    """Production configuration management."""

    def __init__(self, config_file: Optional[str] = None):
        self.config_file = config_file
        self.config = None

    def load_config(self, environment: str = None) -> ProductionConfig:
        """Load configuration from file or environment."""
        # Load from environment variable if specified
        env = environment or os.getenv("FSSPECKIT_ENV", "development")
        environment_enum = Environment(env.lower())

        # Load from config file if provided
        if self.config_file and Path(self.config_file).exists():
            config_dict = self._load_from_file()
        else:
            config_dict = self._load_from_environment()

        # Override with environment-specific settings
        env_overrides = self._get_environment_overrides(environment_enum)
        config_dict.update(env_overrides)

        # Create production config with validation
        self.config = ProductionConfig(environment=environment_enum, **config_dict)
        return self.config

    def _load_from_file(self) -> Dict[str, Any]:
        """Load configuration from YAML or JSON file."""
        if not YAML_AVAILABLE:
            raise ValueError("PyYAML is required for YAML configuration files")

        with open(self.config_file, 'r') as f:
            if self.config_file.endswith('.yaml') or self.config_file.endswith('.yml'):
                return yaml.safe_load(f)
            elif self.config_file.endswith('.json'):
                return json.load(f)
            else:
                raise ValueError(f"Unsupported config file format: {self.config_file}")

    def _load_from_environment(self) -> Dict[str, Any]:
        """Load configuration from environment variables."""
        config = {}

        # Map environment variables to config fields
        env_mappings = {
            'FSSPECKIT_MAX_WORKERS': ('max_workers', int),
            'FSSPECKIT_MEMORY_LIMIT_MB': ('memory_limit_mb', int),
            'FSSPECKIT_AWS_REGION': ('aws_region', str),
            'FSSPECKIT_MAX_RETRIES': ('max_retries', int),
            'FSSPECKIT_ENABLE_AUTHENTICATION': ('enable_authentication', lambda x: x.lower() == 'true'),
            'FSSPECKIT_ENABLE_ENCRYPTION': ('enable_encryption', lambda x: x.lower() == 'true'),
        }

        for env_var, (config_key, converter) in env_mappings.items():
            value = os.getenv(env_var)
            if value is not None:
                try:
                    config[config_key] = converter(value)
                except (ValueError, TypeError):
                    pass  # Skip invalid values

        return config

    def _get_environment_overrides(self, environment: Environment) -> Dict[str, Any]:
        """Get environment-specific configuration overrides."""
        overrides = {}

        if environment == Environment.PRODUCTION:
            overrides.update({
                'max_workers': 8,
                'memory_limit_mb': 16384,
                'enable_metrics': True,
                'enable_tracing': True,
                'max_retries': 5,
                'audit_logging': True,
            })
        elif environment == Environment.STAGING:
            overrides.update({
                'max_workers': 4,
                'memory_limit_mb': 8192,
                'enable_metrics': True,
                'enable_tracing': True,
                'max_retries': 3,
            })
        elif environment == Environment.DEVELOPMENT:
            overrides.update({
                'max_workers': 2,
                'memory_limit_mb': 4096,
                'enable_metrics': False,
                'enable_tracing': False,
                'max_retries': 1,
                'audit_logging': False,
            })

        return overrides


class ProductionService:
    """Production-ready service with all enterprise features."""

    def __init__(self, config: ProductionConfig):
        self.config = config

        # Setup logging
        self.logger = setup_structured_logging(
            name="production_service",
            level="INFO" if config.environment == Environment.PRODUCTION else "DEBUG",
            include_timestamp=True,
            include_extra_fields=True
        )

        # Setup metrics
        self.metrics = ProductionMetrics(config)

        # Setup circuit breakers
        self.circuit_breakers = {}

        # Initialize handlers
        self.duckdb_handler = DuckDBParquetHandler()

        # Health status
        self.is_healthy = True
        self.start_time = datetime.now()

        self.logger.info("Production service initialized", extra={
            "environment": config.environment.value,
            "deployment_mode": config.deployment_mode.value,
            "max_workers": config.max_workers
        })

    async def process_data_request(
        self,
        query: str,
        source_path: str,
        output_path: Optional[str] = None
    ) -> Dict[str, Any]:
        """Process a data request with production-grade features."""
        request_start_time = time.time()
        request_id = f"req_{int(request_start_time * 1000)}"

        self.logger.info("Processing data request", extra={
            "request_id": request_id,
            "query": query,
            "source_path": source_path,
            "output_path": output_path
        })

        try:
            # Validate inputs
            if not self._validate_request(query, source_path):
                raise ValueError("Invalid request parameters")

            # Increment active connections
            self.metrics.increment_active_connections()

            # Process request with circuit breaker
            circuit_breaker = self._get_circuit_breaker("data_processing")
            result = await circuit_breaker.call(
                self._execute_data_processing, query, source_path, output_path
            )

            # Record success metrics
            duration = time.time() - request_start_time
            self.metrics.record_request("process_data", source_path, "success", duration)

            self.logger.info("Request completed successfully", extra={
                "request_id": request_id,
                "duration": duration,
                "records_processed": result.get("record_count", 0)
            })

            return {
                "status": "success",
                "request_id": request_id,
                "result": result,
                "duration": duration
            }

        except Exception as e:
            # Record failure metrics
            duration = time.time() - request_start_time
            self.metrics.record_request("process_data", source_path, "error", duration)

            self.logger.error("Request failed", extra={
                "request_id": request_id,
                "error": str(e),
                "duration": duration
            })

            return {
                "status": "error",
                "request_id": request_id,
                "error": str(e),
                "duration": duration
            }

        finally:
            # Decrement active connections
            self.metrics.decrement_active_connections()

    def _validate_request(self, query: str, source_path: str) -> bool:
        """Validate request parameters."""
        try:
            # Validate SQL query
            if query:
                validate_sql_query(query)

            # Validate source path
            if not source_path:
                return False

            # Additional validation based on security requirements
            if self.config.enable_authentication and not self._authenticate_request():
                return False

            return True

        except Exception:
            return False

    def _authenticate_request(self) -> bool:
        """Authenticate request (simplified for demo)."""
        # In production, implement proper authentication
        return True

    def _get_circuit_breaker(self, service_name: str) -> CircuitBreaker:
        """Get or create circuit breaker for a service."""
        if service_name not in self.circuit_breakers:
            self.circuit_breakers[service_name] = CircuitBreaker(
                failure_threshold=self.config.circuit_breaker_threshold,
                timeout_seconds=60.0
            )
        return self.circuit_breakers[service_name]

    async def _execute_data_processing(
        self,
        query: str,
        source_path: str,
        output_path: Optional[str]
    ) -> Dict[str, Any]:
        """Execute the actual data processing."""
        processing_start = time.time()

        # Load data
        with self.duckdb_handler as handler:
            # Register dataset
            dataset_name = "source_data"
            handler.register_dataset(dataset_name, source_path)

            # Optimize query if enabled
            if query and self.config.environment != Environment.DEVELOPMENT:
                query = optimize_sql_query(query)

            # Execute query
            if query:
                result_table = handler.execute_sql(query)
            else:
                # Simple data load if no query
                result_table = handler.execute_sql(f"SELECT * FROM {dataset_name}")

            # Record data processing metrics
            bytes_processed = result_table.nbytes if result_table else 0
            self.metrics.record_data_processed("query", source_path, bytes_processed)

            # Save output if specified
            if output_path:
                self._save_result(result_table, output_path)

            processing_time = time.time() - processing_start

            return {
                "record_count": len(result_table) if result_table else 0,
                "bytes_processed": bytes_processed,
                "processing_time": processing_time,
                "output_path": output_path
            }

    def _save_result(self, table: pa.Table, output_path: str):
        """Save result to output path."""
        try:
            output_file = Path(output_path)
            output_file.parent.mkdir(parents=True, exist_ok=True)

            if output_path.endswith('.parquet'):
                pq.write_table(table, output_path, compression='snappy')
            elif output_path.endswith('.csv'):
                table.to_pandas().to_csv(output_path, index=False)
            else:
                raise ValueError(f"Unsupported output format: {output_path}")

        except Exception as e:
            self.logger.error(f"Failed to save result to {output_path}: {e}")
            raise

    async def health_check(self) -> Dict[str, Any]:
        """Perform comprehensive health check."""
        try:
            checks = {
                "service_status": "healthy",
                "uptime_seconds": (datetime.now() - self.start_time).total_seconds(),
                "memory_usage_mb": self._get_memory_usage(),
                "active_connections": self._get_active_connections(),
                "circuit_breakers": self._get_circuit_breaker_status(),
                "database_connection": await self._check_database_connection(),
                "storage_connectivity": await self._check_storage_connectivity(),
            }

            # Overall health status
            all_healthy = all(
                status in ["healthy", "connected", "ok"]
                for status in checks.values()
                if isinstance(status, str)
            )

            self.is_healthy = all_healthy
            checks["overall_status"] = "healthy" if all_healthy else "unhealthy"

            return checks

        except Exception as e:
            self.logger.error(f"Health check failed: {e}")
            return {
                "overall_status": "unhealthy",
                "error": str(e)
            }

    def _get_memory_usage(self) -> float:
        """Get current memory usage in MB."""
        try:
            import psutil
            process = psutil.Process()
            return process.memory_info().rss / 1024 / 1024
        except ImportError:
            return 0.0

    def _get_active_connections(self) -> int:
        """Get current active connections count."""
        if PROMETHEUS_AVAILABLE and self.config.enable_metrics:
            return int(self.metrics.metrics['active_connections']._value._value)
        return 0

    def _get_circuit_breaker_status(self) -> Dict[str, str]:
        """Get status of all circuit breakers."""
        return {
            name: breaker.state for name, breaker in self.circuit_breakers.items()
        }

    async def _check_database_connection(self) -> str:
        """Check database connectivity."""
        try:
            with self.duckdb_handler as handler:
                handler.execute_sql("SELECT 1")
            return "connected"
        except Exception:
            return "disconnected"

    async def _check_storage_connectivity(self) -> str:
        """Check storage connectivity."""
        try:
            # Simple connectivity check - in production, check actual storage
            return "connected"
        except Exception:
            return "disconnected"


def create_production_deployment_files():
    """Create production deployment configuration files."""

    print("📝 Creating production deployment files...")

    # Create deployment directory
    deploy_dir = Path("deployment")
    deploy_dir.mkdir(exist_ok=True)

    # Docker configuration
    dockerfile_content = """FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \\
    gcc \\
    g++ \\
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create non-root user
RUN useradd --create-home --shell /bin/bash fsspeckit
USER fsspeckit

# Expose port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \\
    CMD python -c "import requests; requests.get('http://localhost:8000/health')" || exit 1

# Start application
CMD ["python", "-m", "fsspeckit.production_server"]
"""

    dockerfile_path = deploy_dir / "Dockerfile"
    with open(dockerfile_path, 'w') as f:
        f.write(dockerfile_content)

    # Docker Compose configuration
    docker_compose_content = """version: '3.8'

services:
  fsspeckit-app:
    build: .
    ports:
      - "8000:8000"
    environment:
      - FSSPECKIT_ENV=production
      - FSSPECKIT_MAX_WORKERS=4
      - FSSPECKIT_MEMORY_LIMIT_MB=8192
      - FSSPECKIT_AWS_REGION=us-east-1
    volumes:
      - ./data:/app/data
      - ./logs:/app/logs
    restart: unless-stopped
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8000/health"]
      interval: 30s
      timeout: 10s
      retries: 3
      start_period: 40s

  prometheus:
    image: prom/prometheus:latest
    ports:
      - "9090:9090"
    volumes:
      - ./monitoring/prometheus.yml:/etc/prometheus/prometheus.yml
      - prometheus_data:/prometheus
    command:
      - '--config.file=/etc/prometheus/prometheus.yml'
      - '--storage.tsdb.path=/prometheus'
      - '--web.console.libraries=/etc/prometheus/console_libraries'
      - '--web.console.templates=/etc/prometheus/consoles'
    restart: unless-stopped

  grafana:
    image: grafana/grafana:latest
    ports:
      - "3000:3000"
    environment:
      - GF_SECURITY_ADMIN_PASSWORD=admin
    volumes:
      - grafana_data:/var/lib/grafana
      - ./monitoring/grafana:/etc/grafana/provisioning
    restart: unless-stopped

volumes:
  prometheus_data:
  grafana_data:
"""

    docker_compose_path = deploy_dir / "docker-compose.yml"
    with open(docker_compose_path, 'w') as f:
        f.write(docker_compose_content)

    # Kubernetes deployment
    k8s_deployment_content = """apiVersion: apps/v1
kind: Deployment
metadata:
  name: fsspeckit-app
  labels:
    app: fsspeckit
spec:
  replicas: 3
  selector:
    matchLabels:
      app: fsspeckit
  template:
    metadata:
      labels:
        app: fsspeckit
    spec:
      containers:
      - name: fsspeckit
        image: fsspeckit:latest
        ports:
        - containerPort: 8000
        env:
        - name: FSSPECKIT_ENV
          value: "production"
        - name: FSSPECKIT_MAX_WORKERS
          value: "4"
        - name: FSSPECKIT_MEMORY_LIMIT_MB
          value: "8192"
        resources:
          requests:
            memory: "4Gi"
            cpu: "1"
          limits:
            memory: "8Gi"
            cpu: "2"
        livenessProbe:
          httpGet:
            path: /health
            port: 8000
          initialDelaySeconds: 30
          periodSeconds: 10
        readinessProbe:
          httpGet:
            path: /health
            port: 8000
          initialDelaySeconds: 5
          periodSeconds: 5
        securityContext:
          runAsNonRoot: true
          runAsUser: 1000
          allowPrivilegeEscalation: false
          readOnlyRootFilesystem: true
        volumeMounts:
        - name: data
          mountPath: /app/data
        - name: logs
          mountPath: /app/logs
      volumes:
      - name: data
        persistentVolumeClaim:
          claimName: fsspeckit-data-pvc
      - name: logs
        persistentVolumeClaim:
          claimName: fsspeckit-logs-pvc
---
apiVersion: v1
kind: Service
metadata:
  name: fsspeckit-service
spec:
  selector:
    app: fsspeckit
  ports:
  - protocol: TCP
    port: 80
    targetPort: 8000
  type: LoadBalancer
"""

    k8s_deployment_path = deploy_dir / "k8s-deployment.yaml"
    with open(k8s_deployment_path, 'w') as f:
        f.write(k8s_deployment_content)

    # Production configuration
    production_config_content = """# Production Configuration
environment: production
deployment_mode: kubernetes

# Infrastructure
aws_region: us-east-1
azure_region: eastus
gcp_region: us-central1

# Performance
max_workers: 8
memory_limit_mb: 16384
connection_pool_size: 20
request_timeout_seconds: 60

# Security
enable_authentication: true
enable_encryption: true
audit_logging: true

# Monitoring
enable_metrics: true
enable_tracing: true
health_check_interval_seconds: 30

# Resilience
max_retries: 5
retry_backoff_seconds: 2.0
circuit_breaker_threshold: 10

# Compliance
data_retention_days: 2555  # 7 years
enable_data_lineage: true
compliance_region: us
"""

    config_path = deploy_dir / "production-config.yaml"
    with open(config_path, 'w') as f:
        f.write(production_config_content)

    print(f"   Created deployment files in {deploy_dir}:")
    print(f"     - Dockerfile")
    print(f"     - docker-compose.yml")
    print(f"     - k8s-deployment.yaml")
    print(f"     - production-config.yaml")


def demonstrate_configuration_management():
    """Demonstrate production configuration management."""

    print("\n⚙️ Configuration Management Demonstration")

    # Create sample config file
    config_content = """
environment: staging
deployment_mode: docker

max_workers: 4
memory_limit_mb: 8192
aws_region: us-west-2

enable_authentication: true
enable_metrics: true
max_retries: 3
"""

    config_file = "sample_config.yaml"
    with open(config_file, 'w') as f:
        f.write(config_content)

    try:
        # Load configuration
        config_manager = ConfigurationManager(config_file)
        config = config_manager.load_config()

        print(f"   Environment: {config.environment.value}")
        print(f"   Max workers: {config.max_workers}")
        print(f"   Memory limit: {config.memory_limit_mb}MB")
        print(f"   Authentication enabled: {config.enable_authentication}")
        print(f"   Metrics enabled: {config.enable_metrics}")

        print("   ✅ Configuration management successful")

    except Exception as e:
        print(f"   ❌ Configuration management failed: {e}")

    finally:
        # Cleanup
        try:
            os.remove(config_file)
        except:
            pass


async def demonstrate_production_service():
    """Demonstrate production service capabilities."""

    print("\n🚀 Production Service Demonstration")

    # Create production configuration
    config = ProductionConfig(
        environment=Environment.STAGING,
        deployment_mode=DeploymentMode.LOCAL,
        max_workers=2,
        memory_limit_mb=4096,
        enable_metrics=True,
        enable_authentication=True,
        max_retries=3
    )

    # Initialize service
    service = ProductionService(config)

    # Create sample data for testing
    sample_data = pa.table({
        "id": pa.array(range(1000)),
        "value": pa.array([i * 1.5 for i in range(1000)]),
        "category": pa.array([f"cat_{i % 10}" for i in range(1000)])
    })

    # Save sample data
    sample_path = "sample_data.parquet"
    pq.write_table(sample_data, sample_path)

    try:
        print("   1. Processing data requests...")

        # Process some requests
        requests = [
            ("SELECT category, AVG(value) as avg_value FROM source_data GROUP BY category", sample_path, "output1.parquet"),
            ("SELECT * FROM source_data WHERE value > 500", sample_path, "output2.parquet"),
            ("SELECT category, COUNT(*) as count FROM source_data GROUP BY category HAVING count > 50", sample_path, "output3.parquet")
        ]

        for query, source, output in requests:
            result = await service.process_data_request(query, source, output)
            status = "✅" if result["status"] == "success" else "❌"
            print(f"   {status} Query processed: {len(query.split())} words, {result.get('duration', 0):.2f}s")

        print("\n   2. Health check...")
        health = await service.health_check()
        print(f"   Overall status: {health['overall_status']}")
        print(f"   Uptime: {health['uptime_seconds']:.1f}s")
        print(f"   Memory usage: {health['memory_usage_mb']:.1f}MB")
        print(f"   Active connections: {health['active_connections']}")

        print("\n   3. Circuit breaker status...")
        for name, status in health['circuit_breakers'].items():
            print(f"   {name}: {status}")

        print("\n   ✅ Production service demonstration successful")

    except Exception as e:
        print(f"   ❌ Production service demonstration failed: {e}")

    finally:
        # Cleanup
        try:
            os.remove(sample_path)
            for i in range(1, 4):
                try:
                    os.remove(f"output{i}.parquet")
                except:
                    pass
        except:
            pass


def demonstrate_monitoring_setup():
    """Demonstrate monitoring and observability setup."""

    print("\n📊 Monitoring Setup Demonstration")

    # Check if monitoring dependencies are available
    monitoring_available = PROMETHEUS_AVAILABLE

    if monitoring_available:
        print("   ✅ Prometheus client available")

        # Create sample metrics
        request_counter = prometheus.Counter(
            'demo_requests_total',
            'Total demo requests',
            ['method', 'status']
        )

        request_histogram = prometheus.Histogram(
            'demo_request_duration_seconds',
            'Demo request duration'
        )

        # Simulate some metrics
        print("   Simulating metrics collection...")
        for i in range(10):
            request_counter.labels(method='GET', status='200').inc()
            request_histogram.observe(0.1 + (i * 0.01))

        print("   ✅ Metrics collection simulated")

        # Display current metrics values (simplified)
        print("   Sample metrics:")
        print(f"     Total requests: 10")
        print(f"     Average duration: ~0.15s")

    else:
        print("   ⚠️ Prometheus client not available - install with: pip install prometheus_client")

    print("\n   Monitoring Components:")
    print("     • Prometheus: Metrics collection and storage")
    print("     • Grafana: Visualization and dashboards")
    print("     • AlertManager: Alerting and notifications")
    print("     • Structured logging: Request tracing and debugging")


async def main():
    """Run all production pattern examples."""

    print("🏭 Production Patterns Example")
    print("=" * 60)
    print("This example demonstrates production-ready patterns and")
    print("best practices for deploying fsspeckit solutions.")

    try:
        # Run all demonstrations
        create_production_deployment_files()
        demonstrate_configuration_management()
        await demonstrate_production_service()
        demonstrate_monitoring_setup()

        print("\n" + "=" * 60)
        print("✅ Production patterns examples completed!")

        print(f"\n🎯 Production Takeaways:")
        print("• Configuration management for different environments")
        print("• Circuit breaker patterns for resilience")
        print("• Comprehensive monitoring and observability")
        print("• Security best practices and compliance")
        print("• Container orchestration with Docker and Kubernetes")
        print("• Health checks and graceful degradation")

        print(f"\n🏗️ Deployment Patterns:")
        print("• Docker containerization with health checks")
        print("• Kubernetes orchestration with auto-scaling")
        print("• Prometheus metrics collection")
        print("• Grafana dashboards and alerting")
        print("• Structured logging for debugging")
        print("• Graceful shutdown and restart handling")

        print(f"\n🔒 Security Considerations:")
        print("• Authentication and authorization")
        print("• Data encryption in transit and at rest")
        print("• Audit logging and compliance")
        print("• Network security and access controls")
        print("• Secrets management")
        print("• Vulnerability scanning and patching")

    except Exception as e:
        print(f"\n❌ Example failed: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())