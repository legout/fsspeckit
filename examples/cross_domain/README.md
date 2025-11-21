# Cross-Domain Integration Examples

This directory contains comprehensive examples that demonstrate how different fsspeckit packages work together to build complete, enterprise-grade data solutions.

## Overview

The cross-domain examples showcase the full power of the fsspeckit ecosystem by combining multiple packages to solve real-world business problems. These examples demonstrate production-ready patterns, best practices, and operational excellence.

## Prerequisites

Before diving into these examples, you should have completed:

- All Getting Started examples
- All Workflow examples
- All Advanced datasets examples
- SQL filters and schema management examples
- Common utilities examples
- Strong understanding of data engineering concepts
- Familiarity with production deployment patterns
- Understanding of monitoring and observability principles

## Available Examples

### 1. `end_to_end_pipeline.py` - Complete Data Pipeline
**Prerequisites**: All previous examples completed
**Time**: 60-90 minutes
**Complexity**: Advanced

**What you'll learn:**
- Building complete end-to-end data pipelines
- Integration of datasets, sql, common, and storage_options packages
- Production-grade error handling and retry logic
- Parallel processing across multiple data sources
- Comprehensive logging and monitoring
- Data quality validation and optimization

**Key concepts:**
- Cross-package orchestration and coordination
- Asynchronous data processing patterns
- Configuration management for different environments
- Circuit breaker patterns for resilience
- Comprehensive metrics collection and observability
- Production deployment patterns

**Package integrations demonstrated:**
- **datasets + sql**: Advanced data filtering and transformation
- **datasets + storage_options**: Multi-cloud data operations
- **common + datasets**: Parallel processing and optimization
- **sql + common**: Query validation and optimization
- **All packages**: Complete pipeline orchestration

**Real-world applications:**
- Enterprise data warehouse ETL pipelines
- Real-time analytics platforms
- Multi-cloud data integration solutions
- Data lake processing and optimization
- Business intelligence data preparation

### 2. `production_patterns.py` - Production Deployment Patterns
**Prerequisites**: Production deployment experience
**Time**: 45-75 minutes
**Complexity**: Advanced to Expert

**What you'll learn:**
- Production-grade configuration management
- Docker and Kubernetes deployment patterns
- Monitoring, observability, and alerting setup
- Security, authentication, and compliance
- Performance optimization and auto-scaling
- Testing strategies for production systems

**Key concepts:**
- Environment-specific configuration management
- Circuit breaker patterns for fault tolerance
- Comprehensive health checking and monitoring
- Prometheus metrics integration
- Graceful degradation and recovery
- Production security best practices

**Operational patterns demonstrated:**
- **Configuration**: Environment-aware settings and validation
- **Resilience**: Circuit breakers, retries, and graceful failure
- **Monitoring**: Metrics collection and health checking
- **Security**: Authentication, encryption, and audit logging
- **Deployment**: Docker containers and Kubernetes orchestration
- **Observability**: Structured logging and distributed tracing

**Real-world applications:**
- Enterprise data platform deployments
- Microservices architectures with data components
- High-availability data processing systems
- Compliance-ready data solutions
- Multi-region data deployments

## Cross-Domain Integration Patterns

### Pattern 1: Data Processing + Storage

```python
# Combine datasets package with storage_options for cloud operations
from fsspeckit.datasets import DuckDBParquetHandler
from fsspeckit.storage_options import AwsStorageOptions

# Configure cloud storage
aws_options = AwsStorageOptions(region="us-east-1")

# Process data across multiple clouds
with DuckDBParquetHandler() as handler:
    # Register cloud datasets
    handler.register_dataset("aws_data", "s3://bucket/data/", storage_options=aws_options.to_dict())

    # Cross-cloud analytics
    result = handler.execute_sql("""
        SELECT * FROM aws_data
        WHERE date >= '2024-01-01'
    """)
```

### Pattern 2: SQL Optimization + Performance

```python
# Combine sql package with parallel processing from common
from fsspeckit.sql import optimize_sql_query, validate_sql_query
from fsspeckit.common import run_parallel

# Optimize and validate queries in parallel
queries = [
    "SELECT * FROM data WHERE category = 'A'",
    "SELECT * FROM data WHERE category = 'B'",
    "SELECT * FROM data WHERE category = 'C'"
]

# Validate and optimize queries
def process_query(query):
    validate_sql_query(query)
    return optimize_sql_query(query)

optimized_queries = run_parallel(process_query, queries, max_workers=4)
```

### Pattern 3: Monitoring + Data Operations

```python
# Combine structured logging with data processing
from fsspeckit.common import setup_structured_logging
from fsspeckit.datasets import DuckDBParquetHandler

# Setup comprehensive logging
logger = setup_structured_logging(
    name="data_pipeline",
    level="INFO",
    include_timestamp=True,
    include_extra_fields=True
)

# Process data with detailed logging
with DuckDBParquetHandler() as handler:
    logger.info("Starting data processing", extra={
        "operation": "data_query",
        "dataset": "sales_data"
    })

    result = handler.execute_sql("SELECT COUNT(*) FROM sales_data")

    logger.info("Query completed", extra={
        "record_count": result[0][0],
        "operation": "data_query",
        "status": "success"
    })
```

### Pattern 4: Configuration + Multi-Package Orchestration

```python
# Configuration-driven multi-package operations
class DataPlatform:
    def __init__(self, config):
        self.config = config
        self.datasets_handler = DuckDBParquetHandler()
        self.logger = setup_structured_logging(config.logging)

    def process_pipeline(self, pipeline_config):
        # Orchestrate multiple packages
        for stage in pipeline_config.stages:
            logger.info(f"Executing stage: {stage.name}")

            if stage.type == "sql_query":
                result = self._execute_sql_stage(stage)
            elif stage.type == "data_optimization":
                result = self._execute_optimization_stage(stage)
            elif stage.type == "cloud_sync":
                result = self._execute_sync_stage(stage)
```

## Advanced Integration Scenarios

### Scenario 1: Multi-Cloud Data Platform

```python
# Enterprise multi-cloud platform combining all packages
class MultiCloudDataPlatform:
    def __init__(self, config):
        # Storage options for each provider
        self.aws_options = AwsStorageOptions(**config.aws)
        self.azure_options = AzureStorageOptions(**config.azure)
        self.gcp_options = GcsStorageOptions(**config.gcp)

        # Core processing
        self.handler = DuckDBParquetHandler()
        self.logger = setup_structured_logging(config.logging)

    async def sync_all_clouds(self, query: str):
        # Execute query across all clouds simultaneously
        tasks = [
            self._process_cloud_data(query, "aws", self.aws_options),
            self._process_cloud_data(query, "azure", self.azure_options),
            self._process_cloud_data(query, "gcp", self.gcp_options)
        ]

        results = await asyncio.gather(*tasks)
        return self._combine_results(results)
```

### Scenario 2: Real-Time Analytics Platform

```python
# Real-time platform with monitoring and resilience
class RealTimeAnalyticsPlatform:
    def __init__(self, config):
        self.metrics = ProductionMetrics(config)
        self.circuit_breaker = CircuitBreaker()
        self.handler = DuckDBParquetHandler()

    async def process_stream(self, data_stream):
        async for batch in data_stream:
            try:
                # Process with circuit breaker protection
                result = await self.circuit_breaker.call(
                    self._process_batch, batch
                )

                # Record metrics
                self.metrics.record_data_processing(
                    "stream_batch", "real_time", len(batch)
                )

            except Exception as e:
                self.logger.error(f"Batch processing failed: {e}")
                # Continue processing next batch with resilience
```

### Scenario 3: Compliance-Ready Data Pipeline

```python
# Compliance-focused pipeline with audit logging
class ComplianceDataPipeline:
    def __init__(self, config):
        self.config = config
        self.audit_logger = setup_structured_logging(
            "audit",
            level="INFO",
            include_extra_fields=True
        )

    async def process_with_compliance(self, data, compliance_rules):
        # Log all data access for compliance
        self.audit_logger.info("Data processing started", extra={
            "data_size": len(data),
            "compliance_rules": compliance_rules,
            "user": self.config.current_user,
            "timestamp": datetime.now().isoformat()
        })

        # Process with compliance checks
        filtered_data = self._apply_compliance_rules(data, compliance_rules)

        # Log processing completion
        self.audit_logger.info("Data processing completed", extra={
            "records_processed": len(filtered_data),
            "records_filtered": len(data) - len(filtered_data)
        })

        return filtered_data
```

## Performance and Optimization

### Cross-Domain Performance Patterns

1. **Parallel Processing Optimization**
   ```python
   # Optimize across packages using parallel execution
   from fsspeckit.common import run_parallel

   def parallel_data_processing(datasets, queries):
       return run_parallel(
           lambda args: process_dataset(*args),
           [(dataset, query) for dataset, query in zip(datasets, queries)],
           max_workers=min(len(datasets), 8)
       )
   ```

2. **Memory Optimization Across Packages**
   ```python
   # Coordinate memory usage between packages
   class MemoryAwareProcessor:
       def __init__(self, memory_limit_mb):
           self.memory_limit = memory_limit_mb
           self.handler = DuckDBParquetHandler()

       def process_with_memory_management(self, large_dataset):
           # Process in chunks to manage memory
           chunk_size = self._calculate_optimal_chunk_size(large_dataset)
           return self._process_in_chunks(large_dataset, chunk_size)
   ```

3. **Query Optimization Pipeline**
   ```python
   # Multi-stage query optimization
   def optimize_query_pipeline(sql_query, schema):
       # Validate query structure
       validate_sql_query(sql_query)

       # Optimize for specific backend
       optimized_sql = optimize_sql_query(sql_query)

       # Convert to execution plan
       execution_plan = create_execution_plan(optimized_sql, schema)

       return execution_plan
   ```

## Testing Cross-Domain Integrations

### Integration Testing Strategies

1. **Package Integration Tests**
   ```python
   # Test package interactions
   def test_datasets_sql_integration():
       with DuckDBParquetHandler() as handler:
           # Test SQL query optimization
           original_query = "SELECT * FROM data WHERE value > 100"
           optimized_query = optimize_sql_query(original_query)

           # Verify optimized query produces same results
           original_result = handler.execute_sql(original_query)
           optimized_result = handler.execute_sql(optimized_query)

           assert len(original_result) == len(optimized_result)
   ```

2. **End-to-End Pipeline Tests**
   ```python
   # Test complete pipelines
   async def test_end_to_end_pipeline():
       config = create_test_config()
       pipeline = EndToEndPipeline(config)

       # Run pipeline with test data
       result = await pipeline.run_pipeline()

       # Verify results
       assert result["status"] == "success"
       assert result["metrics"]["execution_summary"]["total_records_processed"] > 0
   ```

3. **Performance Tests**
   ```python
   # Performance testing across packages
   def benchmark_cross_domain_performance():
       start_time = time.time()

       # Execute complex cross-domain operation
       result = execute_complex_pipeline()

       duration = time.time() - start_time
       throughput = result["record_count"] / duration

       # Verify performance meets requirements
       assert throughput > MINIMUM_THROUGHPUT
       assert duration < MAXIMUM_DURATION
   ```

## Best Practices

### Cross-Domain Integration Best Practices

1. **Configuration Management**
   - Use environment-specific configuration files
   - Validate all configurations at startup
   - Implement configuration versioning
   - Support both file and environment variable inputs

2. **Error Handling**
   - Implement consistent error handling across packages
   - Use structured logging with correlation IDs
   - Implement circuit breakers for external dependencies
   - Provide meaningful error messages and recovery suggestions

3. **Performance Optimization**
   - Profile cross-package interactions
   - Optimize data transfer between packages
   - Use appropriate parallel processing strategies
   - Monitor resource usage across all packages

4. **Security**
   - Implement consistent authentication across packages
   - Use encryption for data in transit and at rest
   - Validate all inputs and sanitize outputs
   - Implement audit logging for compliance

5. **Monitoring and Observability**
   - Collect metrics from all packages
   - Implement distributed tracing
   - Use consistent logging formats
   - Set up appropriate alerts and notifications

## Production Deployment

### Deployment Architecture

```yaml
# Example production architecture
services:
  # Core data processing service
  data-processor:
    image: fsspeckit/processor:latest
    replicas: 3
    resources:
      requests:
        memory: 4Gi
        cpu: 1
      limits:
        memory: 8Gi
        cpu: 2
    env:
      - name: FSSPECKIT_ENV
        value: production
      - name: FSSPECKIT_MAX_WORKERS
        value: "4"

  # Monitoring service
  prometheus:
    image: prom/prometheus:latest
    ports:
      - "9090:9090"
    volumes:
      - ./monitoring/prometheus.yml:/etc/prometheus/prometheus.yml

  # Visualization service
  grafana:
    image: grafana/grafana:latest
    ports:
      - "3000:3000"
    environment:
      - GF_SECURITY_ADMIN_PASSWORD=admin
```

### Scaling Strategies

1. **Horizontal Scaling**
   - Deploy multiple instances of stateless services
   - Use load balancers for request distribution
   - Implement auto-scaling based on metrics

2. **Vertical Scaling**
   - Optimize memory usage for large datasets
   - Use appropriate CPU allocation for compute-intensive tasks
   - Monitor resource utilization and adjust accordingly

3. **Data Partitioning**
   - Partition data across multiple storage systems
   - Use appropriate sharding strategies
   - Implement data locality optimizations

## Monitoring and Observability

### Comprehensive Monitoring Setup

```python
# Example monitoring configuration
class ProductionMonitoring:
    def __init__(self, config):
        # Metrics collection
        self.metrics = ProductionMetrics(config)

        # Health checking
        self.health_checker = HealthChecker(config)

        # Alerting
        self.alert_manager = AlertManager(config.alerting)

    def setup_monitoring(self):
        # Setup Prometheus metrics
        self.setup_prometheus_metrics()

        # Setup health checks
        self.setup_health_checks()

        # Setup alerts
        self.setup_alerts()

    def setup_prometheus_metrics(self):
        # Custom metrics for cross-domain operations
        self.cross_domain_operations = prometheus.Counter(
            'cross_domain_operations_total',
            'Total cross-domain operations',
            ['operation_type', 'packages_involved']
        )
```

### Alerting Strategies

1. **Performance Alerts**
   - Query execution time thresholds
   - Memory usage alerts
   - Error rate thresholds

2. **Business Alerts**
   - Data freshness alerts
   - Data quality issues
   - Pipeline completion failures

3. **Infrastructure Alerts**
   - Service availability
   - Resource exhaustion
   - Network connectivity issues

## Troubleshooting

### Common Cross-Domain Issues

1. **Package Version Conflicts**
   - Ensure compatible package versions
   - Use dependency management tools
   - Implement compatibility checks

2. **Configuration Issues**
   - Validate configuration at startup
   - Provide clear error messages
   - Implement configuration drift detection

3. **Performance Issues**
   - Profile cross-package interactions
   - Identify bottlenecks in integration points
   - Optimize data transfer between packages

### Debugging Strategies

1. **Structured Logging**
   ```python
   # Use correlation IDs for request tracing
   correlation_id = str(uuid.uuid4())
   logger.info("Processing request", extra={
       "correlation_id": correlation_id,
       "operation": "data_processing",
       "packages_used": ["datasets", "sql", "common"]
   })
   ```

2. **Distributed Tracing**
   ```python
   # Trace operations across packages
   with trace_span("cross_domain_operation"):
       datasets_result = process_with_datasets(data)
       sql_result = process_with_sql(datasets_result)
       final_result = process_with_common(sql_result)
   ```

3. **Performance Profiling**
   ```python
   # Profile cross-domain operations
   import cProfile
   cProfile.run('cross_domain_function()', 'profile_output.prof')
   ```

## Next Steps

After mastering cross-domain integrations, you'll be ready for:

### Real-World Implementations
- **Enterprise Data Platforms**: Complete organizational data solutions
- **Multi-Cloud Architectures**: Global data distribution strategies
- **Real-Time Analytics**: Production streaming analytics platforms
- **Machine Learning Pipelines**: End-to-end ML data preparation

### Advanced Topics
- **Custom Package Development**: Extending the fsspeckit ecosystem
- **Performance Engineering**: Advanced optimization techniques
- **Security and Compliance**: Enterprise-grade security implementations
- **DevOps and SRE**: Operations and reliability engineering

## Dependencies

```bash
# Core fsspeckit packages
pip install fsspeckit[datasets,sql,common,storage_options]

# Production dependencies
pip install pyarrow duckdb pandas polars

# Monitoring and observability
pip install prometheus-client structlog

# Deployment and orchestration
pip install docker kubernetes pyyaml

# Testing and development
pip install pytest pytest-asyncio pytest-cov

# Security and compliance
pip install cryptography pydantic
```

## Getting Help

- **Documentation**: Complete fsspeckit API reference and integration guides
- **Examples**: Additional examples in parent directories
- **Community**: GitHub discussions and integration-specific issues
- **Support**: Commercial support options for enterprise deployments
- **Training**: Advanced workshops and production deployment training

Remember: Cross-domain integration requires understanding of individual packages and how they work together. Start with simple integrations and gradually build up to complex, production-ready solutions!