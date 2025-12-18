# Performance Tuning Guide: Memory Monitoring Optimization

This guide helps you optimize the enhanced PyArrow memory monitoring system for different performance requirements and use cases.

## Performance Overview

The enhanced memory monitoring system is designed to provide comprehensive memory tracking with minimal overhead. Understanding the performance characteristics helps you make informed configuration decisions.

### Performance Characteristics

| Configuration | Overhead | Monitoring Frequency | Memory Protection | Best For |
|---------------|----------|---------------------|-------------------|----------|
| **Basic** (PyArrow only) | ~1% | Per operation | PyArrow allocation | Development, low-risk operations |
| **Standard** (Dual tracking) | ~2-3% | Per operation | PyArrow + Process RSS | Production workloads |
| **Enhanced** (Full monitoring) | ~3-5% | Per operation | PyArrow + Process + System | Safety-critical operations |
| **Optimized** (Throttled) | ~1-2% | Every N operations | PyArrow + Process RSS | High-throughput processing |

## Configuration Strategies

### 1. Development Environment

**Priority**: Minimal overhead, easy debugging

```python
# Low-overhead monitoring for development
from fsspeckit.datasets.pyarrow.memory import MemoryMonitor

# Conservative limits, basic monitoring
dev_monitor = MemoryMonitor(
    max_pyarrow_mb=512,      # Conservative limit
    max_process_memory_mb=1024, # Total process limit
    min_system_available_mb=256, # Keep system responsive
)

# Usage in development
for chunk in process_in_chunks(dataset, memory_monitor=dev_monitor):
    # Process with minimal overhead
    process_chunk(chunk)
    
    # Check pressure every 10 chunks (reduce overhead)
    if dev_monitor.should_check_memory(chunks_processed, check_interval=10):
        pressure = dev_monitor.check_memory_pressure()
        if pressure.value in ['warning', 'critical']:
            print(f"Memory pressure: {pressure.value}")
```

### 2. Production High-Throughput

**Priority**: Performance, efficiency

```python
# Optimized for production performance
from fsspeckit.datasets.pyarrow.memory import MemoryMonitor, MemoryPressureLevel

# Higher limits, less frequent checking
prod_monitor = MemoryMonitor(
    max_pyarrow_mb=4096,     # Generous PyArrow limit
    max_process_memory_mb=8192, # Large process limit
    min_system_available_mb=1024, # Keep system stable
)

# High-throughput processing
for chunk in process_in_chunks(dataset, memory_monitor=prod_monitor):
    # Process efficiently
    result = process_chunk(chunk)
    
    # Check memory less frequently for better performance
    if prod_monitor.should_check_memory(chunks_processed, check_interval=50):
        pressure = prod_monitor.check_memory_pressure()
        
        # Only log warnings/critical to reduce overhead
        if pressure in [MemoryPressureLevel.WARNING, MemoryPressureLevel.CRITICAL]:
            logger.warning(f"Memory pressure detected: {pressure.value}")
```

### 3. Safety-Critical Operations

**Priority**: Maximum protection, early warning

```python
# Maximum monitoring for safety-critical operations
from fsspeckit.datasets.pyarrow.memory import MemoryMonitor, MemoryPressureLevel
import gc

# Conservative limits, frequent checking
safety_monitor = MemoryMonitor(
    max_pyarrow_mb=1024,     # Conservative limit
    max_process_memory_mb=2048, # Conservative process limit
    min_system_available_mb=512, # Keep system responsive
)

# Safety-critical processing
for chunk in process_in_chunks(dataset, memory_monitor=safety_monitor):
    # Frequent memory checks
    pressure = safety_monitor.check_memory_pressure()
    
    if pressure == MemoryPressureLevel.WARNING:
        # Early intervention
        logger.info("Memory usage high, triggering cleanup")
        gc.collect()
        
    elif pressure == MemoryPressureLevel.CRITICAL:
        # Aggressive intervention
        logger.warning("Memory usage critical, reducing operation size")
        # Could reduce chunk size dynamically
        
    elif pressure == MemoryPressureLevel.EMERGENCY:
        # Fail-safe
        raise MemoryError("Cannot continue safely with current memory constraints")
    
    result = process_chunk(chunk)
```

### 4. Container/Cloud Environments

**Priority**: Resource awareness, limits enforcement

```python
import os
import psutil

def create_cloud_monitor():
    """Create monitor optimized for cloud/container environments."""
    
    # Get container memory limit (Kubernetes, Docker, etc.)
    container_limit = os.environ.get('CONTAINER_MEMORY_LIMIT')
    
    if container_limit:
        # Parse container memory limit
        if container_limit.lower().endswith('g'):
            total_memory_mb = int(container_limit[:-1]) * 1024
        elif container_limit.lower().endswith('m'):
            total_memory_mb = int(container_limit[:-1])
        else:
            total_memory_mb = int(container_limit)
    else:
        # Fallback to system memory
        total_memory_mb = psutil.virtual_memory().total // (1024 * 1024)
    
    # Allocate memory with safety margins
    return MemoryMonitor(
        max_pyarrow_mb=int(total_memory_mb * 0.4),    # 40% for PyArrow
        max_process_memory_mb=int(total_memory_mb * 0.7), # 70% total process
        min_system_available_mb=int(total_memory_mb * 0.1), # 10% for system
    )

# Usage in cloud environment
cloud_monitor = create_cloud_monitor()
```

## Memory Check Optimization

### Understanding Check Frequency

The `should_check_memory()` method helps balance monitoring accuracy with performance:

```python
# Different check frequencies for different use cases

# High frequency: Maximum safety, higher overhead
if monitor.should_check_memory(chunks_processed, check_interval=1):
    pressure = monitor.check_memory_pressure()

# Medium frequency: Balanced approach
if monitor.should_check_memory(chunks_processed, check_interval=10):
    pressure = monitor.check_memory_pressure()

# Low frequency: Maximum performance, reduced safety
if monitor.should_check_memory(chunks_processed, check_interval=100):
    pressure = monitor.check_memory_pressure()
```

### Adaptive Frequency Based on Data Size

```python
def adaptive_memory_monitoring(dataset_size_rows, base_check_interval=10):
    """Adjust memory check frequency based on dataset size."""
    
    # Larger datasets can use less frequent checking
    if dataset_size_rows > 1_000_000:
        check_interval = base_check_interval * 5  # Less frequent for large datasets
    elif dataset_size_rows > 100_000:
        check_interval = base_check_interval * 2  # Moderate frequency
    else:
        check_interval = base_check_interval  # Frequent for small datasets
    
    return check_interval

# Usage
dataset_size = large_dataset.count_rows()
check_interval = adaptive_memory_monitoring(dataset_size)

for chunk in process_in_chunks(dataset, memory_monitor=monitor):
    chunks_processed += 1
    
    if monitor.should_check_memory(chunks_processed, check_interval):
        pressure = monitor.check_memory_pressure()
        # ... handle pressure ...
```

## Memory Limit Calculation

### Environment-Based Limits

```python
def calculate_optimal_limits():
    """Calculate optimal memory limits based on system characteristics."""
    
    import psutil
    
    # Get system information
    total_memory_mb = psutil.virtual_memory().total // (1024 * 1024)
    available_memory_mb = psutil.virtual_memory().available // (1024 * 1024)
    cpu_count = psutil.cpu_count()
    
    # Calculate limits based on system capacity
    if total_memory_mb < 4096:  # Less than 4GB
        # Very constrained environment
        pyarrow_limit = 512
        process_limit = 1024
        system_reserve = 256
    elif total_memory_mb < 8192:  # Less than 8GB
        # Moderately constrained
        pyarrow_limit = 1024
        process_limit = 2048
        system_reserve = 512
    elif total_memory_mb < 16384:  # Less than 16GB
        # Comfortable environment
        pyarrow_limit = 2048
        process_limit = 4096
        system_reserve = 1024
    else:
        # High-capacity environment
        pyarrow_limit = 4096
        process_limit = 8192
        system_reserve = 2048
    
    # Adjust based on available memory (not just total)
    if available_memory_mb < system_reserve * 2:
        # Reduce limits if system is already memory-constrained
        pyarrow_limit = int(pyarrow_limit * 0.5)
        process_limit = int(process_limit * 0.7)
    
    return MemoryMonitor(
        max_pyarrow_mb=pyarrow_limit,
        max_process_memory_mb=process_limit,
        min_system_available_mb=system_reserve,
    )
```

### Application-Specific Limits

```python
def create_application_monitor(app_type="general"):
    """Create monitor tuned for specific application types."""
    
    monitors = {
        "data_pipeline": MemoryMonitor(
            max_pyarrow_mb=2048,
            max_process_memory_mb=4096,
            min_system_available_mb=1024,
        ),
        
        "analytics": MemoryMonitor(
            max_pyarrow_mb=1024,
            max_process_memory_mb=2048,
            min_system_available_mb=512,
        ),
        
        "ml_training": MemoryMonitor(
            max_pyarrow_mb=4096,
            max_process_memory_mb=8192,
            min_system_available_mb=2048,
        ),
        
        "batch_processing": MemoryMonitor(
            max_pyarrow_mb=1024,
            max_process_memory_mb=2048,
            min_system_available_mb=512,
        ),
    }
    
    return monitors.get(app_type, MemoryMonitor())

# Usage
pipeline_monitor = create_application_monitor("data_pipeline")
```

## Performance Monitoring and Profiling

### Memory Monitoring Overhead Measurement

```python
import time
from fsspeckit.datasets.pyarrow.memory import MemoryMonitor

def measure_monitoring_overhead():
    """Measure the performance overhead of memory monitoring."""
    
    # Create test monitor
    monitor = MemoryMonitor(max_pyarrow_mb=1024)
    
    # Baseline: operations without monitoring
    start_time = time.perf_counter()
    for i in range(1000):
        # Simulate processing work
        _ = [j**2 for j in range(1000)]
    baseline_time = time.perf_counter() - start_time
    
    # With monitoring: frequent checks
    start_time = time.perf_counter()
    for i in range(1000):
        # Simulate processing work
        _ = [j**2 for j in range(1000)]
        # Check memory every iteration (high overhead)
        if monitor.should_check_memory(i, check_interval=1):
            monitor.check_memory_pressure()
    frequent_time = time.perf_counter() - start_time
    
    # With monitoring: infrequent checks
    start_time = time.perf_counter()
    for i in range(1000):
        # Simulate processing work
        _ = [j**2 for j in range(1000)]
        # Check memory every 100 iterations (low overhead)
        if monitor.should_check_memory(i, check_interval=100):
            monitor.check_memory_pressure()
    infrequent_time = time.perf_counter() - start_time
    
    # Calculate overhead percentages
    frequent_overhead = (frequent_time / baseline_time - 1) * 100
    infrequent_overhead = (infrequent_time / baseline_time - 1) * 100
    
    print(f"Baseline time: {baseline_time:.3f}s")
    print(f"Frequent monitoring overhead: {frequent_overhead:.1f}%")
    print(f"Infrequent monitoring overhead: {infrequent_overhead:.1f}%")
    
    return frequent_overhead, infrequent_overhead
```

### Performance-Optimized Processing Pipeline

```python
class OptimizedMemoryAwarePipeline:
    """Performance-optimized pipeline with adaptive memory monitoring."""
    
    def __init__(self, performance_mode="balanced"):
        self.performance_mode = performance_mode
        self.monitor = self._create_optimized_monitor()
        self.stats = {
            "chunks_processed": 0,
            "memory_checks": 0,
            "pressure_events": 0
        }
    
    def _create_optimized_monitor(self):
        """Create monitor optimized for performance mode."""
        configs = {
            "performance": {  # Maximum performance
                "check_interval": 50,
                "pyarrow_limit": 4096,
                "process_limit": 8192,
            },
            "balanced": {     # Balanced approach
                "check_interval": 20,
                "pyarrow_limit": 2048,
                "process_limit": 4096,
            },
            "safety": {       # Maximum safety
                "check_interval": 5,
                "pyarrow_limit": 1024,
                "process_limit": 2048,
            }
        }
        
        config = configs.get(self.performance_mode, configs["balanced"])
        
        return MemoryMonitor(
            max_pyarrow_mb=config["pyarrow_limit"],
            max_process_memory_mb=config["process_limit"],
            min_system_available_mb=512,
        )
    
    def process_with_monitoring(self, dataset):
        """Process dataset with optimized memory monitoring."""
        
        for chunk in process_in_chunks(dataset, memory_monitor=self.monitor):
            self.stats["chunks_processed"] += 1
            
            # Optimized memory checking
            if self.monitor.should_check_memory(
                self.stats["chunks_processed"], 
                check_interval=self._get_check_interval()
            ):
                self.stats["memory_checks"] += 1
                pressure = self.monitor.check_memory_pressure()
                
                if pressure != MemoryPressureLevel.NORMAL:
                    self.stats["pressure_events"] += 1
                    self._handle_pressure(pressure)
            
            # Process the chunk
            yield self.process_chunk(chunk)
    
    def _get_check_interval(self):
        """Get dynamic check interval based on performance mode."""
        intervals = {
            "performance": 50,
            "balanced": 20,
            "safety": 5
        }
        return intervals.get(self.performance_mode, 20)
    
    def _handle_pressure(self, pressure):
        """Handle memory pressure events."""
        if pressure == MemoryPressureLevel.WARNING:
            # Minimal intervention for performance
            pass
        elif pressure == MemoryPressureLevel.CRITICAL:
            # Moderate intervention
            import gc
            gc.collect()
        elif pressure == MemoryPressureLevel.EMERGENCY:
            # Maximum intervention
            raise MemoryError("Emergency memory pressure detected")
    
    def get_performance_stats(self):
        """Get performance statistics."""
        total_ops = self.stats["chunks_processed"]
        check_rate = self.stats["memory_checks"] / total_ops if total_ops > 0 else 0
        pressure_rate = self.stats["pressure_events"] / total_ops if total_ops > 0 else 0
        
        return {
            **self.stats,
            "memory_check_rate": check_rate,
            "pressure_event_rate": pressure_rate,
            "performance_mode": self.performance_mode
        }

# Usage examples
high_perf_pipeline = OptimizedMemoryAwarePipeline("performance")
balanced_pipeline = OptimizedMemoryAwarePipeline("balanced")
safety_pipeline = OptimizedMemoryAwarePipeline("safety")

# Process with different performance characteristics
for chunk in balanced_pipeline.process_with_monitoring(large_dataset):
    process_chunk(chunk)

stats = balanced_pipeline.get_performance_stats()
print(f"Processed {stats['chunks_processed']} chunks with {stats['memory_check_rate']:.1%} check rate")
```

## Best Practices Summary

### 1. Choose Appropriate Monitoring Level

- **Development**: Basic monitoring, frequent checks
- **Production**: Standard monitoring, balanced frequency
- **Critical Operations**: Enhanced monitoring, frequent checks
- **High-Throughput**: Optimized monitoring, infrequent checks

### 2. Set Realistic Limits

```python
# Good: Based on system capacity
import psutil
available_mb = psutil.virtual_memory().available // (1024 * 1024)
monitor = MemoryMonitor(
    max_pyarrow_mb=int(available_mb * 0.4),
    max_process_memory_mb=int(available_mb * 0.7),
)

# Avoid: Arbitrary limits
monitor = MemoryMonitor(
    max_pyarrow_mb=100000,  # Too high
    max_process_memory_mb=500000,  # Unrealistic
)
```

### 3. Optimize Check Frequency

```python
# Good: Based on workload characteristics
check_interval = 10 if dataset_size < 100000 else 50

# Avoid: Fixed frequency regardless of workload
check_interval = 10  # May be too frequent for large datasets
```

### 4. Monitor Performance Impact

```python
# Regularly measure monitoring overhead
overhead_frequent, overhead_infrequent = measure_monitoring_overhead()
print(f"Memory monitoring overhead: {overhead_infrequent:.1f}%")
```

### 5. Use Graceful Degradation

```python
# Implement adaptive strategies
if pressure == MemoryPressureLevel.CRITICAL:
    # Reduce batch size, trigger GC, but continue processing
    current_batch_size = max(current_batch_size // 2, 1000)
    gc.collect()
```

By following these optimization strategies, you can achieve the right balance between memory protection and performance for your specific use case.