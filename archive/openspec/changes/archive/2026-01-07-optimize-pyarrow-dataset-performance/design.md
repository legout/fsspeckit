# Design: Optimize PyArrow Dataset Performance

## Context
The PyArrow dataset operations have severe performance bottlenecks that make them unusable for large datasets. Current implementation uses Python loops and converts entire tables to Python lists, causing:
- O(n²) complexity in deduplication operations
- Memory issues with large datasets
- Poor scalability (>1GB datasets become problematic)

## Goals / Non-Goals

### Goals
- Achieve 10-100x performance improvement for large dataset operations
- Enable processing of datasets larger than available system memory
- Maintain identical results to current implementation (correctness first)
- Add performance monitoring and metrics

### Non-Goals
- Changing the external API or user interface
- Modifying DuckDB backend performance (separate concern)
- Implementing distributed processing capabilities
- Adding new data formats or compression algorithms

## Decisions

### Decision 1: Replace Python Loops with Vectorized Operations
**Problem**: Current deduplication uses `table.to_pylist()` and Python loops
**Solution**: Use PyArrow's built-in vectorized operations
**Rationale**: PyArrow operations are optimized in C++ and significantly faster than Python loops

**Implementation Approach**:
```python
# Instead of:
for row in table.to_pylist():
    key = tuple(row[col] for col in key_columns)
    if key not in seen_keys:
        deduped_rows.append(row)

# Use PyArrow operations:
unique_indices = pc.drop_duplicates(
    table.select(key_columns)
).to_pylist()
```

### Decision 2: Implement Chunked Processing for Memory Management
**Problem**: Loading entire large datasets into memory causes OOM errors
**Solution**: Process data in configurable chunks with configurable memory limits
**Rationale**: Bounded memory usage enables processing of arbitrarily large datasets

**Implementation Strategy**:
- Default chunk size: 1M rows (configurable)
- Memory limit: 2GB peak usage (configurable)
- Progress tracking for long operations
- Graceful handling of partial failures

### Decision 3: Streaming Processing for Very Large Datasets
**Problem**: Even chunked processing may not work for datasets >> available memory
**Solution**: Implement true streaming for operations that can be done incrementally
**Rationale**: Some operations (like deduplication) can be done in streaming fashion

**Streaming Approach**:
- Use PyArrow IPC format for streaming reads
- Process individual row groups without loading full files
- Maintain incremental state (e.g., seen keys) across chunks
- Write results incrementally

### Decision 4: Performance Monitoring and Metrics
**Problem**: Users have no visibility into operation characteristics
**Solution**: Add comprehensive performance metrics collection
**Rationale**: Enables optimization and capacity planning

**Metrics to Collect**:
- Processing time (wall clock)
- Memory usage peaks
- Data throughput (MB/s, rows/s)
- Operation-specific metrics (files processed, chunks processed)
- Backend-specific metrics

## Risks / Trade-offs

### Risk 1: Complexity vs Performance
**Risk**: Vectorized operations may be more complex to implement and debug
**Mitigation**: 
- Implement incrementally with extensive testing
- Maintain fallback to current implementation if needed
- Comprehensive test suite with various dataset sizes

### Risk 2: Memory Usage vs Speed
**Risk**: Chunked processing may be slower than loading everything into memory for medium datasets
**Mitigation**:
- Smart chunking: use memory-based chunks instead of fixed row counts
- Adaptive chunk sizing based on available memory
- Option to disable chunking for smaller datasets

### Risk 3: Backwards Compatibility
**Risk**: Performance optimizations may change edge case behavior
**Mitigation**:
- Extensive compatibility testing
- Preserve exact same results for all inputs
- Add configuration options to fall back to old behavior if needed

## Migration Plan

### Phase 1: Core Vectorization (Weeks 1-2)
1. Replace deduplication Python loops with PyArrow operations
2. Optimize merge operations to use vectorized functions
3. Add comprehensive test suite
4. Benchmark against current implementation

### Phase 2: Chunked Processing (Weeks 3-4)
1. Implement configurable chunked processing
2. Add memory monitoring and limits
3. Progress tracking for long operations
4. Performance testing with various dataset sizes

### Phase 3: Streaming Processing (Weeks 5-6)
1. Implement streaming for applicable operations
2. Optimize for very large datasets
3. Add advanced monitoring and metrics
4. Documentation and examples

### Rollback Strategy
- Feature flags to disable optimizations
- Fallback to original implementation
- Clear error messages if optimization fails

## Open Questions

### Question 1: Optimal Chunk Size Strategy
**Question**: Should chunk size be based on rows, memory usage, or file boundaries?
**Considerations**:
- Row-based: Simple but may cause memory spikes
- Memory-based: More predictable but requires estimation
- File-based: Natural boundaries but may create uneven chunks

**Current Decision**: Start with row-based (configurable), evaluate memory-based if needed

### Question 2: Error Handling in Streaming Operations
**Question**: How should partial failures be handled in streaming operations?
**Options**:
- Fail entire operation (simplest)
- Skip problematic chunks and continue (more robust)
- Retry failed chunks with backoff (most robust but complex)

**Current Decision**: Skip problematic chunks with detailed logging, continue operation

### Question 3: Progress Reporting
**Question**: How should progress be reported for long operations?
**Options**:
- Callback function for progress updates
- Return progress in result object
- Log progress at intervals
- All of the above

**Current Decision**: All of the above - provide multiple ways to track progress
