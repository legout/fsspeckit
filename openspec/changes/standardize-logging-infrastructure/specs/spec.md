# Logging Infrastructure Specifications

## Central Logging Configuration

### `src/fsspeckit/common/logging_config.py`

#### Core Functions

##### `setup_logging()`
```python
def setup_logging(
    level: str = "INFO",
    format_string: Optional[str] = None,
    include_timestamp: bool = True,
    enable_console: bool = True,
    enable_file: bool = False,
    file_path: Optional[str] = None
) -> None:
    """
    Configure logging for the entire fsspeckit package.
    
    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        format_string: Custom log format string
        include_timestamp: Whether to include timestamp in logs
        enable_console: Whether to output to console
        enable_file: Whether to output to file
        file_path: Path for log file output
    """
```

##### `get_logger()`
```python
def get_logger(name: str) -> logging.Logger:
    """
    Get a properly configured logger for a module.
    
    Args:
        name: Module name (typically __name__)
        
    Returns:
        Configured logger instance
    """
```

#### Default Configuration
- **Format**: `%(asctime)s - %(name)s - %(levelname)s - %(message)s`
- **Level**: INFO (configurable via environment variable `FSSPECKIT_LOG_LEVEL`)
- **Handlers**: Console output by default

## Module Update Specifications

### Logger Import Pattern
```python
# Standard import for all modules
from fsspeckit.common.logging_config import get_logger

logger = get_logger(__name__)
```

### Log Level Usage Guidelines

#### DEBUG
- File path resolution
- Data reading/writing operations
- Function entry/exit for complex operations
- Performance metrics (timing, bytes processed)

#### INFO
- Successful operation completion
- Configuration values at startup
- High-level progress information

#### WARNING
- Recoverable errors
- Deprecated usage
- Performance concerns
- Missing optional features

#### ERROR
- Failed operations that don't crash program
- Permission/access issues
- Invalid data formats
- Network/IO failures

#### CRITICAL
- Fatal errors that terminate execution
- System resource exhaustion
- Configuration errors preventing startup

### Exception Handling Patterns

#### Standard Pattern
```python
try:
    result = perform_operation()
    logger.info("Operation completed successfully: %s", operation_name)
    return result
except SpecificException as e:
    logger.error("Specific error in %s: %s", operation_name, e, exc_info=True)
    raise
except Exception as e:
    logger.error("Unexpected error in %s: %s", operation_name, e, exc_info=True)
    raise RuntimeError(f"Unexpected error in {operation_name}") from e
```

#### Cleanup Error Pattern
```python
try:
    cleanup_resources()
except Exception as e:
    logger.error("Cleanup failed: %s", e, exc_info=True)
    # Continue cleanup of other resources
```

### Performance Considerations

#### Lazy String Formatting
```python
# Good - uses lazy formatting
logger.info("Processing file %s with %d bytes", file_path, file_size)

# Avoid - eager formatting
logger.info(f"Processing file {file_path} with {file_size} bytes")
```

#### Debug Message Guards
```python
# Guard expensive debug operations
if logger.isEnabledFor(logging.DEBUG):
    debug_data = expensive_debug_info()
    logger.debug("Detailed debug info: %s", debug_data)
```

## Message Format Standards

### Consistent Context Structure
1. **Operation Context**: What is being done (reading, writing, processing)
2. **Resource Context**: File paths, URLs, table names
3. **Quantitative Context**: Sizes, counts, timing
4. **Error Context**: Specific error details and impact

### Examples

#### File Operations
```python
logger.info("Reading %s bytes from %s", file_size, file_path)
logger.warning("Skipping corrupted file: %s [invalid checksum]", file_path)
logger.error("Failed to write %s: permission denied", output_path, exc_info=True)
```

#### Database Operations
```python
logger.info("Executing query on table %s: %d rows affected", table_name, row_count)
logger.debug("Query execution time: %.3f seconds", execution_time)
logger.error("Database connection failed: %s", connection_string, exc_info=True)
```

#### Configuration
```python
logger.info("Initialized with config: backend=%s, cache=%s", backend, cache_enabled)
logger.warning("Using deprecated configuration option: %s", option_name)
```

## Environment Variables

### Configuration Variables
- `FSSPECKIT_LOG_LEVEL`: Set default log level (DEBUG, INFO, WARNING, ERROR)
- `FSSPECKIT_LOG_FORMAT`: Custom log format string
- `FSSPECKIT_LOG_FILE`: Enable file logging to specified path
- `FSSPECKIT_LOG_TIMESTAMP`: Enable/disable timestamps (true/false)

## Testing Requirements

### Unit Tests
1. **Logger Configuration**: Verify setup_logging creates correct configuration
2. **Message Formatting**: Test log message formats and context
3. **Level Filtering**: Verify message filtering by log level
4. **Exception Logging**: Test exc_info parameter includes stack traces

### Integration Tests
1. **Multi-module Logging**: Verify consistent behavior across modules
2. **Performance**: Verify no significant performance impact
3. **Environment Variables**: Test configuration via environment variables

### Logging Verification Tests
```python
def test_logging_capture(caplog):
    logger = get_logger(__name__)
    
    with caplog.at_level(logging.INFO):
        logger.info("Test message: %s", "context")
    
    assert "Test message: context" in caplog.text
    assert logger.level == logging.INFO
```

## Files Requiring Updates

### Core Modules
- `src/fsspeckit/core/ext.py` - Replace print statements, add operation logging
- `src/fsspeckit/core/filesystem.py` - Add filesystem operation logging
- `src/fsspeckit/core/maintenance.py` - Add maintenance operation logging
- `src/fsspeckit/core/merge.py` - Add merge operation logging

### Dataset Modules
- `src/fsspeckit/datasets/duckdb.py` - Replace print statements, add query logging
- `src/fsspeckit/datasets/pyarrow.py` - Add dataset operation logging
- `src/fsspeckit/datasets/deltalake.py` - Add deltalake operation logging

### Utility Modules
- `src/fsspeckit/utils/pyarrow.py` - Add pyarrow operation logging
- `src/fsspeckit/utils/polars.py` - Add polars operation logging
- `src/fsspeckit/utils/duckdb.py` - Add duckdb utility logging
- `src/fsspeckit/utils/datetime.py` - Add timezone conversion logging

### Storage Options
- `src/fsspeckit/storage_options/cloud.py` - Add cloud operation logging
- `src/fsspeckit/storage_options/git.py` - Add git operation logging

## Migration Checklist

### Phase 1: Infrastructure
- [ ] Create logging_config.py with setup_logging and get_logger
- [ ] Update __init__.py to configure logging on import
- [ ] Add environment variable handling
- [ ] Test basic logging functionality

### Phase 2: Module Updates
- [ ] Update all modules to use get_logger(__name__)
- [ ] Replace print() statements with appropriate logger calls
- [ ] Add exception logging with exc_info=True
- [ ] Standardize log message formats
- [ ] Add debug logging for key operations

### Phase 3: Validation
- [ ] Verify all print statements are replaced
- [ ] Test log level configuration
- [ ] Validate performance impact
- [ ] Update documentation and examples