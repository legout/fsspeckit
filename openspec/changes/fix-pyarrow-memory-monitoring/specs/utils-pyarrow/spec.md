## MODIFIED Requirements
### Requirement: Memory Monitoring and Limits
The PyArrow dataset operations SHALL monitor both PyArrow allocation AND system memory usage to enforce memory limits and prevent OOM errors.

#### Scenario: System memory monitoring with psutil available
- **GIVEN** psutil is installed
- **WHEN** chunked processing is performed with max_memory_mb=2048
- **THEN** both PyArrow allocation and process RSS SHALL be monitored
- **AND** operations SHALL pause if either limit is approached
- **AND** detailed memory metrics SHALL be included in performance reports

#### Scenario: Fallback when psutil unavailable
- **GIVEN** psutil is NOT installed
- **WHEN** chunked processing is performed
- **THEN** only PyArrow allocation SHALL be monitored
- **AND** a warning SHALL be logged about limited memory monitoring
- **AND** operations SHALL continue with PyArrow-only limits

#### Scenario: Graceful degradation under memory pressure
- **GIVEN** system memory usage exceeds 90% of configured limit
- **WHEN** processing continues
- **THEN** chunk size SHALL be automatically reduced
- **AND** garbage collection SHALL be triggered
- **AND** warning SHALL be logged with current memory status

## ADDED Requirements
### Requirement: Configurable System Memory Thresholds
The memory monitoring system SHALL support configurable thresholds for system-level memory management.

#### Scenario: Process memory limit configuration
- **GIVEN** max_process_memory_mb=4096 is configured
- **WHEN** process RSS exceeds 4096 MB
- **THEN** MemoryError SHALL be raised with detailed diagnostics
- **AND** current memory breakdown SHALL be included in error message

#### Scenario: System available memory threshold
- **GIVEN** min_system_available_mb=512 is configured
- **WHEN** system available memory drops below 512 MB
- **THEN** operations SHALL pause and attempt graceful degradation
- **AND** warning SHALL be logged with system memory status
