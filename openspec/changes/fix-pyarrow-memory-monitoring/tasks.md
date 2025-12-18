## 1. Implementation
- [ ] 1.1 Add psutil as optional dependency in pyproject.toml
- [ ] 1.2 Create MemoryMonitor class with dual tracking (PyArrow + system)
- [ ] 1.3 Update PerformanceMonitor to use new MemoryMonitor
- [ ] 1.4 Update process_in_chunks to use system memory checks
- [ ] 1.5 Implement graceful degradation with tiered memory pressure levels
- [ ] 1.6 Add new configuration parameters (max_process_memory_mb, min_system_available_mb)

## 2. Testing
- [ ] 2.1 Test memory monitoring with psutil available
- [ ] 2.2 Test fallback behavior when psutil unavailable
- [ ] 2.3 Test graceful degradation under memory pressure
- [ ] 2.4 Test cross-platform compatibility (Linux, macOS, Windows)

## 3. Documentation
- [ ] 3.1 Document new memory monitoring capabilities
- [ ] 3.2 Add examples for memory-constrained environments
- [ ] 3.3 Update API reference with new parameters
