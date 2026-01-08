"""Memory monitoring utilities for PyArrow-based dataset operations."""

from enum import Enum
from typing import Any, Final
import pyarrow as pa
import os

# Optional psutil import for enhanced system monitoring
try:
    import psutil
except ImportError:
    psutil = None  # type: ignore

from fsspeckit.common.logging import get_logger

logger = get_logger(__name__)

# Constants for memory conversion
BYTES_IN_MB: Final[int] = 1024 * 1024


class MemoryPressureLevel(Enum):
    """Memory pressure levels for graceful degradation."""

    NORMAL = "normal"
    WARNING = "warning"
    CRITICAL = "critical"
    EMERGENCY = "emergency"


def max_pressure(
    p1: MemoryPressureLevel, p2: MemoryPressureLevel
) -> MemoryPressureLevel:
    """Returns the more severe memory pressure level."""
    levels = {
        MemoryPressureLevel.NORMAL: 0,
        MemoryPressureLevel.WARNING: 1,
        MemoryPressureLevel.CRITICAL: 2,
        MemoryPressureLevel.EMERGENCY: 3,
    }
    return p1 if levels[p1] >= levels[p2] else p2


class MemoryMonitor:
    """Monitors both PyArrow and system memory usage.

    This class provides tiered memory pressure monitoring to help prevent
    out-of-memory errors during large dataset operations. It combines
    PyArrow-specific allocation tracking with general system memory monitoring.
    """

    def __init__(
        self,
        max_pyarrow_mb: int = 2048,
        max_process_memory_mb: int | None = None,
        min_system_available_mb: int = 512,
    ):
        """Initialize the memory monitor.

        Args:
            max_pyarrow_mb: Maximum allowed PyArrow-allocated memory in MB.
            max_process_memory_mb: Optional maximum total process memory (RSS) in MB.
            min_system_available_mb: Minimum required system available memory in MB.
        """
        self.max_pyarrow_mb = max_pyarrow_mb
        self.max_process_memory_mb = max_process_memory_mb
        self.min_system_available_mb = min_system_available_mb

        if psutil is not None:
            self._process = psutil.Process(os.getpid())
        else:
            self._process = None
            if max_process_memory_mb or min_system_available_mb:
                logger.debug(
                    "psutil not available; system memory thresholds will be ignored."
                )

    def get_memory_status(self) -> dict[str, float]:
        """Return current memory metrics in MB.

        Returns:
            Dictionary with keys:
                - pyarrow_allocated_mb: Bytes allocated by PyArrow
                - process_rss_mb: Resident Set Size of the process (if psutil available)
                - system_available_mb: Total system available memory (if psutil available)
        """
        status = {"pyarrow_allocated_mb": pa.total_allocated_bytes() / BYTES_IN_MB}

        if psutil is not None and self._process:
            try:
                status["process_rss_mb"] = self._process.memory_info().rss / BYTES_IN_MB
                status["system_available_mb"] = (
                    psutil.virtual_memory().available / BYTES_IN_MB
                )
            except Exception as e:
                # Catching generic Exception because psutil can raise various errors
                # depending on OS/permissions
                logger.warning(f"Could not access system memory info via psutil: {e}")

        return status

    def check_memory_pressure(self) -> MemoryPressureLevel:
        """Evaluate current memory usage against thresholds.

        Tiered thresholds:
        - NORMAL: < 70% of limits
        - WARNING: 70-90% of limits
        - CRITICAL: > 90% of limits
        - EMERGENCY: Exceeds absolute limits

        Returns:
            Current MemoryPressureLevel
        """
        status = self.get_memory_status()
        pa_used = status["pyarrow_allocated_mb"]
        pa_ratio = pa_used / self.max_pyarrow_mb

        pressure = MemoryPressureLevel.NORMAL

        # Check PyArrow limits
        if pa_ratio >= 1.0:
            pressure = MemoryPressureLevel.EMERGENCY
        elif pa_ratio > 0.9:
            pressure = MemoryPressureLevel.CRITICAL
        elif pa_ratio > 0.7:
            pressure = MemoryPressureLevel.WARNING

        # Check Process limits if configured and available
        if (
            psutil is not None
            and self.max_process_memory_mb
            and "process_rss_mb" in status
        ):
            rss_used = status["process_rss_mb"]
            rss_ratio = rss_used / self.max_process_memory_mb

            if rss_ratio >= 1.0:
                pressure = max_pressure(pressure, MemoryPressureLevel.EMERGENCY)
            elif rss_ratio > 0.9:
                pressure = max_pressure(pressure, MemoryPressureLevel.CRITICAL)
            elif rss_ratio > 0.7:
                pressure = max_pressure(pressure, MemoryPressureLevel.WARNING)

        # Check System availability if available
        if psutil is not None and "system_available_mb" in status:
            sys_avail = status["system_available_mb"]
            # For system availability, we check how close we are to the minimum
            if sys_avail < self.min_system_available_mb:
                pressure = max_pressure(pressure, MemoryPressureLevel.EMERGENCY)
            elif sys_avail < self.min_system_available_mb * 1.5:
                pressure = max_pressure(pressure, MemoryPressureLevel.CRITICAL)
            elif sys_avail < self.min_system_available_mb * 2.0:
                pressure = max_pressure(pressure, MemoryPressureLevel.WARNING)

        if pressure != MemoryPressureLevel.NORMAL:
            logger.warning(
                f"Memory pressure detected: {pressure.value}. {self.get_detailed_status()}"
            )

        return pressure

    def should_check_memory(
        self, chunks_processed: int, check_interval: int = 10
    ) -> bool:
        """Determine if memory should be checked based on process progress.

        Args:
            chunks_processed: Number of chunks processed so far.
            check_interval: How often (in chunks) to perform a memory check.

        Returns:
            True if memory should be checked now.
        """
        return chunks_processed % check_interval == 0

    def get_detailed_status(self) -> str:
        """Get detailed status string for logging and error messages.

        Returns:
            Formatted string with memory metrics and limits.
        """
        status = self.get_memory_status()
        details = [
            f"PyArrow: {status['pyarrow_allocated_mb']:.1f}/{self.max_pyarrow_mb} MB"
        ]

        if "process_rss_mb" in status and self.max_process_memory_mb:
            details.append(
                f"Process RSS: {status['process_rss_mb']:.1f}/{self.max_process_memory_mb} MB"
            )
        elif "process_rss_mb" in status:
            details.append(f"Process RSS: {status['process_rss_mb']:.1f} MB")

        if "system_available_mb" in status:
            details.append(
                f"System Available: {status['system_available_mb']:.1f} MB (min: {self.min_system_available_mb} MB)"
            )

        return " | ".join(details)


def bytes_to_mb(b: int) -> float:
    """Convert bytes to megabytes."""
    return b / BYTES_IN_MB


def mb_to_bytes(mb: float) -> int:
    """Convert megabytes to bytes."""
    return int(mb * BYTES_IN_MB)
