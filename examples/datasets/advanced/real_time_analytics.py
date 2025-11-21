"""
Real-Time Analytics Example

This advanced-level example demonstrates real-time analytics capabilities
using fsspeckit's dataset utilities with streaming data processing.

The example covers:
1. Real-time data ingestion and processing
2. Stream processing patterns with PyArrow
3. Window-based analytics calculations
4. Real-time dashboard data preparation
5. Performance optimization for low-latency processing
6. Monitoring and alerting for streaming systems

This example shows how to build production-grade real-time analytics
pipelines using fsspeckit.
"""

from __future__ import annotations

import asyncio
import tempfile
import time
from collections import defaultdict, deque
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

# For real-time scenarios, we'd typically use additional libraries
# These are optional dependencies
try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False

from fsspeckit.datasets import DuckDBParquetHandler


class RealTimeDataGenerator:
    """Simulates real-time data sources for demonstration."""

    def __init__(self, seed_value: int = 42):
        import random
        random.seed(seed_value)

        self.products = [
            "Laptop Pro", "iPhone 15", "iPad Pro", "AirPods Pro", "MacBook Air",
            "Surface Pro", "Galaxy Tab", "Echo Dot", "Kindle Oasis", "Nintendo Switch"
        ]

        self.regions = ["North America", "Europe", "Asia Pacific", "Latin America"]
        self.channels = ["Online", "Retail", "Partner", "Mobile", "Marketplace"]
        self.customers = [f"Cust_{i:06d}" for i in range(1, 10001)]

        self.transaction_counter = 0
        self.last_timestamp = datetime.now()

    def generate_transaction(self) -> pa.Table:
        """Generate a single transaction record."""
        import random

        self.transaction_counter += 1
        self.last_timestamp = datetime.now()

        # Simulate some patterns in the data
        if random.random() < 0.1:  # 10% chance of return
            is_returned = True
            transaction_type = "return"
            quantity = -random.randint(1, 5)
        else:
            is_returned = False
            transaction_type = "sale"
            quantity = random.randint(1, 10)

        record = {
            "transaction_id": f"TXN_{self.transaction_counter:010d}",
            "timestamp": self.last_timestamp,
            "timestamp_ms": int(self.last_timestamp.timestamp() * 1000),
            "transaction_type": transaction_type,
            "product": random.choice(self.products),
            "category": random.choice(["Electronics", "Accessories", "Computers", "Mobile"]),
            "quantity": abs(quantity),
            "unit_price": round(random.uniform(50.0, 2000.0), 2),
            "region": random.choice(self.regions),
            "channel": random.choice(self.channels),
            "customer_id": random.choice(self.customers),
            "campaign_id": f"CAMP_{random.randint(1, 50):03d}",
            "device_type": random.choice(["Desktop", "Mobile", "Tablet"]),
            "session_id": f"SESS_{random.randint(1, 5000):05d}",
            "ip_address": f"192.168.{random.randint(1, 255)}.{random.randint(1, 255)}"
        }

        # Calculate derived fields
        record["total_amount"] = abs(quantity) * record["unit_price"]
        record["tax_amount"] = record["total_amount"] * 0.08 if not is_returned else 0
        record["net_amount"] = record["total_amount"] - record["tax_amount"]

        # Add random quality flags
        record["is_valid"] = random.random() < 0.99  # 99% valid
        record["is_fraud"] = random.random() < 0.001  # 0.1% fraud
        record["processing_time_ms"] = random.randint(10, 200)

        return pa.table([record])


class StreamingAnalyticsProcessor:
    """Processes real-time analytics calculations."""

    def __init__(self, window_size_minutes: int = 5):
        self.window_size_minutes = window_size_minutes
        self.window_size_ms = window_size_minutes * 60 * 1000

        # Use deque for efficient sliding window
        self.transaction_buffer = deque(maxlen=10000)
        self.aggregated_data = defaultdict(lambda: defaultdict(list))

        # Performance tracking
        self.processed_count = 0
        self.error_count = 0
        self.start_time = time.time()

    def process_transaction(self, transaction: pa.Table):
        """Process a single transaction and update analytics."""
        try:
            # Validate transaction
            if len(transaction) == 0:
                return

            # Add to buffer
            for i in range(len(transaction)):
                record = transaction.slice(i, 1)
                self.transaction_buffer.append(record)

            # Update real-time metrics
            self._update_metrics(record)

            # Process window calculations
            self._process_windows()

            self.processed_count += 1

        except Exception as e:
            self.error_count += 1
            print(f"Error processing transaction: {e}")

    def _update_metrics(self, record: pa.Table):
        """Update real-time metrics for a single record."""

        try:
            timestamp_ms = record.column("timestamp_ms")[0].as_pylist()[0]
            timestamp = datetime.fromtimestamp(timestamp_ms / 1000)

            # Time-based aggregations
            minute_key = timestamp.strftime("%Y-%m-%d %H:%M")
            hour_key = timestamp.strftime("%Y-%m-%d %H:00")
            day_key = timestamp.strftime("%Y-%m-%d")

            # Extract values
            net_amount = record.column("net_amount")[0].as_pylist()[0]
            region = record.column("region")[0].as_pylist()[0]
            channel = record.column("channel")[0].py().decode()

            # Store aggregations
            self.aggregated_data["minute"][minute_key].append(net_amount)
            self.aggregated_data["hour"][hour_key].append(net_amount)
            self.aggregated_data["day"][day_key].append(net_amount)
            self.aggregated_data["region"][region].append(net_amount)
            self.aggregated_data["channel"][channel].append(net_amount)

        except Exception as e:
            print(f"Error updating metrics: {e}")

    def _process_windows(self):
        """Process sliding window calculations."""

        current_time_ms = int(time.time() * 1000)
        cutoff_time_ms = current_time_ms - self.window_size_ms

        # Remove old records from buffer
        while self.transaction_buffer:
            oldest_record_time = self.transaction_buffer[0].column("timestamp_ms")[0].as_pylist()[0]
            if oldest_record_time < cutoff_time_ms:
                self.transaction_buffer.popleft()
            else:
                break

        # Calculate window metrics
        if len(self.transaction_buffer) > 0:
            buffer_data = pa.Table.from_pydict({
                col: [col[0].as_pylist() if isinstance(col[0], pa.Scalar) else col for col in zip(*self.transaction_buffer.values())]
            } if self.transaction_buffer else pa.table([])

            self._calculate_window_metrics(buffer_data)

    def _calculate_window_metrics(self, buffer_data: pa.Table):
        """Calculate metrics for the current window."""
        try:
            if len(buffer_data) == 0:
                return

            # Basic statistics
            total_amounts = pc.sum(buffer_data.column("net_amount"))
            total_quantity = pc.sum(buffer_data.column("quantity"))
            avg_amount = pc.mean(buffer_data.column("net_amount"))

            # Category breakdowns
            category_counts = pc.value_counts(buffer_data.column("category"))
            region_counts = pc.value_counts(buffer_data.column("region"))
            channel_counts = pc.value_counts(buffer_data.column("channel"))

            # High-value transactions
            high_value_count = pc.sum(
                pc.greater(buffer_data.column("net_amount"), 1000)
            )

            # Fraud detection
            fraud_count = pc.sum(buffer_data.column("is_returned"))
            valid_count = pc.sum(buffer_data.column("is_valid"))
            total_count = len(buffer_data)

            # Calculate rates
            fraud_rate = (fraud_count / total_count * 100) if total_count > 0 else 0
            return_rate = (fraud_count / total_count * 100) if total_count > 0 else 0

            # Store window metrics
            window_metrics = {
                "timestamp": datetime.now(),
                "window_size_minutes": self.window_size_minutes,
                "transaction_count": total_count,
                "total_amount": total_amounts.as_py(),
                "total_quantity": total_quantity.as_py(),
                "avg_amount": avg_amount.as_py(),
                "high_value_count": high_value_count.as_py(),
                "valid_count": valid_count.as_py(),
                "invalid_count": total_count - valid_count,
                "fraud_count": fraud_count.as_pylist(),
                "fraud_rate": fraud_rate,
                "return_rate": return_rate,
                "categories": dict(zip(category_counts[0].to_pylist(), category_counts[1].to_pylist())),
                "regions": dict(zip(region_counts[0].to_pylist(), region_counts[1].to_pylist())),
                "channels": dict(zip(channel_counts[0].to_pylist(), channel_counts[1].to_pylist()))
            }

            # In real-time system, you'd send this to monitoring/alerting
            self._check_alerts(window_metrics)

        except Exception as e:
            print(f"Error calculating window metrics: {e}")

    def _check_alerts(self, metrics: Dict[str, Any]):
        """Check for alert conditions."""
        try:
            # Alert on high fraud rate
            if metrics["fraud_rate"] > 2.0:  # 2% threshold
                print(f"🚨 HIGH FRAUD ALERT: {metrics['fraud_rate']:.2f}%")

            # Alert on invalid transactions
            if metrics["invalid_count"] > metrics["transaction_count"] * 0.1:  # 10% threshold
                print(f"⚠️ HIGH INVALID RATE: {metrics['invalid_count']} invalid transactions")

            # Alert on low average amount
            if metrics["avg_amount"] < 50.0:
                print(f"ℹ️ LOW AVERAGE AMOUNT: ${metrics['avg_amount']:.2f}")

        except Exception as e:
            print(f"Error checking alerts: {e}")

    def get_real_time_metrics(self) -> Dict[str, Any]:
        """Get current real-time metrics."""
        try:
            uptime = time.time() - self.start_time

            # Calculate rates
            processing_rate = self.processed_count / uptime if uptime > 0 else 0
            error_rate = (self.error_count / (self.processed_count + self.error_count) * 100
                        if self.processed_count + self.error_count > 0 else 0)

            return {
                "uptime_seconds": uptime,
                "total_processed": self.processed_count,
                "total_errors": self.error_count,
                "processing_rate_per_second": processing_rate,
                "error_rate_percent": error_rate,
                "buffer_size": len(self.transaction_buffer),
                "aggregation_keys": {
                    category: len(values) for category, values in self.aggregated_data.items()
                }
            }

        except Exception as e:
            print(f"Error getting metrics: {e}")
            return {}

    def get_dashboard_data(self) -> Dict[str, Any]:
        """Get data for real-time dashboard."""
        try:
            dashboard_data = {
                "real_time_metrics": self.get_real_time_metrics(),
                "current_window": {
                    "size_minutes": self.window_size_minutes,
                    "transaction_count": len(self.transaction_buffer)
                },
                "recent_aggregations": {},
                "performance_metrics": {}
            }

            # Get recent aggregations (last 5 minutes)
            now = datetime.now()
            for timeframe, data_list in self.aggregated_data.items():
                recent_data = []
                for key, values in data_list.items():
                    if values:
                        avg_value = sum(values) / len(values)
                        recent_data.append({
                            "period": key,
                            "value": avg_value,
                            "count": len(values)
                        })

                if recent_data:
                    dashboard_data["recent_aggregations"][timeframe] = recent_data

            # Performance metrics
            if self.processed_count > 0:
                dashboard_data["performance_metrics"] = {
                    "avg_processing_time_ms": 50,  # Would track actual processing time
                    "throughput_per_second": self.get_real_time_metrics()["processing_rate_per_second"],
                    "memory_efficiency": "Good" if len(self.transaction_buffer) < 1000 else "Needs attention"
                }

            return dashboard_data

        except Exception as e:
            print(f"Error getting dashboard data: {e}")
            return {}

    def get_historical_data(self, hours: int = 24) -> pa.Table:
        """Get historical aggregated data for trend analysis."""
        try:
            # In a real system, this would query historical storage
            # For demo, we'll aggregate the current aggregations

            historical_records = []

            # Aggregate by hour for the last N hours
            for hour in range(24, 24 + hours):
                hour_key = datetime.now().replace(hour=(hour % 24)).strftime("%Y-%m-%d %H:00")
                if hour_key in self.aggregated_data["hour"]:
                    values = self.aggregated_data["hour"][hour_key]
                    if values:
                        record = {
                            "hour": hour_key,
                            "total_amount": sum(values),
                            "transaction_count": len(values),
                            "avg_amount": sum(values) / len(values)
                        }
                        historical_records.append(record)

            return pa.Table(historical_records)

        except Exception as e:
            print(f"Error getting historical data: {e}")
            return pa.table([])

    def export_dashboard_data(self, output_path: str):
        """Export current dashboard data for external consumption."""
        try:
            dashboard_data = self.get_dashboard_data()
            historical_data = self.get_historical_data()

            # Combine data for export
            export_data = {
                "generated_at": datetime.now().isoformat(),
                "real_time_metrics": dashboard_data["real_time_metrics"],
                "current_window": dashboard_data["current_window"],
                "aggregations": dashboard_data["recent_aggregations"],
                "performance": dashboard_data["performance_metrics"]
            }

            # Save as JSON (for web dashboards)
            import json
            json_path = Path(output_path) / "dashboard.json"
            with open(json_path, 'w') as f:
                json.dump(export_data, f, indent=2, default=str)

            # Save historical data as Parquet
            if len(historical_data) > 0:
                parquet_path = Path(output_path) / "historical.parquet"
                pq.write_table(historical_data, parquet_path)

            print(f"Dashboard data exported to {output_path}")

        except Exception as e:
            print(f"Error exporting dashboard data: {e}")


class RealTimeAnalyticsSystem:
    """Main real-time analytics system coordinator."""

    def __init__(self, output_dir: str):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Initialize components
        self.data_generator = RealTimeDataGenerator()
        self.processor = StreamingAnalyticsProcessor(window_size_minutes=10)

        # System state
        self.is_running = False
        self.simulation_speed = 1.0  # Transactions per second

    def start_streaming(self, duration_minutes: int = 5, simulation_speed: float = 1.0):
        """Start real-time data streaming and processing."""
        print(f"🚀 Starting Real-Time Analytics System")
        print(f"   Duration: {duration_minutes} minutes")
        print(f"   Simulation speed: {simulation_speed}x real-time")
        print(f"   Output directory: {self.output_dir}")

        self.is_running = True
        self.simulation_speed = simulation_speed
        start_time = time.time()
        end_time = start_time + (duration_minutes * 60)

        try:
            while self.is_running and time.time() < end_time:
                # Generate transaction
                transaction = self.data_generator.generate_transaction()

                # Process in real-time
                self.processor.process_transaction(transaction)

                # Control simulation speed
                time.sleep(1.0 / self.simulation_speed)

                # Periodic status update
                if int(time.time() - start_time) % 30 == 0:  # Every 30 seconds
                    self._print_status()

                # Periodic dashboard export
                if int(time.time() - start_time) % 60 == 0:  # Every minute
                    self._export_dashboard()

        except KeyboardInterrupt:
            print("\n🛑 Streaming stopped by user")
        except Exception as e:
            print(f"❌ Streaming error: {e}")
        finally:
            self.is_running = False
            self._print_status()

    def stop_streaming(self):
        """Stop real-time streaming."""
        self.is_running = False
        print("🛑 Streaming stopped")

    def _print_status(self):
        """Print current system status."""
        metrics = self.processor.get_real_time_metrics()
        dashboard = self.processor.get_dashboard_data()

        print(f"\n📊 System Status (Uptime: {metrics['uptime_seconds']:.0f}s)")
        print(f"   Processed: {metrics['total_processed']:,} transactions")
        print(f"   Errors: {metrics['total_errors']}")
        print(f"   Rate: {metrics['processing_rate_per_second']:.1f} tx/sec")
        print(f"   Buffer: {metrics['buffer_size']} records")
        print(f"   Error rate: {metrics['error_rate_percent']:.2f}%")

        # Show recent aggregations
        if dashboard["recent_aggregations"]:
            print("\n📈 Recent Aggregations:")
            for timeframe, agg_data in dashboard["recent_aggregations"].items():
                print(f"   {timeframe}: {len(agg_data)} keys")

    def _export_dashboard(self):
        """Export dashboard data for web consumption."""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            export_dir = self.output_dir / f"dashboard_{timestamp}"
            export_dir.mkdir(exist_ok=True)

            self.processor.export_dashboard_data(str(export_dir))

            # Also save real-time metrics
            metrics_file = self.output_dir / "current_metrics.json"
            import json
            with open(metrics_file, 'w') as f:
                json.dump({
                    "timestamp": datetime.now().isoformat(),
                    "metrics": self.processor.get_real_time_metrics(),
                    "dashboard": self.processor.get_dashboard_data()
                }, f, indent=2, default=str)

        except Exception as e:
            print(f"Error exporting dashboard: {e}")

    def generate_sample_dashboard(self, duration_minutes: int = 2):
        """Generate sample dashboard data for demonstration."""
        print(f"📊 Generating Sample Dashboard Data ({duration_minutes} minutes)")

        try:
            # Start short streaming session
            self.start_streaming(duration_minutes=duration_minutes, simulation_speed=10)

            # Generate comprehensive report
            print(f"\n📋 Analytics Report:")
            metrics = self.processor.get_real_time_metrics()
            dashboard = self.processor.get_dashboard_data()

            print(f"   Total Processing:")
            print(f"     Transactions: {metrics['total_processed']:,}")
            print(f"     Errors: {metrics['total_errors']}")
            print(f"     Uptime: {metrics['uptime_seconds']:.1f}s")
            print(f"     Processing Rate: {metrics['processing_rate_per_second']:.1f} tx/sec")

            if "recent_aggregations" in dashboard:
                print(f"\n   Real-Time Aggregations:")
                for timeframe, data in dashboard["recent_aggregations"].items():
                    if data:
                        total = sum(item["value"] for item in data)
                        count = sum(item["count"] for item in data)
                        print(f"     {timeframe}: ${total:,.0f} from {count} records")

            # Export final dashboard
            self._export_dashboard()

        except Exception as e:
            print(f"Error generating sample dashboard: {e}")


def demonstrate_real_time_processing():
    """Demonstrate real-time processing capabilities."""

    print("\n🔄 Real-Time Processing Demonstration")

    temp_dir = Path(tempfile.mkdtemp())

    try:
        # Initialize system
        analytics_system = RealTimeAnalyticsSystem(str(temp_dir))

        # Generate sample dashboard data
        analytics_system.generate_sample_dashboard(duration_minutes=2)

        print("\n🎯 Real-Time Features Demonstrated:")
        print("   ✓ Stream processing of transaction data")
        print("   ✓ Sliding window analytics")
        "   ✓ Real-time metric calculations")
        "   ✓ Performance monitoring")
        "   ✅ Alert detection for anomalies")
        print("   ✓ Dashboard data export")
        print("   ✓ Historical data aggregation")

    except Exception as e:
        print(f"❌ Real-time processing demo failed: {e}")
        raise

    finally:
        import shutil
        shutil.rmtree(temp_dir)


def demonstrate_streaming_patterns():
    """Demonstrate different streaming data patterns."""

    print("\n📡 Streaming Data Patterns")

    temp_dir = Path(tempfile.mkdtemp())

    try:
        print("1. High-Frequency Transaction Stream")
        print("   Processing thousands of transactions per second")
        print("   Low-latency requirements (sub-second)")
        print("   Memory-efficient sliding windows")
        print("   Real-time fraud detection")

        high_freq_system = RealTimeAnalyticsSystem(str(temp_dir / "high_freq"))
        high_freq_system.start_streaming(duration_minutes=1, simulation_speed=100)

        print("\n2. Batch Processing Stream")
        print("   Processing transactions in batches")
        print("   Optimized for throughput")
        print("   Periodic aggregation")
        print("   Memory-managed buffering")

        batch_system = RealTimeAnalyticsSystem(str(temp_dir / "batch"))
        batch_system.start_streaming(duration_minutes=1, simulation_speed=5)

        print("\n3. Event-Driven Analytics")
        print("   Trigger-based processing")
        print("   Complex event correlations")
        print("   Multi-stream aggregation")
        print("   Real-time alerting")

        event_system = RealTimeAnalyticsSystem(str(temp_dir / "event"))
        event_system.start_streaming(duration_minutes=1, simulation_speed=20)

        print("\n💡 Streaming Patterns Summary:")
        print("   • High-Frequency: Maximum throughput, minimal latency")
        print("   • Batch Processing: Balanced approach, good for reporting")
        print("   • Event-Driven: Responsive processing, complex logic")
        print("   • Hybrid: Combine patterns for different use cases")

        # Compare performance characteristics
        metrics_comparison = [
            ("High-Frequency", {"latency": "<100ms", "throughput": "1000+ tx/s", "memory": "High"}),
            ("Batch Processing", {"latency": "1-5s", "throughput": "100-500 tx/s", "memory": "Medium"}),
            ("Event-Driven", {"latency": "50-500ms", "throughput": "50-200 tx/s", "memory": "Low"})
        ]

        print(f"\n📊 Performance Characteristics:")
        for pattern, characteristics in metrics_comparison:
            print(f"   {pattern:16} | Latency: {characteristics['latency']:<10} | Throughput: {characteristics['throughput']:<12} | Memory: {characteristics['memory']:<10}")

    except Exception as e:
        print(f"❌ Streaming patterns demo failed: {e}")
        raise

    finally:
        import shutil
        shutil.rmtree(temp_dir)


def demonstrate_dashboard_integration():
    """Demonstrate dashboard integration patterns."""

    print("\n📱 Dashboard Integration")

    temp_dir = Path(tempfile.mkdtemp())

    try:
        # Create analytics system
        analytics = RealTimeAnalyticsSystem(str(temp_dir))

        # Simulate data processing
        print("1. Processing sample data for dashboard...")
        analytics.start_streaming(duration_minutes=2, simulation_speed=50)

        print("2. Dashboard Data Structure:")
        dashboard_data = analytics.get_dashboard_data()

        print("   Real-time Metrics:")
        for key, value in dashboard_data["real_time_metrics"].items():
            print(f"     {key}: {value}")

        if "current_window" in dashboard_data:
            print(f"   Current Window:")
            for key, value in dashboard_data["current_window"].items():
                print(f"     {key}: {value}")

        print("   Aggregations:")
        for timeframe, data in dashboard_data.get("recent_aggregations", {}).items():
            print(f"     {timeframe}: {len(data)} aggregation points")

        # Export in different formats
        print("\n3. Export Formats:")

        # JSON for web dashboards
        json_path = temp_dir / "dashboard.json"
        import json
        with open(json_path, 'w') as f:
            json.dump(dashboard_data, f, indent=2, default=str)
        print(f"   JSON exported: {json_path}")

        # CSV for spreadsheet tools
        if PANDAS_AVAILABLE:
            # Create tabular data for CSV export
            metrics = analytics.processor.get_real_time_metrics()
            historical = analytics.processor.get_historical_data()

            metrics_data = [{
                "metric": key,
                "value": value,
                "timestamp": datetime.now().isoformat()
            } for key, value in metrics.items()]

            csv_path = temp_dir / "metrics.csv"
            metrics_df = pd.DataFrame(metrics_data)
            metrics_df.to_csv(csv_path, index=False)
            print(f"   CSV exported: {csv_path}")

        # Parquet for analysis tools
        parquet_path = temp_dir / "historical.parquet"
        if len(historical) > 0:
            pq.write_table(historical, parquet_path)
            print(f"   Parquet exported: {parquet_path}")

        print("\n4. Dashboard Integration Patterns:")

        print("   Pattern 1: Real-Time Web Dashboard")
        print("   • JSON API endpoints")
        print("   • WebSocket data streaming")
        print("   • Server-Sent Events (SSE)")
        print("   • Update intervals: 1-5 seconds")

        print("\n   Pattern 2: Business Intelligence Tools")
        print("   • Direct database connections")
        print("   • Scheduled data exports")
        print("   • Integration with Tableau/PowerBI")
        print("   • OLAP data cubes")

        print("\n   Pattern 3: Mobile Applications")
        print("   • REST API endpoints")
        print("   • Push notifications")
        "   • Local caching strategies")
        print("   • Offline-first design")

        print("\n🔗 Dashboard Technology Stack:")
        print("   • Frontend: React/Vue.js/D3.js")
        print("   • Backend: FastAPI/Django/Flask")
        print("   • Database: DuckDB/ClickHouse")
        print("   • Message Queue: Kafka/RabbitMQ")
        print("   • Monitoring: Prometheus/Grafana")

    except Exception as e:
        print(f"❌ Dashboard integration demo failed: {e}")
        raise

    finally:
        import shutil
        shutil.rmtree(temp_dir)


def main():
    """Run all real-time analytics examples."""

    print("⚡ Real-Time Analytics Example")
    print("=" * 60)
    print("This example demonstrates real-time analytics capabilities")
    print("using fsspeckit's dataset utilities with streaming data.")

    try:
        # Run all demonstrations
        demonstrate_real_time_processing()
        demonstrate_streaming_patterns()
        demonstrate_dashboard_integration()

        print("\n" + "=" * 60)
        print("✅ Real-time analytics completed successfully!")

        print("\n🎯 Advanced Takeaways:")
        print("• Real-time processing requires careful memory management")
        print("• Sliding windows are key for time-based analytics")
        "• Alert systems need appropriate thresholds")
        print("• Dashboard integration requires multiple export formats")
        print("• Performance monitoring is essential for production systems")
        print("• Error handling and recovery are critical for reliability")

        print("\n🔗 Production Considerations:")
        print("• Implement proper logging and monitoring")
        print("• Add circuit breakers for cascading failures")
        print("• Consider data durability and backup strategies")
        print("• Implement proper security and access controls")
        print("• Plan for horizontal scaling and load balancing")

    except Exception as e:
        print(f"\n❌ Example failed: {e}")
        raise


if __name__ == "__main__":
    main()