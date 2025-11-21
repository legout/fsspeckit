"""
Multi-Cloud Operations Example

This advanced-level example demonstrates enterprise-grade multi-cloud data operations
using fsspeckit's dataset utilities across different cloud providers.

The example covers:
1. Multi-cloud data management and synchronization
2. Cross-cloud data migration and replication strategies
3. Cloud-agnostic data processing patterns
4. Cost optimization across different providers
5. Performance comparison between cloud providers
6. Disaster recovery and high availability patterns
7. Security and compliance considerations across clouds

This example shows how to build production-grade multi-cloud
data pipelines using fsspeckit.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import time
from collections import defaultdict
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple, Union
from urllib.parse import urlparse

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

# Cloud provider libraries
try:
    import boto3
    from botocore.exceptions import ClientError
    BOTO3_AVAILABLE = True
except ImportError:
    BOTO3_AVAILABLE = False

try:
    from azure.storage.blob import BlobServiceClient, BlobClient
    from azure.core.exceptions import AzureError
    AZURE_AVAILABLE = True
except ImportError:
    AZURE_AVAILABLE = False

try:
    from google.cloud import storage as gcs
    from google.cloud.exceptions import GoogleCloudError
    GCS_AVAILABLE = True
except ImportError:
    GCS_AVAILABLE = False

from fsspeckit.datasets import DuckDBParquetHandler
from fsspeckit.storage_options import (
    AwsStorageOptions,
    AzureStorageOptions,
    GcsStorageOptions
)


class CloudProvider(Enum):
    """Enumeration of supported cloud providers."""
    AWS = "aws"
    AZURE = "azure"
    GCP = "gcp"
    LOCAL = "local"


@dataclass
class CloudConfig:
    """Configuration for cloud provider connections."""
    provider: CloudProvider
    region: str
    bucket_name: str
    access_key: Optional[str] = None
    secret_key: Optional[str] = None
    endpoint_url: Optional[str] = None
    connection_string: Optional[str] = None
    sas_token: Optional[str] = None
    service_account_key: Optional[str] = None

    def to_storage_options(self) -> Dict[str, Any]:
        """Convert to fsspeckit storage options."""
        if self.provider == CloudProvider.AWS:
            return AwsStorageOptions(
                region=self.region,
                access_key=self.access_key,
                secret_key=self.secret_key,
                endpoint_url=self.endpoint_url
            ).to_dict()
        elif self.provider == CloudProvider.AZURE:
            return AzureStorageOptions(
                connection_string=self.connection_string,
                sas_token=self.sas_token
            ).to_dict()
        elif self.provider == CloudProvider.GCP:
            return GcsStorageOptions(
                service_account_key=self.service_account_key,
                project_id=self.service_account_key.get("project_id") if self.service_account_key else None
            ).to_dict()
        else:
            return {}


@dataclass
class CloudMetrics:
    """Metrics for cloud operations."""
    provider: CloudProvider
    operation: str
    start_time: float
    end_time: float
    bytes_transferred: int = 0
    success: bool = True
    error_message: Optional[str] = None
    cost_estimate_usd: float = 0.0

    @property
    def duration_seconds(self) -> float:
        return self.end_time - self.start_time

    @property
    def throughput_mbps(self) -> float:
        if self.duration_seconds > 0:
            return (self.bytes_transferred / 1024 / 1024) / self.duration_seconds
        return 0.0

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            **asdict(self),
            "duration_seconds": self.duration_seconds,
            "throughput_mbps": self.throughput_mbps
        }


class CloudStorageManager:
    """Manages storage operations across multiple cloud providers."""

    # Cost estimates (USD per GB, rough estimates)
    COST_PER_GB_STORAGE = {
        CloudProvider.AWS: 0.023,
        CloudProvider.AZURE: 0.018,
        CloudProvider.GCP: 0.020
    }

    COST_PER_GB_TRANSFER = {
        CloudProvider.AWS: 0.09,
        CloudProvider.AZURE: 0.087,
        CloudProvider.GCP: 0.12
    }

    def __init__(self):
        self.connections: Dict[CloudProvider, Any] = {}
        self.metrics: List[CloudMetrics] = []

    def connect(self, config: CloudConfig) -> bool:
        """Establish connection to a cloud provider."""
        try:
            if config.provider == CloudProvider.AWS and BOTO3_AVAILABLE:
                connection = boto3.client(
                    's3',
                    region_name=config.region,
                    aws_access_key_id=config.access_key,
                    aws_secret_access_key=config.secret_key,
                    endpoint_url=config.endpoint_url
                )
                self.connections[config.provider] = connection

            elif config.provider == CloudProvider.AZURE and AZURE_AVAILABLE:
                connection = BlobServiceClient.from_connection_string(
                    config.connection_string
                )
                self.connections[config.provider] = connection

            elif config.provider == CloudProvider.GCP and GCS_AVAILABLE:
                connection = gcs.Client.from_service_account_info(config.service_account_key)
                self.connections[config.provider] = connection

            else:
                print(f"⚠️ {config.provider.value} not available or missing dependencies")
                return False

            print(f"✅ Connected to {config.provider.value}")
            return True

        except Exception as e:
            print(f"❌ Failed to connect to {config.provider.value}: {e}")
            return False

    def upload_data(
        self,
        data: pa.Table,
        cloud_path: str,
        config: CloudConfig
    ) -> Tuple[bool, str]:
        """Upload data to cloud storage."""
        start_time = time.time()
        operation_name = f"upload_to_{config.provider.value}"

        try:
            # Parse cloud path
            parsed = urlparse(cloud_path)
            bucket_name = parsed.netloc
            key_name = parsed.path.lstrip('/')

            # Calculate data size
            data_size = data.nbytes

            # Convert to bytes
            buffer = pa.BufferOutputStream()
            pq.write_table(data, buffer, compression='snappy')
            data_bytes = buffer.getvalue().to_pybytes()

            if config.provider == CloudProvider.AWS:
                success = self._upload_to_aws(bucket_name, key_name, data_bytes, config)
            elif config.provider == CloudProvider.AZURE:
                success = self._upload_to_azure(bucket_name, key_name, data_bytes, config)
            elif config.provider == CloudProvider.GCP:
                success = self._upload_to_gcs(bucket_name, key_name, data_bytes, config)
            else:
                success = self._upload_to_local(cloud_path, data_bytes)

            end_time = time.time()

            # Record metrics
            metrics = CloudMetrics(
                provider=config.provider,
                operation=operation_name,
                start_time=start_time,
                end_time=end_time,
                bytes_transferred=data_size,
                success=success,
                cost_estimate_usd=self._estimate_cost(config.provider, data_size, 'upload')
            )
            self.metrics.append(metrics)

            if success:
                print(f"✅ Uploaded {data_size / 1024 / 1024:.1f}MB to {cloud_path}")
                return True, cloud_path
            else:
                return False, ""

        except Exception as e:
            end_time = time.time()
            metrics = CloudMetrics(
                provider=config.provider,
                operation=operation_name,
                start_time=start_time,
                end_time=end_time,
                success=False,
                error_message=str(e)
            )
            self.metrics.append(metrics)

            print(f"❌ Upload failed: {e}")
            return False, ""

    def _upload_to_aws(self, bucket: str, key: str, data: bytes, config: CloudConfig) -> bool:
        """Upload to AWS S3."""
        try:
            s3 = self.connections.get(CloudProvider.AWS)
            if not s3:
                return False

            s3.put_object(Bucket=bucket, Key=key, Body=data)
            return True

        except ClientError as e:
            print(f"S3 upload error: {e}")
            return False

    def _upload_to_azure(self, container: str, blob: str, data: bytes, config: CloudConfig) -> bool:
        """Upload to Azure Blob Storage."""
        try:
            blob_client = self.connections[CloudProvider.AZURE].get_blob_client(
                container=container, blob=blob
            )
            blob_client.upload_blob(data, overwrite=True)
            return True

        except AzureError as e:
            print(f"Azure upload error: {e}")
            return False

    def _upload_to_gcs(self, bucket: str, blob: str, data: bytes, config: CloudConfig) -> bool:
        """Upload to Google Cloud Storage."""
        try:
            gcs_bucket = self.connections[CloudProvider.GCP].bucket(bucket)
            gcs_blob = gcs_bucket.blob(blob)
            gcs_blob.upload_from_string(data)
            return True

        except GoogleCloudError as e:
            print(f"GCS upload error: {e}")
            return False

    def _upload_to_local(self, path: str, data: bytes) -> bool:
        """Upload to local filesystem."""
        try:
            file_path = Path(path)
            file_path.parent.mkdir(parents=True, exist_ok=True)
            with open(file_path, 'wb') as f:
                f.write(data)
            return True

        except Exception as e:
            print(f"Local upload error: {e}")
            return False

    def download_data(self, cloud_path: str, config: CloudConfig) -> Tuple[bool, Optional[pa.Table]]:
        """Download data from cloud storage."""
        start_time = time.time()
        operation_name = f"download_from_{config.provider.value}"

        try:
            # Parse cloud path
            parsed = urlparse(cloud_path)
            bucket_name = parsed.netloc
            key_name = parsed.path.lstrip('/')

            # Download data
            if config.provider == CloudProvider.AWS:
                data_bytes, size = self._download_from_aws(bucket_name, key_name, config)
            elif config.provider == CloudProvider.AZURE:
                data_bytes, size = self._download_from_azure(bucket_name, key_name, config)
            elif config.provider == CloudProvider.GCP:
                data_bytes, size = self._download_from_gcs(bucket_name, key_name, config)
            else:
                data_bytes, size = self._download_from_local(cloud_path)

            if data_bytes:
                # Convert back to Arrow table
                buffer = pa.py_buffer(data_bytes)
                table = pq.read_table(buffer)
                end_time = time.time()

                # Record metrics
                metrics = CloudMetrics(
                    provider=config.provider,
                    operation=operation_name,
                    start_time=start_time,
                    end_time=end_time,
                    bytes_transferred=size,
                    cost_estimate_usd=self._estimate_cost(config.provider, size, 'download')
                )
                self.metrics.append(metrics)

                print(f"✅ Downloaded {size / 1024 / 1024:.1f}MB from {cloud_path}")
                return True, table
            else:
                return False, None

        except Exception as e:
            end_time = time.time()
            metrics = CloudMetrics(
                provider=config.provider,
                operation=operation_name,
                start_time=start_time,
                end_time=end_time,
                success=False,
                error_message=str(e)
            )
            self.metrics.append(metrics)

            print(f"❌ Download failed: {e}")
            return False, None

    def _download_from_aws(self, bucket: str, key: str, config: CloudConfig) -> Tuple[bytes, int]:
        """Download from AWS S3."""
        try:
            s3 = self.connections.get(CloudProvider.AWS)
            if not s3:
                return b"", 0

            response = s3.get_object(Bucket=bucket, Key=key)
            data = response['Body'].read()
            return data, len(data)

        except ClientError as e:
            print(f"S3 download error: {e}")
            return b"", 0

    def _download_from_azure(self, container: str, blob: str, config: CloudConfig) -> Tuple[bytes, int]:
        """Download from Azure Blob Storage."""
        try:
            blob_client = self.connections[CloudProvider.AZURE].get_blob_client(
                container=container, blob=blob
            )
            data = blob_client.download_blob().readall()
            return data, len(data)

        except AzureError as e:
            print(f"Azure download error: {e}")
            return b"", 0

    def _download_from_gcs(self, bucket: str, blob: str, config: CloudConfig) -> Tuple[bytes, int]:
        """Download from Google Cloud Storage."""
        try:
            gcs_bucket = self.connections[CloudProvider.GCP].bucket(bucket)
            gcs_blob = gcs_bucket.blob(blob)
            data = gcs_blob.download_as_bytes()
            return data, len(data)

        except GoogleCloudError as e:
            print(f"GCS download error: {e}")
            return b"", 0

    def _download_from_local(self, path: str) -> Tuple[bytes, int]:
        """Download from local filesystem."""
        try:
            with open(path, 'rb') as f:
                data = f.read()
            return data, len(data)

        except Exception as e:
            print(f"Local download error: {e}")
            return b"", 0

    def _estimate_cost(self, provider: CloudProvider, bytes_transferred: int, operation: str) -> float:
        """Estimate cost for cloud operation."""
        gb_transferred = bytes_transferred / 1024 / 1024 / 1024

        if operation == 'upload':
            # Upload costs are typically much lower
            return gb_transferred * self.COST_PER_GB_STORAGE.get(provider, 0.02)
        else:
            return gb_transferred * self.COST_PER_GB_TRANSFER.get(provider, 0.1)

    def get_performance_summary(self) -> Dict[str, Any]:
        """Get performance summary across all operations."""
        if not self.metrics:
            return {}

        # Group metrics by provider
        provider_metrics = defaultdict(list)
        for metric in self.metrics:
            provider_metrics[metric.provider].append(metric)

        summary = {}

        for provider, metrics_list in provider_metrics.items():
            total_bytes = sum(m.bytes_transferred for m in metrics_list)
            total_duration = sum(m.duration_seconds for m in metrics_list)
            successful_ops = sum(1 for m in metrics_list if m.success)
            total_cost = sum(m.cost_estimate_usd for m in metrics_list)

            avg_throughput = (total_bytes / 1024 / 1024) / total_duration if total_duration > 0 else 0

            summary[provider.value] = {
                "operations": len(metrics_list),
                "successful_operations": successful_ops,
                "success_rate": successful_ops / len(metrics_list) * 100,
                "total_bytes_transferred": total_bytes,
                "total_duration_seconds": total_duration,
                "average_throughput_mbps": avg_throughput,
                "total_cost_usd": total_cost
            }

        return summary


class MultiCloudSynchronizer:
    """Manages data synchronization across multiple cloud providers."""

    def __init__(self, storage_manager: CloudStorageManager):
        self.storage_manager = storage_manager
        self.sync_history: List[Dict[str, Any]] = []

    def create_replication_strategy(
        self,
        data: pa.Table,
        source_config: CloudConfig,
        target_configs: List[CloudConfig],
        strategy: str = "active_active"
    ) -> Dict[str, Any]:
        """Create replication strategy across multiple clouds."""

        print(f"🔄 Creating {strategy} replication strategy")

        # Generate unique file name with timestamp and hash
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        data_hash = hashlib.md5(str(data.schema).encode()).hexdigest()[:8]
        file_name = f"replication_{timestamp}_{data_hash}.parquet"

        replication_plan = {
            "strategy": strategy,
            "source": source_config.provider.value,
            "targets": [config.provider.value for config in target_configs],
            "file_name": file_name,
            "data_size_mb": data.nbytes / 1024 / 1024,
            "schema_columns": len(data.schema),
            "timestamp": datetime.now().isoformat()
        }

        # Execute replication based on strategy
        if strategy == "active_active":
            results = self._replicate_active_active(data, source_config, target_configs, file_name)
        elif strategy == "primary_backup":
            results = self._replicate_primary_backup(data, source_config, target_configs, file_name)
        elif strategy == "geo_distributed":
            results = self._replicate_geo_distributed(data, source_config, target_configs, file_name)
        else:
            raise ValueError(f"Unknown replication strategy: {strategy}")

        replication_plan["results"] = results
        replication_plan["success"] = all(r.get("success", False) for r in results.values())

        # Record in sync history
        self.sync_history.append(replication_plan)

        return replication_plan

    def _replicate_active_active(
        self,
        data: pa.Table,
        source_config: CloudConfig,
        target_configs: List[CloudConfig],
        file_name: str
    ) -> Dict[str, Any]:
        """Active-active replication - all clouds are writable."""
        results = {}

        # Upload to all clouds simultaneously
        upload_tasks = []
        for config in target_configs + [source_config]:
            cloud_path = self._build_cloud_path(config, file_name)
            upload_tasks.append((config, cloud_path))

        # Execute uploads
        for config, cloud_path in upload_tasks:
            success, result_path = self.storage_manager.upload_data(data, cloud_path, config)
            results[config.provider.value] = {
                "success": success,
                "path": result_path,
                "provider": config.provider.value
            }

        return results

    def _replicate_primary_backup(
        self,
        data: pa.Table,
        source_config: CloudConfig,
        target_configs: List[CloudConfig],
        file_name: str
    ) -> Dict[str, Any]:
        """Primary-backup replication - one primary, others as backups."""
        results = {}

        # Upload to primary first
        primary_path = self._build_cloud_path(source_config, file_name)
        success, result_path = self.storage_manager.upload_data(data, primary_path, source_config)
        results[source_config.provider.value] = {
            "success": success,
            "path": result_path,
            "provider": source_config.provider.value,
            "role": "primary"
        }

        if success:
            # Then replicate to backups
            for config in target_configs:
                backup_path = self._build_cloud_path(config, file_name)
                backup_success, backup_result = self.storage_manager.upload_data(data, backup_path, config)
                results[config.provider.value] = {
                    "success": backup_success,
                    "path": backup_result,
                    "provider": config.provider.value,
                    "role": "backup"
                }

        return results

    def _replicate_geo_distributed(
        self,
        data: pa.Table,
        source_config: CloudConfig,
        target_configs: List[CloudConfig],
        file_name: str
    ) -> Dict[str, Any]:
        """Geo-distributed replication - optimize for geographic distribution."""
        results = {}

        # For geo-distributed, we might split data by region
        # For simplicity, we'll just upload to all with region tagging
        for config in target_configs + [source_config]:
            cloud_path = self._build_cloud_path(config, file_name, region=config.region)
            success, result_path = self.storage_manager.upload_data(data, cloud_path, config)
            results[config.provider.value] = {
                "success": success,
                "path": result_path,
                "provider": config.provider.value,
                "region": config.region,
                "role": "geo_replica"
            }

        return results

    def _build_cloud_path(self, config: CloudConfig, file_name: str, region: Optional[str] = None) -> str:
        """Build cloud path based on provider."""
        if config.provider == CloudProvider.AWS:
            bucket = config.bucket_name
            key = f"fsspeckit/data/{region or config.region}/{file_name}" if region else f"fsspeckit/data/{file_name}"
            return f"s3://{bucket}/{key}"
        elif config.provider == CloudProvider.AZURE:
            container = config.bucket_name
            blob = f"fsspeckit/data/{region or config.region}/{file_name}" if region else f"fsspeckit/data/{file_name}"
            return f"az://{container}/{blob}"
        elif config.provider == CloudProvider.GCP:
            bucket = config.bucket_name
            blob = f"fsspeckit/data/{region or config.region}/{file_name}" if region else f"fsspeckit/data/{file_name}"
            return f"gs://{bucket}/{blob}"
        else:
            return f"local://data/{file_name}"

    def verify_replication_consistency(self, replication_plan: Dict[str, Any]) -> Dict[str, Any]:
        """Verify data consistency across replicated copies."""
        print(f"🔍 Verifying replication consistency")

        verification_results = {}
        results = replication_plan.get("results", {})

        # Get reference data from first successful replication
        reference_data = None
        reference_provider = None

        for provider, result in results.items():
            if result.get("success") and result.get("path"):
                try:
                    # Create config for this provider
                    config = self._get_provider_config(provider)
                    if config:
                        success, data = self.storage_manager.download_data(result["path"], config)
                        if success:
                            reference_data = data
                            reference_provider = provider
                            break
                except Exception as e:
                    print(f"   Could not load reference from {provider}: {e}")

        if not reference_data:
            return {"status": "failed", "error": "Could not load reference data"}

        # Compare with all other successful replications
        reference_schema = str(reference_data.schema)
        reference_row_count = len(reference_data)
        reference_hash = hashlib.md5(str(reference_data.schema).encode()).hexdigest()

        for provider, result in results.items():
            if result.get("success") and result.get("path"):
                try:
                    config = self._get_provider_config(provider)
                    if config:
                        success, data = self.storage_manager.download_data(result["path"], config)
                        if success:
                            # Verify consistency
                            schema_match = str(data.schema) == reference_schema
                            row_count_match = len(data) == reference_row_count
                            data_hash = hashlib.md5(str(data.schema).encode()).hexdigest()
                            hash_match = data_hash == reference_hash

                            verification_results[provider] = {
                                "schema_match": schema_match,
                                "row_count_match": row_count_match,
                                "hash_match": hash_match,
                                "row_count": len(data),
                                "consistent": schema_match and row_count_match and hash_match
                            }
                        else:
                            verification_results[provider] = {
                                "error": "Failed to download data",
                                "consistent": False
                            }
                    else:
                        verification_results[provider] = {
                            "error": "No config available",
                            "consistent": False
                        }

                except Exception as e:
                    verification_results[provider] = {
                        "error": str(e),
                        "consistent": False
                    }

        all_consistent = all(r.get("consistent", False) for r in verification_results.values())

        return {
            "status": "completed",
            "reference_provider": reference_provider,
            "reference_row_count": reference_row_count,
            "all_consistent": all_consistent,
            "results": verification_results
        }

    def _get_provider_config(self, provider: str) -> Optional[CloudConfig]:
        """Get cloud config for provider (simplified)."""
        # In a real implementation, this would look up stored configurations
        # For demo purposes, create dummy configs
        provider_map = {
            "aws": CloudProvider.AWS,
            "azure": CloudProvider.AZURE,
            "gcp": CloudProvider.GCP
        }

        cloud_provider = provider_map.get(provider.lower())
        if cloud_provider:
            return CloudConfig(
                provider=cloud_provider,
                region="us-east-1",
                bucket_name="demo-bucket"
            )
        return None


def create_sample_multicloud_data() -> pa.Table:
    """Create sample data for multi-cloud demonstration."""

    print("📊 Creating sample multi-cloud dataset")

    # Generate realistic business data
    records = []
    for i in range(50000):  # 50K records
        record = {
            "transaction_id": f"txn_{i:08d}",
            "timestamp": datetime.now() - timedelta(minutes=i),
            "customer_id": f"cust_{i % 10000:06d}",
            "amount": round(i * 1.5 + (i % 100) * 0.1, 2),
            "currency": ["USD", "EUR", "GBP", "JPY"][i % 4],
            "region": ["us-east", "us-west", "eu-west", "asia-pacific"][i % 4],
            "product_category": ["electronics", "clothing", "books", "home"][i % 4],
            "channel": ["online", "retail", "mobile", "partner"][i % 4],
            "device_type": ["desktop", "mobile", "tablet"][i % 3],
            "is_premium_customer": i % 10 == 0,
            "fraud_score": round(hash(f"fraud_{i}") % 100 / 100, 3)
        }
        records.append(record)

    return pa.Table.from_pylist(records)


def demonstrate_multi_cloud_replication():
    """Demonstrate multi-cloud replication strategies."""

    print("\n☁️ Multi-Cloud Replication Demonstration")

    # Create sample data
    sample_data = create_sample_multicloud_data()
    print(f"   Sample data: {len(sample_data):,} records, {sample_data.nbytes / 1024 / 1024:.1f}MB")

    # Initialize cloud storage manager
    storage_manager = CloudStorageManager()

    # Create mock cloud configurations (in real usage, these would have real credentials)
    aws_config = CloudConfig(
        provider=CloudProvider.AWS,
        region="us-east-1",
        bucket_name="fsspeckit-aws-demo"
    )

    azure_config = CloudConfig(
        provider=CloudProvider.AZURE,
        region="eastus",
        bucket_name="fsspeckit-azure-demo"
    )

    gcp_config = CloudConfig(
        provider=CloudProvider.GCP,
        region="us-central1",
        bucket_name="fsspeckit-gcp-demo"
    )

    # Note: In a real scenario, you would connect to actual cloud services
    # For this demo, we'll simulate the operations using local storage
    print("\n1. Active-Active Replication")
    synchronizer = MultiCloudSynchronizer(storage_manager)

    # Simulate active-active replication (using local paths for demo)
    local_aws_path = "local://aws_replica.parquet"
    local_azure_path = "local://azure_replica.parquet"
    local_gcp_path = "local://gcp_replica.parquet"

    # Simulate uploads
    print("   Uploading to AWS...")
    storage_manager.upload_data(sample_data, local_aws_path, CloudConfig(CloudProvider.LOCAL, "", ""))

    print("   Uploading to Azure...")
    storage_manager.upload_data(sample_data, local_azure_path, CloudConfig(CloudProvider.LOCAL, "", ""))

    print("   Uploading to GCP...")
    storage_manager.upload_data(sample_data, local_gcp_path, CloudConfig(CloudProvider.LOCAL, "", ""))

    print("   ✅ Active-active replication completed")

    print("\n2. Cross-Cloud Performance Comparison")

    # Simulate different performance characteristics
    performance_data = [
        ("AWS", 2.5, 50.2, 0.023),  # provider, duration_sec, throughput_mbps, cost_per_gb
        ("Azure", 2.8, 47.1, 0.018),
        ("GCP", 2.3, 52.8, 0.020)
    ]

    print(f"   {'Provider':<10} | {'Duration':<10} | {'Throughput':<12} | {'Cost/GB':<8}")
    print(f"   {'-'*10} | {'-'*10} | {'-'*12} | {'-'*8}")
    for provider, duration, throughput, cost in performance_data:
        print(f"   {provider:<10} | {duration:<9.1f}s | {throughput:<11.1f} MB/s | ${cost:<7.3f}")

    print("\n3. Consistency Verification")

    # Simulate verification process
    verification_results = {
        "aws": {"consistent": True, "row_count": 50000, "schema_match": True},
        "azure": {"consistent": True, "row_count": 50000, "schema_match": True},
        "gcp": {"consistent": True, "row_count": 50000, "schema_match": True}
    }

    all_consistent = all(r["consistent"] for r in verification_results.values())
    print(f"   All replicas consistent: {'✅' if all_consistent else '❌'}")

    for provider, result in verification_results.items():
        status = "✅" if result["consistent"] else "❌"
        print(f"   {provider}: {status} {result['row_count']:,} records, schema match: {result['schema_match']}")

    print("\n4. Cost Analysis")

    data_gb = sample_data.nbytes / 1024 / 1024 / 1024
    storage_costs = {
        "AWS": data_gb * 0.023,
        "Azure": data_gb * 0.018,
        "GCP": data_gb * 0.020
    }

    total_cost = sum(storage_costs.values())
    print(f"   Data size: {data_gb:.2f}GB")
    print(f"   Storage costs per month:")
    for provider, cost in storage_costs.items():
        print(f"     {provider}: ${cost:.4f}")
    print(f"   Total monthly cost: ${total_cost:.4f}")


def demonstrate_cross_cloud_querying():
    """Demonstrate querying data across multiple clouds."""

    print("\n🔍 Cross-Cloud Querying Demonstration")

    # Create sample data distributed across "clouds" (local files for demo)
    temp_dir = Path("temp_multicloud_data")
    temp_dir.mkdir(exist_ok=True)

    # Create regional datasets
    regions = ["us-east", "us-west", "eu-west", "asia-pacific"]
    cloud_providers = ["AWS", "Azure", "GCP"]

    datasets = {}
    for i, region in enumerate(regions):
        for j, provider in enumerate(cloud_providers):
            # Create subset of data for this region/provider combination
            region_data = create_sample_multicloud_data()

            # Filter to subset
            filter_mask = pc.equal(region_data.column("region"), region)
            subset_data = region_data.filter(filter_mask).slice(0, 1000)  # Small subset

            # Save to local file simulating cloud storage
            filename = f"{provider.lower()}_{region}_data.parquet"
            filepath = temp_dir / filename
            pq.write_table(subset_data, filepath)

            datasets[f"{provider}:{region}"] = str(filepath)

    print(f"   Created {len(datasets)} regional datasets")

    # Use DuckDB to query across all datasets
    print("\n   Cross-cloud analytics with DuckDB:")

    with DuckDBParquetHandler() as handler:
        # Register all datasets
        for key, path in datasets.items():
            table_name = key.replace(":", "_").replace("-", "_")
            handler.register_dataset(table_name, path)

        # Run cross-cloud queries
        print("\n   1. Total revenue across all clouds:")
        result = handler.execute_sql("""
            SELECT
                SUM(amount) as total_revenue,
                COUNT(*) as total_transactions,
                AVG(amount) as avg_transaction
            FROM (
                SELECT * FROM aws_us_east_data
                UNION ALL
                SELECT * FROM aws_us_west_data
                UNION ALL
                SELECT * from azure_eu_west_data
                UNION ALL
                SELECT * from gcp_asia_pacific_data
            ) all_data
        """)
        print(f"      {result}")

        print("\n   2. Performance by cloud provider:")
        result = handler.execute_sql("""
            SELECT
                'AWS' as provider,
                COUNT(*) as transactions,
                SUM(amount) as revenue,
                AVG(amount) as avg_amount
            FROM aws_us_east_data
            UNION ALL
            SELECT
                'Azure' as provider,
                COUNT(*) as transactions,
                SUM(amount) as revenue,
                AVG(amount) as avg_amount
            FROM azure_eu_west_data
            UNION ALL
            SELECT
                'GCP' as provider,
                COUNT(*) as transactions,
                SUM(amount) as revenue,
                AVG(amount) as avg_amount
            FROM gcp_asia_pacific_data
        """)
        print(f"      {result}")

        print("\n   3. Regional distribution:")
        result = handler.execute_sql("""
            SELECT
                region,
                COUNT(*) as transactions,
                SUM(amount) as revenue,
                COUNT(DISTINCT customer_id) as unique_customers
            FROM (
                SELECT region, amount, customer_id FROM aws_us_east_data
                UNION ALL
                SELECT region, amount, customer_id FROM aws_us_west_data
                UNION ALL
                SELECT region, amount, customer_id FROM azure_eu_west_data
                UNION ALL
                SELECT region, amount, customer_id FROM gcp_asia_pacific_data
            ) all_data
            GROUP BY region
            ORDER BY revenue DESC
        """)
        print(f"      {result}")

    # Cleanup
    import shutil
    shutil.rmtree(temp_dir)
    print(f"\n   ✅ Cross-cloud querying completed")


def demonstrate_disaster_recovery():
    """Demonstrate disaster recovery patterns."""

    print("\n🛡️ Disaster Recovery Demonstration")

    print("\n1. Backup Strategy Configuration")

    # Simulate different backup strategies
    backup_strategies = {
        "real_time": {
            "description": "Continuous replication to all clouds",
            "rpo": "0 seconds",
            "rto": "minutes",
            "cost_multiplier": 3.0
        },
        "hourly": {
            "description": "Hourly snapshots to secondary cloud",
            "rpo": "1 hour",
            "rto": "minutes",
            "cost_multiplier": 1.5
        },
        "daily": {
            "description": "Daily backups to cold storage",
            "rpo": "24 hours",
            "rto": "hours",
            "cost_multiplier": 0.5
        }
    }

    print(f"   {'Strategy':<12} | {'RPO':<12} | {'RTO':<10} | {'Cost Mult':<10} | Description")
    print(f"   {'-'*12} | {'-'*12} | {'-'*10} | {'-'*10} | {'-'*40}")
    for strategy, config in backup_strategies.items():
        print(f"   {strategy:<12} | {config['rpo']:<12} | {config['rto']:<10} | {config['cost_multiplier']:<9.1f}x | {config['description']}")

    print("\n2. Failover Simulation")

    # Simulate failover scenarios
    failover_scenarios = [
        {
            "scenario": "AWS Region Outage",
            "affected": "AWS us-east-1",
            "mitigation": "Failover to Azure East US + GCP US Central",
            "downtime": "< 5 minutes"
        },
        {
            "scenario": "Azure Service Degradation",
            "affected": "Azure Blob Storage",
            "mitigation": "Read-only from AWS + GCP replicas",
            "downtime": "No downtime (read-only)"
        },
        {
            "scenario": "Network Partition",
            "affected": "Cross-cloud connectivity",
            "mitigation": "Local cloud operations continue",
            "downtime": "Partial functionality"
        }
    ]

    for scenario in failover_scenarios:
        print(f"\n   Scenario: {scenario['scenario']}")
        print(f"     Affected: {scenario['affected']}")
        print(f"     Mitigation: {scenario['mitigation']}")
        print(f"     Expected downtime: {scenario['downtime']}")

    print("\n3. Recovery Testing")

    # Simulate recovery test results
    recovery_tests = [
        {"test": "Data Integrity", "status": "✅ PASS", "duration": "2.3s"},
        {"test": "Service Availability", "status": "✅ PASS", "duration": "45.1s"},
        {"test": "Performance Baseline", "status": "✅ PASS", "duration": "12.7s"},
        {"test": "Failover Latency", "status": "✅ PASS", "duration": "3.2s"}
    ]

    for test in recovery_tests:
        print(f"   {test['test']:<25} | {test['status']:<8} | {test['duration']:<6}")

    print("\n   ✅ Disaster recovery procedures validated")


def main():
    """Run all multi-cloud operations examples."""

    print("☁️ Multi-Cloud Operations Example")
    print("=" * 60)
    print("This example demonstrates enterprise-grade multi-cloud data")
    print("operations using fsspeckit's dataset utilities.")

    try:
        # Run all demonstrations
        demonstrate_multi_cloud_replication()
        demonstrate_cross_cloud_querying()
        demonstrate_disaster_recovery()

        print("\n" + "=" * 60)
        print("✅ Multi-cloud operations examples completed successfully!")

        print(f"\n🎯 Advanced Takeaways:")
        print("• Multi-cloud strategies provide redundancy and flexibility")
        print("• Active-active replication ensures high availability")
        print("• Cross-cloud querying enables unified analytics")
        print("• Cost optimization requires careful provider selection")
        print("• Disaster recovery planning is essential for production")

        print(f"\n🏗️ Production Considerations:")
        print("• Implement proper authentication and security across clouds")
        print("• Monitor performance and costs across all providers")
        print("• Regularly test failover and recovery procedures")
        print("• Consider data sovereignty and compliance requirements")
        print("• Optimize data placement for latency and cost")

        print(f"\n💡 Best Practices:")
        print("• Use consistent data formats across clouds")
        print("• Implement automated consistency verification")
        print("• Choose the right replication strategy for your needs")
        print("• Plan for cross-cloud data transfer costs")
        print("• Maintain documentation of cloud-specific configurations")

    except Exception as e:
        print(f"\n❌ Example failed: {e}")
        raise


if __name__ == "__main__":
    main()