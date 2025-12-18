# Sync and Manage Files

This guide covers how to synchronize files and directories between different storage backends and manage file operations efficiently.

## File Synchronization

### Basic File Sync

```python
from fsspeckit import filesystem
from fsspeckit.common.misc import sync_files

# Create source and destination filesystems
src_fs = filesystem("file")
dst_fs = filesystem("s3://bucket/")

# Sync individual files
sync_files(
    add_files=["file1.txt", "file2.txt", "file3.txt"],
    delete_files=[],
    src_fs=src_fs,
    dst_fs=dst_fs,
    src_path="/source/",
    dst_path="/target/",
    verbose=True
)
```

### Sync with Deletions

```python
# Sync files including deletions
sync_files(
    add_files=["new_file.txt", "updated_file.txt"],
    delete_files=["old_file.txt", "obsolete_file.txt"],
    src_fs=src_fs,
    dst_fs=dst_fs,
    src_path="/source/",
    dst_path="/target/",
    verbose=True
)
```

### Directory Synchronization

```python
from fsspeckit.common.misc import sync_dir

# Synchronize entire directories
sync_dir(
    src_fs=src_fs,
    dst_fs=dst_fs,
    src_path="/local/data/",
    dst_path="/s3/remote-data/",
    parallel=True,
    n_jobs=4,
    verbose=True
)
```

### Cross-Cloud Synchronization

```python
# Sync between different cloud providers
s3_fs = filesystem("s3", storage_options=s3_options)
gcs_fs = filesystem("gs", storage_options=gcs_options)

sync_dir(
    src_fs=s3_fs,
    dst_fs=gcs_fs,
    src_path="s3://source-bucket/data/",
    dst_path="gs://dest-bucket/data/",
    progress=True
)
```

## Advanced Synchronization

### Selective Sync

```python
import os
from pathlib import Path

def get_recent_files(directory, days=7):
    """Get files modified in last N days"""
    recent_files = []
    cutoff_time = time.time() - (days * 24 * 3600)
    
    for root, dirs, files in os.walk(directory):
        for file in files:
            file_path = os.path.join(root, file)
            if os.path.getmtime(file_path) > cutoff_time:
                rel_path = os.path.relpath(file_path, directory)
                recent_files.append(rel_path)
    
    return recent_files

# Sync only recent files
recent_files = get_recent_files("/data/source", days=7)
sync_files(
    add_files=recent_files,
    delete_files=[],
    src_fs=src_fs,
    dst_fs=dst_fs,
    src_path="/data/source/",
    dst_path="/backup/recent/",
    verbose=True
)
```

### Batch Synchronization

```python
def sync_in_batches(src_fs, dst_fs, file_list, batch_size=100):
    """Sync files in batches to avoid overwhelming the system"""
    
    for i in range(0, len(file_list), batch_size):
        batch = file_list[i:i + batch_size]
        print(f"Syncing batch {i//batch_size + 1}: {len(batch)} files")
        
        sync_files(
            add_files=batch,
            delete_files=[],
            src_fs=src_fs,
            dst_fs=dst_fs,
            src_path="/source/",
            dst_path="/target/",
            verbose=False
        )
        
        # Small delay between batches
        time.sleep(1)

# Usage
all_files = src_fs.ls("/source/", detail=True)
file_names = [f['name'] for f in all_files if f['type'] == 'file']
sync_in_batches(src_fs, dst_fs, file_names, batch_size=50)
```

### Conflict Resolution

```python
def sync_with_conflict_resolution(src_fs, dst_fs, src_path, dst_path, strategy="newer"):
    """Sync with conflict resolution"""
    
    # Get file lists from both sides
    src_files = src_fs.ls(src_path, detail=True)
    dst_files = dst_fs.ls(dst_path, detail=True)
    
    src_file_dict = {f['name']: f for f in src_files}
    dst_file_dict = {f['name']: f for f in dst_files}
    
    files_to_add = []
    files_to_delete = []
    
    # Check for files to add/update
    for name, src_info in src_file_dict.items():
        if name not in dst_file_dict:
            # File exists only in source
            files_to_add.append(name)
        else:
            dst_info = dst_file_dict[name]
            
            # Conflict resolution based on strategy
            if strategy == "newer":
                if src_info['modified'] > dst_info['modified']:
                    files_to_add.append(name)
            elif strategy == "larger":
                if src_info['size'] > dst_info['size']:
                    files_to_add.append(name)
            elif strategy == "source_wins":
                files_to_add.append(name)
    
    # Check for files to delete
    for name in dst_file_dict:
        if name not in src_file_dict:
            files_to_delete.append(name)
    
    # Perform sync
    if files_to_add or files_to_delete:
        sync_files(
            add_files=files_to_add,
            delete_files=files_to_delete,
            src_fs=src_fs,
            dst_fs=dst_fs,
            src_path=src_path,
            dst_path=dst_path,
            verbose=True
        )
    
    return len(files_to_add), len(files_to_delete)

# Usage
added, deleted = sync_with_conflict_resolution(
    src_fs, dst_fs, 
    "/source/", "/target/", 
    strategy="newer"
)
print(f"Added/updated: {added}, Deleted: {deleted}")
```

## Partition Management

### Extract Partition Information

```python
from fsspeckit.common.misc import get_partitions_from_path

# Extract partitions from file paths
paths = [
    "data/year=2023/month=01/day=15/file.parquet",
    "data/year=2023/month=01/day=16/file.parquet",
    "data/year=2023/month=02/day=01/file.parquet",
    "data/category=sales/region=us-east/data.parquet"
]

for path in paths:
    partitions = get_partitions_from_path(path)
    print(f"Path: {path}")
    print(f"Partitions: {partitions}")
    print()

# Output:
# Path: data/year=2023/month=01/day=15/file.parquet
# Partitions: {'year': '2023', 'month': '01', 'day': '15'}
```

### Partition-Based Sync

```python
def sync_partitions(src_fs, dst_fs, base_path, partition_filter=None):
    """Sync specific partitions"""
    
    # Get all directories (partitions)
    all_dirs = src_fs.find(base_path, directories=True, files=False)
    
    # Filter partitions if specified
    if partition_filter:
        filtered_dirs = []
        for dir_path in all_dirs:
            partitions = get_partitions_from_path(dir_path)
            if partition_filter(partitions):
                filtered_dirs.append(dir_path)
        dirs_to_sync = filtered_dirs
    else:
        dirs_to_sync = all_dirs
    
    # Sync each partition
    for partition_dir in dirs_to_sync:
        rel_path = os.path.relpath(partition_dir, base_path)
        
        sync_dir(
            src_fs=src_fs,
            dst_fs=dst_fs,
            src_path=partition_dir,
            dst_path=f"{base_path}/{rel_path}",
            verbose=True
        )

# Usage: Sync only 2023 data
def filter_2023(partitions):
    return partitions.get('year') == '2023'

sync_partitions(src_fs, dst_fs, "/data/", filter_2023)
```

### Partition Maintenance

```python
def cleanup_old_partitions(fs, base_path, partition_key, keep_count):
    """Keep only N most recent partitions"""
    
    # Get all partition directories
    partition_dirs = []
    for item in fs.ls(base_path, detail=True):
        if item['type'] == 'directory':
            partitions = get_partitions_from_path(item['name'])
            if partition_key in partitions:
                partition_dirs.append((partitions[partition_key], item['name']))
    
    # Sort by partition value (assuming it's sortable)
    partition_dirs.sort(reverse=True)
    
    # Delete old partitions
    for i, (value, dir_path) in enumerate(partition_dirs):
        if i >= keep_count:
            print(f"Deleting old partition: {dir_path} (value: {value})")
            fs.rm(dir_path, recursive=True)

# Usage: Keep last 12 months of data
cleanup_old_partitions(fs, "/data/", "month", 12)
```

## File Operations

### Batch File Operations

```python
def batch_copy_files(src_fs, dst_fs, file_pairs, batch_size=10):
    """Copy multiple files in batches"""
    
    for i in range(0, len(file_pairs), batch_size):
        batch = file_pairs[i:i + batch_size]
        print(f"Processing batch {i//batch_size + 1}: {len(batch)} files")
        
        for src_path, dst_path in batch:
            try:
                # Copy file
                with src_fs.open(src_path, 'rb') as src_file:
                    with dst_fs.open(dst_path, 'wb') as dst_file:
                        dst_file.write(src_file.read())
                
                print(f"Copied: {src_path} -> {dst_path}")
                
            except Exception as e:
                print(f"Failed to copy {src_path}: {e}")

# Usage
file_pairs = [
    ("/source/file1.txt", "/dest/file1.txt"),
    ("/source/file2.txt", "/dest/file2.txt"),
    # ... more pairs
]

batch_copy_files(src_fs, dst_fs, file_pairs, batch_size=5)
```

### File Validation

```python
def validate_file_transfer(src_fs, dst_fs, src_path, dst_path):
    """Validate that file was transferred correctly"""
    
    # Check file sizes
    src_info = src_fs.info(src_path)
    dst_info = dst_fs.info(dst_path)
    
    if src_info['size'] != dst_info['size']:
        return False, f"Size mismatch: {src_info['size']} vs {dst_info['size']}"
    
    # Optional: Check checksums for critical files
    if src_info['size'] < 100 * 1024 * 1024:  # Files < 100MB
        import hashlib
        
        # Calculate source checksum
        with src_fs.open(src_path, 'rb') as f:
            src_hash = hashlib.md5(f.read()).hexdigest()
        
        # Calculate destination checksum
        with dst_fs.open(dst_path, 'rb') as f:
            dst_hash = hashlib.md5(f.read()).hexdigest()
        
        if src_hash != dst_hash:
            return False, f"Checksum mismatch: {src_hash} vs {dst_hash}"
    
    return True, "Validation successful"

def safe_sync_with_validation(src_fs, dst_fs, file_list):
    """Sync files with validation"""
    
    failed_files = []
    
    for file_path in file_list:
        # Copy file
        dst_path = file_path
        try:
            with src_fs.open(file_path, 'rb') as src_file:
                with dst_fs.open(dst_path, 'wb') as dst_file:
                    dst_file.write(src_file.read())
            
            # Validate
            is_valid, message = validate_file_transfer(src_fs, dst_fs, file_path, dst_path)
            
            if is_valid:
                print(f"✓ {file_path}: {message}")
            else:
                print(f"✗ {file_path}: {message}")
                failed_files.append(file_path)
                
        except Exception as e:
            print(f"✗ {file_path}: Transfer failed - {e}")
            failed_files.append(file_path)
    
    return failed_files

# Usage
failed = safe_sync_with_validation(src_fs, dst_fs, file_list)
if failed:
    print(f"Failed transfers: {len(failed)}")
```

## Performance Optimization

### Parallel Operations

```python
from fsspeckit.common.misc import run_parallel

def copy_single_file(args):
    """Copy single file - for parallel execution"""
    src_fs, dst_fs, src_path, dst_path = args
    
    try:
        with src_fs.open(src_path, 'rb') as src_file:
            with dst_fs.open(dst_path, 'wb') as dst_file:
                dst_file.write(src_file.read())
        return True, src_path, None
    except Exception as e:
        return False, src_path, str(e)

def parallel_copy(src_fs, dst_fs, file_pairs, max_workers=8):
    """Copy files in parallel"""
    
    # Prepare arguments
    args_list = [(src_fs, dst_fs, src, dst) for src, dst in file_pairs]
    
    # Run in parallel
    results = run_parallel(
        func=copy_single_file,
        data=args_list,
        max_workers=max_workers,
        progress=True
    )
    
    # Process results
    successful = []
    failed = []
    
    for success, file_path, error in results:
        if success:
            successful.append(file_path)
        else:
            failed.append((file_path, error))
    
    return successful, failed

# Usage
file_pairs = [("src/file1.txt", "dst/file1.txt") for _ in range(100)]
successful, failed = parallel_copy(src_fs, dst_fs, file_pairs, max_workers=4)

print(f"Successfully copied: {len(successful)}")
print(f"Failed: {len(failed)}")
for file_path, error in failed:
    print(f"  {file_path}: {error}")
```

### Memory-Efficient Operations

```python
def copy_large_file(src_fs, dst_fs, src_path, dst_path, chunk_size=8*1024*1024):
    """Copy large files in chunks to manage memory"""
    
    with src_fs.open(src_path, 'rb') as src_file:
        with dst_fs.open(dst_path, 'wb') as dst_file:
            while True:
                chunk = src_file.read(chunk_size)
                if not chunk:
                    break
                dst_file.write(chunk)
    
    return True

def sync_large_files(src_fs, dst_fs, file_list, size_threshold=100*1024*1024):
    """Sync files with special handling for large files"""
    
    for file_path in file_list:
        file_info = src_fs.info(file_path)
        
        if file_info['size'] > size_threshold:
            print(f"Copying large file ({file_info['size']/1024/1024:.1f}MB): {file_path}")
            copy_large_file(src_fs, dst_fs, file_path, file_path)
        else:
            # Use regular copy for smaller files
            with src_fs.open(file_path, 'rb') as src_file:
                with dst_fs.open(file_path, 'wb') as dst_file:
                    dst_file.write(src_file.read())
```

## Error Handling and Recovery

### Retry Logic

```python
import time
import random

def sync_with_retry(src_fs, dst_fs, src_path, dst_path, max_retries=3, backoff_factor=2):
    """Sync with exponential backoff retry"""
    
    for attempt in range(max_retries):
        try:
            sync_dir(
                src_fs=src_fs,
                dst_fs=dst_fs,
                src_path=src_path,
                dst_path=dst_path,
                verbose=False
            )
            return True, None
            
        except Exception as e:
            if attempt == max_retries - 1:
                return False, str(e)
            
            # Calculate delay with jitter
            base_delay = backoff_factor ** attempt
            jitter = random.uniform(0, 0.1 * base_delay)
            delay = base_delay + jitter
            
            print(f"Attempt {attempt + 1} failed: {e}")
            print(f"Retrying in {delay:.1f} seconds...")
            time.sleep(delay)
    
    return False, "Max retries exceeded"

# Usage
success, error = sync_with_retry(src_fs, dst_fs, "/src/", "/dst/")
if not success:
    print(f"Sync failed after retries: {error}")
```

### Incremental Sync

```python
def incremental_sync(src_fs, dst_fs, src_path, dst_path, state_file="sync_state.json"):
    """Incremental sync using state file"""
    
    import json
    import os
    
    # Load previous state
    state = {}
    if os.path.exists(state_file):
        with open(state_file, 'r') as f:
            state = json.load(f)
    
    # Get current files
    src_files = src_fs.ls(src_path, detail=True)
    
    files_to_sync = []
    updated_state = {}
    
    for file_info in src_files:
        file_path = file_info['name']
        file_key = os.path.relpath(file_path, src_path)
        
        # Check if file needs sync
        last_modified = file_info['modified']
        file_size = file_info['size']
        
        if (file_key not in state or 
            state[file_key]['modified'] != last_modified or
            state[file_key]['size'] != file_size):
            
            files_to_sync.append(file_path)
        
        # Update state
        updated_state[file_key] = {
            'modified': last_modified,
            'size': file_size
        }
    
    # Sync changed files
    if files_to_sync:
        print(f"Syncing {len(files_to_sync)} changed files...")
        
        for file_path in files_to_sync:
            rel_path = os.path.relpath(file_path, src_path)
            dst_path_full = os.path.join(dst_path, rel_path)
            
            # Ensure destination directory exists
            dst_fs.makedirs(os.path.dirname(dst_path_full), exist_ok=True)
            
            # Copy file
            with src_fs.open(file_path, 'rb') as src_file:
                with dst_fs.open(dst_path_full, 'wb') as dst_file:
                    dst_file.write(src_file.read())
            
            print(f"Synced: {rel_path}")
    
    # Save updated state
    with open(state_file, 'w') as f:
        json.dump(updated_state, f, indent=2, default=str)
    
    return len(files_to_sync)

# Usage
synced_count = incremental_sync(src_fs, dst_fs, "/source/", "/dest/")
print(f"Synced {synced_count} files")
```

## Best Practices

1. **Use Parallel Operations**: Enable parallel processing for large file sets
2. **Batch Processing**: Process files in batches to manage resources
3. **Validation**: Verify file transfers, especially for critical data
4. **Error Handling**: Implement retry logic and proper error handling
5. **Incremental Sync**: Use state tracking for efficient incremental updates
6. **Memory Management**: Use chunked copying for large files
7. **Monitoring**: Use verbose logging and progress tracking

For more information on filesystem operations, see [Work with Filesystems](work-with-filesystems.md).