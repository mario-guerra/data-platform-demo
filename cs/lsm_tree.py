"""
Log-Structured Merge Tree (LSM Tree) Implementation

A simplified implementation of an LSM tree for educational purposes.
LSM trees are widely used in modern databases like Cassandra, LevelDB, and RocksDB.

Key concepts demonstrated:
- Write-optimized data structure
- Memtable (in-memory sorted structure)
- SSTable (sorted string table) on disk
- Compaction process
- Range queries
"""

import heapq
import json
import os
import tempfile
import threading
import time
from collections import OrderedDict
from typing import Dict, Iterator, List, Optional, Tuple, Union


class MemTable:
    """In-memory sorted table using a balanced tree structure (simulated with OrderedDict)"""
    
    def __init__(self, max_size: int = 1000):
        self.data: OrderedDict = OrderedDict()
        self.max_size = max_size
        self.size = 0
        self.lock = threading.RLock()
    
    def put(self, key: str, value: str) -> bool:
        """Insert or update a key-value pair. Returns True if table is full."""
        with self.lock:
            if key not in self.data:
                self.size += 1
            self.data[key] = value
            # Keep OrderedDict sorted by key
            if len(self.data) > 1:
                self.data = OrderedDict(sorted(self.data.items()))
            return self.size >= self.max_size
    
    def get(self, key: str) -> Optional[str]:
        """Get value for a key."""
        with self.lock:
            return self.data.get(key)
    
    def delete(self, key: str) -> None:
        """Mark a key as deleted (tombstone)."""
        with self.lock:
            self.data[key] = None  # Tombstone
    
    def range_query(self, start_key: str, end_key: str) -> Iterator[Tuple[str, str]]:
        """Get all key-value pairs in range [start_key, end_key)."""
        with self.lock:
            for key, value in self.data.items():
                if start_key <= key < end_key and value is not None:
                    yield (key, value)
    
    def to_list(self) -> List[Tuple[str, Optional[str]]]:
        """Convert to sorted list for flushing to SSTable."""
        with self.lock:
            return list(self.data.items())
    
    def clear(self) -> None:
        """Clear the memtable."""
        with self.lock:
            self.data.clear()
            self.size = 0


class SSTable:
    """Sorted String Table - immutable sorted file on disk"""
    
    def __init__(self, filename: str, level: int = 0):
        self.filename = filename
        self.level = level
        self.index: Dict[str, int] = {}  # Sparse index: key -> file offset
        self.min_key: Optional[str] = None
        self.max_key: Optional[str] = None
        self.size = 0
    
    @classmethod
    def create_from_memtable(cls, memtable: MemTable, filename: str, level: int = 0) -> 'SSTable':
        """Create SSTable from memtable data."""
        sstable = cls(filename, level)
        
        with open(filename, 'w') as f:
            data = memtable.to_list()
            for i, (key, value) in enumerate(data):
                if i == 0:
                    sstable.min_key = key
                if value is not None:  # Skip tombstones in SSTable
                    # Build sparse index (every 10th key for demo)
                    if i % 10 == 0:
                        sstable.index[key] = f.tell()
                    
                    entry = json.dumps({"key": key, "value": value}) + "\n"
                    f.write(entry)
                    sstable.size += 1
            
            if data:
                sstable.max_key = data[-1][0]
        
        return sstable
    
    def get(self, key: str) -> Optional[str]:
        """Get value for a key from SSTable."""
        if not self._contains_key(key):
            return None
        
        # Find the appropriate position using sparse index
        start_offset = 0
        for idx_key, offset in self.index.items():
            if idx_key <= key:
                start_offset = offset
            else:
                break
        
        # Linear scan from the index position
        with open(self.filename, 'r') as f:
            f.seek(start_offset)
            for line in f:
                entry = json.loads(line.strip())
                if entry["key"] == key:
                    return entry["value"]
                elif entry["key"] > key:
                    break
        
        return None
    
    def range_query(self, start_key: str, end_key: str) -> Iterator[Tuple[str, str]]:
        """Get all key-value pairs in range [start_key, end_key)."""
        if not self._overlaps_range(start_key, end_key):
            return
        
        with open(self.filename, 'r') as f:
            for line in f:
                entry = json.loads(line.strip())
                key, value = entry["key"], entry["value"]
                if start_key <= key < end_key:
                    yield (key, value)
                elif key >= end_key:
                    break
    
    def _contains_key(self, key: str) -> bool:
        """Check if SSTable might contain the key."""
        return (self.min_key is None or key >= self.min_key) and \
               (self.max_key is None or key <= self.max_key)
    
    def _overlaps_range(self, start_key: str, end_key: str) -> bool:
        """Check if SSTable overlaps with the given range."""
        return (self.min_key is None or self.min_key < end_key) and \
               (self.max_key is None or self.max_key >= start_key)


class LSMTree:
    """Log-Structured Merge Tree implementation"""
    
    def __init__(self, base_dir: str = None, memtable_size: int = 100, max_levels: int = 4):
        self.base_dir = base_dir or tempfile.mkdtemp(prefix="lsm_")
        self.memtable_size = memtable_size
        self.max_levels = max_levels
        
        # Current memtable
        self.memtable = MemTable(memtable_size)
        
        # Immutable memtables waiting to be flushed
        self.immutable_memtables: List[MemTable] = []
        
        # SSTables organized by levels
        self.levels: List[List[SSTable]] = [[] for _ in range(max_levels)]
        
        # Threading
        self.lock = threading.RLock()
        self.flush_thread = None
        self.compaction_thread = None
        
        # Ensure base directory exists
        os.makedirs(self.base_dir, exist_ok=True)
    
    def put(self, key: str, value: str) -> None:
        """Insert or update a key-value pair."""
        with self.lock:
            # Add to current memtable
            is_full = self.memtable.put(key, value)
            
            if is_full:
                # Move current memtable to immutable list
                self.immutable_memtables.append(self.memtable)
                self.memtable = MemTable(self.memtable_size)
                
                # Trigger background flush
                self._trigger_flush()
    
    def get(self, key: str) -> Optional[str]:
        """Get value for a key."""
        with self.lock:
            # Check current memtable first
            value = self.memtable.get(key)
            if value is not None:
                return value
            
            # Check immutable memtables
            for memtable in reversed(self.immutable_memtables):
                value = memtable.get(key)
                if value is not None:
                    return value
            
            # Check SSTables from newest to oldest
            for level in self.levels:
                for sstable in reversed(level):  # Newer SSTables last
                    value = sstable.get(key)
                    if value is not None:
                        return value
            
            return None
    
    def delete(self, key: str) -> None:
        """Delete a key (add tombstone)."""
        self.put(key, None)  # Tombstone
    
    def range_query(self, start_key: str, end_key: str) -> Iterator[Tuple[str, str]]:
        """Get all key-value pairs in range [start_key, end_key)."""
        # Collect all sources and merge
        sources = []
        
        with self.lock:
            # Add memtable
            sources.append(self.memtable.range_query(start_key, end_key))
            
            # Add immutable memtables
            for memtable in self.immutable_memtables:
                sources.append(memtable.range_query(start_key, end_key))
            
            # Add SSTables
            for level in self.levels:
                for sstable in level:
                    sources.append(sstable.range_query(start_key, end_key))
        
        # Merge with deduplication (newer values override older ones)
        yield from self._merge_sources(sources)
    
    def _merge_sources(self, sources: List[Iterator[Tuple[str, str]]]) -> Iterator[Tuple[str, str]]:
        """Merge multiple sorted sources, keeping the newest value for each key."""
        # Use a min-heap to merge sorted sequences
        heap = []
        iterators = []
        
        # Initialize heap with first element from each source
        for i, source in enumerate(sources):
            try:
                key, value = next(source)
                heapq.heappush(heap, (key, value, i))
                iterators.append(source)
            except StopIteration:
                iterators.append(None)
        
        last_key = None
        
        while heap:
            key, value, source_idx = heapq.heappop(heap)
            
            # Only yield if this is a new key (deduplication)
            if key != last_key:
                if value is not None:  # Skip tombstones
                    yield (key, value)
                last_key = key
            
            # Get next element from the same source
            if iterators[source_idx] is not None:
                try:
                    next_key, next_value = next(iterators[source_idx])
                    heapq.heappush(heap, (next_key, next_value, source_idx))
                except StopIteration:
                    iterators[source_idx] = None
    
    def _trigger_flush(self) -> None:
        """Trigger background flush of immutable memtables."""
        if self.flush_thread is None or not self.flush_thread.is_alive():
            self.flush_thread = threading.Thread(target=self._flush_memtables)
            self.flush_thread.daemon = True
            self.flush_thread.start()
    
    def _flush_memtables(self) -> None:
        """Flush immutable memtables to L0 SSTables."""
        while True:
            with self.lock:
                if not self.immutable_memtables:
                    break
                
                memtable = self.immutable_memtables.pop(0)
            
            # Create SSTable from memtable
            timestamp = int(time.time() * 1000000)  # microseconds
            filename = os.path.join(self.base_dir, f"L0_{timestamp}.sst")
            sstable = SSTable.create_from_memtable(memtable, filename, level=0)
            
            with self.lock:
                self.levels[0].append(sstable)
            
            # Trigger compaction if needed
            self._maybe_compact()
    
    def _maybe_compact(self) -> None:
        """Check if compaction is needed and trigger it."""
        # Simple compaction strategy: compact when level has too many SSTables
        level_limits = [4, 10, 40, 160]  # Maximum SSTables per level
        
        for level in range(len(self.levels) - 1):
            if len(self.levels[level]) > level_limits[level]:
                if self.compaction_thread is None or not self.compaction_thread.is_alive():
                    self.compaction_thread = threading.Thread(
                        target=self._compact_level, args=(level,)
                    )
                    self.compaction_thread.daemon = True
                    self.compaction_thread.start()
                break
    
    def _compact_level(self, level: int) -> None:
        """Compact SSTables from one level to the next."""
        if level >= len(self.levels) - 1:
            return
        
        with self.lock:
            # Take all SSTables from current level
            source_sstables = self.levels[level].copy()
            self.levels[level].clear()
            
            # Take overlapping SSTables from next level
            next_level_sstables = self.levels[level + 1].copy()
            self.levels[level + 1].clear()
        
        # Merge all SSTables
        sources = []
        for sstable in source_sstables + next_level_sstables:
            sources.append(sstable.range_query("", "~"))  # Full range
        
        # Create new SSTable at next level
        timestamp = int(time.time() * 1000000)
        filename = os.path.join(self.base_dir, f"L{level+1}_{timestamp}.sst")
        
        # Write merged data to new SSTable
        new_sstable = SSTable(filename, level + 1)
        with open(filename, 'w') as f:
            for i, (key, value) in enumerate(self._merge_sources(sources)):
                if i == 0:
                    new_sstable.min_key = key
                
                if i % 10 == 0:
                    new_sstable.index[key] = f.tell()
                
                entry = json.dumps({"key": key, "value": value}) + "\n"
                f.write(entry)
                new_sstable.size += 1
                new_sstable.max_key = key
        
        # Add new SSTable to next level
        with self.lock:
            self.levels[level + 1].append(new_sstable)
        
        # Clean up old SSTable files
        for sstable in source_sstables + next_level_sstables:
            try:
                os.remove(sstable.filename)
            except OSError:
                pass  # File might not exist
    
    def stats(self) -> Dict[str, Union[int, List[int]]]:
        """Get statistics about the LSM tree."""
        with self.lock:
            return {
                "memtable_size": self.memtable.size,
                "immutable_memtables": len(self.immutable_memtables),
                "level_sizes": [len(level) for level in self.levels],
                "total_sstables": sum(len(level) for level in self.levels)
            }
    
    def close(self) -> None:
        """Close the LSM tree and clean up resources."""
        # Wait for background threads to finish
        if self.flush_thread and self.flush_thread.is_alive():
            self.flush_thread.join(timeout=5.0)
        
        if self.compaction_thread and self.compaction_thread.is_alive():
            self.compaction_thread.join(timeout=5.0)


# Example usage and demonstration
def demo_lsm_tree():
    """Demonstrate LSM tree operations."""
    print("LSM Tree Demo")
    print("=" * 50)
    
    # Create LSM tree
    lsm = LSMTree(memtable_size=10)  # Small size for demonstration
    
    # Insert some data
    print("Inserting data...")
    for i in range(25):
        key = f"key_{i:03d}"
        value = f"value_{i}"
        lsm.put(key, value)
        print(f"Inserted: {key} -> {value}")
    
    print(f"\nLSM Tree Stats: {lsm.stats()}")
    
    # Test retrieval
    print("\nTesting retrieval...")
    for key in ["key_005", "key_015", "key_024", "nonexistent"]:
        value = lsm.get(key)
        print(f"Get {key}: {value}")
    
    # Test range query
    print("\nTesting range query (key_010 to key_015)...")
    for key, value in lsm.range_query("key_010", "key_016"):
        print(f"  {key} -> {value}")
    
    # Test deletion
    print("\nDeleting key_012...")
    lsm.delete("key_012")
    
    print("Range query after deletion (key_010 to key_015)...")
    for key, value in lsm.range_query("key_010", "key_016"):
        print(f"  {key} -> {value}")
    
    print(f"\nFinal LSM Tree Stats: {lsm.stats()}")
    
    # Clean up
    lsm.close()
    print("\nDemo completed!")


if __name__ == "__main__":
    demo_lsm_tree()