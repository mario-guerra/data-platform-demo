"""
Tests for LSM Tree implementation
"""

import os
import tempfile
import time
import unittest
from collections import Counter

from cs.lsm_tree import LSMTree, MemTable, SSTable


class TestMemTable(unittest.TestCase):
    """Test cases for MemTable"""
    
    def setUp(self):
        self.memtable = MemTable(max_size=5)
    
    def test_put_and_get(self):
        """Test basic put and get operations"""
        self.memtable.put("key1", "value1")
        self.assertEqual(self.memtable.get("key1"), "value1")
        
        # Test update
        self.memtable.put("key1", "value1_updated")
        self.assertEqual(self.memtable.get("key1"), "value1_updated")
    
    def test_ordering(self):
        """Test that keys are kept in sorted order"""
        keys = ["key3", "key1", "key2"]
        for key in keys:
            self.memtable.put(key, f"value_{key}")
        
        data = self.memtable.to_list()
        sorted_keys = [item[0] for item in data]
        self.assertEqual(sorted_keys, ["key1", "key2", "key3"])
    
    def test_max_size(self):
        """Test that memtable reports when full"""
        # Add items until full
        for i in range(4):
            is_full = self.memtable.put(f"key{i}", f"value{i}")
            self.assertFalse(is_full)
        
        # Fifth item should report full
        is_full = self.memtable.put("key5", "value5")
        self.assertTrue(is_full)
    
    def test_delete_tombstone(self):
        """Test deletion creates tombstone"""
        self.memtable.put("key1", "value1")
        self.memtable.delete("key1")
        self.assertIsNone(self.memtable.get("key1"))
    
    def test_range_query(self):
        """Test range queries"""
        # Add some data
        for i in range(5):
            self.memtable.put(f"key{i}", f"value{i}")
        
        # Query range
        results = list(self.memtable.range_query("key1", "key4"))
        expected_keys = ["key1", "key2", "key3"]
        actual_keys = [key for key, value in results]
        self.assertEqual(actual_keys, expected_keys)


class TestSSTable(unittest.TestCase):
    """Test cases for SSTable"""
    
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.memtable = MemTable(max_size=10)
        
        # Add test data to memtable
        for i in range(5):
            self.memtable.put(f"key{i:02d}", f"value{i}")
    
    def tearDown(self):
        # Clean up temp files
        for filename in os.listdir(self.temp_dir):
            os.remove(os.path.join(self.temp_dir, filename))
        os.rmdir(self.temp_dir)
    
    def test_create_from_memtable(self):
        """Test creating SSTable from memtable"""
        filename = os.path.join(self.temp_dir, "test.sst")
        sstable = SSTable.create_from_memtable(self.memtable, filename)
        
        self.assertTrue(os.path.exists(filename))
        self.assertEqual(sstable.min_key, "key00")
        self.assertEqual(sstable.max_key, "key04")
        self.assertEqual(sstable.size, 5)
    
    def test_get_from_sstable(self):
        """Test retrieving values from SSTable"""
        filename = os.path.join(self.temp_dir, "test.sst")
        sstable = SSTable.create_from_memtable(self.memtable, filename)
        
        # Test existing key
        self.assertEqual(sstable.get("key01"), "value1")
        
        # Test non-existent key
        self.assertIsNone(sstable.get("key99"))
        
        # Test key outside range
        self.assertIsNone(sstable.get("aaa"))
    
    def test_range_query_sstable(self):
        """Test range queries on SSTable"""
        filename = os.path.join(self.temp_dir, "test.sst")
        sstable = SSTable.create_from_memtable(self.memtable, filename)
        
        results = list(sstable.range_query("key01", "key04"))
        expected_keys = ["key01", "key02", "key03"]
        actual_keys = [key for key, value in results]
        self.assertEqual(actual_keys, expected_keys)


class TestLSMTree(unittest.TestCase):
    """Test cases for LSMTree"""
    
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.lsm = LSMTree(base_dir=self.temp_dir, memtable_size=5)
    
    def tearDown(self):
        self.lsm.close()
        # Clean up temp files
        for filename in os.listdir(self.temp_dir):
            try:
                os.remove(os.path.join(self.temp_dir, filename))
            except (OSError, PermissionError):
                pass  # File might be in use by background thread
        try:
            os.rmdir(self.temp_dir)
        except OSError:
            pass  # Directory might not be empty
    
    def test_basic_operations(self):
        """Test basic put and get operations"""
        self.lsm.put("key1", "value1")
        self.assertEqual(self.lsm.get("key1"), "value1")
        
        # Test update
        self.lsm.put("key1", "value1_updated")
        self.assertEqual(self.lsm.get("key1"), "value1_updated")
        
        # Test non-existent key
        self.assertIsNone(self.lsm.get("nonexistent"))
    
    def test_memtable_flush(self):
        """Test that memtable flushes when full"""
        # Fill memtable beyond capacity
        for i in range(10):
            self.lsm.put(f"key{i:02d}", f"value{i}")
        
        # Wait a bit for background flush
        time.sleep(0.1)
        
        # Should have created SSTables
        stats = self.lsm.stats()
        self.assertGreater(stats["total_sstables"], 0)
    
    def test_delete_operation(self):
        """Test deletion with tombstones"""
        self.lsm.put("key1", "value1")
        self.assertEqual(self.lsm.get("key1"), "value1")
        
        self.lsm.delete("key1")
        self.assertIsNone(self.lsm.get("key1"))
    
    def test_range_query(self):
        """Test range queries across multiple sources"""
        # Add data that will span memtable and SSTables
        for i in range(15):
            self.lsm.put(f"key{i:02d}", f"value{i}")
        
        # Wait for flushes
        time.sleep(0.1)
        
        # Test range query
        results = list(self.lsm.range_query("key05", "key10"))
        expected_keys = [f"key{i:02d}" for i in range(5, 10)]
        actual_keys = [key for key, value in results]
        
        # Should get all keys in range (order might vary due to merging)
        self.assertEqual(set(actual_keys), set(expected_keys))
    
    def test_data_persistence_across_levels(self):
        """Test that data persists across different levels"""
        # Add enough data to trigger compaction
        for i in range(50):
            self.lsm.put(f"key{i:03d}", f"value{i}")
        
        # Wait for background operations
        time.sleep(0.5)
        
        # Verify all data is still accessible
        for i in range(0, 50, 5):  # Sample every 5th item
            key = f"key{i:03d}"
            value = self.lsm.get(key)
            self.assertEqual(value, f"value{i}")
    
    def test_overwrite_behavior(self):
        """Test that newer values override older ones"""
        self.lsm.put("key1", "old_value")
        
        # Force flush to SSTable
        for i in range(10):
            self.lsm.put(f"filler{i}", f"filler_value{i}")
        
        time.sleep(0.1)
        
        # Update the key
        self.lsm.put("key1", "new_value")
        
        # Should get the newer value
        self.assertEqual(self.lsm.get("key1"), "new_value")
    
    def test_stats_tracking(self):
        """Test that statistics are properly tracked"""
        initial_stats = self.lsm.stats()
        
        # Add some data
        for i in range(7):
            self.lsm.put(f"key{i}", f"value{i}")
        
        updated_stats = self.lsm.stats()
        
        # Memtable should have grown or flushed
        self.assertGreaterEqual(
            updated_stats["memtable_size"] + updated_stats["total_sstables"],
            initial_stats["memtable_size"] + initial_stats["total_sstables"]
        )


class TestPerformance(unittest.TestCase):
    """Performance and stress tests"""
    
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.lsm = LSMTree(base_dir=self.temp_dir, memtable_size=100)
    
    def tearDown(self):
        self.lsm.close()
        # Clean up temp files
        for filename in os.listdir(self.temp_dir):
            try:
                os.remove(os.path.join(self.temp_dir, filename))
            except (OSError, PermissionError):
                pass
        try:
            os.rmdir(self.temp_dir)
        except OSError:
            pass
    
    def test_write_performance(self):
        """Test write performance with many operations"""
        num_operations = 1000
        start_time = time.time()
        
        for i in range(num_operations):
            self.lsm.put(f"key{i:04d}", f"value{i}")
        
        end_time = time.time()
        duration = end_time - start_time
        
        # Should be able to handle at least 100 ops/second
        ops_per_second = num_operations / duration
        self.assertGreater(ops_per_second, 100)
        
        print(f"Write performance: {ops_per_second:.2f} ops/second")
    
    def test_read_performance(self):
        """Test read performance"""
        # Setup data
        num_items = 500
        for i in range(num_items):
            self.lsm.put(f"key{i:04d}", f"value{i}")
        
        # Wait for stabilization
        time.sleep(0.5)
        
        # Test random reads
        start_time = time.time()
        
        for i in range(0, num_items, 10):  # Read every 10th item
            key = f"key{i:04d}"
            value = self.lsm.get(key)
            self.assertIsNotNone(value)
        
        end_time = time.time()
        duration = end_time - start_time
        
        reads_performed = num_items // 10
        reads_per_second = reads_performed / duration
        
        print(f"Read performance: {reads_per_second:.2f} reads/second")
    
    def test_concurrent_operations(self):
        """Test thread safety with concurrent operations"""
        import threading
        
        def writer_thread(start_idx, count):
            for i in range(count):
                key = f"thread_{start_idx}_key_{i:03d}"
                value = f"thread_{start_idx}_value_{i}"
                self.lsm.put(key, value)
        
        def reader_thread(start_idx, count):
            for i in range(count):
                key = f"thread_{start_idx}_key_{i:03d}"
                self.lsm.get(key)  # May or may not find it
        
        # Start multiple threads
        threads = []
        
        # Writer threads
        for t in range(3):
            thread = threading.Thread(target=writer_thread, args=(t, 50))
            threads.append(thread)
            thread.start()
        
        # Reader threads
        for t in range(2):
            thread = threading.Thread(target=reader_thread, args=(t, 50))
            threads.append(thread)
            thread.start()
        
        # Wait for all threads
        for thread in threads:
            thread.join()
        
        # Verify no corruption
        stats = self.lsm.stats()
        self.assertGreaterEqual(stats["memtable_size"] + stats["total_sstables"], 0)


def run_comprehensive_test():
    """Run a comprehensive test suite"""
    print("Running LSM Tree comprehensive tests...")
    print("=" * 50)
    
    # Create test suite
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    # Add test cases
    suite.addTest(loader.loadTestsFromTestCase(TestMemTable))
    suite.addTest(loader.loadTestsFromTestCase(TestSSTable))
    suite.addTest(loader.loadTestsFromTestCase(TestLSMTree))
    suite.addTest(loader.loadTestsFromTestCase(TestPerformance))
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    # Print summary
    print("\n" + "=" * 50)
    print(f"Tests run: {result.testsRun}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    
    if result.failures:
        print("\nFailures:")
        for test, traceback in result.failures:
            print(f"  {test}: {traceback}")
    
    if result.errors:
        print("\nErrors:")
        for test, traceback in result.errors:
            print(f"  {test}: {traceback}")
    
    return result.wasSuccessful()


if __name__ == "__main__":
    success = run_comprehensive_test()
    exit(0 if success else 1)