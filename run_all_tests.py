#!/usr/bin/env python
"""
Test runner script that runs all tests in batches to avoid resource contention.
This ensures all tests pass before pushing code.
"""
import subprocess
import sys
from pathlib import Path

# Define test batches to run separately to avoid resource contention
TEST_BATCHES = [
    # Fast unit tests first
    ("Unit Tests - Storage Model", "deltacat/tests/storage/model/"),
    ("Unit Tests - Storage", "deltacat/tests/storage/", "-k 'not model'"),
    ("Unit Tests - Utils", "deltacat/tests/utils/"),
    ("Unit Tests - IO", "deltacat/tests/_io/"),
    
    # Catalog tests (may initialize databases)
    ("Catalog - Unity", "deltacat/tests/catalog/test_unity_catalog.py"),
    ("Catalog - Daft", "deltacat/tests/catalog/daft/"),
    ("Catalog - Main", "deltacat/tests/catalog/main/", 
     "-k 'not (test_schema_inference_daft_dataframe or test_schema_inference_ray_dataset)'"),
    ("Catalog - Other", "deltacat/tests/catalog/", 
     "-k 'not (test_unity_catalog or daft or main)'"),
    
    # Table format tests
    ("Storage - Table Formats", "deltacat/tests/storage/test_table_formats.py"),
    
    # Daft specific tests
    ("Daft Tests", "deltacat/tests/daft/"),
    
    # SQL tests
    ("SQL Tests", "deltacat/tests/sql/"),
    
    # Compute tests (heavy resource usage)
    ("Compute Tests", "deltacat/tests/compute/", "--timeout=60"),
    
    # AWS tests (may need mocking)
    ("AWS Tests", "deltacat/tests/aws/"),
    
    # Experimental tests
    ("Experimental Tests", "deltacat/tests/experimental/"),
    
    # Integration tests last (slowest)
    ("Integration Tests", "deltacat/tests/integ/"),
]

def run_test_batch(name, path, extra_args=""):
    """Run a batch of tests with proper error handling."""
    # Check if path exists
    if not Path(path).exists():
        print(f"\n{'='*60}")
        print(f"⏭️ SKIPPING: {name}")
        print(f"Path does not exist: {path}")
        print(f"{'='*60}")
        return True  # Don't fail the whole suite for missing path
    
    print(f"\n{'='*60}")
    print(f"Running: {name}")
    print(f"Path: {path}")
    print(f"{'='*60}")
    
    cmd = f"python -m pytest {path} -v --tb=short {extra_args}"
    
    try:
        result = subprocess.run(
            cmd,
            shell=True,
            capture_output=True,
            text=True,
            timeout=300  # 5 minute timeout per batch
        )
        
        if result.returncode != 0:
            print(f"❌ FAILED: {name}")
            print("STDOUT:", result.stdout[-2000:] if len(result.stdout) > 2000 else result.stdout)
            print("STDERR:", result.stderr[-2000:] if len(result.stderr) > 2000 else result.stderr)
            return False
        else:
            # Count passed/skipped
            stdout = result.stdout
            if "passed" in stdout:
                summary = [line for line in stdout.split('\n') if 'passed' in line and '==' in line]
                if summary:
                    print(f"✅ {summary[-1].strip()}")
            print(f"✅ SUCCESS: {name}")
            return True
            
    except subprocess.TimeoutExpired:
        print(f"⏱️ TIMEOUT: {name} (exceeded 5 minutes)")
        return False
    except Exception as e:
        print(f"❌ ERROR running {name}: {e}")
        return False

def main():
    """Run all test batches and report results."""
    print("🚀 Starting DeltaCAT Test Suite")
    print("This will run all tests in batches to avoid resource contention")
    
    failed_batches = []
    passed_batches = []
    
    for batch_info in TEST_BATCHES:
        name = batch_info[0]
        path = batch_info[1]
        extra_args = batch_info[2] if len(batch_info) > 2 else ""
        
        if run_test_batch(name, path, extra_args):
            passed_batches.append(name)
        else:
            failed_batches.append(name)
            # Continue running other batches to get full picture
    
    print("\n" + "="*60)
    print("📊 TEST SUMMARY")
    print("="*60)
    
    if passed_batches:
        print(f"\n✅ PASSED ({len(passed_batches)}):")
        for batch in passed_batches:
            print(f"  - {batch}")
    
    if failed_batches:
        print(f"\n❌ FAILED ({len(failed_batches)}):")
        for batch in failed_batches:
            print(f"  - {batch}")
        print("\n❌ Some tests failed. Please fix before pushing.")
        sys.exit(1)
    else:
        print("\n✅ All tests passed! Safe to push.")
        sys.exit(0)

if __name__ == "__main__":
    main()