#!/usr/bin/env python3
"""
Data Platform Component Validation Script
Quick verification that all components are properly installed and working
"""

import os
import sys


def test_file_structure():
    """Test that all key files exist"""
    print("📁 Testing file structure...")

    key_files = [
        "docker-compose.yml",
        "Makefile",
        "README.md",
        "ingestion/java-producer/pom.xml",
        "spark/python/streaming_job.py",
        "spark/scala/build.sbt",
        "infra/azure/terraform/main.tf",
        ".github/workflows/ci.yml",
        ".github/workflows/azure-infra.yml",
        "cs/lsm_tree.py",
        "ml/serving/lambda_handler.py",
        "nifi/README.md",
        "azure/README.md",
    ]

    all_exist = True
    for file in key_files:
        exists = os.path.exists(file)
        status = "✓" if exists else "✗"
        print(f"   {status} {file}")
        if not exists:
            all_exist = False

    return all_exist


def test_python_imports():
    """Test that Python components import correctly"""
    print("\n🐍 Testing Python imports...")

    try:
        # Add current directory to path for imports
        sys.path.insert(0, ".")

        # Test imports without storing in variables to avoid unused import warnings
        import cs.lsm_tree  # noqa: F401

        print("   ✓ LSM Tree imports successfully")

        import ml.serving.lambda_handler  # noqa: F401

        print("   ✓ ML serving handler imports successfully")

        return True

    except Exception as e:
        print(f"   ✗ Import error: {e}")
        return False


def test_lsm_tree():
    """Test LSM Tree functionality"""
    print("\n🔍 Testing LSM Tree functionality...")

    try:
        import tempfile

        from cs.lsm_tree import LSMTree

        # Quick functional test
        lsm = LSMTree(tempfile.mkdtemp(), memtable_size=5)

        # Insert data
        lsm.put("test_key", "test_value")

        # Retrieve data
        value = lsm.get("test_key")
        assert value == "test_value", f"Expected 'test_value', got '{value}'"

        # Test non-existent key
        missing = lsm.get("missing_key")
        assert missing is None, f"Expected None, got '{missing}'"

        lsm.close()
        print("   ✓ LSM Tree basic operations working")

        return True

    except Exception as e:
        print(f"   ✗ LSM Tree test failed: {e}")
        return False


def test_ml_serving():
    """Test ML serving handler"""
    print("\n🤖 Testing ML serving handler...")

    try:
        from ml.serving.lambda_handler import ModelPredictor

        predictor = ModelPredictor()

        test_order = {
            "product": "laptop",
            "region": "us-east",
            "currency": "USD",
            "quantity": 1,
        }

        result = predictor.predict(test_order)

        # Validate response structure
        required_keys = ["predicted_price", "confidence", "model_type", "timestamp"]
        for key in required_keys:
            assert key in result, f"Missing key '{key}' in prediction result"

        print("   ✓ ML serving handler working")
        predicted_price = result["predicted_price"]
        confidence = result["confidence"]
        print(f"     Sample prediction: ${predicted_price} (confidence: {confidence})")

        return True

    except Exception as e:
        print(f"   ✗ ML serving test failed: {e}")
        return False


def test_docker_compose():
    """Test Docker Compose configuration syntax"""
    print("\n🐳 Testing Docker Compose configuration...")

    try:
        # Simple syntax check by trying to parse the file
        import yaml

        with open("docker-compose.yml", "r") as f:
            compose_config = yaml.safe_load(f)

        # Basic validation
        assert "services" in compose_config, "Missing 'services' section"

        required_services = ["kafka", "zookeeper", "postgres", "minio", "spark-master"]
        for service in required_services:
            assert service in compose_config["services"], f"Missing service: {service}"

        print("   ✓ Docker Compose configuration valid")
        return True

    except Exception as e:
        print(f"   ✗ Docker Compose validation failed: {e}")
        return False


def main():
    """Run all validation tests"""
    print("🚀 Data Platform Component Validation")
    print("=" * 50)

    tests = [
        ("File Structure", test_file_structure),
        ("Python Imports", test_python_imports),
        ("LSM Tree", test_lsm_tree),
        ("ML Serving", test_ml_serving),
        ("Docker Compose", test_docker_compose),
    ]

    results = []

    for test_name, test_func in tests:
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"\n❌ {test_name} test crashed: {e}")
            results.append((test_name, False))

    # Summary
    print("\n" + "=" * 50)
    print("📊 VALIDATION SUMMARY")
    print("=" * 50)

    passed = 0
    total = len(results)

    for test_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status:<8} {test_name}")
        if result:
            passed += 1

    print(f"\nResults: {passed}/{total} tests passed")

    if passed == total:
        print("\n🎉 All components validated successfully!")
        print("\n📋 Ready for deployment!")
        print("   Next steps:")
        print("   1. Run: make up")
        print("   2. Run: make kafka-producer")
        print("   3. Configure NiFi (see nifi/README.md)")
        print("   4. Run: make stream && make batch")
        print("   5. Deploy to Azure via GitHub Actions")
        return True
    else:
        print(f"\n⚠️  {total - passed} test(s) failed. Please check the errors above.")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
