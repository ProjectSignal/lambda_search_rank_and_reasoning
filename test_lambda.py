#!/usr/bin/env python3
"""
Test script for the Reasoning Lambda function
"""

import json
import asyncio
from lambda_handler import lambda_handler, process_reasoning_request
from logging_config import setup_logger

logger = setup_logger(__name__)

def test_lambda_handler():
    """Test the Lambda handler with sample event"""
    with open('test_event.json', 'r') as f:
        test_event = json.load(f)

    print("Testing Lambda handler with event:")
    print(json.dumps(test_event, indent=2))
    print("\n" + "="*50 + "\n")

    result = lambda_handler(test_event, None)

    print("Lambda result:")
    print(json.dumps(result, indent=2, default=str))

    return result

async def test_async_processing():
    """Test the async processing function directly"""
    with open('test_event.json', 'r') as f:
        test_event = json.load(f)

    print("Testing async processing directly:")
    print(json.dumps(test_event, indent=2))
    print("\n" + "="*50 + "\n")

    result = await process_reasoning_request(test_event)

    print("Async processing result:")
    print(json.dumps(result, indent=2, default=str))

    return result

def main():
    """Main test function"""
    print("Starting Reasoning Lambda tests...\n")

    # Test 1: Lambda handler
    print("=== Test 1: Lambda Handler ===")
    try:
        result1 = test_lambda_handler()
        if result1['statusCode'] == 200:
            print("✅ Lambda handler test PASSED")
        else:
            print("❌ Lambda handler test FAILED")
    except Exception as e:
        print(f"❌ Lambda handler test ERROR: {str(e)}")

    print("\n")

    # Test 2: Async processing
    print("=== Test 2: Async Processing ===")
    try:
        result2 = asyncio.run(test_async_processing())
        if result2['statusCode'] == 200:
            print("✅ Async processing test PASSED")
        else:
            print("❌ Async processing test FAILED")
    except Exception as e:
        print(f"❌ Async processing test ERROR: {str(e)}")

if __name__ == "__main__":
    main()