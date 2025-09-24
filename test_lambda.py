#!/usr/bin/env python3
"""
Test script for the Reasoning Lambda function
"""
import argparse
import json
import os
from dotenv import load_dotenv
from logging_config import setup_logger
from lambda_handler import lambda_handler
from db import searchOutputCollection

load_dotenv()
logger = setup_logger(__name__)

def main():
    parser = argparse.ArgumentParser(description="Run RankAndReasoning Lambda locally")
    parser.add_argument("--search-id", required=True, help="Existing searchId with SEARCH_COMPLETE status")
    parser.add_argument("--ranking", dest="ranking", action="store_true", help="Enable ranking")
    parser.add_argument("--no-ranking", dest="ranking", action="store_false", help="Disable ranking")
    parser.add_argument("--reasoning", dest="reasoning", action="store_true", help="Enable reasoning insights")
    parser.add_argument("--no-reasoning", dest="reasoning", action="store_false", help="Disable reasoning insights")
    parser.add_argument("--top-k", type=int, default=10, help="Top K nodes to analyze for reasoning")
    parser.set_defaults(ranking=True, reasoning=False)
    args = parser.parse_args()

    search_id = args.search_id

    print("🚀 Starting RankAndReasoning Lambda Test")
    print(f"📌 Using searchId: {search_id}")
    print(f"⚙️  Ranking: {args.ranking}, Reasoning: {args.reasoning}, Top-K for insights: {args.top_k}")

    event = {
        "searchId": search_id,
        "ranking_enabled": args.ranking,
        "reasoning_enabled": args.reasoning,
        "top_k_for_insights": args.top_k
    }

    result = lambda_handler(event, None)
    status = result.get("statusCode")
    body_raw = result.get("body")
    body = None
    try:
        body = json.loads(body_raw) if isinstance(body_raw, str) else body_raw
    except Exception:
        body = {"raw": body_raw}

    print("\n=== Lambda Response ===")
    print(f"Status Code: {status}\n")
    print("Response Body:")
    print(json.dumps(body, indent=2, default=str))

    # Validate DB updates
    doc = searchOutputCollection.find_one({"_id": search_id})
    if not doc:
        print(f"[ERROR] Search document not found: {search_id}")
        raise SystemExit(1)

    status_str = doc.get("status")
    print("\n4. Validating search document update...")
    print(f"   Status: {status_str}")

    ranked = (doc.get("results") or {}).get("ranked") or []
    if args.ranking:
        if ranked and isinstance(ranked, list):
            print(f"   ✅ Ranking completed with {len(ranked)} results")
        else:
            print("   ❌ Ranked results missing or empty")
            raise SystemExit(1)
    else:
        print("   ⚠️ Ranking disabled - skipped validation")

    if args.reasoning:
        reasoning = doc.get("reasoning")
        if reasoning:
            print("   ✅ Reasoning results present")
        else:
            print("   ❌ Reasoning results missing")
            raise SystemExit(1)

    print("\n[SUCCESS] RankAndReasoning test completed successfully!")

if __name__ == "__main__":
    main()