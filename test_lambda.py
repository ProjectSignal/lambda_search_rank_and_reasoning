#!/usr/bin/env python3
"""
Test script for the Reasoning Lambda function
"""
import argparse
import json
from dotenv import load_dotenv
from logging_config import setup_logger
from lambda_handler import lambda_handler
from api_client import get_search_document

DEFAULT_USER_ID = "6797bf304791caa516f6da9e"

load_dotenv()
logger = setup_logger(__name__)

def main():
    parser = argparse.ArgumentParser(description="Run RankAndReasoning Lambda locally")
    parser.add_argument("--search-id", required=True, help="Existing searchId with SEARCH_COMPLETE status")
    parser.add_argument("--ranking", dest="ranking", action="store_true", help="Enable ranking")
    parser.add_argument("--no-ranking", dest="ranking", action="store_false", help="Disable ranking")
    parser.add_argument("--reasoning", dest="reasoning", action="store_true", help="Enable reasoning insights")
    parser.add_argument("--no-reasoning", dest="reasoning", action="store_false", help="Disable reasoning insights")
    parser.add_argument(
        "--candidate-ids",
        type=str,
        default="",
        help="Comma separated list of candidate nodeIds to process (defaults to all)"
    )
    parser.add_argument(
        "--max-concurrent-calls",
        type=int,
        default=5,
        help="Override max concurrent calls to downstream models"
    )
    parser.add_argument(
        "--user-id",
        type=str,
        default=DEFAULT_USER_ID,
        help="UserId associated with the search document"
    )
    parser.set_defaults(ranking=True, reasoning=False)
    args = parser.parse_args()

    search_id = args.search_id
    candidate_ids = [cid.strip() for cid in args.candidate_ids.split(",") if cid.strip()]
    user_id = args.user_id
    candidate_filter = set(candidate_ids) if candidate_ids else None

    print("🚀 Starting RankAndReasoning Lambda Test")
    print(f"📌 Using searchId: {search_id}")
    print(f"⚙️  Ranking: {args.ranking}, Reasoning: {args.reasoning}")
    if candidate_ids:
        print(f"🎯 Candidate subset: {candidate_ids}")

    event = {
        "searchId": search_id,
        "userId": user_id,
        "ranking_enabled": args.ranking,
        "reasoning_enabled": args.reasoning,
        "max_concurrent_calls": args.max_concurrent_calls
    }
    if candidate_ids:
        event["candidateIds"] = candidate_ids

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
    doc = get_search_document(search_id, user_id=user_id)
    if not doc:
        print(f"[ERROR] Search document not found: {search_id}")
        raise SystemExit(1)

    status_str = doc.get("status")
    print("\n4. Validating search document update...")
    print(f"   Status: {status_str}")

    # Validate candidate array produced by ranking step
    candidates = (doc.get("results") or {}).get("candidates") or []
    if args.ranking:
        # Count candidates that have ranking scores (indicating ranking was performed)
        scored_candidates = [
            c for c in candidates
            if c.get("score") is not None
            and (candidate_filter is None or c.get("nodeId") in candidate_filter)
        ]
        if scored_candidates:
            print(
                f"   ✅ Ranking completed with {len(scored_candidates)} scored results "
                f"out of {len(candidate_filter) if candidate_filter else len(candidates)} candidates"
            )
        else:
            print("   ❌ No scores found (ranking may have failed)")
            raise SystemExit(1)
    else:
        print("   ⚠️ Ranking disabled - skipped validation")

    if args.reasoning:
        reasoning = doc.get("reasoning")
        if reasoning:
            filtered = [
                c for c in candidates
                if c.get("reasoning")
                and (candidate_filter is None or c.get("nodeId") in candidate_filter)
            ]
            if filtered:
                print(f"   ✅ Reasoning results present for {len(filtered)} candidates")
            else:
                print("   ❌ Reasoning results missing")
                raise SystemExit(1)
        else:
            print("   ❌ Reasoning results missing")
            raise SystemExit(1)

    print("\n[SUCCESS] RankAndReasoning test completed successfully!")

if __name__ == "__main__":
    main()
