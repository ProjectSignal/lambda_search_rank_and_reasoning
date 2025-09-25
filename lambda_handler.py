import json
import asyncio
import os
import traceback
from datetime import datetime, timezone
from typing import Dict, List
from dotenv import load_dotenv
from reasoning_logic import SearchReasoning
from logging_config import setup_logger
from db import searchOutputCollection
from ranking import build_candidate_materials, process_people_direct, convert_objectids_to_strings

# Load environment variables (for local testing)
load_dotenv()

logger = setup_logger(__name__)

class SearchStatus:
    """Search execution status tracking"""
    NEW = "NEW"
    HYDE_COMPLETE = "HYDE_COMPLETE"
    SEARCH_COMPLETE = "SEARCH_COMPLETE"
    RANK_AND_REASONING_COMPLETE = "RANK_AND_REASONING_COMPLETE"
    ERROR = "ERROR"

def get_utc_now():
    """Returns current UTC datetime in ISO format"""
    return datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()

async def process_reasoning_request(event_data: dict) -> dict:

    def adapt_hyde_response_to_rank_details(hyde_resp: dict) -> dict:
        """
        Adapt HyDE hydeAnalysis.response to the flattened details object
        expected by convert_hyde_details_to_xml in ranking.py.

        If the input already appears flattened (has 'locations' or 'skills' as arrays of strings),
        return it as-is.
        """
        if not isinstance(hyde_resp, dict):
            return {}

        # If already flattened (heuristic)
        if any(k in hyde_resp for k in ["locations", "skills", "organizations", "sectors", "db_queries"]):
            return hyde_resp

        details = {}

        # Locations
        loc = hyde_resp.get("locationDetails", {})
        if isinstance(loc, dict):
            locs = []
            for it in loc.get("locations", []) or []:
                if isinstance(it, dict):
                    name = it.get("name")
                else:
                    name = it
                if name:
                    locs.append(name)
            if locs:
                details["locations"] = locs
            op = loc.get("operator")
            if op:
                details["location_operator"] = op

        # Organizations
        org = hyde_resp.get("organisationDetails", {}) or hyde_resp.get("organizationDetails", {})
        if isinstance(org, dict):
            orgs = []
            for it in org.get("organizations", []) or []:
                if isinstance(it, dict):
                    name = it.get("name")
                else:
                    name = it
                if name:
                    orgs.append(name)
            if orgs:
                details["organizations"] = orgs
            op = org.get("operator")
            if op:
                details["organization_operator"] = op
            temporal = org.get("temporal")
            if temporal:
                details["organization_temporal"] = temporal

        # Sectors
        sec = hyde_resp.get("sectorDetails", {})
        if isinstance(sec, dict):
            secs = []
            for it in sec.get("sectors", []) or []:
                if isinstance(it, dict):
                    name = it.get("name")
                else:
                    name = it
                if name:
                    secs.append(name)
            if secs:
                details["sectors"] = secs
            op = sec.get("operator")
            if op:
                details["sector_operator"] = op
            temporal = sec.get("temporal")
            if temporal:
                details["sector_temporal"] = temporal

        # Skills
        skl = hyde_resp.get("skillDetails", {})
        if isinstance(skl, dict):
            skills = []
            for it in skl.get("skills", []) or []:
                if isinstance(it, dict):
                    name = it.get("name")
                else:
                    name = it
                if name:
                    skills.append(name)
            if skills:
                details["skills"] = skills
            op = skl.get("operator")
            if op:
                details["skill_operator"] = op

        # Database Queries
        dbq = hyde_resp.get("dbQueryDetails", {})
        if isinstance(dbq, dict):
            queries = []
            for q in dbq.get("queries", []) or []:
                if isinstance(q, dict):
                    fld = q.get("field", "")
                    desc = q.get("description", "") or ""
                    if fld:
                        queries.append({"field": fld, "description": desc})
            if queries:
                details["db_queries"] = queries
            op = dbq.get("operator")
            if op:
                details["db_query_operator"] = op

        return details
    """
    Process reasoning request for search results from searchOutput collection

    Args:
        event_data: Dictionary containing:
            - searchId: Search ID to load results from searchOutput collection
            - ranking_enabled: Whether to perform ranking (default: True)
            - reasoning_enabled: Whether to perform reasoning analysis (default: False)
            - top_k_for_insights: Number of top results for detailed insights (default: 10)
            OR legacy format:
            - nodes: List of node objects with nodeId
            - query: Search query string
            - reasoning_model: LLM model to use (defaults to groq_llama for parity)
            - hyde_analysis: Hyde analysis results (optional)
            - max_concurrent_calls: Max concurrent LLM calls (optional, defaults to 5)

    Returns:
        Dictionary with reasoning results and metadata
    """
    start_time = datetime.utcnow()
    search_id = None

    try:
        # Check if this is the new searchId-based approach
        search_id = event_data.get('searchId')

        # New parameters for controlling ranking and reasoning
        ranking_enabled = event_data.get('ranking_enabled', True)
        reasoning_enabled = event_data.get('reasoning_enabled', False)
        top_k_for_insights = event_data.get('top_k_for_insights', 10)
        
        if search_id:
            # New approach: load from searchOutput collection
            logger.info(f"Processing reasoning for searchId: {search_id}")
            
            # Get search document and verify FetchAndRank is complete
            search_doc = searchOutputCollection.find_one({"_id": search_id})
            if not search_doc:
                return {
                    'statusCode': 404,
                    'body': json.dumps({
                        'error': f'Search document not found for searchId: {search_id}'
                    })
                }

            # Verify search is complete
            current_status = search_doc.get("status")
            if current_status != SearchStatus.SEARCH_COMPLETE:
                return {
                    'statusCode': 400,
                    'body': json.dumps({
                        'error': f'Search not complete for searchId: {search_id}, status: {current_status}'
                    })
                }

            # Extract data from search document
            query = search_doc.get("query", "")
            results_data = search_doc.get("results", {})
            hyde_analysis = search_doc.get("hydeAnalysis", {}).get("response", {})
            # Adapt HyDE response to flattened details for ranking XML prompt
            hyde_details_for_rank = adapt_hyde_response_to_rank_details(hyde_analysis)

            # Get model from flags or use default
            flags = search_doc.get("flags", {})
            model = flags.get('reasoning_model', flags.get('model', 'groq_llama'))
            max_concurrent_calls = event_data.get('max_concurrent_calls', 5)

            # Get candidates from search results
            candidates = results_data.get("candidates", [])
            if not candidates:
                return {
                    'statusCode': 400,
                    'body': json.dumps({
                        'error': 'No candidates found for ranking and reasoning'
                    })
                }

            logger.info(f"Found {len(candidates)} candidates for processing")

            materials = build_candidate_materials(candidates, search_doc.get("hydeAnalysis", {}))
            enriched_candidates = materials["enriched_list"]
            enriched_map = materials["enriched_map"]
            transformed_map = materials["transformed_map"]

            if not enriched_candidates:
                logger.warning("No enriched candidates available after Mongo fetch; persisting original candidate list")
                searchOutputCollection.update_one(
                    {"_id": search_id},
                    {"$set": {
                        "results.ranked": [],
                        "results.candidates": candidates
                    }}
                )
                nodes = [{"nodeId": c.get("personId")} for c in candidates if c.get("personId")]
            else:
                similarity_sorted = sorted(
                    enriched_candidates,
                    key=lambda c: c.get("similarity", 0),
                    reverse=True
                )

                minimal_results: List[Dict] = []
                final_candidates: List[Dict] = []

                if ranking_enabled:
                    DEFAULT_TOP_N = 150
                    rank_top_n = event_data.get('rank_top_n')
                    if rank_top_n is None:
                        try:
                            rank_top_n = int(os.getenv('RANK_TOP_N', DEFAULT_TOP_N))
                        except Exception:
                            rank_top_n = DEFAULT_TOP_N

                    rank_ids = [
                        entry["personId"]
                        for entry in similarity_sorted[:rank_top_n]
                        if entry.get("personId") in transformed_map
                    ]
                    rank_people = [transformed_map[pid] for pid in rank_ids]
                    logger.info(
                        f"Ranking {len(rank_people)} candidates (requested top {rank_top_n}, available {len(similarity_sorted)})"
                    )

                    ranked_results = []
                    if rank_people:
                        ranked_results = await process_people_direct(
                            rank_people,
                            query,
                            hyde_analysis_flags=hyde_details_for_rank,
                            batch_size=5,
                            max_concurrent_tasks=max_concurrent_calls,
                            reasoning_model=model
                        )
                        logger.info(f"Ranking completed: {len(ranked_results)} scored profiles")

                    score_threshold = float(os.getenv("RANK_SCORE_THRESHOLD", "6.5"))
                    filtered_ranked = [
                        r for r in ranked_results
                        if r.get("recommendationScore", 0) >= score_threshold
                    ]

                    # OPTIMIZED: Single Progressive List Approach
                    # Load existing candidates list (with match metadata intact)
                    search_doc = searchOutputCollection.find_one({"_id": search_id})
                    existing_candidates = search_doc.get("results", {}).get("candidates", [])
                    candidates_by_id = {c["personId"]: c for c in existing_candidates}

                    # Progressive enhancement - add ranking scores
                    scored_count = 0
                    for ranked in filtered_ranked:
                        result_copy = convert_objectids_to_strings(ranked.copy())
                        pid = result_copy.pop("personId", None)
                        if not pid or pid not in candidates_by_id:
                            continue

                        # Add score to existing candidate object
                        candidates_by_id[pid]["score"] = result_copy.get("recommendationScore", 0)
                        candidates_by_id[pid]["ranked"] = True
                        scored_count += 1

                        # Add other ranking metadata if needed
                        for key, value in result_copy.items():
                            if key not in ["recommendationScore"]:  # Avoid duplicating renamed fields
                                candidates_by_id[pid][key] = value

                    # Sort by score (list order = ranking) - scored items first, then by similarity
                    final_candidates = sorted(
                        candidates_by_id.values(),
                        key=lambda x: (
                            x.get("score", -1),  # Scored items first (higher scores first)
                            x.get("similarity", 0)  # Then by similarity
                        ),
                        reverse=True
                    )

                    logger.info(
                        f"Final candidate distribution -> scored: {scored_count}, total: {len(final_candidates)}"
                    )
                else:
                    logger.info(f"Ranking disabled; returning candidates ordered by similarity")
                    # OPTIMIZED: Still use single progressive list even when ranking is disabled
                    search_doc = searchOutputCollection.find_one({"_id": search_id})
                    existing_candidates = search_doc.get("results", {}).get("candidates", [])
                    final_candidates = sorted(
                        existing_candidates,
                        key=lambda x: x.get("similarity", 0),
                        reverse=True
                    )

                summary = results_data.get("summary", {}) or {}
                summary.update({
                    "count": len(final_candidates),
                    "topK": len([c for c in final_candidates if c.get("ranked", False)]),
                    "idsOnly": False
                })

                # OPTIMIZED: Single update - replace only candidates array, remove ranked array
                searchOutputCollection.update_one(
                    {"_id": search_id},
                    {
                        "$set": {
                            "results.candidates": final_candidates,
                            "results.summary": summary
                        },
                        "$unset": {
                            "results.ranked": ""  # Remove deprecated ranked array
                        }
                    }
                )

                nodes = [{"nodeId": entry.get("personId")} for entry in final_candidates if entry.get("personId")]
            

        # Legacy path removed: this service only supports searchId-based execution

        # Validate required data
        if not query:
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'error': 'No query provided'
                })
            }

        # Handle reasoning if enabled
        results = []
        if reasoning_enabled:
            if not nodes:
                return {
                    'statusCode': 400,
                    'body': json.dumps({
                        'error': 'No nodes available for reasoning'
                    })
                }

            # Limit nodes for reasoning analysis to top_k_for_insights
            reasoning_nodes = nodes[:top_k_for_insights] if len(nodes) > top_k_for_insights else nodes

            logger.info(f"Processing reasoning for {len(reasoning_nodes)} nodes (top {top_k_for_insights}) with model: {model}")

            # Initialize SearchReasoning
            search_reasoning = SearchReasoning(max_concurrent_calls=max_concurrent_calls)

            # Process the batch
            results = await search_reasoning.batch_analyze_profiles(
                reasoning_nodes,
                query,
                model,
                hyde_analysis
            )

            logger.info(f"Reasoning completed for {len(results)} nodes")
        else:
            logger.info("Reasoning disabled, skipping detailed analysis")

        # Calculate processing time and statistics
        processing_time = (datetime.utcnow() - start_time).total_seconds()
        successful = len([r for r in results if 'error' not in r])
        failed = len([r for r in results if 'error' in r])

        logger.info(f"Processing completed. Successful: {successful}, Failed: {failed}, Time: {processing_time}s")

        # OPTIMIZED: Progressive enhancement - add reasoning insights to existing candidates
        if reasoning_enabled and search_id and results:
            # Load current candidates list (which may already have ranking scores)
            current_search_doc = searchOutputCollection.find_one({"_id": search_id})
            current_candidates = current_search_doc.get("results", {}).get("candidates", [])
            candidates_by_id = {c["personId"]: c for c in current_candidates}

            # Add reasoning insights to candidates in-place
            for result in results:
                person_id = result.get("nodeId")
                if person_id and person_id in candidates_by_id:
                    candidates_by_id[person_id]["reasoning"] = {
                        "summary": result.get("summary", ""),
                        "highlights": result.get("highlights", []),
                        "metadata": result.get("metadata", {}),
                        "reasoning_complete": True if 'error' not in result else False
                    }
                    if 'error' in result:
                        candidates_by_id[person_id]["reasoning"]["error"] = result["error"]

            # Update candidates array with reasoning data
            updated_candidates = list(candidates_by_id.values())
            searchOutputCollection.update_one(
                {"_id": search_id},
                {"$set": {"results.candidates": updated_candidates}}
            )

        # Simplified reasoning results for metadata only (no longer storing full results separately)
        reasoning_results = {
            'metadata': {
                'total_nodes': len(nodes),
                'reasoning_nodes_processed': len(results) if results else 0,
                'successful_count': successful,
                'failed_count': failed,
                'processing_time_seconds': processing_time,
                'model_used': model,
                'query': query,
                'ranking_enabled': ranking_enabled,
                'reasoning_enabled': reasoning_enabled,
                'timestamp': get_utc_now()
            }
            # NOTE: Full reasoning results now embedded in candidates array
        }

        # If using searchId approach, update the search document
        if search_id:
            now = datetime.utcnow()

            # Determine final status and stage based on operations performed
            if reasoning_enabled:
                final_status = SearchStatus.RANK_AND_REASONING_COMPLETE
                stage = "RANK_AND_REASONING"
                stage_message = f"Ranking and reasoning completed, {successful} successful, {failed} failed"
                metrics_key = "metrics.rankAndReasoningMs"
            else:
                # Only ranking was performed
                final_status = SearchStatus.RANK_AND_REASONING_COMPLETE
                stage = "RANKING"
                stage_message = f"Ranking completed, {len(nodes)} results processed"
                metrics_key = "metrics.rankingMs"

            # OPTIMIZED: Only store reasoning metadata, not full results (those are now in candidates)
            update_result = searchOutputCollection.update_one(
                {"_id": search_id},
                {
                    "$set": {
                        "reasoning.metadata": reasoning_results['metadata'],  # Only metadata
                        "status": final_status,
                        metrics_key: processing_time * 1000,
                        "updatedAt": now
                    },
                    "$unset": {
                        "reasoning.results": ""  # Remove full reasoning results array
                    },
                    "$push": {
                        "events": {
                            "stage": stage,
                            "message": stage_message,
                            "timestamp": now
                        }
                    }
                }
            )
            
            if update_result.matched_count == 0:
                logger.error(f"Failed to update search document for searchId: {search_id}")

            logger.info(f"Updated search document {search_id} with reasoning results")

        return {
            'statusCode': 200,
            'body': json.dumps(reasoning_results)  # Fixed: now returns JSON string
        }

    except Exception as e:
        logger.error(f"Error processing reasoning request: {str(e)}")
        logger.error(traceback.format_exc())
        
        # Update search document with error state if we have searchId
        if search_id:
            try:
                now = datetime.utcnow()
                searchOutputCollection.update_one(
                    {"_id": search_id},
                    {
                        "$set": {
                            "status": SearchStatus.ERROR,
                            "error": {
                                "stage": "RANK_AND_REASONING",
                                "message": str(e),
                                "stackTrace": traceback.format_exc(),
                                "occurredAt": now
                            },
                            "updatedAt": now
                        },
                        "$push": {
                            "events": {
                                "stage": "RANK_AND_REASONING",
                                "message": f"Error: {str(e)}",
                                "timestamp": now
                            }
                        }
                    }
                )
            except Exception as db_error:
                logger.error(f"Failed to update error state: {db_error}")
        
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': f'Internal server error: {str(e)}',
                'timestamp': get_utc_now()
            })
        }

def lambda_handler(event, context):
    """
    AWS Lambda handler for ranking and optional reasoning.

    Expected event format:
    {
        "searchId": "uuid-string",           // required; loads candidates and hydeAnalysis from searchOutput collection
        "ranking_enabled": true,             // optional, default true
        "reasoning_enabled": false,          // optional, default false
        "top_k_for_insights": 10,            // optional, default 10
        "max_concurrent_calls": 5            // optional, default 5
    }
    """
    try:
        logger.info(f"Received reasoning request: {json.dumps(event, default=str)}")

        # Run the async processing
        result = asyncio.run(process_reasoning_request(event))

        logger.info(f"Reasoning request completed with status: {result['statusCode']}")
        return result

    except Exception as e:
        logger.error(f"Lambda handler error: {str(e)}")
        logger.error(traceback.format_exc())
        return {
            'statusCode': 500,
            'body': {
                'error': f'Lambda handler error: {str(e)}',
                'timestamp': get_utc_now()
            }
        }

# For local testing
if __name__ == "__main__":
    # Minimal local test: expects an existing searchId in DB
    test_event = {
        "searchId": os.getenv("TEST_SEARCH_ID", "test-search-id-123"),
        "ranking_enabled": True,
        "reasoning_enabled": False,
        "top_k_for_insights": 10
    }
    print("Testing searchId-based approach:")
    res = lambda_handler(test_event, None)
    print(json.dumps(res, indent=2, default=str))