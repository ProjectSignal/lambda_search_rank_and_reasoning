import json
import asyncio
import os
import traceback
from datetime import datetime, timezone
from dotenv import load_dotenv
from reasoning_logic import SearchReasoning
from logging_config import setup_logger
from db import nodes_collection, searchOutputCollection

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

            # Initialize variables
            ranked_results = []
            nodes = []

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

            # Always perform ranking when ranking_enabled (default True)
            if ranking_enabled:
                logger.info(f"Performing ranking on {len(candidates)} candidates...")

                # Import ranking function
                from ranking import process_people_direct

                # Perform ranking
                ranked_results = await process_people_direct(
                    candidates,
                    query,
                    hyde_analysis_flags=hyde_details_for_rank,
                    batch_size=5,
                    max_concurrent_tasks=max_concurrent_calls,
                    reasoning_model=model
                )

                logger.info(f"Ranking completed: {len(ranked_results)} results")

                # Store minimal ranked results in database
                minimal_results = []
                for r in ranked_results:
                    # Find the original candidate data to preserve search metadata
                    person_id = r.get("personId")
                    original_candidate = next((c for c in candidates if c.get("personId") == person_id), None)

                    minimal_result = {
                        "nodeId": person_id,
                        "score": r.get("recommendationScore", 0),
                        # Preserve original search metadata from candidates
                        "similarity": original_candidate.get("similarity", 0) if original_candidate else 0,
                        "matchedBoth": original_candidate.get("matchedBoth", False) if original_candidate else False,
                        "matchedOrgOnly": original_candidate.get("matchedOrgOnly", False) if original_candidate else False,
                        "matchedSkillOnly": original_candidate.get("matchedSkillOnly", False) if original_candidate else False
                    }
                    minimal_results.append(minimal_result)

                # Update search document with ranked results
                searchOutputCollection.update_one(
                    {"_id": search_id},
                    {"$set": {
                        "results.ranked": minimal_results
                    }}
                )

                # Use ranked results for reasoning
                nodes = [{"nodeId": r.get("personId")} for r in ranked_results if r.get("personId")]
            else:
                # No ranking, use candidates directly for reasoning
                logger.info(f"Skipping ranking, using {len(candidates)} candidates directly")
                nodes = [{"nodeId": c.get("personId")} for c in candidates if c.get("personId")]
            

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

        reasoning_results = {
            'results': results,
            'metadata': {
                'total_nodes': len(nodes),
                'reasoning_nodes_processed': len(results),
                'successful_count': successful,
                'failed_count': failed,
                'processing_time_seconds': processing_time,
                'model_used': model,
                'query': query,
                'ranking_enabled': ranking_enabled,
                'reasoning_enabled': reasoning_enabled,
                'timestamp': get_utc_now()
            }
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

            update_result = searchOutputCollection.update_one(
                {"_id": search_id},
                {
                    "$set": {
                        "reasoning": reasoning_results,
                        "status": final_status,
                        metrics_key: processing_time * 1000,
                        "updatedAt": now
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