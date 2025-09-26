import json
import asyncio
import os
import traceback
from copy import deepcopy
from datetime import datetime, timezone
from typing import Dict, List
from dotenv import load_dotenv
from reasoning_logic import SearchReasoning
from logging_config import setup_logger
from api_client import (
    get_search_document,
    update_search_document,
    SearchServiceError,
)
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
        """Flatten HyDE response into the structure expected by ranking prompts."""
        if not isinstance(hyde_resp, dict):
            return {}

        if any(key in hyde_resp for key in ("locations", "skills", "organizations", "sectors", "db_queries")):
            return hyde_resp

        details = {}

        loc = hyde_resp.get("locationDetails", {})
        if isinstance(loc, dict):
            names = []
            for item in loc.get("locations", []) or []:
                if isinstance(item, dict):
                    name = item.get("name")
                else:
                    name = item
                if name:
                    names.append(name)
            if names:
                details["locations"] = names
            operator = loc.get("operator")
            if operator:
                details["location_operator"] = operator

        org = hyde_resp.get("organisationDetails", {}) or hyde_resp.get("organizationDetails", {})
        if isinstance(org, dict):
            names = []
            for item in org.get("organizations", []) or []:
                if isinstance(item, dict):
                    name = item.get("name")
                else:
                    name = item
                if name:
                    names.append(name)
            if names:
                details["organizations"] = names
            operator = org.get("operator")
            if operator:
                details["organization_operator"] = operator
            temporal = org.get("temporal")
            if temporal:
                details["organization_temporal"] = temporal

        sector = hyde_resp.get("sectorDetails", {})
        if isinstance(sector, dict):
            names = []
            for item in sector.get("sectors", []) or []:
                if isinstance(item, dict):
                    name = item.get("name")
                else:
                    name = item
                if name:
                    names.append(name)
            if names:
                details["sectors"] = names
            operator = sector.get("operator")
            if operator:
                details["sector_operator"] = operator
            temporal = sector.get("temporal")
            if temporal:
                details["sector_temporal"] = temporal

        skills = hyde_resp.get("skillDetails", {})
        if isinstance(skills, dict):
            names = []
            for item in skills.get("skills", []) or []:
                if isinstance(item, dict):
                    name = item.get("name")
                else:
                    name = item
                if name:
                    names.append(name)
            if names:
                details["skills"] = names
            operator = skills.get("operator")
            if operator:
                details["skill_operator"] = operator

        db_queries = hyde_resp.get("dbQueryDetails", {})
        if isinstance(db_queries, dict):
            queries = []
            for query in db_queries.get("queries", []) or []:
                if isinstance(query, dict):
                    field_name = query.get("field", "")
                    description = query.get("description", "") or ""
                    if field_name:
                        queries.append({"field": field_name, "description": description})
            if queries:
                details["db_queries"] = queries
            operator = db_queries.get("operator")
            if operator:
                details["db_query_operator"] = operator

        return details

    start_time = datetime.utcnow()
    search_id = event_data.get("searchId") or event_data.get("search_id") or event_data.get("search_output_id")

    try:
        if not search_id:
            return {
                "statusCode": 400,
                "body": json.dumps({"error": "Missing searchId in request"})
            }

        logger.info(f"Processing rank & reasoning for searchId={search_id}")

        search_doc = get_search_document(search_id)
        if not search_doc:
            return {
                "statusCode": 404,
                "body": json.dumps({"error": f"Search document not found for searchId: {search_id}"})
            }

        query = search_doc.get("query", "")
        if not query:
            return {
                "statusCode": 400,
                "body": json.dumps({"error": "Search document missing query text"})
            }

        flags = search_doc.get("flags", {}) or {}
        ranking_enabled = event_data.get("ranking_enabled")
        if ranking_enabled is None:
            ranking_enabled = True
        reasoning_enabled = event_data.get("reasoning_enabled")
        if reasoning_enabled is None:
            reasoning_enabled = bool(flags.get("reasoning", True))

        model = (
            event_data.get("reasoning_model")
            or flags.get("reasoning_model")
            or flags.get("model")
            or "groq_llama"
        )

        max_concurrent_calls_value = event_data.get("max_concurrent_calls", 5)
        try:
            max_concurrent_calls = int(max_concurrent_calls_value)
        except (TypeError, ValueError):
            max_concurrent_calls = 5

        results_data = search_doc.get("results", {}) or {}
        existing_candidates: List[Dict] = results_data.get("candidates", []) or []
        if not existing_candidates:
            return {
                "statusCode": 400,
                "body": json.dumps({"error": "No candidates available in search results"})
            }

        candidate_ids = event_data.get("candidateIds") or event_data.get("candidate_ids")
        if candidate_ids and isinstance(candidate_ids, str):
            candidate_ids = [candidate_ids]

        candidate_map = {c.get("nodeId"): c for c in existing_candidates if c.get("nodeId")}
        if candidate_ids:
            selected_ids = [str(cid) for cid in candidate_ids if cid]
        else:
            selected_ids = list(candidate_map.keys())

        selected_ids = [cid for cid in selected_ids if cid in candidate_map]
        missing_candidate_ids = []
        if candidate_ids:
            requested = [str(cid) for cid in candidate_ids if cid]
            missing_candidate_ids = [cid for cid in requested if cid not in candidate_map]
            if missing_candidate_ids:
                logger.warning(
                    "The following candidateIds were not found in search results: %s",
                    missing_candidate_ids
                )

        if not selected_ids:
            logger.warning("No matching candidateIds provided - skipping processing")
            return {
                "statusCode": 200,
                "body": json.dumps({
                    "message": "No matching candidates found for provided candidateIds",
                    "processedCandidateIds": [],
                    "missingCandidateIds": missing_candidate_ids
                })
            }

        hyde_analysis_full = search_doc.get("hydeAnalysis", {}) or {}
        hyde_analysis_response = hyde_analysis_full.get("response", {}) or {}
        hyde_details_for_rank = adapt_hyde_response_to_rank_details(hyde_analysis_response)

        batch_candidates = [candidate_map[cid] for cid in selected_ids]
        materials = build_candidate_materials(batch_candidates, hyde_analysis_full)
        transformed_map = materials.get("transformed_map", {})

        ranking_results_map: Dict[str, Dict] = {}
        if ranking_enabled:
            rank_people = [transformed_map[cid] for cid in selected_ids if cid in transformed_map]

            if not rank_people:
                logger.warning("No enriched candidates available for ranking in this batch")
            else:
                rank_batch_size_value = (
                    event_data.get("rank_batch_size")
                    or os.getenv("RANK_BATCH_SIZE")
                    or 5
                )
                try:
                    rank_batch_size = int(rank_batch_size_value)
                except (TypeError, ValueError):
                    rank_batch_size = 5

                ranked_results = await process_people_direct(
                    rank_people,
                    query,
                    hyde_analysis_flags=hyde_details_for_rank,
                    batch_size=rank_batch_size,
                    max_concurrent_tasks=max_concurrent_calls,
                    reasoning_model=model
                )
                for ranked in ranked_results:
                    ranked_copy = convert_objectids_to_strings(ranked.copy())
                    pid = ranked_copy.pop("nodeId", None)
                    if not pid:
                        continue
                    ranking_results_map[pid] = ranked_copy

            for cid in selected_ids:
                candidate = candidate_map[cid]
                payload = ranking_results_map.get(cid)
                if payload:
                    candidate["score"] = payload.get("recommendationScore", candidate.get("score"))
                    for key, value in payload.items():
                        if key == "recommendationScore":
                            continue
                        candidate[key] = value
                else:
                    candidate.pop("score", None)
        else:
            logger.info("Ranking disabled for this invocation; preserving existing scores")

        reasoning_results_map: Dict[str, Dict] = {}
        reasoning_results: List[Dict] = []
        if reasoning_enabled:
            reasoning_nodes = [{"nodeId": cid} for cid in selected_ids]
            if not reasoning_nodes:
                logger.warning("No reasoning nodes available for batch")
            else:
                search_reasoning = SearchReasoning(max_concurrent_calls=max_concurrent_calls)
                reasoning_results = await search_reasoning.batch_analyze_profiles(
                    reasoning_nodes,
                    query,
                    model,
                    hyde_analysis_response
                )
                for result in reasoning_results:
                    node_id = result.get("nodeId")
                    if node_id:
                        reasoning_results_map[node_id] = result

            for cid in selected_ids:
                candidate = candidate_map[cid]
                result = reasoning_results_map.get(cid)
                if not result:
                    candidate["reasoning"] = {
                        "summary": "",
                        "highlights": [],
                        "metadata": {},
                        "reasoning_complete": False,
                        "error": "Reasoning output missing"
                    }
                    continue

                if "error" in result:
                    candidate["reasoning"] = {
                        "summary": "",
                        "highlights": [],
                        "metadata": {},
                        "reasoning_complete": False,
                        "error": result.get("error")
                    }
                else:
                    candidate["reasoning"] = {
                        "summary": result.get("summary", ""),
                        "highlights": result.get("highlights", []),
                        "metadata": result.get("metadata", {}),
                        "reasoning_complete": True
                    }
        else:
            logger.info("Reasoning disabled for this invocation")

        def sort_key(candidate: Dict) -> tuple:
            score = candidate.get("score")
            similarity = candidate.get("similarity", 0)
            if score is None:
                return (float("-inf"), similarity)
            return (score, similarity)

        sorted_candidates = sorted(existing_candidates, key=sort_key, reverse=True)

        summary = results_data.get("summary", {}) or {}
        summary.update({
            "count": len(sorted_candidates),
            "topK": len([c for c in sorted_candidates if c.get("score") is not None]),
            "idsOnly": False
        })

        processing_time = (datetime.utcnow() - start_time).total_seconds()

        existing_metadata = (search_doc.get("reasoning") or {}).get("metadata", {}) or {}
        cumulative_processing_time = float(existing_metadata.get("processing_time_seconds", 0.0)) + processing_time

        processed_reasoning = [
            c for c in sorted_candidates
            if isinstance(c.get("reasoning"), dict)
        ]
        successful_reasoning = [
            c for c in processed_reasoning
            if c["reasoning"].get("reasoning_complete") and not c["reasoning"].get("error")
        ]
        failed_reasoning = [
            c for c in processed_reasoning
            if c["reasoning"].get("reasoning_complete") is False or c["reasoning"].get("error")
        ]

        metadata = existing_metadata.copy()
        metadata.update({
            "total_nodes": len(sorted_candidates),
            "reasoning_nodes_processed": len(processed_reasoning),
            "successful_count": len(successful_reasoning),
            "failed_count": len(failed_reasoning),
            "processing_time_seconds": cumulative_processing_time,
            "model_used": model,
            "query": query,
            "ranking_enabled": bool(ranking_enabled),
            "reasoning_enabled": bool(reasoning_enabled),
            "timestamp": get_utc_now()
        })

        batch_number = event_data.get("batchNumber") or event_data.get("batch_number")
        total_batches = event_data.get("totalBatches") or event_data.get("total_batches")
        if batch_number:
            metadata["last_batch_number"] = batch_number
        if total_batches:
            metadata["batches_total"] = total_batches

        raw_is_final_batch = event_data.get("isFinalBatch")
        if raw_is_final_batch is None:
            raw_is_final_batch = event_data.get("is_final_batch")

        if raw_is_final_batch is None and batch_number is not None and total_batches is not None:
            try:
                is_final_batch = int(batch_number) == int(total_batches)
            except (TypeError, ValueError):
                is_final_batch = False
        else:
            is_final_batch = bool(raw_is_final_batch)

        now = datetime.utcnow()
        stage_message_parts = [f"Processed {len(selected_ids)} candidates"]
        if batch_number and total_batches:
            stage_message_parts.append(f"batch {batch_number}/{total_batches}")
        elif batch_number:
            stage_message_parts.append(f"batch {batch_number}")
        stage_message = ", ".join(stage_message_parts)

        existing_results = deepcopy(search_doc.get("results") or {})
        existing_results["candidates"] = sorted_candidates
        existing_results["summary"] = summary
        existing_results.pop("ranked", None)

        existing_reasoning = deepcopy(search_doc.get("reasoning") or {})
        existing_reasoning["metadata"] = metadata

        existing_metrics = deepcopy(search_doc.get("metrics") or {})
        current_ms = existing_metrics.get("rankAndReasoningMs", 0) or 0
        existing_metrics["rankAndReasoningMs"] = current_ms + processing_time * 1000

        set_fields = {
            "results": existing_results,
            "reasoning": existing_reasoning,
            "metrics": existing_metrics,
            "updatedAt": now
        }
        if is_final_batch:
            set_fields["status"] = SearchStatus.RANK_AND_REASONING_COMPLETE

        try:
            update_search_document(
                search_id,
                set_fields=set_fields,
                append_events=[
                    {
                        "stage": "RANK_AND_REASONING",
                        "message": stage_message,
                        "timestamp": now
                    }
                ],
                expected_statuses=[SearchStatus.SEARCH_COMPLETE, SearchStatus.RANK_AND_REASONING_COMPLETE],
            )
        except SearchServiceError as update_error:
            logger.error("Failed to update search document %s: %s", search_id, update_error)
            raise

        response_body = {
            "metadata": metadata,
            "processedCandidateIds": selected_ids,
            "missingCandidateIds": missing_candidate_ids,
            "rankingApplied": bool(ranking_enabled),
            "reasoningApplied": bool(reasoning_enabled),
            "processingTimeSeconds": processing_time
        }

        if batch_number:
            response_body["batchNumber"] = batch_number
        if total_batches:
            response_body["totalBatches"] = total_batches
        response_body["isFinalBatch"] = is_final_batch

        return {
            "statusCode": 200,
            "body": json.dumps(response_body)
        }

    except Exception as e:
        logger.error(f"Error processing reasoning request: {str(e)}")
        logger.error(traceback.format_exc())
        
        # Update search document with error state if we have searchId
        if search_id:
            try:
                now = datetime.utcnow()
                update_search_document(
                    search_id,
                    set_fields={
                        "status": SearchStatus.ERROR,
                        "error": {
                            "stage": "RANK_AND_REASONING",
                            "message": str(e),
                            "stackTrace": traceback.format_exc(),
                            "occurredAt": now
                        },
                        "updatedAt": now
                    },
                    append_events=[
                        {
                            "stage": "RANK_AND_REASONING",
                            "message": f"Error: {str(e)}",
                            "timestamp": now
                        }
                    ],
                )
            except SearchServiceError as db_error:
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
        "searchId": "uuid-string",            // required
        "candidateIds": ["nodeId1", ...],   // optional; defaults to all candidates
        "batchNumber": 1,                      // optional metadata for logging
        "totalBatches": 12,                    // optional metadata for logging
        "isFinalBatch": false,                 // optional; mark final batch to set completion status
        "ranking_enabled": true,               // optional, default true
        "reasoning_enabled": true,             // optional, default flags.reasoning
        "max_concurrent_calls": 5,             // optional, default 5
        "reasoning_model": "gemini"           // optional, overrides flags.reasoning_model
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
        "reasoning_enabled": True,
        "candidateIds": []
    }
    print("Testing searchId-based approach:")
    res = lambda_handler(test_event, None)
    print(json.dumps(res, indent=2, default=str))
