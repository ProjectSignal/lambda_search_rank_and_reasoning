from logging_config import setup_logger
from prompts.search_ranking import message
import json
import xml.etree.ElementTree as ET
import re
from typing import Any, List, Dict, Tuple, Optional
import asyncio
import aiofiles
import time
from datetime import datetime
import math
import os

from api_client import fetch_nodes_by_ids, SearchServiceError
from llm_helper import LLMManager
from model_config import MODEL_CONFIGS
import traceback

logger = setup_logger(__name__)
# import logging
# logger = setup_logger(__name__)
# logger = logging.getLogger(__name__)


def convert_objectids_to_strings(obj):
    """Pass-through function for API-based approach.
    Data is already in serializable format from backend API."""
    return obj


def process_mutuals(mutual_ids):
    """Fetch mutual connection metadata for the provided identifiers."""
    if not mutual_ids:
        return []

    node_ids = []
    for mid in mutual_ids:
        if isinstance(mid, dict) and "$oid" in mid:
            # Handle Extended JSON format
            node_ids.append(str(mid["$oid"]))
        elif mid:
            # Convert to string
            node_ids.append(str(mid))

    if not node_ids:
        return []

    try:
        mutual_map = fetch_nodes_by_ids(
            node_ids,
            projection={"_id": 1, "name": 1, "avatarURL": 1}
        )
    except SearchServiceError as exc:
        logger.error("Failed to fetch mutual connections via API: %s", exc)
        return []

    results = []
    for node_id in node_ids:
        doc = mutual_map.get(node_id)
        if not doc:
            continue
        normalized_id = str(doc.get("_id") or doc.get("nodeId") or node_id)
        results.append(
            {
                "nodeId": normalized_id,
                "name": doc.get("name", ""),
                "avatarURL": doc.get("avatarURL", "")
            }
        )
    return results


def analyze_hyde_data_requirements(hyde_result: dict) -> dict:
    """Determine which additional Mongo fields are needed for ranking."""
    required_fields = set()
    db_queries = []

    if not hyde_result:
        return {"additional_fields": [], "db_queries": []}

    hyde_response = hyde_result.get("response", hyde_result)

    if hyde_response.get("dbBasedQuery", False):
        db_details = hyde_response.get("dbQueryDetails", {})
        for query in db_details.get("queries", []):
            field = query.get("field")
            if not field:
                continue
            db_queries.append(query)
            if field.startswith("education."):
                required_fields.add("education")
            elif field.startswith("accomplishments."):
                required_fields.add("accomplishments")
            elif field.startswith("workExperience.") and not field.startswith("workExperience.0."):
                required_fields.add("workExperience")
            elif field.startswith("certifications."):
                required_fields.add("accomplishments")

    return {
        "additional_fields": list(required_fields),
        "db_queries": db_queries
    }


def build_candidate_materials(candidates: List[Dict], hyde_result: dict) -> Dict[str, Any]:
    """Fetch Mongo documents and prepare data needed for ranking and output."""
    hyde_requirements = analyze_hyde_data_requirements(hyde_result)
    additional_fields = hyde_requirements.get("additional_fields", [])

    node_ids: List[str] = []
    for cand in candidates:
        pid = cand.get("nodeId")
        if not pid:
            continue
        node_ids.append(str(pid))

    if not node_ids:
        return {
            "enriched_list": [],
            "enriched_map": {},
            "transformed_map": {},
            "missing_ids": [],
            "hyde_requirements": hyde_requirements
        }

    base_projection = {
        "_id": 1,
        "about": 1,
        "name": 1,
        "currentLocation": 1,
        "workExperience": 1,
        "scrapped": 1,
        "connectionLevel": 1,
        "linkedinUsername": 1,
        "linkedinHeadline": 1,
        "contacts": 1,
        "avatarURL": 1,
        "mutual": 1,
        "stage": 1,
        "education": 1,
        "accomplishments": 1,
        "volunteering": 1
    }

    for field in additional_fields:
        base_projection[field] = 1

    try:
        fetched_docs = fetch_nodes_by_ids(node_ids, projection=base_projection)
    except SearchServiceError as exc:
        logger.error("Failed to fetch candidate materials via API: %s", exc)
        fetched_docs = {}

    mongo_docs = {}
    for key, doc in fetched_docs.items():
        normalized_id = str(doc.get("_id") or doc.get("nodeId") or key)
        doc["_id"] = normalized_id
        mongo_docs[normalized_id] = doc

    enriched_list: List[Dict] = []
    enriched_map: Dict[str, Dict] = {}
    transformed_map: Dict[str, Dict] = {}
    missing_ids: List[str] = []

    for candidate in candidates:
        pid = candidate.get("nodeId")
        if not pid:
            continue

        doc = mongo_docs.get(pid)
        if not doc or not doc.get("scrapped"):
            missing_ids.append(pid)
            continue

        doc_clean = convert_objectids_to_strings(doc)
        candidate_copy = convert_objectids_to_strings(candidate.copy())
        for obsolete_flag in ("matchedBoth", "matchedOrgOnly", "matchedSkillOnly", "matchedSectorOnly"):
            candidate_copy.pop(obsolete_flag, None)

        work_experience = doc_clean.get("workExperience", []) or []
        if work_experience:
            first_exp = work_experience[0]
            current_work = {
                "companyName": first_exp.get("companyName", ""),
                "duration": first_exp.get("duration", ""),
                "description": first_exp.get("description", ""),
                "location": first_exp.get("location", ""),
                "title": first_exp.get("title", "")
            }
        else:
            current_work = {
                "companyName": "",
                "duration": "",
                "description": "",
                "location": "",
                "title": ""
            }

        mutuals_raw = doc.get("mutual") or doc.get("contacts", {}).get("mutuals", [])
        mutuals = process_mutuals(mutuals_raw)

        transformed_person = {
            "nodeId": pid,
            "userId": candidate_copy.get("userId", ""),
            "name": doc_clean.get("name", ""),
            "aboutMe": doc_clean.get("about", ""),
            "currentLocation": doc_clean.get("currentLocation", ""),
            "avatarURL": doc_clean.get("avatarURL", ""),
            "mutuals": mutuals,
            "linkedinHeadline": doc_clean.get("linkedinHeadline", ""),
            "education": doc_clean.get("education", []),
            "accomplishments": doc_clean.get("accomplishments", {}),
            "volunteering": doc_clean.get("volunteering", []),
            "workExperience": work_experience,
            "currentWork": current_work
        }

        transformed_map[pid] = transformed_person

        enriched_entry = {
            **candidate_copy,
            "nodeId": pid,
            "type": "person",
            "name": doc_clean.get("name", ""),
            "stage": doc_clean.get("stage", ""),
            "currentLocation": doc_clean.get("currentLocation", ""),
            "connectionLevel": doc_clean.get("connectionLevel", ""),
            "linkedinUsername": doc_clean.get("linkedinUsername", ""),
            "linkedinHeadline": doc_clean.get("linkedinHeadline", ""),
            "contacts": doc_clean.get("contacts", {}),
            "currentWork": current_work,
            "avatarURL": doc_clean.get("avatarURL", ""),
            "mutuals": mutuals,
            "score": None
        }

        enriched_entry = convert_objectids_to_strings(enriched_entry)
        enriched_list.append(enriched_entry)
        enriched_map[pid] = enriched_entry

    if missing_ids:
        logger.warning(f"Missing or unsuited Mongo documents for {len(missing_ids)} candidates: {missing_ids[:5]}{'...' if len(missing_ids) > 5 else ''}")

    return {
        "enriched_list": enriched_list,
        "enriched_map": enriched_map,
        "transformed_map": transformed_map,
        "missing_ids": missing_ids,
        "hyde_requirements": hyde_requirements
    }


class FingerprintMapper:
    def __init__(self):
        self._map = {}
        self._reverse_map = {}
        self._current_char = 'a'
        self._lock = asyncio.Lock()
        self._debug_log = []
        logger.info("Initialized FingerprintMapper")

    async def get_fingerprint(self, name: str) -> str:
        """Get or create a fingerprint for a name."""
        if not name:
            logger.warning("Empty name provided for fingerprint generation")
            return ""

        async with self._lock:
            if name in self._map:
                logger.debug(
                    f"Existing fingerprint found for '{name}': {self._map[name]}")
                return self._map[name]

            fingerprint = self._current_char
            self._map[name] = fingerprint
            self._reverse_map[fingerprint] = name
            logger.debug(
                f"Created new fingerprint for '{name}': {fingerprint}")

            # Move to next character
            if self._current_char == 'z':
                self._current_char = 'aa'
            else:
                if len(self._current_char) == 1:
                    self._current_char = chr(ord(self._current_char) + 1)
                else:
                    last_char = self._current_char[-1]
                    if last_char == 'z':
                        self._current_char = self._current_char[:-
                                                                1] + 'a' + 'a'
                    else:
                        self._current_char = self._current_char[:-1] + chr(
                            ord(last_char) + 1)

            return fingerprint

    async def get_original_name(self, fingerprint: str) -> str:
        """Get the original name for a fingerprint."""
        return self._reverse_map.get(fingerprint)

    async def replace_fingerprints_in_results(self, results: List[Dict], original_data: List[Dict]) -> List[Dict]:
        """Replace fingerprints with original data in the results."""
        async with self._lock:
            updated_results = []
            for result in results:
                result_copy = result.copy()
                fingerprint = result_copy.get('id')

                if fingerprint in self._reverse_map:
                    node_id = self._reverse_map[fingerprint]

                    # Find original person data
                    original_person = next(
                        (p for p in original_data if p.get(
                            'nodeId') == node_id),
                        None
                    )

                    if original_person:
                        result_copy['nodeId'] = node_id
                        result_copy['userId'] = original_person.get(
                            'userId', '')
                        result_copy['name'] = original_person.get('name', '')
                        del result_copy['id']
                    else:
                        logger.warning(
                            f"No original data found for {node_id}")
                else:
                    logger.warning(
                        f"No mapping found for fingerprint {fingerprint}")

                updated_results.append(result_copy)

            return updated_results

    async def _save_debug_logs(self):
        """Save fingerprint mapping debug logs to file."""
        try:
            debug_folder = "debug_logs"
            os.makedirs(debug_folder, exist_ok=True)

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            async with aiofiles.open(f"{debug_folder}/fingerprint_mapping_{timestamp}.log", 'w') as f:
                await f.write("\n".join(self._debug_log))
                await f.write("\n\nCurrent Maps:\n")
                await f.write(f"Forward Map: {json.dumps(self._map, indent=2)}\n")
                await f.write(f"Reverse Map: {json.dumps(self._reverse_map, indent=2)}")

            # Clear the log after saving
            self._debug_log = []
            logger.info(
                f"Debug logs saved to fingerprint_mapping_{timestamp}.log")

        except Exception as e:
            logger.error(f"Error saving fingerprint debug logs: {str(e)}")


async def convert_persons_to_xml(persons: List[Dict], fingerprint_mapper: FingerprintMapper) -> str:
    """Convert a list of persons' data to XML format using jsonToXml.py for rich profile data."""
    from jsonToXml import json_to_xml
    
    root = ET.Element("listOfPerson")

    for person in persons:
        # Get fingerprint derived from the candidate's node ID
        original_id = person.get("nodeId", "")
        fingerprint = await fingerprint_mapper.get_fingerprint(original_id)

        # Create person wrapper with fingerprint ID
        person_wrapper = ET.SubElement(root, "person")
        
        # Add fingerprint ID
        id_elem = ET.SubElement(person_wrapper, "id")
        id_elem.text = fingerprint

        # Use jsonToXml to convert the person data to rich XML
        # We need to prepare the person data in the expected format for jsonToXml
        person_data_for_xml = {
            "name": person.get("name", ""),
            "linkedinHeadline": person.get("linkedinHeadline", ""),
            "about": person.get("aboutMe", ""),
            "currentLocation": person.get("currentLocation", ""),
            "education": person.get("education", []),
            "workExperience": person.get("workExperience", []),
            "accomplishments": person.get("accomplishments", {}),
            "volunteering": person.get("volunteering", [])
        }

        # Generate rich XML using jsonToXml
        rich_xml_str = json_to_xml(person_data_for_xml)
        
        # Parse the generated XML and append its content (excluding the root <profile> tag)
        try:
            rich_xml_root = ET.fromstring(rich_xml_str)
            # Copy all child elements from the rich XML to our person wrapper
            for child in rich_xml_root:
                person_wrapper.append(child)
        except ET.ParseError as e:
            logger.error(f"Error parsing rich XML for person {original_id}: {str(e)}")
            # Fallback to basic info if XML parsing fails
            about = ET.SubElement(person_wrapper, "about")
            about.text = person.get("aboutMe", "")
            
            location = ET.SubElement(person_wrapper, "currentLocation")
            location.text = person.get("currentLocation", "")

    return ET.tostring(root, encoding='unicode', method='xml')


async def extract_skills_from_output(output_text: str) -> List[str]:
    """Extract skills from the output XML."""
    skills = []
    pattern = r'<skill>(.*?)</skill>'
    matches = re.finditer(pattern, output_text)
    return [match.group(1) for match in matches]


async def extract_score_data(response_text: str) -> List[Dict]:
    """Extract and parse all profile outputs from the response."""
    logger.info("Starting data extraction from response")
    results = []

    try:
        output_pattern = r'<output>\s*(.*?)\s*</output>'
        outputs = list(re.finditer(output_pattern, response_text, re.DOTALL))
        logger.info(f"Found {len(outputs)} output blocks in response")

        for i, output in enumerate(outputs, 1):
            try:
                output_text = output.group(1)
                logger.info(f"Processing output block {i}/{len(outputs)}")

                async def extract_value(tag: str) -> str:
                    pattern = f'<{tag}>(.*?)</{tag}>'
                    match = re.search(pattern, output_text, re.DOTALL)
                    if not match:
                        logger.warning(
                            f"Warning: No match found for tag '{tag}'")
                        return None if tag in ['skillMatch', 'locationMatch', 'entityMatch', 'databaseMatch', 'sectorMatch'] else ""

                    value = match.group(1).strip()
                    return convert_value(value, tag)

                def convert_value(value: str, tag: str):
                    if value in ['1', '[1]', '1.0']:
                        return 1
                    elif value in ['0', '[0]', '0.0']:
                        return 0
                    elif value in ['0.5', '[0.5]']:
                        return 0.5
                    elif value.lower() in ['null', '[null]', 'none']:
                        return None
                    elif tag == 'recommendationScore':
                        try:
                            return float(value.strip('[]'))
                        except:
                            logger.warning(
                                f"Warning: Could not convert score '{value}' to float")
                            return 0.0
                    elif tag in ['skillMatch', 'locationMatch', 'entityMatch', 'databaseMatch', 'sectorMatch']:
                        # Handle any numeric value for match fields
                        try:
                            return float(value.strip('[]'))
                        except:
                            logger.warning(
                                f"Warning: Could not convert match score '{value}' to float for {tag}")
                            return 0.0
                    return value.strip('[]').strip()

                result = {
                    "id": await extract_value("id"),  # Changed from name to id
                    "skillMatch": await extract_value("skillMatch"),
                    "locationMatch": await extract_value("locationMatch"),
                    "entityMatch": await extract_value("entityMatch"),
                    "databaseMatch": await extract_value("databaseMatch"),
                    "sectorMatch": await extract_value("sectorMatch"),
                    "recommendationScore": await extract_value("recommendationScore"),
                    # "reasoning": await extract_value("reasoning"),
                    "skills": await extract_skills_from_output(output_text)
                }

                logger.info(f"Successfully extracted data for profile {i}")
                results.append(result)

            except Exception as e:
                logger.error(f"Error processing output block {i}: {str(e)}")
                logger.warning(
                    f"Problematic output text: {output_text[:200]}...")
                continue

    except Exception as e:
        logger.critical(f"Critical error in extract_score_data: {str(e)}")
        logger.warning(f"Response text preview: {response_text[:200]}...")
        raise

    logger.info(
        f"Completed data extraction with {len(results)} successful results")
    return results


def convert_hyde_details_to_xml(details: Optional[Dict]) -> str:
    """Converts the hyde_analysis_flags dict to an XML string for the prompt."""
    logger.info(f"Converting hyde details to XML")
    if not details:
        return "<hyde_analysis />"  # Return empty tag if no details

    root = ET.Element("hyde_analysis")

    if details.get("locations"):
        # Add operator attribute to the parent tag
        locs_elem = ET.SubElement(
            root, "locations", operator=details.get("location_operator", "AND"))
        for loc in details["locations"]:
            loc_elem = ET.SubElement(locs_elem, "location")
            loc_elem.text = loc

    if details.get("organizations"):
        # Add operator attribute and temporal information to the parent tag
        org_attrs = {"operator": details.get("organization_operator", "AND")}
        if details.get("organization_temporal"):
            org_attrs["temporal"] = details.get("organization_temporal")
        orgs_elem = ET.SubElement(root, "organizations", **org_attrs)
        for org in details["organizations"]:
            org_elem = ET.SubElement(orgs_elem, "organization")
            org_elem.text = org

    if details.get("skills"):
        # Add operator attribute to the parent tag
        skills_elem = ET.SubElement(
            root, "skills", operator=details.get("skill_operator", "AND"))
        for skill in details["skills"]:
            skill_elem = ET.SubElement(skills_elem, "skill")
            skill_elem.text = skill

    # Add database queries section if present - NOW AT TOP LEVEL
    if details.get("db_queries"):
        db_elem = ET.SubElement(
            root, "database_queries", operator=details.get("db_query_operator", "AND"))
        for query in details["db_queries"]:
            query_elem = ET.SubElement(db_elem, "query")

            field_elem = ET.SubElement(query_elem, "field")
            field_elem.text = query.get("field", "")

            desc_elem = ET.SubElement(query_elem, "description")
            desc_elem.text = query.get("description", "")

    # Add sectors section if present (new feature)
    if details.get("sectors"):
        # Add operator attribute and temporal information to the parent tag
        sector_attrs = {"operator": details.get("sector_operator", "OR")}
        if details.get("sector_temporal"):
            sector_attrs["temporal"] = details.get("sector_temporal")
        sectors_elem = ET.SubElement(root, "sectors", **sector_attrs)
        for sector in details["sectors"]:
            sector_elem = ET.SubElement(sectors_elem, "sector")
            sector_elem.text = sector

    # If root has no children, return empty tag, otherwise return string
    if not list(root):
        return "<hyde_analysis />"
    return ET.tostring(root, encoding='unicode', method='xml')


async def process_batch(persons: List[Dict], query: str,
                        fingerprint_mapper: FingerprintMapper, hyde_analysis_flags: dict = None, max_retries: int = 3, reasoning_model: str = "anthropic_haiku") -> List[Dict]:
    """Process a batch of persons and return their rankings."""
    batch_id = str(hash(frozenset([p["nodeId"] for p in persons])))

    try:
        logger.info(
            f"\n[{datetime.now()}] Starting batch {batch_id} with {len(persons)} persons")

        # Convert persons to XML format
        try:
            persons_xml = await convert_persons_to_xml(persons, fingerprint_mapper)
            # Count number of person elements in XML
            person_count = persons_xml.count("<person>")
            logger.info(
                f"[{datetime.now()}] Successfully converted {person_count} persons to XML format")
        except Exception as e:
            logger.error(
                f"[{datetime.now()}] Error converting persons to XML: {str(e)}")
            raise

        # Convert hyde details to XML
        hyde_analysis_xml = convert_hyde_details_to_xml(hyde_analysis_flags)

        current_date = datetime.now().strftime("%Y-%m-%d")
        prompt = message.replace("{{LIST_OF_PERSONS}}", persons_xml)\
                        .replace("{{QUERY}}", query)\
                        .replace("{{HYDE_ANALYSIS_XML}}", hyde_analysis_xml)\
                        .replace("{{CURRENT_DATE}}", current_date)
        for attempt in range(max_retries):
            try:
                logger.info(
                    f"[{datetime.now()}] Making API call for batch {batch_id} (attempt {attempt + 1}/{max_retries}) with {person_count} profiles")
                model = LLMManager()
                # Store input prompt locally for debugging
                # debug_folder = "debug_logs"
                # os.makedirs(debug_folder, exist_ok=True)
                # timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                # async with aiofiles.open(f"{debug_folder}/input_prompt_{batch_id}_{timestamp}.txt", 'w') as f:
                #     await f.write(prompt)

                response = await model.get_completion(
                    provider=reasoning_model,  # Use the provider key from your MODEL_CONFIGS
                    messages=[
                        {"role": "user",
                         "content": prompt},
                    ]
                )
                response_text = response.choices[0].message.content

                # Store LLM response locally for debugging
                # async with aiofiles.open(f"{debug_folder}/llm_response_{batch_id}_{timestamp}.txt", 'w') as f:
                #     await f.write(response_text)

                break
            except Exception as e:
                last_error = e
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt
                    logger.info(
                        f"[{datetime.now()}] Attempt {attempt + 1} failed: {str(e)}. Waiting {wait_time} seconds...")
                    await asyncio.sleep(wait_time)
                else:
                    logger.error(
                        f"[{datetime.now()}] All attempts failed for batch {batch_id}")
                    raise last_error

        if not response_text:
            logger.warning(
                f"[{datetime.now()}] Warning: Empty response received for batch {batch_id}")
            return []

        logger.info(
            f"[{datetime.now()}] Processing response text of length {len(response_text)}")

        # Extract data from response
        results = await extract_score_data(response_text)
        if not results:
            logger.warning(
                f"[{datetime.now()}] Warning: No results extracted from response for batch {batch_id}")
            return []

        # Store extracted results locally for debugging
        # async with aiofiles.open(f"{debug_folder}/extracted_results_{batch_id}_{timestamp}.json", 'w') as f:
        #     await f.write(json.dumps(results, indent=2))

        # Replace fingerprints with original names
        try:
            results = await fingerprint_mapper.replace_fingerprints_in_results(results, persons)
            logger.info(
                f"[{datetime.now()}] Successfully processed {len(results)}/{person_count} profiles for batch {batch_id}")

            # Store final results after fingerprint replacement for debugging
            # async with aiofiles.open(f"{debug_folder}/final_results_{batch_id}_{timestamp}.json", 'w') as f:
            #     await f.write(json.dumps(results, indent=2))

        except Exception as e:
            logger.error(
                f"[{datetime.now()}] Error replacing fingerprints: {str(e)}")
            raise

        # If we got fewer results than input profiles, log a warning
        if len(results) < person_count:
            logger.warning(
                f"[{datetime.now()}] Warning: Missing results for {person_count - len(results)} profiles in batch {batch_id}")

        logger.info(
            f"[{datetime.now()}] Successfully completed batch {batch_id}")
        return results

    except Exception as e:
        logger.critical(
            f"[{datetime.now()}] Critical error in batch {batch_id}: {str(e)}")
        logger.warning(f"[{datetime.now()}] Traceback:")
        traceback.print_exc()

        # Save error information
        # try:
        #     debug_folder = "debug_logs"
        #     os.makedirs(debug_folder, exist_ok=True)

        #     error_info = {
        #         "error": str(e),
        #         "traceback": traceback.format_exc(),
        #         "batch_id": batch_id,
        #         "input_profile_count": len(persons),
        #         "xml_profile_count": person_count if 'person_count' in locals() else None
        #     }

        #     timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        #     async with aiofiles.open(f"{debug_folder}/error_{batch_id}_{timestamp}.json", 'w') as f:
        #         await f.write(json.dumps(error_info, indent=2))
        # except Exception as log_error:
        #     logger.error(
        #         f"[{datetime.now()}] Failed to save error information: {str(log_error)}")

        return []


async def chunk_list(lst: List, chunk_size: int) -> List[List]:
    """Split a list into chunks of specified size."""
    return [lst[i:i + chunk_size] for i in range(0, len(lst), chunk_size)]


async def process_people_direct(transformed_people: List[Dict], query: str, reasoning_model: str = "anthropic_haiku",
                                hyde_analysis_flags: dict = None, batch_size: int = 5, max_concurrent_tasks: int = 5) -> List[Dict]:
    try:
        start_time = time.time()
        logger.info(
            f"[{datetime.now()}] Starting direct processing of {len(transformed_people)} people")

        fingerprint_mapper = FingerprintMapper()
        # Store transformed_people as JSON for debugging
        # try:
        #     debug_folder = "debug_logs"
        #     os.makedirs(debug_folder, exist_ok=True)

        #     timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        #     debug_file_path = os.path.join(
        #         debug_folder, f"transformed_people_{timestamp}.json")
        #     hyde_analysis_flags_path = os.path.join(
        #         debug_folder, f"hyde_analysis_flags_{timestamp}.json")

        #     async with aiofiles.open(debug_file_path, 'w') as f:
        #         await f.write(json.dumps(transformed_people, indent=2))

        #     async with aiofiles.open(hyde_analysis_flags_path, 'w') as f:
        #         await f.write(json.dumps(hyde_analysis_flags, indent=2))

        #     logger.info(
        #         f"[{datetime.now()}] Saved {len(transformed_people)} transformed people to {debug_file_path}")
        # except Exception as e:
        #     logger.error(
        #         f"[{datetime.now()}] Failed to save transformed people: {str(e)}")

        # # Create batches
        batches = await chunk_list(transformed_people, batch_size)
        logger.info(f"[{datetime.now()}] Created {len(batches)} batches")

        # Process batches with concurrency control and retries
        sem = asyncio.Semaphore(max_concurrent_tasks)

        async def process_batch_with_semaphore(batch):
            async with sem:
                try:
                    result = await process_batch(batch, query, fingerprint_mapper, hyde_analysis_flags=hyde_analysis_flags, max_retries=3, reasoning_model=reasoning_model)
                    if not result:
                        logger.warning(
                            f"[{datetime.now()}] Warning: Empty result for batch with {len(batch)} people")
                    return result
                except Exception as e:
                    logger.error(
                        f"[{datetime.now()}] Error processing batch: {str(e)}")
                    traceback.print_exc()
                    return []

        tasks = [process_batch_with_semaphore(batch) for batch in batches]
        batch_results = await asyncio.gather(*tasks)

        # Collect and log results
        results = []
        for i, batch_result in enumerate(batch_results):
            results.extend(batch_result)
            logger.info(
                f"[{datetime.now()}] Batch {i+1}/{len(batches)} returned {len(batch_result)} results")

        logger.info(
            f"[{datetime.now()}] Total results: {len(results)} out of {len(transformed_people)} input people")

        # Store final aggregated results for debugging
        final_results = sorted(results, key=lambda x: x.get(
            'recommendationScore', 0), reverse=True)
        # try:
        #     final_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        #     async with aiofiles.open(f"debug_logs/final_aggregated_results_{final_timestamp}.json", 'w') as f:
        #         await f.write(json.dumps(final_results, indent=2))
        #     logger.info(
        #         f"[{datetime.now()}] Saved final aggregated results to debug_logs/final_aggregated_results_{final_timestamp}.json")
        # except Exception as e:
        #     logger.error(
        #         f"[{datetime.now()}] Failed to save final aggregated results: {str(e)}")

        return final_results

    except Exception as e:
        logger.critical(
            f"[{datetime.now()}] Critical error in process_people_direct: {str(e)}")
        traceback.print_exc()
        return []
    finally:
        await cleanup_old_debug_logs()


async def process_people(json_file_path: str, query: str, reasoning_model: str = "anthropic_haiku",
                         hyde_analysis_flags: dict = None, batch_size: int = 5, max_concurrent_tasks: int = 5, max_retries: int = 3) -> List[Dict]:
    """Process all people in batches with asyncio tasks."""
    try:
        start_time = time.time()
        logger.info(f"[{datetime.now()}] Starting processing...")

        async with aiofiles.open(json_file_path, 'r') as f:
            data = json.loads(await f.read())

        fingerprint_mapper = FingerprintMapper()

        # people_to_process = data.get("result", [])
        people_to_process = data.get("result", [])[:5]
        total_people = len(people_to_process)

        batches = await chunk_list(people_to_process, batch_size)

        # Process batches with asyncio.gather and semaphore for concurrency control
        sem = asyncio.Semaphore(max_concurrent_tasks)

        async def process_batch_with_semaphore(batch):
            async with sem:
                return await process_batch(batch, query, fingerprint_mapper, hyde_analysis_flags=hyde_analysis_flags, max_retries=max_retries, reasoning_model=reasoning_model)

        tasks = [process_batch_with_semaphore(batch) for batch in batches]
        batch_results = await asyncio.gather(*tasks, return_exceptions=True)

        results = []
        for batch_result in batch_results:
            if isinstance(batch_result, Exception):
                logger.error(f"Batch processing failed: {str(batch_result)}")
                continue
            results.extend(batch_result)

        return sorted(results, key=lambda x: x.get('recommendationScore', 0), reverse=True)

    finally:
        # Cleanup old debug logs if needed
        await cleanup_old_debug_logs()


async def cleanup_old_debug_logs(max_age_days: int = 7):
    """Clean up debug logs older than specified days."""
    debug_folder = "debug_logs"
    if not os.path.exists(debug_folder):
        return

    current_time = time.time()
    for filename in os.listdir(debug_folder):
        filepath = os.path.join(debug_folder, filename)
        if os.path.getmtime(filepath) < current_time - (max_age_days * 86400):
            try:
                os.remove(filepath)
            except Exception as e:
                logger.error(f"Failed to remove old log file {filepath}: {e}")

if __name__ == "__main__":
    async def main():
        # Record start time
        start_time = time.time()

        # Process all people with the query
        results = await process_people_direct(
            'debug_logs/transformed_people_20250612_194707.json',
            'graduating this year from bits or iit',
            reasoning_model="gemini",
            hyde_analysis_flags={
                "locations": [],
                "location_operator": "OR",
                "organizations": [],
                "organization_operator": "OR",
                "skills": [],
                "skill_operator": "OR",
                "db_queries": [
                    {
                        "field": "education.dates",
                        "regex": ".*2025.*",
                        "description": "Graduating this year (2025)"
                    },
                    {
                        "field": "education.school",
                        "regex": "(?i)\\bBITS\\b|\\bBirla Institute of Technology & Science\\b|\\bIIT\\b|\\bIndian Institute of Technology\\b",
                        "description": "From BITS or IIT"
                    }
                ],
                "db_query_operator": "AND"
            },
            batch_size=5,
            max_concurrent_tasks=10
        )

        # Calculate total time taken
        total_time = time.time() - start_time

        # Save sorted results
        async with aiofiles.open('rerank_results_new_gemini.json', 'w') as f:
            await f.write(json.dumps(results, indent=2))

        logger.info(f"Processed {len(results)} people")
        logger.info("Results saved to rerank_results_new_gemini.json")
        logger.info(
            f"Total time taken: {total_time:.2f} seconds ({total_time/60:.2f} minutes)")

    # Run the async main function
    asyncio.run(main())
