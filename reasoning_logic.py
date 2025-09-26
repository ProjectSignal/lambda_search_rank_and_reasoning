# right side bar insights v15
import traceback
from datetime import datetime
from api_client import get_node_document, SearchServiceError
from ranking import convert_hyde_details_to_xml
from jsonToXml import json_to_xml
from prompts.sidebar_reasoning import message as search_reasoning_prompt, prefill, stop_sequences
from llm_helper import LLMManager
from logging_config import setup_logger

logger = setup_logger(__name__)
from typing import Dict, Any, List
import xml.etree.ElementTree as ET
import re
import json

import asyncio

class SearchReasoningParser:
    @staticmethod
    def extract_between_tags(text: str, start_tag: str = "<output>", end_tag: str = "</output>") -> str:
        """Extract content between specified tags"""
        try:
            # Find the last instance of output tags (in case there are multiple)
            matches = list(re.finditer(
                f'{start_tag}(.*?){end_tag}', text, re.DOTALL))
            if matches:
                return matches[-1].group(1).strip()
            return text.strip()
        except Exception as e:
            logger.error(f"Error extracting content: {str(e)}")
            raise ValueError(
                f"Could not extract content between tags: {str(e)}")

    @staticmethod
    def parse_insight(insight_text: str) -> dict:
        """Parse a single insight block into a dictionary"""
        try:
            result = {}
            # Find all direct child tags
            tag_matches = re.finditer(
                r'<(\w+)>(.*?)</\1>', insight_text, re.DOTALL)
            for match in tag_matches:
                tag_name, tag_content = match.groups()
                result[tag_name] = tag_content.strip()
            return result
        except Exception as e:
            logger.error(f"Error parsing insight: {str(e)}")
            return {}

    @staticmethod
    def parse_role_indicator(indicator_text: str) -> dict:
        """Parse a single role indicator block into a dictionary"""
        try:
            result = {
                "title": "",
                "rating": "",
                "keyPoints": []
            }

            # Parse title
            title_match = re.search(
                r'<title>(.*?)</title>', indicator_text, re.DOTALL)
            if title_match:
                result["title"] = title_match.group(1).strip()

            # Parse rating
            rating_match = re.search(
                r'<rating>(.*?)</rating>', indicator_text, re.DOTALL)
            if rating_match:
                result["rating"] = rating_match.group(1).strip()

            # Parse keyPoints
            key_points_match = re.search(
                r'<keyPoints>(.*?)</keyPoints>', indicator_text, re.DOTALL)
            if key_points_match:
                points = re.finditer(
                    r'<point>(.*?)</point>', key_points_match.group(1), re.DOTALL)
                result["keyPoints"] = [point.group(
                    1).strip() for point in points]

            return result
        except Exception as e:
            logger.error(f"Error parsing role indicator: {str(e)}")
            return {
                "title": "",
                "rating": "",
                "keyPoints": []
            }

    @staticmethod
    def parse_metadata(metadata_content: str) -> dict:
        """Parse metadata section"""
        try:
            metadata = {
                "roleFitIndicator": None,
                "roleIndicators": []
            }

            # Parse roleFitIndicator
            role_fit_match = re.search(
                r'<roleFitIndicator>(.*?)</roleFitIndicator>', metadata_content)
            if role_fit_match:
                metadata["roleFitIndicator"] = role_fit_match.group(1).strip()

            # Parse roleIndicators
            role_indicators_match = re.search(
                r'<roleIndicators>(.*?)</roleIndicators>', metadata_content, re.DOTALL)
            if role_indicators_match:
                indicators = re.finditer(
                    r'<indicator>(.*?)</indicator>', role_indicators_match.group(1), re.DOTALL)
                for indicator in indicators:
                    parsed_indicator = SearchReasoningParser.parse_role_indicator(
                        indicator.group(1))
                    if parsed_indicator:
                        metadata["roleIndicators"].append(parsed_indicator)

            return metadata
        except Exception as e:
            logger.error(f"Error parsing metadata: {str(e)}")
            return {
                "roleFitIndicator": None,
                "roleIndicators": []
            }

    @staticmethod
    def parse_explanation(explanation_content: str) -> str:
        """Parse and clean explanation text"""
        try:
            # Clean up whitespace while preserving sentence structure
            cleaned = explanation_content.strip()
            # Replace multiple whitespace with single space
            cleaned = re.sub(r'\s+', ' ', cleaned)
            return cleaned
        except Exception as e:
            logger.error(f"Error parsing explanation: {str(e)}")
            return ""

    def parse_output(self, content: str) -> Dict[str, Any]:
        """Parse the entire output content into a structured dictionary"""
        try:
            # Extract content between output tags
            output_content = self.extract_between_tags(
                content, "<output>", "</output>")
            if not output_content:
                raise ValueError("No content to parse after extraction")

            # Initialize result structure
            result = {
                "insights": [],
                "metadata": {
                    "roleFitIndicator": None,
                    "roleIndicators": []
                }
            }

            # Extract major sections using regex
            insights_match = re.search(
                r'<insights>(.*?)</insights>', output_content, re.DOTALL)

            # Process insights
            if insights_match:
                insights = re.finditer(
                    r'<insight>(.*?)</insight>', insights_match.group(1), re.DOTALL)
                for insight in insights:
                    parsed_insight = self.parse_insight(insight.group(1))
                    if parsed_insight:
                        result["insights"].append(parsed_insight)

            # Process metadata directly from output content since roleIndicators is at root level
            result["metadata"] = self.parse_metadata(output_content)

            logger.debug(
                f"Successfully parsed output with {len(result['insights'])} insights")
            return result

        except Exception as e:
            logger.error(f"Error in parse_output: {str(e)}")
            return {
                "insights": [],
                "metadata": {
                    "roleFitIndicator": None,
                    "roleIndicators": []
                }
            }


class SearchReasoning:
    def __init__(self, max_concurrent_calls: int = 5):
        self.llm = LLMManager()
        self.parser = SearchReasoningParser()
        self.max_concurrent_calls = max_concurrent_calls
        self.semaphore = None  # Will be initialized in batch_analyze_profiles
        logger.info(
            f"Initialized SearchReasoning with max_concurrent_calls: {max_concurrent_calls}")

    async def analyze_profile(self, profile_xml: str, query: str, model: str = "groq_deepseek", hyde_analysis_flags: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Analyze profile using LiteLLM and search reasoning prompt

        Args:
            profile_xml (str): The XML representation of the profile
            query (str): The search query
            model (str): The model to use for generation. Defaults to "groq_deepseek"
            hyde_analysis_flags (Dict[str, Any]): Pre-analyzed query criteria from Hyde
        """
        try:
            logger.info(f"Starting profile analysis using model: {model}")

            # Convert hyde analysis flags to XML if provided
            hyde_analysis_xml = convert_hyde_details_to_xml(
                hyde_analysis_flags) if hyde_analysis_flags else "<hyde_analysis />"

            # Replace placeholders in prompt with actual values
            current_date = datetime.now().strftime("%Y-%m-%d")
            prompt = search_reasoning_prompt.replace("{{PROFILE_XML}}", profile_xml).replace(
                "{{QUERY}}", query).replace("{{HYDE_ANALYSIS_XML}}", hyde_analysis_xml).replace("{{CURRENT_DATE}}", current_date)

            # Store prompt locally for debugging
            # try:
            #     with open(f'search_reasoning_prompt_debug_{datetime.now().strftime("%Y%m%d_%H%M%S")}.txt', 'w') as f:
            #         f.write(prompt)
            #     logger.debug(
            #         "Saved debug prompt to search_reasoning_prompt_debug.txt")
            # except Exception as e:
            #     logger.error(f"Failed to write debug prompt file: {str(e)}")

            # Call LLM using config
            response = await self.llm.get_completion(
                provider=model,
                messages=[
                    {"role": "user", "content": prompt},
                    {"role": "assistant", "content": prefill}
                ],
                stop=stop_sequences,
            )

            # Parse the response
            output_text = response.choices[0].message.content + \
                stop_sequences[0]
            result = self.parser.parse_output(output_text)
            logger.info(
                f"Successfully completed profile analysis with model: {model}")
            return result

        except Exception as e:
            logger.error(
                f"Error analyzing profile with model {model}: {str(e)}")
            raise

    async def process_single_node(self, node: Dict[str, Any], query: str, model: str = "gemini", hyde_analysis_flags: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Process a single node with semaphore control

        Args:
            node (Dict[str, Any]): The node data to process
            query (str): The search query
            model (str): The model to use for generation. Defaults to "groq_deepseek"
            hyde_analysis_flags (Dict[str, Any]): Pre-analyzed query criteria from Hyde
        """
        async with self.semaphore:
            try:
                node_id = node.get('nodeId')
                if not node_id:
                    logger.error("Missing nodeId in node data")
                    return {'error': 'Missing nodeId'}

                # Fetch node data from API
                try:
                    node_data = get_node_document(node_id)
                except SearchServiceError as exc:
                    logger.error("Node fetch failed for %s: %s", node_id, exc)
                    node_data = None

                if not node_data:
                    logger.warning(f"Node not found in data service: {node_id}")
                    return {
                        'nodeId': node_id,
                        'error': 'Node not found in data service'
                    }

                # Convert node data to XML
                try:
                    node_xml = json_to_xml(node_data)
                except Exception as xml_error:
                    logger.error(
                        f"XML conversion error for node {node_id}: {str(xml_error)}")
                    return {
                        'nodeId': node_id,
                        'error': f'XML conversion error: {str(xml_error)}'
                    }

                # Analyze profile with specified model and hyde analysis
                analysis_result = await self.analyze_profile(node_xml, query, model, hyde_analysis_flags)
                analysis_result['nodeId'] = node_id
                return analysis_result

            except Exception as e:
                logger.error(
                    f"Unexpected error processing node {node.get('nodeId')}: {str(e)}")
                return {
                    'nodeId': node.get('nodeId'),
                    'error': f'Unexpected error: {str(e)}'
                }

    async def batch_analyze_profiles(self, nodes: List[Dict[str, Any]], query: str, model: str = "gemini", hyde_analysis_flags: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        """
        Process multiple nodes concurrently with controlled parallelism

        Args:
            nodes (List[Dict[str, Any]]): List of nodes to process
            query (str): The search query
            model (str): The model to use for generation. Defaults to "groq_deepseek"
            hyde_analysis_flags (Dict[str, Any]): Pre-analyzed query criteria from Hyde
        """
        try:
            logger.info(
                f"Starting batch analysis of {len(nodes)} nodes using model: {model}")
            self.semaphore = asyncio.Semaphore(self.max_concurrent_calls)

            # Create tasks for all nodes with the specified model and hyde analysis
            tasks = [self.process_single_node(
                node, query, model, hyde_analysis_flags) for node in nodes]

            # Execute tasks concurrently and gather results
            results = await asyncio.gather(*tasks)
            logger.info(
                f"Completed batch analysis of {len(results)} nodes using model: {model}")
            return results
        except Exception as e:
            logger.error(
                f"Error in batch processing with model {model}: {str(e)}")
            raise
