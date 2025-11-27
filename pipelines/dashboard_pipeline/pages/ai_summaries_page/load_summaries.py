"""
Data loading module for AI summaries from S3.
Fetches and formats AI-generated energy summaries with caching.
"""
# pylint: disable = W1203

import json
import logging
from datetime import datetime
from typing import Dict, List, Optional
import boto3
from botocore.exceptions import ClientError
import streamlit as st

logger = logging.getLogger(__name__)

# Hardcoded S3 bucket name
S3_BUCKET_NAME = "c20-power-monitor-s3"


@st.cache_data(ttl=300)  # Cache for 5 minutes
def get_latest_summary() -> Optional[Dict]:
    """
    Fetch the most recent AI summary from S3.
    
    Returns:
        Dict: Latest summary data with timestamp, summary text, and metadata.
        None: If fetch fails or no summary exists.
    """
    try:
        s3_client = boto3.client('s3')

        response = s3_client.get_object(
            Bucket=S3_BUCKET_NAME,
            Key='summaries/summary-latest.json'
        )

        summary_data = json.loads(response['Body'].read().decode('utf-8'))
        logger.info("Latest summary fetched successfully")
        return summary_data

    except ClientError as e:
        logger.error(f"Failed to fetch latest summary: {e}")
        return None


@st.cache_data(ttl=300)  # Cache for 5 minutes
def list_all_summaries(max_summaries: int = 20) -> List[Dict]:
    """
    List all available summaries from S3, sorted by most recent first.
    
    Args:
        max_summaries (int): Maximum number of summaries to retrieve.
        
    Returns:
        List[Dict]: List of summary metadata (timestamp, s3_key).
    """
    try:
        s3_client = boto3.client('s3')

        # Remove MaxKeys to fetch ALL summaries, then slice after sorting
        response = s3_client.list_objects_v2(
            Bucket=S3_BUCKET_NAME,
            Prefix='summaries/summary-'
        )

        summaries = []
        for obj in response.get('Contents', []):
            key = obj['Key']

            # Skip the latest.json file
            if key == 'summaries/summary-latest.json':
                continue

            # Extract timestamp from filename: summary-2025-11-25-11-24-36.json
            filename = key.split('/')[-1]
            timestamp_str = filename.replace(
                'summary-', '').replace('.json', '')

            try:
                timestamp = datetime.strptime(
                    timestamp_str, '%Y-%m-%d-%H-%M-%S')
                summaries.append({
                    'timestamp': timestamp,
                    's3_key': key,
                    'last_modified': obj['LastModified']
                })
            except ValueError:
                logger.warning(f"Could not parse timestamp from {filename}")
                continue

        # Sort by timestamp, most recent first
        summaries.sort(key=lambda x: x['timestamp'], reverse=True)

        logger.info(f"Found {len(summaries)} summaries")
        return summaries[:max_summaries]  # Slice AFTER sorting

    except ClientError as e:
        logger.error(f"Failed to list summaries: {e}")
        return []


@st.cache_data(ttl=300)
def get_summary_by_key(s3_key: str) -> Optional[Dict]:
    """
    Fetch a specific summary from S3 by its key.
    
    Args:
        s3_key (str): S3 object key for the summary.
        
    Returns:
        Dict: Summary data.
        None: If fetch fails.
    """
    try:
        s3_client = boto3.client('s3')

        response = s3_client.get_object(
            Bucket=S3_BUCKET_NAME,
            Key=s3_key
        )

        summary_data = json.loads(response['Body'].read().decode('utf-8'))
        return summary_data

    except ClientError as e:
        logger.error(f"Failed to fetch summary {s3_key}: {e}")
        return None


def format_timestamp(timestamp: datetime) -> str:
    """
    Format timestamp for display.
    
    Args:
        timestamp (datetime): Timestamp to format.
        
    Returns:
        str: Formatted timestamp string.
    """
    return timestamp.strftime('%d %B %Y, %H:%M UTC')
