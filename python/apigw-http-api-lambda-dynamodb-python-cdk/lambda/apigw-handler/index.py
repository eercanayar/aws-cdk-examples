# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import boto3
import os
import json
import logging
import uuid
from datetime import datetime

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s'
)
logger = logging.getLogger()

dynamodb_client = boto3.client("dynamodb")


def handler(event, context):
    # Extract security-relevant context
    request_id = context.aws_request_id
    source_ip = event.get('requestContext', {}).get('identity', {}).get('sourceIp', 'unknown')
    user_agent = event.get('requestContext', {}).get('identity', {}).get('userAgent', 'unknown')
    request_time = event.get('requestContext', {}).get('requestTime', datetime.utcnow().isoformat())
    
    table = os.environ.get("TABLE_NAME")
    
    # Log security context for audit trail
    security_context = {
        'request_id': request_id,
        'source_ip': source_ip,
        'user_agent': user_agent,
        'request_time': request_time,
        'event_type': 'api_request',
        'table_name': table,
        'function_name': context.function_name
    }
    
    logger.info(json.dumps({
        **security_context,
        'message': 'Request received',
        'has_payload': bool(event.get("body"))
    }))
    
    try:
        if event["body"]:
            item = json.loads(event["body"])
            logger.info(json.dumps({
                **security_context,
                'message': 'Processing request with payload',
                'payload_keys': list(item.keys()) if isinstance(item, dict) else 'non_dict_payload'
            }))
            
            year = str(item["year"])
            title = str(item["title"])
            id = str(item["id"])
            
            dynamodb_client.put_item(
                TableName=table,
                Item={"year": {"N": year}, "title": {"S": title}, "id": {"S": id}},
            )
            
            logger.info(json.dumps({
                **security_context,
                'message': 'Data inserted successfully',
                'operation': 'put_item',
                'item_id': id
            }))
            
        else:
            logger.info(json.dumps({
                **security_context,
                'message': 'Processing request without payload - using default data'
            }))
            
            default_id = str(uuid.uuid4())
            dynamodb_client.put_item(
                TableName=table,
                Item={
                    "year": {"N": "2012"},
                    "title": {"S": "The Amazing Spider-Man 2"},
                    "id": {"S": default_id},
                },
            )
            
            logger.info(json.dumps({
                **security_context,
                'message': 'Default data inserted successfully',
                'operation': 'put_item',
                'item_id': default_id
            }))
        
        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"message": "Successfully inserted data!"}),
        }
        
    except Exception as e:
        logger.error(json.dumps({
            **security_context,
            'message': 'Error processing request',
            'error': str(e),
            'error_type': type(e).__name__
        }))
        
        return {
            "statusCode": 500,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"message": "Internal server error"}),
        }