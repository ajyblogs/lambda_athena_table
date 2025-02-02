import json
import boto3
import logging
import traceback
import os
from typing import Dict, List
from string import Template
from botocore.exceptions import ClientError

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
glue_client = boto3.client('glue')

# Get configuration from environment variables
DATABASE = os.environ['DATABASE_NAME']
DATA_BUCKET = os.environ['DATA_BUCKET']

def read_table_configs() -> List[Dict]:
    """Read table configurations from the JSON file."""
    try:
        with open('table_configs.json', 'r') as file:
            return json.load(file)['tables']
    except Exception as e:
        logger.error(f"Error reading table configurations: {str(e)}")
        raise

def parse_column_definitions(columns_config: List[Dict]) -> List[Dict]:
    """Convert column configurations to Glue column definitions."""
    return [
        {
            'Name': col['name'],
            'Type': col['type']
        }
        for col in columns_config
    ]

def create_table(table_config: Dict) -> bool:
    """Create a table using Glue CreateTable API."""
    try:
        table_input = {
            'Name': table_config['name'],
            'DatabaseName': DATABASE,
            'Description': table_config.get('description', ''),
            'TableType': 'EXTERNAL_TABLE',
            'StorageDescriptor': {
                'Columns': parse_column_definitions(table_config['columns']),
                'Location': f"s3://{DATA_BUCKET}/{table_config['location']}"
            }
        }

        glue_client.create_table(
            DatabaseName=DATABASE,
            TableInput=table_input
        )
        
        logger.info(f"Successfully created table: {table_config['name']}")
        return True
        
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code')
        if error_code == 'AlreadyExistsException':
            logger.warning(f"Table {table_config['name']} already exists")
            return True
        else:
            logger.error(f"Error creating table {table_config['name']}: {str(e)}")
            return False
    except Exception as e:
        logger.error(f"Unexpected error creating table {table_config['name']}: {str(e)}")
        return False

def create_tables(table_configs: List[Dict]) -> Dict:
    """Create Glue tables based on configurations."""
    success_count = 0
    failed_tables = []
    
    for config in table_configs:
        try:
            if create_table(config):
                success_count += 1
            else:
                failed_tables.append(config['name'])
        except Exception as e:
            failed_tables.append(config['name'])
            logger.error(f"Error processing table {config['name']}: {str(e)}")
            continue
    
    return {
        'success_count': success_count,
        'failed_tables': failed_tables,
        'total_tables': len(table_configs)
    }

def validate_env_vars() -> None:
    """Validate that all required environment variables are set."""
    required_vars = ['DATABASE_NAME', 'DATA_BUCKET']
    missing_vars = [var for var in required_vars if not os.environ.get(var)]
    
    if missing_vars:
        raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")

def lambda_handler(event: Dict, context) -> Dict:
    """Lambda handler function."""
    try:
        # Validate environment variables
        validate_env_vars()
        
        logger.info("Starting table creation process")
        logger.info(f"Using Database: {DATABASE}")
        logger.info(f"Data Bucket: {DATA_BUCKET}")
        
        # Read table configurations
        table_configs = read_table_configs()
        
        # Create tables
        result = create_tables(table_configs)
        
        return {
            'statusCode': 200,
            'body': {
                'message': f"Processed {result['total_tables']} tables. {result['success_count']} succeeded, {len(result['failed_tables'])} failed",
                'details': result,
            }
        }
            
    except Exception as e:
        error_msg = str(e)
        logger.error(f"Error: {error_msg}")
        logger.error(traceback.format_exc())
        return {
            'statusCode': 500,
            'body': {
                'error': error_msg
            }
        }
