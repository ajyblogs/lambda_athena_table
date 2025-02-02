import json
import boto3
import logging
import traceback
import os
from typing import Dict, List
from string import Template

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize Athena client
athena_client = boto3.client('athena')

# Get configuration from environment variables
DATABASE = os.environ['DATABASE_NAME']
DATA_BUCKET = os.environ['DATA_BUCKET']
RESULTS_BUCKET = os.environ['RESULTS_BUCKET']

def read_table_configs() -> List[Dict]:
    """Read table configurations from the JSON file."""
    try:
        with open('table_configs.json', 'r') as file:
            return json.load(file)['tables']
    except Exception as e:
        logger.error(f"Error reading table configurations: {str(e)}")
        raise

def execute_query(query: str, database: str) -> bool:
    """Execute Athena query and return success status."""
    try:
        response = athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={
                'Database': database
            },
            ResultConfiguration={
                'OutputLocation': f's3://{RESULTS_BUCKET}/athena-query-results/'
            }
        )
        logger.info(f"Successfully executed query: {query}")
        return True
    except Exception as e:
        logger.error(f"Error executing query: {str(e)}")
        return False

def create_tables(table_configs: List[Dict]) -> Dict:
    """Create Athena tables based on configurations."""
    success_count = 0
    failed_tables = []
    
    for config in table_configs:
        try:
            # Replace placeholders in the query with actual values
            template = Template(config['query'])
            query = template.safe_substitute(
                DATA_BUCKET=DATA_BUCKET
            )
            
            if execute_query(query, DATABASE):
                success_count += 1
                logger.info(f"Successfully created table: {config['name']}")
            else:
                failed_tables.append(config['name'])
                logger.error(f"Failed to create table: {config['name']}")
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
    required_vars = ['DATABASE_NAME', 'DATA_BUCKET', 'RESULTS_BUCKET']
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
        logger.info(f"Results Bucket: {RESULTS_BUCKET}")
        
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
