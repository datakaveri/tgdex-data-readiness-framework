import os
import boto3
import zipfile
import logging
import tempfile
import time
import json
from main import main  

# Configure logging
logging.getLogger().setLevel(logging.INFO)
logger = logging.getLogger()

# Configure formatter for more detailed logs
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
for handler in logger.handlers:
    handler.setFormatter(formatter)

# Initialize AWS clients
s3_client = boto3.client('s3')

def lambda_handler(event, context):
    """
    Lambda function handler to run data readiness framework on files in an S3 bucket.
    
    Parameters
    ----------
    event : dict
        Lambda event object with the following keys:
            bucket: str
                The name of the S3 bucket containing the files to process.
            prefix: str
                The prefix of the files to process within the given bucket.
    context : object
        Lambda context object.
    
    Returns
    -------
    dict
        A dictionary with a single key "status" which maps to "done".
    """
    start_time = time.time()
    request_id = context.aws_request_id if context else 'local-run'
    logger.info(f"Lambda invocation started - RequestID: {request_id}")
    try:
        # Log the incoming event
        logger.info(f"Received event: {json.dumps(event)}")
        
        # Extract S3 folder key from request body
        body = json.loads(event.get('body', '{}'))
        folder_key = body.get('folder_key')
        logger.info(f"Extracted folder_key: {folder_key}")
        
        if not folder_key:
            logger.error("Missing required parameter: folder_key")
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'Missing folder_key parameter'})
            }
        
        # Get bucket name from environment variable
        bucket_name = os.environ.get('S3_BUCKET_NAME')
        if not bucket_name:
            logger.error("Missing required environment variable: S3_BUCKET_NAME")
            return {
                'statusCode': 500,
                'body': json.dumps({'error': 'Server configuration error: Missing S3_BUCKET_NAME'})
            }
        
        logger.info(f"Using bucket: {bucket_name}")
        
        # Check if the folder exists in S3
        logger.info(f"Checking if folder exists: {folder_key}")
        try:
            # List objects with the given prefix and limit to 1 result
            response = s3_client.list_objects_v2(
                Bucket=bucket_name,
                Prefix=folder_key,
                MaxKeys=1
            )
            
            # If no contents or the only item is the folder itself (ends with /)
            if 'Contents' not in response or (
                len(response['Contents']) == 1 and 
                response['Contents'][0]['Key'] == folder_key and 
                folder_key.endswith('/')
            ):
                logger.error(f"Folder not found or empty: {folder_key}")
                return {
                    'statusCode': 404,
                    'body': json.dumps({
                        'error': 'Folder not found or empty',
                        'folder_key': folder_key
                    })
                }
                
            logger.info(f"Folder exists: {folder_key}")
        except Exception as e:
            logger.error(f"Error checking folder existence: {str(e)}", exc_info=True)
            return {
                'statusCode': 500,
                'body': json.dumps({
                    'error': 'Error checking folder existence',
                    'error_message': str(e)
                })
            }
        
        
        with tempfile.TemporaryDirectory() as temp_dir:
            # Download files from S3
            for obj in s3_client.list_objects_v2(Bucket=bucket_name, Prefix=folder_key).get('Contents', []):
                if obj['Key'].endswith('/'):
                    continue
                local_path = os.path.join(temp_dir, os.path.basename(obj['Key']))
                s3_client.download_file(bucket_name, obj['Key'], local_path)
                # Unzip if required
                if local_path.endswith('.zip'):
                    with zipfile.ZipFile(local_path, 'r') as zf:
                        zf.extractall(temp_dir)
            # Run your framework
            main(temp_dir)
            # Upload reports to S3
            for root, _, files in os.walk(temp_dir):
                for f in files:
                    if f.endswith('.json'):
                        report_key = f"data-readiness-reports/{os.path.basename(root)}/{f}"
                        s3_client.upload_file(os.path.join(root, f), bucket_name, report_key)
        
        
        end_time = time.time()
        logger.info(f"Lambda invocation completed - RequestID: {request_id}")
        logger.info(f"Total execution time: {end_time - start_time:.2f} seconds")
        return {"status": "done"}
    except Exception as e:
        logger.error(f"Error processing request: {str(e)}", exc_info=True)
        total_duration = time.time() - start_time
        logger.info(f"Lambda execution failed after {total_duration:.2f} seconds")
        
        error_response = {
            'statusCode': 500,
            'body': json.dumps({
                'error': 'Internal server error',
                'error_message': str(e),
                'request_id': request_id
            })
        }

