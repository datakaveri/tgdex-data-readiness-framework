import os
import boto3
import zipfile
import logging
import tempfile
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
    try:
        bucket = event['bucket']
        prefix = event['prefix']
        with tempfile.TemporaryDirectory() as temp_dir:
            # Download files from S3
            for obj in s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix).get('Contents', []):
                if obj['Key'].endswith('/'):
                    continue
                local_path = os.path.join(temp_dir, os.path.basename(obj['Key']))
                s3_client.download_file(bucket, obj['Key'], local_path)
                # Unzip if required
                if local_path.endswith('.zip'):
                    with zipfile.ZipFile(local_path, 'r') as zf:
                        zf.extractall(temp_dir)
            # Run your framework
            main(temp_dir)
            # Upload reports to S3
            for root, _, files in os.walk(temp_dir):
                for f in files:
                    if f.endswith(('.json', '.pdf')):
                        s3_client.upload_file(os.path.join(root, f), bucket, f"reports/{os.path.basename(root)}/{f}")
        return {"status": "done"}
    except Exception as e:
        logger.error(f"Error occurred while processing files: {e}")
        return {"status": "error", "error": str(e)}

