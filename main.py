import json
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
import boto3
from botocore.exceptions import ClientError, NoCredentialsError

# --- Pydantic Models for API Request Body ---
# This defines the structure and validates the input you send to the API.
class S3Config(BaseModel):
    aws_access_key_id: str = Field(..., example="YOUR_AWS_ACCESS_KEY_ID")
    aws_secret_access_key: str = Field(..., example="YOUR_AWS_SECRET_ACCESS_KEY")
    s3_bucket_name: str = Field(..., example="your-airbyte-output-bucket")
    s3_bucket_path: str = Field(..., example="vapormedia", description="The base path inside the bucket where stream folders are located.")

# --- FastAPI Application ---
app = FastAPI(
    title="S3 JSONL Transformation Service",
    description="An API to fetch Airbyte JSONL outputs from S3, transform them into a single JSON object."
)

def fetch_and_transform_from_s3(config: S3Config) -> dict:
    """
    Connects to S3, finds stream directories, reads all jsonl files,
    and consolidates them into a single dictionary.
    """
    print("Initializing S3 client...")
    try:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=config.aws_access_key_id,
            aws_secret_access_key=config.aws_secret_access_key
            # region_name="us-east-1"
        )
        # Use list_objects_v2 to find top-level "directories" which are the streams
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=config.s3_bucket_name, Prefix=f"{config.s3_bucket_path}/", Delimiter='/')
        
        # This is the corrected line:
        stream_prefixes = []
        
        for page in pages:
            if "CommonPrefixes" in page:
                for obj in page['CommonPrefixes']:
                    stream_prefixes.append(obj['Prefix'])

    except NoCredentialsError:
        raise HTTPException(status_code=401, detail="AWS credentials not found or invalid.")
    except ClientError as e:
        # Handle cases like bucket not found
        if e.response['Error']['Code'] == 'NoSuchBucket':
            raise HTTPException(status_code=404, detail=f"S3 bucket '{config.s3_bucket_name}' not found.")
        raise HTTPException(status_code=500, detail=f"An S3 client error occurred: {e}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred during S3 connection: {e}")

    if not stream_prefixes:
        raise HTTPException(status_code=404, detail=f"No stream directories found under the path '{config.s3_bucket_path}/' in bucket '{config.s3_bucket_name}'.")

    consolidated_data = {}
    print(f"Found stream directories: {[prefix.split('/')[-2] for prefix in stream_prefixes]}")

    # Iterate through each stream directory
    for prefix in stream_prefixes:
        stream_name = prefix.split('/')[-2] # Extract stream name (e.g., 'commits')
        consolidated_data[stream_name] = []
        
        print(f"Processing stream: {stream_name}")
        
        # Find all objects within that stream's prefix
        objects_in_stream = s3_client.list_objects_v2(Bucket=config.s3_bucket_name, Prefix=prefix)
        
        if 'Contents' not in objects_in_stream:
            print(f"  - No files found in '{prefix}'. Skipping.")
            continue

        # Process each.jsonl file
        for obj in objects_in_stream['Contents']:
            s3_key = obj['Key']
            if s3_key.endswith('.jsonl'):
                print(f"  - Reading file: {s3_key}")
                try:
                    file_obj = s3_client.get_object(Bucket=config.s3_bucket_name, Key=s3_key)
                    # Fix: Access the Body and then read from it
                    file_content = file_obj['Body'].read().decode('utf-8')
                    
                    # Each line in a jsonl file is a separate JSON object
                    for line in file_content.strip().split('\n'):
                        if line:
                            record = json.loads(line)
                            # We only care about the actual data, not the Airbyte metadata
                            if '_airbyte_data' in record:
                                consolidated_data[stream_name].append(record['_airbyte_data'])
                            else:
                                # If there's no _airbyte_data wrapper, use the entire record
                                consolidated_data[stream_name].append(record)
                except Exception as e:
                    print(f"    - Failed to process file {s3_key}: {e}")

    return consolidated_data

# --- API Endpoint ---
@app.post("/transform", summary="Transform S3 JSONL to Single JSON")
async def create_transformation(config: S3Config):
    """
    Provide your AWS S3 credentials and bucket information.
    This endpoint will scan the specified path for stream directories,
    read all `.jsonl` files within them, and return a single consolidated
    JSON object.
    """
    try:
        transformed_json = fetch_and_transform_from_s3(config)
        return transformed_json
    except HTTPException as e:
        # Re-raise HTTPExceptions to let FastAPI handle the response
        raise e
    except Exception as e:
        # Catch any other unexpected errors
        raise HTTPException(status_code=500, detail=f"An internal error occurred: {str(e)}")

@app.get("/", summary="Health Check")
def read_root():
    return {"status": "OK"}