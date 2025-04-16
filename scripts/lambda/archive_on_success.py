import boto3
import os
import json
from datetime import datetime

s3_client = boto3.client('s3')

def lambda_handler(event, context):
    # Extract the bucket name and prefix from the event
    bucket_name = event['bucket_name']
    prefix = event['prefix']
    
    # List objects under the specified prefix
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    
    if 'Contents' not in response:
        return {
            'statusCode': 200,
            'body': 'No files found to archive.'
        }
    
    # Get the current date in the format YYYY/MM/DD
    current_date = datetime.now().strftime('%Y/%m/%d')
    
    for obj in response['Contents']:
        object_key = obj['Key']
        
        # Define the new object key for the archive location with the date
        archive_key = f'archive/{current_date}/{os.path.basename(object_key)}'
        
        # Copy the object to the new location
        s3_client.copy_object(
            Bucket=bucket_name,
            CopySource={'Bucket': bucket_name, 'Key': object_key},
            Key=archive_key
        )
        
        # Delete the original object
        s3_client.delete_object(Bucket=bucket_name, Key=object_key)
    
    return {
        'statusCode': 200,
        'body': 'Files moved to archive successfully.'
    }
