import boto3
import json
import os

def lambda_handler(event, context):
    sfn_client = boto3.client('stepfunctions')
    sfn_arn = os.environ['STEP_FUNCTION_ARN'] # Define STEP_FUNCTION_ARN env variable in Lambda

    try:
        # Extract S3 event details
        bucket_name = event['Records'][0]['s3']['bucket']['name']
        object_key = event['Records'][0]['s3']['object']['key']
   

        # Prepare input for Step Function (e.g., S3 object information)
        sfn_input = {
            'bucket': bucket_name,
            'key': object_key
        }

        # Start Step Function execution
        response = sfn_client.start_execution(
            stateMachineArn=sfn_arn,
            input=json.dumps(sfn_input)  # Input must be a JSON string
        )

        print(f"Step Function execution started: {response['executionArn']}")
        return {
            'statusCode': 200,
            'body': json.dumps('Step Function execution started successfully!')
        }

    except Exception as e:
        print(f"Error: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error starting Step Function: {str(e)}')
        }
