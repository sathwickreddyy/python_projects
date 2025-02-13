import boto3
import json
import os

# Initialize SSM client
ssm_client = boto3.client('ssm', region_name='ap-southeast-2')


def lambda_handler(event, context):
    # Define configuration details
    redis_endpoint = os.environ.get('REDIS_ENDPOINT')
    sns_topic_arn = os.environ.get('SNS_TOPIC_ARN')

    try:
        # Write Redis endpoint to SSM Parameter Store
        ssm_client.put_parameter(
            Name="/app/config/redis_endpoint",
            Value=redis_endpoint,
            Type="String",
            Overwrite=True
        )
        print("Redis endpoint updated in SSM Parameter Store.")

        # Write SNS Topic ARN to SSM Parameter Store
        ssm_client.put_parameter(
            Name="/app/config/sns_topic_arn",
            Value=sns_topic_arn,
            Type="String",
            Overwrite=True
        )
        print("SNS Topic ARN updated in SSM Parameter Store.")

        return {
            'statusCode': 200,
            'body': json.dumps('Configuration updated successfully.')
        }
    except Exception as e:
        print(f"Error updating configuration: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps('Error updating configuration.')
        }
