import boto3
import botocore.config
import json
from datetime import datetime

'''
Install Latest Boto3 in Lambda to support bedrock to do that

1.  pip install boto3 -t python/
2.  Compress the folder - zip -r python_boto3.zip python
3.  In Lambda UI, create a layer and upload this zip, also add compatible runtimes.
3.  Go to lambda function add a layer under custom layers section
'''


def blog_generate_using_bedrock(blog_topic: str) -> str:
    prompt = f"""<s>[INST]Human: Write a 200 words blog on the topic {blog_topic}
    Assistant:[/INST]
    """
    body = {
        "prompt": prompt,
        "max_tokens": 512,
        "temperature": 0.5,
        "top_p": 0.9,
        "top_k": 50
    }

    try:
        # Initialize the Bedrock client
        bedrock = boto3.client("bedrock-runtime", region_name="ap-southeast-2", config=botocore.config.Config(
            read_timeout=300,
            retries={
                'max_attempts': 3
            }
        ))
        # Invoke the model and capture the response
        response = bedrock.invoke_model(body=json.dumps(body), modelId="mistral.mistral-large-2402-v1:0")
        # Access the body of the response
        response_content = response['body'].read().decode('utf-8')
        # Parse the JSON response content
        response_data = json.loads(response_content)
        print(response_data)
        # Extract the generated blog details
        blog_details = response_data["outputs"][0]["text"]
        return blog_details
    except Exception as e:
        print(f"Error Generating the blog: {e}")
        return ""


def save_blog_details_s3(s3_bucket: str, s3_key: str, blog_details: str):
    s3 = boto3.client("s3")
    try:
        s3.put_object(Bucket=s3_bucket, Key=s3_key, Body=blog_details)
        print(f"Blog saved to S3: {s3_bucket}/{s3_key}")
    except Exception as e:
        print(f"Error saving blog to S3: {e}")


def lambda_handler(event, context):
    print(event)
    event = json.loads(event["body"])
    blog_topic = event["blog_topic"]
    generated_blog = blog_generate_using_bedrock(blog_topic=blog_topic)

    if generated_blog:
        current_time = datetime.now().strftime("%D-%H-%M-%S")
        s3_key = f"blog-output/{current_time}.txt"
        s3_bucket = "aws-bedrock-generations-practise"
        save_blog_details_s3(s3_bucket=s3_bucket, s3_key=s3_key, blog_details=generated_blog)
    else:
        print("Blog Generation Failed | No blog was generated")

    return {
        "statusCode": 200,
        "body": json.dumps("Blog Generation Completed")
    }
