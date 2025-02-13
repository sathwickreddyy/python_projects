import boto3
import logging

# Configure logging
# logging.basicConfig(
#     filename="/var/log/leader_election.log",
#     level=logging.INFO,
#     format="%(asctime)s [%(levelname)s] %(message)s"
# )

class SSMConfigManager:
    def __init__(self, region_name='ap-southeast-2'):
        self.ssm_client = boto3.client('ssm', region_name=region_name)

    def get_parameter(self, name):
        """
        Fetch a parameter value from AWS Systems Manager (SSM) Parameter Store.
        """
        try:
            response = self.ssm_client.get_parameter(Name=name)
            return response['Parameter']['Value']
        except Exception as e:
            logging.error(f"Failed to fetch parameter {name}: {e}")
            print(f"Failed to fetch parameter {name}: {e}")
