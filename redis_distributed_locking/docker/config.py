import os
from dotenv import load_dotenv

class LocalConfigManager:
    def __init__(self, config_file=".env"):
        # Load environment variables from .env file
        load_dotenv(config_file)

    def get_parameter(self, name):
        """
        Fetch a parameter value from local environment variables or .env file.
        """
        value = os.getenv(name)
        if not value:
            raise Exception(f"Configuration parameter '{name}' not found.")
        return value
