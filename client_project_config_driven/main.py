from client.DataIngestionClient import DataIngestionClient

url = "postgresql://docker_user:Sathwick@18@localhost:5433/config_driven_approach"

with DataIngestionClient(database_url=url, config_path="data-sources.yaml") as dataIngestionClient:
    stats = dataIngestionClient.execute_data_source_loading_to_db("market_data_csv")
    print(f"Loaded {stats.successful_records} records")
