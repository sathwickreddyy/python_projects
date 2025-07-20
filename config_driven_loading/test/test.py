from client.DataIngestionClient import DataIngestionClient

url = "postgresql://docker_user:Sathwick@18@localhost:5433/config_driven_approach"

with DataIngestionClient(database_url=url, config_path="data-sources.yaml") as dataIngestionClient:
    stats = dataIngestionClient.execute_data_source_loading_to_db(source_name = "user_profile_json")
    # dataIngestionClient.publish_to_cps(list[Proto], cps_url, cps_port, cps_topic)
    print(f"Loaded {stats.successful_records} records")