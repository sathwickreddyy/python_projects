from client.DataIngestionClient import DataIngestionClient

with DataIngestionClient as dataIngestionClient:
    url = "postgresql://docker_user:Sathwick@18@localhost:5433/config_driven_approach"

    stats = dataIngestionClient.execute_data_source_loading_to_db("market_data_csv")

    # dataIngestionClient.publish_to_cps(list[Proto], cps_url, cps_port, cps_topic)
    print(f"Loaded {stats.successful_records} records")