from client.DataIngestionClient import DataIngestionClient

url = "postgresql://docker_user:Sathwick@18@localhost:5433/config_driven_approach"

with DataIngestionClient(database_url=url, config_path="data-sources.yaml") as dataIngestionClient:
    stats = dataIngestionClient.execute_data_source_loading_to_db("market_data_csv")

    # dataIngestionClient.publish_to_cps(list[Proto], cps_url, cps_port, cps_topic)
    print(f"Loaded {stats.successful_records} records")


"""
Flow 1:
    Any input:
        Step 1: Get the File
        Step 2: Create a config - YAML 
        Step 3: Load that DB2
        
Flow 1:
    Any input:
        Step 1: Proto Object / List<Proto>
        Step 2: dataIngestionClient.publish_to_cps(list<Proto>, cps_url, cps_port, cps_topic) with proper error handling.
                dataIngestionClient.persist_proto_to_db2(list<Proto>, table_name, config_path="data-sources.yaml")
                
                Driver Object / List<Proto> as is stored in DB2 (Table is present in the Db)
                    - DIRECT (Priority) / MAPPED
                
                # Python to CPS publish code
                
                # dataIngestionClient.persist_csv_to_db2(list<Proto>, table_name, config_path="data-sources.yaml")
                # dataIngestionClient.persist_json_to_db2(list<Proto>, table_name, config_path="data-sources.yaml")
        Step 2: Create a config - YAML 
        Step 3: Load that DB2
      
      
    Without Framework:
    1. Proto setup in fast api framework ( if no one done this - scratch ) - analysis - 2-3 hours
    
    In framework:
    1. How does code accept any proto list and persist to any db2 table with Direct or Mapped Strategy (2 days)


    JSON result -> DB2 (No Mapping) Expectation is camelCase to SNAKE_CASE
    
    [
        {
            z:x
            z:x
            z:x
        },
        {
            z:x
            z:x
            z:x
        }
    ]
"""
