```
project-root/
├── db_utils.py                           # Reusable DB engine utilities
├── ingestion_utils.py                    # Reusable ingestion wrapper
├── test_connection.py                    # DB connection health check
├── test_user_profiles_ingestion.py       # User profiles MAPPED strategy
├── test_order_details_ingestion.py       # Order details MAPPED strategy  
├── test_direct_ingestion.py              # DIRECT strategy demo
├── test_performance.py                   # Performance & concurrency tests
├── cornercase_validation_issue.py        # Missing/invalid fields
├── cornercase_processing_issue.py        # Data type conversion errors
├── cornercase_malformed_json.py          # Malformed JSON handling
├── cornercase_db_connection_issue.py     # DB connection failures
├── sample-data/                          # Your JSON files
├── data-sources.yaml                     # Your YAML config
└── README.md                             # Quick start guide
```