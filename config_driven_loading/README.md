Here's your fixed README.md with nested table of contents:

# Config-Driven Data Ingestion Library

A comprehensive, enterprise-ready Python library for flexible data ingestion supporting both direct database connections (DB2) and SQLAlchemy engines, designed for corporate environments with network restrictions.

## 📋 Table of Contents

- [🚀 Quick Start](#-quick-start)
  - [Installation](#installation)
  - [Basic Usage](#basic-usage)
- [🔌 Connection Options](#-connection-options)
  - [1. Read-Only Mode (No Database Connection) (Test Mode)](#1-read-only-mode-no-database-connection-test-mode)
  - [2. DB2 Direct Connection (RECOMMENDED)](#2-db2-direct-connection-recommended)
  - [3. SQLAlchemy Engine (Alternate Approach - WIP)](#3-sqlalchemy-engine-alternate-approach---wip)
  - [4. Database URL (Not Recommended)](#4-database-url-not-recommended)
- [🔧 Interfaces Available for Client Projects](#-interfaces-available-for-client-projects)
  - [Core Methods](#core-methods)
  - [Utility Methods to Ease Clients](#utility-methods-to-ease-clients)
  - [Connectivity Test Results](#connectivity-test-results)
- [🏭 Production Mode](#-production-mode)
  - [Reference 1: Validation Reference - Complete Validation Workflow](#reference-1-validation-reference---complete-validation-workflow)
  - [Output Examples](#output-examples)
    - [Configuration Validation Success](#configuration-validation-success)
    - [Configuration Validation Failure](#configuration-validation-failure)
  - [Reference 2: DB2 Connection with Full Validation](#reference-2-db2-connection-with-full-validation)
    - [Database Execution Output - Failed](#database-execution-output---failed)
  - [Reference 3: Error Handling](#reference-3-error-handling)
    - [Comprehensive Error Handling Example](#comprehensive-error-handling-example)
- [🧪 Non Prod Mode](#-non-prod-mode)
  - [1. Read-Only Mode (Testing/Validation)](#1-read-only-mode-testingvalidation)
    - [Read-Only Mode Execution Output](#read-only-mode-execution-output)
  - [SQLAlchemy Engine Example](#sqlalchemy-engine-example)
- [🔄 Multiple Ingestions](#-multiple-ingestions)
  - [Sequential Multiple Sources](#sequential-multiple-sources)
  - [Custom Multiple Source Execution](#custom-multiple-source-execution)
  - [Parallel Execution (Advanced)](#parallel-execution-advanced)
- [⚡ Client Implementation Effort](#-client-implementation-effort)
  - [Minimal Implementation](#minimal-implementation)
  - [Basic Production Implementation](#basic-production-implementation)
  - [Complete Production Implementation](#complete-production-implementation)
- [🔧 Troubleshooting](#-troubleshooting)
  - [Common Issues and Solutions](#common-issues-and-solutions)
    - [1. Configuration Validation Fails](#1-configuration-validation-fails)
    - [2. DB2 Connection Issues](#2-db2-connection-issues)
    - [3. Schema/Table Access Issues](#3-schematableschema-access-issues)
    - [4. Memory Issues with Large Files](#4-memory-issues-with-large-files)
    - [5. Performance Issues](#5-performance-issues)
  - [Debug Mode](#debug-mode)
  - [Health Check Script](#health-check-script)
- [📝 Configuration File Example](#-configuration-file-example)
- [🎯 Quick Reference](#-quick-reference)
  - [Essential Imports](#essential-imports)
  - [One-Line Examples](#one-line-examples)
- [🏗️ Internal Implementation Architecture](#️-internal-implementation-architecture)
  - [Framework Architecture](#framework-architecture)
  - [Core Components](#core-components)
  - [Sample Configuration Files](#sample-configuration-files)
    - [Data Sources Configuration (data-sources.yml)](#data-sources-configuration-data-sourcesyml)
  - [High Level Architecture Diagram](#high-level-architecture-diagram)
  - [Class Diagram](#class-diagram)
  - [Sequence Diagram](#sequence-diagram)
  - [Key Developer Benefits](#key-developer-benefits)

## 🚀 Quick Start

### Installation
```bash
pip install config-driven-data-ingestion
```

### Basic Usage
```python
from data_ingestion_client import DataIngestionClient

# Option 1: Read-only mode (no database required)
with DataIngestionClient(config_path="data-sources.yaml") as client:
    stats = client.execute_data_source("my_data_source")
    print(f"Processed {stats.total_records} records")
```

## 🔌 Connection Options

### 1. Read-Only Mode (No Database Connection) (Test Mode)
Perfect for testing, validation, and data preview without database access.

```python
from data_ingestion_client import DataIngestionClient

# Initialize without any database connectivity
client = DataIngestionClient(config_path="data-sources.yaml")
```

### 2. DB2 Direct Connection (RECOMMENDED)
Recommended for corporate environments with network restrictions.

```python
import ms.db2
from data_ingestion_client import DataIngestionClient
from db_utils import DB2ConnectionManager

# Create DB2 connection
conn = DB2ConnectionManager.create_connection("YOUR_DB2_SERVER")

# Initialize with DB2 connection
client = DataIngestionClient(
    db_connection=conn,
    connection_type="db2",
    config_path="data-sources.yaml"
)
```

### 3. SQLAlchemy Engine (Alternate Approach - WIP)
Traditional approach using SQLAlchemy engines.

```python
from sqlalchemy import create_engine
from data_ingestion_client import DataIngestionClient

# Create SQLAlchemy engine
engine = create_engine("postgresql://user:pass@localhost/db")

# Initialize with engine
client = DataIngestionClient(
    engine=engine,
    config_path="data-sources.yaml"
)
```

### 4. Database URL (Not Recommended)
Library creates SQLAlchemy engine internally.

```python
from data_ingestion_client import DataIngestionClient

# Initialize with database URL
client = DataIngestionClient(
    database_url="postgresql://user:pass@localhost/db",
    config_path="data-sources.yaml"
)
```

## 🔧 Interfaces Available for Client Projects

### Core Methods

| Method                      | Description                                                                    | Returns                   |
|-----------------------------|--------------------------------------------------------------------------------|---------------------------|
| `execute_data_source(name)` | Execute single data source from reading to writing to database (if configured) | `LoadingStats`            |
| `execute_all_sources()`     | Execute all configured sources in YAML provided                                | `Dict[str, LoadingStats]` |
| `get_available_sources()`   | List available data sources configs provided in YAML file                      | `List[str]`               |
| `validate_configuration()`  | Validate configuration of entire YAML                                          | `Dict[str, Any]`          |
| `test_connectivity()`       | Test database connectivity on safer note                                       | `Dict[str, Any]`          |
| `get_connectivity_info()`   | Get connection information - good to have                                      | `Dict[str, Any]`          |
| `get_source_config(name)`   | Get specific source config - good to have                                      | `Optional[Dict]`          |

### Utility Methods to Ease Clients

```python
# Test database connectivity
connectivity_result = client.test_connectivity()

# Get detailed connection information
conn_info = client.get_connectivity_info()

# Validate configuration
validation = client.validate_configuration()

# Get available data sources
sources = client.get_available_sources()
```

### Connectivity Test Results
```json
{
  "connectivity_mode": "direct_connection",
  "test_passed": true,
  "details": {
    "connection_status": "Direct connection available",
    "db_info": {
      "current_server": "<>",
      "current_user": "<>",
      "service_level": "DB2 v11.5.4.0",
      "connection_type": "DB2 Direct Connection"
    }
  }
}
```

## 🏭 Production Mode

### Reference 1: Validation Reference - Complete Validation Workflow before triggering execute_data_source()

```python
from data_ingestion_client import DataIngestionClient
import json

def validate_and_execute():
    """Complete validation and execution workflow"""
    
    with DataIngestionClient(config_path="data-sources.yaml") as client:
        
        # Step 1: Validate configuration
        print("🔍 Step 1: Validating Configuration...")
        validation = client.validate_configuration()
        
        if not validation["valid"]:
            print("❌ Configuration validation failed!")
            print("\n📋 Configuration Errors:")
            for error in validation["errors"]:
                print(f"   - {error}")
            
            print("\n📋 Source-specific Issues:")
            for source_name, source_info in validation["sources"].items():
                if not source_info.get("valid", True):
                    print(f"   ❌ {source_name}:")
                    for error in source_info.get("errors", []):
                        print(f"      - {error}")
            return False
        
        print("✅ Configuration validation passed!")
        
        # Step 2: Check source availability
        print("\n🔍 Step 2: Checking Source Availability...")
        sources = client.get_available_sources()
        print(f"✅ Found {len(sources)} data sources:")
        for source in sources:
            print(f"   - {source}")
        
        # Step 3: Test connectivity (if database connection available)
        print("\n🔍 Step 3: Testing Connectivity...")
        connectivity = client.test_connectivity()
        if connectivity["test_passed"]:
            print(f"✅ Connectivity test passed: {connectivity['connectivity_mode']}")
        else:
            print(f"⚠️  Connectivity mode: {connectivity['connectivity_mode']}")
        
        return True

if __name__ == "__main__":
    validate_and_execute()
```

### Output Examples

#### Configuration Validation Success
```json
{
  "valid": true,
  "connectivity": {
    "mode": "direct_connection",
    "connection_type": "db2",
    "has_database_access": true
  },
  "sources": {
    "user_profile_json": {
      "type": "JSON",
      "target_enabled": true,
      "valid": true,
      "errors": []
    },
    "order_details_json": {
      "type": "JSON", 
      "target_enabled": true,
      "valid": true,
      "errors": []
    }
  },
  "errors": []
}
```

#### Configuration Validation Failure
```json
{
  "valid": false,
  "sources": {
    "invalid_source": {
      "valid": false,
      "errors": [
        "Source file not found: missing_file.json",
        "Missing required field: target_config.table"
      ]
    }
  },
  "errors": [
    "Source invalid_source: Source file not found",
    "Database connectivity validation failed"
  ]
}
```

### Reference 2: DB2 Connection with Full Validation

```python
import ms.db2
from data_ingestion_client import DataIngestionClient
from db_utils import DB2ConnectionManager

def db2_connection_example():
    """Complete DB2 connection example with validation"""
    
    SERVER_NAME = "YOUR_DB2_SERVER"
    
    # Create DB2 connection
    conn = DB2ConnectionManager.create_connection(SERVER_NAME)
    
    try:
        # Test connection health
        conn_test = DB2ConnectionManager.test_connection(conn)
        if not conn_test["success"]:
            print(f"❌ Connection test failed: {conn_test['error']}")
            return
        
        print(f"✅ DB2 connected: {conn_test['current_server']}")
        
        # Initialize client with DB2 connection
        with DataIngestionClient(
            db_connection=conn,
            connection_type="db2",
            config_path="data-sources.yaml"
        ) as client:
            
            # Validate configuration
            validation = client.validate_configuration()
            if not validation["valid"]:
                print("Configuration validation failed")
                return
            
            # Execute ingestion
            stats = client.execute_data_source("user_profile_json")
            print(f"✅ Loaded {stats.successful_records} records to DB2")
            
    finally:
        DB2ConnectionManager.close_connection(conn)

db2_connection_example()
```

#### Database Execution Output - Failed
```json
{
  "total_records": 150,
  "successful_records": 147,
  "error_records": 3,
  "write_time_ms": 1250,
  "records_per_second": 117.6,
  "batch_count": 2,
  "success_rate": 98.0,
  "has_errors": true,
  "validation_errors": [
    "Row 45: Required field 'email' is missing or null",
    "Row 78: Invalid date format for field 'date_of_birth'",
    "Row 132: Field 'age' conversion failed: invalid literal for int()"
  ]
}
```

### Reference 3: Error Handling

#### Comprehensive Error Handling Example
```python
from data_ingestion_client import DataIngestionClient
from models.core.exceptions import DataIngestionException

def robust_ingestion_example():
    """Comprehensive error handling example"""
    
    try:
        with DataIngestionClient(config_path="data-sources.yaml") as client:
            
            # Validate configuration first
            validation = client.validate_configuration()
            if not validation["valid"]:
                handle_configuration_errors(validation)
                return False
            
            # Test connectivity
            connectivity = client.test_connectivity()
            if not connectivity["test_passed"]:
                print(f"⚠️ Connectivity issue: {connectivity.get('details', {}).get('error', 'Unknown')}")
            
            # Execute with error handling
            source_name = "user_profile_json"
            stats = client.execute_data_source(source_name)
            
            # Check for processing errors
            if stats.has_errors:
                handle_processing_errors(stats)
            
            return stats.error_records == 0
            
    except DataIngestionException as e:
        print(f"❌ Data Ingestion Error: {str(e)}")
        return False
    except FileNotFoundError as e:
        print(f"❌ Configuration file not found: {str(e)}")
        return False
    except Exception as e:
        print(f"❌ Unexpected error: {str(e)}")
        return False

def handle_configuration_errors(validation):
    """Handle configuration validation errors"""
    print("❌ Configuration Validation Failed!")
    print("\n📋 Global Errors:")
    for error in validation.get("errors", []):
        print(f"   - {error}")
    
    print("\n📋 Source-Specific Errors:")
    for source_name, source_info in validation.get("sources", {}).items():
        if not source_info.get("valid", True):
            print(f"   ❌ {source_name}:")
            for error in source_info.get("errors", []):
                print(f"      - {error}")

def handle_processing_errors(stats):
    """Handle data processing errors"""
    print(f"\n⚠️ Processing completed with {stats.error_records} errors:")
    
    # Show validation errors
    if stats.validation_errors:
        print("📋 Validation Errors:")
        for error in stats.validation_errors[:5]:  # Show first 5
            print(f"   - {error}")
    
    # Show conversion errors
    if stats.conversion_errors:
        print("📋 Conversion Errors:")
        for error in stats.conversion_errors[:5]:  # Show first 5
            print(f"   - {error}")
    
    # Show processing errors
    if stats.processing_errors:
        print("📋 Processing Errors:")
        for error in stats.processing_errors[:5]:  # Show first 5
            print(f"   - {error}")

# Execute with comprehensive error handling
success = robust_ingestion_example()
```

## 🧪 Non Prod Mode

### 1. Read-Only Mode (Testing/Validation)

```python
from data_ingestion_client import DataIngestionClient

def read_only_example():
    """Example of read-only mode for testing without database"""
    
    # No database connection required
    with DataIngestionClient(config_path="data-sources.yaml") as client:
        
        # Validate configuration
        validation = client.validate_configuration()
        print(f"Config valid: {validation['valid']}")
        
        # Execute in print mode (shows sample records)
        stats = client.execute_data_source("user_profile_json")
        print(f"Processed {stats.total_records} records (print mode)")

read_only_example()
```

#### Read-Only Mode Execution Output
```
======================================================================
📋 SAMPLE RECORDS TO BE INSERTED INTO public.user_profiles
======================================================================

📄 Record 1:
   user_id: user001
   first_name: John
   last_name: Doe
   email: john.doe@example.com
   city: New York
   country: USA

📄 Record 2:
   user_id: user002
   first_name: Jane
   last_name: Smith
   email: jane.smith@example.com
   city: Los Angeles
   country: USA

📊 SUMMARY:
   Total records: 2
   Valid records: 2
   Invalid records: 0
   Sample shown: 2
   Database mode: print_only
======================================================================
```

### SQLAlchemy Engine Example

```python
from sqlalchemy import create_engine
from data_ingestion_client import DataIngestionClient

def sqlalchemy_example():
    """SQLAlchemy engine example"""
    
    # Create engine
    engine = create_engine("postgresql://user:pass@localhost/db")
    
    try:
        with DataIngestionClient(
            engine=engine,
            config_path="data-sources.yaml"
        ) as client:
            
            # Test connectivity
            connectivity = client.test_connectivity()
            print(f"Connectivity: {connectivity['test_passed']}")
            
            # Execute ingestion
            stats = client.execute_data_source("user_profile_json")
            print(f"Records: {stats.successful_records}/{stats.total_records}")
            
    finally:
        engine.dispose()

sqlalchemy_example()
```

## 🔄 Multiple Ingestions

### Sequential Multiple Sources
```python
def multiple_sources_sequential():
    """Execute multiple data sources sequentially"""
    
    with DataIngestionClient(
        db_connection=conn,
        connection_type="db2",
        config_path="data-sources.yaml"
    ) as client:
        
        # Execute all sources
        results = client.execute_all_sources()
        
        # Print summary
        total_records = sum(stats.total_records for stats in results.values())
        successful_records = sum(stats.successful_records for stats in results.values())
        
        print(f"📊 Overall Results:")
        print(f"   Sources: {len(results)}")
        print(f"   Records: {successful_records}/{total_records}")
        print(f"   Success Rate: {(successful_records/total_records*100):.1f}%")
        
        # Individual results
        for source_name, stats in results.items():
            status = "✅" if stats.error_records == 0 else "⚠️"
            print(f"   {status} {source_name}: {stats.successful_records}/{stats.total_records}")

multiple_sources_sequential()
```

### Custom Multiple Source Execution
```python
def custom_multiple_sources():
    """Execute specific sources with custom handling"""
    
    source_names = ["user_profile_json", "order_details_json", "product_csv"]
    
    with DataIngestionClient(config_path="data-sources.yaml") as client:
        
        results = {}
        
        for source_name in source_names:
            try:
                print(f"🚀 Processing {source_name}...")
                stats = client.execute_data_source(source_name)
                results[source_name] = {
                    "success": True,
                    "stats": stats
                }
                print(f"✅ {source_name}: {stats.successful_records} records")
                
            except Exception as e:
                results[source_name] = {
                    "success": False,
                    "error": str(e)
                }
                print(f"❌ {source_name}: {str(e)}")
        
        return results

results = custom_multiple_sources()
```

### Parallel Execution (Advanced)
```python
import concurrent.futures
from data_ingestion_client import DataIngestionClient

def parallel_ingestion():
    """Execute multiple sources in parallel (use with caution)"""
    
    def execute_source(source_config):
        source_name, config_path = source_config
        
        # Each thread gets its own client instance
        with DataIngestionClient(config_path=config_path) as client:
            return source_name, client.execute_data_source(source_name)
    
    source_configs = [
        ("user_profile_json", "data-sources.yaml"),
        ("order_details_json", "data-sources.yaml"),
        ("product_csv", "data-sources.yaml")
    ]
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        futures = [executor.submit(execute_source, config) for config in source_configs]
        
        results = {}
        for future in concurrent.futures.as_completed(futures):
            source_name, stats = future.result()
            results[source_name] = stats
            print(f"✅ {source_name}: {stats.successful_records} records")
    
    return results

# Use parallel execution carefully - ensure database can handle concurrent connections
results = parallel_ingestion()
```

## ⚡ Client Implementation Effort

### Minimal Implementation
```python
# Minimal client implementation for read-only mode
from data_ingestion_client import DataIngestionClient

# Single line initialization for testing
with DataIngestionClient(config_path="my-config.yaml") as client:
    stats = client.execute_data_source("my_source")
    print(f"Processed: {stats.total_records} records")
```

### Basic Production Implementation
1. **Install library**: `pip install config-driven-data-ingestion`
2. **Create configuration file**: Define your data sources in YAML
3. **Establish database connectivity**: DB2 connection or SQLAlchemy engine
4. **Add error handling**: Configuration validation and connectivity testing
5. **Implement logging**: Standard Python logging for monitoring

### Complete Production Implementation
1. **Configuration management**: Environment-specific configs
2. **Connection pooling**: Optimal database connection settings
3. **Monitoring integration**: Metrics collection and alerting
4. **Error recovery**: Retry mechanisms and failure handling
5. **Testing suite**: Unit tests for your specific use cases

## 🔧 Troubleshooting

### Common Issues and Solutions

#### 1. Configuration Validation Fails
```bash
❌ Problem: "Source file not found: data/users.json"
✅ Solution: Check file paths are relative to execution directory
```
```python
# Check current directory
import os
print(f"Current directory: {os.getcwd()}")
print(f"Config file exists: {os.path.exists('data-sources.yaml')}")
```

#### 2. DB2 Connection Issues
```bash
❌ Problem: "Failed to connect to DB2 server"
✅ Solution: Verify server name and network connectivity
```
```python
# Test DB2 connection separately
import ms.db2
try:
    conn = ms.db2.connect("YOUR_SERVER")
    print("✅ DB2 connection successful")
    conn.close()
except Exception as e:
    print(f"❌ DB2 connection failed: {e}")
```

#### 3. Schema/Table Access Issues
```bash
❌ Problem: "Table 'schema.table' not found or no access"
✅ Solution: Verify table exists and user has proper permissions
```
```python
# Validate table access
from db_utils import DB2ConnectionManager
conn = DB2ConnectionManager.create_connection("YOUR_SERVER")
result = DB2ConnectionManager.validate_table_access(conn, "SCHEMA", "TABLE")
print(f"Table access: {result}")
```

#### 4. Memory Issues with Large Files
```bash
❌ Problem: "Memory error processing large JSON file"
✅ Solution: Use streaming processing or reduce batch sizes
```
```yaml
# In your data-sources.yaml, reduce batch size
target_config:
  batch_size: 100  # Reduce from default 1000
```

#### 5. Performance Issues
```bash
❌ Problem: "Slow ingestion performance"
✅ Solution: Optimize batch sizes and connection pooling
```
```python
# Optimize for performance
client = DataIngestionClient(
    database_url="postgresql://user:pass@host/db",
    pool_size=20,      # Increase connection pool
    max_overflow=30,   # Allow more concurrent connections
    config_path="config.yaml"
)
```

### Debug Mode
```python
# Enable debug logging for troubleshooting
client = DataIngestionClient(
    config_path="data-sources.yaml",
    log_level="DEBUG",     # Enable debug logging
    json_logs=True        # Structured JSON logs
)
```

### Health Check Script
```python
def health_check():
    """Complete health check for troubleshooting"""
    
    print("🔍 Data Ingestion Library Health Check")
    print("=" * 45)
    
    # Check 1: Configuration file
    config_path = "data-sources.yaml"
    if not os.path.exists(config_path):
        print(f"❌ Configuration file not found: {config_path}")
        return False
    print(f"✅ Configuration file found: {config_path}")
    
    # Check 2: Library initialization
    try:
        with DataIngestionClient(config_path=config_path) as client:
            print("✅ Library initialization successful")
            
            # Check 3: Configuration validation
            validation = client.validate_configuration()
            if validation["valid"]:
                print("✅ Configuration validation passed")
            else:
                print("❌ Configuration validation failed")
                return False
            
            # Check 4: Source availability
            sources = client.get_available_sources()
            print(f"✅ Found {len(sources)} data sources")
            
            # Check 5: Connectivity test
            connectivity = client.test_connectivity()
            print(f"✅ Connectivity mode: {connectivity['connectivity_mode']}")
            
            return True
            
    except Exception as e:
        print(f"❌ Health check failed: {str(e)}")
        return False

if __name__ == "__main__":
    health_check()
```

## 📝 Configuration File Example

```yaml
# data-sources.yaml
data_sources:
  user_profile_json:
    type: "JSON"
    source_config:
      file_path: "data/user_profiles.json"
      json_path: "$.users[*]"
      encoding: "utf-8"
    target_config:
      schema_name: "public"
      table: "user_profiles"
      type: "table"
      batch_size: 1000
      enabled: true
    input_output_mapping:
      mapping_strategy: "MAPPED"
      column_mappings:
        - source: "id"
          target: "user_id"
          data_type: "STRING"
          required: true
        - source: "profile.personal.firstName"
          target: "first_name"
          data_type: "STRING"
          required: true
    validation:
      required_columns: ["id"]
      data_quality_checks: true
```

## 🎯 Quick Reference

### Essential Imports
```python
# Core library
from data_ingestion_client import DataIngestionClient

# DB2 utilities (for corporate environments)
from db_utils import DB2ConnectionManager
import ms.db2

# SQLAlchemy (for standard environments)
from sqlalchemy import create_engine
```

### One-Line Examples
```python
# Read-only mode
DataIngestionClient(config_path="config.yaml").execute_data_source("source_name")

# DB2 direct connection
DataIngestionClient(db_connection=ms.db2.connect("SERVER"), config_path="config.yaml")

# SQLAlchemy engine
DataIngestionClient(engine=create_engine("postgresql://..."), config_path="config.yaml")
```

## 🏗️ Internal Implementation Architecture

Framework that eliminates repetitive data loading code by using configuration files. Instead of writing custom code for each data source, we configure once and reuse everywhere.

### Framework Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   DATA SOURCES  │    │   ORCHESTRATOR   │    │  Database       │
│                 │    │                  │    │    Schema       │
│ • CSV Files     │────│ Config Reader    │────│                 │
│ • Excel Files   │    │ Data Processor   │    │ • api_data      │
│ • REST APIs     │    │ Column Mapper    │    │ • market_trends │
│ • JSON Files    │    │ Database Writer  │    │ • risk_metrics  │
└─────────────────┘    └──────────────────┘    └─────────────────┘
```

### Core Components

- **DataSourceFactory**: Creates appropriate loaders based on configuration type  
- **DataOrchestrator**: Manages the entire pipeline with error handling and retry logic [One Time Implementation]  
- **DataProcessor**: Handles transformations and column mapping  
- **DatabaseWriter**: Executes batch operations to Database schema tables  

### Sample Configuration Files

#### Data Sources Configuration (data-sources.yml)
```yaml
# Data Sources Configuration for Database Schema
data_sources:
  market_data_csv:
    type: "csv"
    source:
      file_path: "/data/market/daily_rates.csv"
      delimiter: ","
      header: true
      encoding: "UTF-8"
    target:
      schema: "MarketData"
      table: "market_trends"
      batch_size: 500
    column_mapping:
      - source: "date" → target: "trade_date"
      - source: "currency_pair" → target: "currency"
      - source: "rate" → target: "exchange_rate"
      - source: "volume" → target: "trading_volume"
  risk_metrics_excel:
    type: "excel"
    source:
      file_path: "/data/risk/monthly_risk.xlsx"
      sheet_name: "RiskData"
      skip_rows: 1
    target:
      schema: "RiskMetrics"
      table: "risk_metrics"
      batch_size: 200
    column_mapping:
      - source: "Portfolio ID" → target: "portfolio_id"
      - source: "VaR 95%" → target: "var_95"
      - source: "Expected Shortfall" → target: "expected_shortfall"
      - source: "Liquidity Score" → target: "liquidity_score"
    validation:
      required_columns: ["Portfolio ID", "VaR 95%"]
      data_quality_checks: true
  rest_api_data:
    type: "rest_api"
    source:
      url: "https://api.provider.com/v1/liq"
      method: "GET"
      headers:
        Authorization: "Bearer ${API_TOKEN}"
        Content-Type: "application/json"
      timeout: 30
      retry_attempts: 3
    target:
      schema: "APIData"
      table: "forecast_data"
      batch_size: 1000
    column_mapping:
      - source: "id" → target: "id"
      - source: "assetClass" → target: "asset_class"
      - source: "predictedLiquidity" → target: "predicted_liquidity"
      - source: "confidenceLevel" → target: "confidence_level"
      - source: "forecastDate" → target: "forecast_date"
  config_json:
    type: "json"
    source:
      file_path: "/config/portfolio_settings.json"
      json_path: "$.portfolios[*]"
    target:
      schema: "ConfigData"
      table: "portfolio_config"
      batch_size: 100
    column_mapping:
      - source: "id" → target: "portfolio_id"
      - source: "name" → target: "portfolio_name"
      - source: "riskProfile" → target: "risk_profile"
      - source: "liquidityThreshold" → target: "liquidity_threshold"

# Global Settings
global_settings:
  error_handling:
    continue_on_error: true
    error_threshold: 10
    notification_email: "dev-team@company.com"
  data_quality:
    enable_validation: true
    null_value_handling: "skip"
    duplicate_handling: "ignore"
  performance:
    connection_pool_size: 10
    query_timeout: 300
    memory_limit: "2GB"
```

### High Level Architecture Diagram

![Architecture Diagram
![Class](zz_image_dump/systemKey Developer Benefits

- **Code Reusability**: Write once, configure multiple times - no duplicate data loading logic
- **Maintenance Reduction**: Single codebase handles all data sources through configuration
- **Easy Onboarding**: New data sources added via YAML files, not code changes
- **Error Handling**: Built-in retry logic and comprehensive error reporting
- **Performance**: Batch processing and connection pooling optimize database operations