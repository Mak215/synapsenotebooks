# IngestAggregatedVNETLogs Notebook

## Overview

These Jupyter Notebooks provides an end-to-end workflow for ingesting and processing exploded VNET logs. The notebook reads data from Azure Data Lake Storage (ADLS), summarizes the data, and writes the results to Azure Data Explorer (Kusto). It is designed to handle large-scale log data by processing it in hourly increments, ensuring efficient and streamlined data ingestion and aggregation.

## Notebook Features

- **ADLS Integration**: Connects to Azure Data Lake Storage (Gen2) to fetch parquet files for processing.
- **Kusto Integration**: Writes aggregated data to a Kusto table for further analysis.
- **Hourly Aggregations**: Processes logs in hourly increments, allowing fine-grained analysis and efficient resource usage.
- **Summarization**: Summarizes logs based on key fields such as `Source_IP`, `Destination_IP`, `TrafficFlow`, and other relevant attributes.
- **Customizable Parameters**: Allows users to configure settings such as date range, region, and linked services for flexible operations.
- **Debugging Utilities**: Includes debugging functions to log and track process flow.

---

## Prerequisites

- **Azure Synapse Environment**: The notebook is configured to run in an Azure Synapse environment with a PySpark kernel.
- **Azure Data Lake Storage (ADLS)**: The data source for the logs must be available in an ADLS Gen2 account.
- **Azure Data Explorer (Kusto)**: The target database and table must be configured in Azure Data Explorer.
- **Synapse Linked Services**: Proper linked services must be set up in Synapse for both ADLS and Kusto.

---

## Parameters

The following parameters can be adjusted to customize the notebook's behavior:

| Parameter                | Description                                                                                     | Example Value                                  |
|--------------------------|-------------------------------------------------------------------------------------------------|-----------------------------------------------|
| `WRITE`                 | Controls whether data is written to Kusto. Useful for testing without actual ingestion.         | `True`                                        |
| `DEBUG`                 | Prints debug statements to output for troubleshooting.                                          | `True`                                        |
| `ETL_DATE`              | Specifies the date for which logs should be processed. Can be `today`, `yesterday`, or a date.  | `"yesterday"`                                 |
| `REGION`                | Specifies the region for the data. Currently supports one region.                               | `"wcus"`                                      |
| `CONTAINER`             | ADLS container where the logs are stored.                                                       | `"prod"`                                      |
| `DATA_PATH`             | Directory path in the ADLS container.                                                           | `"feeds/logs"`                                |
| `DOMAIN`                | ADLS domain service location.                                                                   | `"sipvnetlogsadlsgen2.dfs.core.windows.net"`  |
| `LINKED_SERVICE_NAME`   | Synapse linked service name for connecting to ADLS.                                             | `"AzureDataLakeStorage_sipvnetlogsadlsgen2_LinkedService"` |
| `SIP_DATABASE`          | Name of the Kusto database.                                                                     | `"vnetlogs_uat"`                              |
| `SIP_TABLE`             | Name of the Kusto table where data will be written.                                             | `"logs_aggregated"`                           |
| `SIP_LINKED_SERVICE_NAME` | Synapse linked service name for connecting to Kusto.                                          | `"AzureDataExplorer_Sipvnetlogsprod_LinkedService"` |

---

## Key Components

### 1. **Imports and Setup**
- Loads necessary libraries, including PySpark, datetime utilities, and custom functions.
- Configures the notebook's environment.

### 2. **ADLS Connector**
The `ADLSConnector` class is responsible for:
- Connecting to ADLS using a linked service.
- Reading parquet files from the specified path.
- Generating hourly timestamps for processing.

### 3. **Data Processing**
The notebook processes the data using PySpark:
- Reads parquet files from the ADLS path.
- Summarizes the data into hourly increments.
- Extracts additional metadata, such as subscription IDs and resource groups.

### 4. **Kusto Writer**
The `SIPWriter` class handles:
- Connecting to the Kusto cluster using Synapse linked services.
- Writing summarized data to the specified Kusto table.

### 5. **Debugging Utilities**
Provides helper functions like `log_debug` to log messages and debug the pipeline.

---

## Execution Flow

1. **Configure Parameters**: Adjust the parameters in the `# Parameters` section to define the desired behavior.
2. **Initialize ADLS Connector**: The `ADLSConnector` class connects to ADLS and fetches parquet files for the specified date and region.
3. **Process Data**: The notebook processes the data in hourly increments, applying summarization and metadata extraction.
4. **Write to Kusto**: The `SIPWriter` class writes the summarized data to the configured Kusto table.

---

## Usage Instructions

1. Clone the repository containing this notebook:
   ```bash
   git clone https://github.com/Mak215/synapsenotebooks.git
   ```
2. Open the notebook in Azure Synapse Studio.
3. Configure the parameters as needed.
4. Run the notebook sequentially, ensuring that each step completes successfully.

---

## Debugging

- Set the `DEBUG` parameter to `True` to enable detailed logging.
- Use the `log_debug` function to print intermediate outputs and track the notebook's execution.

---

## Notes

- Ensure that the Synapse environment has the required permissions to access ADLS and Kusto.
- The notebook uses Synapse-specific configurations for authentication and linked services.
- The `WRITE` parameter can be set to `False` to test the pipeline without writing data to Kusto.

---

## License

This project is licensed under the [MIT License](LICENSE).

---

## Contact

For questions or support, please contact [Mak215](https://github.com/Mak215).

--- 

Let me know if you'd like any modifications or additional details!
