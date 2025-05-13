# eComply Integration Pipelines

## Overview

This project automates data replication for eComply using a series of pipelines.  
It connects to ArcGIS and eComply services, processes domain values, contracts, work orders, and work order line items.

```mermaid
graph LR
data[Data Source Adapter] --> val[Validation Filter]
val --> trans[Transformation Filter]
trans --> enrich[Enrichment Filter]
enrich --> repo[Data Repository Adapter]
```

## Configuration

- Logging is configured to use a rotating logger.
- Proxy and credentials are set before connecting to ArcGIS and eComply services.
- Pipeline parameters like URLs, usernames, and passwords should be securely managed (consider environment variables).

## Script Overview

- **pipelines.py**: Main script orchestrating multiple pipeline operations such as querying domains, applying edits, and processing work orders.
- Each pipeline step logs its output for monitoring.
- Exception handling is implemented to log and raise errors if they occur.

## Usage

Run the script from the project root:

```
python pipelines.py
```

Ensure that all dependencies and environment configurations are properly set before executing the pipelines.

## Logging

Log files are stored in the `eComply_logs` directory with the filename `eComply.log`.