# Job Scraper System

A distributed job scraping and processing pipeline built with Azure services.

## Architecture Overview

```
┌─────────────┐      ┌──────────────┐      ┌─────────────┐
│   Career    │      │   Producer   │      │  Azure      │
│   Webpage   │─────▶│   Service    │─────▶│  EventHub   │
└─────────────┘      └──────────────┘      └─────────────┘
                            │                      │
                            │                      │
                       (Scrapes &                  │
                        Transforms)                │
                            │                      ▼
                            │               ┌─────────────┐
                            │               │  Consumer   │
                            │               │   Service   │
                            │               └─────────────┘
                            │                      │
                            │                      │
                            │               (Enriches with
                            │                Full Details)
                            │                      │
                            ▼                      ▼
                     Parquet Format         ┌─────────────┐
                                            │    Azure    │
                                            │   Storage   │
                                            └─────────────┘
                                                   │
                                            Parquet + JSON
                                             (with metrics)
```

## Services

### 1. Producer Service

**Purpose**: Scrapes job listings from career webpage and publishes to EventHub

**Workflow**:
- Scrapes career webpage for job listings
- Transforms scraped data into Parquet format
- Sends Parquet data to Azure EventHub for processing

**Output**: Raw job listings in Parquet format

### 2. Consumer Service

**Purpose**: Enriches job data with full descriptions and generates metrics

**Workflow**:
- Consumes Parquet data from Azure EventHub
- Extracts job position names from the data
- Scrapes full job descriptions using position names
- Creates enrichment metrics based on requirements
- Uploads to Azure Storage:
  - Enriched Parquet file with full job data
  - JSON file containing calculated metrics

**Output**: Enriched dataset with metrics

## Known Issues

### 1. Job Title to URL Mapping

**Problem**: Did a workaround by extracting job description url in the initial consuming level

**Details**:
- Slight variations in job title names cause lookup errors
- Example: "Software Engineer" vs "Software Engineer - Backend" may reference the same or different positions

**Impact**: Some job descriptions may fail to be scraped or may retrieve incorrect descriptions

### 2. Infrastructure & Observability Gaps

**Problem**: Limited configuration and monitoring capabilities

**Missing Features**:
- No monitoring/observability layer for tracking pipeline health
- Limited configuration options for Azure Storage I/O operations
- Limited configuration options for EventHub connection management
- No retry mechanisms or error handling strategies
- No logging/alerting for failed scraping attempts

**Impact**: Difficult to debug issues and optimize performance in production

## Future Improvements

1. **Implement robust job title normalization** to improve URL matching
2. **Add Application Insights** for monitoring and diagnostics
3. **Create configurable connection pools** for Azure services
4. **Implement retry policies** with exponential backoff
5. **Add structured logging** with correlation IDs
6. **Create health check endpoints** for both services
7. **Implement circuit breakers** for external dependencies

## Technology Stack

- **Messaging**: Azure EventHub
- **Storage**: Azure Blob Storage
- **Data Format**: Apache Parquet, JSON
- **Services**: Microservices architecture (Producer/Consumer pattern)