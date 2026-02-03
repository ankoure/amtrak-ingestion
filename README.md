# Amtrak Ingestion

A serverless data pipeline built on AWS Chalice that ingests, enriches, and processes real-time train data from multiple transit providers (Amtrak, VIA Rail, and Brightline).

## Overview

This project fetches real-time train data from the [Amtraker API](https://amtraker.com), enriches it with GTFS schedule information, generates arrival and departure events, and stores the processed data in AWS S3 for analysis.

### Key Features

- Real-time train data ingestion from Amtraker API
- GTFS data enrichment for scheduled metrics (headway, travel time)
- Multi-provider support (Amtrak, VIA Rail, Brightline)
- Automated event generation for arrivals and departures
- AWS S3 storage with efficient gzip compression
- Daily data collation and aggregation
- Serverless architecture using AWS Lambda

## Architecture

```text
Amtraker API → read.py → transform.py → write.py → S3
                  ↑
              gtfs.py (GTFS enrichment)
```

### Data Flow

1. **Ingestion**: Fetch train data from Amtraker API every 5 minutes
2. **Validation**: Validate response with Pydantic models
3. **Transformation**: Convert to Polars DataFrame, filter, and split by provider
4. **Enrichment**: Add GTFS direction IDs, scheduled headway, and travel time
5. **Event Generation**: Create arrival/departure events with service dates
6. **Storage**: Compress and upload to S3
7. **Collation**: Daily aggregation of events into CSV files by route/stop

## Installation

### Prerequisites

- Python 3.12+
- AWS account with appropriate permissions
- AWS CLI configured with credentials

### Setup

1. Clone the repository:
   ```bash
   git clone https://github.com/transitmatters/amtrak-ingestion.git
   cd amtrak-ingestion
   ```

2. Create and activate a virtual environment:
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```

3. Install dependencies:
   ```bash
   pip install -e ".[dev]"
   ```

## Configuration

### Environment Variables

Create a `.env` file in the project root:

```bash
AWS_PROFILE=your-profile-name  # Optional, for local development
```

### Provider Configuration

Enable or disable providers in `amtraker_ingestion/chalicelib/config.py`:

```python
AMTRAK_ENABLED = True
VIA_ENABLED = True
BRIGHTLINE_ENABLED = False
```

## Usage

### Local Development

Run the Chalice application locally:

```bash
cd amtraker_ingestion
chalice local
```

Test endpoints:

```bash
# Health check
curl http://localhost:8000/

# Trigger GTFS update
curl -X POST http://localhost:8000/gtfs/update

# Trigger data ingestion
curl -X POST http://localhost:8000/amtraker/update

# Collate specific date
curl -X POST "http://localhost:8000/amtraker/collate?year=2025&month=11&day=15"
```

### Deployment

Deploy to AWS:

```bash
cd amtraker_ingestion
chalice deploy
```

For production:

```bash
chalice deploy --stage prod
```

## Scheduled Tasks

The application runs three scheduled Lambda functions:

| Schedule          | Function               | Description                          |
| ----------------- | ---------------------- | ------------------------------------ |
| Daily 2:00 AM UTC | `update_gtfs_cache`    | Check and update GTFS bundles        |
| Every 5 minutes   | `consume_amtraker_api` | Fetch train data and generate events |
| Daily 3:00 AM UTC | `collate_previous_day` | Aggregate previous day's data        |

## S3 Data Structure

```text
s3://amtrak-performance/
├── GTFS/
│   ├── Amtrak.zip
│   ├── VIA.zip
│   └── last_modified.json
├── Events-live/
│   ├── raw/{Provider}/Year={YYYY}/Month={MM}/Day={DD}/
│   │   └── _{HH}_{MM}.json.gz
│   └── daily-{Provider}-data/{route}_{direction}_{stop}/
│       └── Year={YYYY}/Month={MM}/Day={DD}/events.csv.gz
└── last_checked.json
```

## Event Schema

Each event contains:

| Field               | Type     | Description                                    |
| ------------------- | -------- | ---------------------------------------------- |
| `service_date`      | date     | Operational date of the trip                   |
| `route_id`          | string   | Route identifier                               |
| `trip_id`           | string   | Unique trip identifier                         |
| `direction_id`      | integer  | Direction of travel (0 or 1)                   |
| `stop_id`           | string   | Station code                                   |
| `stop_sequence`     | integer  | Order of stop in trip                          |
| `vehicle_id`        | string   | Vehicle identifier                             |
| `vehicle_label`     | string   | Human-readable vehicle label                   |
| `event_type`        | string   | "ARR" or "DEP"                                 |
| `event_time`        | datetime | Timestamp of the event                         |
| `scheduled_headway` | integer  | Expected time between vehicles (seconds)       |
| `scheduled_tt`      | integer  | Expected travel time from trip start (seconds) |

## Documentation

Build the Sphinx documentation:

```bash
pip install -e ".[docs]"
cd docs
make html
```

View the documentation at `docs/build/html/index.html`.

## Project Structure

```text
amtrak-ingestion/
├── amtraker_ingestion/
│   ├── app.py                 # Chalice application entry point
│   ├── .chalice/              # Chalice configuration
│   └── chalicelib/
│       ├── main.py            # Pipeline orchestration
│       ├── read.py            # API data ingestion
│       ├── write.py           # Event generation
│       ├── transform.py       # Data enrichment
│       ├── gtfs.py            # GTFS processing
│       ├── config.py          # Configuration and logging
│       ├── constants.py       # Constants and enums
│       ├── disk.py            # Local file storage
│       ├── s3_upload.py       # S3 operations
│       ├── timefilter.py      # Time-based filtering
│       └── models/            # Pydantic data models
├── docs/                      # Sphinx documentation
├── tests/                     # Test suite
├── scripts/                   # Utility scripts
└── pyproject.toml            # Project configuration
```

## Technologies

- **Framework**: [AWS Chalice](https://github.com/aws/chalice) (serverless Python)
- **Data Processing**: [Polars](https://pola.rs/) (high-performance DataFrames)
- **Validation**: [Pydantic](https://docs.pydantic.dev/) (data schema validation)
- **Cloud Storage**: AWS S3
- **Scheduling**: AWS EventBridge (via Chalice)
- **Testing**: pytest

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Make your changes
4. Run tests (`pytest`)
5. Commit your changes (`git commit -m 'Add amazing feature'`)
6. Push to the branch (`git push origin feature/amazing-feature`)
7. Open a Pull Request

## License

MIT License

## Acknowledgments

- [Amtraker](https://amtraker.com) for providing the train data API
- [TransitMatters](https://transitmatters.org/) for supporting transit data initiatives
