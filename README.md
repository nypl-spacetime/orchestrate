# Space/Time Orchestrator

Outputs list of [Space/Time ETL steps](https://github.com/nypl-spacetime/spacetime-etl) in order based on their `dependsOn` value.

## Prerequisited

- [spacetime-config](https://github.com/nypl-spacetime/spacetime-config)
- [spacetime-etl](https://github.com/nypl-spacetime/spacetime-etl), with some ETL modules installed

## Installation & Usage

Installation:

    git clone https://github.com/nypl-spacetime/orchestrator.git

Usage:

    node index.js
