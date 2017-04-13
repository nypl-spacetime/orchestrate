# NYC Space/Time Directory Orchestrator

Outputs list of [NYC Space/Time Directory ETL steps](https://github.com/nypl-spacetime/spacetime-etl) in order based on their `dependsOn` value.

## Prerequisited

- [spacetime-config](https://github.com/nypl-spacetime/spacetime-config)
- [spacetime-etl](https://github.com/nypl-spacetime/spacetime-etl), with some ETL modules installed

## Installation & Usage

Installation:

    npm install -g nypl-spacetime/orchestrator
    spacetime-orchestrator

Or:

    git clone https://github.com/nypl-spacetime/orchestrator.git
    cd orchestrator
    node index.js

## Commands:

- `list`: Outputs a JSON array with all ETL steps, in the right order
- `graph`: Outputs a JSON graph of ETL steps and their dependencies
- `run`: Runs all ETL steps, in the right order
