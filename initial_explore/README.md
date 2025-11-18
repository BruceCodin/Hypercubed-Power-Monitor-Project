# Initial Explore

## Purpose

This directory contains **exploratory Jupyter notebooks** used during initial research and API testing for the Energy Monitor project. These notebooks are **for development reference only** and are not part of the production pipeline.

## Contents

- API endpoint testing and authentication
- Data schema exploration
- Sample data retrieval and inspection
- Column mapping and data structure analysis

## Scope

### What this IS:
- ✅ Quick prototyping and experimentation
- ✅ Understanding DNO data structures
- ✅ Testing API credentials and access
- ✅ Documentation for team familiarity

### What this is NOT:
- ❌ Production code
- ❌ Part of the ETL pipeline
- ❌ Published or deployed
- ❌ Maintained long-term

## Data Sources Explored

- **SP Energy Networks** - Distribution Network Live Outages
- **UK Power Networks** - Live Faults
- **National Grid** - Live Power Cuts (preliminary testing)

## Usage

These notebooks require API keys stored in `.env` (not committed to repo). Notebooks are intended to be run locally for exploration purposes only.

## Status

**Development Phase Only** - Code in this directory will be refactored and integrated into the main ETL pipeline in `/src/extractors/`.